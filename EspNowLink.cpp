#include "EspNowLink.h"

#include "sdkconfig.h"
#include "radio.hpp"
#include "tool.h"

#include "esp_wifi.h"
#include "esp_now.h"
#include "esp_log.h"
#include "esp_err.h"
#include "esp_netif.h"
#include "esp_mac.h"
#include "nvs_flash.h"

#include <atomic>
#include <cstring>

#define TAG "espnow_link"
#define NVS_NAMESPACE "espnow_link" // NVS命名空间
#define HANDSHAKE_TIMEOUT_MS 3000   // 握手超时时间（3秒）

typedef uint8_t mac_t[ESP_NOW_ETH_ALEN]; // MAC地址类型

// 握手包结构（在驱动层拦截，不传递给上层）
typedef struct
{
    uint8_t magic;       // 握手魔数 0xAB（用于识别握手包）
    mac_t peer_mac;      // 对方MAC地址
    uint8_t channel;     // WiFi信道
    uint8_t is_response; // 0=握手请求, 1=握手回复
} __attribute__((packed)) handshake_packet_t;

#define HANDSHAKE_MAGIC 0xAB // 握手包魔数
#define HANDSHAKE_SIZE sizeof(handshake_packet_t)

typedef struct
{
    union
    {
        struct // 主机和从机的MAC地址
        {
            mac_t MAC_MASTER;
            mac_t MAC_SLAVE;
            uint8_t channel;
        };
        uint8_t raw[RADIO_MAX_PACKET_SIZE]; // raw数据
    };
} __attribute__((packed)) bcast_packet_t; // 广播包结构类型

// NVS存储结构体：保存配对设备信息
typedef struct
{
    mac_t peer_mac;  // 配对设备的MAC地址
    uint8_t channel; // WiFi信道
    bool is_valid;   // 标记此结构体是否有效
} __attribute__((packed)) paired_device_info_t;

typedef enum
{
    ps_UNPAIRED,
    ps_PAIRED,
} pairStatus_t;

static std::atomic<pairStatus_t> pairStatus(ps_UNPAIRED); // 配对状态
static std::atomic<int16_t> lastRSSI(0);                  // 最后的RSSI值
static std::atomic<bool> INIT_SUCCESS(false);             // 初始化成功标志
static mac_t MAC_TARGET = {0};                            // 目标MAC地址
static QueueHandle_t radioPackRecv = nullptr;             // 数据接收队列
static QueueHandle_t radioPacketDelivery = nullptr;       // 数据分发队列
static bool stored_device_loaded = false;                 // 标记是否已加载存储的设备
static paired_device_info_t cached_device = {};           // 缓存的历史配对设备
static std::atomic<bool> allow_new_pairing(true);         // 仅手动触发时才发现新设备
static QueueHandle_t handshake_queue = nullptr;           // 握手响应队列

static const mac_t broadcast_addr = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
static volatile bool s_send_in_flight = false; // 当前是否有在飞数据包
static TickType_t s_last_no_mem_log = 0;       // 上次 NO_MEM 日志时间

/*
 * 添加对等对象,使用无加密，STA接口，指定信道
 */
void add_peer(const uint8_t *peer_mac, uint8_t channel)
{
    if (esp_now_is_peer_exist(peer_mac))
    {
        ESP_LOGW(TAG, "Peer already exists: " MACSTR, MAC2STR(peer_mac));
        return;
    }

    esp_now_peer_info_t peer_info = {};
    memcpy(peer_info.peer_addr, peer_mac, ESP_NOW_ETH_ALEN);
    peer_info.channel = channel;   // 使用指定信道
    peer_info.encrypt = false;     // 是否启用加密
    peer_info.ifidx = WIFI_IF_STA; // 使用STA接口

    // 添加对等对象
    esp_err_t ret = esp_now_add_peer(&peer_info);
    if (ret == ESP_OK)
        ESP_LOGI(TAG, "Peer added: " MACSTR " (channel: %d)", MAC2STR(peer_mac), channel);
    else
        ESP_LOGE(TAG, "Failed to add peer: %s", esp_err_to_name(ret));
}

/*
 * 添加对等对象,使用无加密，STA接口，当前信道通讯
 */
void add_peer(const uint8_t *peer_mac)
{
    add_peer(peer_mac, 0); // 使用当前WiFi所在信道
}

/**
 * @brief 保存配对设备信息到NVS
 * @param peer_mac 配对设备的MAC地址
 * @param channel WiFi信道
 * @return esp_err_t 操作结果
 */
static esp_err_t save_paired_device(const uint8_t *peer_mac, uint8_t channel)
{
    paired_device_info_t device_info = {};
    memcpy(device_info.peer_mac, peer_mac, ESP_NOW_ETH_ALEN);
    device_info.channel = channel;
    device_info.is_valid = true;

    nvs_handle_t nvs_handle;
    esp_err_t err = nvs_open(NVS_NAMESPACE, NVS_READWRITE, &nvs_handle);
    if (err != ESP_OK)
    {
        ESP_LOGE(TAG, "Failed to open NVS namespace: %s", esp_err_to_name(err));
        return err;
    }

    // 存储配对设备信息
    err = nvs_set_blob(nvs_handle, "peer_info", &device_info, sizeof(device_info));
    if (err != ESP_OK)
    {
        ESP_LOGE(TAG, "Failed to save device info to NVS: %s", esp_err_to_name(err));
        nvs_close(nvs_handle);
        return err;
    }

    // 提交更改
    err = nvs_commit(nvs_handle);
    if (err != ESP_OK)
    {
        ESP_LOGE(TAG, "Failed to commit NVS changes: %s", esp_err_to_name(err));
        nvs_close(nvs_handle);
        return err;
    }

    ESP_LOGI(TAG, "Device info saved to NVS: " MACSTR ", channel: %d", MAC2STR(peer_mac), channel);
    nvs_close(nvs_handle);
    return ESP_OK;
}

/**
 * @brief 从NVS加载配对设备信息
 * @param device_info 用于存储读取的设备信息的指针
 * @return esp_err_t 操作结果，ESP_OK表示成功读取有效的设备信息
 */
static esp_err_t load_paired_device(paired_device_info_t *device_info)
{
    if (device_info == nullptr)
        return ESP_ERR_INVALID_ARG;

    nvs_handle_t nvs_handle;
    esp_err_t err = nvs_open(NVS_NAMESPACE, NVS_READONLY, &nvs_handle);
    if (err != ESP_OK)
    {
        ESP_LOGD(TAG, "NVS namespace not found, no saved device info: %s", esp_err_to_name(err));
        device_info->is_valid = false;
        return err;
    }

    // 读取配对设备信息
    size_t size = sizeof(paired_device_info_t);
    err = nvs_get_blob(nvs_handle, "peer_info", device_info, &size);
    nvs_close(nvs_handle);

    if (err != ESP_OK)
    {
        ESP_LOGD(TAG, "Failed to load device info from NVS: %s", esp_err_to_name(err));
        device_info->is_valid = false;
        return err;
    }

    if (!device_info->is_valid)
    {
        ESP_LOGW(TAG, "Loaded device info is not valid");
        return ESP_ERR_INVALID_STATE;
    }

    ESP_LOGI(TAG, "Device info loaded from NVS: " MACSTR ", channel: %d",
             MAC2STR(device_info->peer_mac), device_info->channel);
    return ESP_OK;
}

/**
 * @brief 清除NVS中的配对设备信息
 * @return esp_err_t 操作结果
 */
static esp_err_t clear_paired_device()
{
    nvs_handle_t nvs_handle;
    esp_err_t err = nvs_open(NVS_NAMESPACE, NVS_READWRITE, &nvs_handle);
    if (err != ESP_OK)
    {
        ESP_LOGW(TAG, "Failed to open NVS namespace for clearing: %s", esp_err_to_name(err));
        return err;
    }

    err = nvs_erase_key(nvs_handle, "peer_info");
    if (err != ESP_OK && err != ESP_ERR_NVS_NOT_FOUND)
    {
        ESP_LOGE(TAG, "Failed to clear device info from NVS: %s", esp_err_to_name(err));
        nvs_close(nvs_handle);
        return err;
    }

    nvs_commit(nvs_handle);
    nvs_close(nvs_handle);
    ESP_LOGI(TAG, "Device info cleared from NVS");
    return ESP_OK;
}

/**
 * @brief 发送握手请求包
 * @param peer_mac 对方MAC地址
 * @param channel WiFi信道
 * @return ESP_OK 发送成功, 其他值表示失败
 */
static esp_err_t send_handshake_request(const uint8_t *peer_mac, uint8_t channel)
{
    handshake_packet_t hs_pkt = {};
    hs_pkt.magic = HANDSHAKE_MAGIC;
    hs_pkt.is_response = 0; // 握手请求
    hs_pkt.channel = channel;
    memcpy(hs_pkt.peer_mac, peer_mac, ESP_NOW_ETH_ALEN);

    esp_err_t ret = esp_now_send(peer_mac, (uint8_t *)&hs_pkt, HANDSHAKE_SIZE);
    if (ret == ESP_OK)
    {
        ESP_LOGD(TAG, "Handshake request sent to " MACSTR, MAC2STR(peer_mac));
    }
    else
    {
        ESP_LOGW(TAG, "Failed to send handshake request: %s", esp_err_to_name(ret));
    }
    return ret;
}

/**
 * @brief 发送握手回复包
 * @param peer_mac 对方MAC地址
 * @param channel WiFi信道
 * @return ESP_OK 发送成功, 其他值表示失败
 */
static esp_err_t send_handshake_response(const uint8_t *peer_mac, uint8_t channel)
{
    handshake_packet_t hs_pkt = {};
    hs_pkt.magic = HANDSHAKE_MAGIC;
    hs_pkt.is_response = 1; // 握手回复
    hs_pkt.channel = channel;
    memcpy(hs_pkt.peer_mac, peer_mac, ESP_NOW_ETH_ALEN);

    esp_err_t ret = esp_now_send(peer_mac, (uint8_t *)&hs_pkt, HANDSHAKE_SIZE);
    if (ret == ESP_OK)
    {
        ESP_LOGD(TAG, "Handshake response sent to " MACSTR, MAC2STR(peer_mac));
    }
    else
    {
        ESP_LOGW(TAG, "Failed to send handshake response: %s", esp_err_to_name(ret));
    }
    return ret;
}

/**
 * @brief 验证握手请求包（辅助函数，减少代码重复）
 * @param hs_pkt 握手包指针
 * @return true 有效的握手请求, false 无效
 */
static inline bool is_valid_handshake_request(const handshake_packet_t *hs_pkt)
{
    return hs_pkt != nullptr && hs_pkt->magic == HANDSHAKE_MAGIC && hs_pkt->is_response == 0;
}

/**
 * @brief 验证握手响应包（辅助函数，减少代码重复）
 * @param hs_pkt 握手包指针
 * @return true 有效的握手响应, false 无效
 */
static inline bool is_valid_handshake_response(const handshake_packet_t *hs_pkt)
{
    return hs_pkt != nullptr && hs_pkt->magic == HANDSHAKE_MAGIC && hs_pkt->is_response == 1;
}

/**
 * @brief 等待指定对端的握手响应（仅首次配对使用）
 */
static bool wait_handshake_response(const uint8_t *peer_mac, uint32_t timeout_ms)
{
    TickType_t deadline = xTaskGetTickCount() + pdMS_TO_TICKS(timeout_ms);
    handshake_packet_t hs_pkt = {};

    while (true)
    {
        TickType_t now = xTaskGetTickCount();
        if (now >= deadline)
            break;

        TickType_t remain = deadline - now;
        if (xQueueReceive(handshake_queue, &hs_pkt, remain) != pdTRUE)
            break;

        if (is_valid_handshake_response(&hs_pkt) &&
            memcmp(hs_pkt.peer_mac, peer_mac, ESP_NOW_ETH_ALEN) == 0)
        {
            ESP_LOGD(TAG, "Handshake response received from " MACSTR, MAC2STR(hs_pkt.peer_mac));
            return true;
        }
        // 若不是目标对端的响应，丢弃并继续等待
    }

    ESP_LOGW(TAG, "Handshake response timeout from " MACSTR, MAC2STR(peer_mac));
    return false;
}

/**
 * @brief 从机等待指定对端的握手请求（仅首次配对使用）
 */
static bool wait_handshake_request(const uint8_t *peer_mac, handshake_packet_t *hs_pkt)
{
    TickType_t deadline = xTaskGetTickCount() + pdMS_TO_TICKS(HANDSHAKE_TIMEOUT_MS);

    while (true)
    {
        TickType_t now = xTaskGetTickCount();
        if (now >= deadline)
            break;

        TickType_t remain = deadline - now;
        if (xQueueReceive(handshake_queue, hs_pkt, remain) != pdTRUE)
            break;

        if (is_valid_handshake_request(hs_pkt) &&
            memcmp(hs_pkt->peer_mac, peer_mac, ESP_NOW_ETH_ALEN) == 0)
        {
            return true;
        }
        // 若不是目标对端的请求，丢弃并继续等待
    }

    ESP_LOGW(TAG, "Handshake request timeout from " MACSTR, MAC2STR(peer_mac));
    return false;
}

/**
 * @brief 执行完整握手流程（仅在驱动层）
 * @param peer_mac 对方MAC地址
 * @param channel WiFi信道
 * @return true 握手成功, false 握手失败
 * @note 仅首次配对时由主动方调用
 */
static bool perform_handshake(const uint8_t *peer_mac, uint8_t channel)
{
    ESP_LOGI(TAG, "Starting handshake with " MACSTR " (initiator)", MAC2STR(peer_mac));

    // 主动握手：发送请求并等待响应
    for (int retry = 0; retry < 3; retry++)
    {
        ESP_LOGD(TAG, "Handshake attempt %d/3", retry + 1);

        if (send_handshake_request(peer_mac, channel) != ESP_OK)
        {
            vTaskDelay(100 / portTICK_PERIOD_MS);
            continue;
        }

        if (wait_handshake_response(peer_mac, 1000)) // 等待1秒响应
        {
            ESP_LOGI(TAG, "Handshake successful");
            return true;
        }
    }
    ESP_LOGW(TAG, "Handshake failed after 3 attempts");
    return false;
}

/**
 * @brief Master 侧：基于广播得到的从机信息完成初次配对（含初始响应 + 握手）
 */
static bool master_pair_new_slave(bcast_packet_t *bp, radio_packet_t *rp)
{
    uint8_t retry_count = 3;
    while (retry_count > 0)
    {
        add_peer(bp->MAC_SLAVE, bp->channel);

        esp_err_t result = esp_now_send(bp->MAC_SLAVE, rp->raw, sizeof(rp->raw));
        if (result != ESP_OK)
        {
            retry_count--;
            vTaskDelay(10 / portTICK_PERIOD_MS);
            continue;
        }

        ESP_LOGD(TAG, "Initial response sent, performing handshake...");
        if (!perform_handshake(bp->MAC_SLAVE, bp->channel))
        {
            ESP_LOGW(TAG, "Handshake failed with " MACSTR, MAC2STR(bp->MAC_SLAVE));
            retry_count--;
            vTaskDelay(100 / portTICK_PERIOD_MS);
            continue;
        }

        pairStatus.store(ps_PAIRED);
        memcpy(MAC_TARGET, bp->MAC_SLAVE, ESP_NOW_ETH_ALEN);
        ESP_LOGI(TAG, "Connected to new Slave (handshake verified): " MACSTR,
                 MAC2STR(bp->MAC_SLAVE));

        save_paired_device(bp->MAC_SLAVE, bp->channel);
        return true;
    }

    return false;
}

/**
 * @brief Slave 侧：基于广播得到的主机信息完成初次配对（含初始响应 + 握手回复）
 */
static bool slave_pair_new_master(bcast_packet_t *bp, radio_packet_t *rp)
{
    esp_wifi_get_mac(WIFI_IF_STA, bp->MAC_SLAVE);
    wifi_second_chan_t second;
    esp_wifi_get_channel(&bp->channel, &second);

    add_peer(bp->MAC_MASTER);
    esp_err_t result = esp_now_send(bp->MAC_MASTER, rp->raw, sizeof(rp->raw));
    if (result != ESP_OK)
    {
        ESP_LOGD(TAG, "Reply to Master failed: %s", esp_err_to_name(result));
        return false;
    }

    ESP_LOGD(TAG, "Initial response sent, waiting for handshake request...");
    handshake_packet_t hs_pkt = {};
    if (!wait_handshake_request(bp->MAC_MASTER, &hs_pkt))
    {
        return false;
    }

    if (send_handshake_response(bp->MAC_MASTER, bp->channel) != ESP_OK)
    {
        ESP_LOGW(TAG, "Failed to send handshake response to " MACSTR, MAC2STR(bp->MAC_MASTER));
        return false;
    }

    pairStatus.store(ps_PAIRED);
    memcpy(MAC_TARGET, bp->MAC_MASTER, ESP_NOW_ETH_ALEN);
    cached_device = {0};
    memcpy(cached_device.peer_mac, bp->MAC_MASTER, ESP_NOW_ETH_ALEN);
    cached_device.channel = bp->channel;
    cached_device.is_valid = true;
    stored_device_loaded = true;
    allow_new_pairing.store(false);
    ESP_LOGI(TAG, "Paired with new Master (handshake verified): " MACSTR, MAC2STR(bp->MAC_MASTER));

    save_paired_device(bp->MAC_MASTER, bp->channel);
    return true;
}

/**
 *  选择WiFi 启动模式
 *   1. 未配置SSID和密码，启动AP模式
 *   2. 配置了SSID和密码，启动STA模式
 *   3. STA模式下连接失败，启动AP模式
 */
void espnow_wifi_init()
{

    static bool wifi_is_started = false;
    if (wifi_is_started)
        return;

    esp_err_t err = nvs_flash_init();
    if (err == ESP_ERR_NVS_NO_FREE_PAGES || err == ESP_ERR_NVS_NEW_VERSION_FOUND)
    {
        ESP_ERROR_CHECK(nvs_flash_erase());
        err = nvs_flash_init();
    }
    ESP_ERROR_CHECK(err);

    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));
    esp_netif_create_default_wifi_sta();
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_start());

    // 设定协议为 LR（若支持）
#ifdef WIFI_PROTOCOL_LR
    esp_err_t pr = esp_wifi_set_protocol(WIFI_IF_STA, WIFI_PROTOCOL_LR);
    if (pr != ESP_OK)
        ESP_LOGW(TAG, "Set LR protocol failed: %s", esp_err_to_name(pr));
#endif

    // 设置 ESPNOW 速率为 LR 250K（若支持）
    esp_err_t rate_ret =
        esp_wifi_config_espnow_rate(WIFI_IF_STA, WIFI_PHY_RATE_LORA_250K);
    if (rate_ret != ESP_OK)
        ESP_LOGW(TAG, "Set LR rate failed: %s", esp_err_to_name(rate_ret));
    else
        ESP_LOGI(TAG, "ESP-NOW LR rate set OK");

    // 设置最大发射功率
    ESP_ERROR_CHECK(esp_wifi_set_max_tx_power(84)); // 约等于 20dBm
    wifi_is_started = true;
}

class EspNowLink : public RadioLink
{
public:
    esp_err_t recv(radio_packet_t *rp) override;
    esp_err_t send(radio_packet_t *rp) override;
    bool is_connected() override;
    esp_err_t start() override;
    esp_err_t rest() override;
};

// ESP-NOW 接收回调函数
static void data_recv_cb(const esp_now_recv_info_t *recv_info, const uint8_t *data, int len)
{
    // 获取目的/源MAC地址
    const uint8_t *dst_mac = recv_info->des_addr;
    const uint8_t *src_mac = recv_info->src_addr;
    // 获取RSSI
    wifi_pkt_rx_ctrl_t *rx_ctrl = (wifi_pkt_rx_ctrl_t *)recv_info->rx_ctrl;
    int rssi = rx_ctrl->rssi;

    // 判断是否为广播包
    bool broadcast = (dst_mac[0] & 0x01) != 0; // 广播地址第一个字节最低位为1

    lastRSSI.store(rssi);
    if (data == nullptr)
        return;

    // ★ 新增：检查是否为握手包（驱动层拦截）
    if (len == HANDSHAKE_SIZE && data[0] == HANDSHAKE_MAGIC)
    {
        // 这是握手包，在驱动层处理，不传给应用层
        auto hs_pkt = (handshake_packet_t *)data;
        ESP_LOGD(TAG, "Handshake packet received: is_response=%d", hs_pkt->is_response);

        // 将握手包放入握手队列
        if (handshake_queue != nullptr)
        {
            xQueueOverwrite(handshake_queue, (void *)data);
        }
        return; // ★ 不继续处理，直接返回
    }

    // 已配对后仅接受来自目标对端的业务数据（丢弃其它来源/广播）
    if (pairStatus.load() == ps_PAIRED)
    {
        if (src_mac == nullptr || memcmp(src_mac, MAC_TARGET, ESP_NOW_ETH_ALEN) != 0)
        {
            return;
        }
    }

    // 正常数据包处理
    auto rp = (radio_packet_t *)data;
    rp->is_broadcast = broadcast;

    if (len > sizeof(radio_packet_t))
    {
        ESP_LOGW(TAG, "Recv data 长度超过 32字节, len:%d", len);
        return;
    }

    xQueueOverwrite(radioPackRecv, (radio_packet_t *)data);
}

// ESP-NOW 发送回调函数
static void data_sent_cb(const uint8_t *mac_addr, esp_now_send_status_t status)
{
    // 发送完成（成功或失败）后释放在飞标志
    s_send_in_flight = false;
    if (status != ESP_NOW_SEND_SUCCESS)
    {
        ESP_LOGW(TAG, "ESP-NOW send callback status=%d", (int)status);
    }
}

void master_pairing_loop()
{
    // 等待初始化完毕
    while (!INIT_SUCCESS.load())
        vTaskDelay(100);

    // 若已加载历史设备，直接采用历史设备作为通信对端（不再进行握手）
    if (stored_device_loaded && cached_device.is_valid)
    {
        add_peer(cached_device.peer_mac, cached_device.channel);
        memcpy(MAC_TARGET, cached_device.peer_mac, ESP_NOW_ETH_ALEN);
        pairStatus.store(ps_PAIRED);
        ESP_LOGI(TAG, "Reconnected to saved Slave without handshake: " MACSTR,
                 MAC2STR(cached_device.peer_mac));
        return;
    }

    // 进入新设备发现模式：广播查找新的从机
    ESP_LOGI(TAG, "Broadcasting to discover new Slave...");
    while (pairStatus.load() == ps_UNPAIRED)
    {
        radio_packet_t rp = {};
        auto bp = (bcast_packet_t *)rp.data;

        // 获取本机MAC地址&channel
        esp_wifi_get_mac(WIFI_IF_STA, bp->MAC_MASTER);

        wifi_second_chan_t second;
        esp_wifi_get_channel(&bp->channel, &second);

        // * STEP 1 在当前信道广播数据包 100ms/次
        auto ret = esp_now_send(broadcast_addr, rp.raw, sizeof(rp.raw));
        ESP_ERROR_CHECK(ret);

        if (xQueueReceive(radioPackRecv, &rp, 100 / portTICK_PERIOD_MS) == pdTRUE)
        {
            // * STEP 2 收到响应，验证并完成配对
            auto is_not_broadcast = rp.is_broadcast == 0;
            auto macCheckOk = is_valid_mac(bp->MAC_SLAVE, sizeof(mac_t));
            if (macCheckOk && is_not_broadcast)
            {
                if (master_pair_new_slave(bp, &rp))
                    break; // 成功连接，跳出循环
            }
            else
            {
                ESP_LOGW(TAG, "Ignore packet, slave MAC %s, broadcast %s",
                         macCheckOk ? "OK" : "ERR",
                         is_not_broadcast ? "No" : "Yes");
            }
        };
    }
};

/*
 * 从机配对流程：
 *  - 监听主机的广播包（校验 CRC、广播标志、主机 MAC 合法性）
 *  - 回填自身 MAC 与当前信道，重新计算校验和
 *  - 将主机加入对等节点并回复相同数据包
 *  - 回复成功则记录目标 MAC 并将配对状态置为已配对
 */
void slave_pairing_loop()
{
    while (!INIT_SUCCESS.load())
        vTaskDelay(100);

    while (pairStatus.load() == ps_UNPAIRED)
    {
        // 历史设备直接重连（不进行握手，除非手动触发新设备配对）
        if (stored_device_loaded)
        {
            add_peer(cached_device.peer_mac, cached_device.channel);
            memcpy(MAC_TARGET, cached_device.peer_mac, ESP_NOW_ETH_ALEN);
            pairStatus.store(ps_PAIRED);
            ESP_LOGI(TAG, "Reconnected to saved Master without handshake: " MACSTR,
                     MAC2STR(cached_device.peer_mac));
            return;
        }

        // 未允许自动发现新设备时，仅等待手动触发
        if (!allow_new_pairing.load())
        {
            vTaskDelay(500 / portTICK_PERIOD_MS);
            continue;
        }

        // 进入新设备发现模式：监听广播包查找新的主机（仅在手动触发时）
        ESP_LOGI(TAG, "Listening for broadcast to discover new Master...");
        radio_packet_t rp = {};

        // 阻塞等待来自主机的广播
        if (xQueueReceive(radioPackRecv, &rp, portMAX_DELAY) != pdTRUE)
            continue;

        auto bp = (bcast_packet_t *)rp.data;

        bool is_broadcast = rp.is_broadcast != 0;
        bool masterMacOk = is_valid_mac(bp->MAC_MASTER, sizeof(mac_t));

        if (!(is_broadcast && masterMacOk))
        {
            ESP_LOGD(TAG, "Ignore packet, master MAC %s, broadcast %s",
                     masterMacOk ? "OK" : "ERR",
                     is_broadcast ? "Yes" : "No");
            continue;
        }

        if (slave_pair_new_master(bp, &rp))
            return;
    }
};

/**
 *  处理设备配对和收包
 */
void espnow_link_task(void *pvParameters)
{
    static radio_packet_t rp = {};

    while (true)
    {
        switch (pairStatus.load())
        {
        case ps_UNPAIRED:
        {
#ifdef CONFIG_RADIO_LINK_SLAVE_MODE
            slave_pairing_loop();
#else
            master_pairing_loop();
#endif
        }
        break;

        case ps_PAIRED:
        {
            // 仅处理业务数据（心跳机制已移除）
            if (xQueueReceive(radioPackRecv, &rp, portMAX_DELAY) == pdTRUE)
            {
                xQueueOverwrite(radioPacketDelivery, &rp);
            }
        }
        break;
        default:
            break;
        }
    }
}

esp_err_t EspNowLink::recv(radio_packet_t *rp)
{
    xQueueReceive(radioPacketDelivery, rp, portMAX_DELAY);
    return ESP_OK;
}

esp_err_t EspNowLink::send(radio_packet_t *rp)
{
    if (pairStatus.load() != ps_PAIRED)
        return ESP_ERR_NOT_FINISHED;

    // 背压：上一包尚未完成则丢弃当前帧，保持节拍不堆积
    if (s_send_in_flight)
    {
        return ESP_ERR_INVALID_STATE;
    }

    s_send_in_flight = true;
    /* *根据实际载荷大小发送数据 */
    esp_err_t ret = esp_now_send(MAC_TARGET,
                                 rp->raw,
                                 rp->len + RADIO_PACKET_BASIC_SIZE);
    if (ret != ESP_OK)
    {
        // 发送未入队，释放在飞标志
        s_send_in_flight = false;
        if (ret == ESP_ERR_ESPNOW_NO_MEM)
        {
            // 队列满或内存不足：节制打印日志（1s节流）
            TickType_t now = xTaskGetTickCount();
            if (now - s_last_no_mem_log > pdMS_TO_TICKS(1000))
            {
                s_last_no_mem_log = now;
                ESP_LOGW(TAG, "ESP-NOW queue full: %s (len=%u)", esp_err_to_name(ret), (unsigned)sizeof(*rp));
            }
        }
        else
        {
            ESP_LOGE(TAG, "esp_now_send failed: %s (len=%u)", esp_err_to_name(ret), (unsigned)sizeof(*rp));
        }
    }
    return ret;
}

esp_err_t EspNowLink::start()
{

    // 创建队列
    radioPackRecv = xQueueCreate(1, sizeof(radio_packet_t));
    configASSERT(radioPackRecv != nullptr);

    radioPacketDelivery = xQueueCreate(1, sizeof(radio_packet_t));
    configASSERT(radioPacketDelivery != nullptr);

    // 创建握手队列（驱动层拦截的握手包队列）
    handshake_queue = xQueueCreate(1, HANDSHAKE_SIZE);
    configASSERT(handshake_queue != nullptr);

    // 创建任务
    xTaskCreate(espnow_link_task, "espnow_link_task", 1024 * 10, NULL, TP_H, NULL);

    /*** WiFi ***/
    espnow_wifi_init(); // 初始化WiFi
    // 关闭省电，降低发送排队延迟
    ESP_ERROR_CHECK(esp_wifi_set_ps(WIFI_PS_NONE));

    /*** ESP-NOW ***/
    // 初始化 ESP-NOW
    ESP_ERROR_CHECK(esp_now_init());

    add_peer(broadcast_addr); // 添加广播地址

    // 注册回调函数
    ESP_ERROR_CHECK(esp_now_register_recv_cb(data_recv_cb));
    ESP_ERROR_CHECK(esp_now_register_send_cb(data_sent_cb));

    // 尝试设置 ESP-NOW 通讯速率为 LORA 模式
    esp_err_t rate_result = esp_wifi_config_espnow_rate(WIFI_IF_STA, WIFI_PHY_RATE_LORA_500K);
    if (rate_result == ESP_OK)
        ESP_LOGI(TAG, "Set ESPNOW WIFI_PHY_RATE_LORA_500K");
    else
        ESP_LOGI(TAG, "Set ESPNOW RATE FAIL: %s", esp_err_to_name(rate_result));

    // 在启动阶段尝试加载历史配对设备，仅执行一次
    if (!stored_device_loaded)
    {
        paired_device_info_t loaded = {};
        if (load_paired_device(&loaded) == ESP_OK)
        {
            cached_device = loaded;
            stored_device_loaded = true;
            allow_new_pairing.store(false);
            ESP_LOGI(TAG, "Found saved device; will reconnect without handshake: " MACSTR,
                     MAC2STR(cached_device.peer_mac));
        }
        else
        {
            ESP_LOGI(TAG, "No saved device loaded; new pairing allowed on demand");
        }
    }

    INIT_SUCCESS.store(true);
    return ESP_OK;
}

bool EspNowLink::is_connected()
{
    return pairStatus.load() == ps_PAIRED;
}

/**
 * @brief 强制进入新设备配对模式，停止握手历史设备，转而发现新设备
 *
 * 调用此接口后，会立即停止与历史设备的握手尝试，清除已保存的配对信息，
 * 并进入新设备发现模式。配对循环会回到广播/监听模式寻找新的设备。
 *
 * @return esp_err_t 操作结果
 */
esp_err_t EspNowLink::rest()
{
    // 重置通信链路：清除已保存配对、允许新设备发现并回到未配对状态
    stored_device_loaded = false;
    allow_new_pairing.store(true);
    cached_device = {};

    esp_err_t ret = clear_paired_device();
    if (ret != ESP_OK && ret != ESP_ERR_NVS_NOT_FOUND)
    {
        ESP_LOGW(TAG, "Failed to clear paired device: %s", esp_err_to_name(ret));
        // 即便清除失败，也继续执行重置到未配对状态
    }

    pairStatus.store(ps_UNPAIRED);
    memset(MAC_TARGET, 0, ESP_NOW_ETH_ALEN);

    ESP_LOGI(TAG, "Link reset: cleared saved device and enabled new pairing discovery");
    return ESP_OK;
};

RadioLink *createEspNowLink()
{
    return (RadioLink *)new EspNowLink();
}
