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
static bool stored_device_loaded = false;                 // 历史设备加载标记
static paired_device_info_t cached_device = {};           // 历史配对设备
static std::atomic<bool> allow_new_pairing(true);         // 发现新设备标志
static TickType_t s_last_recv = 0;                        // 上次接收时间
static const int TIMEOUT_NO_RECV_MS = 80;                 // 无接收超时时间

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
        ESP_LOGW(TAG, "ESP-NOW send callback - Send Fail");
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

            // * STEP 2 收到从机响应，验证并告知从机接受配对
            auto macCheckOk = is_valid_mac(bp->MAC_SLAVE, sizeof(mac_t));
            if (!macCheckOk)
            {
                ESP_LOGW(TAG, "Invalid Slave MAC in response");
                continue;
            }

            /**
             *从机的响应数据包既是广播数据包，但不同的是广播包中的 MAC_SLAVE
             *字段会被从机填充为自己的 MAC 地址，以便主机识别。主机只需要校验该
             *MAC 地址是否合法即可将数据包返回从机以告知从机配对成功。
             */
            add_peer(bp->MAC_SLAVE);
            auto ret = esp_now_send(bp->MAC_SLAVE, rp.raw, sizeof(rp.raw));

            if (ret != ESP_OK)
            {
                ESP_LOGW(TAG, "Failed to send initial response to Slave: %s", esp_err_to_name(ret));
                continue;
            }

            /*
             * 设置配对成功状态，储存目标MAC地址到NVS
             */
            pairStatus.store(ps_PAIRED);
            memcpy(MAC_TARGET, bp->MAC_SLAVE, ESP_NOW_ETH_ALEN);
            save_paired_device(bp->MAC_SLAVE, bp->channel);
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
        auto bp = (bcast_packet_t *)rp.data;

        // 阻塞等待来自主机的广播
        if (xQueueReceive(radioPackRecv, &rp, portMAX_DELAY) != pdTRUE)
            continue;
        /**
         *从机的响应数据包既是广播数据包，但不同的是广播包中的 MAC_SLAVE
         *字段会被从机填充为自己的 MAC 地址，以便主机识别。主机只需要校验该
         *MAC 地址是否合法即可将数据包返回从机以告知从机配对成功。
         */

        if (!rp.is_broadcast)
        {
            ESP_LOGW(TAG, "Received non-broadcast packet during pairing, ignoring");
            continue;
        }

        bool masterMacOk = is_valid_mac(bp->MAC_MASTER, sizeof(mac_t));
        if (!masterMacOk)
        {
            ESP_LOGW(TAG, "Invalid Master MAC in broadcast");
            continue;
        }

        // 缓存主机MAC地址
        mac_t cache_mac_master = {0};
        memcpy(&cache_mac_master, bp->MAC_MASTER, ESP_NOW_ETH_ALEN);

        // * STEP 1 收到主机广播，回填自身MAC与channel后回复
        if (!is_valid_mac(bp->MAC_MASTER, sizeof(mac_t)))
        {
            ESP_LOGW(TAG, "Invalid Master MAC in broadcast");
            continue;
        }

        add_peer(cache_mac_master);
        wifi_second_chan_t second;
        esp_wifi_get_mac(WIFI_IF_STA, bp->MAC_SLAVE);
        esp_wifi_get_channel(&bp->channel, &second);

        // * STEP 2 回复并等待主机确认配对
        auto result =
            esp_now_send(cache_mac_master, rp.raw, sizeof(rp.raw));
        if (result != ESP_OK)
        {
            ESP_LOGW(TAG, "Reply to Master failed: %s", esp_err_to_name(result));
            continue;
        }

        result = xQueueReceive(radioPackRecv, &rp, pdMS_TO_TICKS(100));
        if (result != pdTRUE && !rp.is_broadcast)
        {
            ESP_LOGW(TAG, "No confirmation from Master, retrying...");
            continue;
        }

        if (memcmp(bp->MAC_MASTER, cache_mac_master, ESP_NOW_ETH_ALEN) != 0)
        {
            pairStatus.store(ps_PAIRED);
            memcpy(MAC_TARGET, bp->MAC_MASTER, ESP_NOW_ETH_ALEN);
            save_paired_device(cache_mac_master, bp->channel);
        }
    };
}

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
                s_last_recv = xTaskGetTickCount(); // 更新最后接收时间
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

/**
 * @brief 检查ESP-NOW链接的连接状态
 *
 * @details 通过检查配对状态和接收超时来判断设备是否保持连接。
 *          当设备已配对且在指定时间内收到过消息时，认为连接有效。
 * @return true  - 设备已配对且未超时，连接正常
 * @return false - 设备未配对或超过接收超时时间，连接断开
 */
bool EspNowLink::is_connected()
{
    TickType_t now = xTaskGetTickCount();
    auto is_timeout = (now - s_last_recv) > pdMS_TO_TICKS(TIMEOUT_NO_RECV_MS);
    return (pairStatus.load() == ps_PAIRED) && !is_timeout;
}

/**
 * @brief 强制进入新设备配对模式
 *
 * 调用此接口后，清除已保存的配对信息，
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
