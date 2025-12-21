#ifndef ESP_NOW_LINK_H
#define ESP_NOW_LINK_H

#include "radio.hpp"

RadioLink *createEspNowLink();

void espnow_wifi_init();

// 清除已配对设备信息，强制进入配对模式
esp_err_t espnow_clear_paired_device();

// 获取配对状态（是否完成配对握手）
bool espnow_is_paired();

// 获取连接状态（是否有活跃的数据交互）
// 注意：连接状态 = 配对状态 AND 有活跃数据传输
bool espnow_is_connected();

#endif
