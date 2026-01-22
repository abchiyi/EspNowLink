#ifndef ESP_NOW_LINK_H
#define ESP_NOW_LINK_H

#include "radio.hpp"

RadioLink *createEspNowLink();

void espnow_wifi_init_sta(bool LR_mode = false);
void espnow_wifi_init_apsta(bool LR_mode = false);

#endif
