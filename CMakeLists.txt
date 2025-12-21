set(srcs

    "EspNowLink.h"
)

set(include_dirs ".")

idf_component_register(SRCS "EspNowLink.cpp" "${srcs}"
                       INCLUDE_DIRS "${include_dirs}"
                       PRIV_REQUIRES nvs_flash
                       REQUIRES Tool Radio esp_wifi esp_netif
                       )
