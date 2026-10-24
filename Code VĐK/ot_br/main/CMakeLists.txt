idf_component_register(
    SRCS "esp_ot_br.c"
    INCLUDE_DIRS "."
    REQUIRES
        nvs_flash
        esp_event
        esp_netif
        esp_wifi
        openthread
        vfs
        esp_coex
        mdns
        esp_http_client
        esp_http_server
        driver
        cjson
        mqtt
)

target_compile_definitions(${COMPONENT_LIB} PUBLIC "OPENTHREAD_FTD=1")