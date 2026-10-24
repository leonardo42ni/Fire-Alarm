idf_component_register(
    SRCS "esp_ot_rcp.c"
    INCLUDE_DIRS "."
    REQUIRES
        esp_event
        nvs_flash
        vfs
        openthread
        ieee802154
)
