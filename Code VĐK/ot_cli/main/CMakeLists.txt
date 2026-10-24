idf_component_register(
    SRCS "esp_ot_cli.c"
    INCLUDE_DIRS "."
    REQUIRES
        openthread
        nvs_flash
        esp_netif
        esp_event
        vfs
        esp_driver_uart
        i2cdev
        ccs811
        sht3x
        json
)