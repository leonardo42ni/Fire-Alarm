#include <stdio.h>
#include <string.h>
#include "nvs_flash.h"
#include "esp_event.h"
#include "esp_netif.h"
#include "esp_log.h"
#include "esp_openthread.h"
#include "esp_openthread_netif_glue.h"
#include "esp_openthread_types.h"
#include "esp_openthread_lock.h"
#include "esp_vfs_eventfd.h"
#include "openthread/instance.h"
#include "openthread/thread.h"
#include "openthread/dataset.h"
#include "openthread/coap.h"
#include "openthread/ip6.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "driver/gpio.h"
#include "i2cdev.h"
#include "ccs811.h"
#include "sht3x.h"
#include "cJSON.h"

#define TAG "ot_router"

// I2C pins (ESP32-H2)
#define I2C_SDA_PIN     GPIO_NUM_4
#define I2C_SCL_PIN     GPIO_NUM_5
#define I2C_PORT        I2C_NUM_0

// CCS811 WAK pin: LOW = thức, HIGH = ngủ
#define CCS811_WAK_PIN  GPIO_NUM_12
// CCS811 RST pin: HIGH = hoạt động bình thường, LOW = reset
#define CCS811_RST_PIN  GPIO_NUM_10

// CoAP: gửi multicast ff03::1 (realm-local all-nodes) đến border router
#define COAP_PEER_ADDR  "ff03::1"
#define COAP_PEER_PORT  5683
#define COAP_URI_PATH   "sensor"

// Chu kỳ gửi dữ liệu (ms)
#define SENSOR_INTERVAL_MS  10000

typedef struct {
    float    temperature; // °C
    float    humidity;    // %RH
    uint16_t co2;         // ppm
    uint16_t tvoc;        // ppb
} sensor_data_t;

static ccs811_dev_t   s_ccs811;
static sht3x_t        s_sht3x;
static otCoapResource s_relay_res;

// ─── CoAP ────────────────────────────────────────────────────────────────────

// Gửi payload JSON đến border router - gọi từ trong OT context (lock đã được giữ)
static void coap_relay_send(otInstance *instance, const char *payload)
{
    otMessage *msg = otCoapNewMessage(instance, NULL);
    if (!msg) { ESP_LOGE(TAG, "relay: alloc fail"); return; }

    otCoapMessageInit(msg, OT_COAP_TYPE_NON_CONFIRMABLE, OT_COAP_CODE_POST);
    otCoapMessageGenerateToken(msg, OT_COAP_DEFAULT_TOKEN_LENGTH);

    otError err;
    err = otCoapMessageAppendUriPathOptions(msg, COAP_URI_PATH);
    if (err != OT_ERROR_NONE) goto fail;
    err = otCoapMessageAppendContentFormatOption(msg, OT_COAP_OPTION_CONTENT_FORMAT_JSON);
    if (err != OT_ERROR_NONE) goto fail;
    err = otCoapMessageSetPayloadMarker(msg);
    if (err != OT_ERROR_NONE) goto fail;
    err = otMessageAppend(msg, payload, (uint16_t)strlen(payload));
    if (err != OT_ERROR_NONE) goto fail;

    {
        otMessageInfo fwd_info;
        memset(&fwd_info, 0, sizeof(fwd_info));
        otIp6AddressFromString(COAP_PEER_ADDR, &fwd_info.mPeerAddr);
        fwd_info.mPeerPort = COAP_PEER_PORT;
        err = otCoapSendRequest(instance, msg, &fwd_info, NULL, NULL);
    }
    if (err == OT_ERROR_NONE) {
        ESP_LOGI(TAG, "relay -> BR: %s", payload);
        return;
    }
fail:
    ESP_LOGE(TAG, "relay: send fail %d", err);
    otMessageFree(msg);
}

// CoAP resource handler: nhận từ SED, inject node_id từ IPv6 source, chuyển tiếp lên BR
// Tự động xử lý mọi SED mới mà không cần sửa code
static void coap_relay_handler(void *ctx, otMessage *msg, const otMessageInfo *info)
{
    otInstance *instance = (otInstance *)ctx;
    uint8_t buf[256];
    uint16_t len = otMessageRead(msg, otMessageGetOffset(msg), buf, sizeof(buf) - 1);
    buf[len] = '\0';

    cJSON *json = cJSON_Parse((char *)buf);
    if (!json) { ESP_LOGE(TAG, "relay: bad JSON"); return; }

    // Nếu payload đã có node_id → là gói của chính router bị loopback, bỏ qua
    if (cJSON_GetObjectItem(json, "node_id")) {
        cJSON_Delete(json);
        return;
    }

    // Dùng nhóm hex cuối của IPv6 source làm node_id → mỗi SED có ID riêng tự động
    char src_ip[48];
    otIp6AddressToString(&info->mPeerAddr, src_ip, sizeof(src_ip));
    const char *last_colon = strrchr(src_ip, ':');
    const char *suffix = last_colon ? last_colon + 1 : src_ip;
    char node_id[32];
    snprintf(node_id, sizeof(node_id), "SED_%.4s", suffix);

    cJSON_AddStringToObject(json, "node_id", node_id);
    char *fwd = cJSON_PrintUnformatted(json);
    cJSON_Delete(json);
    if (!fwd) { ESP_LOGE(TAG, "relay: JSON print fail"); return; }

    ESP_LOGI(TAG, "relay: %s -> %s", src_ip, fwd);
    coap_relay_send(instance, fwd);
    free(fwd);
}

static void coap_send(otInstance *instance, const sensor_data_t *d)
{
    char buf[160];
    snprintf(buf, sizeof(buf),
             "{\"node_id\":\"Router_H2\",\"temp\":%.1f,\"hum\":%.1f,\"co2\":%u,\"tvoc\":%u}",
             d->temperature, d->humidity, d->co2, d->tvoc);

    esp_openthread_lock_acquire(portMAX_DELAY);

    otMessage *msg = otCoapNewMessage(instance, NULL);
    if (!msg) {
        ESP_LOGE(TAG, "CoAP alloc fail");
        esp_openthread_lock_release();
        return;
    }

    otCoapMessageInit(msg, OT_COAP_TYPE_NON_CONFIRMABLE, OT_COAP_CODE_POST);
    otCoapMessageGenerateToken(msg, 4);

    otError err = otCoapMessageAppendUriPathOptions(msg, COAP_URI_PATH);
    if (err != OT_ERROR_NONE) goto fail;

    // Content-Format: application/json = 50
    err = otCoapMessageAppendContentFormatOption(msg, 50);
    if (err != OT_ERROR_NONE) goto fail;

    err = otCoapMessageSetPayloadMarker(msg);
    if (err != OT_ERROR_NONE) goto fail;

    err = otMessageAppend(msg, buf, (uint16_t)strlen(buf));
    if (err != OT_ERROR_NONE) goto fail;

    {
        otMessageInfo info;
        memset(&info, 0, sizeof(info));
        otIp6AddressFromString(COAP_PEER_ADDR, &info.mPeerAddr);
        info.mPeerPort = COAP_PEER_PORT;

        err = otCoapSendRequest(instance, msg, &info, NULL, NULL);
        if (err == OT_ERROR_NONE) {
            ESP_LOGI(TAG, "CoAP POST -> %s : %s", COAP_URI_PATH, buf);
            esp_openthread_lock_release();
            return;
        }
    }

fail:
    ESP_LOGE(TAG, "CoAP send fail: %d", err);
    otMessageFree(msg);
    esp_openthread_lock_release();
}

// ─── Tasks ───────────────────────────────────────────────────────────────────

static void sensor_task(void *arg)
{
    otInstance *instance = (otInstance *)arg;
    sensor_data_t data;

    // Chờ Thread attach vào mạng
    vTaskDelay(pdMS_TO_TICKS(8000));

    while (1) {
        // Đọc SHT31 (nhiệt độ + độ ẩm)
        if (sht3x_measure(&s_sht3x, &data.temperature, &data.humidity) == ESP_OK) {
            ESP_LOGI(TAG, "SHT31  temp=%.1f°C  hum=%.1f%%",
                     data.temperature, data.humidity);
        } else {
            ESP_LOGW(TAG, "SHT31 read error");
            data.temperature = 25.0f;
            data.humidity    = 50.0f;
        }

        // Chỉ đọc CCS811 khi nhiệt độ > 24°C
        if (data.temperature > 24.0f) {
            // WAK giữ LOW thường trực - sensor đo liên tục MODE_1S
            // Chỉ retry khi DATA_READY chưa sẵn sàng; lỗi hardware (MAX_RESIST...) thì fail fast
            esp_err_t ccs_err = ESP_FAIL;
            for (int retry = 0; retry < 3; retry++) {
                ccs_err = ccs811_get_results(&s_ccs811, &data.tvoc, &data.co2, NULL, NULL);
                if (ccs_err != CCS811_ERR_NO_NEW_DATA) break;
                vTaskDelay(pdMS_TO_TICKS(1200));
            }
            if (ccs_err == ESP_OK && data.co2 > 0) {
                ESP_LOGI(TAG, "CCS811 co2=%uppm  tvoc=%uppb", data.co2, data.tvoc);
                ccs811_set_environmental_data(&s_ccs811, data.temperature, data.humidity);
            } else {
                ESP_LOGW(TAG, "CCS811 read error (err=0x%x)", ccs_err);
                if (ccs_err == CCS811_ERR_MAX_RESIST) {
                    ESP_LOGW(TAG, "CCS811 MAX_RESISTANCE — reinitializing sensor");
                    ccs811_init(&s_ccs811);
                }
                data.co2  = 0;
                data.tvoc = 0;
            }
        } else {
            ESP_LOGI(TAG, "Temp<=24°C, CCS811 skipped");
            data.co2  = 0;
            data.tvoc = 0;
        }

        // Gửi CoAP khi đã là Child/Router/Leader
        esp_openthread_lock_acquire(portMAX_DELAY);
        otDeviceRole role = otThreadGetDeviceRole(instance);
        esp_openthread_lock_release();

        if (role >= OT_DEVICE_ROLE_CHILD) {
            coap_send(instance, &data);
        } else {
            ESP_LOGW(TAG, "Not attached (role=%d), skip send", role);
        }

        vTaskDelay(pdMS_TO_TICKS(SENSOR_INTERVAL_MS));
    }
}

static void monitor_task(void *arg)
{
    otInstance *instance = (otInstance *)arg;
    const char *role_str[] = {"Disabled", "Detached", "Child", "Router", "Leader"};
    while (1) {
        esp_openthread_lock_acquire(portMAX_DELAY);
        otDeviceRole role = otThreadGetDeviceRole(instance);
        esp_openthread_lock_release();
        ESP_LOGI(TAG, "Thread role: %s", role_str[role]);
        vTaskDelay(pdMS_TO_TICKS(5000));
    }
}

// ─── app_main ─────────────────────────────────────────────────────────────────

void app_main(void)
{
    esp_vfs_eventfd_config_t eventfd_config = {.max_fds = 3};
    ESP_ERROR_CHECK(nvs_flash_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_vfs_eventfd_register(&eventfd_config));

    // ── Khởi tạo I2C và cảm biến ──────────────────────────────────────────
    ESP_ERROR_CHECK(i2cdev_init());

    ESP_ERROR_CHECK(sht3x_init_desc(&s_sht3x, SHT3X_I2C_ADDR_GND,
                                    I2C_PORT, I2C_SDA_PIN, I2C_SCL_PIN));
    s_sht3x.i2c_dev.cfg.master.clk_speed = 100000;
    ESP_ERROR_CHECK(sht3x_init(&s_sht3x));
    ESP_LOGI(TAG, "SHT31 OK");

    // Cấu hình WAK và RST pin cho CCS811
    gpio_config_t ccs_gpio_cfg = {
        .pin_bit_mask = (1ULL << CCS811_WAK_PIN) | (1ULL << CCS811_RST_PIN),
        .mode         = GPIO_MODE_OUTPUT,
        .pull_up_en   = GPIO_PULLUP_DISABLE,
        .pull_down_en = GPIO_PULLDOWN_DISABLE,
        .intr_type    = GPIO_INTR_DISABLE,
    };
    gpio_config(&ccs_gpio_cfg);
    gpio_set_level(CCS811_RST_PIN, 1);  // HIGH = không reset
    gpio_set_level(CCS811_WAK_PIN, 0);  // LOW = thức
    // Chờ tSTART sau power-on (18–20ms theo datasheet)
    vTaskDelay(pdMS_TO_TICKS(20));

    ESP_ERROR_CHECK(ccs811_init_desc(&s_ccs811, CCS811_I2C_ADDRESS_1,
                                     I2C_PORT, I2C_SDA_PIN, I2C_SCL_PIN));
    s_ccs811.i2c_dev.cfg.master.clk_speed = 100000;
    ESP_ERROR_CHECK(ccs811_init(&s_ccs811));
    // Giữ MODE_1S (mặc định từ ccs811_init) để DATA_READY luôn có trong vòng 1s
    // Không dùng WAK sleep: timer sensor và timer task sẽ trôi pha → DATA_READY=0
    ESP_LOGI(TAG, "CCS811 OK (mode 1s, WAK=GPIO12 LOW-always, RST=GPIO10 HIGH-always)");

    // ── Khởi tạo OpenThread ────────────────────────────────────────────────
    esp_openthread_platform_config_t ot_cfg = {
        .radio_config = {
            .radio_mode = RADIO_MODE_NATIVE,
        },
        .host_config = {
            .host_connection_mode = HOST_CONNECTION_MODE_NONE,
        },
        .port_config = {
            .storage_partition_name = "nvs",
            .netif_queue_size = 10,
            .task_queue_size  = 20,
        },
    };

    ESP_ERROR_CHECK(esp_openthread_init(&ot_cfg));
    otInstance *instance = esp_openthread_get_instance();

    esp_openthread_lock_acquire(portMAX_DELAY);

    // Dataset mạng Thread
    otOperationalDataset dataset;
    memset(&dataset, 0, sizeof(dataset));

    dataset.mActiveTimestamp.mSeconds             = 1;
    dataset.mComponents.mIsActiveTimestampPresent = true;

    dataset.mPanId                      = 0x1234;
    dataset.mComponents.mIsPanIdPresent = true;

    dataset.mChannel                      = 15;
    dataset.mComponents.mIsChannelPresent = true;

    const char *net_name = "MyFireAlarm";
    memcpy(dataset.mNetworkName.m8, net_name, strlen(net_name));
    dataset.mComponents.mIsNetworkNamePresent = true;

    uint8_t key[16] = {0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
                       0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff};
    memcpy(dataset.mNetworkKey.m8, key, 16);
    dataset.mComponents.mIsNetworkKeyPresent = true;

    otDatasetSetActive(instance, &dataset);
    otIp6SetEnabled(instance, true);
    otThreadSetEnabled(instance, true);

    // Khởi CoAP - vừa là client (gửi sensor của router) vừa là server (nhận và relay từ SED)
    otCoapStart(instance, OT_DEFAULT_COAP_PORT);

    memset(&s_relay_res, 0, sizeof(s_relay_res));
    s_relay_res.mUriPath = "sensor";
    s_relay_res.mHandler = coap_relay_handler;
    s_relay_res.mContext = instance;
    otCoapAddResource(instance, &s_relay_res);

    esp_openthread_lock_release();

    ESP_LOGI(TAG, "Router init done, joining Thread network...");

    xTaskCreate(monitor_task, "ot_monitor", 2048, instance, 5, NULL);
    xTaskCreate(sensor_task,  "ot_sensor",  4096, instance, 5, NULL);

    esp_openthread_launch_mainloop();
}
