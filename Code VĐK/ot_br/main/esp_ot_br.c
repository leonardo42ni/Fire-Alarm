#include <stdio.h>
#include <string.h>
#include <time.h>
#include "nvs_flash.h"
#include "esp_netif.h"
#include "esp_event.h"
#include "esp_log.h"
#include "esp_wifi.h"
#include "esp_http_server.h"
#include "esp_openthread.h"
#include "esp_openthread_types.h"
#include "esp_openthread_lock.h"
#include "esp_openthread_netif_glue.h"
#include "esp_openthread_border_router.h"
#include "esp_vfs_eventfd.h"
#include "openthread/instance.h"
#include "openthread/thread.h"
#include "openthread/dataset.h"
#include "openthread/ip6.h"
#include "openthread/coap.h"
#include "driver/uart.h"
#include "driver/gpio.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "cJSON.h"
#include "mqtt_client.h"
#include "esp_crt_bundle.h"
#include "esp_sntp.h"

#define TAG "OT_BR"

// ---- WiFi credentials ----
#define WIFI_SSID "casablanca"
#define WIFI_PASS "abcxyz123"

// ---- UART RCP pins (WROOM32 TX → H2 RX, WROOM32 RX ← H2 TX) ----
// Thay đổi 2 giá trị này cho khớp với dây nối thực tế
#define RCP_UART_RX_PIN  GPIO_NUM_4   // WROOM32 GPIO4  ← H2 TX
#define RCP_UART_TX_PIN  GPIO_NUM_5   // WROOM32 GPIO5  → H2 RX
#define RCP_UART_BAUD    460800       // Phải đúng với firmware H2 RCP

// ---- Thread network config ----
#define THREAD_CHANNEL      15
#define THREAD_PAN_ID       0x1234
#define THREAD_NETWORK_NAME "MyFireAlarm"
static const uint8_t THREAD_KEY[16] = {
    0x00,0x11,0x22,0x33,0x44,0x55,0x66,0x77,
    0x88,0x99,0xaa,0xbb,0xcc,0xdd,0xee,0xff
};

// ---- MQTT config (HiveMQ Cloud) ----
#define MQTT_BROKER_URI  "mqtts://204e1cd42856497598802c289c184353.s1.eu.hivemq.cloud:8883"
#define MQTT_USERNAME    "hivemq.webclient.1778922990654"
#define MQTT_PASSWORD    "72*hjEd91GyHsWSi?,P&"
#define MQTT_TOPIC_BASE  "sensor"                 // topic: sensor/<node_id>

// ---- Sensor node data ----
typedef struct {
    char node_id[32];
    float temperature;
    float humidity;
    uint16_t co2;
    uint16_t tvoc;
    char last_seen[32];
    int active;
} sensor_node_t;

#define MAX_NODES 5
static sensor_node_t s_nodes[MAX_NODES];
static int s_node_count = 0;
static SemaphoreHandle_t s_node_mutex = NULL;

static httpd_handle_t s_web_server = NULL;

// WiFi netif handle — cần truyền cho OpenThread làm backbone
static esp_netif_t *s_wifi_netif = NULL;

// ---- MQTT client ----
static esp_mqtt_client_handle_t s_mqtt_client = NULL;
static volatile bool s_mqtt_connected = false;

// ---- Time sync flag ----
static volatile bool s_time_synced = false;

// ---- Helpers ----

static void get_time_str(char *buf, size_t len)
{
    if (!s_time_synced) {
        strncpy(buf, "Chua dong bo gio", len - 1);
        buf[len - 1] = '\0';
        return;
    }
    time_t now;
    struct tm ti;
    time(&now);
    localtime_r(&now, &ti);
    strftime(buf, len, "%H:%M:%S %d/%m/%Y", &ti);
}

static sensor_node_t *find_or_add_node(const char *id)
{
    for (int i = 0; i < s_node_count; i++) {
        if (strcmp(s_nodes[i].node_id, id) == 0) return &s_nodes[i];
    }
    if (s_node_count < MAX_NODES) {
        strncpy(s_nodes[s_node_count].node_id, id,
                sizeof(s_nodes[0].node_id) - 1);
        s_nodes[s_node_count].active = 1;
        return &s_nodes[s_node_count++];
    }
    return NULL;
}

// ---- MQTT ----

static void mqtt_event_handler(void *arg, esp_event_base_t base,
                               int32_t event_id, void *event_data)
{
    switch ((esp_mqtt_event_id_t)event_id) {
        case MQTT_EVENT_CONNECTED:
            s_mqtt_connected = true;
            ESP_LOGI(TAG, "MQTT connected to broker");
            break;
        case MQTT_EVENT_DISCONNECTED:
            s_mqtt_connected = false;
            ESP_LOGW(TAG, "MQTT disconnected from broker");
            break;
        case MQTT_EVENT_ERROR:
            ESP_LOGE(TAG, "MQTT error event");
            break;
        default:
            break;
    }
}

static void mqtt_init(void)
{
    esp_mqtt_client_config_t cfg = {
        .broker = {
            .address.uri = MQTT_BROKER_URI,
            .verification.crt_bundle_attach = esp_crt_bundle_attach,
        },
        .credentials = {
            .username = MQTT_USERNAME,
            .authentication.password = MQTT_PASSWORD,
        },
    };
    s_mqtt_client = esp_mqtt_client_init(&cfg);
    esp_mqtt_client_register_event(s_mqtt_client, ESP_EVENT_ANY_ID,
                                   mqtt_event_handler, NULL);
    esp_mqtt_client_start(s_mqtt_client);
    ESP_LOGI(TAG, "MQTT client started, broker: %s", MQTT_BROKER_URI);
}

// ---- CoAP + node update ----

static void update_node_data(const char *payload, const char *src_ip)
{
    cJSON *json = cJSON_Parse(payload);
    if (!json) { ESP_LOGE(TAG, "JSON parse failed"); return; }

    char node_id[32];
    cJSON *nid = cJSON_GetObjectItem(json, "node_id");
    if (nid && cJSON_IsString(nid) && nid->valuestring[0]) {
        strncpy(node_id, nid->valuestring, sizeof(node_id) - 1);
        node_id[sizeof(node_id) - 1] = '\0';
    } else {
        size_t ip_len = strlen(src_ip);
        const char *suffix = ip_len > 8 ? src_ip + ip_len - 8 : src_ip;
        snprintf(node_id, sizeof(node_id), "Node_%s", suffix);
    }

    char *mqtt_payload = NULL;

    xSemaphoreTake(s_node_mutex, portMAX_DELAY);
    sensor_node_t *node = find_or_add_node(node_id);
    if (node) {
        cJSON *t = cJSON_GetObjectItem(json, "temp");
        cJSON *h = cJSON_GetObjectItem(json, "hum");
        cJSON *c = cJSON_GetObjectItem(json, "co2");
        cJSON *v = cJSON_GetObjectItem(json, "tvoc");
        if (t) node->temperature = (float)t->valuedouble;
        if (h) node->humidity    = (float)h->valuedouble;
        if (c) node->co2         = (uint16_t)c->valueint;
        if (v) node->tvoc        = (uint16_t)v->valueint;
        get_time_str(node->last_seen, sizeof(node->last_seen));
        node->active = 1;
        ESP_LOGI(TAG, "Node %s: %.1f°C %.1f%% CO2=%d TVOC=%d",
                 node->node_id, node->temperature, node->humidity,
                 node->co2, node->tvoc);

        // Build MQTT payload inside the mutex so all fields are consistent
        cJSON *pub = cJSON_CreateObject();
        cJSON_AddStringToObject(pub, "node_id",     node->node_id);
        cJSON_AddNumberToObject(pub, "temperature", node->temperature);
        cJSON_AddNumberToObject(pub, "humidity",    node->humidity);
        cJSON_AddNumberToObject(pub, "co2",         node->co2);
        cJSON_AddNumberToObject(pub, "tvoc",        node->tvoc);
        cJSON_AddStringToObject(pub, "last_seen",   node->last_seen);
        mqtt_payload = cJSON_PrintUnformatted(pub);
        cJSON_Delete(pub);
    }
    xSemaphoreGive(s_node_mutex);

    // Publish after releasing the mutex to avoid holding it during network I/O
    if (mqtt_payload) {
        if (s_mqtt_connected) {
            char topic[72];
            snprintf(topic, sizeof(topic), "%s/%s", MQTT_TOPIC_BASE, node_id);
            esp_mqtt_client_publish(s_mqtt_client, topic, mqtt_payload, 0, 1, 0);
            ESP_LOGI(TAG, "MQTT -> %s : %s", topic, mqtt_payload);
        } else {
            ESP_LOGW(TAG, "MQTT not connected, skip publish for %s", node_id);
        }
        free(mqtt_payload);
    }

    cJSON_Delete(json);
}

// ---- Web server ----

static esp_err_t root_get_handler(httpd_req_t *req)
{
    httpd_resp_set_type(req, "text/html; charset=UTF-8");

    httpd_resp_sendstr_chunk(req,
        "<!DOCTYPE html><html><head>"
        "<meta charset='UTF-8'>"
        "<meta http-equiv='refresh' content='5'>"
        "<title>Thread Sensor Network</title>"
        "<style>"
        "body{font-family:Arial;margin:20px;background:#f0f0f0}"
        "h1{color:#333;text-align:center}"
        ".container{display:flex;flex-wrap:wrap;gap:20px;justify-content:center}"
        ".card{background:#fff;border-radius:10px;box-shadow:0 4px 8px rgba(0,0,0,.1);padding:20px;width:300px}"
        ".card h2{margin-top:0;color:#2196F3}"
        ".sv{font-size:24px;font-weight:bold;margin:10px 0}"
        ".temp{color:#FF5722}.hum{color:#2196F3}.co2{color:#4CAF50}.tvoc{color:#9C27B0}"
        ".ts{font-size:12px;color:#999;margin-top:15px}"
        ".offline{color:#f44336;font-weight:bold}"
        "</style></head><body>"
        "<h1>Thread Sensor Network</h1>"
        "<div class='container'>");

    char card[400];
    xSemaphoreTake(s_node_mutex, portMAX_DELAY);
    for (int i = 0; i < s_node_count; i++) {
        if (s_nodes[i].active) {
            snprintf(card, sizeof(card),
                "<div class='card'><h2>Node: %s</h2>"
                "<div class='sv temp'>%.1f C</div>"
                "<div class='sv hum'>%.1f %%</div>"
                "<div class='sv co2'>CO2: %d ppm</div>"
                "<div class='sv tvoc'>TVOC: %d ppb</div>"
                "<div class='ts'>%s</div></div>",
                s_nodes[i].node_id,
                s_nodes[i].temperature, s_nodes[i].humidity,
                s_nodes[i].co2, s_nodes[i].tvoc,
                s_nodes[i].last_seen);
        } else {
            snprintf(card, sizeof(card),
                "<div class='card'><h2>Node: %s</h2>"
                "<div class='offline'>Node offline</div>"
                "<div class='ts'>%s</div></div>",
                s_nodes[i].node_id, s_nodes[i].last_seen);
        }
        httpd_resp_sendstr_chunk(req, card);
    }
    xSemaphoreGive(s_node_mutex);

    httpd_resp_sendstr_chunk(req, "</div></body></html>");
    httpd_resp_sendstr_chunk(req, NULL); // end chunked
    return ESP_OK;
}

static esp_err_t json_get_handler(httpd_req_t *req)
{
    cJSON *arr = cJSON_CreateArray();
    xSemaphoreTake(s_node_mutex, portMAX_DELAY);
    for (int i = 0; i < s_node_count; i++) {
        if (!s_nodes[i].active) continue;
        cJSON *obj = cJSON_CreateObject();
        cJSON_AddStringToObject(obj, "node_id",     s_nodes[i].node_id);
        cJSON_AddNumberToObject(obj, "temperature", s_nodes[i].temperature);
        cJSON_AddNumberToObject(obj, "humidity",    s_nodes[i].humidity);
        cJSON_AddNumberToObject(obj, "co2",         s_nodes[i].co2);
        cJSON_AddNumberToObject(obj, "tvoc",        s_nodes[i].tvoc);
        cJSON_AddStringToObject(obj, "last_seen",   s_nodes[i].last_seen);
        cJSON_AddItemToArray(arr, obj);
    }
    xSemaphoreGive(s_node_mutex);

    char *s = cJSON_PrintUnformatted(arr);
    cJSON_Delete(arr);
    httpd_resp_set_type(req, "application/json");
    httpd_resp_sendstr(req, s ? s : "[]");
    free(s);
    return ESP_OK;
}

static void start_web_server(void)
{
    httpd_config_t cfg = HTTPD_DEFAULT_CONFIG();
    cfg.lru_purge_enable = true;

    if (httpd_start(&s_web_server, &cfg) != ESP_OK) {
        ESP_LOGE(TAG, "Web server start failed");
        return;
    }
    httpd_uri_t root = { .uri="/",     .method=HTTP_GET, .handler=root_get_handler };
    httpd_uri_t api  = { .uri="/data", .method=HTTP_GET, .handler=json_get_handler };
    httpd_register_uri_handler(s_web_server, &root);
    httpd_register_uri_handler(s_web_server, &api);
    ESP_LOGI(TAG, "Web server started");
}

// ---- SNTP ----

static bool s_sntp_started = false;

static void sntp_sync_cb(struct timeval *tv)
{
    s_time_synced = true;
    ESP_LOGI(TAG, "Time synced (epoch=%lld)", (long long)tv->tv_sec);
}

static void sntp_start(void)
{
    if (s_sntp_started) return;
    s_sntp_started = true;
    // Múi giờ Việt Nam UTC+7
    setenv("TZ", "ICT-7", 1);
    tzset();
    esp_sntp_setoperatingmode(SNTP_OPMODE_POLL);
    esp_sntp_setservername(0, "pool.ntp.org");
    sntp_set_time_sync_notification_cb(sntp_sync_cb);
    esp_sntp_init();
    ESP_LOGI(TAG, "SNTP started, waiting for time sync...");
}

// ---- WiFi ----

static void wifi_event_handler(void *arg, esp_event_base_t base,
                               int32_t id, void *data)
{
    if (base == WIFI_EVENT && id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
    } else if (base == IP_EVENT && id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *e = (ip_event_got_ip_t *)data;
        ESP_LOGI(TAG, "WiFi IP: " IPSTR, IP2STR(&e->ip_info.ip));
        esp_netif_create_ip6_linklocal(s_wifi_netif);
        sntp_start();
        if (!s_web_server) start_web_server();
        if (!s_mqtt_client) mqtt_init();
    } else if (base == IP_EVENT && id == IP_EVENT_GOT_IP6) {
        ip_event_got_ip6_t *e6 = (ip_event_got_ip6_t *)data;
        ESP_LOGI(TAG, "WiFi IPv6: " IPV6STR, IPV62STR(e6->ip6_info.ip));
    } else if (base == WIFI_EVENT && id == WIFI_EVENT_STA_DISCONNECTED) {
        esp_wifi_connect();
        ESP_LOGW(TAG, "WiFi disconnected, reconnecting...");
    }
}

static void wifi_init(void)
{
    // Lưu lại netif để dùng làm backbone cho OpenThread
    s_wifi_netif = esp_netif_create_default_wifi_sta();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));
    ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID,
                                               wifi_event_handler, NULL));
    ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_STA_GOT_IP,
                                               wifi_event_handler, NULL));
    ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_GOT_IP6,
                                               wifi_event_handler, NULL));
    wifi_config_t wcfg = {
        .sta = {
            .ssid = WIFI_SSID,
            .password = WIFI_PASS,
            .threshold.authmode = WIFI_AUTH_WPA2_PSK,
        },
    };
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wcfg));
    ESP_ERROR_CHECK(esp_wifi_start());
}

// ---- CoAP ----

static void coap_recv_handler(void *ctx, otMessage *msg,
                              const otMessageInfo *info)
{
    otInstance *inst = (otInstance *)ctx;
    uint8_t buf[256];
    char ip[48];

    uint16_t len = otMessageRead(msg, otMessageGetOffset(msg),
                                  buf, sizeof(buf) - 1);
    buf[len] = '\0';
    otIp6AddressToString(&info->mPeerAddr, ip, sizeof(ip));
    ESP_LOGI(TAG, "CoAP from %s: %s", ip, buf);

    update_node_data((char *)buf, ip);

    if (otCoapMessageGetType(msg) == OT_COAP_TYPE_CONFIRMABLE) {
        otMessage *resp = otCoapNewMessage(inst, NULL);
        if (resp) {
            otCoapMessageInitResponse(resp, msg,
                OT_COAP_TYPE_ACKNOWLEDGMENT, OT_COAP_CODE_CHANGED);
            if (otCoapSendResponse(inst, resp, info) != OT_ERROR_NONE) {
                otMessageFree(resp);
                ESP_LOGE(TAG, "CoAP ACK send failed");
            }
        }
    }
}

// ---- OpenThread task ----

static void ot_task(void *pv)
{
    esp_openthread_platform_config_t config = {
        .radio_config = {
            .radio_mode = RADIO_MODE_UART_RCP,
            .radio_uart_config = {
                .port = UART_NUM_1,
                .uart_config = {
                    .baud_rate        = RCP_UART_BAUD,
                    .data_bits        = UART_DATA_8_BITS,
                    .parity           = UART_PARITY_DISABLE,
                    .stop_bits        = UART_STOP_BITS_1,
                    // Không dùng hardware flow control (không có dây RTS/CTS)
                    .flow_ctrl        = UART_HW_FLOWCTRL_DISABLE,
                    .rx_flow_ctrl_thresh = 0,
                    .source_clk       = UART_SCLK_DEFAULT,
                },
                .rx_pin = RCP_UART_RX_PIN,
                .tx_pin = RCP_UART_TX_PIN,
            },
        },
        .host_config = {
            .host_connection_mode = HOST_CONNECTION_MODE_NONE,
        },
        .port_config = {
            .storage_partition_name = "nvs",
            .netif_queue_size = 10,
            .task_queue_size  = 10,
        },
    };

    ESP_ERROR_CHECK(esp_openthread_init(&config));

    esp_openthread_set_backbone_netif(s_wifi_netif);

    otInstance *inst = esp_openthread_get_instance();

    esp_openthread_lock_acquire(portMAX_DELAY);

    // Dataset
    otOperationalDataset ds;
    memset(&ds, 0, sizeof(ds));

    ds.mActiveTimestamp.mSeconds             = 1;
    ds.mComponents.mIsActiveTimestampPresent = true;

    ds.mPanId                                = THREAD_PAN_ID;
    ds.mComponents.mIsPanIdPresent           = true;

    ds.mChannel                              = THREAD_CHANNEL;
    ds.mComponents.mIsChannelPresent         = true;

    memcpy(ds.mNetworkName.m8, THREAD_NETWORK_NAME, strlen(THREAD_NETWORK_NAME));
    ds.mComponents.mIsNetworkNamePresent     = true;

    memcpy(ds.mNetworkKey.m8, THREAD_KEY, 16);
    ds.mComponents.mIsNetworkKeyPresent      = true;

    otDatasetSetActive(inst, &ds);
    otIp6SetEnabled(inst, true);
    otThreadSetEnabled(inst, true);

    // CoAP
    otCoapStart(inst, OT_DEFAULT_COAP_PORT);

    static otCoapResource res;
    memset(&res, 0, sizeof(res));
    res.mUriPath = "sensor";
    res.mHandler = coap_recv_handler;
    res.mContext = inst;
    otCoapAddResource(inst, &res);

    ESP_LOGI(TAG, "Border Router ready on coap://[addr]/sensor");

    // border_router_init() tự release lock bên trong — không gọi lock_release() thủ công
    ESP_ERROR_CHECK(esp_openthread_border_router_init());
    esp_openthread_launch_mainloop();

    vTaskDelete(NULL);
}

// ---- app_main ----

void app_main(void)
{
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES ||
        ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ESP_ERROR_CHECK(nvs_flash_init());
    }

    esp_vfs_eventfd_config_t evcfg = { .max_fds = 5 };
    ESP_ERROR_CHECK(esp_vfs_eventfd_register(&evcfg));

    ESP_ERROR_CHECK(esp_event_loop_create_default());
    ESP_ERROR_CHECK(esp_netif_init());

    s_node_mutex = xSemaphoreCreateMutex();
    memset(s_nodes, 0, sizeof(s_nodes));

    wifi_init();

    // Stack 20480 — border router cần nhiều hơn task thường
    xTaskCreate(ot_task, "ot_task", 20480, NULL, 5, NULL);
}
