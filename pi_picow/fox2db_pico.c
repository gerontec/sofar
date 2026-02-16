/*
 * fox2db_pico.c - E-Box Battery Reader & MQTT Publisher for Raspberry Pi Pico W
 *
 * Reads E-Box battery data via RS232 (UART) every 60s and publishes to MQTT
 * Provides battery SOC, voltage, and current for soyo1min_pico
 *
 * Hardware Requirements:
 * - Raspberry Pi Pico W
 * - RS232 to TTL converter module connected to UART0 (GPIO 0/1)
 * - E-Box battery system with serial interface
 * - WiFi network
 * - MQTT broker at 192.168.178.218
 *
 * Build:
 *   mkdir build && cd build
 *   cmake ..
 *   make fox2db_pico
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include "pico/stdlib.h"
#include "pico/cyw43_arch.h"
#include "hardware/uart.h"
#include "hardware/gpio.h"
#include "lwip/apps/mqtt.h"
#include "lwip/ip_addr.h"

#define VERSION "1.0.0-pico-ebox"

// ============================================================================
// Configuration
// ============================================================================

// WiFi Configuration
#define WIFI_SSID "YOUR_WIFI_SSID"
#define WIFI_PASSWORD "YOUR_WIFI_PASSWORD"

// MQTT Configuration
#define MQTT_BROKER "192.168.178.218"
#define MQTT_PORT 1883
#define MQTT_CLIENT_ID "pico_fox2db_ebox"

// MQTT Topics to Publish (for soyo1min_pico)
#define MQTT_TOPIC_SOC_BAT2 "sofar/soc_bat2"        // Battery SOC (%)
#define MQTT_TOPIC_BAT2_CURRENT "sofar/bat2_current" // Battery current (A)
#define MQTT_TOPIC_BAT_VOLTAGE "ebox/voltage"       // Battery voltage (V)
#define MQTT_TOPIC_STATUS "fox2db/status"           // Status messages

// E-Box UART Configuration
#define UART_ID uart0
#define UART_TX_PIN 0       // GPIO 0
#define UART_RX_PIN 1       // GPIO 1
#define UART_BAUD_RATE 115200

// E-Box Protocol
#define EBOX_COMMAND "bat"
#define EBOX_MAX_LINES 50
#define EBOX_MAX_LINE_LEN 256
#define EBOX_TIMEOUT_MS 4000

// Update interval
#define READ_INTERVAL_MS 60000  // 60 seconds

// ============================================================================
// Data Structures
// ============================================================================

typedef struct {
    char lines[EBOX_MAX_LINES][EBOX_MAX_LINE_LEN];
    int count;
} LineBuffer;

typedef struct {
    int battery_num;
    float voltage_v;        // Voltage in Volts
    float current_a;        // Current in Amperes
    float soc_percent;      // State of Charge in %
    bool valid;
} BatteryData;

// ============================================================================
// Global Variables
// ============================================================================

static mqtt_client_t *mqtt_client = NULL;
static bool mqtt_connected = false;
static bool wifi_connected = false;

// ============================================================================
// Function Declarations
// ============================================================================

static void wifi_init(void);
static void mqtt_init(void);
static void uart_init_rs232(void);
static bool uart_read_line(char *buffer, int max_len, uint32_t timeout_ms);
static void read_ebox_data(LineBuffer *buffer);
static bool parse_battery_data(const LineBuffer *buffer, BatteryData *data);
static void publish_mqtt(const BatteryData *data);
static bool is_numeric(const char *str);

// ============================================================================
// WiFi Initialization
// ============================================================================

static void wifi_init(void) {
    printf("WiFi: Connecting to %s...\n", WIFI_SSID);

    if (cyw43_arch_init()) {
        printf("WiFi: Failed to initialize\n");
        return;
    }

    cyw43_arch_enable_sta_mode();

    int retry_count = 0;
    while (retry_count < 10) {
        printf("WiFi: Connection attempt %d/10\n", retry_count + 1);

        if (cyw43_arch_wifi_connect_timeout_ms(WIFI_SSID, WIFI_PASSWORD,
                                                CYW43_AUTH_WPA2_AES_PSK, 30000) == 0) {
            printf("WiFi: Connected!\n");
            wifi_connected = true;
            return;
        }

        printf("WiFi: Failed, retrying...\n");
        retry_count++;
        sleep_ms(1000);
    }

    printf("WiFi: Failed to connect\n");
}

// ============================================================================
// MQTT Initialization
// ============================================================================

static void mqtt_connection_cb(mqtt_client_t *client, void *arg, mqtt_connection_status_t status) {
    if (status == MQTT_CONNECT_ACCEPTED) {
        printf("MQTT: Connected to broker\n");
        mqtt_connected = true;

        // Publish online status
        const char *msg = "Fox2DB EBox reader online";
        mqtt_publish(client, MQTT_TOPIC_STATUS, msg, strlen(msg), 0, 0, NULL, NULL);
    } else {
        printf("MQTT: Connection failed, status: %d\n", status);
        mqtt_connected = false;
    }
}

static void mqtt_init(void) {
    if (!wifi_connected) {
        printf("MQTT: WiFi not connected\n");
        return;
    }

    printf("MQTT: Initializing...\n");

    mqtt_client = mqtt_client_new();
    if (!mqtt_client) {
        printf("MQTT: Failed to create client\n");
        return;
    }

    ip_addr_t broker_ip;
    if (!ip4addr_aton(MQTT_BROKER, &broker_ip)) {
        printf("MQTT: Invalid broker IP\n");
        return;
    }

    struct mqtt_connect_client_info_t ci = {0};
    ci.client_id = MQTT_CLIENT_ID;

    printf("MQTT: Connecting to %s:%d\n", MQTT_BROKER, MQTT_PORT);

    err_t err = mqtt_client_connect(mqtt_client, &broker_ip, MQTT_PORT,
                                    mqtt_connection_cb, NULL, &ci);

    if (err != ERR_OK) {
        printf("MQTT: Connect failed, error: %d\n", err);
    }
}

// ============================================================================
// UART (RS232) Functions
// ============================================================================

static void uart_init_rs232(void) {
    printf("UART: Initializing on GPIO %d (TX) and %d (RX)\n", UART_TX_PIN, UART_RX_PIN);

    uart_init(UART_ID, UART_BAUD_RATE);

    gpio_set_function(UART_TX_PIN, GPIO_FUNC_UART);
    gpio_set_function(UART_RX_PIN, GPIO_FUNC_UART);

    uart_set_format(UART_ID, 8, 1, UART_PARITY_NONE);
    uart_set_fifo_enabled(UART_ID, true);

    printf("UART: Initialized at %d baud\n", UART_BAUD_RATE);
}

static bool uart_read_line(char *buffer, int max_len, uint32_t timeout_ms) {
    uint32_t start_time = to_ms_since_boot(get_absolute_time());
    int pos = 0;

    while (pos < max_len - 1) {
        uint32_t now = to_ms_since_boot(get_absolute_time());
        if (now - start_time > timeout_ms) {
            buffer[pos] = '\0';
            return pos > 0;
        }

        if (uart_is_readable(UART_ID)) {
            char c = uart_getc(UART_ID);
            buffer[pos++] = c;

            // Check for end of line (CRLF)
            if (pos >= 2 && buffer[pos-2] == '\r' && buffer[pos-1] == '\n') {
                buffer[pos] = '\0';
                return true;
            }
        } else {
            sleep_ms(1);
        }
    }

    buffer[pos] = '\0';
    return true;
}

// ============================================================================
// E-Box Communication
// ============================================================================

static void read_ebox_data(LineBuffer *buffer) {
    buffer->count = 0;

    // Flush RX buffer
    while (uart_is_readable(UART_ID)) {
        uart_getc(UART_ID);
    }

    // Send command
    char cmd[64];
    snprintf(cmd, sizeof(cmd), "%s\n", EBOX_COMMAND);
    printf("E-Box: Sending command: %s", cmd);
    uart_puts(UART_ID, cmd);

    // Read response lines
    char end_marker[] = "\r$$\r\n";
    bool done = false;
    int retry = 0;
    const int max_retries = 3;

    while (!done && buffer->count < EBOX_MAX_LINES && retry < max_retries) {
        char line[EBOX_MAX_LINE_LEN];

        if (uart_read_line(line, sizeof(line), EBOX_TIMEOUT_MS)) {
            printf("E-Box: Received: %s", line);

            // Store line
            if (buffer->count < EBOX_MAX_LINES) {
                strncpy(buffer->lines[buffer->count], line, EBOX_MAX_LINE_LEN - 1);
                buffer->lines[buffer->count][EBOX_MAX_LINE_LEN - 1] = '\0';
                buffer->count++;
            }

            // Check for end marker
            if (strcmp(line, end_marker) == 0) {
                done = true;
            }
        } else {
            // Timeout - retry
            retry++;
            if (retry < max_retries) {
                printf("E-Box: Timeout, retrying (%d/%d)...\n", retry, max_retries);
                uart_puts(UART_ID, cmd);
                sleep_ms(100);
            }
        }
    }

    printf("E-Box: Received %d lines\n", buffer->count);
}

// ============================================================================
// Data Parsing
// ============================================================================

static bool is_numeric(const char *str) {
    if (!str || !*str) return false;
    char *endptr;
    strtod(str, &endptr);
    return *endptr == '\0' || *endptr == '\r' || *endptr == '\n' || *endptr == '%';
}

static bool parse_battery_data(const LineBuffer *buffer, BatteryData *data) {
    data->valid = false;
    float total_voltage = 0.0f;
    float total_current = 0.0f;
    float min_soc = 100.0f;
    int battery_count = 0;

    for (int i = 0; i < buffer->count; i++) {
        const char *line = buffer->lines[i];

        // Skip empty lines and markers
        if (strlen(line) == 0 || strstr(line, "$") || strstr(line, "#") || strstr(line, "Power")) {
            continue;
        }

        // Check if line starts with digit (battery line)
        if (line[0] < '1' || line[0] > '9') {
            continue;
        }

        // Tokenize line
        char copy[EBOX_MAX_LINE_LEN];
        strncpy(copy, line, sizeof(copy) - 1);
        copy[sizeof(copy) - 1] = '\0';

        char *fields[20];
        int field_count = 0;

        char *token = strtok(copy, " \t\r\n");
        while (token && field_count < 20) {
            fields[field_count++] = token;
            token = strtok(NULL, " \t\r\n");
        }

        // Need at least 3 fields for voltage, current
        if (field_count < 3) {
            continue;
        }

        // Validate numeric fields
        if (!is_numeric(fields[0]) || !is_numeric(fields[1]) || !is_numeric(fields[2])) {
            continue;
        }

        // Skip "Absent" batteries
        if (field_count > 8 && strcmp(fields[8], "Absent") == 0) {
            continue;
        }

        // Parse battery data
        float voltage_mv = atof(fields[1]);
        float current_ma = atof(fields[2]);

        // Find SOC field (contains %)
        float soc = 0.0f;
        for (int j = 3; j < field_count; j++) {
            if (strchr(fields[j], '%')) {
                // Remove % sign and parse
                char soc_str[32];
                strncpy(soc_str, fields[j], sizeof(soc_str) - 1);
                soc_str[sizeof(soc_str) - 1] = '\0';
                char *percent = strchr(soc_str, '%');
                if (percent) *percent = '\0';
                soc = atof(soc_str);
                break;
            }
        }

        total_voltage += voltage_mv;
        total_current += current_ma;
        if (soc < min_soc) {
            min_soc = soc;
        }
        battery_count++;

        printf("Battery %d: Voltage=%.1fmV, Current=%.1fmA, SOC=%.1f%%\n",
               battery_count, voltage_mv, current_ma, soc);
    }

    if (battery_count > 0) {
        data->battery_num = battery_count;
        data->voltage_v = total_voltage / 1000.0f;  // mV to V
        data->current_a = total_current / 1000.0f;  // mA to A
        data->soc_percent = min_soc;
        data->valid = true;

        printf("\nTotal: %d batteries, %.2fV, %.2fA, SOC=%.1f%%\n",
               battery_count, data->voltage_v, data->current_a, data->soc_percent);

        return true;
    }

    return false;
}

// ============================================================================
// MQTT Publishing
// ============================================================================

static void publish_mqtt(const BatteryData *data) {
    if (!mqtt_connected || !mqtt_client) {
        printf("MQTT: Not connected, skipping publish\n");
        return;
    }

    char payload[64];

    // Publish SOC (for soyo1min_pico as soc_bat2)
    snprintf(payload, sizeof(payload), "%.1f", data->soc_percent);
    mqtt_publish(mqtt_client, MQTT_TOPIC_SOC_BAT2, payload, strlen(payload),
                0, 0, NULL, NULL);
    printf("MQTT: Published SOC=%.1f%% to %s\n", data->soc_percent, MQTT_TOPIC_SOC_BAT2);

    // Publish Current in Amperes (for soyo1min_pico as bat2_current)
    snprintf(payload, sizeof(payload), "%.2f", data->current_a);
    mqtt_publish(mqtt_client, MQTT_TOPIC_BAT2_CURRENT, payload, strlen(payload),
                0, 0, NULL, NULL);
    printf("MQTT: Published Current=%.2fA to %s\n", data->current_a, MQTT_TOPIC_BAT2_CURRENT);

    // Publish Voltage in Volts
    snprintf(payload, sizeof(payload), "%.2f", data->voltage_v);
    mqtt_publish(mqtt_client, MQTT_TOPIC_BAT_VOLTAGE, payload, strlen(payload),
                0, 0, NULL, NULL);
    printf("MQTT: Published Voltage=%.2fV to %s\n", data->voltage_v, MQTT_TOPIC_BAT_VOLTAGE);
}

// ============================================================================
// Main Function
// ============================================================================

int main() {
    stdio_init_all();
    sleep_ms(2000);

    printf("\n");
    printf("===========================================\n");
    printf("  Fox2DB E-Box Reader for Pico W\n");
    printf("  Version: %s\n", VERSION);
    printf("===========================================\n\n");

    // Initialize hardware
    uart_init_rs232();
    wifi_init();

    if (wifi_connected) {
        mqtt_init();
    }

    printf("\nStarting 60s read loop...\n\n");

    // Main loop
    uint32_t last_read = 0;

    while (true) {
        uint32_t now = to_ms_since_boot(get_absolute_time());

        // LED blink based on status
        if (!wifi_connected) {
            cyw43_arch_gpio_put(CYW43_WL_GPIO_LED_PIN, (now / 250) % 2);
        } else if (!mqtt_connected) {
            cyw43_arch_gpio_put(CYW43_WL_GPIO_LED_PIN, (now / 100) % 2);
        } else {
            cyw43_arch_gpio_put(CYW43_WL_GPIO_LED_PIN, (now / 1000) % 2);
        }

        // Read E-Box every 60 seconds
        if (now - last_read >= READ_INTERVAL_MS) {
            printf("\n=== Reading E-Box Data ===\n");

            LineBuffer buffer;
            read_ebox_data(&buffer);

            BatteryData data;
            if (parse_battery_data(&buffer, &data)) {
                if (mqtt_connected) {
                    publish_mqtt(&data);
                }
            } else {
                printf("E-Box: Failed to parse battery data\n");
            }

            last_read = now;
            printf("\n=== Next read in 60s ===\n\n");
        }

        sleep_ms(100);
    }

    return 0;
}
