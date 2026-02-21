/*
 * ebox_pico.c - E-Box Battery Monitor for Raspberry Pi Pico W
 *
 * Reads battery data from E-Box via RS232 (UART) and publishes SOC to MQTT
 *
 * Hardware Requirements:
 * - Raspberry Pi Pico W
 * - RS232 to TTL converter module connected to UART0 (GPIO 0/1)
 * - WiFi network
 * - MQTT broker
 *
 * Build:
 *   mkdir build && cd build
 *   cmake ..
 *   make
 *
 * Flash:
 *   - Hold BOOTSEL button while connecting USB
 *   - Copy build/ebox_pico.uf2 to RPI-RP2 drive
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "pico/stdlib.h"
#include "pico/cyw43_arch.h"
#include "hardware/uart.h"
#include "hardware/gpio.h"
#include "lwip/apps/mqtt.h"
#include "lwip/ip_addr.h"

#include "wifi_config.h"

// Version
#define VERSION "1.0.0-pico"

// LED blink patterns
#define LED_BLINK_CONNECTING 250    // Fast blink when connecting
#define LED_BLINK_CONNECTED 1000    // Slow blink when connected
#define LED_BLINK_ERROR 100         // Very fast blink on error

// Global variables
static mqtt_client_t *mqtt_client = NULL;
static bool mqtt_connected = false;
static bool wifi_connected = false;

// Buffer for UART reading
typedef struct {
    char lines[EBOX_MAX_LINES][EBOX_MAX_LINE_LEN];
    int count;
} LineBuffer;

// Battery data structure
typedef struct {
    int battery_num;
    float voltage_mv;
    float current_ma;
    float soc_percent;
    bool valid;
} BatteryData;

// Function declarations
static void wifi_init(void);
static void mqtt_init(void);
static void uart_init_rs232(void);
static void read_ebox_data(LineBuffer *buffer);
static bool parse_battery_data(const LineBuffer *buffer, BatteryData *data);
static void publish_mqtt(const BatteryData *data);
static void led_blink_pattern(uint32_t interval_ms);

// LED blink task
static void led_blink_pattern(uint32_t interval_ms) {
    static uint32_t last_blink = 0;
    static bool led_on = false;

    uint32_t now = to_ms_since_boot(get_absolute_time());
    if (now - last_blink >= interval_ms) {
        led_on = !led_on;
        cyw43_arch_gpio_put(CYW43_WL_GPIO_LED_PIN, led_on);
        last_blink = now;
    }
}

// MQTT connection callback
static void mqtt_connection_cb(mqtt_client_t *client, void *arg, mqtt_connection_status_t status) {
    if (status == MQTT_CONNECT_ACCEPTED) {
        printf("MQTT: Connected to broker\n");
        mqtt_connected = true;
    } else {
        printf("MQTT: Connection failed, status: %d\n", status);
        mqtt_connected = false;
    }
}

// MQTT publish callback
static void mqtt_pub_request_cb(void *arg, err_t result) {
    if (result == ERR_OK) {
        printf("MQTT: Published successfully\n");
    } else {
        printf("MQTT: Publish failed, error: %d\n", result);
    }
}

// Initialize WiFi
static void wifi_init(void) {
    printf("WiFi: Connecting to %s...\n", WIFI_SSID);

    if (cyw43_arch_init()) {
        printf("WiFi: Failed to initialize\n");
        return;
    }

    cyw43_arch_enable_sta_mode();

    // Connect to WiFi
    int retry_count = 0;
    while (retry_count < 10) {
        printf("WiFi: Connection attempt %d/10\n", retry_count + 1);

        if (cyw43_arch_wifi_connect_timeout_ms(WIFI_SSID, WIFI_PASSWORD,
                                                CYW43_AUTH_WPA2_AES_PSK, 30000) == 0) {
            printf("WiFi: Connected successfully!\n");
            printf("WiFi: IP address: %s\n", ip4addr_ntoa(netif_ip4_addr(netif_list)));
            wifi_connected = true;
            return;
        }

        printf("WiFi: Connection failed, retrying...\n");
        retry_count++;
        sleep_ms(1000);
    }

    printf("WiFi: Failed to connect after 10 attempts\n");
}

// Initialize MQTT
static void mqtt_init(void) {
    if (!wifi_connected) {
        printf("MQTT: WiFi not connected, skipping MQTT init\n");
        return;
    }

    printf("MQTT: Initializing client...\n");

    mqtt_client = mqtt_client_new();
    if (mqtt_client == NULL) {
        printf("MQTT: Failed to create client\n");
        return;
    }

    // Parse broker IP
    ip_addr_t broker_ip;
    if (!ip4addr_aton(MQTT_BROKER_IP, &broker_ip)) {
        printf("MQTT: Invalid broker IP address\n");
        return;
    }

    // Connect to broker
    struct mqtt_connect_client_info_t ci;
    memset(&ci, 0, sizeof(ci));
    ci.client_id = MQTT_CLIENT_ID;

    printf("MQTT: Connecting to %s:%d\n", MQTT_BROKER_IP, MQTT_BROKER_PORT);

    err_t err = mqtt_client_connect(mqtt_client, &broker_ip, MQTT_BROKER_PORT,
                                    mqtt_connection_cb, NULL, &ci);

    if (err != ERR_OK) {
        printf("MQTT: Connect failed, error: %d\n", err);
    }
}

// Initialize UART for RS232
static void uart_init_rs232(void) {
    printf("UART: Initializing on GPIO %d (TX) and %d (RX)\n", UART_TX_PIN, UART_RX_PIN);

    // Initialize UART
    uart_init(UART_ID, UART_BAUD_RATE);

    // Set TX and RX pins
    gpio_set_function(UART_TX_PIN, GPIO_FUNC_UART);
    gpio_set_function(UART_RX_PIN, GPIO_FUNC_UART);

    // Set UART format: 8N1
    uart_set_format(UART_ID, 8, 1, UART_PARITY_NONE);

    // Enable UART FIFO
    uart_set_fifo_enabled(UART_ID, true);

    printf("UART: Initialized at %d baud\n", UART_BAUD_RATE);
}

// Read line from UART with timeout
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
            sleep_ms(1);  // Small delay to prevent busy waiting
        }
    }

    buffer[pos] = '\0';
    return true;
}

// Send command to E-Box and read response
static void read_ebox_data(LineBuffer *buffer) {
    buffer->count = 0;

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
            // Timeout - retry sending command
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

// Check if string is numeric
static bool is_numeric(const char *str) {
    if (!str || !*str) return false;
    char *endptr;
    strtod(str, &endptr);
    return *endptr == '\0' || *endptr == '\r' || *endptr == '\n';
}

// Parse battery data from response
static bool parse_battery_data(const LineBuffer *buffer, BatteryData *data) {
    data->valid = false;

    for (int i = 0; i < buffer->count; i++) {
        const char *line = buffer->lines[i];

        // Skip empty lines and markers
        if (strlen(line) == 0 || strstr(line, "$") || strstr(line, "#") || strstr(line, "Power")) {
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

        // Need at least 13 fields for battery data
        if (field_count < 13) {
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
        data->battery_num = atoi(fields[0]);
        data->voltage_mv = atof(fields[1]);
        data->current_ma = atof(fields[2]);
        data->soc_percent = (field_count > 12) ? atof(fields[12]) : 0.0f;
        data->valid = true;

        printf("Battery %d: Voltage=%.1fmV, Current=%.1fmA, SOC=%.1f%%\n",
               data->battery_num, data->voltage_mv, data->current_ma, data->soc_percent);

        return true;  // Return first valid battery
    }

    return false;
}

// Publish battery data to MQTT
static void publish_mqtt(const BatteryData *data) {
    if (!mqtt_connected || mqtt_client == NULL) {
        printf("MQTT: Not connected, skipping publish\n");
        return;
    }

    char payload[128];
    err_t err;

    // Publish SOC
    snprintf(payload, sizeof(payload), "%.1f", data->soc_percent);
    err = mqtt_publish(mqtt_client, MQTT_TOPIC_SOC, payload, strlen(payload),
                      0, 0, mqtt_pub_request_cb, NULL);
    if (err != ERR_OK) {
        printf("MQTT: Failed to publish SOC, error: %d\n", err);
    }

    // Publish Voltage
    snprintf(payload, sizeof(payload), "%.1f", data->voltage_mv);
    err = mqtt_publish(mqtt_client, MQTT_TOPIC_VOLTAGE, payload, strlen(payload),
                      0, 0, mqtt_pub_request_cb, NULL);
    if (err != ERR_OK) {
        printf("MQTT: Failed to publish voltage, error: %d\n", err);
    }

    // Publish Current
    snprintf(payload, sizeof(payload), "%.1f", data->current_ma);
    err = mqtt_publish(mqtt_client, MQTT_TOPIC_CURRENT, payload, strlen(payload),
                      0, 0, mqtt_pub_request_cb, NULL);
    if (err != ERR_OK) {
        printf("MQTT: Failed to publish current, error: %d\n", err);
    }
}

// Main function
int main() {
    // Initialize USB stdio for debugging
    stdio_init_all();

    // Wait for USB connection (optional, for debugging)
    sleep_ms(2000);

    printf("\n");
    printf("===========================================\n");
    printf("  E-Box Battery Monitor for Pico W\n");
    printf("  Version: %s\n", VERSION);
    printf("===========================================\n\n");

    // Initialize hardware
    uart_init_rs232();
    wifi_init();

    if (wifi_connected) {
        mqtt_init();
    }

    printf("\nStarting main loop...\n\n");

    // Main loop
    uint32_t last_read = 0;
    const uint32_t read_interval_ms = 60000;  // Read every 60 seconds

    while (true) {
        uint32_t now = to_ms_since_boot(get_absolute_time());

        // LED blink pattern based on status
        if (!wifi_connected) {
            led_blink_pattern(LED_BLINK_CONNECTING);
        } else if (!mqtt_connected) {
            led_blink_pattern(LED_BLINK_ERROR);
        } else {
            led_blink_pattern(LED_BLINK_CONNECTED);
        }

        // Read E-Box data periodically
        if (now - last_read >= read_interval_ms) {
            printf("\n--- Reading E-Box data ---\n");

            LineBuffer buffer;
            read_ebox_data(&buffer);

            BatteryData data;
            if (parse_battery_data(&buffer, &data)) {
                printf("Parsed battery data successfully\n");
                publish_mqtt(&data);
            } else {
                printf("Failed to parse battery data\n");
            }

            last_read = now;
            printf("--- Next read in %d seconds ---\n\n", read_interval_ms / 1000);
        }

        sleep_ms(100);
    }

    return 0;
}
