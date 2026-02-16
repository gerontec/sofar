/*
 * read_inverter_pico.c - Sofar Inverter Modbus Reader for Raspberry Pi Pico W
 *
 * Reads inverter data via RS232/Modbus RTU and publishes via MQTT
 * Replaces file-based interface with MQTT publish
 *
 * Hardware Requirements:
 * - Raspberry Pi Pico W
 * - RS232 to TTL converter connected to UART0
 * - WiFi network
 * - MQTT broker at 192.168.178.218
 *
 * Build:
 *   mkdir build && cd build
 *   cmake ..
 *   make
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

#include "sofar_config.h"

#define VERSION "1.0.0-pico"

// Modbus function codes
#define MODBUS_FC_READ_HOLDING_REGISTERS 0x03

// CRC calculation for Modbus RTU
static uint16_t modbus_crc16(const uint8_t *data, size_t len) {
    uint16_t crc = 0xFFFF;

    for (size_t i = 0; i < len; i++) {
        crc ^= data[i];
        for (int j = 0; j < 8; j++) {
            if (crc & 0x0001) {
                crc >>= 1;
                crc ^= 0xA001;
            } else {
                crc >>= 1;
            }
        }
    }

    return crc;
}

// Inverter data structure
typedef struct {
    int16_t grid_w;
    int16_t bat_w;
    uint16_t soc_bat1;
    uint16_t soc_bat2;
    int16_t bat2_current;
    uint16_t state;

    bool grid_w_valid;
    bool bat_w_valid;
    bool soc_bat1_valid;
    bool soc_bat2_valid;
    bool bat2_current_valid;
    bool state_valid;
} InverterData;

// Global variables
static mqtt_client_t *mqtt_client = NULL;
static bool mqtt_connected = false;
static bool wifi_connected = false;
static InverterData inverter_data = {0};

// Function declarations
static void wifi_init(void);
static void mqtt_init(void);
static void uart_modbus_init(void);
static bool modbus_read_holding_register(uint16_t reg_addr, uint16_t *value);
static void read_inverter_data(void);
static void publish_inverter_data(void);

// Initialize Modbus UART
static void uart_modbus_init(void) {
    printf("Modbus: Initializing on GPIO %d (TX), %d (RX)\n",
           UART_MODBUS_TX_PIN, UART_MODBUS_RX_PIN);

    // Initialize UART
    uart_init(UART_MODBUS, UART_MODBUS_BAUD);

    // Set TX and RX pins
    gpio_set_function(UART_MODBUS_TX_PIN, GPIO_FUNC_UART);
    gpio_set_function(UART_MODBUS_RX_PIN, GPIO_FUNC_UART);

    // Set UART format: 8N1 (8 data bits, no parity, 1 stop bit)
    uart_set_format(UART_MODBUS, 8, 1, UART_PARITY_NONE);

    // Set FIFO
    uart_set_fifo_enabled(UART_MODBUS, true);

    printf("Modbus: Initialized at %d baud\n", UART_MODBUS_BAUD);
}

// Read a single Modbus holding register
static bool modbus_read_holding_register(uint16_t reg_addr, uint16_t *value) {
    uint8_t request[8];
    uint8_t response[7];
    uint16_t crc;

    // Build Modbus RTU request
    request[0] = MODBUS_UNIT_ID;
    request[1] = MODBUS_FC_READ_HOLDING_REGISTERS;
    request[2] = (reg_addr >> 8) & 0xFF;    // Register address high
    request[3] = reg_addr & 0xFF;           // Register address low
    request[4] = 0x00;                      // Quantity high (read 1 register)
    request[5] = 0x01;                      // Quantity low

    // Calculate CRC
    crc = modbus_crc16(request, 6);
    request[6] = crc & 0xFF;                // CRC low
    request[7] = (crc >> 8) & 0xFF;         // CRC high

    // Flush RX buffer
    while (uart_is_readable(UART_MODBUS)) {
        uart_getc(UART_MODBUS);
    }

    // Send request
    uart_write_blocking(UART_MODBUS, request, 8);

    // Wait for response (7 bytes: slave_id + function + byte_count + data(2) + crc(2))
    uint32_t start_time = to_ms_since_boot(get_absolute_time());
    size_t received = 0;

    while (received < 7) {
        if (to_ms_since_boot(get_absolute_time()) - start_time > MODBUS_TIMEOUT_MS) {
            printf("Modbus: Timeout reading register 0x%04X\n", reg_addr);
            return false;
        }

        if (uart_is_readable(UART_MODBUS)) {
            response[received++] = uart_getc(UART_MODBUS);
        }
    }

    // Verify response
    if (response[0] != MODBUS_UNIT_ID) {
        printf("Modbus: Invalid unit ID in response\n");
        return false;
    }

    if (response[1] != MODBUS_FC_READ_HOLDING_REGISTERS) {
        printf("Modbus: Invalid function code in response (0x%02X)\n", response[1]);
        return false;
    }

    if (response[2] != 2) {  // Byte count should be 2 for 1 register
        printf("Modbus: Invalid byte count in response\n");
        return false;
    }

    // Verify CRC
    uint16_t received_crc = response[5] | (response[6] << 8);
    uint16_t calculated_crc = modbus_crc16(response, 5);

    if (received_crc != calculated_crc) {
        printf("Modbus: CRC error (received: 0x%04X, calculated: 0x%04X)\n",
               received_crc, calculated_crc);
        return false;
    }

    // Extract register value (big-endian)
    *value = (response[3] << 8) | response[4];

    return true;
}

// Read all inverter data
static void read_inverter_data(void) {
    uint16_t raw_value;

    // Read grid power (I16, 10W resolution)
    if (modbus_read_holding_register(REG_GRID_POWER, &raw_value)) {
        inverter_data.grid_w = (int16_t)raw_value * 10;
        inverter_data.grid_w_valid = true;
        printf("Grid: %d W\n", inverter_data.grid_w);
    } else {
        inverter_data.grid_w_valid = false;
    }

    sleep_ms(100);  // Small delay between requests

    // Read battery power (I16, 10W resolution)
    if (modbus_read_holding_register(REG_BAT_POWER, &raw_value)) {
        inverter_data.bat_w = (int16_t)raw_value * 10;
        inverter_data.bat_w_valid = true;
        printf("Battery: %d W\n", inverter_data.bat_w);
    } else {
        inverter_data.bat_w_valid = false;
    }

    sleep_ms(100);

    // Read SOC Battery 1 (U16, %)
    if (modbus_read_holding_register(REG_BAT1_SOC, &raw_value)) {
        inverter_data.soc_bat1 = raw_value;
        inverter_data.soc_bat1_valid = true;
        printf("SOC Bat1: %u %%\n", inverter_data.soc_bat1);
    } else {
        inverter_data.soc_bat1_valid = false;
    }

    sleep_ms(100);

    // Read SOC Battery 2 (U16, %)
    if (modbus_read_holding_register(REG_BAT2_SOC, &raw_value)) {
        inverter_data.soc_bat2 = raw_value;
        inverter_data.soc_bat2_valid = true;
        printf("SOC Bat2: %u %%\n", inverter_data.soc_bat2);
    } else {
        inverter_data.soc_bat2_valid = false;
    }

    sleep_ms(100);

    // Read Battery 2 current (I16, 0.01A resolution)
    if (modbus_read_holding_register(REG_BAT2_CURRENT, &raw_value)) {
        inverter_data.bat2_current = (int16_t)raw_value;
        inverter_data.bat2_current_valid = true;
        printf("Bat2 Current: %.2f A\n", inverter_data.bat2_current * 0.01f);
    } else {
        inverter_data.bat2_current_valid = false;
    }

    sleep_ms(100);

    // Read inverter state (U16, 0-7)
    if (modbus_read_holding_register(REG_INVERTER_STATE, &raw_value)) {
        inverter_data.state = raw_value;
        inverter_data.state_valid = true;
        printf("State: %u\n", inverter_data.state);
    } else {
        inverter_data.state_valid = false;
    }
}

// Publish inverter data to MQTT
static void publish_inverter_data(void) {
    if (!mqtt_connected || !mqtt_client) {
        return;
    }

    char payload[64];

    // Publish Grid W
    if (inverter_data.grid_w_valid) {
        snprintf(payload, sizeof(payload), "%d", inverter_data.grid_w);
        mqtt_publish(mqtt_client, MQTT_PUB_GRID_W, payload, strlen(payload), 0, 0, NULL, NULL);
    }

    // Publish Battery W
    if (inverter_data.bat_w_valid) {
        snprintf(payload, sizeof(payload), "%d", inverter_data.bat_w);
        mqtt_publish(mqtt_client, MQTT_PUB_BAT_W, payload, strlen(payload), 0, 0, NULL, NULL);
    }

    // Publish SOC Bat1
    if (inverter_data.soc_bat1_valid) {
        snprintf(payload, sizeof(payload), "%u", inverter_data.soc_bat1);
        mqtt_publish(mqtt_client, MQTT_PUB_SOC_BAT1, payload, strlen(payload), 0, 0, NULL, NULL);
    }

    // Publish SOC Bat2
    if (inverter_data.soc_bat2_valid) {
        snprintf(payload, sizeof(payload), "%u", inverter_data.soc_bat2);
        mqtt_publish(mqtt_client, MQTT_PUB_SOC_BAT2, payload, strlen(payload), 0, 0, NULL, NULL);
    }

    // Publish Bat2 Current
    if (inverter_data.bat2_current_valid) {
        snprintf(payload, sizeof(payload), "%.2f", inverter_data.bat2_current * 0.01f);
        mqtt_publish(mqtt_client, MQTT_PUB_BAT2_CURRENT, payload, strlen(payload), 0, 0, NULL, NULL);
    }

    // Publish State
    if (inverter_data.state_valid) {
        snprintf(payload, sizeof(payload), "%u", inverter_data.state);
        mqtt_publish(mqtt_client, MQTT_PUB_STATE, payload, strlen(payload), 0, 0, NULL, NULL);
    }

    // Publish CSV line (for backward compatibility with soyo1min)
    if (inverter_data.grid_w_valid && inverter_data.bat_w_valid &&
        inverter_data.soc_bat2_valid && inverter_data.soc_bat1_valid &&
        inverter_data.bat2_current_valid && inverter_data.state_valid) {

        snprintf(payload, sizeof(payload), "%d,%d,%u,%u,%.2f,%u",
                 inverter_data.grid_w,
                 inverter_data.bat_w,
                 inverter_data.soc_bat2,
                 inverter_data.soc_bat1,
                 inverter_data.bat2_current * 0.01f,
                 inverter_data.state);

        mqtt_publish(mqtt_client, MQTT_PUB_CSV, payload, strlen(payload), 0, 0, NULL, NULL);

        printf("MQTT: Published CSV: %s\n", payload);
    }
}

// WiFi initialization
static void wifi_init(void) {
    printf("WiFi: Connecting to %s...\n", SOFAR_WIFI_SSID);

    if (cyw43_arch_init()) {
        printf("WiFi: Failed to initialize\n");
        return;
    }

    cyw43_arch_enable_sta_mode();

    int retry_count = 0;
    while (retry_count < 10) {
        printf("WiFi: Connection attempt %d/10\n", retry_count + 1);

        if (cyw43_arch_wifi_connect_timeout_ms(SOFAR_WIFI_SSID, SOFAR_WIFI_PASSWORD,
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

// MQTT connection callback
static void mqtt_connection_cb(mqtt_client_t *client, void *arg, mqtt_connection_status_t status) {
    if (status == MQTT_CONNECT_ACCEPTED) {
        printf("MQTT: Connected to broker\n");
        mqtt_connected = true;

        // Publish status
        const char *msg = "Sofar reader online";
        mqtt_publish(client, MQTT_PUB_STATUS, msg, strlen(msg), 0, 0, NULL, NULL);
    } else {
        printf("MQTT: Connection failed, status: %d\n", status);
        mqtt_connected = false;
    }
}

// MQTT initialization
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

    // Parse broker IP
    ip_addr_t broker_ip;
    if (!ip4addr_aton(SOFAR_MQTT_BROKER, &broker_ip)) {
        printf("MQTT: Invalid broker IP\n");
        return;
    }

    // Connect
    struct mqtt_connect_client_info_t ci = {0};
    ci.client_id = SOFAR_MQTT_CLIENT_ID;

    printf("MQTT: Connecting to %s:%d\n", SOFAR_MQTT_BROKER, SOFAR_MQTT_PORT);

    err_t err = mqtt_client_connect(mqtt_client, &broker_ip, SOFAR_MQTT_PORT,
                                    mqtt_connection_cb, NULL, &ci);

    if (err != ERR_OK) {
        printf("MQTT: Connect failed, error: %d\n", err);
    }
}

// Main function
int main() {
    stdio_init_all();
    sleep_ms(2000);

    printf("\n");
    printf("===========================================\n");
    printf("  Sofar Inverter Reader for Pico W\n");
    printf("  Version: %s\n", VERSION);
    printf("===========================================\n\n");

    // Initialize hardware
    uart_modbus_init();
    wifi_init();

    if (wifi_connected) {
        mqtt_init();
    }

    printf("\nStarting read loop...\n\n");

    // Main loop
    uint32_t last_update = 0;

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

        // Read and publish inverter data
        if (now - last_update >= UPDATE_INTERVAL_MS) {
            printf("\n--- Reading Inverter Data ---\n");
            read_inverter_data();

            if (mqtt_connected) {
                publish_inverter_data();
            }

            last_update = now;
        }

        sleep_ms(100);
    }

    return 0;
}
