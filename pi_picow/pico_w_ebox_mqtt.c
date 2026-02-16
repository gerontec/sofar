/**
 * Raspberry Pi Pico W - MQTT + Modbus + EBox Controller
 *
 * Architecture:
 * - WiFi: Connect to WLAN, subscribe MQTT topics
 * - MQTT: Subscribe/Publish battery + inverter status
 * - Core 0: SOYO (UART0 → RS485) + EBox (PIO UART → RS232)
 * - Core 1: Relay (UART1 → RS485)
 *
 * Hardware Connections:
 *   UART0 (GPIO 0/1 + GPIO 2)     → MAX485 #1 → SOYO (4800 baud, RS485)
 *   UART1 (GPIO 4/5 + GPIO 6)     → MAX485 #2 → Relay (9600 baud, RS485)
 *   PIO UART (GPIO 8/9)           → MAX3232   → EBox (115200 baud, RS232)
 *
 * MQTT Topics:
 *   Subscribe: pico/soyo/watts, pico/relay/state
 *   Publish: pico/status, pico/ebox/soc (every 10s)
 *
 * EBox RS232 Protocol:
 *   Command: "pwr\r\n" (ASCII)
 *   Response: Multiple lines with battery data
 *   Format: "1  51018  0  19000  ...  11%  ..."
 *   Parse: Field with "%" is SOC
 */

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>

#include "pico/stdlib.h"
#include "pico/multicore.h"
#include "pico/mutex.h"
#include "pico/cyw43_arch.h"
#include "hardware/uart.h"
#include "hardware/gpio.h"
#include "hardware/timer.h"
#include "hardware/pio.h"
#include "hardware/clocks.h"

#include "lwip/apps/mqtt.h"
#include "lwip/ip_addr.h"

// PIO UART (we'll use a simple bit-bang implementation)
// For production, use https://github.com/raspberrypi/pico-examples/tree/master/pio/uart_rx

// ============================================================================
// CONFIGURATION
// ============================================================================

#define WIFI_SSID       "YourWiFiSSID"
#define WIFI_PASSWORD   "YourWiFiPassword"
#define MQTT_BROKER_IP  "192.168.1.100"
#define MQTT_BROKER_PORT 1883

// Hardware config
#define SOYO_UART       uart0
#define SOYO_UART_TX    0
#define SOYO_UART_RX    1
#define SOYO_DE_PIN     2
#define SOYO_BAUD       4800

#define RELAY_UART      uart1
#define RELAY_UART_TX   4
#define RELAY_UART_RX   5
#define RELAY_DE_PIN    6
#define RELAY_BAUD      9600

// EBox RS232 (PIO UART)
#define EBOX_TX_PIN     8
#define EBOX_RX_PIN     9
#define EBOX_BAUD       115200

// Timing
#define SOYO_CYCLE_MS       3000
#define RELAY_CHECK_MS      100
#define EBOX_CYCLE_MS       30000  // Read EBox every 30 seconds
#define STATUS_PUBLISH_MS   5000

#define RELAY_SLAVE_ID  1
#define SOYO_DEVICE_ID  1

// ============================================================================
// PUBLIC RAM
// ============================================================================

typedef struct {
    // Control
    volatile uint16_t soyo_target_watts;
    volatile bool     soyo_enable;
    volatile uint8_t  relay_target_state;
    volatile bool     relay_needs_update;

    // Status
    volatile uint8_t  relay_current_state;
    volatile uint8_t  relay_current_bits;
    volatile int16_t  soyo_last_watts;
    volatile int8_t   ebox_soc;           // Average SOC (0-100%)
    volatile uint8_t  ebox_battery_count; // Number of batteries detected
    volatile uint32_t soyo_cycle_count;
    volatile uint32_t relay_cycle_count;
    volatile uint32_t ebox_cycle_count;
    volatile uint8_t  error_flags;

    mutex_t mutex;
} PublicRAM;

static PublicRAM g_public_ram = {
    .soyo_target_watts = 0,
    .soyo_enable = false,
    .relay_target_state = 0,
    .relay_needs_update = false,
    .relay_current_state = 0xFF,
    .relay_current_bits = 0x00,
    .soyo_last_watts = -1,
    .ebox_soc = -1,
    .ebox_battery_count = 0,
    .soyo_cycle_count = 0,
    .relay_cycle_count = 0,
    .ebox_cycle_count = 0,
    .error_flags = 0
};

#define ERR_SOYO_TIMEOUT    (1 << 0)
#define ERR_SOYO_CRC        (1 << 1)
#define ERR_RELAY_TIMEOUT   (1 << 2)
#define ERR_RELAY_CRC       (1 << 3)
#define ERR_WIFI_FAILED     (1 << 4)
#define ERR_MQTT_FAILED     (1 << 5)
#define ERR_EBOX_TIMEOUT    (1 << 6)

// State Map (same as before)
typedef struct {
    uint8_t state;
    uint8_t relay1, relay2, relay3;
} StateMap;

static const StateMap STATE_MAP[] = {
    {0,  0, 0, 0}, {1,  1, 0, 0}, {2,  0, 1, 0}, {3,  1, 1, 0},
    {4,  0, 0, 1}, {5,  1, 0, 1}, {6,  0, 1, 1}, {7,  1, 1, 1},
    {11, 0, 0, 0}
};
#define STATE_MAP_SIZE (sizeof(STATE_MAP) / sizeof(StateMap))

// ============================================================================
// SIMPLE SOFTWARE UART FOR EBOX (RS232)
// ============================================================================

// Simple bit-bang UART for TX only (RX can be added if needed)
static void ebox_uart_init(uint tx_pin, uint rx_pin, uint baud) {
    (void)rx_pin; // TODO: Implement RX
    gpio_init(tx_pin);
    gpio_set_dir(tx_pin, GPIO_OUT);
    gpio_put(tx_pin, 1);  // Idle high
}

static void ebox_uart_putc(uint tx_pin, uint baud, char c) {
    uint32_t bit_time_us = 1000000 / baud;

    gpio_put(tx_pin, 0);  // Start bit
    sleep_us(bit_time_us);

    for (int i = 0; i < 8; i++) {
        gpio_put(tx_pin, (c >> i) & 1);
        sleep_us(bit_time_us);
    }

    gpio_put(tx_pin, 1);  // Stop bit
    sleep_us(bit_time_us);
}

static void ebox_uart_puts(uint tx_pin, uint baud, const char *str) {
    while (*str) {
        ebox_uart_putc(tx_pin, baud, *str++);
    }
}

// Simple RX (blocking, timeout)
static int ebox_uart_getc_timeout(uint rx_pin, uint baud, uint timeout_ms) {
    uint32_t bit_time_us = 1000000 / baud;
    absolute_time_t timeout_time = make_timeout_time_ms(timeout_ms);

    // Wait for start bit (falling edge)
    while (gpio_get(rx_pin) == 1) {
        if (time_reached(timeout_time)) {
            return -1;  // Timeout
        }
        sleep_us(10);
    }

    // Wait for middle of start bit
    sleep_us(bit_time_us / 2);

    // Read 8 data bits
    uint8_t byte = 0;
    for (int i = 0; i < 8; i++) {
        sleep_us(bit_time_us);
        if (gpio_get(rx_pin)) {
            byte |= (1 << i);
        }
    }

    // Wait for stop bit
    sleep_us(bit_time_us);

    return byte;
}

static int ebox_read_line(uint rx_pin, uint baud, char *buf, size_t len, uint timeout_ms) {
    size_t idx = 0;
    absolute_time_t start = get_absolute_time();

    while (idx < len - 1) {
        uint remaining_ms = timeout_ms - (absolute_time_diff_us(start, get_absolute_time()) / 1000);
        if (remaining_ms <= 0) {
            break;
        }

        int c = ebox_uart_getc_timeout(rx_pin, baud, remaining_ms);
        if (c < 0) {
            break;  // Timeout
        }

        if (c == '\n') {
            buf[idx] = '\0';
            return idx;
        }

        if (c != '\r') {
            buf[idx++] = c;
        }
    }

    buf[idx] = '\0';
    return idx;
}

// ============================================================================
// EBOX INTERFACE
// ============================================================================

static bool parse_soc_from_field(const char *field, int *soc) {
    // Look for percentage sign
    const char *pct = strchr(field, '%');
    if (!pct) {
        return false;
    }

    // Parse number before %
    char num_buf[16];
    size_t len = pct - field;
    if (len >= sizeof(num_buf)) {
        len = sizeof(num_buf) - 1;
    }
    strncpy(num_buf, field, len);
    num_buf[len] = '\0';

    *soc = atoi(num_buf);
    return true;
}

static bool read_ebox_soc(void) {
    // Send "pwr\r\n" command
    ebox_uart_puts(EBOX_TX_PIN, EBOX_BAUD, "pwr\r\n");

    // Read response lines
    char line[256];
    int total_soc = 0;
    int battery_count = 0;

    for (int i = 0; i < 10; i++) {  // Max 10 lines
        int len = ebox_read_line(EBOX_RX_PIN, EBOX_BAUD, line, sizeof(line), 1000);

        if (len <= 0) {
            break;  // Timeout or end
        }

        // Skip header line
        if (strstr(line, "Power") || strstr(line, "#") || strstr(line, "$")) {
            continue;
        }

        // Parse fields (space-separated)
        char *token = strtok(line, " \t");
        int field_idx = 0;

        while (token != NULL && field_idx < 20) {
            // Check if this field contains SOC (has %)
            int soc;
            if (parse_soc_from_field(token, &soc)) {
                total_soc += soc;
                battery_count++;
                printf("EBox: Battery %d SOC = %d%%\n", battery_count, soc);
                break;  // Found SOC for this line, move to next line
            }

            token = strtok(NULL, " \t");
            field_idx++;
        }
    }

    if (battery_count > 0) {
        int avg_soc = total_soc / battery_count;
        g_public_ram.ebox_soc = avg_soc;
        g_public_ram.ebox_battery_count = battery_count;
        g_public_ram.error_flags &= ~ERR_EBOX_TIMEOUT;
        printf("EBox: Average SOC = %d%% (%d batteries)\n", avg_soc, battery_count);
        return true;
    }

    g_public_ram.error_flags |= ERR_EBOX_TIMEOUT;
    return false;
}

static bool ebox_timer_callback(struct repeating_timer *t) {
    (void)t;

    read_ebox_soc();
    g_public_ram.ebox_cycle_count++;

    return true;
}

// ============================================================================
// CRC & RS485 (same as before)
// ============================================================================

static uint16_t crc16_modbus(const uint8_t *data, size_t len) {
    uint16_t crc = 0xFFFF;
    for (size_t i = 0; i < len; i++) {
        crc ^= data[i];
        for (int j = 0; j < 8; j++) {
            crc = (crc & 0x0001) ? (crc >> 1) ^ 0xA001 : (crc >> 1);
        }
    }
    return crc;
}

static inline void rs485_tx_mode(uint de_pin) {
    gpio_put(de_pin, 1);
    sleep_us(10);
}

static inline void rs485_rx_mode(uint de_pin) {
    sleep_us(10);
    gpio_put(de_pin, 0);
}

// ============================================================================
// SOYO INTERFACE (same as before, abbreviated)
// ============================================================================

static bool send_soyo_command(uint16_t watts) {
    uint8_t cmd[8];
    cmd[0] = SOYO_DEVICE_ID;
    cmd[1] = 0x06;
    cmd[2] = 0x0B;
    cmd[3] = 0xB8;
    cmd[4] = (watts >> 8) & 0xFF;
    cmd[5] = watts & 0xFF;
    uint16_t crc = crc16_modbus(cmd, 6);
    cmd[6] = crc & 0xFF;
    cmd[7] = (crc >> 8) & 0xFF;

    while (uart_is_readable(SOYO_UART)) uart_getc(SOYO_UART);

    rs485_tx_mode(SOYO_DE_PIN);
    uart_write_blocking(SOYO_UART, cmd, 8);
    rs485_rx_mode(SOYO_DE_PIN);

    uint8_t resp[8];
    absolute_time_t timeout = make_timeout_time_ms(500);
    for (int i = 0; i < 8; i++) {
        while (!uart_is_readable(SOYO_UART)) {
            if (time_reached(timeout)) {
                g_public_ram.error_flags |= ERR_SOYO_TIMEOUT;
                return false;
            }
            sleep_us(100);
        }
        resp[i] = uart_getc(SOYO_UART);
    }

    uint16_t recv_crc = resp[6] | (resp[7] << 8);
    uint16_t calc_crc = crc16_modbus(resp, 6);
    if (recv_crc != calc_crc) {
        g_public_ram.error_flags |= ERR_SOYO_CRC;
        return false;
    }

    g_public_ram.error_flags &= ~(ERR_SOYO_TIMEOUT | ERR_SOYO_CRC);
    return true;
}

static bool soyo_timer_callback(struct repeating_timer *t) {
    (void)t;

    uint16_t target_watts = g_public_ram.soyo_target_watts;
    bool enable = g_public_ram.soyo_enable;

    if (!enable) target_watts = 0;

    if (send_soyo_command(target_watts)) {
        g_public_ram.soyo_last_watts = target_watts;
        cyw43_arch_gpio_put(CYW43_WL_GPIO_LED_PIN, 1);
        sleep_ms(50);
        cyw43_arch_gpio_put(CYW43_WL_GPIO_LED_PIN, 0);
    }

    g_public_ram.soyo_cycle_count++;
    return true;
}

// ============================================================================
// RELAY INTERFACE (abbreviated, same as before)
// ============================================================================

static bool write_relay_status(uint8_t state) {
    // Implementation same as pico_w_mqtt_modbus.c
    // (abbreviated for brevity)
    return true;  // Placeholder
}

static void core1_relay_handler(void) {
    uint8_t last_state = 0xFF;

    while (true) {
        if (g_public_ram.relay_needs_update) {
            uint8_t target = g_public_ram.relay_target_state;
            if (target != last_state) {
                if (write_relay_status(target)) {
                    last_state = target;
                    g_public_ram.relay_current_state = target;
                }
            }
            g_public_ram.relay_needs_update = false;
            g_public_ram.relay_cycle_count++;
        }
        sleep_ms(RELAY_CHECK_MS);
    }
}

// ============================================================================
// MQTT
// ============================================================================

static mqtt_client_t *mqtt_client = NULL;

static void mqtt_connection_cb(mqtt_client_t *client, void *arg, mqtt_connection_status_t status) {
    (void)client;
    (void)arg;

    if (status == MQTT_CONNECT_ACCEPTED) {
        printf("MQTT connected!\n");
        g_public_ram.error_flags &= ~ERR_MQTT_FAILED;

        mqtt_subscribe(client, "pico/soyo/watts", 1, NULL, NULL);
        mqtt_subscribe(client, "pico/soyo/enable", 1, NULL, NULL);
        mqtt_subscribe(client, "pico/relay/state", 1, NULL, NULL);
    } else {
        printf("MQTT connection failed: %d\n", status);
        g_public_ram.error_flags |= ERR_MQTT_FAILED;
    }
}

// ============================================================================
// MAIN
// ============================================================================

int main(void) {
    stdio_init_all();

    printf("\n=== Pico W MQTT Modbus + EBox Controller ===\n");

    // Initialize WiFi
    if (cyw43_arch_init()) {
        printf("ERROR: WiFi init failed!\n");
        return 1;
    }

    cyw43_arch_enable_sta_mode();
    printf("Connecting to WiFi '%s'...\n", WIFI_SSID);

    if (cyw43_arch_wifi_connect_timeout_ms(WIFI_SSID, WIFI_PASSWORD,
                                            CYW43_AUTH_WPA2_AES_PSK, 30000)) {
        printf("ERROR: WiFi connection failed!\n");
        return 1;
    }

    printf("WiFi connected!\n");

    // Initialize MQTT
    mqtt_client = mqtt_client_new();
    struct mqtt_connect_client_info_t ci;
    memset(&ci, 0, sizeof(ci));
    ci.client_id = "pico_w_ebox";

    ip_addr_t broker_ip;
    ipaddr_aton(MQTT_BROKER_IP, &broker_ip);

    printf("Connecting to MQTT broker...\n");
    mqtt_client_connect(mqtt_client, &broker_ip, MQTT_BROKER_PORT,
                       mqtt_connection_cb, NULL, &ci);

    // Initialize mutex
    mutex_init(&g_public_ram.mutex);

    // Initialize UARTs
    uart_init(SOYO_UART, SOYO_BAUD);
    gpio_set_function(SOYO_UART_TX, GPIO_FUNC_UART);
    gpio_set_function(SOYO_UART_RX, GPIO_FUNC_UART);

    uart_init(RELAY_UART, RELAY_BAUD);
    gpio_set_function(RELAY_UART_TX, GPIO_FUNC_UART);
    gpio_set_function(RELAY_UART_RX, GPIO_FUNC_UART);

    // Initialize RS485 DE/RE
    gpio_init(SOYO_DE_PIN);
    gpio_set_dir(SOYO_DE_PIN, GPIO_OUT);
    gpio_put(SOYO_DE_PIN, 0);

    gpio_init(RELAY_DE_PIN);
    gpio_set_dir(RELAY_DE_PIN, GPIO_OUT);
    gpio_put(RELAY_DE_PIN, 0);

    // Initialize EBox UART (software bit-bang)
    ebox_uart_init(EBOX_TX_PIN, EBOX_RX_PIN, EBOX_BAUD);
    gpio_init(EBOX_RX_PIN);
    gpio_set_dir(EBOX_RX_PIN, GPIO_IN);
    gpio_pull_up(EBOX_RX_PIN);

    printf("Hardware initialized\n");

    // Launch Core 1
    multicore_launch_core1(core1_relay_handler);

    // Setup timers
    struct repeating_timer soyo_timer, ebox_timer;
    add_repeating_timer_ms(SOYO_CYCLE_MS, soyo_timer_callback, NULL, &soyo_timer);
    add_repeating_timer_ms(EBOX_CYCLE_MS, ebox_timer_callback, NULL, &ebox_timer);

    printf("Ready!\n");

    // Main loop
    absolute_time_t last_status = get_absolute_time();

    while (true) {
        cyw43_arch_poll();

        absolute_time_t now = get_absolute_time();
        if (absolute_time_diff_us(last_status, now) >= STATUS_PUBLISH_MS * 1000) {
            char status[512];
            snprintf(status, sizeof(status),
                     "{\"soyo_watts\":%d,\"relay_state\":%d,\"relay_bits\":\"0x%02X\","
                     "\"ebox_soc\":%d,\"ebox_batteries\":%d,\"errors\":\"0x%02X\"}",
                     g_public_ram.soyo_last_watts,
                     g_public_ram.relay_current_state,
                     g_public_ram.relay_current_bits,
                     g_public_ram.ebox_soc,
                     g_public_ram.ebox_battery_count,
                     g_public_ram.error_flags);

            mqtt_publish(mqtt_client, "pico/status", status, strlen(status), 1, 0, NULL, NULL);

            // Also publish SOC separately
            char soc_str[16];
            snprintf(soc_str, sizeof(soc_str), "%d", g_public_ram.ebox_soc);
            mqtt_publish(mqtt_client, "pico/ebox/soc", soc_str, strlen(soc_str), 1, 1, NULL, NULL);

            last_status = now;
        }

        sleep_ms(100);
    }

    return 0;
}
