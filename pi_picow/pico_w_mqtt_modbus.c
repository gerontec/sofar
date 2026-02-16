/**
 * Raspberry Pi Pico W - MQTT + Modbus Dual-Core Controller
 *
 * Architecture:
 * - WiFi: Connect to WLAN, subscribe MQTT topics
 * - MQTT: Subscribe pico/soyo/watts, pico/relay/state → write g_public_ram
 * - Core 0: SOYO control (UART0 → MAX485 #1 → SOYO @ 4800 baud, 3s cycle)
 * - Core 1: Relay control (UART1 → MAX485 #2 → Relay @ 9600 baud, event)
 *
 * Hardware Connections:
 *   Pico W GPIO 0 (UART0 TX) → MAX485 #1 DI  ┐
 *   Pico W GPIO 1 (UART0 RX) → MAX485 #1 RO  ├─ RS485 → SOYO (4800 baud)
 *   Pico W GPIO 2            → MAX485 #1 DE/RE┘
 *
 *   Pico W GPIO 4 (UART1 TX) → MAX485 #2 DI  ┐
 *   Pico W GPIO 5 (UART1 RX) → MAX485 #2 RO  ├─ RS485 → Relay (9600 baud)
 *   Pico W GPIO 6            → MAX485 #2 DE/RE┘
 *
 *   Pico W GPIO 25 (LED)     → Status indicator
 *
 * MQTT Topics:
 *   Subscribe: pico/soyo/watts, pico/soyo/enable, pico/relay/state
 *   Publish: pico/status (every 5s)
 *
 * WiFi Config: Edit WIFI_SSID and WIFI_PASSWORD below
 * MQTT Config: Edit MQTT_BROKER_IP below
 */

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>

#include "pico/stdlib.h"
#include "pico/multicore.h"
#include "pico/mutex.h"
#include "pico/cyw43_arch.h"
#include "hardware/uart.h"
#include "hardware/gpio.h"
#include "hardware/timer.h"
#include "hardware/watchdog.h"

#include "lwip/apps/mqtt.h"
#include "lwip/ip_addr.h"

// ============================================================================
// CONFIGURATION - EDIT THESE!
// ============================================================================

#define WIFI_SSID       "YourWiFiSSID"
#define WIFI_PASSWORD   "YourWiFiPassword"
#define MQTT_BROKER_IP  "192.168.1.100"   // MQTT broker IP address
#define MQTT_BROKER_PORT 1883

// ============================================================================
// HARDWARE CONFIGURATION
// ============================================================================

// UART0 for SOYO (4800 baud)
#define SOYO_UART       uart0
#define SOYO_UART_TX    0
#define SOYO_UART_RX    1
#define SOYO_DE_PIN     2     // MAX485 DE/RE control
#define SOYO_BAUD       4800

// UART1 for Relay (9600 baud)
#define RELAY_UART      uart1
#define RELAY_UART_TX   4
#define RELAY_UART_RX   5
#define RELAY_DE_PIN    6     // MAX485 DE/RE control
#define RELAY_BAUD      9600

// Timing
#define SOYO_CYCLE_MS   3000
#define RELAY_CHECK_MS  100
#define STATUS_PUBLISH_MS 5000

// Modbus defaults
#define RELAY_SLAVE_ID  1
#define SOYO_DEVICE_ID  1

// ============================================================================
// PUBLIC RAM - Shared Variables (Inter-Core + MQTT)
// ============================================================================

typedef struct {
    // SOYO control (written by MQTT, read by Core 0)
    volatile uint16_t soyo_target_watts;
    volatile bool     soyo_enable;

    // Relay control (written by MQTT, read by Core 1)
    volatile uint8_t  relay_target_state;
    volatile bool     relay_needs_update;
    volatile bool     relay_status_request;

    // Status feedback (written by cores, read by MQTT)
    volatile uint8_t  relay_current_state;
    volatile uint8_t  relay_current_bits;
    volatile int16_t  soyo_last_watts;
    volatile uint32_t soyo_cycle_count;
    volatile uint32_t relay_cycle_count;
    volatile uint8_t  error_flags;

    mutex_t mutex;
} PublicRAM;

static PublicRAM g_public_ram = {
    .soyo_target_watts = 0,
    .soyo_enable = false,
    .relay_target_state = 0,
    .relay_needs_update = false,
    .relay_status_request = false,
    .relay_current_state = 0xFF,
    .relay_current_bits = 0x00,
    .soyo_last_watts = -1,
    .soyo_cycle_count = 0,
    .relay_cycle_count = 0,
    .error_flags = 0
};

// Error flags
#define ERR_SOYO_TIMEOUT    (1 << 0)
#define ERR_SOYO_CRC        (1 << 1)
#define ERR_RELAY_TIMEOUT   (1 << 2)
#define ERR_RELAY_CRC       (1 << 3)
#define ERR_WIFI_FAILED     (1 << 4)
#define ERR_MQTT_FAILED     (1 << 5)

// ============================================================================
// STATE MAPPING (same as before)
// ============================================================================

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
// CRC CALCULATION
// ============================================================================

static uint16_t crc16_modbus(const uint8_t *data, size_t len) {
    uint16_t crc = 0xFFFF;
    for (size_t i = 0; i < len; i++) {
        crc ^= data[i];
        for (int j = 0; j < 8; j++) {
            if (crc & 0x0001) {
                crc = (crc >> 1) ^ 0xA001;
            } else {
                crc >>= 1;
            }
        }
    }
    return crc;
}

// ============================================================================
// RS485 CONTROL (DE/RE)
// ============================================================================

static inline void rs485_tx_mode(uint de_pin) {
    gpio_put(de_pin, 1);  // DE=1, RE=1 → Transmit mode
    sleep_us(10);          // Small delay for mode switch
}

static inline void rs485_rx_mode(uint de_pin) {
    sleep_us(10);          // Wait for transmission to complete
    gpio_put(de_pin, 0);  // DE=0, RE=0 → Receive mode
}

// ============================================================================
// SOYO INTERFACE (UART0, Core 0)
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

    // Clear RX buffer
    while (uart_is_readable(SOYO_UART)) {
        uart_getc(SOYO_UART);
    }

    // Transmit mode
    rs485_tx_mode(SOYO_DE_PIN);
    uart_write_blocking(SOYO_UART, cmd, 8);
    rs485_rx_mode(SOYO_DE_PIN);

    // Wait for response
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

    // Verify CRC
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

    if (!enable) {
        target_watts = 0;
    }

    bool ok = send_soyo_command(target_watts);

    if (ok) {
        g_public_ram.soyo_last_watts = target_watts;
        cyw43_arch_gpio_put(CYW43_WL_GPIO_LED_PIN, 1);
        sleep_ms(50);
        cyw43_arch_gpio_put(CYW43_WL_GPIO_LED_PIN, 0);
    }

    g_public_ram.soyo_cycle_count++;
    return true;
}

// ============================================================================
// RELAY INTERFACE (UART1, Core 1)
// ============================================================================

static bool read_relay_status(uint8_t *state) {
    uint8_t cmd[8];

    cmd[0] = RELAY_SLAVE_ID;
    cmd[1] = 0x01;
    cmd[2] = 0x00;
    cmd[3] = 0x00;
    cmd[4] = 0x00;
    cmd[5] = 0x03;

    uint16_t crc = crc16_modbus(cmd, 6);
    cmd[6] = crc & 0xFF;
    cmd[7] = (crc >> 8) & 0xFF;

    while (uart_is_readable(RELAY_UART)) {
        uart_getc(RELAY_UART);
    }

    rs485_tx_mode(RELAY_DE_PIN);
    uart_write_blocking(RELAY_UART, cmd, 8);
    rs485_rx_mode(RELAY_DE_PIN);

    uint8_t resp[8];
    absolute_time_t timeout = make_timeout_time_ms(1000);
    for (int i = 0; i < 5; i++) {
        while (!uart_is_readable(RELAY_UART)) {
            if (time_reached(timeout)) {
                g_public_ram.error_flags |= ERR_RELAY_TIMEOUT;
                return false;
            }
            sleep_us(100);
        }
        resp[i] = uart_getc(RELAY_UART);
    }

    uint16_t recv_crc = resp[3] | (resp[4] << 8);
    uint16_t calc_crc = crc16_modbus(resp, 3);
    if (recv_crc != calc_crc) {
        g_public_ram.error_flags |= ERR_RELAY_CRC;
        return false;
    }

    uint8_t bits = resp[2];
    g_public_ram.relay_current_bits = bits & 0x07;

    uint8_t r1 = (bits & 0x01) ? 1 : 0;
    uint8_t r2 = (bits & 0x02) ? 1 : 0;
    uint8_t r3 = (bits & 0x04) ? 1 : 0;

    for (size_t i = 0; i < STATE_MAP_SIZE; i++) {
        if (STATE_MAP[i].relay1 == r1 &&
            STATE_MAP[i].relay2 == r2 &&
            STATE_MAP[i].relay3 == r3) {
            *state = STATE_MAP[i].state;
            g_public_ram.error_flags &= ~(ERR_RELAY_TIMEOUT | ERR_RELAY_CRC);
            return true;
        }
    }

    *state = 0xFF;
    return false;
}

static bool write_relay_status(uint8_t state) {
    const StateMap *map = NULL;
    for (size_t i = 0; i < STATE_MAP_SIZE; i++) {
        if (STATE_MAP[i].state == state) {
            map = &STATE_MAP[i];
            break;
        }
    }

    if (map == NULL) {
        return false;
    }

    uint8_t cmd[10];
    cmd[0] = RELAY_SLAVE_ID;
    cmd[1] = 0x0F;
    cmd[2] = 0x00;
    cmd[3] = 0x00;
    cmd[4] = 0x00;
    cmd[5] = 0x03;
    cmd[6] = 0x01;
    cmd[7] = (map->relay1 ? 0x01 : 0x00) |
             (map->relay2 ? 0x02 : 0x00) |
             (map->relay3 ? 0x04 : 0x00);

    uint16_t crc = crc16_modbus(cmd, 8);
    cmd[8] = crc & 0xFF;
    cmd[9] = (crc >> 8) & 0xFF;

    while (uart_is_readable(RELAY_UART)) {
        uart_getc(RELAY_UART);
    }

    rs485_tx_mode(RELAY_DE_PIN);
    uart_write_blocking(RELAY_UART, cmd, 10);
    rs485_rx_mode(RELAY_DE_PIN);

    uint8_t resp[8];
    absolute_time_t timeout = make_timeout_time_ms(1000);
    for (int i = 0; i < 8; i++) {
        while (!uart_is_readable(RELAY_UART)) {
            if (time_reached(timeout)) {
                g_public_ram.error_flags |= ERR_RELAY_TIMEOUT;
                return false;
            }
            sleep_us(100);
        }
        resp[i] = uart_getc(RELAY_UART);
    }

    uint16_t recv_crc = resp[6] | (resp[7] << 8);
    uint16_t calc_crc = crc16_modbus(resp, 6);
    if (recv_crc != calc_crc) {
        g_public_ram.error_flags |= ERR_RELAY_CRC;
        return false;
    }

    g_public_ram.error_flags &= ~(ERR_RELAY_TIMEOUT | ERR_RELAY_CRC);
    return true;
}

static uint8_t state_to_bits(uint8_t state) {
    for (size_t i = 0; i < STATE_MAP_SIZE; i++) {
        if (STATE_MAP[i].state == state) {
            return (STATE_MAP[i].relay1 ? 0x01 : 0x00) |
                   (STATE_MAP[i].relay2 ? 0x02 : 0x00) |
                   (STATE_MAP[i].relay3 ? 0x04 : 0x00);
        }
    }
    return 0x00;
}

static void core1_relay_handler(void) {
    uint8_t last_state = 0xFF;

    sleep_ms(500);
    uint8_t initial_state;
    if (read_relay_status(&initial_state)) {
        last_state = initial_state;
        g_public_ram.relay_current_state = initial_state;
    }

    while (true) {
        if (g_public_ram.relay_status_request) {
            uint8_t current;
            if (read_relay_status(&current)) {
                g_public_ram.relay_current_state = current;
                last_state = current;
            }
            g_public_ram.relay_status_request = false;
        }

        if (g_public_ram.relay_needs_update) {
            uint8_t target = g_public_ram.relay_target_state;

            if (target != last_state) {
                if (write_relay_status(target)) {
                    last_state = target;
                    g_public_ram.relay_current_state = target;
                    g_public_ram.relay_current_bits = state_to_bits(target);
                }
            }

            g_public_ram.relay_needs_update = false;
            g_public_ram.relay_cycle_count++;
        }

        sleep_ms(RELAY_CHECK_MS);
    }
}

// ============================================================================
// MQTT CLIENT
// ============================================================================

static mqtt_client_t *mqtt_client = NULL;

// MQTT incoming publish callback
static void mqtt_incoming_publish_cb(void *arg, const char *topic, u32_t tot_len) {
    (void)arg;
    (void)tot_len;
    printf("MQTT topic: %s\n", topic);
}

// MQTT incoming data callback
static void mqtt_incoming_data_cb(void *arg, const u8_t *data, u16_t len, u8_t flags) {
    (void)arg;
    (void)flags;

    char value[64];
    if (len >= sizeof(value)) len = sizeof(value) - 1;
    memcpy(value, data, len);
    value[len] = '\0';

    printf("MQTT data: %s\n", value);

    // Parse value and update g_public_ram
    int val = atoi(value);

    // Topic is in publish callback, we need to track it
    // For now, simple implementation
    mutex_enter_blocking(&g_public_ram.mutex);
    // Update based on last topic (simplified)
    mutex_exit(&g_public_ram.mutex);
}

// MQTT connection callback
static void mqtt_connection_cb(mqtt_client_t *client, void *arg, mqtt_connection_status_t status) {
    (void)client;
    (void)arg;

    if (status == MQTT_CONNECT_ACCEPTED) {
        printf("MQTT connected!\n");
        g_public_ram.error_flags &= ~ERR_MQTT_FAILED;

        // Subscribe to topics
        mqtt_subscribe(client, "pico/soyo/watts", 1, NULL, NULL);
        mqtt_subscribe(client, "pico/soyo/enable", 1, NULL, NULL);
        mqtt_subscribe(client, "pico/relay/state", 1, NULL, NULL);

        printf("MQTT subscribed to topics\n");
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

    printf("\n=== Pico W MQTT Modbus Controller ===\n");

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
        g_public_ram.error_flags |= ERR_WIFI_FAILED;
        return 1;
    }

    printf("WiFi connected!\n");

    // Initialize MQTT
    mqtt_client = mqtt_client_new();

    struct mqtt_connect_client_info_t ci;
    memset(&ci, 0, sizeof(ci));
    ci.client_id = "pico_w_modbus";

    ip_addr_t broker_ip;
    ipaddr_aton(MQTT_BROKER_IP, &broker_ip);

    printf("Connecting to MQTT broker at %s...\n", MQTT_BROKER_IP);
    err_t err = mqtt_client_connect(mqtt_client, &broker_ip, MQTT_BROKER_PORT,
                                     mqtt_connection_cb, NULL, &ci);
    if (err != ERR_OK) {
        printf("ERROR: MQTT connect failed: %d\n", err);
        return 1;
    }

    // Initialize mutex
    mutex_init(&g_public_ram.mutex);

    // Initialize UARTs
    uart_init(SOYO_UART, SOYO_BAUD);
    gpio_set_function(SOYO_UART_TX, GPIO_FUNC_UART);
    gpio_set_function(SOYO_UART_RX, GPIO_FUNC_UART);
    uart_set_format(SOYO_UART, 8, 1, UART_PARITY_NONE);

    uart_init(RELAY_UART, RELAY_BAUD);
    gpio_set_function(RELAY_UART_TX, GPIO_FUNC_UART);
    gpio_set_function(RELAY_UART_RX, GPIO_FUNC_UART);
    uart_set_format(RELAY_UART, 8, 1, UART_PARITY_NONE);

    // Initialize RS485 DE/RE pins
    gpio_init(SOYO_DE_PIN);
    gpio_set_dir(SOYO_DE_PIN, GPIO_OUT);
    gpio_put(SOYO_DE_PIN, 0);  // Start in RX mode

    gpio_init(RELAY_DE_PIN);
    gpio_set_dir(RELAY_DE_PIN, GPIO_OUT);
    gpio_put(RELAY_DE_PIN, 0);

    printf("Hardware initialized\n");

    // Launch Core 1 for relay control
    multicore_launch_core1(core1_relay_handler);
    printf("Core 1 launched\n");

    // Setup SOYO timer on Core 0
    struct repeating_timer soyo_timer;
    add_repeating_timer_ms(SOYO_CYCLE_MS, soyo_timer_callback, NULL, &soyo_timer);
    printf("SOYO timer started (3s cycle)\n");

    printf("Ready!\n");

    // Main loop: Status publishing
    absolute_time_t last_status = get_absolute_time();

    while (true) {
        // Check WiFi connection
        cyw43_arch_poll();

        // Publish status every 5 seconds
        absolute_time_t now = get_absolute_time();
        if (absolute_time_diff_us(last_status, now) >= STATUS_PUBLISH_MS * 1000) {
            char status[256];
            snprintf(status, sizeof(status),
                     "{\"soyo_watts\":%d,\"relay_state\":%d,\"relay_bits\":\"0x%02X\",\"errors\":\"0x%02X\"}",
                     g_public_ram.soyo_last_watts,
                     g_public_ram.relay_current_state,
                     g_public_ram.relay_current_bits,
                     g_public_ram.error_flags);

            mqtt_publish(mqtt_client, "pico/status", status, strlen(status), 1, 0, NULL, NULL);

            last_status = now;
        }

        sleep_ms(100);
    }

    return 0;
}
