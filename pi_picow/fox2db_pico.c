/*
 * fox2db_pico.c - Complete Fox2DB Power Management for Raspberry Pi Pico W
 *
 * Port of fox2db.c with full control logic:
 * - Reads EBox battery data via RS232 (UART)
 * - Receives inverter data via MQTT (PCC, Bat1 Power)
 * - Calculates excess power with EBox efficiency
 * - State matching (0-7) with 8-level power control
 * - Deep discharge protection
 * - Stabilization cycles (prevents flapping)
 * - Hysteresis
 * - Ramp limiting
 * - Controls 3 GPIO pins for relay states 0-7
 *
 * Hardware:
 * - Raspberry Pi Pico W
 * - RS232 to TTL converter (UART0, GPIO 0/1) for EBox
 * - 3x GPIO outputs (GPIO 2-4) for relay control (binary 0-7)
 * - WiFi + MQTT broker at 192.168.178.218
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <math.h>
#include "pico/stdlib.h"
#include "pico/cyw43_arch.h"
#include "hardware/uart.h"
#include "hardware/gpio.h"
#include "lwip/apps/mqtt.h"
#include "lwip/ip_addr.h"

#define VERSION "1.0.0-pico-fox2db-full"

// ============================================================================
// Configuration
// ============================================================================

// WiFi/MQTT
#define WIFI_SSID "YOUR_WIFI_SSID"
#define WIFI_PASSWORD "YOUR_WIFI_PASSWORD"
#define MQTT_BROKER "192.168.178.218"
#define MQTT_PORT 1883
#define MQTT_CLIENT_ID "pico_fox2db_full"

// MQTT Topics
#define MQTT_TOPIC_INVERTER_JSON "inverter/power_grid_exchange/json"  // Subscribe
#define MQTT_TOPIC_STATE_PUB "fox2db/state"                          // Publish current state
#define MQTT_TOPIC_STATUS "fox2db/status"                            // Publish status

// EBox UART
#define UART_EBOX uart0
#define UART_TX_PIN 0
#define UART_RX_PIN 1
#define UART_BAUD 115200
#define EBOX_COMMAND "bat"
#define EBOX_MAX_LINES 50
#define EBOX_MAX_LINE_LEN 256
#define EBOX_TIMEOUT_MS 4000

// Relay Control GPIO (3-bit binary for states 0-7)
#define RELAY_PIN_0 2  // LSB
#define RELAY_PIN_1 3
#define RELAY_PIN_2 4  // MSB

// Regelwerk Parameters
#define MIN_EXCESS 1010              // Minimum excess power to start (W)
#define MAX_GRID_DRAW 1500           // Maximum grid draw allowed (W)
#define MAX_SOC 99                   // Maximum SOC (%)
#define HYSTERESIS 505               // Hysteresis power (W)
#define STABILIZATION_CYCLES 2       // Cycles before state change
#define DEEP_DISCHARGE_LOWER 6       // Lower SOC limit (%)
#define DEEP_DISCHARGE_UPPER 8       // Upper SOC limit (%)
#define DEEP_DISCHARGE_TARGET 7      // Charge target during protection (%)

// Update intervals
#define EBOX_READ_INTERVAL_MS 60000  // Read EBox every 60s
#define CONTROL_INTERVAL_MS 5000     // Run control loop every 5s

// State to Power Mapping (Watts)
static const int STATE_POWER[8] = {0, 3000, 3650, 6650, 3900, 7100, 7800, 11400};

// ============================================================================
// Data Structures
// ============================================================================

typedef struct {
    char lines[EBOX_MAX_LINES][EBOX_MAX_LINE_LEN];
    int count;
} LineBuffer;

typedef struct {
    float soc_percent;      // State of Charge (%)
    float current_a;        // Current in Amperes
    float voltage_v;        // Voltage in Volts
    bool valid;
    uint32_t timestamp;
} EBoxData;

typedef struct {
    double pcc;            // Grid power in W (from JSON in kW)
    double bat1;           // Battery power in W (from JSON in kW)
    double soc_bat1;       // SOC in %
    bool valid;
    uint32_t timestamp;
} InverterData;

typedef struct {
    int current_state;
    int stable_count;
    bool deep_discharge_protection;
    double last_excess;
} ControllerState;

// ============================================================================
// Global Variables
// ============================================================================

static mqtt_client_t *mqtt_client = NULL;
static bool mqtt_connected = false;
static bool wifi_connected = false;
static EBoxData ebox_data = {0};
static InverterData inverter_data = {0};
static ControllerState controller = {.current_state = 0, .stable_count = 0, .deep_discharge_protection = false};

// ============================================================================
// Function Declarations
// ============================================================================

static void wifi_init(void);
static void mqtt_init(void);
static void uart_init_ebox(void);
static void gpio_relays_init(void);
static void set_relay_state(int state);
static bool uart_read_line(char *buffer, int max_len, uint32_t timeout_ms);
static void read_ebox_data(EBoxData *data);
static bool parse_ebox_data(const LineBuffer *buffer, EBoxData *data);
static int get_state_power(int state);
static int find_best_state(int budget);
static int get_next_state_up(int current_state);
static void control_loop(void);
static bool is_numeric(const char *str);

// ============================================================================
// Relay Control
// ============================================================================

static void gpio_relays_init(void) {
    gpio_init(RELAY_PIN_0);
    gpio_init(RELAY_PIN_1);
    gpio_init(RELAY_PIN_2);
    gpio_set_dir(RELAY_PIN_0, GPIO_OUT);
    gpio_set_dir(RELAY_PIN_1, GPIO_OUT);
    gpio_set_dir(RELAY_PIN_2, GPIO_OUT);
    set_relay_state(0);
    printf("Relays: Initialized on GPIO %d, %d, %d\n", RELAY_PIN_0, RELAY_PIN_1, RELAY_PIN_2);
}

static void set_relay_state(int state) {
    if (state < 0) state = 0;
    if (state > 7) state = 7;

    gpio_put(RELAY_PIN_0, state & 0x01);
    gpio_put(RELAY_PIN_1, state & 0x02);
    gpio_put(RELAY_PIN_2, state & 0x04);

    controller.current_state = state;
    printf("Relays: State=%d (%dW)\n", state, STATE_POWER[state]);

    if (mqtt_connected && mqtt_client) {
        char payload[32];
        snprintf(payload, sizeof(payload), "%d", state);
        mqtt_publish(mqtt_client, MQTT_TOPIC_STATE_PUB, payload, strlen(payload), 0, 0, NULL, NULL);
    }
}

static int get_state_power(int state) {
    if (state < 0 || state > 7) return 0;
    return STATE_POWER[state];
}

static int find_best_state(int budget) {
    int best_state = 0;
    int best_power = 0;
    for (int i = 0; i < 8; i++) {
        if (STATE_POWER[i] <= budget && STATE_POWER[i] > best_power) {
            best_state = i;
            best_power = STATE_POWER[i];
        }
    }
    return best_state;
}

static int get_next_state_up(int current_state) {
    int current_power = get_state_power(current_state);
    int next_state = current_state;
    int next_power = current_power;

    for (int i = 0; i < 8; i++) {
        int p = STATE_POWER[i];
        if (p > current_power && (p < next_power || next_power == current_power)) {
            next_state = i;
            next_power = p;
        }
    }
    return next_state;
}

// ============================================================================
// WiFi & MQTT
// ============================================================================

static void wifi_init(void) {
    printf("WiFi: Connecting to %s...\n", WIFI_SSID);
    if (cyw43_arch_init()) {
        printf("WiFi: Failed to initialize\n");
        return;
    }
    cyw43_arch_enable_sta_mode();

    int retry = 0;
    while (retry < 10) {
        printf("WiFi: Attempt %d/10\n", retry + 1);
        if (cyw43_arch_wifi_connect_timeout_ms(WIFI_SSID, WIFI_PASSWORD,
                                                CYW43_AUTH_WPA2_AES_PSK, 30000) == 0) {
            printf("WiFi: Connected!\n");
            wifi_connected = true;
            return;
        }
        retry++;
        sleep_ms(1000);
    }
    printf("WiFi: Failed\n");
}

// MQTT incoming data callback
static void mqtt_incoming_data_cb(void *arg, const u8_t *data, u16_t len, u8_t flags) {
    char payload[512];
    if (len >= sizeof(payload)) return;

    memcpy(payload, data, len);
    payload[len] = '\0';

    // Parse JSON manually: {"ActivePower_PCC_Total":0.25,"Power_Bat1":-0.15,"SOC_Bat1":85.5}
    char *pcc_start = strstr(payload, "\"ActivePower_PCC_Total\":");
    char *bat_start = strstr(payload, "\"Power_Bat1\":");
    char *soc_start = strstr(payload, "\"SOC_Bat1\":");

    if (pcc_start && bat_start && soc_start) {
        double pcc_kw = atof(pcc_start + 24);
        double bat_kw = atof(bat_start + 13);
        double soc = atof(soc_start + 11);

        inverter_data.pcc = pcc_kw * 1000.0;   // kW to W
        inverter_data.bat1 = bat_kw * 1000.0;  // kW to W
        inverter_data.soc_bat1 = soc;
        inverter_data.valid = true;
        inverter_data.timestamp = to_ms_since_boot(get_absolute_time());

        printf("MQTT: PCC=%.0fW, Bat1=%.0fW, SOC=%.1f%%\n",
               inverter_data.pcc, inverter_data.bat1, inverter_data.soc_bat1);
    }
}

static void mqtt_connection_cb(mqtt_client_t *client, void *arg, mqtt_connection_status_t status) {
    if (status == MQTT_CONNECT_ACCEPTED) {
        printf("MQTT: Connected\n");
        mqtt_connected = true;
        mqtt_subscribe(client, MQTT_TOPIC_INVERTER_JSON, 0, NULL, (void*)MQTT_TOPIC_INVERTER_JSON);
        printf("MQTT: Subscribed to %s\n", MQTT_TOPIC_INVERTER_JSON);

        const char *msg = "Fox2DB full controller online";
        mqtt_publish(client, MQTT_TOPIC_STATUS, msg, strlen(msg), 0, 0, NULL, NULL);
    } else {
        printf("MQTT: Connection failed, status: %d\n", status);
        mqtt_connected = false;
    }
}

static void mqtt_init(void) {
    if (!wifi_connected) return;
    printf("MQTT: Initializing...\n");

    mqtt_client = mqtt_client_new();
    if (!mqtt_client) return;

    mqtt_set_inpub_callback(mqtt_client, NULL, mqtt_incoming_data_cb, NULL);

    ip_addr_t broker_ip;
    if (!ip4addr_aton(MQTT_BROKER, &broker_ip)) return;

    struct mqtt_connect_client_info_t ci = {0};
    ci.client_id = MQTT_CLIENT_ID;

    printf("MQTT: Connecting to %s:%d\n", MQTT_BROKER, MQTT_PORT);
    mqtt_client_connect(mqtt_client, &broker_ip, MQTT_PORT, mqtt_connection_cb, NULL, &ci);
}

// ============================================================================
// EBox Reading (from previous ebox_pico.c)
// ============================================================================

static void uart_init_ebox(void) {
    printf("UART: Initializing EBox on GPIO %d/%d\n", UART_TX_PIN, UART_RX_PIN);
    uart_init(UART_EBOX, UART_BAUD);
    gpio_set_function(UART_TX_PIN, GPIO_FUNC_UART);
    gpio_set_function(UART_RX_PIN, GPIO_FUNC_UART);
    uart_set_format(UART_EBOX, 8, 1, UART_PARITY_NONE);
    uart_set_fifo_enabled(UART_EBOX, true);
    printf("UART: Initialized at %d baud\n", UART_BAUD);
}

static bool uart_read_line(char *buffer, int max_len, uint32_t timeout_ms) {
    uint32_t start = to_ms_since_boot(get_absolute_time());
    int pos = 0;

    while (pos < max_len - 1) {
        if (to_ms_since_boot(get_absolute_time()) - start > timeout_ms) {
            buffer[pos] = '\0';
            return pos > 0;
        }
        if (uart_is_readable(UART_EBOX)) {
            char c = uart_getc(UART_EBOX);
            buffer[pos++] = c;
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

static bool is_numeric(const char *str) {
    if (!str || !*str) return false;
    char *endptr;
    strtod(str, &endptr);
    return *endptr == '\0' || *endptr == '\r' || *endptr == '\n' || *endptr == '%';
}

static bool parse_ebox_data(const LineBuffer *buffer, EBoxData *data) {
    data->valid = false;
    float total_voltage = 0.0f;
    float total_current = 0.0f;
    float min_soc = 100.0f;
    int battery_count = 0;

    for (int i = 0; i < buffer->count; i++) {
        const char *line = buffer->lines[i];
        if (strlen(line) == 0 || strstr(line, "$") || strstr(line, "#")) continue;
        if (line[0] < '1' || line[0] > '9') continue;

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

        if (field_count < 3) continue;
        if (!is_numeric(fields[1]) || !is_numeric(fields[2])) continue;
        if (field_count > 8 && strcmp(fields[8], "Absent") == 0) continue;

        float voltage_mv = atof(fields[1]);
        float current_ma = atof(fields[2]);
        float soc = 0.0f;

        for (int j = 3; j < field_count; j++) {
            if (strchr(fields[j], '%')) {
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
        if (soc < min_soc) min_soc = soc;
        battery_count++;
    }

    if (battery_count > 0) {
        data->voltage_v = total_voltage / 1000.0f;
        data->current_a = total_current / 1000.0f;
        data->soc_percent = min_soc;
        data->valid = true;
        data->timestamp = to_ms_since_boot(get_absolute_time());
        printf("EBox: %.2fV, %.2fA, SOC=%.1f%%\n", data->voltage_v, data->current_a, data->soc_percent);
        return true;
    }
    return false;
}

static void read_ebox_data(EBoxData *data) {
    LineBuffer buffer;
    buffer.count = 0;

    // Flush RX
    while (uart_is_readable(UART_EBOX)) uart_getc(UART_EBOX);

    // Send command
    char cmd[64];
    snprintf(cmd, sizeof(cmd), "%s\n", EBOX_COMMAND);
    printf("EBox: Sending '%s'", cmd);
    uart_puts(UART_EBOX, cmd);

    // Read response
    char end_marker[] = "\r$$\r\n";
    bool done = false;
    int retry = 0;

    while (!done && buffer.count < EBOX_MAX_LINES && retry < 3) {
        char line[EBOX_MAX_LINE_LEN];
        if (uart_read_line(line, sizeof(line), EBOX_TIMEOUT_MS)) {
            if (buffer.count < EBOX_MAX_LINES) {
                strncpy(buffer.lines[buffer.count], line, EBOX_MAX_LINE_LEN - 1);
                buffer.lines[buffer.count][EBOX_MAX_LINE_LEN - 1] = '\0';
                buffer.count++;
            }
            if (strcmp(line, end_marker) == 0) done = true;
        } else {
            retry++;
            if (retry < 3) {
                printf("EBox: Timeout, retry %d/3\n", retry);
                uart_puts(UART_EBOX, cmd);
                sleep_ms(100);
            }
        }
    }

    printf("EBox: Received %d lines\n", buffer.count);
    parse_ebox_data(&buffer, data);
}

// ============================================================================
// Main Control Logic (from fox2db.c fb_controller)
// ============================================================================

static void control_loop(void) {
    // Check data validity
    uint32_t now = to_ms_since_boot(get_absolute_time());
    bool ebox_valid = ebox_data.valid && (now - ebox_data.timestamp < 120000);  // 2 min timeout
    bool inv_valid = inverter_data.valid && (now - inverter_data.timestamp < 60000);  // 1 min timeout

    if (!ebox_valid || !inv_valid) {
        printf("Control: Data invalid (EBox=%d, Inv=%d)\n", ebox_valid, inv_valid);
        return;
    }

    double soc = ebox_data.soc_percent;
    double pcc = inverter_data.pcc;
    double bat1 = inverter_data.bat1;
    double bat_cur = ebox_data.current_a;
    int relay_st = controller.current_state;

    // Calculate EBox efficiency and excess
    double ebox_eff = (relay_st > 0) ? fmax(bat_cur * 2 * 53 + 1, get_state_power(relay_st)) : 0;
    double excess = pcc + ebox_eff + bat1;

    printf("Control: SOC=%.1f%%, PCC=%.0f, Bat1=%.0f, BatCur=%.2f, EBoxEff=%.0f, Excess=%.0f\n",
           soc, pcc, bat1, bat_cur, ebox_eff, excess);

    int new_state = relay_st;
    char reason[256] = "";

    // EMERGENCY CHARGE with target
    if (controller.deep_discharge_protection) {
        if (soc < DEEP_DISCHARGE_TARGET) {
            new_state = 1;
            snprintf(reason, sizeof(reason), "EMERGENCY_CHARGE_TO_%d%% (current %.1f%%)",
                    DEEP_DISCHARGE_TARGET, soc);
        } else {
            new_state = 0;
            controller.deep_discharge_protection = false;
            snprintf(reason, sizeof(reason), "CHARGE_TARGET_REACHED (%.1f%% >= %d%%)",
                    soc, DEEP_DISCHARGE_TARGET);
        }
    }
    // CRITICAL SOC
    else if (soc < DEEP_DISCHARGE_LOWER) {
        new_state = 1;
        controller.deep_discharge_protection = true;
        snprintf(reason, sizeof(reason), "CRITICAL_SOC_PROTECTION (%.1f%% < %d%%)",
                soc, DEEP_DISCHARGE_LOWER);
    }
    // BATTERY FULL
    else if (soc >= MAX_SOC) {
        new_state = 0;
        snprintf(reason, sizeof(reason), "BATTERY_FULL (%.1f%% >= %d%%)", soc, MAX_SOC);
    }
    // INSUFFICIENT EXCESS
    else if (excess < MIN_EXCESS) {
        new_state = 0;
        snprintf(reason, sizeof(reason), "INSUFFICIENT_EXCESS (%.0fW < %dW)", excess, MIN_EXCESS);
    }
    // POWER MATCHING
    else {
        int budget = (int)(excess + MAX_GRID_DRAW);
        int best = find_best_state(budget);
        snprintf(reason, sizeof(reason), "POWER_MATCHING (Excess: %.0fW, Budget: %dW, Best: %d)",
                excess, budget, best);

        // Ramp limiting (prevent jumping multiple states)
        if (best > relay_st) {
            int next_st = get_next_state_up(relay_st);
            if (best > next_st) {
                char tmp[64];
                snprintf(tmp, sizeof(tmp), " | RAMP_LIMITED (%d->%d)", best, next_st);
                strcat(reason, tmp);
                best = next_st;
            }
        }
        new_state = best;
    }

    // STABILIZATION (prevent flapping)
    if (new_state != relay_st) {
        controller.stable_count++;
        if (controller.stable_count >= STABILIZATION_CYCLES) {
            printf("Control: State change %d -> %d (%s)\n", relay_st, new_state, reason);
            set_relay_state(new_state);
            controller.stable_count = 0;

            if (mqtt_connected && mqtt_client) {
                char status[512];
                snprintf(status, sizeof(status), "State %d->%d: %s", relay_st, new_state, reason);
                mqtt_publish(mqtt_client, MQTT_TOPIC_STATUS, status, strlen(status), 0, 0, NULL, NULL);
            }
        } else {
            printf("Control: Stabilizing... (%d/%d)\n", controller.stable_count, STABILIZATION_CYCLES);
        }
    } else {
        controller.stable_count = 0;
    }

    controller.last_excess = excess;
    printf("Control: State=%d, Reason=%s\n\n", relay_st, reason);
}

// ============================================================================
// Main
// ============================================================================

int main() {
    stdio_init_all();
    sleep_ms(2000);

    printf("\n===========================================\n");
    printf("  Fox2DB Full Power Management - Pico W\n");
    printf("  Version: %s\n", VERSION);
    printf("===========================================\n\n");

    uart_init_ebox();
    gpio_relays_init();
    wifi_init();
    if (wifi_connected) mqtt_init();

    printf("\nStarting main loop...\n\n");

    uint32_t last_ebox_read = 0;
    uint32_t last_control = 0;

    while (true) {
        uint32_t now = to_ms_since_boot(get_absolute_time());

        // LED blink
        if (!wifi_connected) {
            cyw43_arch_gpio_put(CYW43_WL_GPIO_LED_PIN, (now / 250) % 2);
        } else if (!mqtt_connected) {
            cyw43_arch_gpio_put(CYW43_WL_GPIO_LED_PIN, (now / 100) % 2);
        } else {
            cyw43_arch_gpio_put(CYW43_WL_GPIO_LED_PIN, (now / 1000) % 2);
        }

        // Read EBox every 60s
        if (now - last_ebox_read >= EBOX_READ_INTERVAL_MS) {
            printf("\n=== Reading EBox ===\n");
            read_ebox_data(&ebox_data);
            last_ebox_read = now;
        }

        // Control loop every 5s
        if (now - last_control >= CONTROL_INTERVAL_MS) {
            if (mqtt_connected) {
                control_loop();
            }
            last_control = now;
        }

        sleep_ms(100);
    }

    return 0;
}
