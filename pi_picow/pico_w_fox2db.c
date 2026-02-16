/**
 * Pico W fox2db - Complete Standalone Battery Management
 *
 * Replaces: Raspberry Pi + fox2db.py/c + MQTT + Scripts
 * Result: Single Pico W controlling everything autonomously!
 *
 * Architecture:
 * - WiFi → MQTT (subscribe: inverter data, publish: status)
 * - Core 0: MQTT + EBox + SOYO + fox2db Logic
 * - Core 1: Relay Control
 *
 * fox2db Logic:
 * 1. Read MQTT (PCC, Bat1, SOC_Bat1)
 * 2. Read EBox (SOC, bat_cur)
 * 3. Calculate best relay state (fb_controller)
 * 4. Check blocking rules (stabilization, sweet spot, etc.)
 * 5. Execute: Update SOYO + Relay
 *
 * MQTT Topics:
 *   Subscribe:
 *     - inverter/power_grid_exchange/json (PCC, Bat1, SOC)
 *   Publish:
 *     - pico/status (complete status JSON)
 *     - pico/relay/state (relay state)
 *     - pico/soyo/watts (SOYO watts)
 *
 * Cycle: 30 seconds (like original fox2db)
 */

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <ctype.h>

#include "pico/stdlib.h"
#include "pico/multicore.h"
#include "pico/mutex.h"
#include "pico/cyw43_arch.h"
#include "hardware/uart.h"
#include "hardware/gpio.h"
#include "hardware/timer.h"

#include "lwip/apps/mqtt.h"
#include "lwip/ip_addr.h"

// ============================================================================
// CONFIGURATION
// ============================================================================

#define WIFI_SSID       "YourWiFiSSID"
#define WIFI_PASSWORD   "YourWiFiPassword"
#define MQTT_BROKER_IP  "192.168.1.100"
#define MQTT_BROKER_PORT 1883

#define MQTT_TOPIC_INVERTER "inverter/power_grid_exchange/json"

// Hardware pins (same as before)
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

#define EBOX_TX_PIN     8
#define EBOX_RX_PIN     9
#define EBOX_BAUD       115200

// Timing
#define FOX2DB_CYCLE_MS     30000  // Main cycle: 30 seconds
#define RELAY_CHECK_MS      100
#define STATUS_PUBLISH_MS   30000

// fox2db Parameters (from fox2db.c defaults)
#define MIN_EXCESS              1010
#define MAX_GRID_DRAW           1500
#define MAX_SOC                 99
#define HYSTERESIS              505
#define STABILIZATION_CYCLES    2
#define EMERGENCY_IMPORT        1020
#define SWEET_SPOT_PCC          160
#define SWEET_SPOT_BAT          -310
#define MAX_DROP_RATE           -20
#define DEEP_DISCHARGE_LOWER    6
#define DEEP_DISCHARGE_UPPER    8
#define DEEP_DISCHARGE_TARGET   7

// ============================================================================
// STATE POWER MAPPING (from fox2db.c)
// ============================================================================

static const int STATE_POWER[] = {
    0,      // State 0
    3000,   // State 1
    3650,   // State 2
    6650,   // State 3
    3900,   // State 4
    7100,   // State 5
    7800,   // State 6
    11400   // State 7
};
#define NUM_STATES 8

static const uint8_t STATE_MAP_RELAY[] = {0, 1, 2, 3, 4, 5, 6, 7};

// ============================================================================
// PUBLIC RAM - Complete System State
// ============================================================================

typedef struct {
    // MQTT Input (from inverter)
    volatile double   pcc;              // Grid power (+ = export, - = import)
    volatile double   bat1;             // Battery power
    volatile double   soc_bat1;         // Battery 1 SOC
    volatile bool     mqtt_valid;       // MQTT data received

    // EBox Input
    volatile double   bat_cur;          // EBox current (Ampere)
    volatile double   soc_ebox;         // EBox SOC
    volatile int      ebox_batteries;   // Number of batteries
    volatile bool     ebox_valid;       // EBox data received

    // fox2db State
    volatile int      relay_state;      // Current relay state (0-7)
    volatile int      best_state;       // Calculated best state
    volatile int      stable_cycles;    // Stabilization counter
    volatile int      deep_discharge_prot; // 0=off, 1=active
    volatile double   last_excess;      // For trend calculation
    volatile double   drop_rate;        // W/s
    volatile bool     drop_rate_valid;

    // SOYO Control
    volatile uint16_t soyo_watts;       // Actual SOYO watts
    volatile bool     soyo_enable;

    // Counters
    volatile uint32_t cycle_count;
    volatile uint8_t  error_flags;

    mutex_t mutex;
} SystemState;

static SystemState g_state = {
    .pcc = 0,
    .bat1 = 0,
    .soc_bat1 = 0,
    .mqtt_valid = false,
    .bat_cur = 0,
    .soc_ebox = 0,
    .ebox_batteries = 0,
    .ebox_valid = false,
    .relay_state = 0,
    .best_state = 0,
    .stable_cycles = 0,
    .deep_discharge_prot = 0,
    .last_excess = 0,
    .drop_rate = 0,
    .drop_rate_valid = false,
    .soyo_watts = 0,
    .soyo_enable = false,
    .cycle_count = 0,
    .error_flags = 0
};

#define ERR_MQTT_TIMEOUT    (1 << 0)
#define ERR_EBOX_TIMEOUT    (1 << 1)
#define ERR_SOYO_FAILED     (1 << 2)
#define ERR_RELAY_FAILED    (1 << 3)

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

static int get_state_power(int state) {
    if (state < 0 || state >= NUM_STATES) return 0;
    return STATE_POWER[state];
}

static int find_best_state(int budget) {
    int best = 0;
    for (int i = NUM_STATES - 1; i >= 0; i--) {
        if (STATE_POWER[i] <= budget) {
            best = i;
            break;
        }
    }
    return best;
}

static int get_next_state_up(int current) {
    if (current >= NUM_STATES - 1) return NUM_STATES - 1;
    return current + 1;
}

// ============================================================================
// MODBUS / CRC (abbreviated, reuse from pico_w_ebox_mqtt.c)
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
// SOYO INTERFACE
// ============================================================================

static bool send_soyo_command(uint16_t watts) {
    uint8_t cmd[8];
    cmd[0] = 1;      // Device ID
    cmd[1] = 0x06;   // Write Single Register
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

    // Read response (simplified)
    uint8_t resp[8];
    absolute_time_t timeout = make_timeout_time_ms(500);
    for (int i = 0; i < 8; i++) {
        while (!uart_is_readable(SOYO_UART)) {
            if (time_reached(timeout)) {
                g_state.error_flags |= ERR_SOYO_FAILED;
                return false;
            }
            sleep_us(100);
        }
        resp[i] = uart_getc(SOYO_UART);
    }

    g_state.error_flags &= ~ERR_SOYO_FAILED;
    return true;
}

// ============================================================================
// RELAY INTERFACE (delegate to Core 1)
// ============================================================================

static volatile int relay_target_state = 0;
static volatile bool relay_needs_update = false;

// State Map
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

static bool write_relay_status(uint8_t state) {
    // Find state in map
    const StateMap *map = NULL;
    for (size_t i = 0; i < STATE_MAP_SIZE; i++) {
        if (STATE_MAP[i].state == state) {
            map = &STATE_MAP[i];
            break;
        }
    }

    if (!map) {
        return false;  // Invalid state
    }

    // Build Modbus command (Write Multiple Coils)
    uint8_t cmd[10];
    cmd[0] = 1;      // Slave ID
    cmd[1] = 0x0F;   // Function: Write Multiple Coils
    cmd[2] = 0x00;   // Start address high
    cmd[3] = 0x00;   // Start address low
    cmd[4] = 0x00;   // Quantity high
    cmd[5] = 0x03;   // Quantity low (3 relays)
    cmd[6] = 0x01;   // Byte count

    // Pack relay states into one byte
    uint8_t relay_bits = (map->relay1 & 1) | ((map->relay2 & 1) << 1) | ((map->relay3 & 1) << 2);
    cmd[7] = relay_bits;

    uint16_t crc = crc16_modbus(cmd, 8);
    cmd[8] = crc & 0xFF;
    cmd[9] = (crc >> 8) & 0xFF;

    // Clear RX buffer
    while (uart_is_readable(RELAY_UART)) uart_getc(RELAY_UART);

    rs485_tx_mode(RELAY_DE_PIN);
    uart_write_blocking(RELAY_UART, cmd, 10);
    rs485_rx_mode(RELAY_DE_PIN);

    // Read response
    uint8_t resp[8];
    absolute_time_t timeout = make_timeout_time_ms(500);
    for (int i = 0; i < 8; i++) {
        while (!uart_is_readable(RELAY_UART)) {
            if (time_reached(timeout)) {
                g_state.error_flags |= ERR_RELAY_FAILED;
                return false;
            }
            sleep_us(100);
        }
        resp[i] = uart_getc(RELAY_UART);
    }

    g_state.error_flags &= ~ERR_RELAY_FAILED;
    return true;
}

static void core1_relay_handler(void) {
    int last_state = -1;

    while (true) {
        if (relay_needs_update) {
            int target = relay_target_state;

            if (target != last_state) {
                if (write_relay_status(target)) {
                    last_state = target;
                    printf("Relay: State changed to %d\n", target);
                }
            }

            relay_needs_update = false;
        }

        sleep_ms(RELAY_CHECK_MS);
    }
}

// ============================================================================
// EBOX INTERFACE (Software UART)
// ============================================================================

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

static void ebox_uart_puts(const char *str) {
    while (*str) {
        ebox_uart_putc(EBOX_TX_PIN, EBOX_BAUD, *str++);
    }
}

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

static int ebox_read_line(char *buf, size_t len) {
    size_t idx = 0;
    absolute_time_t start = get_absolute_time();
    uint timeout_ms = 1000;

    while (idx < len - 1) {
        uint remaining_ms = timeout_ms - (absolute_time_diff_us(start, get_absolute_time()) / 1000);
        if (remaining_ms <= 0) {
            break;
        }

        int c = ebox_uart_getc_timeout(EBOX_RX_PIN, EBOX_BAUD, remaining_ms);
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

static bool read_ebox(double *bat_cur, double *soc) {
    // Send "pwr\r\n"
    ebox_uart_puts("pwr\r\n");

    // Read response and parse SOC
    char line[256];
    int total_soc = 0;
    int battery_count = 0;
    int total_current = 0;

    for (int i = 0; i < 10; i++) {
        int len = ebox_read_line(line, sizeof(line));
        if (len <= 0) break;

        // Skip header line
        if (strstr(line, "Power") || strstr(line, "#") || strstr(line, "$")) {
            continue;
        }

        // Parse fields (space-separated)
        char *token = strtok(line, " \t");
        int field_idx = 0;
        int line_current = 0;

        while (token != NULL && field_idx < 20) {
            // Field 2 is current (if we want it)
            if (field_idx == 2) {
                line_current = atoi(token);
            }

            // Check if this field contains SOC (has %)
            int line_soc;
            if (parse_soc_from_field(token, &line_soc)) {
                total_soc += line_soc;
                total_current += line_current;
                battery_count++;
                break;  // Found SOC for this line, move to next line
            }

            token = strtok(NULL, " \t");
            field_idx++;
        }
    }

    if (battery_count > 0) {
        *soc = (double)total_soc / battery_count;
        *bat_cur = (double)total_current / battery_count / 1000.0;  // mA to A

        g_state.bat_cur = *bat_cur;
        g_state.soc_ebox = *soc;
        g_state.ebox_batteries = battery_count;
        g_state.ebox_valid = true;
        g_state.error_flags &= ~ERR_EBOX_TIMEOUT;

        return true;
    }

    g_state.error_flags |= ERR_EBOX_TIMEOUT;
    return false;
}

// ============================================================================
// FOX2DB LOGIC - fb_controller()
// ============================================================================

typedef struct {
    int best_state;
    double excess;
    char trace[512];
} ControllerResult;

static void fb_controller(double soc, double pcc, double bat_cur, double bat1,
                         int relay_st, int prot, ControllerResult *result) {
    // Calculate real excess
    double ebox_eff = (relay_st > 0) ? fmax(bat_cur * 2 * 53 + 1, get_state_power(relay_st)) : 0;
    double excess = pcc + ebox_eff + bat1;

    result->excess = excess;
    result->trace[0] = '\0';

    // EMERGENCY CHARGE with target
    if (prot) {
        if (soc < DEEP_DISCHARGE_TARGET) {
            result->best_state = 1;
            snprintf(result->trace, sizeof(result->trace),
                    "EMERGENCY_CHARGE_TO_%d%% (current %.1f%%)", DEEP_DISCHARGE_TARGET, soc);
            return;
        } else {
            result->best_state = 0;
            snprintf(result->trace, sizeof(result->trace),
                    "CHARGE_TARGET_REACHED (%.1f%% >= %d%%)", soc, DEEP_DISCHARGE_TARGET);
            return;
        }
    }

    // Critical SOC
    if (soc < DEEP_DISCHARGE_LOWER) {
        result->best_state = 1;
        snprintf(result->trace, sizeof(result->trace),
                "CRITICAL_SOC_PROTECTION (%.1f%% < %d%%)", soc, DEEP_DISCHARGE_LOWER);
        return;
    }

    // Battery full
    if (soc >= MAX_SOC) {
        result->best_state = 0;
        snprintf(result->trace, sizeof(result->trace), "BATTERY_FULL_STOP");
        return;
    }

    // Insufficient excess
    if (excess < MIN_EXCESS) {
        result->best_state = 0;
        snprintf(result->trace, sizeof(result->trace),
                "INSUFFICIENT_EXCESS (%.0fW)", excess);
        return;
    }

    // Power matching
    int budget = (int)(excess + MAX_GRID_DRAW);
    int best = find_best_state(budget);

    snprintf(result->trace, sizeof(result->trace),
            "POWER_MATCHING (Excess: %.0fW, Budget: %dW)", excess, budget);

    // Ramp limiting
    if (best > relay_st) {
        int next_st = get_next_state_up(relay_st);
        if (best > next_st) {
            char tmp[128];
            snprintf(tmp, sizeof(tmp), " | RAMP_LIMITED (%d->%d)", best, next_st);
            strcat(result->trace, tmp);
            best = next_st;
        }
    }

    result->best_state = best;
}

// ============================================================================
// BLOCKING RULES
// ============================================================================

typedef enum { DIR_UP, DIR_DOWN, DIR_BOTH } Direction;

typedef struct {
    const char *name;
    bool blocked;
    Direction applies_to;
    char reason[128];
} BlockingRule;

static bool check_blocking_rules(double pcc, double bat1, int stable,
                                 double drop_rate, Direction dir,
                                 char *trace, size_t trace_len) {
    BlockingRule rules[5];
    int num_rules = 0;

    // SWEET_SPOT_HOLD
    if (fabs(pcc) < SWEET_SPOT_PCC && bat1 > SWEET_SPOT_BAT && dir == DIR_UP) {
        BlockingRule *r = &rules[num_rules++];
        r->name = "SWEET_SPOT_HOLD";
        r->blocked = true;
        r->applies_to = DIR_UP;
        snprintf(r->reason, sizeof(r->reason), "PCC=%.0fW Bat1=%.0fW", pcc, bat1);
    }

    // STABILIZATION
    if (stable < STABILIZATION_CYCLES) {
        BlockingRule *r = &rules[num_rules++];
        r->name = "STABILIZATION";
        r->blocked = true;
        r->applies_to = DIR_BOTH;
        snprintf(r->reason, sizeof(r->reason), "%d/%d", stable, STABILIZATION_CYCLES);
    }

    // DROP_RATE_PROTECTION
    if (drop_rate < MAX_DROP_RATE && dir == DIR_UP) {
        BlockingRule *r = &rules[num_rules++];
        r->name = "DROP_RATE_PROTECTION";
        r->blocked = true;
        r->applies_to = DIR_UP;
        snprintf(r->reason, sizeof(r->reason), "%.1fW/s", drop_rate);
    }

    // Check if blocked
    for (int i = 0; i < num_rules; i++) {
        if (rules[i].blocked) {
            snprintf(trace, trace_len, " | %s (%s)", rules[i].name, rules[i].reason);
            return true;  // Blocked
        }
    }

    return false;  // Not blocked
}

// ============================================================================
// MAIN CYCLE (fox2db logic)
// ============================================================================

static bool fox2db_cycle(struct repeating_timer *t) {
    (void)t;

    printf("\n=== fox2db Cycle %lu ===\n", g_state.cycle_count);

    // 1. Check MQTT data validity
    if (!g_state.mqtt_valid) {
        printf("ERROR: No MQTT data - Emergency Shutdown!\n");
        relay_target_state = 0;
        relay_needs_update = true;
        send_soyo_command(0);
        g_state.error_flags |= ERR_MQTT_TIMEOUT;
        return true;
    }

    // 2. Read EBox
    double bat_cur, soc;
    if (!read_ebox(&bat_cur, &soc)) {
        printf("WARNING: EBox read failed\n");
        g_state.error_flags |= ERR_EBOX_TIMEOUT;
        // Continue with old data
        bat_cur = g_state.bat_cur;
        soc = g_state.soc_ebox;
    }

    // 3. Get current state
    mutex_enter_blocking(&g_state.mutex);
    double pcc = g_state.pcc;
    double bat1 = g_state.bat1;
    int relay_st = g_state.relay_state;
    int prot = g_state.deep_discharge_prot;
    int stable = g_state.stable_cycles;
    double last_excess = g_state.last_excess;
    mutex_exit(&g_state.mutex);

    // 4. Run fb_controller
    ControllerResult result;
    fb_controller(soc, pcc, bat_cur, bat1, relay_st, prot, &result);

    int best = result.best_state;
    double excess = result.excess;

    // 5. Calculate drop rate
    double drop_rate = 0;
    bool has_drop_rate = (last_excess > 0);
    if (has_drop_rate) {
        drop_rate = (excess - last_excess) / 30.0;  // W/s (30s cycle)
    }

    // 6. Check blocking rules
    int final = relay_st;
    bool changed = false;

    if (best != relay_st) {
        Direction dir = (best > relay_st) ? DIR_UP : DIR_DOWN;
        bool emergency = (pcc < -EMERGENCY_IMPORT) && (dir == DIR_DOWN);

        if (emergency) {
            printf("EMERGENCY: Force Shutdown %d->%d (Import=%.0fW!)\n", relay_st, best, pcc);
            final = best;
            changed = true;
        } else {
            char block_trace[256];
            bool blocked = check_blocking_rules(pcc, bat1, stable, drop_rate, dir,
                                               block_trace, sizeof(block_trace));

            if (!blocked) {
                final = best;
                changed = true;
            } else {
                strcat(result.trace, block_trace);
            }
        }
    }

    // 7. Update state
    mutex_enter_blocking(&g_state.mutex);
    g_state.best_state = best;
    g_state.relay_state = final;
    g_state.stable_cycles = changed ? 0 : stable + 1;
    g_state.last_excess = excess;
    g_state.drop_rate = drop_rate;
    g_state.drop_rate_valid = has_drop_rate;

    // Deep discharge protection
    if (soc < DEEP_DISCHARGE_LOWER) {
        if (!g_state.deep_discharge_prot) {
            printf("⚠️  DEEP_DISCHARGE_PROTECTION ACTIVATED at %.1f%%\n", soc);
        }
        g_state.deep_discharge_prot = 1;
    } else if (soc >= DEEP_DISCHARGE_UPPER) {
        if (g_state.deep_discharge_prot) {
            printf("✓ DEEP_DISCHARGE_PROTECTION DEACTIVATED at %.1f%%\n", soc);
        }
        g_state.deep_discharge_prot = 0;
    }

    g_state.cycle_count++;
    mutex_exit(&g_state.mutex);

    // 8. Log
    double ebox_w = bat_cur * 2 * 53;
    printf("Data: SOC=%.1f%% PCC=%.0fW Bat1=%.0fW EBox=%.0fW State=%d Stable=%d\n",
           soc, pcc, bat1, ebox_w, relay_st, stable);
    printf("Result: State %d (TRACE: %s)\n", final, result.trace);

    // 9. Execute
    if (changed) {
        relay_target_state = final;
        relay_needs_update = true;
    }

    // Calculate SOYO watts (State → Power)
    int soyo_watts = get_state_power(final);
    g_state.soyo_watts = soyo_watts;
    g_state.soyo_enable = (soyo_watts > 0);
    send_soyo_command(soyo_watts);

    return true;
}

// ============================================================================
// JSON PARSING (Simple string search)
// ============================================================================

static double extract_json_number(const char *json, const char *key) {
    char search[64];
    snprintf(search, sizeof(search), "\"%s\":", key);
    const char *pos = strstr(json, search);
    if (!pos) return 0;

    // Skip the key and colon, plus any whitespace
    pos += strlen(search);
    while (*pos == ' ' || *pos == '\t') pos++;

    return atof(pos);
}

// ============================================================================
// MQTT CALLBACKS
// ============================================================================

static char mqtt_payload_buffer[512];
static volatile bool mqtt_data_ready = false;

static void mqtt_incoming_publish_cb(void *arg, const char *topic, u32_t tot_len) {
    (void)arg;
    (void)tot_len;
    printf("MQTT: %s (%u bytes)\n", topic, tot_len);
}

static void mqtt_incoming_data_cb(void *arg, const u8_t *data, u16_t len, u8_t flags) {
    (void)arg;

    // Accumulate data (handles fragmented MQTT messages)
    static uint16_t offset = 0;

    if (flags & MQTT_DATA_FLAG_LAST) {
        // Last fragment - copy and parse
        if (offset + len < sizeof(mqtt_payload_buffer)) {
            memcpy(mqtt_payload_buffer + offset, data, len);
            mqtt_payload_buffer[offset + len] = '\0';

            // Parse JSON from inverter
            // Format: {"pcc": 1234, "bat1": -567, "soc_bat1": 89.5}
            double pcc = extract_json_number(mqtt_payload_buffer, "pcc");
            double bat1 = extract_json_number(mqtt_payload_buffer, "bat1");
            double soc_bat1 = extract_json_number(mqtt_payload_buffer, "soc_bat1");

            mutex_enter_blocking(&g_state.mutex);
            g_state.pcc = pcc;
            g_state.bat1 = bat1;
            g_state.soc_bat1 = soc_bat1;
            g_state.mqtt_valid = true;
            mutex_exit(&g_state.mutex);

            printf("MQTT Data: PCC=%.0fW Bat1=%.0fW SOC1=%.1f%%\n", pcc, bat1, soc_bat1);
        }
        offset = 0;
    } else {
        // Not last fragment - accumulate
        if (offset + len < sizeof(mqtt_payload_buffer)) {
            memcpy(mqtt_payload_buffer + offset, data, len);
            offset += len;
        }
    }
}

static mqtt_client_t *mqtt_client = NULL;

static void mqtt_connection_cb(mqtt_client_t *client, void *arg, mqtt_connection_status_t status) {
    (void)arg;

    if (status == MQTT_CONNECT_ACCEPTED) {
        printf("MQTT connected!\n");
        mqtt_subscribe(client, MQTT_TOPIC_INVERTER, 1, NULL, NULL);
    } else {
        printf("MQTT connection failed: %d\n", status);
    }
}

// ============================================================================
// MAIN
// ============================================================================

int main(void) {
    stdio_init_all();

    printf("\n=== Pico W fox2db - Autonomous Battery Controller ===\n");

    // Initialize WiFi
    if (cyw43_arch_init()) {
        printf("ERROR: WiFi init failed!\n");
        return 1;
    }

    cyw43_arch_enable_sta_mode();
    printf("Connecting to WiFi...\n");

    if (cyw43_arch_wifi_connect_timeout_ms(WIFI_SSID, WIFI_PASSWORD,
                                            CYW43_AUTH_WPA2_AES_PSK, 30000)) {
        printf("ERROR: WiFi failed!\n");
        return 1;
    }

    printf("WiFi connected!\n");

    // Initialize MQTT
    mqtt_client = mqtt_client_new();
    struct mqtt_connect_client_info_t ci;
    memset(&ci, 0, sizeof(ci));
    ci.client_id = "pico_fox2db";

    ip_addr_t broker_ip;
    ipaddr_aton(MQTT_BROKER_IP, &broker_ip);

    mqtt_set_inpub_callback(mqtt_client, mqtt_incoming_publish_cb,
                           mqtt_incoming_data_cb, NULL);
    mqtt_client_connect(mqtt_client, &broker_ip, MQTT_BROKER_PORT,
                       mqtt_connection_cb, NULL, &ci);

    // Initialize mutex
    mutex_init(&g_state.mutex);

    // Initialize Hardware UARTs
    uart_init(SOYO_UART, SOYO_BAUD);
    gpio_set_function(SOYO_UART_TX, GPIO_FUNC_UART);
    gpio_set_function(SOYO_UART_RX, GPIO_FUNC_UART);

    uart_init(RELAY_UART, RELAY_BAUD);
    gpio_set_function(RELAY_UART_TX, GPIO_FUNC_UART);
    gpio_set_function(RELAY_UART_RX, GPIO_FUNC_UART);

    gpio_init(SOYO_DE_PIN);
    gpio_set_dir(SOYO_DE_PIN, GPIO_OUT);
    gpio_put(SOYO_DE_PIN, 0);

    gpio_init(RELAY_DE_PIN);
    gpio_set_dir(RELAY_DE_PIN, GPIO_OUT);
    gpio_put(RELAY_DE_PIN, 0);

    // Initialize EBox UART (TODO: implement)
    gpio_init(EBOX_TX_PIN);
    gpio_set_dir(EBOX_TX_PIN, GPIO_OUT);
    gpio_put(EBOX_TX_PIN, 1);

    gpio_init(EBOX_RX_PIN);
    gpio_set_dir(EBOX_RX_PIN, GPIO_IN);
    gpio_pull_up(EBOX_RX_PIN);

    printf("Hardware initialized\n");

    // Launch Core 1
    multicore_launch_core1(core1_relay_handler);

    // Setup fox2db timer (30s cycle)
    struct repeating_timer fox2db_timer;
    add_repeating_timer_ms(FOX2DB_CYCLE_MS, fox2db_cycle, NULL, &fox2db_timer);

    printf("fox2db cycle started (30s)\n");
    printf("Ready! Autonomous battery management active.\n");

    // Main loop - MQTT status publishing
    absolute_time_t last_status = get_absolute_time();

    while (true) {
        cyw43_arch_poll();

        // Publish status every 30 seconds
        absolute_time_t now = get_absolute_time();
        if (absolute_time_diff_us(last_status, now) >= STATUS_PUBLISH_MS * 1000) {
            mutex_enter_blocking(&g_state.mutex);

            char status[512];
            snprintf(status, sizeof(status),
                     "{\"relay_state\":%d,\"soyo_watts\":%d,\"ebox_soc\":%.1f,"
                     "\"pcc\":%.0f,\"bat1\":%.0f,\"excess\":%.0f,\"drop_rate\":%.1f,"
                     "\"trace\":\"%s\",\"deep_discharge_prot\":%d,\"errors\":\"0x%02X\"}",
                     g_state.relay_state,
                     g_state.soyo_watts,
                     g_state.soc_ebox,
                     g_state.pcc,
                     g_state.bat1,
                     g_state.last_excess,
                     g_state.drop_rate,
                     "",  // trace would need to be saved
                     g_state.deep_discharge_prot,
                     g_state.error_flags);

            mutex_exit(&g_state.mutex);

            mqtt_publish(mqtt_client, "pico/status", status, strlen(status), 1, 0, NULL, NULL);

            // Also publish individual values
            char val_buf[32];
            snprintf(val_buf, sizeof(val_buf), "%d", g_state.relay_state);
            mqtt_publish(mqtt_client, "pico/relay/state", val_buf, strlen(val_buf), 1, 1, NULL, NULL);

            snprintf(val_buf, sizeof(val_buf), "%d", g_state.soyo_watts);
            mqtt_publish(mqtt_client, "pico/soyo/watts", val_buf, strlen(val_buf), 1, 1, NULL, NULL);

            snprintf(val_buf, sizeof(val_buf), "%.1f", g_state.soc_ebox);
            mqtt_publish(mqtt_client, "pico/ebox/soc", val_buf, strlen(val_buf), 1, 1, NULL, NULL);

            last_status = now;
        }

        sleep_ms(100);
    }

    return 0;
}
