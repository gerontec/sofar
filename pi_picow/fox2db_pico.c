/*
 * fox2db_pico.c - Power Management Controller for Raspberry Pi Pico W
 *
 * Receives MQTT data and controls relay states for optimal power usage
 * Port of fox2db.c to Pico W
 *
 * Hardware Requirements:
 * - Raspberry Pi Pico W
 * - 3x GPIO outputs for relay control (can control states 0-7)
 * - WiFi network
 * - MQTT broker with inverter data
 *
 * Build:
 *   mkdir build && cd build
 *   cmake ..
 *   make
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include "pico/stdlib.h"
#include "pico/cyw43_arch.h"
#include "hardware/gpio.h"
#include "lwip/apps/mqtt.h"
#include "lwip/ip_addr.h"

// Configuration
#define WIFI_SSID "YOUR_WIFI_SSID"
#define WIFI_PASSWORD "YOUR_WIFI_PASSWORD"
#define MQTT_BROKER "192.168.178.218"
#define MQTT_PORT 1883
#define MQTT_CLIENT_ID "pico_fox2db"

// MQTT Topics to Subscribe
#define MQTT_TOPIC_PCC "inverter/pcc"          // Grid exchange power
#define MQTT_TOPIC_BAT1 "inverter/bat1"        // Battery power
#define MQTT_TOPIC_SOC "inverter/soc"          // State of charge
#define MQTT_TOPIC_BAT_CUR "inverter/bat_cur"  // Battery current

// MQTT Topics to Publish
#define MQTT_TOPIC_STATE "fox2db/state"        // Current relay state
#define MQTT_TOPIC_STATUS "fox2db/status"      // Status messages

// GPIO Pins for Relay Control (3 pins = 8 states: 0-7)
#define RELAY_PIN_0 2  // LSB
#define RELAY_PIN_1 3
#define RELAY_PIN_2 4  // MSB

// Configuration Parameters
#define MIN_EXCESS 1010         // Minimum excess power to start charging (W)
#define MAX_GRID_DRAW 1500      // Maximum allowed grid draw (W)
#define MAX_SOC 99              // Maximum SOC (%)
#define HYSTERESIS 505          // Hysteresis power (W)
#define STABILIZATION_CYCLES 2  // Cycles before state change
#define DEEP_DISCHARGE_LOWER 6  // Deep discharge lower limit (%)
#define DEEP_DISCHARGE_UPPER 8  // Deep discharge upper limit (%)
#define DEEP_DISCHARGE_TARGET 7 // Charge target during protection (%)

// State to Power Mapping (Watts)
static const int STATE_POWER[8] = {
    0,      // State 0: Off
    3000,   // State 1
    3650,   // State 2
    6650,   // State 3
    3900,   // State 4
    7100,   // State 5
    7800,   // State 6
    11400   // State 7
};

// Inverter data structure
typedef struct {
    double pcc;         // Grid exchange power (W) + = export, - = import
    double bat1;        // Battery power (W)
    double soc;         // State of charge (%)
    double bat_cur;     // Battery current (A)

    uint32_t pcc_time;
    uint32_t bat1_time;
    uint32_t soc_time;
    uint32_t bat_cur_time;

    bool pcc_valid;
    bool bat1_valid;
    bool soc_valid;
    bool bat_cur_valid;
} InverterData;

// Controller state
typedef struct {
    int current_state;
    int stable_cycles;
    bool deep_discharge_protection;
} ControllerState;

// Global variables
static mqtt_client_t *mqtt_client = NULL;
static bool mqtt_connected = false;
static bool wifi_connected = false;
static InverterData inverter_data = {0};
static ControllerState controller = {0};

// Function declarations
static void wifi_init(void);
static void mqtt_init(void);
static void gpio_relays_init(void);
static void set_relay_state(int state);
static int get_state_power(int state);
static int find_best_state(int budget);
static void control_loop(void);

// Initialize GPIO for relay control
static void gpio_relays_init(void) {
    gpio_init(RELAY_PIN_0);
    gpio_init(RELAY_PIN_1);
    gpio_init(RELAY_PIN_2);

    gpio_set_dir(RELAY_PIN_0, GPIO_OUT);
    gpio_set_dir(RELAY_PIN_1, GPIO_OUT);
    gpio_set_dir(RELAY_PIN_2, GPIO_OUT);

    // Start with state 0 (all off)
    set_relay_state(0);

    printf("Relays: Initialized on GPIO %d, %d, %d\n",
           RELAY_PIN_0, RELAY_PIN_1, RELAY_PIN_2);
}

// Set relay state (0-7) using 3 GPIO pins
static void set_relay_state(int state) {
    if (state < 0) state = 0;
    if (state > 7) state = 7;

    gpio_put(RELAY_PIN_0, state & 0x01);  // Bit 0
    gpio_put(RELAY_PIN_1, state & 0x02);  // Bit 1
    gpio_put(RELAY_PIN_2, state & 0x04);  // Bit 2

    controller.current_state = state;

    printf("Relays: State set to %d (%d W)\n", state, STATE_POWER[state]);

    // Publish state to MQTT
    if (mqtt_connected && mqtt_client) {
        char payload[32];
        snprintf(payload, sizeof(payload), "%d", state);
        mqtt_publish(mqtt_client, MQTT_TOPIC_STATE, payload, strlen(payload),
                    0, 0, NULL, NULL);
    }
}

// Get power for a given state
static int get_state_power(int state) {
    if (state < 0 || state > 7) return 0;
    return STATE_POWER[state];
}

// Find best state that fits within budget
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

// Main control logic
static void control_loop(void) {
    // Check data validity (require all data)
    if (!inverter_data.pcc_valid || !inverter_data.bat1_valid ||
        !inverter_data.soc_valid || !inverter_data.bat_cur_valid) {
        printf("Control: Incomplete data, skipping\n");
        return;
    }

    double soc = inverter_data.soc;
    double pcc = inverter_data.pcc;
    double bat1 = inverter_data.bat1;
    double bat_cur = inverter_data.bat_cur;
    int relay_st = controller.current_state;

    // Calculate EBox efficiency and real excess
    double ebox_eff = (relay_st > 0) ?
                      fmax(bat_cur * 2 * 53 + 1, get_state_power(relay_st)) : 0;
    double excess = pcc + ebox_eff + bat1;

    printf("Control: SOC=%.1f%%, PCC=%.0fW, Bat1=%.0fW, BatCur=%.2fA, "
           "EBoxEff=%.0fW, Excess=%.0fW\n",
           soc, pcc, bat1, bat_cur, ebox_eff, excess);

    int new_state = relay_st;
    char reason[128] = "";

    // === Deep Discharge Protection ===
    if (controller.deep_discharge_protection) {
        // Protection active - charge to target
        if (soc < DEEP_DISCHARGE_TARGET) {
            new_state = 1;  // Minimum charging state
            snprintf(reason, sizeof(reason),
                    "EMERGENCY_CHARGE_TO_%d%% (current %.1f%%)",
                    DEEP_DISCHARGE_TARGET, soc);
        } else {
            new_state = 0;
            controller.deep_discharge_protection = false;
            snprintf(reason, sizeof(reason),
                    "CHARGE_TARGET_REACHED (%.1f%% >= %d%%)",
                    soc, DEEP_DISCHARGE_TARGET);
        }
    }
    // === Critical SOC - Activate Protection ===
    else if (soc < DEEP_DISCHARGE_LOWER) {
        new_state = 1;
        controller.deep_discharge_protection = true;
        snprintf(reason, sizeof(reason),
                "CRITICAL_SOC_PROTECTION (%.1f%% < %d%%)",
                soc, DEEP_DISCHARGE_LOWER);
    }
    // === Battery Full ===
    else if (soc >= MAX_SOC) {
        new_state = 0;
        snprintf(reason, sizeof(reason), "BATTERY_FULL_STOP");
    }
    // === Insufficient Excess ===
    else if (excess < MIN_EXCESS) {
        new_state = 0;
        snprintf(reason, sizeof(reason),
                "INSUFFICIENT_EXCESS (%.0fW < %dW)", excess, MIN_EXCESS);
    }
    // === Power Matching ===
    else {
        int budget = (int)(excess + MAX_GRID_DRAW);
        new_state = find_best_state(budget);
        snprintf(reason, sizeof(reason),
                "POWER_MATCHING (Excess: %.0fW, Budget: %dW, Best: %d)",
                excess, budget, new_state);
    }

    // === Stabilization Logic ===
    if (new_state != relay_st) {
        controller.stable_cycles++;
        if (controller.stable_cycles >= STABILIZATION_CYCLES) {
            printf("Control: State change %d -> %d (%s)\n",
                   relay_st, new_state, reason);
            set_relay_state(new_state);
            controller.stable_cycles = 0;

            // Publish status
            if (mqtt_connected && mqtt_client) {
                char status[256];
                snprintf(status, sizeof(status),
                        "State %d -> %d: %s", relay_st, new_state, reason);
                mqtt_publish(mqtt_client, MQTT_TOPIC_STATUS, status, strlen(status),
                            0, 0, NULL, NULL);
            }
        } else {
            printf("Control: Stabilizing... (%d/%d cycles)\n",
                   controller.stable_cycles, STABILIZATION_CYCLES);
        }
    } else {
        controller.stable_cycles = 0;
    }

    printf("Control: Current state: %d, Reason: %s\n\n", relay_st, reason);
}

// WiFi initialization
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

// MQTT incoming data callback
static void mqtt_incoming_data_cb(void *arg, const u8_t *data, u16_t len, u8_t flags) {
    const char *topic = (const char *)arg;
    char payload[64];

    if (len >= sizeof(payload)) {
        printf("MQTT: Payload too large\n");
        return;
    }

    memcpy(payload, data, len);
    payload[len] = '\0';

    uint32_t now = to_ms_since_boot(get_absolute_time());
    double value = atof(payload);

    if (strstr(topic, "pcc")) {
        inverter_data.pcc = value;
        inverter_data.pcc_time = now;
        inverter_data.pcc_valid = true;
        printf("MQTT: PCC = %.0f W\n", value);
    }
    else if (strstr(topic, "bat1")) {
        inverter_data.bat1 = value;
        inverter_data.bat1_time = now;
        inverter_data.bat1_valid = true;
        printf("MQTT: Bat1 = %.0f W\n", value);
    }
    else if (strstr(topic, "soc")) {
        inverter_data.soc = value;
        inverter_data.soc_time = now;
        inverter_data.soc_valid = true;
        printf("MQTT: SOC = %.1f %%\n", value);
    }
    else if (strstr(topic, "bat_cur")) {
        inverter_data.bat_cur = value;
        inverter_data.bat_cur_time = now;
        inverter_data.bat_cur_valid = true;
        printf("MQTT: BatCur = %.2f A\n", value);
    }
}

// MQTT connection callback
static void mqtt_connection_cb(mqtt_client_t *client, void *arg, mqtt_connection_status_t status) {
    if (status == MQTT_CONNECT_ACCEPTED) {
        printf("MQTT: Connected to broker\n");
        mqtt_connected = true;

        // Subscribe to all topics
        mqtt_subscribe(client, MQTT_TOPIC_PCC, 0, NULL, (void*)MQTT_TOPIC_PCC);
        mqtt_subscribe(client, MQTT_TOPIC_BAT1, 0, NULL, (void*)MQTT_TOPIC_BAT1);
        mqtt_subscribe(client, MQTT_TOPIC_SOC, 0, NULL, (void*)MQTT_TOPIC_SOC);
        mqtt_subscribe(client, MQTT_TOPIC_BAT_CUR, 0, NULL, (void*)MQTT_TOPIC_BAT_CUR);

        printf("MQTT: Subscribed to all topics\n");

        // Publish online status
        const char *msg = "Fox2DB controller online";
        mqtt_publish(client, MQTT_TOPIC_STATUS, msg, strlen(msg), 0, 0, NULL, NULL);
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

    mqtt_set_inpub_callback(mqtt_client, NULL, mqtt_incoming_data_cb, NULL);

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

// Main function
int main() {
    stdio_init_all();
    sleep_ms(2000);

    printf("\n");
    printf("===========================================\n");
    printf("  Fox2DB Power Management for Pico W\n");
    printf("  Version: 1.0.0-pico\n");
    printf("===========================================\n\n");

    // Initialize hardware
    gpio_relays_init();
    wifi_init();

    if (wifi_connected) {
        mqtt_init();
    }

    printf("\nStarting control loop...\n\n");

    // Main loop
    uint32_t last_update = 0;
    const uint32_t update_interval_ms = 5000;  // Run control every 5 seconds

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

        // Control loop
        if (now - last_update >= update_interval_ms) {
            if (mqtt_connected) {
                control_loop();
            }
            last_update = now;
        }

        sleep_ms(100);
    }

    return 0;
}
