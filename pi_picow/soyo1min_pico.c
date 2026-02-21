/*
 * soyo1min_pico.c - Soyo Inverter Battery Management for Raspberry Pi Pico W
 *
 * Receives inverter data via MQTT and controls Soyo inverter via RS485
 * Soyo inverter only receives commands, provides no feedback
 *
 * Hardware Requirements:
 * - Raspberry Pi Pico W
 * - RS485 to TTL converter module (with DE/RE pins) connected to UART1
 * - WiFi network
 * - MQTT broker with inverter data topics
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
#include <time.h>
#include "pico/stdlib.h"
#include "pico/cyw43_arch.h"
#include "pico/util/datetime.h"
#include "hardware/uart.h"
#include "hardware/gpio.h"
#include "hardware/rtc.h"
#include "lwip/apps/mqtt.h"
#include "lwip/ip_addr.h"

#include "soyo_config.h"

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

#define VERSION "1.0.0-pico"

// Priority conditions
typedef enum {
    PRIO_ENTLADESCHUTZ,     // SOC too low
    PRIO_BAT2_CHARGING,     // Battery charging
    PRIO_SOYOPOWER,         // Manual power setting
    PRIO_FEEDING_TO_GRID,   // PV excess
    PRIO_BUYING_FROM_GRID,  // Grid purchase
    PRIO_DEFAULT,           // Base load
    PRIO_COUNT
} Priority;

static const char* priority_names[] = {
    "entladeschutz",
    "bat2_charging",
    "soyopower",
    "feeding_to_grid",
    "buying_from_grid",
    "default"
};

// Status flags
#define STATUS_ENTLADESCHUTZ    (1 << 0)
#define STATUS_BAT2_CHARGING    (1 << 1)
#define STATUS_BUYING_FROM_GRID (1 << 2)
#define STATUS_FEEDING_TO_GRID  (1 << 3)
#define STATUS_DEFAULT          (1 << 5)

// Inverter data structure
typedef struct {
    int grid_w;
    int bat_w;
    int soc_bat2;
    float bat2_current;
    int state;
    float wp_power;

    // Data validity timestamps
    uint32_t grid_w_time;
    uint32_t bat_w_time;
    uint32_t soc_time;
    uint32_t current_time;
    uint32_t state_time;
    uint32_t wp_power_time;

    // Validity flags
    bool grid_w_valid;
    bool bat_w_valid;
    bool soc_valid;
    bool current_valid;
    bool state_valid;
    bool wp_power_valid;
} InverterData;

// Controller state
typedef struct {
    int last_power;
    int last_grid_w;
    int last_soc;
    float last_current;
    char active_condition[64];
} ControllerState;

// Global variables
static mqtt_client_t *mqtt_client = NULL;
static bool mqtt_connected = false;
static bool wifi_connected = false;
static InverterData inverter_data = {0};
static ControllerState ctrl_state = {0};
static int manual_soyopower = -1;  // -1 = not set

// Function declarations
static void wifi_init(void);
static void mqtt_init(void);
static void uart_rs485_init(void);
static void set_generated_power(int power);
static bool is_night_time(void);
static void control_loop(void);
static void check_data_validity(void);

// RS485 control functions
static inline void rs485_tx_enable(void) {
    gpio_put(UART_RS485_DE_PIN, 1);  // Enable transmitter
    sleep_us(10);  // Small delay for RS485 to switch
}

static inline void rs485_tx_disable(void) {
    sleep_us(10);  // Wait for transmission to complete
    gpio_put(UART_RS485_DE_PIN, 0);  // Disable transmitter
}

// Initialize RS485 UART
static void uart_rs485_init(void) {
    printf("RS485: Initializing on GPIO %d (TX), %d (RX), %d (DE/RE)\n",
           UART_RS485_TX_PIN, UART_RS485_RX_PIN, UART_RS485_DE_PIN);

    // Initialize UART
    uart_init(UART_RS485, UART_RS485_BAUD);

    // Set TX and RX pins
    gpio_set_function(UART_RS485_TX_PIN, GPIO_FUNC_UART);
    gpio_set_function(UART_RS485_RX_PIN, GPIO_FUNC_UART);

    // Set UART format: 8N1
    uart_set_format(UART_RS485, 8, 1, UART_PARITY_NONE);

    // Initialize DE/RE pin (Driver/Receiver Enable)
    gpio_init(UART_RS485_DE_PIN);
    gpio_set_dir(UART_RS485_DE_PIN, GPIO_OUT);
    gpio_put(UART_RS485_DE_PIN, 0);  // Start in receive mode (though we don't receive)

    printf("RS485: Initialized at %d baud\n", UART_RS485_BAUD);
}

// Send power command to Soyo inverter via RS485
// Soyo protocol: 24 56 00 21 PU PL 80 CRC
// No response expected
static void set_generated_power(int power) {
    unsigned char cmd[8];
    unsigned char pu, pl, crc;

    // Clamp power
    if (power < 0) power = 0;
    if (power > MAX_POWER) power = MAX_POWER;

    // Calculate command bytes
    pu = (power >> 8) & 0xFF;
    pl = power & 0xFF;
    crc = (264 - pu - pl) & 0xFF;

    // Build command
    cmd[0] = 0x24;
    cmd[1] = 0x56;
    cmd[2] = 0x00;
    cmd[3] = 0x21;
    cmd[4] = pu;
    cmd[5] = pl;
    cmd[6] = 0x80;
    cmd[7] = crc;

    // Enable RS485 transmitter
    rs485_tx_enable();

    // Send command
    uart_write_blocking(UART_RS485, cmd, 8);

    // Disable transmitter (Soyo doesn't respond)
    rs485_tx_disable();

    printf("Soyo: Power set to %d W (cmd: %02x %02x %02x %02x %02x %02x %02x %02x)\n",
           power, cmd[0], cmd[1], cmd[2], cmd[3], cmd[4], cmd[5], cmd[6], cmd[7]);

    // Publish to MQTT
    if (mqtt_connected && mqtt_client) {
        char payload[32];
        snprintf(payload, sizeof(payload), "%d", power);
        mqtt_publish(mqtt_client, MQTT_TOPIC_SOYO_POWER, payload, strlen(payload),
                    0, 0, NULL, NULL);
    }
}

// Calculate sunrise/sunset times (simplified algorithm)
static double julian_day(int year, int month, int day) {
    if (month <= 2) {
        year -= 1;
        month += 12;
    }
    int A = year / 100;
    int B = 2 - A + (A / 4);
    return floor(365.25 * (year + 4716)) + floor(30.6001 * (month + 1)) + day + B - 1524.5;
}

static void calculate_sun_times(double jd, double lat, double lon,
                                double *sunrise_utc, double *sunset_utc) {
    double n = jd - 2451545.0 + 0.0008;
    double J_star = n - lon / 360.0;
    double M = fmod(357.5291 + 0.98560028 * J_star, 360.0);
    double M_rad = M * M_PI / 180.0;
    double C = 1.9148 * sin(M_rad) + 0.0200 * sin(2 * M_rad) + 0.0003 * sin(3 * M_rad);
    double lambda = fmod(M + C + 180.0 + 102.9372, 360.0);
    double J_transit = 2451545.0 + J_star + 0.0053 * sin(M_rad) -
                       0.0069 * sin(2 * lambda * M_PI / 180.0);

    double lambda_rad = lambda * M_PI / 180.0;
    double sin_dec = sin(lambda_rad) * sin(23.44 * M_PI / 180.0);
    double cos_dec = sqrt(1 - sin_dec * sin_dec);

    double lat_rad = lat * M_PI / 180.0;
    double cos_omega = (sin(-0.83 * M_PI / 180.0) - sin(lat_rad) * sin_dec) /
                       (cos(lat_rad) * cos_dec);

    // Handle polar day/night
    if (cos_omega > 1.0) {
        *sunrise_utc = 0.0;
        *sunset_utc = 0.0;
        return;
    }
    if (cos_omega < -1.0) {
        *sunrise_utc = 12.0;
        *sunset_utc = 12.0;
        return;
    }

    double omega = acos(cos_omega) * 180.0 / M_PI;
    *sunrise_utc = (J_transit - jd - omega / 360.0) * 24.0;
    *sunset_utc = (J_transit - jd + omega / 360.0) * 24.0;
}

// Check if current time is night time
static bool is_night_time(void) {
    datetime_t t;
    if (!rtc_get_datetime(&t)) {
        return false;  // If RTC not available, assume day
    }

    double jd = julian_day(t.year, t.month, t.day);
    double sunrise_utc, sunset_utc;
    calculate_sun_times(jd, LATITUDE, LONGITUDE, &sunrise_utc, &sunset_utc);

    // Add offsets (convert minutes to hours)
    double sunrise_local = sunrise_utc + SUNRISE_OFFSET_MIN / 60.0;
    double sunset_local = sunset_utc + SUNSET_OFFSET_MIN / 60.0;

    // Current time in hours
    double current_hour = t.hour + t.min / 60.0;

    bool is_night = (current_hour < sunrise_local || current_hour > sunset_local);

    return is_night;
}

// WiFi initialization
static void wifi_init(void) {
    printf("WiFi: Connecting to %s...\n", SOYO_WIFI_SSID);

    if (cyw43_arch_init()) {
        printf("WiFi: Failed to initialize\n");
        return;
    }

    cyw43_arch_enable_sta_mode();

    int retry_count = 0;
    while (retry_count < 10) {
        printf("WiFi: Connection attempt %d/10\n", retry_count + 1);

        if (cyw43_arch_wifi_connect_timeout_ms(SOYO_WIFI_SSID, SOYO_WIFI_PASSWORD,
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

// MQTT message callback
static void mqtt_message_cb(void *arg, const char *topic, u32_t tot_len) {
    // This is called when a new message starts
    // The actual data comes in mqtt_data_cb
}

static void mqtt_data_cb(void *arg, const u8_t *data, u16_t len, u8_t flags) {
    // Parse MQTT data
    char payload[64];
    if (len < sizeof(payload)) {
        memcpy(payload, data, len);
        payload[len] = '\0';

        // The topic information is not passed here, we need to track it
        // This is a limitation of lwIP MQTT - we'll use a workaround
        // by parsing all possible values and letting the system decide
        float value = atof(payload);

        uint32_t now = to_ms_since_boot(get_absolute_time());

        // Update inverter data (we'll set all and let validity determine usage)
        // This is simplified - in production you'd track topic per subscription
        printf("MQTT: Received data: %s\n", payload);
    }
}

// MQTT incoming publish callback
static void mqtt_incoming_publish_cb(void *arg, const char *topic, u32_t tot_len) {
    printf("MQTT: Message on topic '%s', length %u\n", topic, tot_len);
}

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

    // Parse based on topic
    if (strstr(topic, "grid")) {
        inverter_data.grid_w = atoi(payload);
        inverter_data.grid_w_time = now;
        inverter_data.grid_w_valid = true;
        printf("MQTT: Grid = %d W\n", inverter_data.grid_w);
    }
    else if (strstr(topic, "soc")) {
        inverter_data.soc_bat2 = atoi(payload);
        inverter_data.soc_time = now;
        inverter_data.soc_valid = true;
        printf("MQTT: SOC = %d %%\n", inverter_data.soc_bat2);
    }
    else if (strstr(topic, "current")) {
        inverter_data.bat2_current = atof(payload);
        inverter_data.current_time = now;
        inverter_data.current_valid = true;
        printf("MQTT: Current = %.2f A\n", inverter_data.bat2_current);
    }
    else if (strstr(topic, "bat_w")) {
        inverter_data.bat_w = atoi(payload);
        inverter_data.bat_w_time = now;
        inverter_data.bat_w_valid = true;
        printf("MQTT: Battery Power = %d W\n", inverter_data.bat_w);
    }
    else if (strstr(topic, "state")) {
        inverter_data.state = atoi(payload);
        inverter_data.state_time = now;
        inverter_data.state_valid = true;
        printf("MQTT: State = %d\n", inverter_data.state);
    }
    else if (strstr(topic, "em0/54")) {
        inverter_data.wp_power = atof(payload);
        inverter_data.wp_power_time = now;
        inverter_data.wp_power_valid = true;
        printf("MQTT: WP Power = %.2f VA\n", inverter_data.wp_power);
    }
}

// MQTT connection callback
static void mqtt_connection_cb(mqtt_client_t *client, void *arg, mqtt_connection_status_t status) {
    if (status == MQTT_CONNECT_ACCEPTED) {
        printf("MQTT: Connected to broker\n");
        mqtt_connected = true;

        // Subscribe to all topics
        mqtt_subscribe(client, MQTT_TOPIC_GRID, 0, NULL, (void*)MQTT_TOPIC_GRID);
        mqtt_subscribe(client, MQTT_TOPIC_SOC, 0, NULL, (void*)MQTT_TOPIC_SOC);
        mqtt_subscribe(client, MQTT_TOPIC_BAT_CURRENT, 0, NULL, (void*)MQTT_TOPIC_BAT_CURRENT);
        mqtt_subscribe(client, MQTT_TOPIC_BAT_POWER, 0, NULL, (void*)MQTT_TOPIC_BAT_POWER);
        mqtt_subscribe(client, MQTT_TOPIC_STATE, 0, NULL, (void*)MQTT_TOPIC_STATE);
        mqtt_subscribe(client, MQTT_TOPIC_WP_POWER, 0, NULL, (void*)MQTT_TOPIC_WP_POWER);

        printf("MQTT: Subscribed to all topics\n");
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

    // Set callbacks
    mqtt_set_inpub_callback(mqtt_client, mqtt_incoming_publish_cb, mqtt_incoming_data_cb, NULL);

    // Parse broker IP
    ip_addr_t broker_ip;
    if (!ip4addr_aton(SOYO_MQTT_BROKER, &broker_ip)) {
        printf("MQTT: Invalid broker IP\n");
        return;
    }

    // Connect
    struct mqtt_connect_client_info_t ci = {0};
    ci.client_id = SOYO_MQTT_CLIENT_ID;

    printf("MQTT: Connecting to %s:%d\n", SOYO_MQTT_BROKER, SOYO_MQTT_PORT);

    err_t err = mqtt_client_connect(mqtt_client, &broker_ip, SOYO_MQTT_PORT,
                                    mqtt_connection_cb, NULL, &ci);

    if (err != ERR_OK) {
        printf("MQTT: Connect failed, error: %d\n", err);
    }
}

// Check data validity based on timestamp
static void check_data_validity(void) {
    uint32_t now = to_ms_since_boot(get_absolute_time());

    if (now - inverter_data.grid_w_time > MQTT_DATA_TIMEOUT_MS) {
        inverter_data.grid_w_valid = false;
    }
    if (now - inverter_data.soc_time > MQTT_DATA_TIMEOUT_MS) {
        inverter_data.soc_valid = false;
    }
    if (now - inverter_data.current_time > MQTT_DATA_TIMEOUT_MS) {
        inverter_data.current_valid = false;
    }
    if (now - inverter_data.wp_power_time > MQTT_DATA_TIMEOUT_MS) {
        inverter_data.wp_power_valid = false;
    }
    if (now - inverter_data.state_time > MQTT_DATA_TIMEOUT_MS) {
        inverter_data.state_valid = false;
    }
}

// Main control loop - priority-based battery management
static void control_loop(void) {
    check_data_validity();

    // Get current values or use last known
    int grid_w = inverter_data.grid_w_valid ? inverter_data.grid_w : ctrl_state.last_grid_w;
    int soc = inverter_data.soc_valid ? inverter_data.soc_bat2 : ctrl_state.last_soc;
    float bat_current = inverter_data.current_valid ? inverter_data.bat2_current : ctrl_state.last_current;
    int state = inverter_data.state_valid ? inverter_data.state : 0;
    float wp_power = inverter_data.wp_power_valid ? inverter_data.wp_power : 17.0f;

    // Check if state != 0 (inverter not ready)
    if (inverter_data.state_valid && state != 0) {
        set_generated_power(0);
        ctrl_state.last_power = 0;
        strcpy(ctrl_state.active_condition, "state_not_0");
        printf("Control: State=%d, power=0W\n", state);
        return;
    }

    // Calculate status byte
    int status_byte = 0;

    if (soc < BAT2_SOC_MIN) {
        status_byte |= STATUS_ENTLADESCHUTZ;
    } else {
        if (grid_w > GRID_FEEDING_THRESHOLD) {
            status_byte |= STATUS_FEEDING_TO_GRID;
        } else if (grid_w < GRID_BUYING_THRESHOLD) {
            status_byte |= STATUS_BUYING_FROM_GRID;
        } else {
            status_byte |= STATUS_DEFAULT;
        }
    }

    // Priority-based control
    int power = 0;
    bool is_night = is_night_time();

    for (int prio = 0; prio < PRIO_COUNT; prio++) {
        if (prio == PRIO_ENTLADESCHUTZ && (status_byte & STATUS_ENTLADESCHUTZ)) {
            power = 0;
            strcpy(ctrl_state.active_condition, priority_names[prio]);
            break;
        }
        else if (prio == PRIO_BAT2_CHARGING && inverter_data.current_valid && bat_current > 2) {
            power = 0;
            strcpy(ctrl_state.active_condition, priority_names[prio]);
            break;
        }
        else if (prio == PRIO_SOYOPOWER && manual_soyopower >= 0) {
            power = manual_soyopower < MAX_POWER ? manual_soyopower : MAX_POWER;
            strcpy(ctrl_state.active_condition, priority_names[prio]);
            break;
        }
        else if (prio == PRIO_FEEDING_TO_GRID && (status_byte & STATUS_FEEDING_TO_GRID)) {
            power = 0;
            strcpy(ctrl_state.active_condition, priority_names[prio]);
            break;
        }
        else if (prio == PRIO_BUYING_FROM_GRID && (status_byte & STATUS_BUYING_FROM_GRID)) {
            int error = abs(grid_w);
            float kp = 1.01f;
            power = (int)(error * kp) + (is_night ? NIGHT_STANDARD_POWER : 0);
            if (power > MAX_POWER) power = MAX_POWER;
            strcpy(ctrl_state.active_condition, priority_names[prio]);
            break;
        }
        else if (prio == PRIO_DEFAULT && (status_byte & STATUS_DEFAULT)) {
            power = is_night ? NIGHT_STANDARD_POWER : 10;
            strcpy(ctrl_state.active_condition, priority_names[prio]);
            break;
        }
    }

    // WP power adjustment (winter months: 9-12, 1-2)
    datetime_t t;
    if (rtc_get_datetime(&t)) {
        int month = t.month;
        if ((strcmp(ctrl_state.active_condition, "buying_from_grid") == 0 ||
             strcmp(ctrl_state.active_condition, "default") == 0) &&
            (month >= 9 || month <= 2)) {

            if (wp_power > NIGHT_STANDARD_POWER) {
                if (power > NIGHT_STANDARD_POWER) {
                    power = NIGHT_STANDARD_POWER;
                }
            }
        }
    }

    // Final clamping
    if (power < 0) power = 0;
    if (power > MAX_POWER) power = MAX_POWER;

    // Update state
    ctrl_state.last_power = power;
    if (inverter_data.grid_w_valid) ctrl_state.last_grid_w = grid_w;
    if (inverter_data.soc_valid) ctrl_state.last_soc = soc;
    if (inverter_data.current_valid) ctrl_state.last_current = bat_current;

    // Set power
    set_generated_power(power);

    printf("Control: Grid=%dW, SOC=%d%%, Current=%.2fA, WP=%.2fVA, Power=%dW, Condition=%s\n",
           grid_w, soc, bat_current, wp_power, power, ctrl_state.active_condition);
}

// Main function
int main() {
    stdio_init_all();
    sleep_ms(2000);

    printf("\n");
    printf("===========================================\n");
    printf("  Soyo Battery Management for Pico W\n");
    printf("  Version: %s\n", VERSION);
    printf("===========================================\n\n");

    // Initialize RTC
    rtc_init();
    datetime_t t = {
        .year = 2026,
        .month = 2,
        .day = 16,
        .hour = 12,
        .min = 0,
        .sec = 0
    };
    rtc_set_datetime(&t);

    // Initialize hardware
    uart_rs485_init();
    wifi_init();

    if (wifi_connected) {
        mqtt_init();
    }

    printf("\nStarting control loop...\n\n");

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

        // Control loop
        if (now - last_update >= UPDATE_INTERVAL_MS) {
            if (mqtt_connected) {
                control_loop();
            }
            last_update = now;
        }

        sleep_ms(10);
    }

    return 0;
}
