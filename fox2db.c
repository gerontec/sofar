/*
 * fox2db.c - SOYO Inverter Power Management Controller
 *
 * Port of fox2db.py to C with command-line configuration
 *
 * Build:
 *   gcc -o fox2db fox2db.c -lpaho-mqtt3c -lcjson -lm -Wall -O2
 *
 * Usage:
 *   ./fox2db [options]
 *
 * Options:
 *   --mqtt-broker <host>          MQTT broker hostname (default: kellertreppe.fritz.box)
 *   --mqtt-port <port>            MQTT port (default: 1883)
 *   --mqtt-topic <topic>          MQTT topic (default: inverter/power_grid_exchange/json)
 *   --mqtt-timeout <sec>          MQTT timeout in seconds (default: 43)
 *   --min-excess <watts>          Minimum excess power (default: 1010)
 *   --max-grid-draw <watts>       Maximum grid draw (default: 1500)
 *   --max-soc <percent>           Maximum SOC (default: 99)
 *   --hysteresis <watts>          Hysteresis power (default: 505)
 *   --stabilization-cycles <n>    Stabilization cycles (default: 2)
 *   --emergency-import <watts>    Emergency import threshold (default: 1020)
 *   --bat-discharge-threshold <w> Battery discharge threshold (default: -220)
 *   --sweet-spot-pcc <watts>      Sweet spot PCC (default: 160)
 *   --sweet-spot-bat <watts>      Sweet spot battery (default: -310)
 *   --max-drop-rate <w/s>         Max drop rate (default: -20)
 *   --deep-discharge-lower <pct>  Deep discharge lower (default: 6)
 *   --deep-discharge-upper <pct>  Deep discharge upper (default: 8)
 *   --deep-discharge-target <pct> Deep discharge charge target (default: 7)
 *   --ebox-script <path>          EBox script path
 *   --ebyte-script <path>         EByte script path
 *   --mqtt-publish-script <path>  MQTT publish script path
 *   --version                     Show version
 *   --help                        Show this help
 */

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h>
#include <math.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <errno.h>
#include <signal.h>
#include <stdarg.h>
#include <MQTTClient.h>
#include <cjson/cJSON.h>

// ═══════════════════════════════════════════════════════════════════════════
//                             VERSION & CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════

#define VERSION "v1.50-C"
#define MAX_PATH_LEN 512
#define MAX_LOG_MSG 1024
#define MAX_TOPIC_LEN 256
#define MAX_PAYLOAD_LEN 4096

// State power mapping (hardware)
typedef struct {
    int state;
    int power;
} StatePowerMap;

static const StatePowerMap STATE_POWER[] = {
    {0, 0},
    {1, 3000},
    {2, 3650},
    {3, 6650},
    {4, 3900},
    {5, 7100},
    {6, 7800},
    {7, 11400}
};
#define NUM_STATES 8

// ═══════════════════════════════════════════════════════════════════════════
//                             CONFIGURATION STRUCTURE
// ═══════════════════════════════════════════════════════════════════════════

typedef struct {
    // Regelwerk-Parameter
    int min_excess;
    int max_grid_draw;
    int max_soc;
    int hysteresis;
    int stabilization_cycles;
    int emergency_import;
    int bat_discharge_threshold;
    int sweet_spot_pcc;
    int sweet_spot_bat;
    int max_drop_rate;
    int deep_discharge_lower;
    int deep_discharge_upper;
    int deep_discharge_charge_target;

    // MQTT
    char mqtt_broker[256];
    int mqtt_port;
    char mqtt_topic[MAX_TOPIC_LEN];
    int mqtt_timeout;

    // Dateipfade
    char path_deep_discharge[MAX_PATH_LEN];
    char path_log[MAX_PATH_LEN];
    char path_relay_state[MAX_PATH_LEN];
    char path_last_change[MAX_PATH_LEN];
    char path_last_excess[MAX_PATH_LEN];
    char path_ebox_data[MAX_PATH_LEN];
    char path_inverter_csv[MAX_PATH_LEN];
    char path_ebox_script[MAX_PATH_LEN];
    char path_ebyte_script[MAX_PATH_LEN];
    char path_mqtt_publish_script[MAX_PATH_LEN];
} Config;

// Global configuration instance
static Config config;

// Initialize configuration with default values
static void init_config(void) {
    // Regelwerk defaults
    config.min_excess = 1010;
    config.max_grid_draw = 1500;
    config.max_soc = 99;
    config.hysteresis = 505;
    config.stabilization_cycles = 2;
    config.emergency_import = 1020;
    config.bat_discharge_threshold = -220;
    config.sweet_spot_pcc = 160;
    config.sweet_spot_bat = -310;
    config.max_drop_rate = -20;
    config.deep_discharge_lower = 6;
    config.deep_discharge_upper = 8;
    config.deep_discharge_charge_target = 7;

    // MQTT defaults
    strncpy(config.mqtt_broker, "kellertreppe.fritz.box", sizeof(config.mqtt_broker) - 1);
    config.mqtt_port = 1883;
    strncpy(config.mqtt_topic, "inverter/power_grid_exchange/json", sizeof(config.mqtt_topic) - 1);
    config.mqtt_timeout = 43;

    // Path defaults
    strncpy(config.path_deep_discharge, "/run/user/1000/deep_discharge_protection_active.txt", sizeof(config.path_deep_discharge) - 1);
    strncpy(config.path_log, "/run/user/1000/fox2db.log", sizeof(config.path_log) - 1);
    strncpy(config.path_relay_state, "/run/user/1000/current_relay_state.txt", sizeof(config.path_relay_state) - 1);
    strncpy(config.path_last_change, "/run/user/1000/last_relay_change.txt", sizeof(config.path_last_change) - 1);
    strncpy(config.path_last_excess, "/run/user/1000/last_excess.txt", sizeof(config.path_last_excess) - 1);
    strncpy(config.path_ebox_data, "/run/user/1000/ebox15k.txt", sizeof(config.path_ebox_data) - 1);
    strncpy(config.path_inverter_csv, "/tmp/inverter.csv", sizeof(config.path_inverter_csv) - 1);
    strncpy(config.path_ebox_script, "/home/pi/python/ebox1arg.py", sizeof(config.path_ebox_script) - 1);
    strncpy(config.path_ebyte_script, "/home/pi/python/ebyteserrequest.py", sizeof(config.path_ebyte_script) - 1);
    strncpy(config.path_mqtt_publish_script, "/home/pi/python/fox2mqtt.py", sizeof(config.path_mqtt_publish_script) - 1);
}

// ═══════════════════════════════════════════════════════════════════════════
//                             MQTT DATA STRUCTURE
// ═══════════════════════════════════════════════════════════════════════════

typedef struct {
    double pcc;
    double bat1;
    double soc_bat1;
    int received;
} MqttData;

// ═══════════════════════════════════════════════════════════════════════════
//                             LOGGING
// ═══════════════════════════════════════════════════════════════════════════

void log_msg(const char *fmt, ...) {
    char msg[MAX_LOG_MSG];
    char timestamp[64];
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);

    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", tm_info);

    va_list args;
    va_start(args, fmt);
    vsnprintf(msg, sizeof(msg), fmt, args);
    va_end(args);

    FILE *fp = fopen(config.path_log, "a");
    if (fp) {
        fprintf(fp, "[%s %s] %s\n", timestamp, VERSION, msg);
        fclose(fp);
    } else {
        fprintf(stderr, "Log Error: %s\n", strerror(errno));
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//                             FILE UTILITIES
// ═══════════════════════════════════════════════════════════════════════════

double read_file_value(const char *path, double default_val) {
    FILE *fp = fopen(path, "r");
    if (!fp) {
        return default_val;
    }

    char buf[64];
    if (fgets(buf, sizeof(buf), fp)) {
        fclose(fp);
        return atof(buf);
    }

    fclose(fp);
    return default_val;
}

int write_file_value(const char *path, const char *fmt, ...) {
    FILE *fp = fopen(path, "w");
    if (!fp) {
        log_msg("Error writing %s: %s", path, strerror(errno));
        return -1;
    }

    va_list args;
    va_start(args, fmt);
    vfprintf(fp, fmt, args);
    va_end(args);

    fclose(fp);
    return 0;
}

int file_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0;
}

// ═══════════════════════════════════════════════════════════════════════════
//                             SUBPROCESS EXECUTION
// ═══════════════════════════════════════════════════════════════════════════

// Helper: Build command string with auto-detection for Python scripts vs binaries
static void build_script_command(char *cmd_buf, size_t buf_size, const char *script_path, const char *args) {
    size_t script_len = strlen(script_path);
    bool is_python = (script_len > 3 && strcmp(script_path + script_len - 3, ".py") == 0);

    if (is_python) {
        snprintf(cmd_buf, buf_size, "python3 %s %s", script_path, args);
    } else {
        snprintf(cmd_buf, buf_size, "%s %s", script_path, args);
    }
}

int execute_command(const char *cmd, char *output, size_t output_size, int timeout_sec) {
    char full_cmd[2048];
    snprintf(full_cmd, sizeof(full_cmd), "timeout %d %s 2>&1", timeout_sec, cmd);

    FILE *fp = popen(full_cmd, "r");
    if (!fp) {
        log_msg("Failed to execute: %s", cmd);
        return -1;
    }

    if (output && output_size > 0) {
        size_t total = 0;
        while (fgets(output + total, output_size - total, fp) && total < output_size - 1) {
            total = strlen(output);
        }
    }

    int status = pclose(fp);
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

// ═══════════════════════════════════════════════════════════════════════════
//                             MQTT FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════

int mqtt_message_arrived(void *context, char *topicName, int topicLen, MQTTClient_message *message) {
    MqttData *data = (MqttData *)context;
    (void)topicLen;  // Unused parameter

    char *payload = malloc(message->payloadlen + 1);
    if (!payload) {
        MQTTClient_freeMessage(&message);
        MQTTClient_free(topicName);
        return 1;
    }

    memcpy(payload, message->payload, message->payloadlen);
    payload[message->payloadlen] = '\0';

    // Parse JSON
    cJSON *json = cJSON_Parse(payload);
    if (json) {
        cJSON *pcc = cJSON_GetObjectItem(json, "ActivePower_PCC_Total");
        cJSON *bat = cJSON_GetObjectItem(json, "Power_Bat1");
        cJSON *soc = cJSON_GetObjectItem(json, "SOC_Bat1");

        if (pcc) data->pcc = cJSON_GetNumberValue(pcc) * 1000.0;  // kW -> W
        if (bat) data->bat1 = cJSON_GetNumberValue(bat) * 1000.0;
        if (soc) data->soc_bat1 = cJSON_GetNumberValue(soc);

        log_msg("MQTT Received: PCC=%.0fW, Bat1=%.0fW, SOC_Bat1=%.1f%%",
                data->pcc, data->bat1, data->soc_bat1);

        data->received = 1;
        cJSON_Delete(json);
    } else {
        log_msg("MQTT JSON Parse Error: %s", payload);
    }

    free(payload);
    MQTTClient_freeMessage(&message);
    MQTTClient_free(topicName);

    return 1;
}

int fetch_mqtt(MqttData *data) {
    MQTTClient client;
    MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
    int rc;

    memset(data, 0, sizeof(MqttData));

    char address[512];
    snprintf(address, sizeof(address), "tcp://%s:%d", config.mqtt_broker, config.mqtt_port);

    MQTTClient_create(&client, address, "fox2db_client", MQTTCLIENT_PERSISTENCE_NONE, NULL);
    MQTTClient_setCallbacks(client, data, NULL, mqtt_message_arrived, NULL);

    conn_opts.keepAliveInterval = 60;
    conn_opts.cleansession = 1;

    if ((rc = MQTTClient_connect(client, &conn_opts)) != MQTTCLIENT_SUCCESS) {
        log_msg("MQTT Connection failed: %d", rc);
        MQTTClient_destroy(&client);
        return -1;
    }

    log_msg("MQTT Connected to %s", config.mqtt_broker);
    MQTTClient_subscribe(client, config.mqtt_topic, 0);

    // Wait for message
    int timeout_ms = config.mqtt_timeout * 1000;
    int elapsed = 0;
    int step_ms = 100;

    while (!data->received && elapsed < timeout_ms) {
        usleep(step_ms * 1000);
        elapsed += step_ms;
    }

    MQTTClient_disconnect(client, 1000);
    MQTTClient_destroy(&client);

    if (!data->received) {
        log_msg("CRITICAL: MQTT Timeout after %ds - NO DATA AVAILABLE", config.mqtt_timeout);
        return -1;
    }

    return 0;
}

// ═══════════════════════════════════════════════════════════════════════════
//                             EBOX DATA READING
// ═══════════════════════════════════════════════════════════════════════════

int read_ebox(double *current_out, double *min_soc_out) {
    if (!file_exists(config.path_ebox_data)) {
        log_msg("EBox data file not found: %s", config.path_ebox_data);
        *current_out = 0.0;
        *min_soc_out = 0.0;
        return -1;
    }

    FILE *fp = fopen(config.path_ebox_data, "r");
    if (!fp) {
        *current_out = 0.0;
        *min_soc_out = 0.0;
        return -1;
    }

    char line[512];
    double total_current = 0.0;
    double min_soc = 100.0;
    int soc_count = 0;

    while (fgets(line, sizeof(line), fp)) {
        // Skip header
        if (strstr(line, "Power Volt")) continue;

        // Parse line
        char *p = line;
        // Remove b' prefix if present
        if (strncmp(p, "b'", 2) == 0) p += 2;

        // Check if starts with digit (battery line)
        if (*p >= '1' && *p <= '3') {
            char *fields[10];
            int nfields = 0;
            char *token = strtok(p, " \t\r\n'");

            while (token && nfields < 10) {
                fields[nfields++] = token;
                token = strtok(NULL, " \t\r\n'");
            }

            // Field 2 is current in mA
            if (nfields > 2) {
                total_current += atof(fields[2]);
            }

            // Look for SOC (contains %)
            for (int i = 0; i < nfields; i++) {
                if (strchr(fields[i], '%')) {
                    double soc = atof(fields[i]);
                    if (soc < min_soc) {
                        min_soc = soc;
                        soc_count++;
                    }
                }
            }
        }
    }

    fclose(fp);

    *current_out = total_current / 1000.0;  // mA -> A
    *min_soc_out = (soc_count > 0) ? min_soc : 0.0;

    return 0;
}

// ═══════════════════════════════════════════════════════════════════════════
//                             STATE POWER UTILITIES
// ═══════════════════════════════════════════════════════════════════════════

int get_state_power(int state) {
    for (int i = 0; i < NUM_STATES; i++) {
        if (STATE_POWER[i].state == state) {
            return STATE_POWER[i].power;
        }
    }
    return 0;
}

int find_best_state(int budget) {
    int best_state = 0;
    int best_power = 0;

    for (int i = 0; i < NUM_STATES; i++) {
        if (STATE_POWER[i].power <= budget && STATE_POWER[i].power > best_power) {
            best_state = STATE_POWER[i].state;
            best_power = STATE_POWER[i].power;
        }
    }

    return best_state;
}

int get_next_state_up(int current_state) {
    int current_power = get_state_power(current_state);
    int next_state = current_state;
    int next_power = current_power;

    for (int i = 0; i < NUM_STATES; i++) {
        int p = STATE_POWER[i].power;
        if (p > current_power && (p < next_power || next_power == current_power)) {
            next_state = STATE_POWER[i].state;
            next_power = p;
        }
    }

    return next_state;
}

// ═══════════════════════════════════════════════════════════════════════════
//                             FB STATE CONTROLLER
// ═══════════════════════════════════════════════════════════════════════════

typedef struct {
    int best_state;
    double excess;
    char trace[512];
} ControllerResult;

void fb_controller(double soc, double pcc, double bat_cur, double bat1,
                   int relay_st, int prot, ControllerResult *result) {
    // Calculate real excess
    double ebox_eff = (relay_st > 0) ? fmax(bat_cur * 2 * 53 + 1, get_state_power(relay_st)) : 0;
    double excess = pcc + ebox_eff + bat1;

    result->excess = excess;
    result->trace[0] = '\0';

    // EMERGENCY CHARGE with target - prevents ping-pong
    if (prot) {
        if (soc < config.deep_discharge_charge_target) {
            result->best_state = 1;
            snprintf(result->trace, sizeof(result->trace),
                    "EMERGENCY_CHARGE_TO_%d%% (current %.1f%%)",
                    config.deep_discharge_charge_target, soc);
            return;
        } else {
            result->best_state = 0;
            snprintf(result->trace, sizeof(result->trace),
                    "CHARGE_TARGET_REACHED (%.1f%% >= %d%%)",
                    soc, config.deep_discharge_charge_target);
            return;
        }
    }

    // Critical SOC without active protection
    if (soc < config.deep_discharge_lower) {
        result->best_state = 1;
        snprintf(result->trace, sizeof(result->trace),
                "CRITICAL_SOC_PROTECTION_ACTIVATE (%.1f%% < %d%%)",
                soc, config.deep_discharge_lower);
        return;
    }

    // Battery full
    if (soc >= config.max_soc) {
        result->best_state = 0;
        snprintf(result->trace, sizeof(result->trace), "BATTERY_FULL_STOP");
        return;
    }

    // Insufficient excess
    if (excess < config.min_excess) {
        result->best_state = 0;
        snprintf(result->trace, sizeof(result->trace),
                "INSUFFICIENT_EXCESS (%.0fW)", excess);
        return;
    }

    // Power matching
    int budget = (int)(excess + config.max_grid_draw);
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

// ═══════════════════════════════════════════════════════════════════════════
//                             BLOCKING RULES
// ═══════════════════════════════════════════════════════════════════════════

typedef enum {
    DIR_UP,
    DIR_DOWN,
    DIR_BOTH
} Direction;

typedef struct {
    const char *name;
    int blocked;
    Direction applies_to;
    char reason[256];
} BlockingRule;

void check_blocking_rules(double pcc, double bat1, int stable, double drop_rate,
                          int pwr_diff, Direction dir, BlockingRule *rules, int *num_rules) {
    (void)dir;  // Unused - rules apply themselves based on direction
    *num_rules = 0;

    // SWEET_SPOT_HOLD
    if (fabs(pcc) < config.sweet_spot_pcc && bat1 > config.sweet_spot_bat) {
        BlockingRule *r = &rules[(*num_rules)++];
        r->name = "SWEET_SPOT_HOLD";
        r->blocked = 1;
        r->applies_to = DIR_UP;
        snprintf(r->reason, sizeof(r->reason), "PCC=%.0fW, Bat1=%.0fW", pcc, bat1);
    }

    // TREND_BLOCK
    if (drop_rate < config.max_drop_rate && drop_rate != 0) {
        BlockingRule *r = &rules[(*num_rules)++];
        r->name = "TREND_BLOCK";
        r->blocked = 1;
        r->applies_to = DIR_UP;
        snprintf(r->reason, sizeof(r->reason), "Drop: %.1fW/s", drop_rate);
    }

    // BAT_GUARD_BLOCK
    if (bat1 < config.bat_discharge_threshold) {
        BlockingRule *r = &rules[(*num_rules)++];
        r->name = "BAT_GUARD_BLOCK";
        r->blocked = 1;
        r->applies_to = DIR_UP;
        snprintf(r->reason, sizeof(r->reason), "Bat1=%.0fW", bat1);
    }

    // STABILIZING (v1.50: only blocks down)
    if (stable < config.stabilization_cycles) {
        BlockingRule *r = &rules[(*num_rules)++];
        r->name = "STABILIZING";
        r->blocked = 1;
        r->applies_to = DIR_DOWN;
        snprintf(r->reason, sizeof(r->reason), "stable=%d", stable);
    }

    // HYSTERESIS
    if (pwr_diff < config.hysteresis) {
        BlockingRule *r = &rules[(*num_rules)++];
        r->name = "HYSTERESIS";
        r->blocked = 1;
        r->applies_to = DIR_DOWN;
        snprintf(r->reason, sizeof(r->reason), "%dW", pwr_diff);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//                             MQTT PUBLISH
// ═══════════════════════════════════════════════════════════════════════════

void publish_mqtt_data(double soc, double soc_bat1, double pcc, double bat1,
                       double ebox_w, int final_state, int state_before,
                       int stable, double excess, double drop_rate,
                       const char *trace, int prot, int has_drop_rate) {
    cJSON *json = cJSON_CreateObject();

    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    char timestamp[64];
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H:%M:%S", tm_info);

    cJSON_AddStringToObject(json, "ts", timestamp);
    cJSON_AddStringToObject(json, "version", VERSION);
    cJSON_AddNumberToObject(json, "soc_bat2", round(soc * 10) / 10);
    cJSON_AddNumberToObject(json, "soc_bat1", round(soc_bat1 * 10) / 10);
    cJSON_AddNumberToObject(json, "pcc", round(pcc));
    cJSON_AddNumberToObject(json, "bat1", round(bat1));
    cJSON_AddNumberToObject(json, "ebox", round(ebox_w));
    cJSON_AddNumberToObject(json, "state", final_state);
    cJSON_AddNumberToObject(json, "state_before", state_before);
    cJSON_AddNumberToObject(json, "stable", stable);
    cJSON_AddNumberToObject(json, "excess", round(excess));

    if (has_drop_rate && drop_rate != 0) {
        cJSON_AddNumberToObject(json, "drop_rate", round(drop_rate * 10) / 10);
    } else {
        cJSON_AddNullToObject(json, "drop_rate");
    }

    cJSON_AddStringToObject(json, "trace", trace);
    cJSON_AddBoolToObject(json, "deep_discharge_active", prot);

    char *json_str = cJSON_PrintUnformatted(json);

    // Call MQTT publish script (support both Python scripts and C binaries)
    char cmd[2048];
    size_t script_len = strlen(config.path_mqtt_publish_script);
    bool is_python = (script_len > 3 &&
                      strcmp(config.path_mqtt_publish_script + script_len - 3, ".py") == 0);

    if (is_python) {
        snprintf(cmd, sizeof(cmd), "python3 %s '%s'", config.path_mqtt_publish_script, json_str);
    } else {
        snprintf(cmd, sizeof(cmd), "%s '%s'", config.path_mqtt_publish_script, json_str);
    }

    char output[256];
    int ret = execute_command(cmd, output, sizeof(output), 5);

    if (ret != 0) {
        log_msg("MQTT-Script Error: %s", output);
    }

    free(json_str);
    cJSON_Delete(json);
}

// ═══════════════════════════════════════════════════════════════════════════
//                             MAIN LOOP
// ═══════════════════════════════════════════════════════════════════════════

void main_loop(void) {
    log_msg("--- Start Cycle ---");

    // INPUT LAYER

    // 1. Update EBox data
    char cmd[2048];
    snprintf(cmd, sizeof(cmd), "%s pwr 1 > %s", config.path_ebox_script, config.path_ebox_data);

    char output[256];
    int ret = execute_command(cmd, output, sizeof(output), 10);
    if (ret != 0) {
        log_msg("EBox Update failed: %s", output);
    }

    // 2. Fetch MQTT data - MUST work!
    MqttData mqtt_data;
    if (fetch_mqtt(&mqtt_data) != 0) {
        // MQTT failed → Emergency shutdown
        log_msg("EMERGENCY SHUTDOWN: MQTT failed");

        build_script_command(cmd, sizeof(cmd), config.path_ebyte_script, "0");
        execute_command(cmd, NULL, 0, 10);
        write_file_value(config.path_relay_state, "0");
        log_msg("→ Forced State 0 (safe mode)");
        return;  // Abort cycle
    }

    // 3. Read other data
    double bat_cur, soc;
    read_ebox(&bat_cur, &soc);

    double pcc = mqtt_data.pcc;
    double bat1 = mqtt_data.bat1;
    double soc_bat1 = mqtt_data.soc_bat1;

    int relay_st = (int)read_file_value(config.path_relay_state, 0);
    int stable = (int)read_file_value(config.path_last_change, 0);
    int prot = (int)read_file_value(config.path_deep_discharge, 0);
    double last_excess = read_file_value(config.path_last_excess, 0);

    // LOGIC LAYER

    ControllerResult result;
    fb_controller(soc, pcc, bat_cur, bat1, relay_st, prot, &result);

    int best = result.best_state;
    double excess = result.excess;
    char trace[1024];
    strcpy(trace, result.trace);

    // Calculate trend
    double drop_rate = (last_excess > 0) ? (excess - last_excess) / 30.0 : 0;
    int has_drop_rate = (last_excess > 0);
    write_file_value(config.path_last_excess, "%.1f", excess);

    int final = relay_st;
    int changed = 0;

    if (best != relay_st) {
        int pwr_diff = abs(get_state_power(best) - get_state_power(relay_st));
        Direction direction = (best > relay_st) ? DIR_UP : DIR_DOWN;
        int emergency = (pcc < -config.emergency_import) && (direction == DIR_DOWN);

        if (emergency) {
            log_msg("EMERGENCY: Force Shutdown %d->%d (Import=%.0fW!)", relay_st, best, pcc);
            final = best;
            changed = 1;
        } else {
            // Check blocking rules
            BlockingRule rules[10];
            int num_rules = 0;
            check_blocking_rules(pcc, bat1, stable, drop_rate, pwr_diff, direction, rules, &num_rules);

            int blocked = 0;
            for (int i = 0; i < num_rules; i++) {
                if (rules[i].blocked &&
                    (rules[i].applies_to == direction || rules[i].applies_to == DIR_BOTH)) {
                    char tmp[256];
                    snprintf(tmp, sizeof(tmp), " | %s (%s)", rules[i].name, rules[i].reason);
                    strcat(trace, tmp);
                    blocked = 1;
                    break;
                }
            }

            if (!blocked) {
                final = best;
                changed = 1;
            }
        }
    }

    // OUTPUT LAYER

    int new_stable = changed ? 0 : stable + 1;
    double ebox_w = bat_cur * 2 * 53;

    log_msg("Data: SOC2=%.1f%% SOC1=%.1f%% PCC=%.0fW Bat1=%.0fW EBox=%.0fW (State=%d) Stable=%d",
            soc, soc_bat1, pcc, bat1, ebox_w, relay_st, new_stable);

    if (has_drop_rate && drop_rate != 0) {
        log_msg("Trend: Excess %.0fW→%.0fW (%+.1fW/s)", last_excess, excess, drop_rate);
    }

    log_msg("Result: State %d (TRACE: %s)", final, trace);

    // MQTT publish
    publish_mqtt_data(soc, soc_bat1, pcc, bat1, ebox_w, final, relay_st,
                     new_stable, excess, drop_rate, trace, prot, has_drop_rate);

    // Write files
    write_file_value(config.path_last_change, "%d", new_stable);

    // Deep discharge protection with improved hysteresis
    int old_prot = prot;
    if (soc < config.deep_discharge_lower) {
        if (!old_prot) {
            log_msg("⚠️  DEEP_DISCHARGE_PROTECTION ACTIVATED at %.1f%% (threshold: %d%%)",
                    soc, config.deep_discharge_lower);
        }
        write_file_value(config.path_deep_discharge, "1");
    } else if (soc >= config.deep_discharge_upper) {
        if (old_prot) {
            log_msg("✓ DEEP_DISCHARGE_PROTECTION DEACTIVATED at %.1f%% (threshold: %d%%)",
                    soc, config.deep_discharge_upper);
        }
        write_file_value(config.path_deep_discharge, "0");
    }
    // Between thresholds: status unchanged (hysteresis)

    // Write inverter CSV
    write_file_value(config.path_inverter_csv, "%.0f,%.0f,%.1f,%.1f,%.1f,%d\n",
                    pcc, bat1, soc, soc_bat1, bat_cur, final);

    // UPDATE RELAY STATE - v1.50: improved error handling
    if (changed) {
        char relay_cmd[1024];
        char args[32];
        snprintf(args, sizeof(args), "%d", final);
        build_script_command(relay_cmd, sizeof(relay_cmd), config.path_ebyte_script, args);

        char relay_output[512];
        int relay_ret = execute_command(relay_cmd, relay_output, sizeof(relay_output), 10);

        if (relay_ret == 0) {
            // Success: update state file
            write_file_value(config.path_relay_state, "%d", final);
            log_msg("Relay OK: %d->%d | %s", relay_st, final, relay_output);
        } else {
            log_msg("Relay ERROR State %d: [%s] — State file NOT updated", final, relay_output);
        }
    } else {
        // No change, keep state file current
        write_file_value(config.path_relay_state, "%d", final);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//                             COMMAND LINE PARSING
// ═══════════════════════════════════════════════════════════════════════════

static struct option long_options[] = {
    {"mqtt-broker", required_argument, 0, 'b'},
    {"mqtt-port", required_argument, 0, 'p'},
    {"mqtt-topic", required_argument, 0, 't'},
    {"mqtt-timeout", required_argument, 0, 'T'},
    {"min-excess", required_argument, 0, 'e'},
    {"max-grid-draw", required_argument, 0, 'g'},
    {"max-soc", required_argument, 0, 's'},
    {"hysteresis", required_argument, 0, 'H'},
    {"stabilization-cycles", required_argument, 0, 'S'},
    {"emergency-import", required_argument, 0, 'E'},
    {"bat-discharge-threshold", required_argument, 0, 'B'},
    {"sweet-spot-pcc", required_argument, 0, 'P'},
    {"sweet-spot-bat", required_argument, 0, 'W'},
    {"max-drop-rate", required_argument, 0, 'D'},
    {"deep-discharge-lower", required_argument, 0, 'L'},
    {"deep-discharge-upper", required_argument, 0, 'U'},
    {"deep-discharge-target", required_argument, 0, 'C'},
    {"ebox-script", required_argument, 0, '1'},
    {"ebyte-script", required_argument, 0, '2'},
    {"mqtt-publish-script", required_argument, 0, '3'},
    {"version", no_argument, 0, 'v'},
    {"help", no_argument, 0, 'h'},
    {0, 0, 0, 0}
};

void print_usage(const char *prog) {
    printf("Usage: %s [options]\n\n", prog);
    printf("Options:\n");
    printf("  --mqtt-broker <host>          MQTT broker (default: %s)\n", config.mqtt_broker);
    printf("  --mqtt-port <port>            MQTT port (default: %d)\n", config.mqtt_port);
    printf("  --mqtt-topic <topic>          MQTT topic\n");
    printf("  --mqtt-timeout <sec>          MQTT timeout (default: %d)\n", config.mqtt_timeout);
    printf("  --min-excess <watts>          Min excess (default: %d)\n", config.min_excess);
    printf("  --max-grid-draw <watts>       Max grid draw (default: %d)\n", config.max_grid_draw);
    printf("  --max-soc <percent>           Max SOC (default: %d)\n", config.max_soc);
    printf("  --hysteresis <watts>          Hysteresis (default: %d)\n", config.hysteresis);
    printf("  --stabilization-cycles <n>    Stabilization cycles (default: %d)\n", config.stabilization_cycles);
    printf("  --emergency-import <watts>    Emergency import (default: %d)\n", config.emergency_import);
    printf("  --bat-discharge-threshold <w> Battery discharge threshold (default: %d)\n", config.bat_discharge_threshold);
    printf("  --sweet-spot-pcc <watts>      Sweet spot PCC (default: %d)\n", config.sweet_spot_pcc);
    printf("  --sweet-spot-bat <watts>      Sweet spot battery (default: %d)\n", config.sweet_spot_bat);
    printf("  --max-drop-rate <w/s>         Max drop rate (default: %d)\n", config.max_drop_rate);
    printf("  --deep-discharge-lower <pct>  Deep discharge lower (default: %d)\n", config.deep_discharge_lower);
    printf("  --deep-discharge-upper <pct>  Deep discharge upper (default: %d)\n", config.deep_discharge_upper);
    printf("  --deep-discharge-target <pct> Deep discharge target (default: %d)\n", config.deep_discharge_charge_target);
    printf("  --ebox-script <path>          EBox script path\n");
    printf("  --ebyte-script <path>         EByte script path\n");
    printf("  --mqtt-publish-script <path>  MQTT publish script path\n");
    printf("  --version                     Show version\n");
    printf("  --help                        Show this help\n");
}

void parse_args(int argc, char **argv) {
    int c;
    int option_index = 0;

    while ((c = getopt_long(argc, argv, "vh", long_options, &option_index)) != -1) {
        switch (c) {
            case 'b': strncpy(config.mqtt_broker, optarg, sizeof(config.mqtt_broker) - 1); break;
            case 'p': config.mqtt_port = atoi(optarg); break;
            case 't': strncpy(config.mqtt_topic, optarg, sizeof(config.mqtt_topic) - 1); break;
            case 'T': config.mqtt_timeout = atoi(optarg); break;
            case 'e': config.min_excess = atoi(optarg); break;
            case 'g': config.max_grid_draw = atoi(optarg); break;
            case 's': config.max_soc = atoi(optarg); break;
            case 'H': config.hysteresis = atoi(optarg); break;
            case 'S': config.stabilization_cycles = atoi(optarg); break;
            case 'E': config.emergency_import = atoi(optarg); break;
            case 'B': config.bat_discharge_threshold = atoi(optarg); break;
            case 'P': config.sweet_spot_pcc = atoi(optarg); break;
            case 'W': config.sweet_spot_bat = atoi(optarg); break;
            case 'D': config.max_drop_rate = atoi(optarg); break;
            case 'L': config.deep_discharge_lower = atoi(optarg); break;
            case 'U': config.deep_discharge_upper = atoi(optarg); break;
            case 'C': config.deep_discharge_charge_target = atoi(optarg); break;
            case '1': strncpy(config.path_ebox_script, optarg, MAX_PATH_LEN - 1); break;
            case '2': strncpy(config.path_ebyte_script, optarg, MAX_PATH_LEN - 1); break;
            case '3': strncpy(config.path_mqtt_publish_script, optarg, MAX_PATH_LEN - 1); break;
            case 'v':
                printf("fox2db %s\n", VERSION);
                exit(0);
            case 'h':
                print_usage(argv[0]);
                exit(0);
            default:
                print_usage(argv[0]);
                exit(1);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//                             MAIN ENTRY POINT
// ═══════════════════════════════════════════════════════════════════════════

int main(int argc, char **argv) {
    // Initialize configuration with defaults
    init_config();

    // Parse command-line arguments (override defaults)
    parse_args(argc, argv);

    log_msg("=== fox2db %s started ===", VERSION);
    log_msg("MQTT Broker: %s:%d, Topic: %s", config.mqtt_broker, config.mqtt_port, config.mqtt_topic);

    main_loop();

    log_msg("=== fox2db finished ===");
    return 0;
}
