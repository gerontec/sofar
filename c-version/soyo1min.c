#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <termios.h>
#include <signal.h>
#include <time.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <math.h>
#include <mosquitto.h>
#include "config.h"

// CRTSCTS may not be defined on some systems
#ifndef CRTSCTS
#define CRTSCTS 020000000000
#endif

// Structures
typedef struct {
    int grid_w;
    int sofarbat_w;
    int soc_bat2;
    int soc_bat1;
    float bat2_current;
    int state;
    int grid_w_valid;
    int bat_w_valid;
    int bat2_current_valid;
    int state_valid;
} InverterData;

typedef struct {
    int last_power;
    int last_grid_w;
    int last_sofarbat_w;
    int last_soc_bat2;
    float last_bat2_current;
    char active_condition[64];
} InverterController;

// Global variables
static int serial_fd = -1;
static struct mosquitto *mqtt_client = NULL;
static float mqtt_power_value = WP_POWER_DEFAULT;
static volatile sig_atomic_t running = 1;
static FILE *log_file = NULL;

// Priority conditions enum
typedef enum {
    PRIO_ENTLADESCHUTZ,
    PRIO_BAT2_CHARGING,
    PRIO_SOYOPOWER,
    PRIO_FEEDING_TO_GRID,
    PRIO_BUYING_FROM_GRID,
    PRIO_DEFAULT,
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

// Logging functions
void log_message(const char *level, const char *fmt, ...) {
    time_t now;
    struct tm *tm_info;
    char timestamp[26];
    va_list args;

    time(&now);
    tm_info = localtime(&now);
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", tm_info);

    if (log_file == NULL) {
        log_file = fopen(LOG_FILE, "a");
    }

    if (log_file) {
        fprintf(log_file, "%s - %s - ", timestamp, level);
        va_start(args, fmt);
        vfprintf(log_file, fmt, args);
        va_end(args);
        fprintf(log_file, "\n");
        fflush(log_file);
    }

    // Also print to stderr for important messages
    if (strcmp(level, "ERROR") == 0 || strcmp(level, "WARNING") == 0) {
        fprintf(stderr, "%s - %s - ", timestamp, level);
        va_start(args, fmt);
        vfprintf(stderr, fmt, args);
        va_end(args);
        fprintf(stderr, "\n");
    }
}

#define LOG_DEBUG(fmt, ...) log_message("DEBUG", fmt, ##__VA_ARGS__)
#define LOG_INFO(fmt, ...) log_message("INFO", fmt, ##__VA_ARGS__)
#define LOG_WARNING(fmt, ...) log_message("WARNING", fmt, ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...) log_message("ERROR", fmt, ##__VA_ARGS__)

// Signal handler
void signal_handler(int signum) {
    running = 0;
    LOG_INFO("Received signal %d, shutting down...", signum);
}

// Lock file management
int acquire_lock(void) {
    FILE *fp;
    pid_t my_pid = getpid();
    pid_t existing_pid;

    // Check if lock file exists
    fp = fopen(LOCK_FILE, "r");
    if (fp) {
        if (fscanf(fp, "%d", &existing_pid) == 1) {
            fclose(fp);
            // Check if process exists
            if (kill(existing_pid, 0) == 0) {
                LOG_ERROR("Another instance is running with PID %d", existing_pid);
                return 0;
            } else {
                LOG_WARNING("Stale lock file found for PID %d, removing it", existing_pid);
                unlink(LOCK_FILE);
            }
        } else {
            fclose(fp);
        }
    }

    // Create lock file
    fp = fopen(LOCK_FILE, "w");
    if (!fp) {
        LOG_ERROR("Failed to create lock file %s: %s", LOCK_FILE, strerror(errno));
        return 0;
    }

    fprintf(fp, "%d\n", my_pid);
    fclose(fp);
    LOG_DEBUG("Lock acquired, PID %d written to %s", my_pid, LOCK_FILE);
    return 1;
}

void release_lock(void) {
    if (unlink(LOCK_FILE) == 0) {
        LOG_DEBUG("Lock file %s removed", LOCK_FILE);
    } else {
        LOG_ERROR("Failed to remove lock file %s: %s", LOCK_FILE, strerror(errno));
    }
}

// Serial port functions
int serial_open(const char *port, int baudrate) {
    struct termios tty;
    int fd;

    (void)baudrate; // Currently hardcoded to 4800, parameter reserved for future use

    fd = open(port, O_RDWR | O_NOCTTY | O_SYNC);
    if (fd < 0) {
        LOG_ERROR("Failed to open serial port %s: %s", port, strerror(errno));
        return -1;
    }

    memset(&tty, 0, sizeof(tty));
    if (tcgetattr(fd, &tty) != 0) {
        LOG_ERROR("Error from tcgetattr: %s", strerror(errno));
        close(fd);
        return -1;
    }

    cfsetospeed(&tty, B4800);
    cfsetispeed(&tty, B4800);

    tty.c_cflag = (tty.c_cflag & ~CSIZE) | CS8;
    tty.c_iflag &= ~IGNBRK;
    tty.c_lflag = 0;
    tty.c_oflag = 0;
    tty.c_cc[VMIN]  = 0;
    tty.c_cc[VTIME] = 10;

    tty.c_iflag &= ~(IXON | IXOFF | IXANY);
    tty.c_cflag |= (CLOCAL | CREAD);
    tty.c_cflag &= ~(PARENB | PARODD);
    tty.c_cflag &= ~CSTOPB;
    tty.c_cflag &= ~CRTSCTS;

    if (tcsetattr(fd, TCSANOW, &tty) != 0) {
        LOG_ERROR("Error from tcsetattr: %s", strerror(errno));
        close(fd);
        return -1;
    }

    LOG_INFO("Serial port %s opened successfully", port);
    sleep(2);
    return fd;
}

void set_generated_power(int power) {
    unsigned char cmd[8];
    unsigned char pu, pl, crc;
    int n;

    // Clamp power
    if (power < 0) power = 0;
    if (power > MAX_POWER) power = MAX_POWER;

    pu = (power >> 8) & 0xFF;
    pl = power & 0xFF;
    crc = (264 - pu - pl) & 0xFF;

    cmd[0] = 0x24;
    cmd[1] = 0x56;
    cmd[2] = 0x00;
    cmd[3] = 0x21;
    cmd[4] = pu;
    cmd[5] = pl;
    cmd[6] = 0x80;
    cmd[7] = crc;

    if (serial_fd < 0) {
        LOG_ERROR("Serial port not open");
        return;
    }

    n = write(serial_fd, cmd, 8);
    if (n < 0) {
        LOG_ERROR("Serial write error: %s", strerror(errno));
        return;
    }

    LOG_INFO("Power set to %d W (from Battery)", power);

    // Read response
    unsigned char response[100];
    n = read(serial_fd, response, sizeof(response));
    if (n > 0) {
        char hex_str[256] = {0};
        for (int i = 0; i < n && i < 50; i++) {
            sprintf(hex_str + strlen(hex_str), "%02x", response[i]);
        }
        LOG_DEBUG("Serial response: %s", hex_str);
    }
}

// MQTT callbacks
void on_mqtt_connect(struct mosquitto *mosq, void *obj, int reason_code) {
    (void)obj; // Unused parameter

    if (reason_code == 0) {
        LOG_INFO("Connected to MQTT broker");
        mosquitto_subscribe(mosq, NULL, MQTT_TOPIC, 0);
    } else {
        LOG_ERROR("Failed to connect to MQTT broker, return code: %d", reason_code);
    }
}

void on_mqtt_message(struct mosquitto *mosq, void *obj, const struct mosquitto_message *msg) {
    (void)mosq; // Unused parameter
    (void)obj;  // Unused parameter

    char *payload = (char*)msg->payload;
    float value;

    if (sscanf(payload, "%f", &value) == 1) {
        mqtt_power_value = value;
        LOG_DEBUG("Received MQTT power value: %.2f VA from %s", mqtt_power_value, msg->topic);
    } else {
        LOG_ERROR("Invalid MQTT power value: %s", payload);
        mqtt_power_value = WP_POWER_DEFAULT;
    }
}

// File reading functions
char* read_last_line(const char *filepath) {
    FILE *fp;
    char *line = NULL;
    char *last_line = NULL;
    size_t len = 0;
    ssize_t read;

    fp = fopen(filepath, "r");
    if (!fp) {
        return NULL;
    }

    while ((read = getline(&line, &len, fp)) != -1) {
        if (last_line) free(last_line);
        last_line = strdup(line);
    }

    free(line);
    fclose(fp);

    return last_line;
}

int parse_inverter(const char *line, InverterData *data) {
    char buffer[1024];
    char *tokens[6] = {NULL};
    int count = 0;

    if (!line || !data) return 0;

    strncpy(buffer, line, sizeof(buffer) - 1);
    buffer[sizeof(buffer) - 1] = '\0';

    // Remove newline
    char *newline = strchr(buffer, '\n');
    if (newline) *newline = '\0';

    // Tokenize
    char *token = strtok(buffer, ",");
    while (token && count < 6) {
        tokens[count++] = token;
        token = strtok(NULL, ",");
    }

    // Parse values
    memset(data, 0, sizeof(InverterData));

    if (tokens[0] && strlen(tokens[0]) > 0) {
        data->grid_w = (int)atof(tokens[0]);
        data->grid_w_valid = 1;
    }

    if (tokens[1] && strlen(tokens[1]) > 0) {
        data->sofarbat_w = (int)atof(tokens[1]);
        data->bat_w_valid = 1;
    }

    if (tokens[2] && strlen(tokens[2]) > 0) {
        data->soc_bat2 = (int)atof(tokens[2]);
    }

    if (tokens[3] && strlen(tokens[3]) > 0) {
        data->soc_bat1 = (int)atof(tokens[3]);
    }

    if (tokens[4] && strlen(tokens[4]) > 0) {
        data->bat2_current = atof(tokens[4]);
        data->bat2_current_valid = 1;
    }

    if (tokens[5] && strlen(tokens[5]) > 0) {
        data->state = atoi(tokens[5]);
        data->state_valid = 1;
    } else {
        data->state = 4;
    }

    // Validation
    if (abs(data->grid_w) > MAX_GRID_W ||
        abs(data->sofarbat_w) > MAX_SOFARBAT_W ||
        data->soc_bat2 < MIN_SOC || data->soc_bat2 > MAX_SOC ||
        fabs(data->bat2_current) > MAX_BAT2_CURRENT ||
        data->state < 0 || data->state > 7) {
        LOG_WARNING("Invalid inverter data");
        return 0;
    }

    return 1;
}

int read_soyopower(void) {
    FILE *fp;
    int power;

    fp = fopen(SOYOPOWER_FILE, "r");
    if (!fp) {
        LOG_DEBUG("Soyopower file not found: %s", SOYOPOWER_FILE);
        return -1;
    }

    if (fscanf(fp, "%d", &power) != 1) {
        LOG_WARNING("Invalid soyopower format");
        fclose(fp);
        return -1;
    }

    fclose(fp);

    if (power < 0 || power > MAX_POWER) {
        LOG_WARNING("Invalid soyopower value, out of range [0, %d]: %d", MAX_POWER, power);
        return -1;
    }

    return power;
}

// Get sunrise/sunset times
int get_sun_times(char *sunrise, char *sunset, size_t bufsize) {
    FILE *fp;
    char line[256];
    char *token;
    int field = 0;

    fp = popen(SUNRISE_SCRIPT, "r");
    if (!fp) {
        LOG_ERROR("Failed to execute %s: %s", SUNRISE_SCRIPT, strerror(errno));
        return 0;
    }

    if (fgets(line, sizeof(line), fp) == NULL) {
        LOG_ERROR("Failed to read from %s", SUNRISE_SCRIPT);
        pclose(fp);
        return 0;
    }

    pclose(fp);

    // Remove newline
    char *newline = strchr(line, '\n');
    if (newline) *newline = '\0';

    // Parse CSV: field 1 is sunrise, field 2 is sunset
    token = strtok(line, ",");
    while (token && field < 3) {
        if (field == 1) {
            strncpy(sunrise, token, bufsize - 1);
            sunrise[bufsize - 1] = '\0';
        } else if (field == 2) {
            strncpy(sunset, token, bufsize - 1);
            sunset[bufsize - 1] = '\0';
        }
        field++;
        token = strtok(NULL, ",");
    }

    if (field < 3) {
        LOG_ERROR("Incomplete sun data from %s", SUNRISE_SCRIPT);
        return 0;
    }

    return 1;
}

// Check if current time is in night window (before sunrise or after sunset)
int is_night_time(void) {
    time_t now;
    struct tm *tm_info;
    char now_str[32], sunrise[32], sunset[32];

    time(&now);
    tm_info = localtime(&now);
    strftime(now_str, sizeof(now_str), "%H:%M:%S", tm_info);

    if (!get_sun_times(sunrise, sunset, sizeof(sunrise))) {
        return 0;
    }

    int is_night = (strcmp(now_str, sunrise) < 0 || strcmp(now_str, sunset) > 0);

    LOG_DEBUG("Sun: Now=%s, Sunrise=%s, Sunset=%s, IsNight=%d",
              now_str, sunrise, sunset, is_night);

    return is_night;
}

// Controller update function
void controller_update(InverterController *ctrl) {
    InverterData inverter_data;
    char *raw_line;
    int soyopower;
    int grid_w, sofarbat_w, soc_bat2, soc_bat1, state;
    float bat2_current;
    int grid_w_valid, bat_w_valid, bat2_current_valid, state_valid;
    int estimated_grid_w;
    float remaining_kwh;
    int status_byte = 0;
    char debug_msg[256] = "";
    int power = 0;
    int is_time_window;
    time_t now_time;
    struct tm *tm_info;
    int current_month;

    is_time_window = is_night_time();

    // Read inverter data
    raw_line = read_last_line(INVERTER_FILE);
    if (raw_line && parse_inverter(raw_line, &inverter_data)) {
        grid_w = inverter_data.grid_w;
        sofarbat_w = inverter_data.sofarbat_w;
        soc_bat2 = inverter_data.soc_bat2;
        soc_bat1 = inverter_data.soc_bat1;
        bat2_current = inverter_data.bat2_current;
        state = inverter_data.state;
        grid_w_valid = inverter_data.grid_w_valid;
        bat_w_valid = inverter_data.bat_w_valid;
        bat2_current_valid = inverter_data.bat2_current_valid;
        state_valid = inverter_data.state_valid;
    } else {
        grid_w = ctrl->last_grid_w;
        sofarbat_w = ctrl->last_sofarbat_w;
        soc_bat2 = ctrl->last_soc_bat2;
        soc_bat1 = 0;
        bat2_current = ctrl->last_bat2_current;
        state = 4;
        grid_w_valid = 0;
        bat_w_valid = 0;
        bat2_current_valid = 0;
        state_valid = 0;
    }

    if (bat2_current_valid) {
        ctrl->last_bat2_current = bat2_current;
    }

    // Read soyopower
    soyopower = read_soyopower();

    // Check state
    if (state_valid && state != 0) {
        power = 0;
        strcpy(ctrl->active_condition, "state_not_0");
        LOG_INFO("State=%d (not 0), forcing power to 0 W", state);
        set_generated_power(power);
        ctrl->last_power = power;

        remaining_kwh = (soc_bat2 / 100.0) * BATTERY_CAPACITY_KWH;
        LOG_INFO("State: Raw_Inverter_Data=%s, Grid_W=%d, SoFarBat_W=%d, "
                 "SOC_Bat2=%d%% (%.2f kWh), SOC_Bat1=%d%% (ignored), "
                 "Power=%dW, Bat2_Current=%.2fA, State=%d, SoyoPower=%s",
                 raw_line ? raw_line : "NULL", grid_w, sofarbat_w,
                 soc_bat2, remaining_kwh, soc_bat1, power, bat2_current,
                 state, soyopower >= 0 ? "" : "N/A");

        if (raw_line) free(raw_line);
        return;
    }

    estimated_grid_w = grid_w_valid ? grid_w : ctrl->last_grid_w;
    remaining_kwh = (soc_bat2 / 100.0) * BATTERY_CAPACITY_KWH;

    if (grid_w_valid && grid_w < -100) {
        LOG_WARNING("Buying from grid: %dW, SOC_Bat2=%d%%", abs(grid_w), soc_bat2);
    }

    // Calculate status byte
    if (soc_bat2 < BAT2_SOC_MIN) {
        status_byte |= STATUS_ENTLADESCHUTZ;
        snprintf(debug_msg, sizeof(debug_msg),
                 "Entladeschutz (status_byte=%d, SOC_Bat2=%d%%)",
                 status_byte, soc_bat2);
    } else {
        int grid_value = grid_w_valid ? grid_w : estimated_grid_w;

        // FIXED: Grid sign corrected
        if (grid_value > 200) {
            status_byte |= STATUS_FEEDING_TO_GRID;
            snprintf(debug_msg, sizeof(debug_msg),
                     "Feeding to grid (%dW), PV excess available (status_byte=%d)",
                     grid_value, status_byte);
        } else if (grid_value < -100) {
            status_byte |= STATUS_BUYING_FROM_GRID;
            snprintf(debug_msg, sizeof(debug_msg),
                     "Buying from grid (%dW), Battery supporting (status_byte=%d)",
                     grid_value, status_byte);
        } else {
            status_byte |= STATUS_DEFAULT;
            snprintf(debug_msg, sizeof(debug_msg),
                     "System balanced (%dW), Battery supporting base load (status_byte=%d)",
                     grid_value, status_byte);
        }
    }

    // Priority-based control
    power = 0;
    ctrl->active_condition[0] = '\0';

    for (int prio = 0; prio < PRIO_COUNT; prio++) {
        if (prio == PRIO_ENTLADESCHUTZ && (status_byte & STATUS_ENTLADESCHUTZ)) {
            power = 0;
            strcpy(ctrl->active_condition, priority_names[prio]);
            LOG_DEBUG("Power set to 0W due to Entladeschutz (SOC_Bat2=%d%%)", soc_bat2);
            break;
        }
        else if (prio == PRIO_BAT2_CHARGING && bat2_current_valid && bat2_current > 2) {
            power = 0;
            strcpy(ctrl->active_condition, priority_names[prio]);
            LOG_DEBUG("Power set to 0W due to Bat2_Current=%.2fA > 2 (Bat2 charging)", bat2_current);
            break;
        }
        else if (prio == PRIO_SOYOPOWER && soyopower >= 0) {
            power = soyopower < MAX_POWER ? soyopower : MAX_POWER;
            strcpy(ctrl->active_condition, priority_names[prio]);
            LOG_DEBUG("Power set to %dW from soyopower.txt", power);
            break;
        }
        else if (prio == PRIO_FEEDING_TO_GRID && (status_byte & STATUS_FEEDING_TO_GRID)) {
            power = 0;
            strcpy(ctrl->active_condition, priority_names[prio]);
            int grid_value = grid_w_valid ? grid_w : estimated_grid_w;
            LOG_DEBUG("Power set to 0W due to excess PV (Grid_W=%dW > 200W)", grid_value);
            break;
        }
        else if (prio == PRIO_BUYING_FROM_GRID && (status_byte & STATUS_BUYING_FROM_GRID)) {
            int grid_value = grid_w_valid ? grid_w : estimated_grid_w;
            int error = abs(grid_value);
            float kp = 1.01;
            power = (int)(error * kp) + (is_time_window ? NIGHT_STANDARD_POWER : 0);
            if (power > MAX_POWER) power = MAX_POWER;
            strcpy(ctrl->active_condition, priority_names[prio]);
            LOG_DEBUG("Power set to %dW to offset grid purchase (Grid_W=%d, Error=%d)",
                     power, grid_value, error);
            break;
        }
        else if (prio == PRIO_DEFAULT && (status_byte & STATUS_DEFAULT)) {
            if (is_time_window) {
                power = NIGHT_STANDARD_POWER;
                strcpy(ctrl->active_condition, priority_names[prio]);
                LOG_DEBUG("Power set to %dW for default system support during night", power);
            } else {
                power = 10;
                strcpy(ctrl->active_condition, priority_names[prio]);
                LOG_DEBUG("Power set to 10W for default system support");
            }
            break;
        }
    }

    // WP power adjustment for winter months
    float wp_power = mqtt_power_value;
    time(&now_time);
    tm_info = localtime(&now_time);
    current_month = tm_info->tm_mon + 1;

    if ((strcmp(ctrl->active_condition, "buying_from_grid") == 0 ||
         strcmp(ctrl->active_condition, "default") == 0) &&
        (current_month == 9 || current_month == 10 || current_month == 11 ||
         current_month == 12 || current_month == 1 || current_month == 2)) {

        if (wp_power > NIGHT_STANDARD_POWER) {
            if (strcmp(ctrl->active_condition, "default") == 0) {
                power = NIGHT_STANDARD_POWER;
                LOG_DEBUG("WP power (%.2f W) > %d in month %d, setting power to %d W for WP support (active: %s)",
                         wp_power, NIGHT_STANDARD_POWER, current_month, NIGHT_STANDARD_POWER, ctrl->active_condition);
            } else {
                if (power > NIGHT_STANDARD_POWER) power = NIGHT_STANDARD_POWER;
                LOG_DEBUG("WP power (%.2f W) > %d in month %d, capping power to %d W (active: %s)",
                         wp_power, NIGHT_STANDARD_POWER, current_month, NIGHT_STANDARD_POWER, ctrl->active_condition);
            }
        } else {
            if (power < 0) power = 0;
            LOG_DEBUG("WP power (%.2f W) <= %d in month %d, keeping power: %d W (active: %s)",
                     wp_power, NIGHT_STANDARD_POWER, current_month, power, ctrl->active_condition);
        }
    } else {
        if (power < 0) power = 0;
        if (power > MAX_POWER) power = MAX_POWER;
        if (current_month >= 3 && current_month <= 8) {
            LOG_DEBUG("Month %d (March-August), supporting WP fully, power: %d W", current_month, power);
        }
    }

    // Update controller state
    ctrl->last_power = power;
    if (grid_w_valid) ctrl->last_grid_w = grid_w;
    if (bat_w_valid) ctrl->last_sofarbat_w = sofarbat_w;
    ctrl->last_soc_bat2 = soc_bat2;

    // Set power
    set_generated_power(power);

    if (strlen(debug_msg) > 0) {
        LOG_DEBUG("%s", debug_msg);
    }

    // Final log
    LOG_INFO("State: Raw_Inverter_Data=%s, Grid_W=%d, SoFarBat_W=%d, "
             "SOC_Bat2=%d%% (%.2f kWh), SOC_Bat1=%d%% (ignored), "
             "Power=%dW, Bat2_Current=%.2fA, State=%d, SoyoPower=%s, "
             "WP_Power=%.2fVA, ActiveCondition=%s",
             raw_line ? raw_line : "NULL", estimated_grid_w, sofarbat_w,
             soc_bat2, remaining_kwh, soc_bat1, power, bat2_current,
             state, soyopower >= 0 ? "set" : "N/A", wp_power, ctrl->active_condition);

    if (raw_line) free(raw_line);
}

// Main function
int main(int argc, char *argv[]) {
    InverterController controller;
    int rc;

    (void)argc; // Unused parameter
    (void)argv; // Unused parameter

    // Set up signal handlers
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    LOG_INFO("Starting soyo1min %s", VERSION);

    // Acquire lock
    if (!acquire_lock()) {
        LOG_ERROR("Exiting due to existing instance");
        return 1;
    }

    // Open serial port
    serial_fd = serial_open(SERIAL_PORT, SERIAL_BAUDRATE);
    if (serial_fd < 0) {
        LOG_ERROR("Failed to initialize serial port %s", SERIAL_PORT);
        release_lock();
        return 1;
    }

    // Initialize MQTT
    mosquitto_lib_init();
    mqtt_client = mosquitto_new(NULL, true, NULL);
    if (!mqtt_client) {
        LOG_ERROR("Failed to create MQTT client");
        close(serial_fd);
        release_lock();
        return 1;
    }

    mosquitto_connect_callback_set(mqtt_client, on_mqtt_connect);
    mosquitto_message_callback_set(mqtt_client, on_mqtt_message);

    rc = mosquitto_connect(mqtt_client, MQTT_BROKER, MQTT_PORT, 60);
    if (rc != MOSQ_ERR_SUCCESS) {
        LOG_ERROR("Failed to connect to MQTT broker %s: %s", MQTT_BROKER, mosquitto_strerror(rc));
        mqtt_power_value = WP_POWER_DEFAULT;
    } else {
        mosquitto_loop_start(mqtt_client);
    }

    // Initialize controller
    memset(&controller, 0, sizeof(controller));
    controller.last_grid_w = 0;
    controller.last_sofarbat_w = 199;
    controller.last_soc_bat2 = 50;
    controller.last_bat2_current = 0;

    // Main loop
    while (running) {
        controller_update(&controller);
        sleep(UPDATE_INTERVAL_SEC);
    }

    // Cleanup
    LOG_INFO("Shutting down...");

    if (mqtt_client) {
        mosquitto_loop_stop(mqtt_client, false);
        mosquitto_disconnect(mqtt_client);
        mosquitto_destroy(mqtt_client);
    }
    mosquitto_lib_cleanup();

    if (serial_fd >= 0) {
        close(serial_fd);
    }

    if (log_file) {
        fclose(log_file);
    }

    release_lock();
    LOG_INFO("Serial and MQTT connections closed");

    return 0;
}
