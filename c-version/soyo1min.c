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
#include <stdbool.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <math.h>
#include <getopt.h>
#include <mosquitto.h>
#include "config.h"

// CRTSCTS may not be defined on some systems
#ifndef CRTSCTS
#define CRTSCTS 020000000000
#endif

// M_PI may not be defined on some systems
#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

// Runtime configuration structure
typedef struct {
    char serial_port[256];
    int serial_baudrate;
    int serial_timeout;
    char mqtt_broker[256];
    int mqtt_port;
    char mqtt_topic[256];
    float wp_power_default;
    int battery_capacity_kwh;
    int max_power;
    int max_bat2_current;
    int night_standard_power;
    int bat2_soc_min;
    int update_interval_sec;
    char lock_file[512];
    char log_file[512];
    char inverter_file[512];
    char soyopower_file[512];
    char sunrise_script[512];
    double latitude;
    double longitude;
    int sunrise_offset_min;
    int sunset_offset_min;
} Config;

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
static Config config;
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
        log_file = fopen(config.log_file, "a");
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
    fp = fopen(config.lock_file, "r");
    if (fp) {
        if (fscanf(fp, "%d", &existing_pid) == 1) {
            fclose(fp);
            // Check if process exists
            if (kill(existing_pid, 0) == 0) {
                LOG_ERROR("Another instance is running with PID %d", existing_pid);
                return 0;
            } else {
                LOG_WARNING("Stale lock file found for PID %d, removing it", existing_pid);
                unlink(config.lock_file);
            }
        } else {
            fclose(fp);
        }
    }

    // Create lock file
    fp = fopen(config.lock_file, "w");
    if (!fp) {
        LOG_ERROR("Failed to create lock file %s: %s", config.lock_file, strerror(errno));
        return 0;
    }

    fprintf(fp, "%d\n", my_pid);
    fclose(fp);
    LOG_DEBUG("Lock acquired, PID %d written to %s", my_pid, config.lock_file);
    return 1;
}

void release_lock(void) {
    if (unlink(config.lock_file) == 0) {
        LOG_DEBUG("Lock file %s removed", config.lock_file);
    } else {
        LOG_ERROR("Failed to remove lock file %s: %s", config.lock_file, strerror(errno));
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
    if (power > config.max_power) power = config.max_power;

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

    fp = fopen(config.soyopower_file, "r");
    if (!fp) {
        LOG_DEBUG("Soyopower file not found: %s", config.soyopower_file);
        return -1;
    }

    if (fscanf(fp, "%d", &power) != 1) {
        LOG_WARNING("Invalid soyopower format");
        fclose(fp);
        return -1;
    }

    fclose(fp);

    if (power < 0 || power > config.max_power) {
        LOG_WARNING("Invalid soyopower value, out of range [0, %d]: %d", config.max_power, power);
        return -1;
    }

    return power;
}

// Calculate Julian Day Number
static double julian_day(int year, int month, int day) {
    if (month <= 2) {
        year -= 1;
        month += 12;
    }
    int A = year / 100;
    int B = 2 - A + (A / 4);
    return floor(365.25 * (year + 4716)) + floor(30.6001 * (month + 1)) + day + B - 1524.5;
}

// Calculate sunrise and sunset times using simplified algorithm
// Works for any location on Earth
static void calculate_sun_times(double jd, double lat, double lon, double *sunrise_utc, double *sunset_utc) {
    double n = jd - 2451545.0 + 0.0008;
    double J_star = n - lon / 360.0;
    double M = fmod(357.5291 + 0.98560028 * J_star, 360.0);
    double M_rad = M * M_PI / 180.0;
    double C = 1.9148 * sin(M_rad) + 0.0200 * sin(2 * M_rad) + 0.0003 * sin(3 * M_rad);
    double lambda = fmod(M + C + 180.0 + 102.9372, 360.0);
    double J_transit = 2451545.0 + J_star + 0.0053 * sin(M_rad) - 0.0069 * sin(2 * lambda * M_PI / 180.0);

    double lambda_rad = lambda * M_PI / 180.0;
    double sin_dec = sin(lambda_rad) * sin(23.44 * M_PI / 180.0);
    double cos_dec = sqrt(1 - sin_dec * sin_dec);

    double lat_rad = lat * M_PI / 180.0;
    double cos_omega = (sin(-0.83 * M_PI / 180.0) - sin(lat_rad) * sin_dec) / (cos(lat_rad) * cos_dec);

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
    *sunrise_utc = (J_transit - 2451545.0 - omega / 360.0) * 24.0;
    *sunset_utc = (J_transit - 2451545.0 + omega / 360.0) * 24.0;

    // Normalize to 0-24 range
    *sunrise_utc = fmod(*sunrise_utc + 24.0, 24.0);
    *sunset_utc = fmod(*sunset_utc + 24.0, 24.0);
}

// Get sunrise/sunset times with configurable location and offsets
int get_sun_times(char *sunrise, char *sunset, size_t bufsize) {
    time_t now;
    struct tm *tm_info;
    double jd, sunrise_utc, sunset_utc;
    double sunrise_local, sunset_local;
    int sunrise_hour, sunrise_min, sunrise_sec;
    int sunset_hour, sunset_min, sunset_sec;

    const double UTC_OFFSET = 1.0; // CET (Central European Time)

    time(&now);
    tm_info = localtime(&now);

    // Check for daylight saving time (CEST = UTC+2)
    double tz_offset = UTC_OFFSET;
    if (tm_info->tm_isdst > 0) {
        tz_offset = 2.0; // CEST
    }

    // Calculate Julian Day
    jd = julian_day(tm_info->tm_year + 1900, tm_info->tm_mon + 1, tm_info->tm_mday);

    // Calculate sun times in UTC using configured location
    calculate_sun_times(jd, config.latitude, config.longitude, &sunrise_utc, &sunset_utc);

    // Convert to local time and add configured offsets
    sunrise_local = sunrise_utc + tz_offset + (config.sunrise_offset_min / 60.0);
    sunset_local = sunset_utc + tz_offset + (config.sunset_offset_min / 60.0);

    // Normalize to 0-24 range
    sunrise_local = fmod(sunrise_local + 24.0, 24.0);
    sunset_local = fmod(sunset_local + 24.0, 24.0);

    // Convert decimal hours to HH:MM:SS
    sunrise_hour = (int)sunrise_local;
    sunrise_min = (int)((sunrise_local - sunrise_hour) * 60);
    sunrise_sec = (int)(((sunrise_local - sunrise_hour) * 60 - sunrise_min) * 60);

    sunset_hour = (int)sunset_local;
    sunset_min = (int)((sunset_local - sunset_hour) * 60);
    sunset_sec = (int)(((sunset_local - sunset_hour) * 60 - sunset_min) * 60);

    // Format times as HH:MM:SS
    snprintf(sunrise, bufsize, "%02d:%02d:%02d", sunrise_hour, sunrise_min, sunrise_sec);
    snprintf(sunset, bufsize, "%02d:%02d:%02d", sunset_hour, sunset_min, sunset_sec);

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
    raw_line = read_last_line(config.inverter_file);
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

        remaining_kwh = (soc_bat2 / 100.0) * config.battery_capacity_kwh;
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
    remaining_kwh = (soc_bat2 / 100.0) * config.battery_capacity_kwh;

    if (grid_w_valid && grid_w < -100) {
        LOG_WARNING("Buying from grid: %dW, SOC_Bat2=%d%%", abs(grid_w), soc_bat2);
    }

    // Calculate status byte
    if (soc_bat2 < config.bat2_soc_min) {
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
            power = soyopower < config.max_power ? soyopower : config.max_power;
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
            power = (int)(error * kp) + (is_time_window ? config.night_standard_power : 0);
            if (power > config.max_power) power = config.max_power;
            strcpy(ctrl->active_condition, priority_names[prio]);
            LOG_DEBUG("Power set to %dW to offset grid purchase (Grid_W=%d, Error=%d)",
                     power, grid_value, error);
            break;
        }
        else if (prio == PRIO_DEFAULT && (status_byte & STATUS_DEFAULT)) {
            if (is_time_window) {
                power = config.night_standard_power;
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

        if (wp_power > config.night_standard_power) {
            if (strcmp(ctrl->active_condition, "default") == 0) {
                power = config.night_standard_power;
                LOG_DEBUG("WP power (%.2f W) > %d in month %d, setting power to %d W for WP support (active: %s)",
                         wp_power, config.night_standard_power, current_month, config.night_standard_power, ctrl->active_condition);
            } else {
                if (power > config.night_standard_power) power = config.night_standard_power;
                LOG_DEBUG("WP power (%.2f W) > %d in month %d, capping power to %d W (active: %s)",
                         wp_power, config.night_standard_power, current_month, config.night_standard_power, ctrl->active_condition);
            }
        } else {
            if (power < 0) power = 0;
            LOG_DEBUG("WP power (%.2f W) <= %d in month %d, keeping power: %d W (active: %s)",
                     wp_power, config.night_standard_power, current_month, power, ctrl->active_condition);
        }
    } else {
        if (power < 0) power = 0;
        if (power > config.max_power) power = config.max_power;
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

// Print version information
// Initialize configuration with default values
void init_config(Config *cfg) {
    strncpy(cfg->serial_port, SERIAL_PORT, sizeof(cfg->serial_port) - 1);
    cfg->serial_baudrate = SERIAL_BAUDRATE;
    cfg->serial_timeout = SERIAL_TIMEOUT;
    strncpy(cfg->mqtt_broker, MQTT_BROKER, sizeof(cfg->mqtt_broker) - 1);
    cfg->mqtt_port = MQTT_PORT;
    strncpy(cfg->mqtt_topic, MQTT_TOPIC, sizeof(cfg->mqtt_topic) - 1);
    cfg->wp_power_default = WP_POWER_DEFAULT;
    cfg->battery_capacity_kwh = BATTERY_CAPACITY_KWH;
    cfg->max_power = MAX_POWER;
    cfg->max_bat2_current = MAX_BAT2_CURRENT;
    cfg->night_standard_power = NIGHT_STANDARD_POWER;
    cfg->bat2_soc_min = BAT2_SOC_MIN;
    cfg->update_interval_sec = UPDATE_INTERVAL_SEC;
    strncpy(cfg->lock_file, LOCK_FILE, sizeof(cfg->lock_file) - 1);
    strncpy(cfg->log_file, LOG_FILE, sizeof(cfg->log_file) - 1);
    strncpy(cfg->inverter_file, INVERTER_FILE, sizeof(cfg->inverter_file) - 1);
    strncpy(cfg->soyopower_file, SOYOPOWER_FILE, sizeof(cfg->soyopower_file) - 1);
    strncpy(cfg->sunrise_script, SUNRISE_SCRIPT, sizeof(cfg->sunrise_script) - 1);
    cfg->latitude = LATITUDE;
    cfg->longitude = LONGITUDE;
    cfg->sunrise_offset_min = SUNRISE_OFFSET_MIN;
    cfg->sunset_offset_min = SUNSET_OFFSET_MIN;
}

void print_version(void) {
    printf("soyo1min version %s\n", VERSION);
    printf("C implementation of Sofar Battery Management System\n");
    printf("Compiled: %s %s\n", __DATE__, __TIME__);
}

// Print help information
void print_help(const Config *cfg) {
    printf("Usage: soyo1min [OPTIONS]\n");
    printf("\n");
    printf("Sofar Battery Management System - Controls battery discharge based on\n");
    printf("grid status, battery SOC, sun times, MQTT data, and manual settings.\n");
    printf("\n");
    printf("Options:\n");
    printf("  -h, --help                        Show this help message and exit\n");
    printf("  -v, --version                     Show version information and exit\n");
    printf("\n");
    printf("Serial Communication:\n");
    printf("  -p, --port <device>               Serial port (default: %s)\n", cfg->serial_port);
    printf("  -b, --baud <rate>                 Baud rate (default: %d)\n", cfg->serial_baudrate);
    printf("      --serial-timeout <sec>        Serial timeout in seconds (default: %d)\n", cfg->serial_timeout);
    printf("\n");
    printf("MQTT Configuration:\n");
    printf("      --mqtt-broker <host>          MQTT broker IP/hostname (default: %s)\n", cfg->mqtt_broker);
    printf("      --mqtt-port <port>            MQTT broker port (default: %d)\n", cfg->mqtt_port);
    printf("      --mqtt-topic <topic>          MQTT topic for heat pump power (default: %s)\n", cfg->mqtt_topic);
    printf("      --wp-power-default <watts>    Default heat pump power if MQTT unavailable (default: %.0f W)\n", cfg->wp_power_default);
    printf("\n");
    printf("Battery Configuration:\n");
    printf("      --battery-capacity <kwh>      Battery capacity in kWh (default: %d)\n", cfg->battery_capacity_kwh);
    printf("      --min-soc <percent>           Minimum SOC for discharge protection (default: %d%%)\n", cfg->bat2_soc_min);
    printf("      --max-power <watts>           Maximum power setting (default: %d W)\n", cfg->max_power);
    printf("      --max-bat2-current <amps>     Maximum battery current (default: %d A)\n", cfg->max_bat2_current);
    printf("      --night-power <watts>         Standard power during night (default: %d W)\n", cfg->night_standard_power);
    printf("\n");
    printf("File Paths:\n");
    printf("      --lock-file <path>            Lock file path (default: %s)\n", cfg->lock_file);
    printf("      --log-file <path>             Log file path (default: %s)\n", cfg->log_file);
    printf("      --inverter-file <path>        Inverter data file (default: %s)\n", cfg->inverter_file);
    printf("      --soyopower-file <path>       Manual power override file (default: %s)\n", cfg->soyopower_file);
    printf("\n");
    printf("Location & Sun Times:\n");
    printf("      --latitude <degrees>          Latitude in decimal degrees (default: %.4f)\n", cfg->latitude);
    printf("      --longitude <degrees>         Longitude in decimal degrees (default: %.4f)\n", cfg->longitude);
    printf("      --sunrise-offset <minutes>    Minutes to add to sunrise (default: %d)\n", cfg->sunrise_offset_min);
    printf("      --sunset-offset <minutes>     Minutes to add to sunset (default: %d)\n", cfg->sunset_offset_min);
    printf("\n");
    printf("Timing:\n");
    printf("  -i, --interval <seconds>          Update interval (default: %d sec)\n", cfg->update_interval_sec);
    printf("\n");
    printf("Examples:\n");
    printf("  soyo1min -p /dev/ttyUSB0 -b 4800\n");
    printf("  soyo1min --mqtt-broker 192.168.1.100 --mqtt-topic power/heatpump\n");
    printf("  soyo1min --battery-capacity 40 --min-soc 10\n");
    printf("  soyo1min -i 5 --night-power 500\n");
    printf("  soyo1min --lock-file /tmp/soyo.lock --log-file /tmp/soyo.log\n");
    printf("  soyo1min --inverter-file /tmp/inverter.csv --soyopower-file /home/pi/power.txt\n");
    printf("  soyo1min --latitude 48.1351 --longitude 11.5820  # Munich coordinates\n");
    printf("  soyo1min --sunrise-offset 30 --sunset-offset -30  # Custom time offsets\n");
    printf("\n");
    printf("Priority System:\n");
    printf("  1. Discharge Protection (SOC < min-soc)\n");
    printf("  2. Battery Charging (current > 2A)\n");
    printf("  3. Manual Override (soyopower file)\n");
    printf("  4. Grid Feeding (PV surplus > 200W)\n");
    printf("  5. Grid Buying (grid draw < -100W)\n");
    printf("  6. Default (night power support)\n");
    printf("\n");
}

// Main function
int main(int argc, char *argv[]) {
    InverterController controller;
    int rc;

    // Initialize configuration with defaults
    init_config(&config);

    // Define long options
    static struct option long_options[] = {
        {"help",                no_argument,       0, 'h'},
        {"version",             no_argument,       0, 'v'},
        {"port",                required_argument, 0, 'p'},
        {"baud",                required_argument, 0, 'b'},
        {"serial-timeout",      required_argument, 0, 128},
        {"mqtt-broker",         required_argument, 0, 129},
        {"mqtt-port",           required_argument, 0, 130},
        {"mqtt-topic",          required_argument, 0, 131},
        {"wp-power-default",    required_argument, 0, 132},
        {"battery-capacity",    required_argument, 0, 133},
        {"min-soc",             required_argument, 0, 134},
        {"max-power",           required_argument, 0, 135},
        {"max-bat2-current",    required_argument, 0, 136},
        {"night-power",         required_argument, 0, 137},
        {"lock-file",           required_argument, 0, 138},
        {"log-file",            required_argument, 0, 139},
        {"inverter-file",       required_argument, 0, 140},
        {"soyopower-file",      required_argument, 0, 141},
        {"sunrise-script",      required_argument, 0, 142},
        {"latitude",            required_argument, 0, 143},
        {"longitude",           required_argument, 0, 144},
        {"sunrise-offset",      required_argument, 0, 145},
        {"sunset-offset",       required_argument, 0, 146},
        {"interval",            required_argument, 0, 'i'},
        {0, 0, 0, 0}
    };

    // Parse command line arguments
    int opt;
    int option_index = 0;
    while ((opt = getopt_long(argc, argv, "hvp:b:i:", long_options, &option_index)) != -1) {
        switch (opt) {
            case 'h':
                print_help(&config);
                return 0;
            case 'v':
                print_version();
                return 0;
            case 'p':
                strncpy(config.serial_port, optarg, sizeof(config.serial_port) - 1);
                config.serial_port[sizeof(config.serial_port) - 1] = '\0';
                break;
            case 'b':
                config.serial_baudrate = atoi(optarg);
                if (config.serial_baudrate <= 0) {
                    fprintf(stderr, "Error: Invalid baud rate: %s\n", optarg);
                    return 1;
                }
                break;
            case 128: // --serial-timeout
                config.serial_timeout = atoi(optarg);
                break;
            case 129: // --mqtt-broker
                strncpy(config.mqtt_broker, optarg, sizeof(config.mqtt_broker) - 1);
                config.mqtt_broker[sizeof(config.mqtt_broker) - 1] = '\0';
                break;
            case 130: // --mqtt-port
                config.mqtt_port = atoi(optarg);
                if (config.mqtt_port <= 0 || config.mqtt_port > 65535) {
                    fprintf(stderr, "Error: Invalid MQTT port: %s\n", optarg);
                    return 1;
                }
                break;
            case 131: // --mqtt-topic
                strncpy(config.mqtt_topic, optarg, sizeof(config.mqtt_topic) - 1);
                config.mqtt_topic[sizeof(config.mqtt_topic) - 1] = '\0';
                break;
            case 132: // --wp-power-default
                config.wp_power_default = atof(optarg);
                break;
            case 133: // --battery-capacity
                config.battery_capacity_kwh = atoi(optarg);
                if (config.battery_capacity_kwh <= 0) {
                    fprintf(stderr, "Error: Invalid battery capacity: %s\n", optarg);
                    return 1;
                }
                break;
            case 134: // --min-soc
                config.bat2_soc_min = atoi(optarg);
                if (config.bat2_soc_min < 0 || config.bat2_soc_min > 100) {
                    fprintf(stderr, "Error: Invalid min SOC (must be 0-100): %s\n", optarg);
                    return 1;
                }
                break;
            case 135: // --max-power
                config.max_power = atoi(optarg);
                break;
            case 136: // --max-bat2-current
                config.max_bat2_current = atoi(optarg);
                break;
            case 137: // --night-power
                config.night_standard_power = atoi(optarg);
                break;
            case 138: // --lock-file
                strncpy(config.lock_file, optarg, sizeof(config.lock_file) - 1);
                config.lock_file[sizeof(config.lock_file) - 1] = '\0';
                break;
            case 139: // --log-file
                strncpy(config.log_file, optarg, sizeof(config.log_file) - 1);
                config.log_file[sizeof(config.log_file) - 1] = '\0';
                break;
            case 140: // --inverter-file
                strncpy(config.inverter_file, optarg, sizeof(config.inverter_file) - 1);
                config.inverter_file[sizeof(config.inverter_file) - 1] = '\0';
                break;
            case 141: // --soyopower-file
                strncpy(config.soyopower_file, optarg, sizeof(config.soyopower_file) - 1);
                config.soyopower_file[sizeof(config.soyopower_file) - 1] = '\0';
                break;
            case 142: // --sunrise-script
                strncpy(config.sunrise_script, optarg, sizeof(config.sunrise_script) - 1);
                config.sunrise_script[sizeof(config.sunrise_script) - 1] = '\0';
                break;
            case 143: // --latitude
                config.latitude = atof(optarg);
                if (config.latitude < -90.0 || config.latitude > 90.0) {
                    fprintf(stderr, "Error: Invalid latitude (must be -90 to 90): %s\n", optarg);
                    return 1;
                }
                break;
            case 144: // --longitude
                config.longitude = atof(optarg);
                if (config.longitude < -180.0 || config.longitude > 180.0) {
                    fprintf(stderr, "Error: Invalid longitude (must be -180 to 180): %s\n", optarg);
                    return 1;
                }
                break;
            case 145: // --sunrise-offset
                config.sunrise_offset_min = atoi(optarg);
                break;
            case 146: // --sunset-offset
                config.sunset_offset_min = atoi(optarg);
                break;
            case 'i':
                config.update_interval_sec = atoi(optarg);
                if (config.update_interval_sec <= 0) {
                    fprintf(stderr, "Error: Invalid interval: %s\n", optarg);
                    return 1;
                }
                break;
            default:
                fprintf(stderr, "Try 'soyo1min --help' for more information.\n");
                return 1;
        }
    }

    // Check for unexpected arguments
    if (optind < argc) {
        fprintf(stderr, "Error: Unexpected argument: %s\n", argv[optind]);
        fprintf(stderr, "Try 'soyo1min --help' for more information.\n");
        return 1;
    }

    // Set up signal handlers
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    LOG_INFO("Starting soyo1min %s", VERSION);
    LOG_INFO("Configuration: port=%s, baud=%d, mqtt=%s:%d, topic=%s",
             config.serial_port, config.serial_baudrate,
             config.mqtt_broker, config.mqtt_port, config.mqtt_topic);
    LOG_INFO("Location: lat=%.4f, lon=%.4f, sunrise_offset=%d min, sunset_offset=%d min",
             config.latitude, config.longitude, config.sunrise_offset_min, config.sunset_offset_min);

    // Acquire lock
    if (!acquire_lock()) {
        LOG_ERROR("Exiting due to existing instance");
        return 1;
    }

    // Open serial port
    serial_fd = serial_open(config.serial_port, config.serial_baudrate);
    if (serial_fd < 0) {
        LOG_ERROR("Failed to initialize serial port %s", config.serial_port);
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

    rc = mosquitto_connect(mqtt_client, config.mqtt_broker, config.mqtt_port, 60);
    if (rc != MOSQ_ERR_SUCCESS) {
        LOG_ERROR("Failed to connect to MQTT broker %s: %s", config.mqtt_broker, mosquitto_strerror(rc));
        mqtt_power_value = config.wp_power_default;
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
        sleep(config.update_interval_sec);
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
