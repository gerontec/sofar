/* soyo1min.c  —  Soyo-Leistungsberechnung → MQTT soyo/set
 * Adaptiert von .119-Version: RS485 serial write ersetzt durch
 * mosquitto_publish("soyo/set", {"W": power}).
 * Liest /tmp/inverter.csv (geschrieben von fox2db.py jede Minute).
 */
#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <stdarg.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <math.h>
#include <getopt.h>
#include <mosquitto.h>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

/* ── Defaults (überschreibbar per CLI) ──────────────────────────────────── */
#define VERSION              "v1.14-mqtt"
#define LOCK_FILE            "/tmp/soyo1min.lock"
#define LOG_FILE             "/tmp/soyo1min.log"
#define INVERTER_FILE        "/tmp/inverter.csv"
#define SOYOPOWER_FILE       "/home/pi/soyopower.txt"
#define MQTT_BROKER_DEFAULT  "192.168.178.218"
#define MQTT_PORT_DEFAULT    1883
#define MQTT_WP_TOPIC        "em0/54"           /* WP-Leistung (VA) */
#define MQTT_SOYO_TOPIC      "soyo/set"          /* Ziel-Publish */
#define WP_POWER_DEFAULT     17.0f
#define BATTERY_CAPACITY_KWH 30
#define MAX_POWER            999
#define MAX_BAT2_CURRENT     200
#define NIGHT_STANDARD_POWER 468
#define BAT2_SOC_MIN         9
#define UPDATE_INTERVAL_SEC  25                  /* <30s ESP32 Stale-Safety */
#define MAX_LOG_SIZE_BYTES   102400
#define LATITUDE             47.6811
#define LONGITUDE            11.5732
#define SUNRISE_OFFSET_MIN   60
#define SUNSET_OFFSET_MIN    -60
#define MAX_GRID_W           40000
#define MAX_SOFARBAT_W       2999
#define MAX_SOC              100
#define MIN_SOC              0

#define STATUS_ENTLADESCHUTZ    (1 << 0)
#define STATUS_BUYING_FROM_GRID (1 << 2)
#define STATUS_FEEDING_TO_GRID  (1 << 3)
#define STATUS_DEFAULT          (1 << 5)

/* ── Konfiguration ──────────────────────────────────────────────────────── */
typedef struct {
    char  mqtt_broker[256];
    int   mqtt_port;
    char  mqtt_wp_topic[256];
    float wp_power_default;
    int   battery_capacity_kwh;
    int   max_power;
    int   max_bat2_current;
    int   night_standard_power;
    int   bat2_soc_min;
    int   update_interval_sec;
    char  lock_file[512];
    char  log_file[512];
    char  inverter_file[512];
    char  soyopower_file[512];
    double latitude;
    double longitude;
    int   sunrise_offset_min;
    int   sunset_offset_min;
} Config;

typedef struct {
    int   grid_w, sofarbat_w, soc_bat2, soc_bat1, state;
    float bat2_current;
    int   grid_w_valid, bat_w_valid, bat2_current_valid, state_valid;
} InverterData;

typedef struct {
    int   last_power, last_grid_w, last_sofarbat_w, last_soc_bat2;
    float last_bat2_current;
    char  active_condition[64];
} InverterController;

typedef enum {
    PRIO_ENTLADESCHUTZ, PRIO_BAT2_CHARGING, PRIO_EM0_POWER,
    PRIO_SOYOPOWER, PRIO_FEEDING_TO_GRID, PRIO_BUYING_FROM_GRID,
    PRIO_DEFAULT, PRIO_COUNT
} Priority;

static const char* priority_names[] = {
    "entladeschutz","bat2_charging","em0_power",
    "soyopower","feeding_to_grid","buying_from_grid","default"
};

/* ── Globals ────────────────────────────────────────────────────────────── */
static Config config;
static struct mosquitto *mqtt_client = NULL;
static float  mqtt_power_value = WP_POWER_DEFAULT;
static float  em0_power_value  = 0.0f;
static volatile sig_atomic_t running = 1;
static FILE  *log_fp = NULL;

/* ── Logging ────────────────────────────────────────────────────────────── */
static void rotate_log(void) {
    struct stat st;
    if (stat(config.log_file, &st) == 0 && st.st_size >= MAX_LOG_SIZE_BYTES) {
        if (log_fp) { fclose(log_fp); log_fp = NULL; }
        unlink(config.log_file);
    }
}

static void log_msg(const char *level, const char *fmt, ...) {
    time_t now; struct tm *t; char ts[26]; va_list ap;
    time(&now); t = localtime(&now);
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", t);
    rotate_log();
    if (!log_fp) log_fp = fopen(config.log_file, "a");
    if (log_fp) {
        fprintf(log_fp, "%s - %s - ", ts, level);
        va_start(ap, fmt); vfprintf(log_fp, fmt, ap); va_end(ap);
        fprintf(log_fp, "\n"); fflush(log_fp);
    }
    if (strcmp(level,"ERROR")==0 || strcmp(level,"WARNING")==0) {
        fprintf(stderr, "%s - %s - ", ts, level);
        va_start(ap, fmt); vfprintf(stderr, fmt, ap); va_end(ap);
        fprintf(stderr, "\n");
    }
}
#define LOG_DEBUG(f,...) log_msg("DEBUG",f,##__VA_ARGS__)
#define LOG_INFO(f,...)  log_msg("INFO", f,##__VA_ARGS__)
#define LOG_WARN(f,...)  log_msg("WARNING",f,##__VA_ARGS__)
#define LOG_ERR(f,...)   log_msg("ERROR",f,##__VA_ARGS__)

/* ── Signal ─────────────────────────────────────────────────────────────── */
static void sig_handler(int s) { running = 0; LOG_INFO("Signal %d, stopping", s); }

/* ── Lock ───────────────────────────────────────────────────────────────── */
static int acquire_lock(void) {
    FILE *fp; pid_t pid;
    fp = fopen(config.lock_file, "r");
    if (fp) {
        if (fscanf(fp, "%d", &pid) == 1) {
            fclose(fp);
            if (kill(pid, 0) == 0) { LOG_ERR("Already running PID %d", pid); return 0; }
        } else fclose(fp);
        unlink(config.lock_file);
    }
    fp = fopen(config.lock_file, "w");
    if (!fp) { LOG_ERR("Cannot create lock %s", config.lock_file); return 0; }
    fprintf(fp, "%d\n", getpid()); fclose(fp);
    return 1;
}
static void release_lock(void) { unlink(config.lock_file); }

/* ── MQTT publish soyo/set ──────────────────────────────────────────────── */
static void publish_soyo_power(int power) {
    char payload[64];
    if (power < 0) power = 0;
    if (power > config.max_power) power = config.max_power;
    snprintf(payload, sizeof(payload), "{\"W\":%d}", power);
    if (mqtt_client) {
        int rc = mosquitto_publish(mqtt_client, NULL, MQTT_SOYO_TOPIC,
                                   (int)strlen(payload), payload, 0, false);
        if (rc == MOSQ_ERR_SUCCESS)
            LOG_INFO("soyo/set → %s", payload);
        else
            LOG_ERR("mosquitto_publish failed: %s", mosquitto_strerror(rc));
    } else {
        LOG_ERR("MQTT client NULL, cannot publish soyo/set %s", payload);
    }
}

/* ── MQTT callbacks ─────────────────────────────────────────────────────── */
static void on_connect(struct mosquitto *mosq, void *obj, int rc) {
    (void)obj;
    if (rc == 0) {
        LOG_INFO("MQTT connected to %s:%d", config.mqtt_broker, config.mqtt_port);
        mosquitto_subscribe(mosq, NULL, config.mqtt_wp_topic, 0);
        mosquitto_subscribe(mosq, NULL, "em0/power", 0);
    } else {
        LOG_ERR("MQTT connect failed rc=%d", rc);
    }
}

static void on_message(struct mosquitto *mosq, void *obj,
                        const struct mosquitto_message *msg) {
    (void)mosq; (void)obj;
    float v;
    if (strcmp(msg->topic, "em0/power") == 0) {
        if (sscanf((char*)msg->payload, "%f", &v) == 1) {
            em0_power_value = v;
            LOG_DEBUG("em0/power=%.1fW", v);
        }
        return;
    }
    if (sscanf((char*)msg->payload, "%f", &v) == 1) {
        mqtt_power_value = v;
        LOG_DEBUG("%s=%.1f VA", msg->topic, v);
    } else {
        mqtt_power_value = config.wp_power_default;
    }
}

/* ── Datei-Helpers ──────────────────────────────────────────────────────── */
static char* read_last_line(const char *path) {
    FILE *fp = fopen(path, "r");
    if (!fp) return NULL;
    char *line = NULL, *last = NULL; size_t n = 0; ssize_t r;
    while ((r = getline(&line, &n, fp)) != -1) {
        free(last); last = strdup(line);
    }
    free(line); fclose(fp);
    return last;
}

static int parse_inverter(const char *line, InverterData *d) {
    char buf[1024]; char *tok[6] = {NULL}; int n = 0;
    if (!line || !d) return 0;
    strncpy(buf, line, sizeof(buf)-1); buf[sizeof(buf)-1] = '\0';
    char *nl = strchr(buf, '\n'); if (nl) *nl = '\0';
    char *t = strtok(buf, ",");
    while (t && n < 6) { tok[n++] = t; t = strtok(NULL, ","); }
    memset(d, 0, sizeof(*d));
    if (tok[0] && *tok[0]) { d->grid_w    = (int)atof(tok[0]); d->grid_w_valid = 1; }
    if (tok[1] && *tok[1]) { d->sofarbat_w= (int)atof(tok[1]); d->bat_w_valid  = 1; }
    if (tok[2] && *tok[2])   d->soc_bat2  = (int)atof(tok[2]);
    if (tok[3] && *tok[3])   d->soc_bat1  = (int)atof(tok[3]);
    if (tok[4] && *tok[4]) { d->bat2_current = atof(tok[4]); d->bat2_current_valid = 1; }
    if (tok[5] && *tok[5]) { d->state = atoi(tok[5]); d->state_valid = 1; } else d->state = 4;
    if (abs(d->grid_w) > MAX_GRID_W || abs(d->sofarbat_w) > MAX_SOFARBAT_W ||
        d->soc_bat2 < MIN_SOC || d->soc_bat2 > MAX_SOC) return 0;
    return 1;
}

static int read_soyopower(void) {
    FILE *fp = fopen(config.soyopower_file, "r"); int p;
    if (!fp) return -1;
    if (fscanf(fp, "%d", &p) != 1) { fclose(fp); return -1; }
    fclose(fp);
    return (p >= 0 && p <= config.max_power) ? p : -1;
}

/* ── Sonnenauf-/-untergang ──────────────────────────────────────────────── */
static double julian_day(int y, int m, int d) {
    if (m <= 2) { y--; m += 12; }
    int A = y/100, B = 2-A+A/4;
    return floor(365.25*(y+4716))+floor(30.6001*(m+1))+d+B-1524.5;
}

static void sun_times(double jd, double lat, double lon,
                      double *sr, double *ss) {
    double n = jd-2451545.0+0.0008;
    double Js = n-lon/360.0;
    double M = fmod(357.5291+0.98560028*Js, 360.0);
    double Mr = M*M_PI/180.0;
    double C = 1.9148*sin(Mr)+0.0200*sin(2*Mr)+0.0003*sin(3*Mr);
    double lam = fmod(M+C+180.0+102.9372, 360.0);
    double Jt = 2451545.0+Js+0.0053*sin(Mr)-0.0069*sin(2*lam*M_PI/180.0);
    double slr = sin(lam*M_PI/180.0)*sin(23.44*M_PI/180.0);
    double clr = sqrt(1-slr*slr);
    double co  = (sin(-0.83*M_PI/180.0)-sin(lat*M_PI/180.0)*slr)
                 /(cos(lat*M_PI/180.0)*clr);
    if (co > 1.0)  { *sr = *ss = 0.0; return; }
    if (co < -1.0) { *sr = *ss = 12.0; return; }
    double om = acos(co)*180.0/M_PI;
    *sr = fmod((Jt-2451545.0-om/360.0)*24.0+24.0, 24.0);
    *ss = fmod((Jt-2451545.0+om/360.0)*24.0+24.0, 24.0);
}

static int is_night(void) {
    time_t now; struct tm *t; char now_s[10], sr_s[10], ss_s[10];
    double sr, ss, tz;
    time(&now); t = localtime(&now);
    strftime(now_s, sizeof(now_s), "%H:%M", t);
    tz = (t->tm_isdst > 0) ? 2.0 : 1.0;
    double jd = julian_day(t->tm_year+1900, t->tm_mon+1, t->tm_mday);
    sun_times(jd, config.latitude, config.longitude, &sr, &ss);
    sr += tz + config.sunrise_offset_min/60.0;
    ss += tz + config.sunset_offset_min/60.0;
    sr = fmod(sr+24.0, 24.0); ss = fmod(ss+24.0, 24.0);
    snprintf(sr_s, sizeof(sr_s), "%02d:%02d", (int)sr, (int)((sr-(int)sr)*60));
    snprintf(ss_s, sizeof(ss_s), "%02d:%02d", (int)ss, (int)((ss-(int)ss)*60));
    int night = strcmp(now_s, sr_s) < 0 || strcmp(now_s, ss_s) > 0;
    LOG_DEBUG("Sun: now=%s sr=%s ss=%s night=%d", now_s, sr_s, ss_s, night);
    return night;
}

/* ── Haupt-Regelschleife ────────────────────────────────────────────────── */
static void controller_update(InverterController *ctrl) {
    InverterData d; char *raw = read_last_line(config.inverter_file);
    int gw, sbw, soc2, soc1, state, gwv, bwv, bcv, stv;
    float bc;

    if (raw && parse_inverter(raw, &d)) {
        gw=d.grid_w; sbw=d.sofarbat_w; soc2=d.soc_bat2; soc1=d.soc_bat1;
        bc=d.bat2_current; state=d.state;
        gwv=d.grid_w_valid; bwv=d.bat_w_valid; bcv=d.bat2_current_valid; stv=d.state_valid;
    } else {
        gw=ctrl->last_grid_w; sbw=ctrl->last_sofarbat_w; soc2=ctrl->last_soc_bat2;
        soc1=0; bc=ctrl->last_bat2_current; state=4;
        gwv=bwv=bcv=stv=0;
    }
    if (bcv) ctrl->last_bat2_current = bc;

    int soyopwr = read_soyopower();
    int nw      = is_night();

    /* State!=0 und kein manueller Override → Soyo aus */
    if (stv && state != 0 && soyopwr < 0) {
        LOG_INFO("State=%d≠0, forcing soyo=0W", state);
        publish_soyo_power(0);
        ctrl->last_power = 0;
        free(raw); return;
    }

    int est_gw = gwv ? gw : ctrl->last_grid_w;
    int sb = 0; char dbg[128]="";

    if (soc2 < config.bat2_soc_min) {
        sb |= STATUS_ENTLADESCHUTZ;
        snprintf(dbg,sizeof(dbg),"Entladeschutz SOC2=%d%%",soc2);
    } else {
        int gv = gwv ? gw : est_gw;
        if (gv > 200)       { sb |= STATUS_FEEDING_TO_GRID;  snprintf(dbg,sizeof(dbg),"PV-Export %dW",gv); }
        else if (gv < -100) { sb |= STATUS_BUYING_FROM_GRID; snprintf(dbg,sizeof(dbg),"Netz-Bezug %dW",gv); }
        else                { sb |= STATUS_DEFAULT;           snprintf(dbg,sizeof(dbg),"Ausgeglichen %dW",gv); }
    }

    int power = 0; ctrl->active_condition[0] = '\0';
    for (int p = 0; p < PRIO_COUNT; p++) {
        if (p==PRIO_ENTLADESCHUTZ && (sb & STATUS_ENTLADESCHUTZ)) {
            power=0; strcpy(ctrl->active_condition, priority_names[p]); break;
        } else if (p==PRIO_BAT2_CHARGING && bcv && bc > 2) {
            power=0; strcpy(ctrl->active_condition, priority_names[p]); break;
        } else if (p==PRIO_EM0_POWER && em0_power_value > 44.0f) {
            power=500; strcpy(ctrl->active_condition, priority_names[p]); break;
        } else if (p==PRIO_SOYOPOWER && soyopwr >= 0) {
            power=(soyopwr<config.max_power?soyopwr:config.max_power);
            strcpy(ctrl->active_condition, priority_names[p]); break;
        } else if (p==PRIO_FEEDING_TO_GRID && (sb & STATUS_FEEDING_TO_GRID)) {
            power=0; strcpy(ctrl->active_condition, priority_names[p]); break;
        } else if (p==PRIO_BUYING_FROM_GRID && (sb & STATUS_BUYING_FROM_GRID)) {
            int gv = gwv?gw:est_gw;
            power = (int)(abs(gv)*1.01f) + (nw ? config.night_standard_power : 0);
            if (power > config.max_power) power = config.max_power;
            strcpy(ctrl->active_condition, priority_names[p]); break;
        } else if (p==PRIO_DEFAULT && (sb & STATUS_DEFAULT)) {
            power = nw ? config.night_standard_power : 10;
            strcpy(ctrl->active_condition, priority_names[p]); break;
        }
    }

    /* WP-Anpassung Winter */
    time_t now; struct tm *tm; time(&now); tm = localtime(&now);
    int mon = tm->tm_mon+1;
    float wp = mqtt_power_value;
    if ((strcmp(ctrl->active_condition,"buying_from_grid")==0 ||
         strcmp(ctrl->active_condition,"default")==0) &&
        (mon==9||mon==10||mon==11||mon==12||mon==1||mon==2)) {
        if (wp > config.night_standard_power) {
            if (power > config.night_standard_power) power = config.night_standard_power;
        }
    }
    if (power < 0) power = 0;
    if (power > config.max_power) power = config.max_power;

    ctrl->last_power = power;
    if (gwv) ctrl->last_grid_w = gw;
    if (bwv) ctrl->last_sofarbat_w = sbw;
    ctrl->last_soc_bat2 = soc2;

    publish_soyo_power(power);

    LOG_INFO("SOC2=%d%% Grid=%dW Bat=%dW Power=%dW WP=%.0f Cond=%s [%s]",
             soc2, est_gw, sbw, power, wp, ctrl->active_condition, dbg);
    free(raw);
}

/* ── Konfiguration Defaults ─────────────────────────────────────────────── */
static void init_config(Config *c) {
    strncpy(c->mqtt_broker, MQTT_BROKER_DEFAULT, sizeof(c->mqtt_broker)-1);
    c->mqtt_port            = MQTT_PORT_DEFAULT;
    strncpy(c->mqtt_wp_topic, MQTT_WP_TOPIC, sizeof(c->mqtt_wp_topic)-1);
    c->wp_power_default     = WP_POWER_DEFAULT;
    c->battery_capacity_kwh = BATTERY_CAPACITY_KWH;
    c->max_power            = MAX_POWER;
    c->max_bat2_current     = MAX_BAT2_CURRENT;
    c->night_standard_power = NIGHT_STANDARD_POWER;
    c->bat2_soc_min         = BAT2_SOC_MIN;
    c->update_interval_sec  = UPDATE_INTERVAL_SEC;
    strncpy(c->lock_file,     LOCK_FILE,     sizeof(c->lock_file)-1);
    strncpy(c->log_file,      LOG_FILE,      sizeof(c->log_file)-1);
    strncpy(c->inverter_file, INVERTER_FILE, sizeof(c->inverter_file)-1);
    strncpy(c->soyopower_file,SOYOPOWER_FILE,sizeof(c->soyopower_file)-1);
    c->latitude             = LATITUDE;
    c->longitude            = LONGITUDE;
    c->sunrise_offset_min   = SUNRISE_OFFSET_MIN;
    c->sunset_offset_min    = SUNSET_OFFSET_MIN;
}

/* ── main ───────────────────────────────────────────────────────────────── */
int main(int argc, char *argv[]) {
    InverterController ctrl; int rc;
    init_config(&config);

    /* CLI Parsing */
    static struct option opts[] = {
        {"mqtt-broker",  required_argument, 0, 1},
        {"mqtt-port",    required_argument, 0, 2},
        {"mqtt-topic",   required_argument, 0, 3},
        {"max-power",    required_argument, 0, 4},
        {"min-soc",      required_argument, 0, 5},
        {"night-power",  required_argument, 0, 6},
        {"interval",     required_argument, 0, 'i'},
        {"inverter-file",required_argument, 0, 7},
        {"log-file",     required_argument, 0, 8},
        {"version",      no_argument,       0, 'v'},
        {"help",         no_argument,       0, 'h'},
        {0,0,0,0}
    };
    int opt, oi=0;
    while ((opt=getopt_long(argc,argv,"vhi:",opts,&oi))!=-1) {
        switch(opt) {
            case 1: strncpy(config.mqtt_broker,   optarg, sizeof(config.mqtt_broker)-1);   break;
            case 2: config.mqtt_port            = atoi(optarg); break;
            case 3: strncpy(config.mqtt_wp_topic, optarg, sizeof(config.mqtt_wp_topic)-1); break;
            case 4: config.max_power            = atoi(optarg); break;
            case 5: config.bat2_soc_min         = atoi(optarg); break;
            case 6: config.night_standard_power = atoi(optarg); break;
            case 'i': config.update_interval_sec = atoi(optarg); break;
            case 7: strncpy(config.inverter_file, optarg, sizeof(config.inverter_file)-1); break;
            case 8: strncpy(config.log_file,      optarg, sizeof(config.log_file)-1);      break;
            case 'v': printf("soyo1min %s\n", VERSION); return 0;
            case 'h':
                printf("soyo1min %s — Soyo-Leistung via MQTT soyo/set\n", VERSION);
                printf("  --mqtt-broker HOST  --mqtt-port PORT  --max-power W\n");
                printf("  --min-soc PCT       --night-power W   --interval S\n");
                return 0;
        }
    }

    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);

    if (!acquire_lock()) return 1;

    LOG_INFO("soyo1min %s start: broker=%s:%d interval=%ds",
             VERSION, config.mqtt_broker, config.mqtt_port, config.update_interval_sec);

    /* MQTT init */
    mosquitto_lib_init();
    mqtt_client = mosquitto_new(NULL, true, NULL);
    if (!mqtt_client) { LOG_ERR("mosquitto_new failed"); release_lock(); return 1; }
    mosquitto_connect_callback_set(mqtt_client, on_connect);
    mosquitto_message_callback_set(mqtt_client, on_message);
    rc = mosquitto_connect(mqtt_client, config.mqtt_broker, config.mqtt_port, 60);
    if (rc != MOSQ_ERR_SUCCESS) {
        LOG_WARN("MQTT connect failed: %s (WP-Daten fehlen)", mosquitto_strerror(rc));
        mqtt_power_value = config.wp_power_default;
    } else {
        mosquitto_loop_start(mqtt_client);
    }

    /* Controller */
    memset(&ctrl, 0, sizeof(ctrl));
    ctrl.last_soc_bat2  = 50;
    ctrl.last_sofarbat_w = 199;

    while (running) {
        controller_update(&ctrl);
        sleep((unsigned)config.update_interval_sec);
    }

    LOG_INFO("soyo1min stopping");
    if (mqtt_client) {
        mosquitto_disconnect(mqtt_client);
        mosquitto_loop_stop(mqtt_client, true);
        mosquitto_destroy(mqtt_client);
    }
    mosquitto_lib_cleanup();
    if (log_fp) fclose(log_fp);
    release_lock();
    return 0;
}
