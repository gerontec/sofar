/**
 * Pico MQTT Daemon - MQTT to Serial Bridge
 *
 * Runs endlessly as a daemon, subscribes to MQTT topics,
 * forwards commands to Pico via USB serial (/dev/ttyACM0).
 *
 * Architecture:
 *   MQTT Topics → Daemon → Serial (/dev/ttyACM0) → Pico → Modbus Hardware
 *
 * Usage:
 *   pico_mqtt_daemon <mqtt_host>
 *   pico_mqtt_daemon localhost
 *   pico_mqtt_daemon 192.168.1.100
 *
 * Systemd Service:
 *   sudo systemctl enable pico-mqtt-daemon
 *   sudo systemctl start pico-mqtt-daemon
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <termios.h>
#include <signal.h>
#include <syslog.h>
#include <sys/stat.h>
#include <errno.h>
#include <MQTTClient.h>

// Configuration
#define PICO_DEVICE       "/dev/ttyACM0"
#define PICO_BAUD         B115200
#define MQTT_CLIENTID     "pico_mqtt_daemon"
#define MQTT_KEEPALIVE    20
#define MQTT_QOS          1
#define STATUS_INTERVAL   5   // Publish status every 5 seconds

// MQTT Topics
#define TOPIC_SOYO_WATTS    "pico/soyo/watts"
#define TOPIC_SOYO_ENABLE   "pico/soyo/enable"
#define TOPIC_RELAY_STATE   "pico/relay/state"
#define TOPIC_RELAY_TRIGGER "pico/relay/trigger"
#define TOPIC_STATUS        "pico/status"
#define TOPIC_HEARTBEAT     "pico/heartbeat"

// Global state
static volatile int running = 1;
static int pico_fd = -1;
static MQTTClient mqtt_client = NULL;
static char mqtt_address[256];

// Signal handler
static void signal_handler(int sig) {
    (void)sig;
    syslog(LOG_INFO, "Received signal, shutting down...");
    running = 0;
}

// Open serial connection to Pico
static int open_pico(void) {
    int fd = open(PICO_DEVICE, O_RDWR | O_NOCTTY);
    if (fd < 0) {
        syslog(LOG_ERR, "Failed to open %s: %s", PICO_DEVICE, strerror(errno));
        return -1;
    }

    // Configure serial port
    struct termios tty;
    if (tcgetattr(fd, &tty) != 0) {
        syslog(LOG_ERR, "tcgetattr failed: %s", strerror(errno));
        close(fd);
        return -1;
    }

    cfsetospeed(&tty, PICO_BAUD);
    cfsetispeed(&tty, PICO_BAUD);

    tty.c_cflag &= ~PARENB;
    tty.c_cflag &= ~CSTOPB;
    tty.c_cflag &= ~CSIZE;
    tty.c_cflag |= CS8;
    tty.c_cflag &= ~CRTSCTS;
    tty.c_cflag |= CREAD | CLOCAL;

    tty.c_lflag &= ~(ICANON | ECHO | ECHOE | ISIG);
    tty.c_iflag &= ~(IXON | IXOFF | IXANY);
    tty.c_oflag &= ~OPOST;

    tty.c_cc[VMIN] = 0;
    tty.c_cc[VTIME] = 10;

    if (tcsetattr(fd, TCSANOW, &tty) != 0) {
        syslog(LOG_ERR, "tcsetattr failed: %s", strerror(errno));
        close(fd);
        return -1;
    }

    tcflush(fd, TCIOFLUSH);
    syslog(LOG_INFO, "Opened serial connection to Pico at %s", PICO_DEVICE);
    return fd;
}

// Send command to Pico
static int send_pico_command(const char *cmd) {
    if (pico_fd < 0) {
        syslog(LOG_WARNING, "Pico not connected, attempting reconnect...");
        pico_fd = open_pico();
        if (pico_fd < 0) {
            return -1;
        }
    }

    ssize_t written = write(pico_fd, cmd, strlen(cmd));
    if (written < 0) {
        syslog(LOG_ERR, "Write to Pico failed: %s", strerror(errno));
        close(pico_fd);
        pico_fd = -1;
        return -1;
    }

    syslog(LOG_DEBUG, "Sent to Pico: %s", cmd);
    return 0;
}

// Read response from Pico (non-blocking)
static int read_pico_response(char *buf, size_t len) {
    if (pico_fd < 0) {
        return -1;
    }

    ssize_t total = 0;
    ssize_t n;

    for (int i = 0; i < 10; i++) {
        n = read(pico_fd, buf + total, len - total - 1);
        if (n > 0) {
            total += n;
        }
        if (total > 0) {
            usleep(100000);  // Wait for more data
        }
    }

    buf[total] = '\0';
    return total;
}

// MQTT message arrived callback
static int message_arrived(void *context, char *topic, int topic_len, MQTTClient_message *message) {
    (void)context;
    (void)topic_len;

    char *payload = (char*)message->payload;
    int payload_len = message->payloadlen;

    // Null-terminate payload
    char value[256];
    if (payload_len >= sizeof(value)) {
        payload_len = sizeof(value) - 1;
    }
    memcpy(value, payload, payload_len);
    value[payload_len] = '\0';

    syslog(LOG_INFO, "MQTT: %s = %s", topic, value);

    // Parse topic and forward to Pico
    char cmd[64];

    if (strcmp(topic, TOPIC_SOYO_WATTS) == 0) {
        // Set SOYO watts
        int watts = atoi(value);
        snprintf(cmd, sizeof(cmd), "s %d\n", watts);
        send_pico_command(cmd);

    } else if (strcmp(topic, TOPIC_SOYO_ENABLE) == 0) {
        // Enable/disable SOYO
        int enable = atoi(value);
        snprintf(cmd, sizeof(cmd), "e %d\n", enable);
        send_pico_command(cmd);

    } else if (strcmp(topic, TOPIC_RELAY_STATE) == 0) {
        // Set relay state
        int state = atoi(value);
        snprintf(cmd, sizeof(cmd), "r %d\n", state);
        send_pico_command(cmd);

    } else if (strcmp(topic, TOPIC_RELAY_TRIGGER) == 0) {
        // Trigger relay update (no-op, already sent above)
        syslog(LOG_DEBUG, "Relay trigger received");

    } else {
        syslog(LOG_WARNING, "Unknown topic: %s", topic);
    }

    MQTTClient_freeMessage(&message);
    MQTTClient_free(topic);
    return 1;
}

// MQTT connection lost callback
static void connection_lost(void *context, char *cause) {
    (void)context;
    syslog(LOG_WARNING, "MQTT connection lost: %s", cause ? cause : "unknown");
}

// Publish status to MQTT
static void publish_status(void) {
    if (mqtt_client == NULL) {
        return;
    }

    // Request status from Pico
    send_pico_command("q\n");
    usleep(200000);  // Wait 200ms for response

    // Read response
    char buf[2048];
    int len = read_pico_response(buf, sizeof(buf));

    if (len > 0) {
        // Publish raw status
        MQTTClient_message pubmsg = MQTTClient_message_initializer;
        pubmsg.payload = buf;
        pubmsg.payloadlen = len;
        pubmsg.qos = MQTT_QOS;
        pubmsg.retained = 1;

        MQTTClient_publishMessage(mqtt_client, TOPIC_STATUS, &pubmsg, NULL);
        syslog(LOG_DEBUG, "Published status (%d bytes)", len);
    }

    // Publish heartbeat
    char heartbeat[64];
    snprintf(heartbeat, sizeof(heartbeat), "%ld", (long)time(NULL));
    MQTTClient_message hb_msg = MQTTClient_message_initializer;
    hb_msg.payload = heartbeat;
    hb_msg.payloadlen = strlen(heartbeat);
    hb_msg.qos = MQTT_QOS;
    hb_msg.retained = 1;

    MQTTClient_publishMessage(mqtt_client, TOPIC_HEARTBEAT, &hb_msg, NULL);
}

// Daemonize process
static void daemonize(void) {
    pid_t pid = fork();

    if (pid < 0) {
        exit(EXIT_FAILURE);
    }

    if (pid > 0) {
        exit(EXIT_SUCCESS);  // Parent exits
    }

    // Child continues
    if (setsid() < 0) {
        exit(EXIT_FAILURE);
    }

    signal(SIGCHLD, SIG_IGN);
    signal(SIGHUP, SIG_IGN);

    pid = fork();
    if (pid < 0) {
        exit(EXIT_FAILURE);
    }

    if (pid > 0) {
        exit(EXIT_SUCCESS);
    }

    umask(0);
    chdir("/");

    // Close all open file descriptors
    for (int fd = sysconf(_SC_OPEN_MAX); fd >= 0; fd--) {
        close(fd);
    }

    // Reopen stdin, stdout, stderr to /dev/null
    stdin = fopen("/dev/null", "r");
    stdout = fopen("/dev/null", "w+");
    stderr = fopen("/dev/null", "w+");
}

// Main daemon loop
int main(int argc, char *argv[]) {
    // Parse arguments
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <mqtt_host> [-d]\n", argv[0]);
        fprintf(stderr, "  mqtt_host    MQTT broker hostname/IP\n");
        fprintf(stderr, "  -d           Run as daemon (background)\n");
        return 1;
    }

    const char *mqtt_host = argv[1];
    int daemon_mode = (argc >= 3 && strcmp(argv[2], "-d") == 0);

    // Setup syslog
    openlog("pico_mqtt_daemon", LOG_PID | LOG_CONS, LOG_DAEMON);
    syslog(LOG_INFO, "Starting Pico MQTT Daemon");

    // Daemonize if requested
    if (daemon_mode) {
        syslog(LOG_INFO, "Running as daemon");
        daemonize();
    }

    // Setup signal handlers
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    // Open serial connection to Pico
    pico_fd = open_pico();
    if (pico_fd < 0) {
        syslog(LOG_ERR, "Failed to open Pico, exiting");
        return 1;
    }

    // Setup MQTT connection
    snprintf(mqtt_address, sizeof(mqtt_address), "tcp://%s:1883", mqtt_host);

    MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
    int rc;

    if ((rc = MQTTClient_create(&mqtt_client, mqtt_address, MQTT_CLIENTID,
                                 MQTTCLIENT_PERSISTENCE_NONE, NULL)) != MQTTCLIENT_SUCCESS) {
        syslog(LOG_ERR, "Failed to create MQTT client: %d", rc);
        return 1;
    }

    // Set callbacks
    MQTTClient_setCallbacks(mqtt_client, NULL, connection_lost, message_arrived, NULL);

    // Connect
    conn_opts.keepAliveInterval = MQTT_KEEPALIVE;
    conn_opts.cleansession = 1;

    if ((rc = MQTTClient_connect(mqtt_client, &conn_opts)) != MQTTCLIENT_SUCCESS) {
        syslog(LOG_ERR, "Failed to connect to MQTT broker at %s: %d", mqtt_address, rc);
        return 1;
    }

    syslog(LOG_INFO, "Connected to MQTT broker at %s", mqtt_address);

    // Subscribe to topics
    MQTTClient_subscribe(mqtt_client, TOPIC_SOYO_WATTS, MQTT_QOS);
    MQTTClient_subscribe(mqtt_client, TOPIC_SOYO_ENABLE, MQTT_QOS);
    MQTTClient_subscribe(mqtt_client, TOPIC_RELAY_STATE, MQTT_QOS);
    MQTTClient_subscribe(mqtt_client, TOPIC_RELAY_TRIGGER, MQTT_QOS);

    syslog(LOG_INFO, "Subscribed to MQTT topics");

    // Main loop
    syslog(LOG_INFO, "Entering main loop");
    time_t last_status = 0;

    while (running) {
        // Publish status periodically
        time_t now = time(NULL);
        if (now - last_status >= STATUS_INTERVAL) {
            publish_status();
            last_status = now;
        }

        // Sleep briefly
        sleep(1);
    }

    // Cleanup
    syslog(LOG_INFO, "Shutting down...");

    MQTTClient_disconnect(mqtt_client, 1000);
    MQTTClient_destroy(&mqtt_client);

    if (pico_fd >= 0) {
        close(pico_fd);
    }

    closelog();
    return 0;
}
