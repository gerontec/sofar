/**
 * Raspberry Pi Modbus MQTT Daemon
 *
 * Runs directly on Raspberry Pi, controls RS485 modules via GPIO UARTs.
 * NO Pico needed - RS485 modules connected directly to RPi pins!
 *
 * Hardware:
 *   UART0 (GPIO 14/15) → RS485 Module #1 → SOYO (4800 baud)
 *   UART2 (GPIO 0/1)   → RS485 Module #2 → Relay (9600 baud)
 *   GPIO 2 → DE/RE (Driver Enable for SOYO RS485)
 *   GPIO 3 → DE/RE (Driver Enable for Relay RS485)
 *
 * RS485 Wiring:
 *   DI (Data In)  → TX Pin (GPIO 14 for UART0, GPIO 0 for UART2)
 *   RO (Recv Out) → RX Pin (GPIO 15 for UART0, GPIO 1 for UART2)
 *   DE (Driver Enable) → GPIO 2/3
 *   RE (Receiver Enable) → GPIO 2/3 (tied together with DE)
 *
 * MQTT Topics:
 *   Subscribe: pico/soyo/watts, pico/relay/state
 *   Publish: pico/status, pico/heartbeat
 *
 * Usage:
 *   rpi_modbus_mqtt_daemon <mqtt_host>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <termios.h>
#include <signal.h>
#include <syslog.h>
#include <stdint.h>
#include <pthread.h>
#include <MQTTClient.h>

// UART Devices (adjust for your Raspberry Pi model)
#define SOYO_UART       "/dev/ttyAMA0"   // UART0 (GPIO 14/15)
#define RELAY_UART      "/dev/ttyAMA1"   // UART2 (GPIO 0/1) - RPi 4/5
#define SOYO_BAUD       B4800
#define RELAY_BAUD      B9600

// GPIO for RS485 DE/RE control (using sysfs or WiringPi)
#define SOYO_DE_PIN     2
#define RELAY_DE_PIN    3

// MQTT Configuration
#define MQTT_CLIENTID   "rpi_modbus_daemon"
#define MQTT_QOS        1
#define STATUS_INTERVAL 5

// MQTT Topics
#define TOPIC_SOYO_WATTS    "pico/soyo/watts"
#define TOPIC_SOYO_ENABLE   "pico/soyo/enable"
#define TOPIC_RELAY_STATE   "pico/relay/state"
#define TOPIC_RELAY_TRIGGER "pico/relay/trigger"
#define TOPIC_STATUS        "pico/status"

// Global State (like pico_modbus_dual.c PublicRAM)
typedef struct {
    volatile uint16_t soyo_target_watts;
    volatile uint8_t  soyo_enable;
    volatile uint8_t  relay_target_state;
    volatile uint8_t  relay_needs_update;

    volatile uint8_t  relay_current_state;
    volatile uint8_t  relay_current_bits;
    volatile int16_t  soyo_last_watts;
    volatile uint32_t error_flags;

    pthread_mutex_t mutex;
} PublicRAM;

static PublicRAM g_public_ram = {
    .soyo_target_watts = 0,
    .soyo_enable = 0,
    .relay_target_state = 0,
    .relay_needs_update = 0,
    .relay_current_state = 0xFF,
    .relay_current_bits = 0x00,
    .soyo_last_watts = -1,
    .error_flags = 0,
    .mutex = PTHREAD_MUTEX_INITIALIZER
};

static volatile int running = 1;
static int soyo_fd = -1;
static int relay_fd = -1;
static MQTTClient mqtt_client = NULL;

// State Map (same as pico version)
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

// Signal handler
static void signal_handler(int sig) {
    (void)sig;
    running = 0;
}

// CRC16 Modbus
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

// Open UART
static int open_uart(const char *device, speed_t baud) {
    int fd = open(device, O_RDWR | O_NOCTTY);
    if (fd < 0) {
        syslog(LOG_ERR, "Failed to open %s: %m", device);
        return -1;
    }

    struct termios tty;
    tcgetattr(fd, &tty);
    cfsetospeed(&tty, baud);
    cfsetispeed(&tty, baud);

    tty.c_cflag &= ~PARENB;
    tty.c_cflag &= ~CSTOPB;
    tty.c_cflag &= ~CSIZE;
    tty.c_cflag |= CS8 | CREAD | CLOCAL;
    tty.c_cflag &= ~CRTSCTS;

    tty.c_lflag &= ~(ICANON | ECHO | ECHOE | ISIG);
    tty.c_iflag &= ~(IXON | IXOFF | IXANY);
    tty.c_oflag &= ~OPOST;

    tty.c_cc[VMIN] = 0;
    tty.c_cc[VTIME] = 10;

    tcsetattr(fd, TCSANOW, &tty);
    tcflush(fd, TCIOFLUSH);

    syslog(LOG_INFO, "Opened %s at %d baud", device,
           baud == B4800 ? 4800 : 9600);
    return fd;
}

// Send SOYO command (same as pico version)
static int send_soyo_command(uint16_t watts) {
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

    // TODO: Set DE/RE high (transmit mode)
    write(soyo_fd, cmd, 8);
    usleep(10000);  // Wait 10ms
    // TODO: Set DE/RE low (receive mode)

    // Read response
    uint8_t resp[8];
    int n = read(soyo_fd, resp, 8);

    if (n == 8) {
        uint16_t recv_crc = resp[6] | (resp[7] << 8);
        uint16_t calc_crc = crc16_modbus(resp, 6);
        if (recv_crc == calc_crc) {
            g_public_ram.soyo_last_watts = watts;
            return 0;
        }
    }

    return -1;
}

// Write relay state (similar to pico version)
static int write_relay_status(uint8_t state) {
    const StateMap *map = NULL;
    for (size_t i = 0; i < STATE_MAP_SIZE; i++) {
        if (STATE_MAP[i].state == state) {
            map = &STATE_MAP[i];
            break;
        }
    }
    if (!map) return -1;

    uint8_t cmd[10];
    cmd[0] = 1;      // Slave ID
    cmd[1] = 0x0F;   // Write Multiple Coils
    cmd[2] = 0x00;
    cmd[3] = 0x00;
    cmd[4] = 0x00;
    cmd[5] = 0x03;   // 3 relays
    cmd[6] = 0x01;   // Byte count
    cmd[7] = (map->relay1 ? 0x01 : 0x00) |
             (map->relay2 ? 0x02 : 0x00) |
             (map->relay3 ? 0x04 : 0x00);

    uint16_t crc = crc16_modbus(cmd, 8);
    cmd[8] = crc & 0xFF;
    cmd[9] = (crc >> 8) & 0xFF;

    // TODO: Set DE/RE high
    write(relay_fd, cmd, 10);
    usleep(10000);
    // TODO: Set DE/RE low

    uint8_t resp[8];
    int n = read(relay_fd, resp, 8);

    if (n == 8) {
        g_public_ram.relay_current_state = state;
        g_public_ram.relay_current_bits = cmd[7] & 0x07;
        return 0;
    }

    return -1;
}

// SOYO thread (runs every 3 seconds)
static void* soyo_thread(void *arg) {
    (void)arg;

    while (running) {
        pthread_mutex_lock(&g_public_ram.mutex);
        uint16_t watts = g_public_ram.soyo_target_watts;
        uint8_t enable = g_public_ram.soyo_enable;
        pthread_mutex_unlock(&g_public_ram.mutex);

        if (enable) {
            send_soyo_command(watts);
        }

        sleep(3);
    }

    return NULL;
}

// Relay thread (event-driven)
static void* relay_thread(void *arg) {
    (void)arg;

    while (running) {
        pthread_mutex_lock(&g_public_ram.mutex);
        uint8_t needs_update = g_public_ram.relay_needs_update;
        uint8_t target = g_public_ram.relay_target_state;
        pthread_mutex_unlock(&g_public_ram.mutex);

        if (needs_update) {
            write_relay_status(target);

            pthread_mutex_lock(&g_public_ram.mutex);
            g_public_ram.relay_needs_update = 0;
            pthread_mutex_unlock(&g_public_ram.mutex);
        }

        usleep(100000);  // 100ms
    }

    return NULL;
}

// MQTT callback
static int message_arrived(void *context, char *topic, int topicLen,
                          MQTTClient_message *message) {
    (void)context;
    (void)topicLen;

    char value[256];
    int len = message->payloadlen;
    if (len >= sizeof(value)) len = sizeof(value) - 1;
    memcpy(value, message->payload, len);
    value[len] = '\0';

    syslog(LOG_INFO, "MQTT: %s = %s", topic, value);

    pthread_mutex_lock(&g_public_ram.mutex);

    if (strcmp(topic, TOPIC_SOYO_WATTS) == 0) {
        g_public_ram.soyo_target_watts = atoi(value);
    } else if (strcmp(topic, TOPIC_SOYO_ENABLE) == 0) {
        g_public_ram.soyo_enable = atoi(value);
    } else if (strcmp(topic, TOPIC_RELAY_STATE) == 0) {
        g_public_ram.relay_target_state = atoi(value);
        g_public_ram.relay_needs_update = 1;
    }

    pthread_mutex_unlock(&g_public_ram.mutex);

    MQTTClient_freeMessage(&message);
    MQTTClient_free(topic);
    return 1;
}

static void connection_lost(void *context, char *cause) {
    (void)context;
    syslog(LOG_WARNING, "MQTT connection lost: %s", cause ? cause : "unknown");
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <mqtt_host>\n", argv[0]);
        return 1;
    }

    const char *mqtt_host = argv[1];

    openlog("rpi_modbus_mqtt_daemon", LOG_PID | LOG_CONS, LOG_DAEMON);
    syslog(LOG_INFO, "Starting RPi Modbus MQTT Daemon");

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    // Open UARTs
    soyo_fd = open_uart(SOYO_UART, SOYO_BAUD);
    relay_fd = open_uart(RELAY_UART, RELAY_BAUD);

    if (soyo_fd < 0 || relay_fd < 0) {
        syslog(LOG_ERR, "Failed to open UARTs");
        return 1;
    }

    // TODO: Setup GPIO for DE/RE control

    // Start threads
    pthread_t soyo_tid, relay_tid;
    pthread_create(&soyo_tid, NULL, soyo_thread, NULL);
    pthread_create(&relay_tid, NULL, relay_thread, NULL);

    // Setup MQTT
    char address[256];
    snprintf(address, sizeof(address), "tcp://%s:1883", mqtt_host);

    MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;

    MQTTClient_create(&mqtt_client, address, MQTT_CLIENTID,
                      MQTTCLIENT_PERSISTENCE_NONE, NULL);
    MQTTClient_setCallbacks(mqtt_client, NULL, connection_lost,
                           message_arrived, NULL);

    conn_opts.keepAliveInterval = 20;
    conn_opts.cleansession = 1;

    if (MQTTClient_connect(mqtt_client, &conn_opts) != MQTTCLIENT_SUCCESS) {
        syslog(LOG_ERR, "Failed to connect to MQTT");
        return 1;
    }

    MQTTClient_subscribe(mqtt_client, TOPIC_SOYO_WATTS, MQTT_QOS);
    MQTTClient_subscribe(mqtt_client, TOPIC_SOYO_ENABLE, MQTT_QOS);
    MQTTClient_subscribe(mqtt_client, TOPIC_RELAY_STATE, MQTT_QOS);

    syslog(LOG_INFO, "Ready!");

    // Main loop
    while (running) {
        sleep(STATUS_INTERVAL);

        // Publish status
        char status[256];
        snprintf(status, sizeof(status),
                 "SOYO: %d W, Relay: state %d bits 0x%02X",
                 g_public_ram.soyo_last_watts,
                 g_public_ram.relay_current_state,
                 g_public_ram.relay_current_bits);

        MQTTClient_message msg = MQTTClient_message_initializer;
        msg.payload = status;
        msg.payloadlen = strlen(status);
        msg.qos = MQTT_QOS;
        msg.retained = 1;

        MQTTClient_publishMessage(mqtt_client, TOPIC_STATUS, &msg, NULL);
    }

    // Cleanup
    pthread_join(soyo_tid, NULL);
    pthread_join(relay_tid, NULL);

    MQTTClient_disconnect(mqtt_client, 1000);
    MQTTClient_destroy(&mqtt_client);

    close(soyo_fd);
    close(relay_fd);

    closelog();
    return 0;
}
