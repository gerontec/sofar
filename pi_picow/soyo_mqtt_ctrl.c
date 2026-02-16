/**
 * SOYO MQTT Controller
 *
 * Sends SOYO control commands via MQTT to Pico daemon.
 * The Pico subscribes to MQTT topics and writes to g_public_ram.
 *
 * Usage:
 *   soyo_mqtt_ctrl <mqtt_host> <watts> [on|off]
 *
 * Examples:
 *   soyo_mqtt_ctrl localhost 1500
 *   soyo_mqtt_ctrl 192.168.1.100 2000 on
 *   soyo_mqtt_ctrl localhost 0 off
 *
 * MQTT Topics:
 *   pico/soyo/watts      - Target watts (uint16_t as string)
 *   pico/soyo/enable     - Enable/disable (0/1)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <MQTTClient.h>

#define TOPIC_SOYO_WATTS  "pico/soyo/watts"
#define TOPIC_SOYO_ENABLE "pico/soyo/enable"
#define QOS 1
#define TIMEOUT 10000L

static void usage(const char *prog) {
    fprintf(stderr, "Usage:\n");
    fprintf(stderr, "  %s <mqtt_host> <watts> [on|off]\n", prog);
    fprintf(stderr, "\n");
    fprintf(stderr, "Arguments:\n");
    fprintf(stderr, "  mqtt_host    MQTT broker hostname/IP (e.g., localhost, 192.168.1.100)\n");
    fprintf(stderr, "  watts        Target watts (0-65535)\n");
    fprintf(stderr, "  on|off       Enable/disable SOYO (default: on)\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "Examples:\n");
    fprintf(stderr, "  %s localhost 1500        # Set 1500W on localhost\n", prog);
    fprintf(stderr, "  %s 192.168.1.100 2000 on # Set 2000W, enable\n", prog);
    fprintf(stderr, "  %s localhost 0 off       # Disable SOYO\n", prog);
    fprintf(stderr, "\n");
    fprintf(stderr, "MQTT Topics:\n");
    fprintf(stderr, "  %s      - Target watts\n", TOPIC_SOYO_WATTS);
    fprintf(stderr, "  %s    - Enable (1) / Disable (0)\n", TOPIC_SOYO_ENABLE);
}

int main(int argc, char *argv[]) {
    // Parse arguments
    if (argc < 3) {
        usage(argv[0]);
        return 1;
    }

    const char *mqtt_host = argv[1];
    int watts = atoi(argv[2]);
    int enable = 1;  // Default: enabled

    // Validate watts
    if (watts < 0 || watts > 65535) {
        fprintf(stderr, "ERROR: Watts must be 0-65535\n");
        return 1;
    }

    // Parse enable/disable
    if (argc >= 4) {
        if (strcmp(argv[3], "off") == 0 || strcmp(argv[3], "0") == 0) {
            enable = 0;
        } else if (strcmp(argv[3], "on") == 0 || strcmp(argv[3], "1") == 0) {
            enable = 1;
        } else {
            fprintf(stderr, "ERROR: Third argument must be 'on' or 'off'\n");
            return 1;
        }
    }

    // Build MQTT connection string
    char address[256];
    snprintf(address, sizeof(address), "tcp://%s:1883", mqtt_host);

    // Create MQTT client
    MQTTClient client;
    MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
    MQTTClient_message pubmsg = MQTTClient_message_initializer;
    MQTTClient_deliveryToken token;
    int rc;

    if ((rc = MQTTClient_create(&client, address, "soyo_ctrl",
                                 MQTTCLIENT_PERSISTENCE_NONE, NULL)) != MQTTCLIENT_SUCCESS) {
        fprintf(stderr, "ERROR: Failed to create MQTT client, return code %d\n", rc);
        return 1;
    }

    // Connect
    conn_opts.keepAliveInterval = 20;
    conn_opts.cleansession = 1;

    if ((rc = MQTTClient_connect(client, &conn_opts)) != MQTTCLIENT_SUCCESS) {
        fprintf(stderr, "ERROR: Failed to connect to MQTT broker at %s, return code %d\n", address, rc);
        fprintf(stderr, "Make sure MQTT broker is running (e.g., mosquitto -v)\n");
        MQTTClient_destroy(&client);
        return 1;
    }

    printf("Connected to MQTT broker at %s\n", address);

    // Publish watts value
    char watts_str[32];
    snprintf(watts_str, sizeof(watts_str), "%d", watts);

    pubmsg.payload = watts_str;
    pubmsg.payloadlen = strlen(watts_str);
    pubmsg.qos = QOS;
    pubmsg.retained = 1;  // Retain so Pico gets it on reconnect

    if ((rc = MQTTClient_publishMessage(client, TOPIC_SOYO_WATTS, &pubmsg, &token)) != MQTTCLIENT_SUCCESS) {
        fprintf(stderr, "ERROR: Failed to publish watts, return code %d\n", rc);
        MQTTClient_disconnect(client, TIMEOUT);
        MQTTClient_destroy(&client);
        return 1;
    }

    MQTTClient_waitForCompletion(client, token, TIMEOUT);
    printf("Published: %s = %s\n", TOPIC_SOYO_WATTS, watts_str);

    // Publish enable/disable value
    char enable_str[2];
    snprintf(enable_str, sizeof(enable_str), "%d", enable);

    pubmsg.payload = enable_str;
    pubmsg.payloadlen = 1;
    pubmsg.qos = QOS;
    pubmsg.retained = 1;

    if ((rc = MQTTClient_publishMessage(client, TOPIC_SOYO_ENABLE, &pubmsg, &token)) != MQTTCLIENT_SUCCESS) {
        fprintf(stderr, "ERROR: Failed to publish enable, return code %d\n", rc);
        MQTTClient_disconnect(client, TIMEOUT);
        MQTTClient_destroy(&client);
        return 1;
    }

    MQTTClient_waitForCompletion(client, token, TIMEOUT);
    printf("Published: %s = %s\n", TOPIC_SOYO_ENABLE, enable_str);

    printf("\nSOYO: %d W (%s)\n", watts, enable ? "enabled" : "disabled");

    // Disconnect
    MQTTClient_disconnect(client, TIMEOUT);
    MQTTClient_destroy(&client);

    return 0;
}
