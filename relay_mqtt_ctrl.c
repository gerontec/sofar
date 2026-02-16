/**
 * Relay MQTT Controller
 *
 * Sends relay control commands via MQTT to Pico daemon.
 * The Pico subscribes to MQTT topics and writes to g_public_ram.
 *
 * Usage:
 *   relay_mqtt_ctrl <mqtt_host> <state>
 *
 * Examples:
 *   relay_mqtt_ctrl localhost 0      # All OFF
 *   relay_mqtt_ctrl localhost 3      # R1+R2 ON
 *   relay_mqtt_ctrl localhost 7      # All ON
 *   relay_mqtt_ctrl 192.168.1.100 11 # State 11
 *
 * MQTT Topics:
 *   pico/relay/state     - Target state (0-7, 11)
 *   pico/relay/trigger   - Trigger update (always 1)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <MQTTClient.h>

#define TOPIC_RELAY_STATE   "pico/relay/state"
#define TOPIC_RELAY_TRIGGER "pico/relay/trigger"
#define QOS 1
#define TIMEOUT 10000L

// State descriptions
static const char* state_desc[] = {
    "All OFF",
    "R1 ON",
    "R2 ON",
    "R1+R2 ON",
    "R3 ON",
    "R1+R3 ON",
    "R2+R3 ON",
    "All ON"
};

static void usage(const char *prog) {
    fprintf(stderr, "Usage:\n");
    fprintf(stderr, "  %s <mqtt_host> <state>\n", prog);
    fprintf(stderr, "\n");
    fprintf(stderr, "Arguments:\n");
    fprintf(stderr, "  mqtt_host    MQTT broker hostname/IP\n");
    fprintf(stderr, "  state        Relay state (0-7, 11)\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "Relay States:\n");
    for (int i = 0; i <= 7; i++) {
        fprintf(stderr, "  %d = %s\n", i, state_desc[i]);
    }
    fprintf(stderr, "  11 = Special state 11\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "Examples:\n");
    fprintf(stderr, "  %s localhost 0       # All OFF\n", prog);
    fprintf(stderr, "  %s localhost 3       # R1+R2 ON\n", prog);
    fprintf(stderr, "  %s localhost 7       # All ON\n", prog);
    fprintf(stderr, "\n");
    fprintf(stderr, "MQTT Topics:\n");
    fprintf(stderr, "  %s     - Target state\n", TOPIC_RELAY_STATE);
    fprintf(stderr, "  %s   - Trigger update\n", TOPIC_RELAY_TRIGGER);
}

int main(int argc, char *argv[]) {
    // Parse arguments
    if (argc < 3) {
        usage(argv[0]);
        return 1;
    }

    const char *mqtt_host = argv[1];
    int state = atoi(argv[2]);

    // Validate state
    if ((state < 0 || state > 7) && state != 11) {
        fprintf(stderr, "ERROR: State must be 0-7 or 11\n");
        return 1;
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

    if ((rc = MQTTClient_create(&client, address, "relay_ctrl",
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

    // Publish state value
    char state_str[32];
    snprintf(state_str, sizeof(state_str), "%d", state);

    pubmsg.payload = state_str;
    pubmsg.payloadlen = strlen(state_str);
    pubmsg.qos = QOS;
    pubmsg.retained = 1;

    if ((rc = MQTTClient_publishMessage(client, TOPIC_RELAY_STATE, &pubmsg, &token)) != MQTTCLIENT_SUCCESS) {
        fprintf(stderr, "ERROR: Failed to publish state, return code %d\n", rc);
        MQTTClient_disconnect(client, TIMEOUT);
        MQTTClient_destroy(&client);
        return 1;
    }

    MQTTClient_waitForCompletion(client, token, TIMEOUT);
    printf("Published: %s = %s\n", TOPIC_RELAY_STATE, state_str);

    // Publish trigger (always 1 to force update)
    pubmsg.payload = "1";
    pubmsg.payloadlen = 1;
    pubmsg.qos = QOS;
    pubmsg.retained = 0;  // Don't retain trigger

    if ((rc = MQTTClient_publishMessage(client, TOPIC_RELAY_TRIGGER, &pubmsg, &token)) != MQTTCLIENT_SUCCESS) {
        fprintf(stderr, "ERROR: Failed to publish trigger, return code %d\n", rc);
        MQTTClient_disconnect(client, TIMEOUT);
        MQTTClient_destroy(&client);
        return 1;
    }

    MQTTClient_waitForCompletion(client, token, TIMEOUT);
    printf("Published: %s = 1\n", TOPIC_RELAY_TRIGGER);

    // Print description
    if (state >= 0 && state <= 7) {
        printf("\nRelay: state %d (%s)\n", state, state_desc[state]);
    } else if (state == 11) {
        printf("\nRelay: state 11 (special)\n");
    }

    // Disconnect
    MQTTClient_disconnect(client, TIMEOUT);
    MQTTClient_destroy(&client);

    return 0;
}
