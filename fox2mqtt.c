/*
 * fox2mqtt.c - Sends JSON status from fox2db via MQTT
 * Replacement for fox2mqtt.py
 *
 * Usage:
 *   fox2mqtt '{"soc":40.0, "pcc":123, "state":1}'
 *   fox2mqtt --json '{"key":"value"}' --topic custom/topic
 *
 * Compile:
 *   gcc -std=c11 -o fox2mqtt fox2mqtt.c -lpaho-mqtt3c -lcjson
 */

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <getopt.h>
#include <cjson/cJSON.h>
#include <MQTTClient.h>

#define VERSION "1.0.0"
#define DEFAULT_BROKER "kellertreppe.fritz.box"
#define DEFAULT_PORT 1883
#define DEFAULT_TOPIC "fox2db/state"
#define DEFAULT_QOS 0
#define DEFAULT_RETAIN false
#define TIMEOUT_MS 4000
#define KEEPALIVE_SEC 10

typedef struct {
    char broker[256];
    int port;
    char topic[256];
    int qos;
    bool retain;
    char json_string[4096];
} Config;

static void init_config(Config *cfg) {
    strncpy(cfg->broker, DEFAULT_BROKER, sizeof(cfg->broker) - 1);
    cfg->broker[sizeof(cfg->broker) - 1] = '\0';
    cfg->port = DEFAULT_PORT;
    strncpy(cfg->topic, DEFAULT_TOPIC, sizeof(cfg->topic) - 1);
    cfg->topic[sizeof(cfg->topic) - 1] = '\0';
    cfg->qos = DEFAULT_QOS;
    cfg->retain = DEFAULT_RETAIN;
    cfg->json_string[0] = '\0';
}

static void print_usage(const char *prog) {
    printf("Usage: %s <json-string> [OPTIONS]\n", prog);
    printf("       %s --json <json-string> [OPTIONS]\n\n", prog);
    printf("Sends JSON status data via MQTT (replacement for fox2mqtt.py)\n\n");
    printf("Options:\n");
    printf("  -j, --json <string>      JSON string to send\n");
    printf("  -t, --topic <topic>      MQTT topic (default: %s)\n", DEFAULT_TOPIC);
    printf("  -b, --broker <host>      MQTT broker (default: %s)\n", DEFAULT_BROKER);
    printf("  -p, --port <port>        MQTT port (default: %d)\n", DEFAULT_PORT);
    printf("  -q, --qos <0|1|2>        QoS level (default: %d)\n", DEFAULT_QOS);
    printf("  -r, --retain             Set retain flag (default: off)\n");
    printf("  -h, --help               Show this help\n");
    printf("      --version            Show version\n\n");
    printf("Examples:\n");
    printf("  %s '{\"soc\":40.0,\"pcc\":-200,\"state\":1}'\n", prog);
    printf("  %s --json '{\"key\":\"value\"}' --topic custom/topic\n", prog);
    printf("  %s '{\"temp\":23.5}' --broker localhost --port 1883\n", prog);
}

static bool validate_json(const char *json_str) {
    cJSON *json = cJSON_Parse(json_str);
    if (json == NULL) {
        const char *error_ptr = cJSON_GetErrorPtr();
        if (error_ptr != NULL) {
            fprintf(stderr, "JSON parse error before: %s\n", error_ptr);
        } else {
            fprintf(stderr, "Invalid JSON format\n");
        }
        return false;
    }
    cJSON_Delete(json);
    return true;
}

static int publish_mqtt(const Config *cfg) {
    MQTTClient client;
    MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
    MQTTClient_message pubmsg = MQTTClient_message_initializer;
    MQTTClient_deliveryToken token;
    int rc;

    // Create broker URL
    char broker_url[512];
    snprintf(broker_url, sizeof(broker_url), "tcp://%s:%d", cfg->broker, cfg->port);

    // Create MQTT client
    rc = MQTTClient_create(&client, broker_url, "fox2mqtt_client",
                           MQTTCLIENT_PERSISTENCE_NONE, NULL);
    if (rc != MQTTCLIENT_SUCCESS) {
        fprintf(stderr, "Failed to create MQTT client, return code %d\n", rc);
        return 1;
    }

    // Set connection options
    conn_opts.keepAliveInterval = KEEPALIVE_SEC;
    conn_opts.cleansession = 1;
    conn_opts.connectTimeout = TIMEOUT_MS / 1000;

    // Connect to broker
    rc = MQTTClient_connect(client, &conn_opts);
    if (rc != MQTTCLIENT_SUCCESS) {
        fprintf(stderr, "Failed to connect to MQTT broker %s:%d, return code %d\n",
                cfg->broker, cfg->port, rc);
        MQTTClient_destroy(&client);
        return 1;
    }

    // Prepare message
    pubmsg.payload = (void *)cfg->json_string;
    pubmsg.payloadlen = strlen(cfg->json_string);
    pubmsg.qos = cfg->qos;
    pubmsg.retained = cfg->retain ? 1 : 0;

    // Publish message
    rc = MQTTClient_publishMessage(client, cfg->topic, &pubmsg, &token);
    if (rc != MQTTCLIENT_SUCCESS) {
        fprintf(stderr, "Failed to publish message, return code %d\n", rc);
        MQTTClient_disconnect(client, TIMEOUT_MS);
        MQTTClient_destroy(&client);
        return 1;
    }

    // Wait for message to be delivered
    rc = MQTTClient_waitForCompletion(client, token, TIMEOUT_MS);
    if (rc != MQTTCLIENT_SUCCESS) {
        fprintf(stderr, "Failed to wait for message delivery, return code %d\n", rc);
    } else {
        fprintf(stderr, "→ MQTT published (%zu bytes) → %s\n",
                strlen(cfg->json_string), cfg->topic);
    }

    // Disconnect and cleanup
    MQTTClient_disconnect(client, TIMEOUT_MS);
    MQTTClient_destroy(&client);

    return rc == MQTTCLIENT_SUCCESS ? 0 : 1;
}

int main(int argc, char *argv[]) {
    Config config;
    init_config(&config);

    static struct option long_options[] = {
        {"json",    required_argument, 0, 'j'},
        {"topic",   required_argument, 0, 't'},
        {"broker",  required_argument, 0, 'b'},
        {"port",    required_argument, 0, 'p'},
        {"qos",     required_argument, 0, 'q'},
        {"retain",  no_argument,       0, 'r'},
        {"help",    no_argument,       0, 'h'},
        {"version", no_argument,       0, 'V'},
        {0, 0, 0, 0}
    };

    // Check for positional JSON argument first
    if (argc > 1 && argv[1][0] != '-') {
        strncpy(config.json_string, argv[1], sizeof(config.json_string) - 1);
        config.json_string[sizeof(config.json_string) - 1] = '\0';
        optind = 2; // Start parsing options from 3rd argument
    }

    int opt;
    while ((opt = getopt_long(argc, argv, "j:t:b:p:q:rhV", long_options, NULL)) != -1) {
        switch (opt) {
            case 'j':
                strncpy(config.json_string, optarg, sizeof(config.json_string) - 1);
                config.json_string[sizeof(config.json_string) - 1] = '\0';
                break;
            case 't':
                strncpy(config.topic, optarg, sizeof(config.topic) - 1);
                config.topic[sizeof(config.topic) - 1] = '\0';
                break;
            case 'b':
                strncpy(config.broker, optarg, sizeof(config.broker) - 1);
                config.broker[sizeof(config.broker) - 1] = '\0';
                break;
            case 'p':
                config.port = atoi(optarg);
                if (config.port <= 0 || config.port > 65535) {
                    fprintf(stderr, "Invalid port number: %s\n", optarg);
                    return 1;
                }
                break;
            case 'q':
                config.qos = atoi(optarg);
                if (config.qos < 0 || config.qos > 2) {
                    fprintf(stderr, "Invalid QoS level: %s (must be 0, 1, or 2)\n", optarg);
                    return 1;
                }
                break;
            case 'r':
                config.retain = true;
                break;
            case 'V':
                printf("fox2mqtt v%s\n", VERSION);
                return 0;
            case 'h':
            default:
                print_usage(argv[0]);
                return opt == 'h' ? 0 : 1;
        }
    }

    // Validate that JSON string was provided
    if (config.json_string[0] == '\0') {
        fprintf(stderr, "Error: No JSON string provided\n\n");
        fprintf(stderr, "Example:\n");
        fprintf(stderr, "  %s '{\"soc\":40.0,\"pcc\":-200,\"state\":1}'\n", argv[0]);
        return 1;
    }

    // Validate JSON format
    if (!validate_json(config.json_string)) {
        fprintf(stderr, "\nInput was: %s\n", config.json_string);
        return 1;
    }

    // Publish to MQTT
    return publish_mqtt(&config);
}
