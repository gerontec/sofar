/*
 * wifi_config.h - WiFi and MQTT configuration for Pico W
 *
 * IMPORTANT: Copy this file to wifi_config.h and fill in your credentials
 */

#ifndef WIFI_CONFIG_H
#define WIFI_CONFIG_H

// WiFi Configuration
#define WIFI_SSID "YOUR_WIFI_SSID"
#define WIFI_PASSWORD "YOUR_WIFI_PASSWORD"

// MQTT Configuration
#define MQTT_BROKER_IP "192.168.178.218"
#define MQTT_BROKER_PORT 1883
#define MQTT_CLIENT_ID "pico_ebox"
#define MQTT_TOPIC_SOC "ebox/soc"           // Topic for SOC (State of Charge)
#define MQTT_TOPIC_VOLTAGE "ebox/voltage"   // Topic for Voltage
#define MQTT_TOPIC_CURRENT "ebox/current"   // Topic for Current
#define MQTT_TOPIC_STATUS "ebox/status"     // Topic for status messages

// UART Configuration for RS232 Module
#define UART_ID uart0
#define UART_TX_PIN 0    // GPIO 0 - TX
#define UART_RX_PIN 1    // GPIO 1 - RX
#define UART_BAUD_RATE 115200

// E-Box Communication Settings
#define EBOX_COMMAND "bat"              // Default command to send
#define EBOX_TIMEOUT_MS 4000            // Read timeout in milliseconds
#define EBOX_MAX_LINES 100              // Maximum lines to buffer
#define EBOX_MAX_LINE_LEN 512           // Maximum line length

#endif // WIFI_CONFIG_H
