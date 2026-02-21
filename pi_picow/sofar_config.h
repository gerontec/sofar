/*
 * sofar_config.h - Configuration for Sofar Inverter Reader on Pico W
 *
 * IMPORTANT: Copy this to sofar_config.h and adjust values for your setup
 */

#ifndef SOFAR_CONFIG_H
#define SOFAR_CONFIG_H

// WiFi Configuration
#define SOFAR_WIFI_SSID "YOUR_WIFI_SSID"
#define SOFAR_WIFI_PASSWORD "YOUR_WIFI_PASSWORD"

// MQTT Configuration
#define SOFAR_MQTT_BROKER "192.168.178.218"
#define SOFAR_MQTT_PORT 1883
#define SOFAR_MQTT_CLIENT_ID "pico_sofar"

// MQTT Topics to Publish (Inverter Data)
#define MQTT_PUB_GRID_W "sofar/grid_w"              // Grid power in W
#define MQTT_PUB_BAT_W "sofar/bat_w"                // Battery power in W
#define MQTT_PUB_SOC_BAT2 "sofar/soc_bat2"          // Battery 2 SOC in %
#define MQTT_PUB_SOC_BAT1 "sofar/soc_bat1"          // Battery 1 SOC in %
#define MQTT_PUB_BAT2_CURRENT "sofar/bat2_current"  // Battery 2 current in A
#define MQTT_PUB_STATE "sofar/state"                // Inverter state (0-7)
#define MQTT_PUB_STATUS "sofar/status"              // Status messages
#define MQTT_PUB_CSV "sofar/csv"                    // Full CSV line (backward compat)
#define MQTT_PUB_JSON "inverter/power_grid_exchange/json"  // JSON format (fox2db compat)

// RS232 UART Configuration for Modbus RTU
#define UART_MODBUS uart0
#define UART_MODBUS_TX_PIN 0     // GPIO 0 - TX
#define UART_MODBUS_RX_PIN 1     // GPIO 1 - RX
#define UART_MODBUS_BAUD 9600    // Sofar typically uses 9600 baud for Modbus

// Modbus Configuration
#define MODBUS_UNIT_ID 1         // Sofar inverter unit ID
#define MODBUS_TIMEOUT_MS 1000   // Response timeout

// Sofar Register Addresses (Modbus Holding Registers)
#define REG_GRID_POWER 0x0485        // Grid power (I16, 10W) - also known as ActivePower_PCC_Total
#define REG_BAT_POWER 0x0606         // Battery power (I16, 10W)
#define REG_BAT1_SOC 0x0608          // Battery 1 SOC (U16, %)
#define REG_BAT2_SOC 0x060A          // Battery 2 SOC (U16, %)
#define REG_BAT2_CURRENT 0x060C      // Battery 2 current (I16, 0.01A)
#define REG_INVERTER_STATE 0x0404    // Inverter state (U16, 0-7)

// Alternative register addresses (some Sofar models use different addresses)
#define REG_GRID_POWER_ALT 0x0212    // Alternative grid power register
#define REG_BAT_POWER_ALT 0x020C     // Alternative battery power register

// Update interval
#define UPDATE_INTERVAL_MS 2000      // Read every 2 seconds

// Modbus Frame Timeouts
#define MODBUS_T35_US 3500           // 3.5 character times at 9600 baud

#endif // SOFAR_CONFIG_H
