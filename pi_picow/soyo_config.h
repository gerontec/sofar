/*
 * soyo_config.h - Configuration for Soyo Inverter Control on Pico W
 *
 * IMPORTANT: Copy this to soyo_config.h and adjust values for your setup
 */

#ifndef SOYO_CONFIG_H
#define SOYO_CONFIG_H

// WiFi Configuration
#define SOYO_WIFI_SSID "YOUR_WIFI_SSID"
#define SOYO_WIFI_PASSWORD "YOUR_WIFI_PASSWORD"

// MQTT Configuration
#define SOYO_MQTT_BROKER "192.168.178.218"
#define SOYO_MQTT_PORT 1883
#define SOYO_MQTT_CLIENT_ID "pico_soyo"

// MQTT Topics to Subscribe (Inverter Data)
#define MQTT_TOPIC_GRID "sofar/grid_w"          // Grid power in W (+ = feeding, - = buying)
#define MQTT_TOPIC_SOC "sofar/soc_bat2"         // Battery SOC in %
#define MQTT_TOPIC_BAT_CURRENT "sofar/bat2_current"  // Battery current in A
#define MQTT_TOPIC_BAT_POWER "sofar/bat_w"      // Battery power in W
#define MQTT_TOPIC_STATE "sofar/state"          // Inverter state (0-7)
#define MQTT_TOPIC_WP_POWER "em0/54"            // Heat pump power in VA

// MQTT Topics to Publish (Status)
#define MQTT_TOPIC_SOYO_POWER "soyo/power"      // Current power setting sent to Soyo
#define MQTT_TOPIC_SOYO_STATUS "soyo/status"    // Status messages

// RS485 UART Configuration
#define UART_RS485 uart1
#define UART_RS485_TX_PIN 4     // GPIO 4 - TX
#define UART_RS485_RX_PIN 5     // GPIO 5 - RX (not used, Soyo only receives)
#define UART_RS485_DE_PIN 6     // GPIO 6 - DE/RE (Driver/Receiver Enable)
#define UART_RS485_BAUD 4800    // Soyo uses 4800 baud

// Battery and Power Configuration
#define BATTERY_CAPACITY_KWH 30
#define MAX_POWER 999                   // Maximum power to Soyo inverter (W)
#define MAX_BAT2_CURRENT 200            // Maximum battery current (A)
#define NIGHT_STANDARD_POWER 390        // Base load during night (W)
#define BAT2_SOC_MIN 9                  // Minimum SOC for discharge protection (%)

// Control Loop Timing
#define UPDATE_INTERVAL_MS 2000         // Update every 2 seconds

// Grid Thresholds
#define GRID_FEEDING_THRESHOLD 200      // Grid > 200W = feeding to grid (PV excess)
#define GRID_BUYING_THRESHOLD -100      // Grid < -100W = buying from grid

// Heat Pump Power Threshold (Winter months)
#define WP_POWER_THRESHOLD 390          // If WP > 390W, cap power

// Location for Sunrise/Sunset Calculation (Lenggries, Germany)
#define LATITUDE 47.6811
#define LONGITUDE 11.5732
#define SUNRISE_OFFSET_MIN 60           // Add 60 minutes to sunrise
#define SUNSET_OFFSET_MIN -60           // Subtract 60 minutes from sunset

// Validation Limits
#define MAX_GRID_W 40000
#define MAX_SOFARBAT_W 2999
#define MAX_SOC 100
#define MIN_SOC 0

// Timeout for MQTT data validity (ms)
#define MQTT_DATA_TIMEOUT_MS 60000      // 60 seconds

#endif // SOYO_CONFIG_H
