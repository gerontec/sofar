#ifndef CONFIG_H
#define CONFIG_H

#define VERSION "v1.13_CONFIGURABLE_LOCATION"

// File paths
#define LOCK_FILE "/tmp/soyo1min.lock"
#define LOG_FILE "/tmp/soyo1min_c.log"
#define INVERTER_FILE "/tmp/inverter.csv"
#define SOYOPOWER_FILE "/home/pi/soyopower.txt"
#define SUNRISE_SCRIPT "./sunrise.py" // Deprecated: using native C calculation

// Serial configuration
#define SERIAL_PORT "/dev/ttyUSB32"
#define SERIAL_BAUDRATE 4800
#define SERIAL_TIMEOUT 1

// Battery and power configuration
#define BATTERY_CAPACITY_KWH 30
#define MAX_POWER 999
#define MAX_BAT2_CURRENT 200
#define NIGHT_STANDARD_POWER 390
#define BAT2_SOC_MIN 9

// MQTT configuration
#define MQTT_BROKER "192.168.178.218"
#define MQTT_PORT 1883
#define MQTT_TOPIC "em0/54"
#define WP_POWER_DEFAULT 17

// Timing
#define UPDATE_INTERVAL_SEC 2

// Logging
#define MAX_LOG_SIZE_BYTES 102400  // 100 KB

// Location (Lenggries, Germany)
#define LATITUDE 47.6811
#define LONGITUDE 11.5732
#define SUNRISE_OFFSET_MIN 60   // Add 60 minutes to calculated sunrise
#define SUNSET_OFFSET_MIN -60   // Subtract 60 minutes from calculated sunset

// Status bytes
#define STATUS_ENTLADESCHUTZ    (1 << 0)  // 1
#define STATUS_BAT2_CHARGING    (1 << 1)  // 2
#define STATUS_BUYING_FROM_GRID (1 << 2)  // 4
#define STATUS_FEEDING_TO_GRID  (1 << 3)  // 8
#define STATUS_RESERVED         (1 << 4)  // 16
#define STATUS_DEFAULT          (1 << 5)  // 32

// Validation limits
#define MAX_GRID_W 40000
#define MAX_SOFARBAT_W 2999
#define MAX_SOC 100
#define MIN_SOC 0

#endif // CONFIG_H
