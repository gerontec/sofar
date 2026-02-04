#ifndef READ_CONFIG_H
#define READ_CONFIG_H

// Serial port configuration
#define SERIAL_PORT "/dev/ttyUSB32"
#define BAUD_RATE 9600
#define UNIT_ID 1
#define TIMEOUT_SEC 1
#define TIMEOUT_USEC 0

// File paths
#define CSV_FILE "/home/pi/python/sofarregister.csv"
#define RAW_OUTPUT_FILE "/tmp/raw.csv"
#define PIVOTED_OUTPUT_FILE "/tmp/pivoted_registers.csv"

// Register configuration
#define MAX_REGISTER 0x203F
#define BLOCK_SIZE 32
#define MAX_REGISTERS 4096
#define MAX_LINE_LENGTH 1024
#define MAX_NAME_LENGTH 256
#define MAX_UNIT_LENGTH 32
#define MAX_SECTION_LENGTH 256

// Feature flags
#define ALLREG 0  // Set to 1 to read all registers, 0 to filter kW/kWh/%

// Register types
typedef enum {
    REG_TYPE_U16,
    REG_TYPE_I16,
    REG_TYPE_U32,
    REG_TYPE_I32,
    REG_TYPE_U64,
    REG_TYPE_BCD16,
    REG_TYPE_ASCII,
    REG_TYPE_UNKNOWN
} RegisterType;

// Register info structure
typedef struct {
    char section[MAX_SECTION_LENGTH];
    char name[MAX_NAME_LENGTH];
    RegisterType type;
    double accuracy;
    char unit[MAX_UNIT_LENGTH];
    int valid;
} RegisterInfo;

// Section definition for reading
typedef struct {
    uint16_t start;
    uint16_t end;
    uint16_t mask_address;
} Section;

#endif // READ_CONFIG_H
