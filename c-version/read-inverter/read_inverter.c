#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <modbus/modbus.h>
#include <ctype.h>
#include <getopt.h>
#include "read_config.h"

// Runtime configuration structure
typedef struct {
    char serial_port[256];
    int baud_rate;
    int unit_id;
    int timeout_sec;
    int timeout_usec;
    char csv_file[512];
    char raw_output_file[512];
    char pivoted_output_file[512];
    int allreg;
    uint16_t max_register;
    int block_size;
} Config;

// Global register info array
static RegisterInfo register_info[MAX_REGISTERS];
static int register_count = 0;

// Forward declaration for embedded registers (if available)
extern int load_embedded_registers(RegisterInfo *register_info, int max_registers) __attribute__((weak));

// Register data storage
typedef struct {
    char section[MAX_SECTION_LENGTH];
    char name[MAX_NAME_LENGTH];
    char value[256];
    char unit[MAX_UNIT_LENGTH];
} RegisterData;

static RegisterData *register_data = NULL;
static int data_count = 0;
static int data_capacity = 0;

// Helper: Convert string to register type
RegisterType string_to_reg_type(const char *str) {
    if (!str || !*str) return REG_TYPE_U16;

    char upper[32] = {0};
    for (int i = 0; i < 31 && str[i]; i++) {
        upper[i] = toupper(str[i]);
    }

    if (strcmp(upper, "U16") == 0) return REG_TYPE_U16;
    if (strcmp(upper, "I16") == 0) return REG_TYPE_I16;
    if (strcmp(upper, "U32") == 0) return REG_TYPE_U32;
    if (strcmp(upper, "I32") == 0) return REG_TYPE_I32;
    if (strcmp(upper, "U64") == 0) return REG_TYPE_U64;
    if (strcmp(upper, "BCD16") == 0) return REG_TYPE_BCD16;
    if (strcmp(upper, "ASCII") == 0) return REG_TYPE_ASCII;

    return REG_TYPE_UNKNOWN;
}

const char* reg_type_to_string(RegisterType type) {
    switch (type) {
        case REG_TYPE_U16: return "U16";
        case REG_TYPE_I16: return "I16";
        case REG_TYPE_U32: return "U32";
        case REG_TYPE_I32: return "I32";
        case REG_TYPE_U64: return "U64";
        case REG_TYPE_BCD16: return "BCD16";
        case REG_TYPE_ASCII: return "ASCII";
        default: return "UNKNOWN";
    }
}

// Helper: Parse hex address range
int parse_address_range(const char *addr_str, uint16_t *start, uint16_t *end) {
    char buffer[64];
    strncpy(buffer, addr_str, sizeof(buffer) - 1);
    buffer[sizeof(buffer) - 1] = '\0';

    // Remove spaces
    char *p = buffer;
    char *dst = buffer;
    while (*p) {
        if (!isspace(*p)) *dst++ = *p;
        p++;
    }
    *dst = '\0';

    // Check for range separators
    char *sep = strstr(buffer, "____");
    if (!sep) sep = strchr(buffer, '-');

    if (sep) {
        *sep = '\0';
        char *end_str = sep + (strstr(buffer, "____") ? 4 : 1);

        *start = (uint16_t)strtol(buffer, NULL, 16);
        if (*end_str) {
            *end = (uint16_t)strtol(end_str, NULL, 16);
        } else {
            *end = *start;
        }
    } else {
        *start = *end = (uint16_t)strtol(buffer, NULL, 16);
    }

    return (*start <= MAX_REGISTER) ? 1 : 0;
}

// Parse CSV field (handle quoted fields)
char* parse_csv_field(char **line_ptr, char delimiter) {
    if (!line_ptr || !*line_ptr) return NULL;

    char *start = *line_ptr;
    char *field = NULL;
    int in_quotes = 0;
    char *p = start;

    // Skip leading whitespace
    while (*p && isspace(*p)) p++;
    start = p;

    // Check if field starts with quote
    if (*p == '"') {
        in_quotes = 1;
        p++;
        start = p;
    }

    // Find end of field
    while (*p) {
        if (in_quotes) {
            if (*p == '"') {
                if (*(p + 1) == '"') {
                    p += 2; // Skip escaped quote
                } else {
                    in_quotes = 0;
                    *p = '\0';
                    p++;
                    break;
                }
            } else {
                p++;
            }
        } else {
            if (*p == delimiter) {
                *p = '\0';
                p++;
                break;
            }
            p++;
        }
    }

    field = strdup(start);
    *line_ptr = *p ? p : NULL;

    return field;
}

// Read register info from CSV
int read_register_info_from_csv(const char *csv_file) {
    FILE *fp = fopen(csv_file, "r");
    if (!fp) {
        fprintf(stderr, "Error: Cannot open CSV file: %s\n", csv_file);
        return 0;
    }

    char line[MAX_LINE_LENGTH];
    char current_section[MAX_SECTION_LENGTH] = "";
    int line_num = 0;

    // Skip header
    if (fgets(line, sizeof(line), fp)) {
        line_num++;
    }

    while (fgets(line, sizeof(line), fp)) {
        line_num++;

        // Remove newline
        char *newline = strchr(line, '\n');
        if (newline) *newline = '\0';

        char *line_ptr = line;
        char *field0 = parse_csv_field(&line_ptr, ';');
        char *field1 = parse_csv_field(&line_ptr, ';');
        char *field2 = parse_csv_field(&line_ptr, ';');
        char *field3 = parse_csv_field(&line_ptr, ';');
        char *field4 = parse_csv_field(&line_ptr, ';');
        char *field5 = parse_csv_field(&line_ptr, ';');

        if (!field1 || strlen(field1) == 0) {
            // New section
            if (field0 && strlen(field0) > 0) {
                strncpy(current_section, field0, MAX_SECTION_LENGTH - 1);
                current_section[MAX_SECTION_LENGTH - 1] = '\0';
                printf("New section: %s\n", current_section);
            }
        } else {
            // Register definition
            uint16_t start_addr, end_addr;
            if (parse_address_range(field1, &start_addr, &end_addr)) {
                for (uint16_t addr = start_addr; addr <= end_addr && addr <= MAX_REGISTER; addr++) {
                    if (addr < MAX_REGISTERS) {
                        RegisterInfo *info = &register_info[addr];
                        strncpy(info->section, current_section, MAX_SECTION_LENGTH - 1);
                        info->section[MAX_SECTION_LENGTH - 1] = '\0';
                        if (field2) {
                            strncpy(info->name, field2, MAX_NAME_LENGTH - 1);
                            info->name[MAX_NAME_LENGTH - 1] = '\0';
                        }
                        info->type = string_to_reg_type(field3);

                        // Parse accuracy
                        if (field4) {
                            char *acc_str = field4;
                            // Replace comma with dot
                            for (char *p = acc_str; *p; p++) {
                                if (*p == ',') *p = '.';
                            }
                            info->accuracy = atof(acc_str);
                            if (info->accuracy == 0.0) info->accuracy = 1.0;
                        } else {
                            info->accuracy = 1.0;
                        }

                        if (field5) {
                            strncpy(info->unit, field5, MAX_UNIT_LENGTH - 1);
                            info->unit[MAX_UNIT_LENGTH - 1] = '\0';
                        }
                        info->valid = 1;
                        register_count++;
                    }
                }
            }
        }

        free(field0);
        free(field1);
        free(field2);
        free(field3);
        free(field4);
        free(field5);
    }

    fclose(fp);
    printf("Read %d register definitions from CSV\n", register_count);
    return register_count;
}

// Check if register should be read (based on config allreg flag)
int should_read_register(const char *unit, const Config *config) {
    if (config->allreg) return 1;

    if (!unit || !*unit) return 0;

    char unit_lower[MAX_UNIT_LENGTH];
    for (int i = 0; unit[i] && i < MAX_UNIT_LENGTH - 1; i++) {
        unit_lower[i] = tolower(unit[i]);
        unit_lower[i + 1] = '\0';
    }

    return (strcmp(unit_lower, "kw") == 0 ||
            strcmp(unit_lower, "kwh") == 0 ||
            strcmp(unit_lower, "%") == 0);
}

// Decode register value
int decode_value(uint16_t *registers, int reg_count, RegisterType type,
                 double accuracy, char *output, size_t output_size) {
    if (!registers || !output) return 0;

    switch (type) {
        case REG_TYPE_U16: {
            uint16_t value = registers[0];
            snprintf(output, output_size, "%.4f", value * accuracy);
            return 1;
        }
        case REG_TYPE_I16: {
            int16_t value = (int16_t)registers[0];
            snprintf(output, output_size, "%.4f", value * accuracy);
            return 1;
        }
        case REG_TYPE_U32: {
            if (reg_count < 2) return 0;
            uint32_t value = ((uint32_t)registers[0] << 16) | registers[1];
            snprintf(output, output_size, "%.4f", value * accuracy);
            return 1;
        }
        case REG_TYPE_I32: {
            if (reg_count < 2) return 0;
            int32_t value = ((int32_t)registers[0] << 16) | registers[1];
            snprintf(output, output_size, "%.4f", value * accuracy);
            return 1;
        }
        case REG_TYPE_U64: {
            if (reg_count < 4) return 0;
            uint64_t value = ((uint64_t)registers[0] << 48) |
                           ((uint64_t)registers[1] << 32) |
                           ((uint64_t)registers[2] << 16) |
                           registers[3];
            snprintf(output, output_size, "%.4f", value * accuracy);
            return 1;
        }
        case REG_TYPE_BCD16: {
            uint16_t bcd = registers[0];
            int value = ((bcd >> 12) & 0xF) * 1000 +
                       ((bcd >> 8) & 0xF) * 100 +
                       ((bcd >> 4) & 0xF) * 10 +
                       (bcd & 0xF);
            snprintf(output, output_size, "%d", value);
            return 1;
        }
        case REG_TYPE_ASCII: {
            char ascii[256] = {0};
            int pos = 0;
            for (int i = 0; i < reg_count && pos < 255; i++) {
                uint8_t high = registers[i] >> 8;
                uint8_t low = registers[i] & 0xFF;
                if (high && isprint(high)) ascii[pos++] = high;
                if (low && isprint(low)) ascii[pos++] = low;
            }
            ascii[pos] = '\0';
            snprintf(output, output_size, "\"%s\"", ascii);
            return 1;
        }
        default:
            return 0;
    }
}

// Get register count for type
int get_register_count(RegisterType type) {
    switch (type) {
        case REG_TYPE_U64: return 4;
        case REG_TYPE_U32:
        case REG_TYPE_I32: return 2;
        default: return 1;
    }
}

// Add register data to array
void add_register_data(const char *section, const char *name,
                       const char *value, const char *unit) {
    if (data_count >= data_capacity) {
        data_capacity = data_capacity == 0 ? 1024 : data_capacity * 2;
        register_data = realloc(register_data, data_capacity * sizeof(RegisterData));
        if (!register_data) {
            fprintf(stderr, "Error: Memory allocation failed\n");
            exit(1);
        }
    }

    RegisterData *data = &register_data[data_count++];
    strncpy(data->section, section, MAX_SECTION_LENGTH - 1);
    data->section[MAX_SECTION_LENGTH - 1] = '\0';
    strncpy(data->name, name, MAX_NAME_LENGTH - 1);
    data->name[MAX_NAME_LENGTH - 1] = '\0';
    strncpy(data->value, value, 255);
    data->value[255] = '\0';
    strncpy(data->unit, unit, MAX_UNIT_LENGTH - 1);
    data->unit[MAX_UNIT_LENGTH - 1] = '\0';
}

// Save raw data to CSV
void save_raw_csv(const char *filename) {
    FILE *fp = fopen(filename, "w");
    if (!fp) {
        fprintf(stderr, "Error: Cannot write to %s\n", filename);
        return;
    }

    fprintf(fp, "section,name,value,unit\n");
    for (int i = 0; i < data_count; i++) {
        RegisterData *data = &register_data[i];
        fprintf(fp, "\"%s\",\"%s\",%s,\"%s\"\n",
                data->section, data->name, data->value, data->unit);
    }

    fclose(fp);
    printf("\nRaw register data saved to '%s' (%d registers)\n", filename, data_count);
}

// Print version information
void print_version(void) {
    printf("%s version %s\n", PROGRAM_NAME, VERSION);
    printf("C implementation of Sofar Inverter Register Reader\n");
    printf("Compiled: %s %s\n", __DATE__, __TIME__);
}

// Initialize configuration with default values
void init_config(Config *config) {
    strncpy(config->serial_port, SERIAL_PORT, sizeof(config->serial_port) - 1);
    config->serial_port[sizeof(config->serial_port) - 1] = '\0';
    config->baud_rate = BAUD_RATE;
    config->unit_id = UNIT_ID;
    config->timeout_sec = TIMEOUT_SEC;
    config->timeout_usec = TIMEOUT_USEC;
    strncpy(config->csv_file, CSV_FILE, sizeof(config->csv_file) - 1);
    config->csv_file[sizeof(config->csv_file) - 1] = '\0';
    strncpy(config->raw_output_file, RAW_OUTPUT_FILE, sizeof(config->raw_output_file) - 1);
    config->raw_output_file[sizeof(config->raw_output_file) - 1] = '\0';
    strncpy(config->pivoted_output_file, PIVOTED_OUTPUT_FILE, sizeof(config->pivoted_output_file) - 1);
    config->pivoted_output_file[sizeof(config->pivoted_output_file) - 1] = '\0';
    config->allreg = ALLREG;
    config->max_register = MAX_REGISTER;
    config->block_size = BLOCK_SIZE;
}

// Print help information
void print_help(const Config *config) {
    printf("Usage: %s [OPTIONS]\n", PROGRAM_NAME);
    printf("\n");
    printf("Sofar Inverter Register Reader - Reads Modbus RTU registers from inverter\n");
    printf("\n");
    printf("Options:\n");
    printf("  -h, --help                    Show this help message and exit\n");
    printf("  -v, --version                 Show version information and exit\n");
    printf("\n");
    printf("Serial Communication:\n");
    printf("  -p, --port <device>           Serial port (default: %s)\n", config->serial_port);
    printf("  -b, --baud <rate>             Baud rate (default: %d)\n", config->baud_rate);
    printf("  -u, --unit-id <id>            Modbus unit ID (default: %d)\n", config->unit_id);
    printf("  -t, --timeout <sec>           Response timeout in seconds (default: %d)\n", config->timeout_sec);
    printf("      --timeout-usec <usec>     Response timeout microseconds (default: %d)\n", config->timeout_usec);
    printf("\n");
    printf("File Paths:\n");
    printf("  -c, --csv <file>              Register definitions CSV (default: %s)\n", config->csv_file);
    printf("  -o, --output <file>           Raw output CSV file (default: %s)\n", config->raw_output_file);
    printf("      --pivoted-output <file>   Pivoted output CSV file (default: %s)\n", config->pivoted_output_file);
    printf("\n");
    printf("Register Options:\n");
    printf("  -a, --all-registers           Read all registers (default: %s)\n", config->allreg ? "yes" : "no");
    printf("  -f, --filter                  Filter kW/kWh/%% only (opposite of --all-registers)\n");
    printf("      --max-register <addr>     Maximum register address in hex (default: 0x%04X)\n", config->max_register);
    printf("      --block-size <size>       Modbus read block size (default: %d)\n", config->block_size);
    printf("\n");
    printf("Examples:\n");
    printf("  %s -p /dev/ttyUSB0 -b 9600 -u 1\n", PROGRAM_NAME);
    printf("  %s --all-registers --output /tmp/inverter.csv\n", PROGRAM_NAME);
    printf("  %s -c custom_registers.csv -o output.csv\n", PROGRAM_NAME);
    printf("\n");
}

// Main function
int main(int argc, char *argv[]) {
    modbus_t *ctx;
    Config config;

    // Initialize configuration with defaults
    init_config(&config);

    // Define long options
    static struct option long_options[] = {
        {"help",            no_argument,       0, 'h'},
        {"version",         no_argument,       0, 'v'},
        {"port",            required_argument, 0, 'p'},
        {"baud",            required_argument, 0, 'b'},
        {"unit-id",         required_argument, 0, 'u'},
        {"timeout",         required_argument, 0, 't'},
        {"timeout-usec",    required_argument, 0, 128},
        {"csv",             required_argument, 0, 'c'},
        {"output",          required_argument, 0, 'o'},
        {"pivoted-output",  required_argument, 0, 129},
        {"all-registers",   no_argument,       0, 'a'},
        {"filter",          no_argument,       0, 'f'},
        {"max-register",    required_argument, 0, 130},
        {"block-size",      required_argument, 0, 131},
        {0, 0, 0, 0}
    };

    // Parse command line arguments
    int opt;
    int option_index = 0;
    while ((opt = getopt_long(argc, argv, "hvp:b:u:t:c:o:af", long_options, &option_index)) != -1) {
        switch (opt) {
            case 'h':
                print_help(&config);
                return 0;
            case 'v':
                print_version();
                return 0;
            case 'p':
                strncpy(config.serial_port, optarg, sizeof(config.serial_port) - 1);
                config.serial_port[sizeof(config.serial_port) - 1] = '\0';
                break;
            case 'b':
                config.baud_rate = atoi(optarg);
                if (config.baud_rate <= 0) {
                    fprintf(stderr, "Error: Invalid baud rate: %s\n", optarg);
                    return 1;
                }
                break;
            case 'u':
                config.unit_id = atoi(optarg);
                if (config.unit_id < 0 || config.unit_id > 247) {
                    fprintf(stderr, "Error: Invalid unit ID (must be 0-247): %s\n", optarg);
                    return 1;
                }
                break;
            case 't':
                config.timeout_sec = atoi(optarg);
                if (config.timeout_sec < 0) {
                    fprintf(stderr, "Error: Invalid timeout: %s\n", optarg);
                    return 1;
                }
                break;
            case 128: // --timeout-usec
                config.timeout_usec = atoi(optarg);
                if (config.timeout_usec < 0) {
                    fprintf(stderr, "Error: Invalid timeout microseconds: %s\n", optarg);
                    return 1;
                }
                break;
            case 'c':
                strncpy(config.csv_file, optarg, sizeof(config.csv_file) - 1);
                config.csv_file[sizeof(config.csv_file) - 1] = '\0';
                break;
            case 'o':
                strncpy(config.raw_output_file, optarg, sizeof(config.raw_output_file) - 1);
                config.raw_output_file[sizeof(config.raw_output_file) - 1] = '\0';
                break;
            case 129: // --pivoted-output
                strncpy(config.pivoted_output_file, optarg, sizeof(config.pivoted_output_file) - 1);
                config.pivoted_output_file[sizeof(config.pivoted_output_file) - 1] = '\0';
                break;
            case 'a':
                config.allreg = 1;
                break;
            case 'f':
                config.allreg = 0;
                break;
            case 130: // --max-register
                config.max_register = (uint16_t)strtol(optarg, NULL, 16);
                break;
            case 131: // --block-size
                config.block_size = atoi(optarg);
                if (config.block_size <= 0 || config.block_size > 125) {
                    fprintf(stderr, "Error: Invalid block size (must be 1-125): %s\n", optarg);
                    return 1;
                }
                break;
            default:
                fprintf(stderr, "Try '%s --help' for more information.\n", PROGRAM_NAME);
                return 1;
        }
    }

    // Check for unexpected arguments
    if (optind < argc) {
        fprintf(stderr, "Error: Unexpected argument: %s\n", argv[optind]);
        fprintf(stderr, "Try '%s --help' for more information.\n", PROGRAM_NAME);
        return 1;
    }

    printf("Sofar Inverter Register Reader (C version %s)\n", VERSION);
    printf("Reading mode: %s\n", config.allreg ? "all registers" : "filtered (kW, kWh, %% only)");
    printf("Serial port: %s @ %d baud\n", config.serial_port, config.baud_rate);
    printf("Unit ID: %d, Timeout: %ds %dus\n\n", config.unit_id, config.timeout_sec, config.timeout_usec);

    // Read register definitions
    memset(register_info, 0, sizeof(register_info));

    // Try embedded registers first (if available)
    if (load_embedded_registers != NULL) {
        register_count = load_embedded_registers(register_info, MAX_REGISTERS);
        if (register_count > 0) {
            printf("Using embedded register definitions (%d registers)\n\n", register_count);
        }
    }

    // Fall back to CSV file if no embedded registers
    if (register_count == 0) {
        printf("Embedded registers not available, loading from CSV...\n");
        register_count = read_register_info_from_csv(config.csv_file);
        if (register_count == 0) {
            fprintf(stderr, "Error: No registers loaded from CSV: %s\n", config.csv_file);
            return 1;
        }
    }

    // Initialize Modbus
    ctx = modbus_new_rtu(config.serial_port, config.baud_rate, 'N', 8, 1);
    if (!ctx) {
        fprintf(stderr, "Error: Failed to create Modbus context: %s\n", modbus_strerror(errno));
        return 1;
    }

    modbus_set_slave(ctx, config.unit_id);
    struct timeval timeout;
    timeout.tv_sec = config.timeout_sec;
    timeout.tv_usec = config.timeout_usec;
    modbus_set_response_timeout(ctx, timeout.tv_sec, timeout.tv_usec);

    if (modbus_connect(ctx) == -1) {
        fprintf(stderr, "Error: Connection failed: %s\n", modbus_strerror(errno));
        modbus_free(ctx);
        return 1;
    }

    printf("Connected to inverter\n");
    printf("\nRegisters with specified names:\n");

    // Allocate buffer for reading registers
    uint16_t *tab_reg = (uint16_t *)malloc(config.block_size * sizeof(uint16_t));
    if (!tab_reg) {
        fprintf(stderr, "Error: Memory allocation failed\n");
        modbus_close(ctx);
        modbus_free(ctx);
        return 1;
    }

    // Define sections to read
    Section sections[] = {
        {0x0040, 0x007F, 0x0040},  // I General
        {0x0400, 0x043F, 0x0400},  // Realtime SysInfo1
        {0x0480, 0x04BF, 0x0480},  // Realtime GridOutput1
        {0x0500, 0x053F, 0x0500},  // Realtime EmergencyOutput1
        {0x0580, 0x05BF, 0x0580},  // Realtime Input PV1
        {0x0600, 0x063F, 0x0600},  // Realtime Input Bat1
        {0x0680, 0x06BF, 0x0680},  // Realtime ElectricityStatistics1
        {0x0700, 0x073F, 0x0700},  // Realtime CombinerInfo1
        {0x0800, 0x083F, 0x0800},  // Voltage Config
        {0x0900, 0x093F, 0x0900},  // Remote Config
        {0x1000, 0x105A, 0x1000},  // Config Basic1
        {0x1100, 0x113F, 0x1100},  // Remote Config
        {0x2000, 0x203F, 0x2000},  // Config Core1
        {0x5000, 0x503F, 0x5000},  // DCDC SysInfo
        {0x6000, 0x603F, 0x6000},  // PCU1
        {0x9000, 0x903F, 0x9000},  // BMS1 System
    };

    int num_sections = sizeof(sections) / sizeof(Section);
    char current_section[MAX_SECTION_LENGTH] = "";

    for (int s = 0; s < num_sections; s++) {
        Section *sec = &sections[s];

        // Read mask registers
        int rc = modbus_read_registers(ctx, sec->mask_address, 4, tab_reg);
        if (rc == -1) {
            fprintf(stderr, "Warning: Failed to read mask at 0x%04X: %s\n",
                    sec->mask_address, modbus_strerror(errno));
            continue;
        }

        uint64_t mask = ((uint64_t)tab_reg[0] << 48) | ((uint64_t)tab_reg[1] << 32) |
                       ((uint64_t)tab_reg[2] << 16) | tab_reg[3];

        // Collect valid registers to read
        uint16_t registers_to_read[64];
        int num_to_read = 0;

        for (uint16_t addr = sec->start; addr <= sec->end; addr++) {
            // Check if register is valid in mask
            int bit_pos = addr - sec->start;
            if (!(mask & (1ULL << bit_pos))) continue;

            // Check if we have info for this register
            if (addr >= MAX_REGISTERS || !register_info[addr].valid) continue;
            if (strlen(register_info[addr].name) == 0) continue;

            RegisterInfo *info = &register_info[addr];

            // Filter by unit if needed
            if (!should_read_register(info->unit, &config)) continue;

            registers_to_read[num_to_read++] = addr;
        }

        // Process collected registers
        for (int i = 0; i < num_to_read; i++) {
            uint16_t addr = registers_to_read[i];
            RegisterInfo *info = &register_info[addr];

            // Print section header
            if (strcmp(info->section, current_section) != 0) {
                strncpy(current_section, info->section, MAX_SECTION_LENGTH - 1);
                printf("\n--- %s ---\n", current_section);
            }

            // Try bulk read of up to 16 registers first
            int reg_count = get_register_count(info->type);
            int bulk_size = 16;
            uint16_t bulk_buffer[16];
            int use_bulk = 0;

            // Try bulk read
            rc = modbus_read_registers(ctx, addr, bulk_size, bulk_buffer);
            if (rc == bulk_size) {
                // Bulk read successful, copy needed registers
                use_bulk = 1;
                for (int j = 0; j < reg_count && j < bulk_size; j++) {
                    tab_reg[j] = bulk_buffer[j];
                }
            } else {
                // Bulk read failed, fallback to single register read
                rc = modbus_read_registers(ctx, addr, reg_count, tab_reg);
            }

            if (rc == -1) {
                printf("0x%04X: %s (%s) - %s : Unable to read register%s\n",
                       addr, info->name, reg_type_to_string(info->type), info->unit,
                       use_bulk ? "" : " (bulk read failed, single read also failed)");
                continue;
            }

            // Decode value
            char value_str[256];
            if (decode_value(tab_reg, reg_count, info->type, info->accuracy, value_str, sizeof(value_str))) {
                printf("0x%04X: %s (%s) - %s : %s%s\n",
                       addr, info->name, reg_type_to_string(info->type), info->unit, value_str,
                       use_bulk ? " [bulk]" : "");
                add_register_data(info->section, info->name, value_str, info->unit);
            } else {
                printf("0x%04X: %s (%s) - %s : Unable to decode value\n",
                       addr, info->name, reg_type_to_string(info->type), info->unit);
            }
        }
    }

    // Save results
    save_raw_csv(config.raw_output_file);

    // Cleanup
    free(tab_reg);
    modbus_close(ctx);
    modbus_free(ctx);
    free(register_data);

    printf("\nFinished reading inverter data\n");

    return 0;
}
