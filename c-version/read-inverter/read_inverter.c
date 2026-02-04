#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <modbus/modbus.h>
#include <ctype.h>
#include "read_config.h"

// Global register info array
static RegisterInfo register_info[MAX_REGISTERS];
static int register_count = 0;

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
                        if (field2) strncpy(info->name, field2, MAX_NAME_LENGTH - 1);
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

                        if (field5) strncpy(info->unit, field5, MAX_UNIT_LENGTH - 1);
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

// Check if register should be read (based on ALLREG flag)
int should_read_register(const char *unit) {
    if (ALLREG) return 1;

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
    strncpy(data->name, name, MAX_NAME_LENGTH - 1);
    strncpy(data->value, value, 255);
    strncpy(data->unit, unit, MAX_UNIT_LENGTH - 1);
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

// Main function
int main(void) {
    modbus_t *ctx;
    uint16_t tab_reg[BLOCK_SIZE];

    printf("Sofar Inverter Register Reader (C version)\n");
    printf("Reading mode: %s\n\n", ALLREG ? "all registers" : "filtered (kW, kWh, %% only)");

    // Read register definitions
    memset(register_info, 0, sizeof(register_info));
    if (read_register_info_from_csv(CSV_FILE) == 0) {
        fprintf(stderr, "Error: No registers loaded from CSV\n");
        return 1;
    }

    // Initialize Modbus
    ctx = modbus_new_rtu(SERIAL_PORT, BAUD_RATE, 'N', 8, 1);
    if (!ctx) {
        fprintf(stderr, "Error: Failed to create Modbus context: %s\n", modbus_strerror(errno));
        return 1;
    }

    modbus_set_slave(ctx, UNIT_ID);
    struct timeval timeout;
    timeout.tv_sec = TIMEOUT_SEC;
    timeout.tv_usec = TIMEOUT_USEC;
    modbus_set_response_timeout(ctx, timeout.tv_sec, timeout.tv_usec);

    if (modbus_connect(ctx) == -1) {
        fprintf(stderr, "Error: Connection failed: %s\n", modbus_strerror(errno));
        modbus_free(ctx);
        return 1;
    }

    printf("Connected to inverter\n");
    printf("\nRegisters with specified names:\n");

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

        // Read valid registers
        for (uint16_t addr = sec->start; addr <= sec->end; addr++) {
            // Check if register is valid in mask
            int bit_pos = addr - sec->start;
            if (!(mask & (1ULL << bit_pos))) continue;

            // Check if we have info for this register
            if (addr >= MAX_REGISTERS || !register_info[addr].valid) continue;
            if (strlen(register_info[addr].name) == 0) continue;

            RegisterInfo *info = &register_info[addr];

            // Filter by unit if needed
            if (!should_read_register(info->unit)) continue;

            // Print section header
            if (strcmp(info->section, current_section) != 0) {
                strncpy(current_section, info->section, MAX_SECTION_LENGTH - 1);
                printf("\n--- %s ---\n", current_section);
            }

            // Read register
            int reg_count = get_register_count(info->type);
            rc = modbus_read_registers(ctx, addr, reg_count, tab_reg);

            if (rc == -1) {
                printf("0x%04X: %s (%s) - %s : Unable to read register\n",
                       addr, info->name, reg_type_to_string(info->type), info->unit);
                continue;
            }

            // Decode value
            char value_str[256];
            if (decode_value(tab_reg, rc, info->type, info->accuracy, value_str, sizeof(value_str))) {
                printf("0x%04X: %s (%s) - %s : %s\n",
                       addr, info->name, reg_type_to_string(info->type), info->unit, value_str);
                add_register_data(info->section, info->name, value_str, info->unit);
            } else {
                printf("0x%04X: %s (%s) - %s : Unable to decode value\n",
                       addr, info->name, reg_type_to_string(info->type), info->unit);
            }
        }
    }

    // Save results
    save_raw_csv(RAW_OUTPUT_FILE);

    // Cleanup
    modbus_close(ctx);
    modbus_free(ctx);
    free(register_data);

    printf("\nFinished reading inverter data\n");

    return 0;
}
