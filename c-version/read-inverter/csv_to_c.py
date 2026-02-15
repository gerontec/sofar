#!/usr/bin/env python3
"""
CSV to C Converter for Register Definitions
Converts sofarregister.csv into embedded C code
"""

import sys
import csv

def escape_string(s):
    """Escape string for C"""
    return s.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')

def parse_address_range(addr_str):
    """Parse hex address range"""
    addr_str = addr_str.replace(' ', '')

    # Check for range separators
    if '____' in addr_str:
        parts = addr_str.split('____')
    elif '-' in addr_str:
        parts = addr_str.split('-')
    else:
        parts = [addr_str, addr_str]

    try:
        start = int(parts[0], 16)
        end = int(parts[1], 16) if len(parts) > 1 and parts[1] else start
        return start, end
    except:
        return None, None

def type_to_enum(type_str):
    """Convert type string to C enum"""
    type_map = {
        'U16': 'REG_TYPE_U16',
        'I16': 'REG_TYPE_I16',
        'U32': 'REG_TYPE_U32',
        'I32': 'REG_TYPE_I32',
        'U64': 'REG_TYPE_U64',
        'BCD16': 'REG_TYPE_BCD16',
        'ASCII': 'REG_TYPE_ASCII',
    }
    return type_map.get(type_str.upper().strip(), 'REG_TYPE_UNKNOWN')

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <sofarregister.csv>")
        sys.exit(1)

    csv_file = sys.argv[1]

    # Read CSV
    registers = []
    current_section = ""

    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        next(reader)  # Skip header

        for row in reader:
            if not row or len(row) < 2:
                continue

            if not row[1] and row[0].strip():
                # New section
                current_section = row[0].strip()
            elif row[1]:
                # Register definition
                start, end = parse_address_range(row[1])
                if start is None or start > 0x203F:
                    continue

                name = row[2].strip() if len(row) > 2 else ""
                reg_type = row[3].strip() if len(row) > 3 else "U16"
                accuracy_str = row[4].strip().replace(',', '.') if len(row) > 4 else "1"
                unit = row[5].strip() if len(row) > 5 else ""

                # Parse accuracy
                try:
                    accuracy = float(''.join(c for c in accuracy_str if c.isdigit() or c == '.'))
                    if accuracy == 0:
                        accuracy = 1.0
                except:
                    accuracy = 1.0

                # Add register entries
                for addr in range(start, min(end + 1, 0x203F + 1)):
                    registers.append({
                        'address': addr,
                        'section': current_section,
                        'name': name,
                        'type': reg_type,
                        'accuracy': accuracy,
                        'unit': unit
                    })

    # Generate C code
    print("// Auto-generated from sofarregister.csv")
    print("// DO NOT EDIT MANUALLY - Regenerate with csv_to_c.py")
    print()
    print("#include <string.h>")
    print("#include \"read_config.h\"")
    print()
    print("// Embedded register definitions")
    print(f"#define EMBEDDED_REGISTER_COUNT {len(registers)}")
    print()
    print("typedef struct {")
    print("    uint16_t address;")
    print("    const char *section;")
    print("    const char *name;")
    print("    RegisterType type;")
    print("    double accuracy;")
    print("    const char *unit;")
    print("} EmbeddedRegisterDef;")
    print()
    print("static const EmbeddedRegisterDef embedded_registers[] = {")

    for reg in registers:
        print(f"    {{0x{reg['address']:04X}, "
              f"\"{escape_string(reg['section'])}\", "
              f"\"{escape_string(reg['name'])}\", "
              f"{type_to_enum(reg['type'])}, "
              f"{reg['accuracy']}, "
              f"\"{escape_string(reg['unit'])}\"}},")

    print("};")
    print()
    print("// Function to load embedded registers")
    print("int load_embedded_registers(RegisterInfo *register_info, int max_registers) {")
    print("    int count = 0;")
    print("    for (int i = 0; i < EMBEDDED_REGISTER_COUNT; i++) {")
    print("        const EmbeddedRegisterDef *def = &embedded_registers[i];")
    print("        if (def->address >= max_registers) continue;")
    print()
    print("        RegisterInfo *info = &register_info[def->address];")
    print("        strncpy(info->section, def->section, MAX_SECTION_LENGTH - 1);")
    print("        info->section[MAX_SECTION_LENGTH - 1] = '\\0';")
    print("        strncpy(info->name, def->name, MAX_NAME_LENGTH - 1);")
    print("        info->name[MAX_NAME_LENGTH - 1] = '\\0';")
    print("        info->type = def->type;")
    print("        info->accuracy = def->accuracy;")
    print("        strncpy(info->unit, def->unit, MAX_UNIT_LENGTH - 1);")
    print("        info->unit[MAX_UNIT_LENGTH - 1] = '\\0';")
    print("        info->valid = 1;")
    print("        count++;")
    print("    }")
    print("    return count;")
    print("}")

if __name__ == '__main__':
    main()
