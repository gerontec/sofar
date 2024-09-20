#!/usr/bin/python3

import sys
import csv
import time
from pymodbus.client.serial import ModbusSerialClient
import logging
from typing import Dict, List, Any, Optional
import struct
import pandas as pd

TIMEOUT = 1

# Set up logging
logging.basicConfig(filename='sofar_inverter.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
SERIAL_PORT = '/dev/ttyUSB32'
BAUD_RATE = 9600
UNIT_ID = 3
CSV_FILE = '/home/pi/python/sofarregister.csv'
MAX_REGISTER = 0x1324
BLOCK_SIZE = 32  # Read 32 registers at a time

def read_register_info_from_csv(csv_file: str) -> Dict[int, Dict[str, Any]]:
    register_info = {}
    current_section = ""

    with open(csv_file, mode='r', encoding='utf-8') as infile:
        reader = csv.reader(infile, delimiter=';')
        next(reader)  # Skip header

        for row in reader:
            if row and len(row) >= 2:
                if not row[1] and row[0].strip():  # New section
                    current_section = row[0].strip()
                    print(f"New section detected: {current_section}")
                elif row[1]:  # Register info
                    address_range = row[1].replace(" ", "")
                    try:
                        if '____' in address_range:
                            start, end = map(lambda x: int(x, 16), address_range.split('____'))
                        elif '-' in address_range:
                            parts = address_range.split('-')
                            start = int(parts[0], 16)
                            end = int(parts[1], 16) if parts[1] else start
                        else:
                            start = end = int(address_range, 16)

                        for address in range(start, end + 1):
                            if address <= MAX_REGISTER:
                                accuracy_str = row[4].replace(',', '.')
                                try:
                                    accuracy = float(''.join(filter(lambda x: x.isdigit() or x == '.', accuracy_str)))
                                except ValueError:
                                    accuracy = 1  # Default to 1 if conversion fails

                                register_info[address] = {
                                    'section': current_section,
                                    'name': row[2].strip(),
                                    'type': row[3].strip().upper() if row[3].strip() else 'U16',
                                    'accuracy': accuracy,
                                    'unit': row[5].strip() if len(row) > 5 else ''
                                }
                    except ValueError as e:
                        logging.warning(f"Invalid address range '{address_range}' for '{row[2]}': {e}")

    return register_info

def read_register_block(client: ModbusSerialClient, start_address: int, count: int) -> Optional[List[int]]:
    try:
        result = client.read_holding_registers(start_address, count, slave=UNIT_ID)
        if not result.isError():
            return result.registers
        logging.error(f"Error reading registers at address 0x{start_address:04X}: {result}")
        return None
    except Exception as e:
        logging.error(f"Exception reading registers at address 0x{start_address:04X}: {e}")
        return None

def decode_value(registers: List[int], reg_type: str, accuracy: float) -> Optional[Any]:
    if not registers:
        return None

    try:
        if reg_type == 'U16':
            value = registers[0]
        elif reg_type == 'I16':
            value = registers[0]
            if value >= 32768:
                value -= 65536
        elif reg_type == 'U32':
            value = (registers[0] << 16) | registers[1] if len(registers) >= 2 else registers[0]
        elif reg_type == 'I32':
            value = (registers[0] << 16) | registers[1] if len(registers) >= 2 else registers[0]
            if value >= 2147483648:
                value -= 4294967296
        elif reg_type == 'U64':
            if len(registers) >= 4:
                value = (registers[0] << 48) | (registers[1] << 32) | (registers[2] << 16) | registers[3]
            elif len(registers) >= 2:
                value = (registers[0] << 16) | registers[1]
            else:
                value = registers[0]
        elif reg_type == 'BCD16':
            value = int(f"{registers[0]:04x}")
        elif reg_type == 'ASCII':
            ascii_chars = []
            for reg in registers:
                high_byte = reg >> 8
                low_byte = reg & 0xFF
                if high_byte:
                    ascii_chars.append(chr(high_byte))
                if low_byte:
                    ascii_chars.append(chr(low_byte))
            return ''.join(ascii_chars).strip('\x00')
        else:
            logging.warning(f"Unknown register type: {reg_type}")
            return None

        return value * accuracy if reg_type != 'ASCII' else value
    except Exception as e:
        logging.error(f"Error decoding value: {e}")
        return None

def read_address_mask(client, start_address):
    mask_registers = read_register_block(client, start_address, 4)
    if mask_registers:
        return int.from_bytes(struct.pack('>HHHH', *mask_registers), byteorder='big')
    return 0

def get_valid_registers(mask, start_address, end_address):
    valid_registers = []
    for address in range(start_address, end_address + 1):
        if mask & (1 << (address - start_address)):
            valid_registers.append(address)
    return valid_registers

def pivot_registers(register_data):
    df = pd.DataFrame(register_data)
    
    # Remove the "I General" prefix from the section names
    df['section'] = df['section'].str.replace(r'^I General\s*[（(].*?[）)]?\s*', '', regex=True)
    
    pivoted = df.pivot(index='section', columns='name', values='value')
    pivoted = pivoted.reset_index()
    
    # Reorder columns to ensure 'section' is the first column
    cols = ['section'] + [col for col in pivoted.columns if col != 'section']
    pivoted = pivoted[cols]
    
    return pivoted

def main():
    global TIMEOUT
    client = ModbusSerialClient(method='rtu', port=SERIAL_PORT, baudrate=BAUD_RATE,
                                parity='N', stopbits=1, bytesize=8, timeout=TIMEOUT)

    if not client.connect():
        logging.error("Failed to connect to the inverter")
        print("Failed to connect to the inverter", file=sys.stderr)
        return

    logging.info("Connected to the inverter")
    register_info = read_register_info_from_csv(CSV_FILE)
    logging.info(f"Read {len(register_info)} register definitions from CSV")

    print("Registers with specified names:")
    current_section = ""
    register_data = []

    sections = [
        (0x0480, 0x04BF, 0x0480),
        (0x0500, 0x053F, 0x0500),
        (0x0580, 0x05BF, 0x0580),
        (0x0600, 0x063F, 0x0600),
        (0x0680, 0x06BF, 0x0680),
    ]

    for start, end, mask_address in sections:
        mask = read_address_mask(client, mask_address)
        valid_registers = get_valid_registers(mask, start, end)

        for address in valid_registers:
            if address in register_info and register_info[address]['name']:
                info = register_info[address]
                if info['section'] != current_section:
                    current_section = info['section']
                    print(f"\n--- {current_section} ---")

                reg_count = 4 if info['type'] == 'U64' else (2 if info['type'] in ['U32', 'I32'] else 1)
                registers = read_register_block(client, address, reg_count)

                if registers:
                    value = decode_value(registers, info['type'], info['accuracy'])
                    if value is not None:
                        formatted_value = f'"{value}"' if info['type'] == 'ASCII' else f"{value:.4f}" if isinstance(value, (int, float)) else str(value)
                        print(f"0x{address:04X}: {info['name']} ({info['type']}) - {info['unit']} : {formatted_value}")
                        register_data.append({
                            'section': info['section'],
                            'name': info['name'],
                            'value': value,
                            'unit': info['unit']
                        })
                    else:
                        print(f"0x{address:04X}: {info['name']} ({info['type']}) - {info['unit']} : Unable to decode value")
                else:
                    print(f"0x{address:04X}: {info['name']} ({info['type']}) - {info['unit']} : Unable to read register")

    client.close()
    logging.info("Finished reading inverter data")

    pivoted_data = pivot_registers(register_data)
    pivoted_data.to_csv('/tmp/pivoted_registers.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
    print("Pivoted register data saved to '/tmp/pivoted_registers.csv'")

if __name__ == "__main__":
    main()
