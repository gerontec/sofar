#!/usr/bin/python3
import csv
import time
from pymodbus.client.serial import ModbusSerialClient
import logging

# Set up logging
logging.basicConfig(filename='sofar_inverter.log', level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
SERIAL_PORT = '/dev/ttyUSB0'
BAUD_RATE = 9600
UNIT_ID = 3
CSV_FILE = 'sofarregister.csv'
MAX_REGISTER = 0x1324
BLOCK_SIZE = 32  # Read 100 registers at a time

def read_register_info_from_csv(csv_file):
    register_info = {}
    current_section = ""
    with open(csv_file, mode='r', encoding='utf-8') as infile:
        reader = csv.reader(infile, delimiter=';')
        next(reader)  # Skip header
        for row in reader:
            if row and not row[1] and row[0]:  # New section
                current_section = row[0].strip()
            elif len(row) >= 6 and row[1]:  # Register info
                address_range = row[1].replace(" ", "")
                if '____' in address_range:
                    start = address_range.split('____')[0]
                elif '-' in address_range:
                    start = address_range.split('-')[0]
                else:
                    start = address_range

                try:
                    address = int(start, 16)
                except ValueError:
                    continue

                if address <= MAX_REGISTER:
                    # Extract numeric part from accuracy
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
    return register_info

def read_register_block(client, start_address, count):
    try:
        result = client.read_holding_registers(start_address, count, slave=UNIT_ID)
        if not result.isError():
            return result.registers
        return None
    except:
        return None

def decode_value(registers, reg_type, accuracy):
    if not registers:
        return None
    
    if reg_type == 'U16':
        return registers[0] * accuracy
    elif reg_type == 'I16':
        value = registers[0]
        return (value if value < 32768 else value - 65536) * accuracy
    elif reg_type == 'U32':
        if len(registers) >= 2:
            return (registers[0] << 16 | registers[1]) * accuracy
        else:
            return registers[0] * accuracy
    elif reg_type == 'I32':
        if len(registers) >= 2:
            value = (registers[0] << 16 | registers[1])
            return (value if value < 2147483648 else value - 4294967296) * accuracy
        else:
            value = registers[0]
            return (value if value < 32768 else value - 65536) * accuracy
    elif reg_type == 'U64':
        if len(registers) >= 4:
            return (registers[0] << 48 | registers[1] << 32 | registers[2] << 16 | registers[3]) * accuracy
        elif len(registers) >= 2:
            return (registers[0] << 16 | registers[1]) * accuracy
        else:
            return registers[0] * accuracy
    elif reg_type == 'BCD16':
        return int(f"{registers[0]:04x}") * accuracy
    elif reg_type == 'ASCII':
        return ''.join(chr(reg >> 8) + chr(reg & 0xFF) for reg in registers).strip('\x00')
    else:
        return None

def main():
    client = ModbusSerialClient(method='rtu', port=SERIAL_PORT, baudrate=BAUD_RATE, 
                                parity='N', stopbits=1, bytesize=8, timeout=1)
    if not client.connect():
        print("Failed to connect to the inverter")
        return

    register_info = read_register_info_from_csv(CSV_FILE)
    
    current_section = ""
    for start_address in range(0, MAX_REGISTER + 1, BLOCK_SIZE):
        end_address = min(start_address + BLOCK_SIZE - 1, MAX_REGISTER)
        registers = read_register_block(client, start_address, end_address - start_address + 1)
        
        if registers:
            for offset, value in enumerate(registers):
                address = start_address + offset
                if address in register_info:
                    info = register_info[address]
                    
                    if info['section'] != current_section:
                        current_section = info['section']
                        print(f"\n--- {current_section} ---")
                    
                    decoded_value = decode_value([value], info['type'], info['accuracy'])
                    if decoded_value is not None:
                        print(f"Register 0x{address:04X} ({info['name']}): {decoded_value} {info['unit']}")

    client.close()

if __name__ == "__main__":
    main()
