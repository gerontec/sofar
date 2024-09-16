#!/usr/bin/python3
import sys
import csv
import time
from pymodbus.client.serial import ModbusSerialClient
import logging

# Set up logging
logging.basicConfig(filename='sofar_inverter.log', level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
SERIAL_PORT = '/dev/ttyUSB32'
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
    
    value = None
    
    if reg_type == 'U16':
        value = registers[0]
    elif reg_type == 'I16':
        value = registers[0]
        if value >= 32768:
            value -= 65536
    elif reg_type == 'U32':
        if len(registers) >= 2:
            value = (registers[0] << 16) | registers[1]
        else:
            value = registers[0]
    elif reg_type == 'I32':
        if len(registers) >= 2:
            value = (registers[0] << 16) | registers[1]
            if value >= 2147483648:
                value -= 4294967296
        else:
            value = registers[0]
            if value >= 32768:
                value -= 65536
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
        return ''.join(chr(reg >> 8) + chr(reg & 0xFF) for reg in registers).strip('\x00')
    
    if value is not None:
        value *= accuracy
    
    return value

def main():
    client = ModbusSerialClient(method='rtu', port=SERIAL_PORT, baudrate=BAUD_RATE, 
                                parity='N', stopbits=1, bytesize=8, timeout=1)
    if not client.connect():
        print("Failed to connect to the inverter", file=sys.stderr)
        return

    register_info = read_register_info_from_csv(CSV_FILE)
    
    # Set up CSV writer
    csv_writer = csv.writer(sys.stdout)
    csv_writer.writerow(['Address', 'Name', 'Value', 'Unit', 'Type', 'Accuracy'])

    current_section = ""
    for start_address in range(0, MAX_REGISTER + 1, BLOCK_SIZE):
        end_address = min(start_address + BLOCK_SIZE - 1, MAX_REGISTER)
        registers = read_register_block(client, start_address, end_address - start_address + 1)
        
        if registers:
            for offset in range(0, len(registers), 2):  # Process two registers at a time
                address = start_address + offset
                if address in register_info:
                    info = register_info[address]
                    
                    if info['section'] != current_section:
                        current_section = info['section']
                        csv_writer.writerow([])
                        csv_writer.writerow([f"--- {current_section} ---"])
                    
                    if info['type'] == 'U32':
                        # For U32, read two consecutive registers
                        value = decode_value(registers[offset:offset+2], info['type'], info['accuracy'])
                    else:
                        value = decode_value([registers[offset]], info['type'], info['accuracy'])

                    if value is not None:
                        # Format the value based on its type
                        if isinstance(value, (int, float)):
                            formatted_value = f"{value:.4f}"
                        else:
                            formatted_value = str(value)
                        
                        csv_writer.writerow([
                            f"0x{address:04X}",
                            info['name'],
                            formatted_value,
                            info['unit'],
                            info['type'],
                            info['accuracy']
                        ])

                    # If we processed a U32 value, skip the next register
                    if info['type'] == 'U32':
                        offset += 1

    client.close()

if __name__ == "__main__":
    main()
