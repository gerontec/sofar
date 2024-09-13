#!/usr/bin/python3
import sys
import csv
import time
from pymodbus.client.serial import ModbusSerialClient
import logging
from datetime import datetime
import pandas as pd

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
    df = pd.read_csv(csv_file, delimiter=';', header=0, 
                     names=['function', 'address', 'name', 'type', 'accuracy', 'unit'])
    
    def parse_address(x):
        if pd.isna(x) or not isinstance(x, str):
            return None
        if '____' in x:
            return int(x.split('____')[0], 16)
        try:
            return int(x, 16)
        except ValueError:
            return None
    
    df['address'] = df['address'].apply(parse_address)
    df['accuracy'] = df['accuracy'].apply(lambda x: str(x).replace(',', '.') if isinstance(x, str) else x)
    df = df.dropna(subset=['address'])
    return df.set_index('address').to_dict('index')


def read_register_block(client, start_address, count):
    try:
        result = client.read_holding_registers(start_address, count, slave=UNIT_ID)
        if not result.isError():
            return result.registers
        return None
    except:
        return None

def parse_accuracy(accuracy):
    if isinstance(accuracy, (int, float)):
        return accuracy
    if isinstance(accuracy, str):
        # Remove any non-numeric characters except for the decimal point
        numeric_part = ''.join(char for char in accuracy if char.isdigit() or char == '.')
        try:
            return float(numeric_part)
        except ValueError:
            return 1.0
    return 1.0

def decode_value(registers, reg_type, accuracy):
    if not registers:
        return None
    
    accuracy = parse_accuracy(accuracy)
    
    value = registers[0]
    if reg_type == 'U16':
        return value * accuracy
    elif reg_type == 'I16':
        return (value if value < 32768 else value - 65536) * accuracy
    elif reg_type == 'U32':
        return (value << 16 | registers[1]) * accuracy if len(registers) >= 2 else value * accuracy
    elif reg_type == 'I32':
        value = (value << 16 | registers[1]) if len(registers) >= 2 else value
        return (value if value < 2147483648 else value - 4294967296) * accuracy
    elif reg_type == 'U64':
        if len(registers) >= 4:
            return (registers[0] << 48 | registers[1] << 32 | registers[2] << 16 | registers[3]) * accuracy
        else:
            return value * accuracy
    elif reg_type == 'BCD16':
        return int(f"{value:04x}") * accuracy
    elif reg_type == 'ASCII':
        return ''.join(chr(reg >> 8) + chr(reg & 0xFF) for reg in registers).strip('\x00')
    else:
        return None

def main():
    client = ModbusSerialClient(method='rtu', port=SERIAL_PORT, baudrate=BAUD_RATE, 
                                parity='N', stopbits=1, bytesize=8, timeout=1)
    if not client.connect():
        print("Failed to connect to the inverter", file=sys.stderr)
        return

    register_info = read_register_info_from_csv(CSV_FILE)
    
    data = []
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    row = {'Timestamp': timestamp}

    for address in range(0x0040, MAX_REGISTER + 1):
        if address in register_info:
            info = register_info[address]
            registers = read_register_block(client, address, 1)
            if registers:
                value = decode_value(registers, info.get('type', 'U16'), info.get('accuracy', 1))
                row[f"{address:04X}_{info.get('name', f'Register_{address:04X}')}"] = value
            else:
                row[f"{address:04X}_{info.get('name', f'Register_{address:04X}')}"] = None

    data.append(row)
    
    # Create a DataFrame and write to CSV
    df = pd.DataFrame(data)
    df.to_csv('sofar_all_data.csv', index=False)
    
    print(f"Data recorded at {timestamp}")

    client.close()

if __name__ == "__main__":
    main()
