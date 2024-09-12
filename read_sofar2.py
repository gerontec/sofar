#!/usr/bin/env python3
import csv
import time
import pymysql
from pymodbus.client import ModbusSerialClient
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder

# Configuration
SERIAL_PORT = '/dev/ttyUSB0'
BAUD_RATE = 9600
UNIT_ID = 1
CSV_FILE = 'sofarregister.csv'

# MariaDB Configuration
DB_CONFIG = {
    'host': '192.168.178.23',
    'user': 'gh',
    'password': 'a12345',
    'database': 'wagodb',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

def read_register_names_from_csv(csv_file):
    register_names = {}
    with open(csv_file, mode='r') as infile:
        reader = csv.reader(infile, delimiter=';')
        next(reader)  # Skip header
        for row in reader:
            if row[1]:  # Check if the second column (register address) is not empty
                address_range = row[1].replace(' ', '').rstrip('_')
                if '____' in address_range:
                    start, end = address_range.split('____')
                elif '--' in address_range:
                    start, end = address_range.split('--')
                elif '___' in address_range:
                    start, end = address_range.split('___')
                elif '_' in address_range:
                    start, end = address_range.split('_')
                else:
                    start = end = address_range

                try:
                    start_address = int(start, 16)
                    end_address = int(end, 16)
                except ValueError:
                    print(f"Warning: Invalid address format: {row[1]}. Skipping this row.")
                    continue

                base_name = row[2].strip() if len(row) > 2 else ''  # Name is in the third column
                for addr in range(start_address, end_address + 1):
                    register_names[addr] = base_name
    return register_names

def read_registers(client, start_address, count):
    try:
        result = client.read_holding_registers(start_address, count, slave=UNIT_ID)
        if result.isError():
            print(f"Error reading registers: {result}")
            return None
        return result.registers
    except Exception as e:
        print(f"Exception reading registers: {e}")
        return None

def decode_value(registers, reg_type):
    decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=Endian.Big, wordorder=Endian.Big)
    if reg_type == "U16":
        return decoder.decode_16bit_uint()
    elif reg_type == "I16":
        return decoder.decode_16bit_int()
    elif reg_type == "U32":
        return decoder.decode_32bit_uint()
    elif reg_type == "I32":
        return decoder.decode_32bit_int()
    elif reg_type == "U64":
        return decoder.decode_64bit_uint()
    elif reg_type == "I64":
        return decoder.decode_64bit_int()
    else:
        print(f"Unknown register type: {reg_type}")
        return None

#def insert_into_db(cursor, timestamp, name, value):
 #   query = "INSERT INTO sofar_inverter_data (timestamp, name, value) VALUES (%s, %s, %s)"
  #  cursor.execute(query, (timestamp, name, value))
def insert_into_db(cursor, timestamp, register, name, value):
    query = "INSERT INTO sofar_inverter_data (timestamp, register, name, value) VALUES (%s, %s, %s, %s)"
    cursor.execute(query, (timestamp, register, name, value))

def main():
    client = ModbusSerialClient(
        method='rtu',
        port=SERIAL_PORT,
        baudrate=BAUD_RATE,
        stopbits=1,
        bytesize=8,
        parity='N'
    )
    if not client.connect():
        print("Failed to connect to the inverter")
        return

    try:
        db_connection = pymysql.connect(**DB_CONFIG)
        with db_connection.cursor() as cursor:
            register_names = read_register_names_from_csv(CSV_FILE)
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')

            for address, name in register_names.items():
                registers = read_registers(client, address, 1)
                if registers is not None:
                    value = decode_value(registers, "U16")  # Assuming all are U16 for simplicity
                    if value is not None:
                        print(f"Inserting: Register: {address:04X}, Name: {name}, Value: {value}")
                        insert_into_db(cursor, timestamp, f"{address:04X}", name, value)

            db_connection.commit()

    except pymysql.Error as err:
        print(f"Database error: {err}")

    finally:
        if 'db_connection' in locals():
            db_connection.close()
        client.close()


if __name__ == "__main__":
    main()
