#!/usr/bin/python3
from pymodbus.client.serial import ModbusSerialClient
import logging
import time
import pymysql
import csv 
from datetime import datetime

# Set up logging
logging.basicConfig(filename='mat3.log', level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logging.getLogger('').addHandler(console_handler)

log = logging.getLogger(__name__)
log.info("Script started")

SERIAL = '/dev/ttyUSB1'
BAUD = 9600

# MariaDB connection configuration
DB_CONFIG = {
    'host': '192.168.178.23',
    'user': 'gh',
    'password': 'a12345',
    'database': 'wagodb'
}

client = ModbusSerialClient(method='rtu', port=SERIAL, baudrate=BAUD, parity='N', stopbits=1, bytesize=8, timeout=2)
client.connect()

def read_register_names_from_csv(csv_file):
    register_names = {}
    with open(csv_file, mode='r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile, delimiter=';')
        for row in reader:
            if row.get("register address"):
                address_range = row["register address"].replace(" ", "").split("--")
                if len(address_range) == 1:
                    start, end = address_range[0].split('-') if '-' in address_range[0] else (address_range[0], address_range[0])
                else:
                    start, end = address_range

                start_address = int(start, 16)
                end_address = int(end, 16)
                
                base_name = row.get('fields', '').strip()
                unit = row.get("unit", "").strip()
                reg_type = row.get("type", "U16").strip().upper()
                try:
                    accuracy = float(row.get("accuracy", "1").replace(',', '.'))
                except ValueError:
                    accuracy = 1
                
                # Specific handling for certain registers
                if start_address in [0x0684, 0x0686, 0x069C, 0x069E]:
                    unit = "kWh"
                elif start_address in range(0x06C9, 0x06CF):
                    if "DCV" in base_name or "Voltage_Bus" in base_name:
                        unit = "V"
                elif start_address in range(0x0584, 0x058A) or start_address in range(0x05C4, 0x05CA):
                    pv_number = 1 if start_address <= 0x0586 else 2
                    if "Voltage" in base_name:
                        unit = "V"
                    elif "Current" in base_name:
                        unit = "A"
                    elif "Power" in base_name:
                        unit = "W"
                elif start_address == 0x05C4:
                    base_name = "Power_PV_Total"
                    unit = "W"
                
                for addr in range(start_address, end_address + 1):
                    register_names[addr] = (base_name, reg_type, unit, accuracy)
    return register_names

def read_register_block(start_address, count):
    try:
        result = client.read_holding_registers(start_address, count, slave=3)
        if not result.isError():
            return result.registers
        else:
            return None
    except Exception as e:
        log.error(f"Error reading block at 0x{start_address:04X}: {e}")
        return None

def read_register_u32(high, low):
    return (high << 16) | low

def format_register_info(register_address, name, value, unit, reg_type):
    if unit:
        unit_str = f" {unit}"
    else:
        unit_str = ""
    
    if "Temperature" in name or unit == "°C":
        return f"Register 0x{register_address:04X} ({name}): {value:.1f}{unit_str} ({reg_type})"
    elif unit in ["V", "A", "W", "kW"]:
        return f"Register 0x{register_address:04X} ({name}): {value:.2f}{unit_str} ({reg_type})"
    elif unit == "kWh":
        return f"Register 0x{register_address:04X} ({name}): {value:.3f}{unit_str} ({reg_type})"
    else:
        return f"Register 0x{register_address:04X} ({name}): {value:.3f}{unit_str} ({reg_type})"

# Read register names from CSV
csv_file = 'sofarregister.csv'
raw_register_names = read_register_names_from_csv(csv_file)

sections = [
    ("I General", 0x0040, 0x007F),
    ("II Realtime SysInfo", 0x0400, 0x04BD),
    ("III PV Input", 0x0580, 0x05FF),
    ("VI Realtime ElectricityStatistics and ClassifiedInfo", 0x0680, 0x06ED),
    ("VII Realtime CombinerInfo and ArcInfo", 0x0700, 0x07C7),
    ("IX Remote Config and VRTConfig", 0x0900, 0x09C1),
    ("X Remote control", 0x1100, 0x1106)
]

MAX_BLOCK_SIZE = 32

# Define a set of register addresses that should always be reported, even if zero
always_report = {0x05C4, 0x0684, 0x0686, 0x069C, 0x069E, 0x1106}

# Connect to MariaDB
db_connection = pymysql.connect(**DB_CONFIG)
cursor = db_connection.cursor()

# Main loop to read and display registers
for section_name, start, end in sections:
    print(f"\n## {section_name} (0x{start:04X}-0x{end:04X})")
    for block_start in range(start, end + 1, MAX_BLOCK_SIZE):
        block_end = min(block_start + MAX_BLOCK_SIZE - 1, end)
        block_size = block_end - block_start + 1

        block_registers = read_register_block(block_start, block_size)
        if block_registers:
            i = 0
            while i < len(block_registers):
                register_address = block_start + i
                reg_info = raw_register_names.get(register_address, (f"Register_0x{register_address:04X}", "U16", "", 1))
                name, reg_type, unit, accuracy = reg_info

                if reg_type in ["U32", "I32", "U64"] and i + 1 < len(block_registers):
                    value = read_register_u32(block_registers[i], block_registers[i + 1])
                    i += 2
                else:
                    value = block_registers[i]
                    i += 1

                adjusted_value = value / accuracy
                
                if adjusted_value != 0 or register_address in always_report:
                    print(format_register_info(register_address, name, adjusted_value, unit, reg_type))
                    print("-" * 40)  # Add a separator for better readability
                    
                    # Insert data into MariaDB
                    insert_query = """
                    INSERT INTO sofar_inverter_data (register, name, value, unit, type)
                    VALUES (%s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (f"0x{register_address:04X}", name[:255], adjusted_value, unit, reg_type))

        else:
            print(f"Unable to read registers from 0x{block_start:04X} to 0x{block_end:04X}")
        time.sleep(0.01)  # Small delay between batches

# Commit the changes and close the database connection
db_connection.commit()
cursor.close()
db_connection.close()

client.close()

print("Data has been saved to the MariaDB database.")
