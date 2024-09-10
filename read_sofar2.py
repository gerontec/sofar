#!/usr/bin/python3
from pymodbus.client.serial import ModbusSerialClient
import logging
import time
import csv

# Set up logging
logging.basicConfig(filename='mat3.log', level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Add console handler to see logs in real-time
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logging.getLogger('').addHandler(console_handler)

log = logging.getLogger(__name__)
log.info("Script started")

SERIAL = '/dev/ttyUSB2'
BAUD = 9600

client = ModbusSerialClient(method='rtu', port=SERIAL, baudrate=BAUD, parity='N', stopbits=1, bytesize=8, timeout=2)
client.connect()

# Function to read register names from CSV
def read_register_names_from_csv(csv_file):
    register_names = {}
    with open(csv_file, mode='r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile, delimiter=';')
        for row in reader:
            if row["register address"]:
                # Use only the first address, ignoring any ranges
                address = row["register address"].split()[0]  # Split and take first part
                address = address.split('-')[0]  # In case of '1334-133F', take '1334'
                address = int(address, 16)  # Convert to integer
                
                name = row["fields"].strip()
                register_type = row.get("type", "").strip()
                unit = row.get("unit", "").strip()
                try:
                    accuracy = float(row.get("accuracy", "1").replace(',', '.'))
                except ValueError:
                    accuracy = 1
                
                # Determine if it's a U32 type based on the 'units' column
                is_u32 = unit.lower() in ['kwh', 'kvarh', 'mwh', 'gj', 'ah', 'kw', 'kvar', 'mw']
                
                register_names[address] = (name, "U32" if is_u32 else register_type, unit, accuracy)
    return register_names

# Read register names from CSV
csv_file = 'sofarregister.csv'
raw_register_names = read_register_names_from_csv(csv_file)

# Function to read a block of registers
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

# Function to read two consecutive registers and combine into a U32 value
def read_register_u32(high, low):
    return (high << 16) | low

# Print section headlines and register values
sections = [
    ("I General", 0x0040, 0x007F),
    ("II Realtime SysInfo", 0x0400, 0x04BD),
    ("III PV Input", 0x0580, 0x05FF),
    ("VI Realtime ElectricityStatistics and ClassifiedInfo", 0x0680, 0x06ED),
    ("VII Realtime CombinerInfo and ArcInfo", 0x0700, 0x07C7),
    ("IX Remote Config and VRTConfig", 0x0900, 0x09C1),
    ("X Remote control", 0x1100, 0x1106)
]

# Reduced block size to avoid large range read failures
MAX_BLOCK_SIZE = 32

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
                reg_address = block_start + i
                reg_info = raw_register_names.get(reg_address, ("Unknown", "U16", "", 1))
                name, reg_type, unit, accuracy = reg_info

                if reg_type.lower() in ['u32', 'i32'] and i + 1 < len(block_registers):
                    value = read_register_u32(block_registers[i], block_registers[i + 1])
                    i += 2
                else:
                    value = block_registers[i]
                    i += 1

                adjusted_value = value / accuracy
                if adjusted_value != 0:
                    print(f"Register 0x{reg_address:04X} ({name}): {adjusted_value:.2f} {unit} ({reg_type})")

        else:
            print(f"Unable to read registers from 0x{block_start:04X} to 0x{block_end:04X}")
        time.sleep(0.01)  # Small delay between batches

client.close()
