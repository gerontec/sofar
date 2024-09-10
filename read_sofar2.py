#!/usr/bin/python3
from pymodbus.client.serial import ModbusSerialClient
import logging
import time
import csv

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

SERIAL = '/dev/ttyUSB2'
BAUD = 9600

client = ModbusSerialClient(method='rtu', port=SERIAL, baudrate=BAUD, parity='N', stopbits=1, bytesize=8, timeout=2)
client.connect()

def read_register_names_from_csv(csv_file):
    register_names = {}
    with open(csv_file, mode='r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile, delimiter=';')
        for row in reader:
            if row["register address"]:
                address_range = row["register address"].replace(" ", "").split("--")
                if len(address_range) == 1:
                    start, end = address_range[0].split('-') if '-' in address_range[0] else (address_range[0], address_range[0])
                else:
                    start, end = address_range

                start_address = int(start, 16)
                end_address = int(end, 16)
                
                base_name = row["fields"].strip()
                unit = row.get("unit", "").strip()
                reg_type = row.get("type", "U16").strip().upper()
                try:
                    accuracy = float(row.get("accuracy", "1").replace(',', '.'))
                except ValueError:
                    accuracy = 1
                
                for addr in range(start_address, end_address + 1):
                    if "PV" in base_name and "-- 16" in base_name:
                        pv_number = ((addr - start_address) // 4) + 1
                        if "Voltage" in base_name:
                            name = f"Voltage_PV{pv_number}"
                            unit = "V"
                        elif "Current" in base_name:
                            name = f"Current_PV{pv_number}"
                            unit = "A"
                        elif "Power" in base_name:
                            name = f"Power_PV{pv_number}"
                            unit = "W"
                        else:
                            name = f"{base_name} (PV{pv_number})"
                    else:
                        name = base_name

                    # Assign specific units for certain registers
                    if addr == 0x05C4:
                        unit = "W"
                    elif addr in [0x0684, 0x0686, 0x069C, 0x069E]:
                        unit = "kWh"
                    
                    register_names[addr] = (name, reg_type, unit, accuracy)
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
    unit_str = f" {unit}" if unit else ""
    return f"Register 0x{register_address:04X} ({name}): {value:.2f}{unit_str} ({reg_type})"

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

                if reg_type in ["U32", "I32"] and i + 1 < len(block_registers):
                    value = read_register_u32(block_registers[i], block_registers[i + 1])
                    i += 2
                else:
                    value = block_registers[i]
                    i += 1

                adjusted_value = value / accuracy
                if adjusted_value != 0 or "PV" in name:
                    print(format_register_info(register_address, name, adjusted_value, unit, reg_type))

        else:
            print(f"Unable to read registers from 0x{block_start:04X} to 0x{block_end:04X}")
        time.sleep(0.01)  # Small delay between batches

client.close()
