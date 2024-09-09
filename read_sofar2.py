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

# Define registers with units and accuracy
registers_with_units = {
    0x0043: ("Version number of this agreement", "", 1),
    0x0044: ("Modbus_Protocol_Version", "", 1),
    0x0404: ("SysState", "", 1),
    0x0417: ("Countdown", "s", 1),
    0x0418: ("Temperature_Env1", "°C", 1),
    0x041A: ("Temperature_HeatSink1", "°C", 1),
    0x0420: ("Temperature_Inv1", "°C", 1),
    0x0426: ("GenerationTime_Today", "min", 1),
    0x0427: ("GenerationTime_Total", "h", 100),  # U32
    0x0429: ("ServiceTime_Total", "h", 100),     # U32
    0x042B: ("InsulationResistance", "kΩ", 1),
    # PV Input registers
    0x0580: ("PV1_Voltage", "V", 10),
    0x0581: ("PV1_Current", "A", 100),
    0x0582: ("PV1_Power", "W", 10),  # U32
    0x0584: ("PV2_Voltage", "V", 10),
    0x0585: ("PV2_Current", "A", 100),
    0x0586: ("PV2_Power", "W", 10),  # U32
    0x0588: ("PV3_Voltage", "V", 10),
    0x0589: ("PV3_Current", "A", 100),
    0x058A: ("PV3_Power", "W", 10),  # U32
    0x058C: ("PV4_Voltage", "V", 10),
    0x058D: ("PV4_Current", "A", 100),
    0x058E: ("PV4_Power", "W", 10),  # U32
    0x0590: ("PV5_Voltage", "V", 10),
    0x0591: ("PV5_Current", "A", 100),
    0x0592: ("PV5_Power", "W", 10),  # U32
    0x0594: ("PV6_Voltage", "V", 10),
    0x0595: ("PV6_Current", "A", 100),
    0x0596: ("PV6_Power", "W", 10),  # U32
    0x0598: ("PV7_Voltage", "V", 10),
    0x0599: ("PV7_Current", "A", 100),
    0x059A: ("PV7_Power", "W", 10),  # U32
    0x059C: ("PV8_Voltage", "V", 10),
    0x059D: ("PV8_Current", "A", 100),
    0x059E: ("PV8_Power", "W", 10),  # U32
    0x05A0: ("PV_Total_Power", "W", 10),  # U32
    0x0498: ("Voltage_Phase_S", "V", 10),
    0x0499: ("Current_Output_S", "A", 100),
    0x049A: ("ActivePower_Output_S", "W", 1),
    0x0686: ("PV_Generation_Total", "kWh", 10),   # U32
    0x068A: ("Load_Consumption_Total", "kWh", 10),  # U32
    0x068E: ("Energy_Purchase_Total", "kWh", 10),  # U32
    0x0692: ("Energy_Selling_Total", "kWh", 10),   # U32
    0x0696: ("Bat_Charge_Total", "kWh", 10),       # U32
    0x069A: ("Bat_Discharge_Total", "kWh", 10),    # U32
    0x0901: ("ActiveOutputLimit", "%", 10),
    0x0909: ("ChgDerateMinPower", "%", 100),
    0x1102: ("ActivePowerControlValue", "%", 100),
    0x1104: ("ReactivePowerControlValue", "%", 10),
    0x1106: ("ActivePowerLimit", "%", 10),
}

# Function to read register names from CSV
def read_register_names_from_csv(csv_file):
    register_names = {}
    with open(csv_file, mode='r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile, delimiter=';')
        for row in reader:
            if row["register address"]:
                addresses = row["register address"].split(" -- ")
                names = row["fields"].split(" -- ")
                for address, name in zip(addresses, names):
                    register_names[int(address, 16)] = name
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

# List of U32 registers
u32_registers = [0x0427, 0x0429, 0x0582, 0x0586, 0x058A, 0x058E, 0x0592, 0x0596, 0x059A, 0x059E, 0x05A0,
                 0x0686, 0x068A, 0x068E, 0x0692, 0x0696, 0x069A]

# Main loop to read and display registers
for section_name, start, end in sections:
    print(f"\n## {section_name} (0x{start:04X}-0x{end:04X})")
    for block_start in range(start, end + 1, MAX_BLOCK_SIZE):
        block_end = min(block_start + MAX_BLOCK_SIZE - 1, end)
        block_size = block_end - block_start + 1

        # Read block of registers
        block_registers = read_register_block(block_start, block_size)
        if block_registers:
            i = 0
            while i < len(block_registers):
                reg_address = block_start + i

                if reg_address in registers_with_units:
                    name, unit, accuracy = registers_with_units[reg_address]

                    # Check if the register is U32 and handle it accordingly
                    if reg_address in u32_registers:  # U32 registers
                        if i + 1 < len(block_registers):  # Ensure we have both registers for U32
                            u32_value = read_register_u32(block_registers[i], block_registers[i + 1])
                            adjusted_value = u32_value / accuracy
                            if adjusted_value != 0:
                                print(f"Register 0x{reg_address:04X} ({name}): {adjusted_value:.1f} {unit}")
                            i += 2  # Skip the next register since it's part of the U32
                        else:
                            print(f"Unable to read U32 register at 0x{reg_address:04X}")
                    else:
                        adjusted_value = block_registers[i] / accuracy
                        if adjusted_value != 0:
                            print(f"Register 0x{reg_address:04X} ({name}): {adjusted_value:.1f} {unit}")
                        i += 1
                else:
                    # For registers not explicitly listed in `registers_with_units`
                    if block_registers[i] != 0:
                        reg_name = raw_register_names.get(reg_address, "Unknown")
                        print(f"Register 0x{reg_address:04X} ({reg_name}): {block_registers[i]} (raw value)")
                    i += 1
        else:
            print(f"Unable to read registers from 0x{block_start:04X} to 0x{block_end:04X}")
        time.sleep(0.01)  # Small delay between batches

client.close()
