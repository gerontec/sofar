#!/usr/bin/python3

# 2026-11-02: switched from RS485 (/dev/ttyUSB32) to Modbus TCP via the LSE-3 LAN logger
#   192.168.178.108:8899 (TRANSPORT below). Pre-switch backup: read.py.bak-20261102-rs485-vor-tcp-umstellung
#   sofar_fast.service still reads via RS485 (mbread) on /dev/ttyUSB32.
#
# Kette am Wechselrichter-Bus /dev/ttyUSB32 (Sofar, Slave 1, 9600 8N1):
#
#   Cron jede Minute: sofar.sh  (haelt /tmp/sofar.lock fuer den ganzen Lauf)
#     1. read.py      -> /tmp/raw.csv, /tmp/pivoted_registers.csv
#     2. pivot2db.py  -> wagodb.inverter_data (1 Zeile/min) + MQTT inverter/power_grid_exchange/json
#     3. PV-Ueberschuss -> /tmp/r.txt
#
#   Parallel als Dienst: sofar_fast.service (/home/pi/python/sofar_fast.py)
#     liest alle 4 s per mbread nur PCC (0x0488), Load (0x04AF), PV1/PV2 (0x0586:4),
#     Bat1/SOC (0x0606:3) und publiziert auf dasselbe MQTT-Topic wie pivot2db.py.
#     Jeder Lesevorgang nimmt /tmp/sofar.lock fuer ~150 ms (LOCK_NB); laeuft gerade
#     sofar.sh, faellt der 4-s-Zyklus aus. Umgekehrt: sofar.sh nutzt flock -n und
#     ueberspringt seinen Minutenlauf, wenn es genau in so ein 150-ms-Fenster faellt.
#
#   Wer hier direkt (ausserhalb sofar.sh) liest, muss ebenfalls /tmp/sofar.lock halten,
#   sonst kollidiert er mit sofar_fast.service auf dem Bus.

import sys
import csv
import time
from pymodbus.client.serial import ModbusSerialClient
from pymodbus.client import ModbusTcpClient
from pymodbus import FramerType
import logging
from typing import Dict, List, Any, Optional
import struct
import re
import pandas as pd

TIMEOUT = 1

# Configuration switch: Set to False to read only kW, kWh, and % (SOC) registers
ALLREG = False

# Set up logging
logging.basicConfig(filename='sofar_inverter.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
TRANSPORT = 'tcp'  # 'tcp' = LSE-3 logger (Modbus TCP gateway), 'rtu' = RS485 /dev/ttyUSB32
TCP_HOST = '192.168.178.108'
TCP_PORT = 8899
# TCP_FRAMER: 'rtu' = logger in Transparency mode (raw RTU over TCP, set 2026-11-02),
#             'socket' = logger in Data collection mode (Modbus TCP gateway)
TCP_FRAMER = 'socket'  # since 2026-11-02 17:22: logger in Data collection mode, cloud via v5_relay.py


def parse_transport(argv):
    """--transport tcp|rtu (also --transport=...); default is the TRANSPORT constant"""
    for i, a in enumerate(argv):
        if a == '--transport' and i + 1 < len(argv):
            v = argv[i + 1]
        elif a.startswith('--transport='):
            v = a.split('=', 1)[1]
        else:
            continue
        if v not in ('tcp', 'rtu'):
            raise SystemExit(f"--transport must be tcp or rtu, not {v!r}")
        return v
    return TRANSPORT
SERIAL_PORT = '/dev/ttyUSB32'
BAUD_RATE = 9600
UNIT_ID = 1
CSV_FILE = '/home/pi/python/sofarregister.csv'
MAX_REGISTER = 0x203F
BLOCK_SIZE = 32  # Read 32 registers at a time
FORCED_REGISTERS = {0x0586, 0x0589}  # Power_PV1, Power_PV2 — bypass address mask
# Grid voltages + frequency for ortsnetz-auslastung.de (read despite unit V/Hz)
GRID_REGISTERS = {'Voltage_Phase_R', 'Voltage_Phase_S', 'Voltage_Phase_T', 'Frequency_Grid'}
# Leakage current (FI, 0x06C4 mA) + insulation resistance (0x042B kOhm) for inverter_data
SAFETY_REGISTERS = {'GFCI', 'InsulationResistance', 'DCI_R', 'DCI_S', 'DCI_T'}  # DCI = DC component of the output current (mA)
# PCC current per phase (0.01 A) for the reactive power at Z1 on venus.heissa.de: the Sofar masks out
# ReactivePower_PCC_* and PowerFactor_PCC_* (always 0), so |Q| = sqrt((U*I)^2 - P^2) is derived in victron_api.php
PCC_CURRENT_REGISTERS = {'Current_PCC_R', 'Current_PCC_S', 'Current_PCC_T'}
# EPS (off-grid output) voltages/currents/frequency, run state, fault words and EPS switch for inverter_data
# (2026-11-02). Fault1-30 = fault tables 0x0405-0x0416 + 0x0432-0x043D, bit n of FaultK = Sofar ID (K-1)*16+n+1.
FAULT_REGISTERS = {f'Fault{i}' for i in range(1, 31)}
EPS_REGISTERS = FAULT_REGISTERS | {'SysState', 'EPS_Control', 'Frequency_Output',
                 'Voltage_Output_R', 'Voltage_Output_S', 'Voltage_Output_T',
                 'Voltage_Load_R', 'Voltage_Load_S', 'Voltage_Load_T',
                 'Current_Load_R', 'Current_Load_S', 'Current_Load_T'}

INDEXED_NAME = re.compile(r'(.*?\D)(\d+)\s*--\s*(\d+)')
REG_SIZE = {'U64': 4, 'U32': 2, 'I32': 2}

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

                        # Indexed ranges such as "0405 -- 0416" / "Fault1 -- 18" (2026-11-02): one register per index,
                        # named Fault1..Fault18. Only expanded if the address span matches index count x register size,
                        # otherwise (U32/U64 spans, strided blocks like "0604 -- 0635" / "Voltage_Bat1 -- 8")
                        # keep the old behaviour: only the start address carries the name.
                        reg_name = row[2].strip()
                        reg_type = row[3].strip().upper() if row[3].strip() else 'U16'
                        targets = [(a, reg_name) for a in range(start, end + 1)]
                        m = INDEXED_NAME.fullmatch(reg_name)
                        if m and '-' in address_range and end == start:
                            span_end = int(address_range.split('-')[-1], 16)
                            first, last = int(m.group(2)), int(m.group(3))
                            size = REG_SIZE.get(reg_type, 1)
                            if last >= first and span_end - start + 1 == (last - first + 1) * size:
                                targets = [(start + i * size, f"{m.group(1)}{first + i}")
                                           for i in range(last - first + 1)]

                        for address, name in targets:
                            if address <= MAX_REGISTER:
                                # Mehrzeilige Accuracy-Felder (z.B. "1\n1" fuer Stunde/Minute) nur in der ersten Zeile
                                # auswerten - sonst wird aus "1\n1" die Zahl 11 (Zeit-/Datumsregister x11 verfaelscht).
                                accuracy_str = row[4].replace(',', '.').splitlines()[0] if row[4].strip() else ''
                                try:
                                    accuracy = float(''.join(filter(lambda x: x.isdigit() or x == '.', accuracy_str)))
                                except ValueError:
                                    accuracy = 1  # Default to 1 if conversion fails

                                register_info[address] = {
                                    'section': current_section,
                                    'name': name,
                                    'type': reg_type,
                                    'accuracy': accuracy,
                                    'unit': row[5].strip() if len(row) > 5 else ''
                                }
                    except ValueError as e:
                        logging.warning(f"Invalid address range '{address_range}' for '{row[2]}': {e}")

    return register_info

def should_read_register(unit: str, name: str = '') -> bool:
    """
    Determines if a register should be read based on the ALLREG flag and unit.
    Returns True if ALLREG is True, if the unit is kW, kWh, or %, or if the
    register name starts with 'TOU' (Time of Use registers).
    """
    if ALLREG:
        return True

    if (name in GRID_REGISTERS or name in SAFETY_REGISTERS or name in PCC_CURRENT_REGISTERS
            or name in EPS_REGISTERS):
        return True

    # Always include all TOU registers
    if name.startswith('TOU'):
        return True

    # Normalize unit for comparison
    unit_lower = unit.lower().strip()

    # Check for kW, kWh, or % (for SOC)
    return unit_lower in ['kw', 'kwh', '%']

# 2026-11-02 prefetch: one block read per section instead of one request per register
# (~120 requests -> ~20; matters on TCP via the logger at ~250 ms per request)
PREFETCH_MAX = 64          # registers per block read (a section is 0x40 long)
REG_CACHE: Dict[int, int] = {}
PREFETCH_STATS = {'blocks': 0, 'failed': 0}


def prefetch_block(client, start_address: int, end_address: int) -> None:
    """read start..end in chunks of <= PREFETCH_MAX into REG_CACHE; a failed chunk is just not cached"""
    address = start_address
    while address <= end_address:
        count = min(PREFETCH_MAX, end_address - address + 1)
        PREFETCH_STATS['blocks'] += 1
        try:
            result = client.read_holding_registers(address, count=count, slave=UNIT_ID)
            if not result.isError() and len(result.registers) == count:
                for i, v in enumerate(result.registers):
                    REG_CACHE[address + i] = v
            else:
                PREFETCH_STATS['failed'] += 1
                logging.info(f"Prefetch 0x{address:04X}+{count} failed: {result}")
        except Exception as e:
            PREFETCH_STATS['failed'] += 1
            logging.info(f"Prefetch 0x{address:04X}+{count} exception: {e}")
        address += count


def read_register_block(client: ModbusSerialClient, start_address: int, count: int) -> Optional[List[int]]:
    cached = [REG_CACHE.get(a) for a in range(start_address, start_address + count)]
    if None not in cached:
        return cached
    try:
        result = client.read_holding_registers(start_address, count=count, slave=UNIT_ID)
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
    global TIMEOUT, TRANSPORT
    TRANSPORT = parse_transport(sys.argv[1:])
    print(f"Transport: {TRANSPORT}")
    if TRANSPORT == 'tcp':
        client = ModbusTcpClient(TCP_HOST, port=TCP_PORT, timeout=TIMEOUT, framer=FramerType(TCP_FRAMER))
    else:
        client = ModbusSerialClient(port=SERIAL_PORT, baudrate=BAUD_RATE,
                                    parity='N', stopbits=1, bytesize=8, timeout=TIMEOUT)

    if not client.connect():
        logging.error("Failed to connect to the inverter")
        print("Failed to connect to the inverter", file=sys.stderr)
        return

    logging.info("Connected to the inverter")
    register_info = read_register_info_from_csv(CSV_FILE)
    logging.info(f"Read {len(register_info)} register definitions from CSV")

    filter_mode = "all registers" if ALLREG else "filtered (kW, kWh, % only)"
    print(f"Reading mode: {filter_mode}")
    print("\nRegisters with specified names:")
    current_section = ""
    register_data = []

    # Define sections to read, including all relevant address ranges
    sections = [
        (0x0040, 0x007F, 0x0040),  # I General（0x0040-0x007F）
        (0x0400, 0x043F, 0x0400),  # Realtime SysInfo1
        (0x0480, 0x04BF, 0x0480),  # Realtime GridOutput1
        (0x0500, 0x053F, 0x0500),  # Realtime EmergencyOutput1
        (0x0580, 0x05BF, 0x0580),  # Realtime Input PV1
        (0x0600, 0x063F, 0x0600),  # Realtime Input Bat1
        (0x0680, 0x06BF, 0x0680),  # Realtime ElectricityStatistics1
        (0x06C0, 0x06FF, 0x06C0),  # Realtime ClassifiedInfo1 (GFCI 0x06C4)
        (0x0700, 0x073F, 0x0700),  # Realtime CombinerInfo1
        (0x0800, 0x083F, 0x0800),  # Voltage Config
        (0x0900, 0x093F, 0x0900),  # Remote Config
        (0x1000, 0x105A, 0x1000),  # Config Basic1
        (0x1100, 0x113F, 0x1100),  # Remote Config (includes Power Export Limit)
        (0x2000, 0x203F, 0x2000),  # Config Core1
        # (0x5000, 0x503F, 0x5000),  # DCDC SysInfo -- deaktiviert 2026-09-10:
        # dieses Modell kennt den Block nicht und antwortet auf jede Abfrage mit
        # Modbus-Exception 0x83/Status 1 (Illegal Data Address) -> 120 Logzeilen/Stunde,
        # keine Daten. Wieder aktivieren, falls ein Geraet mit DCDC-Teil angeschlossen wird.
        (0x6000, 0x603F, 0x6000),  # PCU1
        (0x9000, 0x903F, 0x9000),  # BMS1 System
    ]

    for start, end, mask_address in sections:
        # prefetch: the mask (4 registers at mask_address) plus every register this run would read in the section
        span_end = mask_address + 3
        for a, inf in register_info.items():
            if start <= a <= end and inf['name'] and should_read_register(inf['unit'], inf['name']):
                n = 4 if inf['type'] == 'U64' else (2 if inf['type'] in ['U32', 'I32'] else 1)
                span_end = max(span_end, a + n - 1)
        for a in FORCED_REGISTERS:
            if start <= a <= end:
                span_end = max(span_end, a)
        prefetch_block(client, min(start, mask_address), span_end)
        mask = read_address_mask(client, mask_address)
        valid_registers = get_valid_registers(mask, start, end)
        forced = sorted(a for a in FORCED_REGISTERS if start <= a <= end and a not in valid_registers)
        if forced:
            valid_registers = sorted(set(valid_registers) | set(forced))

        for address in valid_registers:
            if address in register_info and register_info[address]['name']:
                info = register_info[address]
                
                # Skip register if filtering is enabled and unit doesn't match
                if not should_read_register(info['unit'], info['name']):
                    continue
                
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
    print(f"Prefetch: {PREFETCH_STATS['blocks']} block reads, {PREFETCH_STATS['failed']} failed (fallback per register)")
    logging.info("Finished reading inverter data")
    
    # Save register_data to raw.csv before pivoting
    raw_df = pd.DataFrame(register_data)
    raw_df.to_csv('/tmp/raw.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
    print(f"\nRaw register data saved to '/tmp/raw.csv' ({len(register_data)} registers)")

    pivoted_data = pivot_registers(register_data)
    pivoted_data.to_csv('/tmp/pivoted_registers.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
    print("Pivoted register data saved to '/tmp/pivoted_registers.csv'")

if __name__ == "__main__":
    main()
