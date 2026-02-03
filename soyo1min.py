#!/usr/bin/python3
import time
import serial
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
from pathlib import Path
from collections import deque
import os
import sys
import psutil
import paho.mqtt.client as mqtt
import subprocess

VERSION = "v1.9_SIMPLIFIED"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = TimedRotatingFileHandler('/run/user/1000/soyo1min.log', when='H', interval=1, backupCount=2)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

LOCK_FILE = Path('/run/user/1000/soyo1min.lock')

CONFIG = {
    'serial_port': '/dev/ttyUSB32',
    'baudrate': 4800,
    'serial_timeout': 1,
    'battery_capacity_kwh': 30,
    'inverter_file': Path('/run/user/1000/inverter.csv'),
    'soyopower_file': Path('/home/pi/soyopower.txt'),
    'max_power': 999,
    'max_bat2_current': 200,
    'night_standard_power': 390,
    'bat2_soc_min': 9,  # Entladeschutz nur für Bat2
    'priorities': [
        'entladeschutz',
        'bat2_charging',
        'soyopower',
        'buying_from_grid',
        'feeding_to_grid',
        'default'
    ],
    'mqtt_broker': '192.168.178.218',
    'mqtt_power': 'em0/54',
    'wp_power_default': 17
}

VALID_STATUS_BYTES = {1, 2, 4, 8, 16, 32}

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_power_value = CONFIG['wp_power_default']

def on_mqtt_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        logger.info("Connected to MQTT broker")
        client.subscribe(CONFIG['mqtt_power'])
    else:
        logger.error(f"Failed to connect to MQTT broker, return code: {reason_code}")

def on_mqtt_message(client, userdata, msg):
    global mqtt_power_value
    try:
        mqtt_power_value = float(msg.payload.decode())
        logger.debug(f"Received MQTT power value: {mqtt_power_value} VA from {msg.topic}")
    except ValueError as e:
        logger.error(f"Invalid MQTT power value: {msg.payload}, {e}")
        mqtt_power_value = CONFIG['wp_power_default']

mqtt_client.on_connect = on_mqtt_connect
mqtt_client.on_message = on_mqtt_message
try:
    mqtt_client.connect(CONFIG['mqtt_broker'], 1883, 60)
    mqtt_client.loop_start()
except Exception as e:
    logger.error(f"Failed to connect to MQTT broker {CONFIG['mqtt_broker']}: {e}")
    mqtt_power_value = CONFIG['wp_power_default']

try:
    ser = serial.Serial(CONFIG['serial_port'], CONFIG['baudrate'], timeout=CONFIG['serial_timeout'])
    time.sleep(2)
except Exception as e:
    logger.error(f"Failed to initialize serial port {CONFIG['serial_port']}: {e}")
    raise

def acquire_lock():
    my_pid = os.getpid()
    if LOCK_FILE.exists():
        try:
            pid = int(LOCK_FILE.read_text().strip())
            if psutil.pid_exists(pid):
                logger.error(f"Another instance is running with PID {pid}")
                return False
            else:
                logger.warning(f"Stale lock file found for PID {pid}, removing it")
                LOCK_FILE.unlink()
        except (ValueError, IOError) as e:
            logger.error(f"Error reading lock file {LOCK_FILE}: {e}")
            return False
    try:
        LOCK_FILE.write_text(str(my_pid))
        logger.debug(f"Lock acquired, PID {my_pid} written to {LOCK_FILE}")
        return True
    except Exception as e:
        logger.error(f"Failed to create lock file {LOCK_FILE}: {e}")
        return False

def release_lock():
    try:
        if LOCK_FILE.exists():
            LOCK_FILE.unlink()
            logger.debug(f"Lock file {LOCK_FILE} removed")
    except Exception as e:
        logger.error(f"Failed to remove lock file {LOCK_FILE}: {e}")

def read_file_last_line(file_path, default_values, parse_func):
    try:
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            return default_values, ""
        lines = file_path.read_text().strip().splitlines()
        if not lines:
            logger.warning(f"Empty file: {file_path}")
            return default_values, ""
        raw_line = lines[-1]
        parsed_data = parse_func(raw_line.split(','))
        if parsed_data is None:
            return default_values, raw_line
        return parsed_data, raw_line
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return default_values, ""

def parse_inverter(parts):
    try:
        parts = (parts + ['', '', '', '', '', ''])[:6]
        data = {
            'grid_w': int(float(parts[0])) if parts[0].strip() else 0,
            'sofarbat_w': int(float(parts[1])) if parts[1].strip() else 0,
            'soc_bat2': int(float(parts[2])) if parts[2].strip() else 0,
            'soc_bat1': int(float(parts[3])) if parts[3].strip() else 0,  # Nur zum Loggen
            'bat2_current': float(parts[4]) if parts[4].strip() else 0.0,
            'state': int(parts[5]) if parts[5].strip() else 4,
            'grid_w_valid': bool(parts[0].strip()),
            'bat_w_valid': bool(parts[1].strip()),
            'bat2_current_valid': bool(parts[4].strip()),
            'state_valid': bool(parts[5].strip())
        }
        if (abs(data['grid_w']) > 40000 or
            abs(data['sofarbat_w']) > 2999 or
            data['soc_bat2'] < 0 or data['soc_bat2'] > 100 or
            abs(data['bat2_current']) > CONFIG['max_bat2_current'] or
            data['state'] not in {0, 1, 2, 3, 4, 5, 6, 7}):
            logger.warning(f"Invalid inverter data: {parts}")
            return None
        return data
    except (IndexError, ValueError) as e:
        logger.error(f"Error parsing inverter data: {parts}, {e}")
        return None

def parse_soyopower(content):
    try:
        power = int(content.strip())
        if power < 0 or power > CONFIG['max_power']:
            logger.warning(f"Invalid soyopower value, out of range [0, {CONFIG['max_power']}]: {power}")
            return None
        return power
    except ValueError:
        logger.warning(f"Invalid soyopower format: {content}")
        return None

def read_soyopower():
    try:
        if not CONFIG['soyopower_file'].exists():
            logger.debug(f"Soyopower file not found: {CONFIG['soyopower_file']}")
            return None
        content = CONFIG['soyopower_file'].read_text()
        return parse_soyopower(content)
    except Exception as e:
        logger.error(f"Error reading soyopower file {CONFIG['soyopower_file']}: {e}")
        return None

def set_generated_power(power):
    power = max(0, min(power, CONFIG['max_power']))
    pu, pl = power >> 8, power & 0xFF
    crc = (264 - pu - pl) & 0xFF
    try:
        ser.write(b'\x24\x56\x00\x21' + power.to_bytes(2, 'big') + b'\x80' + crc.to_bytes(1, 'big'))
        logger.info(f"Power set to {power} W (from Battery)")
        if incoming := ser.read(100):
            logger.debug(f"Serial response: {incoming.hex()}")
    except Exception as e:
        logger.error(f"Serial write error: {e}")

class InverterController:
    def __init__(self):
        self.last_power = 0
        self.last_grid_w = 0
        self.last_sofarbat_w = 199
        self.last_soc_bat2 = 50
        self.last_bat2_current = 0
        self.priorities = CONFIG['priorities']
        self.active_condition = None

    def update(self):
        global mqtt_power_value
        now_str = datetime.now().strftime('%H:%M:%S')
        sun_data = subprocess.check_output(['./sunrise.py']).decode().strip().split(',')
        sunrise = sun_data[1]
        sunset = sun_data[2]
        is_time_window = (now_str < sunrise or now_str > sunset)
        logger.debug(f"Sun: Now={now_str}, Sunrise={sunrise}, Sunset={sunset}, IsNight={is_time_window}")

        inverter_data, raw_inverter_line = read_file_last_line(
            CONFIG['inverter_file'],
            {
                'grid_w': self.last_grid_w,
                'sofarbat_w': self.last_sofarbat_w,
                'soc_bat2': self.last_soc_bat2,
                'soc_bat1': 0,
                'bat2_current': self.last_bat2_current,
                'state': 4,
                'grid_w_valid': False,
                'bat_w_valid': False,
                'bat2_current_valid': False,
                'state_valid': False
            },
            parse_inverter
        )
        inverter_data = inverter_data or {
            'grid_w': self.last_grid_w,
            'sofarbat_w': self.last_sofarbat_w,
            'soc_bat2': self.last_soc_bat2,
            'soc_bat1': 0,
            'bat2_current': self.last_bat2_current,
            'state': 4,
            'grid_w_valid': False,
            'bat_w_valid': False,
            'bat2_current_valid': False,
            'state_valid': False
        }

        soyopower = read_soyopower()

        grid_w = inverter_data['grid_w']
        sofarbat_w = inverter_data['sofarbat_w']
        soc_bat2 = inverter_data['soc_bat2']
        soc_bat1 = inverter_data['soc_bat1']
        bat2_current = inverter_data['bat2_current']
        state = inverter_data['state']
        grid_w_valid = inverter_data['grid_w_valid']
        bat_w_valid = inverter_data['bat_w_valid']
        bat2_current_valid = inverter_data['bat2_current_valid']
        state_valid = inverter_data['state_valid']
        
        if bat2_current_valid:
            self.last_bat2_current = bat2_current

        if state_valid and state != 0:
            power = 0
            self.active_condition = 'state_not_0'
            logger.info(f"State={state} (not 0), forcing power to 0 W")
            set_generated_power(power)
            self.last_power = power
            logger.info(
                f"State: Raw_Inverter_Data={raw_inverter_line}, Grid_W={grid_w}, SoFarBat_W={sofarbat_w}, "
                f"SOC_Bat2={soc_bat2}% ({(soc_bat2 / 100) * CONFIG['battery_capacity_kwh']:.2f} kWh), SOC_Bat1={soc_bat1}% (ignored), "
                f"Power={power}W, Bat2_Current={bat2_current}A, "
                f"State={state}, SoyoPower={'N/A' if soyopower is None else soyopower}W"
            )
            return

        estimated_grid_w = grid_w if grid_w_valid else self.last_grid_w

        remaining_kwh = (soc_bat2 / 100) * CONFIG['battery_capacity_kwh']

        if grid_w_valid and grid_w < -100:
            logger.warning(f"Buying from grid: {abs(grid_w)}W, SOC_Bat2={soc_bat2}%")

        status_byte = 0
        debug_msg = ""
        
        if soc_bat2 < CONFIG['bat2_soc_min']:
            status_byte |= 1
            debug_msg = f"Entladeschutz (status_byte={status_byte}, SOC_Bat2={soc_bat2}%)"
        else:
            grid_value = grid_w if grid_w_valid else estimated_grid_w
            
            if grid_value < -100:
                status_byte |= 4
                debug_msg = f"Buying from grid ({grid_value}W), Battery supporting (status_byte={status_byte})"
            elif grid_value > 200:
                status_byte |= 8
                debug_msg = f"Feeding to grid ({grid_value}W), PV excess available (status_byte={status_byte})"
            else:
                status_byte |= 32
                debug_msg = f"System balanced ({grid_value}W), Battery supporting base load (status_byte={status_byte})"

        if status_byte not in VALID_STATUS_BYTES:
            logger.warning(f"Unexpected status_byte value: {status_byte}")

        power = 0
        self.active_condition = None
        
        for condition in self.priorities:
            if condition == 'entladeschutz' and status_byte & 1:
                power = 0
                self.active_condition = condition
                logger.debug(f"Power set to 0W due to Entladeschutz (SOC_Bat2={soc_bat2}%)")
                break
                
            elif condition == 'bat2_charging' and bat2_current_valid and bat2_current > 2:
                power = 0
                self.active_condition = condition
                logger.debug(f"Power set to 0W due to Bat2_Current={bat2_current}A > 2 (Bat2 charging)")
                break
                
            elif condition == 'soyopower' and soyopower is not None:
                power = min(soyopower, CONFIG['max_power'])
                self.active_condition = condition
                logger.debug(f"Power set to {power}W from soyopower.txt")
                break
                
            elif condition == 'buying_from_grid' and status_byte & 4:
                error = abs(grid_value)
                kp = 1.01
                power = min(int(error * kp) + (CONFIG['night_standard_power'] if is_time_window else 0), CONFIG['max_power'])
                self.active_condition = condition
                logger.debug(f"Power set to {power}W to offset grid purchase (Grid_W={grid_value}, Error={error})")
                break
                
            elif condition == 'feeding_to_grid' and status_byte & 8:
                power = 0
                self.active_condition = condition
                logger.debug(f"Power set to 0W due to excess PV (Grid_W={grid_value}W > 200W)")
                break
                
            elif condition == 'default' and status_byte & 32:
                if is_time_window:
                    power = CONFIG['night_standard_power']
                    self.active_condition = condition
                    logger.debug(f"Power set to {power}W for default system support during night")
                else:
                    power = 10
                    self.active_condition = condition
                    logger.debug(f"Power set to 10W for default system support")
                break

        wp_power = mqtt_power_value
        current_month = datetime.now().month
        night_standard_power = CONFIG['night_standard_power']
        
        if (self.active_condition in ['buying_from_grid', 'default'] and
            current_month in [9, 10, 11, 12, 1, 2]):
            if wp_power > night_standard_power:
                if self.active_condition == 'default':
                    power = night_standard_power
                    logger.debug(f"WP power ({wp_power} W) > {night_standard_power} in month {current_month}, setting power to {night_standard_power} W for WP support (active: {self.active_condition})")
                else:
                    power = min(power, night_standard_power)
                    logger.debug(f"WP power ({wp_power} W) > {night_standard_power} in month {current_month}, capping power to {night_standard_power} W (active: {self.active_condition})")
            else:
                power = max(0, power)
                logger.debug(f"WP power ({wp_power} W) <= {night_standard_power} in month {current_month}, keeping power: {power} W (active: {self.active_condition})")
        else:
            power = max(0, min(power, CONFIG['max_power']))
            if current_month not in [9, 10, 11, 12, 1, 2]:
                logger.debug(f"Month {current_month} (March-August), supporting WP fully, power: {power} W")

        self.last_power = power
        if grid_w_valid:
            self.last_grid_w = grid_w
        if bat_w_valid:
            self.last_sofarbat_w = sofarbat_w
        self.last_soc_bat2 = soc_bat2

        set_generated_power(power)
        if debug_msg:
            logger.debug(debug_msg)

        logger.info(
            f"State: Raw_Inverter_Data={raw_inverter_line}, Grid_W={estimated_grid_w}, SoFarBat_W={sofarbat_w}, "
            f"SOC_Bat2={soc_bat2}% ({remaining_kwh:.2f} kWh), SOC_Bat1={soc_bat1}% (ignored), "
            f"Power={power}W, Bat2_Current={bat2_current}A, "
            f"State={state}, SoyoPower={'N/A' if soyopower is None else soyopower}W, "
            f"WP_Power={wp_power}VA, ActiveCondition={self.active_condition}"
        )

def main():
    if not acquire_lock():
        logger.error("Exiting due to existing instance")
        sys.exit(1)
    try:
        controller = InverterController()
        while True:
            try:
                controller.update()
                time.sleep(2.0)
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(2)
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        release_lock()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Script terminated by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        ser.close()
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        logger.info("Serial and MQTT connections closed")
        release_lock()
