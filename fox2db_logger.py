#!/usr/bin/env python3
import json
import pymysql
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler

# Logging
log_file = '/home/gh/fox2db_logger.log'
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(log_file, maxBytes=20000, backupCount=1)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Database Config
DB_CONFIG = {
    'host': '10.8.0.1',
    'user': 'gh',
    'password': 'a12345',
    'database': 'wagodb'
}

# MQTT Config
MQTT_CONFIG = {
    'broker': '10.8.0.6',
    'port': 1883,
    'state_topic': 'sofar/state',
    'ebox_topic':  'ebox/pwr',
}

# Letzter bekannter EBox-Leistungswert (negativ = Entladung)
ebox_power_w = None

def get_db_connection():
    return pymysql.connect(**DB_CONFIG)

def parse_timestamp(ts_string):
    try:
        if 'T' in ts_string:
            dt = datetime.fromisoformat(ts_string.replace('Z', '+00:00'))
        else:
            dt = datetime.strptime(ts_string, '%Y-%m-%d %H:%M:%S')
        if dt.tzinfo is None:
            import pytz
            local_tz = pytz.timezone('Europe/Berlin')
            dt = local_tz.localize(dt)
        return dt.astimezone(pytz.UTC).replace(tzinfo=None)
    except:
        return datetime.utcnow()

def insert_data(data):
    global ebox_power_w
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        sql = """
        INSERT INTO pv_inverter
        (timestamp, version, soc_bat2, soc_bat1, pcc, bat1, ebox,
         state, state_before, stable, excess, drop_rate, trace)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        ts = parse_timestamp(data.get('ts', datetime.now().isoformat()))

        # ebox: aus ebox/pwr wenn verfügbar, sonst Fallback auf Payload
        ebox = ebox_power_w if ebox_power_w is not None else data.get('ebox')

        # sofar/state: soc1/soc2 statt soc_bat1/soc_bat2, kein state_before/stable/drop_rate
        # Werte als Prozent speichern (0-100), Grafana-Query macht *100 → 0-10000
        soc1 = data.get('soc_bat1') or data.get('soc1')
        soc2 = data.get('soc_bat2') or data.get('soc2')

        values = (
            ts,
            data.get('version', 'v1.0-ESP'),
            soc2,
            soc1,
            data.get('pcc'),
            data.get('bat1'),
            ebox,
            data.get('state'),
            data.get('state_before'),
            data.get('stable'),
            data.get('excess'),
            data.get('drop_rate'),
            data.get('trace')
        )

        cursor.execute(sql, values)
        conn.commit()
        logger.info(f"Inserted: {ts} State={data.get('state')}, PCC={data.get('pcc')}W, EBox={ebox}W, Excess={data.get('excess')}W, soc1={soc1}, soc2={soc2}")

    except Exception as e:
        logger.error(f"Database error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        logger.info(f"Connected to MQTT broker {MQTT_CONFIG['broker']}")
        client.subscribe(MQTT_CONFIG['state_topic'])
        client.subscribe(MQTT_CONFIG['ebox_topic'])
    else:
        logger.error(f"Connection failed: {reason_code}")

def on_message(client, userdata, msg):
    global ebox_power_w
    try:
        data = json.loads(msg.payload.decode())
        if msg.topic == MQTT_CONFIG['ebox_topic']:
            pw = data.get('power_w')
            if pw is not None:
                ebox_power_w = round(pw)
        elif msg.topic == MQTT_CONFIG['state_topic']:
            insert_data(data)
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
    except Exception as e:
        logger.error(f"Message error: {e}")

def main():
    logger.info("Starting fox2db_logger service")

    client = mqtt.Client(CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(MQTT_CONFIG['broker'], MQTT_CONFIG['port'], 60)
        client.loop_forever()
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        client.disconnect()

if __name__ == "__main__":
    main()
