#!/usr/bin/python3

import csv
import pymysql
from db_config import get_db_connection
from datetime import datetime
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler
import paho.mqtt.client as mqtt
import json

log_file = '/tmp/pivot2db.log'
max_log_size = 100000
backup_count = 1

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = RotatingFileHandler(log_file, maxBytes=max_log_size, backupCount=backup_count)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# MQTT Configuration
MQTT_CONFIG = {
    'broker': 'localhost',
    'port': 1883,
    'topic': 'inverter/power_grid_exchange/json',
    'client_id': 'pivot2db_publisher'
}

def truncate_column_name(column_name):
    return column_name.split()[0]

def sanitize_value(value, is_numeric=True):
    if not is_numeric:
        return value
    if value is None or value == '' or pd.isna(value):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def check_and_update_table(cursor, table_name, columns):
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    if cursor.fetchone():
        cursor.execute(f"DESCRIBE `{table_name}`")
        existing_columns = [row[0] for row in cursor.fetchall()]
        expected_columns = set(['id', 'timestamp', 'device_id'] + columns)
        existing_columns_set = set(existing_columns)
        missing_columns = expected_columns - existing_columns_set
        if missing_columns:
            for col in missing_columns:
                if col not in ['id', 'timestamp', 'device_id']:
                    cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{col}` DECIMAL(10,4) DEFAULT NULL")
    else:
        column_defs = [f"`{col}` DECIMAL(10,4) DEFAULT NULL" for col in columns if col != 'section']
        if 'section' in columns: column_defs.append("`section` VARCHAR(255)")
        sql = f"CREATE TABLE `{table_name}` (id INT AUTO_INCREMENT PRIMARY KEY, timestamp DATETIME, device_id VARCHAR(50), {', '.join(column_defs)})"
        cursor.execute(sql)

def insert_data(cursor, table_name, data):
    sanitized_data = {
        k: sanitize_value(v, is_numeric=k not in ['section', 'device_id', 'timestamp', 'socmaxtime']) 
        for k, v in data.items()
    }
    cols = ', '.join([f"`{k}`" for k in sanitized_data.keys()])
    placeholders = ', '.join(['%s'] * len(sanitized_data))
    sql = f"INSERT INTO `{table_name}` ({cols}) VALUES ({placeholders})"
    cursor.execute(sql, list(sanitized_data.values()))

def publish_to_mqtt(data_row):
    if not hasattr(publish_to_mqtt, "last_soc"): publish_to_mqtt.last_soc = 0
    """
    Publiziert Inverter-Daten via MQTT für fox2db.py - FLOAT WERTE (2 NKST)
    Kompatibel mit paho-mqtt < 2.0 und >= 2.0
    
    Erwartet von fox2db.py:
    - ActivePower_PCC_Total (in W als Float) - Grid Power
    - Power_Bat1 (in W als Float) - Battery Power
    - SOC_Bat1 (in % als Float) - Battery State of Charge
    """
    try:
        active_power_pcc = sanitize_value(data_row.get('ActivePower_PCC_Total'))
        if active_power_pcc is not None:
            active_power_pcc = round(float(active_power_pcc), 2)

        power_bat1 = sanitize_value(data_row.get('Power_Bat1'))
        if power_bat1 is not None:
            power_bat1 = round(float(power_bat1), 2)

        # SOC: keep last-known fallback so fox2db always gets a valid SOC
        soc_bat1 = sanitize_value(data_row.get('SOC_Bat1'))
        if soc_bat1 is not None:
            soc_bat1 = round(float(soc_bat1), 2)
        if not soc_bat1 or soc_bat1 <= 0:
            soc_bat1 = publish_to_mqtt.last_soc
        else:
            publish_to_mqtt.last_soc = soc_bat1

        power_pv1 = sanitize_value(data_row.get('Power_PV1'))
        if power_pv1 is not None:
            power_pv1 = round(float(power_pv1), 2)

        power_pv2 = sanitize_value(data_row.get('Power_PV2'))
        if power_pv2 is not None:
            power_pv2 = round(float(power_pv2), 2)

        load_sys = sanitize_value(data_row.get('ActivePower_Load_Sys'))
        if load_sys is not None:
            load_sys = round(float(load_sys), 2)

        # MQTT Payload erstellen - FLOAT WERTE (2 NKST)
        payload = {
            'ActivePower_PCC_Total': active_power_pcc,
            'Power_Bat1': power_bat1,
            'SOC_Bat1': soc_bat1,
            'Power_PV1': power_pv1,
            'Power_PV2': power_pv2,
            'ActivePower_Load_Sys': load_sys,
            'timestamp': datetime.now().isoformat()
        }
        
        # MQTT Client erstellen - Kompatibel mit alter und neuer paho-mqtt Version
        try:
            # paho-mqtt >= 2.0
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CONFIG['client_id'])
        except (AttributeError, TypeError):
            # paho-mqtt < 2.0 (Fallback)
            client = mqtt.Client(client_id=MQTT_CONFIG['client_id'])
        
        client.connect(MQTT_CONFIG['broker'], MQTT_CONFIG['port'], 60)
        
        # Payload als JSON
        json_payload = json.dumps(payload, ensure_ascii=False)
        
        # Publish
        result = client.publish(MQTT_CONFIG['topic'], json_payload, qos=1, retain=True)
        
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            logger.info(f"MQTT Published: PCC={payload['ActivePower_PCC_Total']}W, "
                       f"Bat1={payload['Power_Bat1']}W, SOC={payload['SOC_Bat1']}%, "
                       f"Load={payload['ActivePower_Load_Sys']}kW")
        else:
            logger.error(f"MQTT Publish failed: {result.rc}")
        
        client.disconnect()
        
    except Exception as e:
        logger.error(f"MQTT Publishing Error: {e}")
        import traceback
        logger.debug(traceback.format_exc())

def main():
    csv_file_path = '/tmp/pivoted_registers.csv'
    table_name = 'inverter_data'
    device_id = '1'
    
    try:
        df = pd.read_csv(csv_file_path)
        columns = [truncate_column_name(col) for col in df.columns]
        if 'socmaxtime' not in columns: columns.append('socmaxtime')

        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute("SELECT @@read_only")
            if cursor.fetchone()[0]: return

            check_and_update_table(cursor, table_name, columns)
            
            cursor.execute("""
                SELECT MAX(SOC_Bat1), MAX(socmaxtime) FROM inverter_data 
                WHERE DATE(timestamp) = CURDATE() AND device_id = %s
            """, (device_id,))
            res = cursor.fetchone()
            current_max = res[0] if res and res[0] is not None else 0
            already_has_time = True if res and res[1] is not None else False
            
            # Alle Zeilen zu einem einzigen Dict mergen (erster Non-NULL-Wert gewinnt)
            merged = {}
            for _, row in df.iterrows():
                for k, v in row.items():
                    key = truncate_column_name(k)
                    import pandas as _pd
                    is_null = v is None or (isinstance(v, float) and _pd.isna(v))
                    if key not in merged:
                        merged[key] = None if is_null else v
                    elif merged[key] is None and not is_null:
                        merged[key] = v

            truncated_row = merged

            # Lesefehler-Erkennung: Inverter liefert bei Timeout 0.0 statt leer.
            # Echtes Gleichgewicht (PCC=0) hat stets verteilte Phasenwerte;
            # bei Lesefehler sind alle drei Phasen exakt 0.0.
            def _is_zero(val):
                if val is None:
                    return False  # missing field != zero
                try:
                    return float(val) == 0.0
                except (TypeError, ValueError):
                    return False
            if (_is_zero(truncated_row.get('ActivePower_PCC_Total')) and
                    _is_zero(truncated_row.get('ActivePower_PCC_R')) and
                    _is_zero(truncated_row.get('ActivePower_PCC_S')) and
                    _is_zero(truncated_row.get('ActivePower_PCC_T'))):
                truncated_row['ActivePower_PCC_Total'] = None
                truncated_row['ActivePower_PCC_R']     = None
                truncated_row['ActivePower_PCC_S']     = None
                truncated_row['ActivePower_PCC_T']     = None
                logger.warning('Lesefehler erkannt (alle PCC-Phasen=0) → NULL gespeichert')

            now = datetime.now()
            truncated_row['timestamp'] = now.strftime('%Y-%m-%d %H:%M:%S')
            truncated_row['device_id'] = device_id

            this_soc = sanitize_value(truncated_row.get('SOC_Bat1'))

            if this_soc is not None:
                if this_soc >= 99.0:
                    if not already_has_time:
                        truncated_row['socmaxtime'] = now.strftime('%H:%M:%S')
                        already_has_time = True
                        current_max = this_soc
                    else:
                        truncated_row['socmaxtime'] = None
                elif this_soc > current_max:
                    current_max = this_soc
                    truncated_row['socmaxtime'] = now.strftime('%H:%M:%S')
                else:
                    truncated_row['socmaxtime'] = None
            else:
                truncated_row['socmaxtime'] = None

            insert_data(cursor, table_name, truncated_row)

            # MQTT Publishing nach dem einzigen DB-Insert
            publish_to_mqtt(truncated_row)
            
            connection.commit()
            logger.info(f"Successfully processed {len(df)} rows")

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        if 'connection' in locals(): 
            connection.close()

if __name__ == "__main__":
    main()
