#!/usr/bin/env python3
"""
wetter_prognose_mqtt.py — Wetter-Tagesprognose Lenggries fuer ESP und fox2dbEasy.

Liest aus heissa wagodb.weather_data (OWM-Vorhersage, 3-h-Raster, von
save_weather.py alle 30 min fortgeschrieben) die Tageslicht-Slots des HEUTIGEN
Tages und publiziert deren Mittel retained:

  wetter/lenggries/heute
  {"date":"2026-09-14","yday":257,"cloud":91.8,"rain":4.56,"pop":0.62,
   "temp":16.9,"hum":87.2,"vis":8523,"wind":1.1,"slots":4,"ts":"..."}

Aggregation wie im Training (wetter_pv_modell.py, weather_daily): part_of_day='d',
Mittelwerte, Regen als Summe. Das kt-Modell selbst rechnen ESP (fox2db_logic.h)
und fox2dbEasy.py — hier werden nur die Eingaenge bereitgestellt.

Cron (Pi kellertreppe): 5,35 * * * * /usr/bin/python3 /home/pi/python/wetter_prognose_mqtt.py
"""
import json
import datetime as dt
import sys

import pymysql
import paho.mqtt.client as mqtt

DB_CONFIG = {
    'host': '10.8.0.1',
    'user': 'gh',
    'password': 'a12345',
    'database': 'wagodb',
    'charset': 'utf8mb4',
    'connect_timeout': 10,
}
LAT, LON = 47.6833, 11.5667          # OWM-Standort Lenggries (locations.key_name='lenggries')
BROKER, PORT = '192.168.178.218', 1883
TOPIC = 'wetter/lenggries/heute'
MIN_SLOTS = 2                        # Winter: nur 2 Tageslicht-Slots im 3-h-Raster


def fetch_today():
    heute = dt.date.today()
    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as c:
            c.execute(
                "SELECT COUNT(*), AVG(cloudiness), SUM(rain_3h), AVG(pop), AVG(temperature),"
                "       AVG(humidity), AVG(visibility), AVG(wind_speed)"
                "  FROM weather_data"
                " WHERE latitude = %s AND longitude = %s AND part_of_day = 'd'"
                "   AND timestamp >= %s AND timestamp < %s",
                (LAT, LON, heute, heute + dt.timedelta(days=1)))
            n, cloud, rain, pop, temp, hum, vis, wind = c.fetchone()
    finally:
        conn.close()
    if not n or n < MIN_SLOTS or cloud is None or hum is None or temp is None:
        return None
    return {
        'date': heute.isoformat(),
        'yday': heute.timetuple().tm_yday,
        'cloud': round(float(cloud), 2),
        'rain': round(float(rain or 0), 3),
        'pop': round(float(pop or 0), 3),
        'temp': round(float(temp), 2),
        'hum': round(float(hum), 2),
        'vis': round(float(vis if vis is not None else 10000)),
        'wind': round(float(wind or 0), 2),
        'slots': int(n),
        'ts': dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
    }


def main():
    wx = fetch_today()
    if wx is None:
        print('keine Tageslicht-Vorhersage fuer heute', file=sys.stderr)
        return 1
    payload = json.dumps(wx, separators=(',', ':'))
    client = mqtt.Client(client_id='wetter_prognose', clean_session=True)
    client.connect(BROKER, PORT, keepalive=10)
    client.loop_start()
    client.publish(TOPIC, payload, qos=1, retain=True).wait_for_publish(timeout=5)
    client.loop_stop()
    client.disconnect()
    print(payload)
    return 0


if __name__ == '__main__':
    sys.exit(main())
