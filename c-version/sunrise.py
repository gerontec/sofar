#!/usr/bin/python3
from astral import LocationInfo
from astral.sun import sun
from datetime import datetime, timedelta
import pytz

def get_lenggries_sun_csv():
    # Konfiguration für Lenggries (PLZ 83661)
    city = LocationInfo("Lenggries", "Germany", "Europe/Berlin", 47.6811, 11.5732)

    # Aktuelles Datum und Zeitzone
    tz = pytz.timezone('Europe/Berlin')
    today = datetime.now(tz)
    s = sun(city.observer, date=today, tzinfo=city.timezone)

    # 60 Minuten zum Sonnenaufgang addieren
    sunrise_plus_60 = s['sunrise'] + timedelta(minutes=60)
    sundown_60 = s['sunset'] - timedelta(minutes=60)

    # Formatierung der Zeiten (HH:MM:SS)
    sunrise_str = sunrise_plus_60.strftime('%H:%M:%S')
    sunset_str = sundown_60.strftime('%H:%M:%S')

    date_str = today.strftime('%Y-%m-%d')

    # CSV String erstellen
    csv_string = f"{date_str},{sunrise_str},{sunset_str}"
    return csv_string

if __name__ == "__main__":
    print(get_lenggries_sun_csv())
