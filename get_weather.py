#!/usr/bin/env python3
"""
get_weather.py  — Wetter-Forecast von web1.heissa.de

Aufruf:
    python3 get_weather.py            # heute Tagesverlauf Sonnenzeit
    python3 get_weather.py --hours 8  # nächste 8h
    python3 get_weather.py --json     # Roh-JSON

Ausgabe (Default):
    Timestamp  Wolken%  Regen   Wetter
    Mittelwert Wolken% für alle Tageslichtstunden
"""
import sys
import json
import urllib.request
import datetime as dt
import argparse

URL      = "https://web1.heissa.de/web1/forecast_api.php"
TIMEOUT  = 5


def fetch(hours: int = 48) -> list[dict]:
    with urllib.request.urlopen(f"{URL}?hours={hours}", timeout=TIMEOUT) as r:
        return json.loads(r.read().decode()).get("rows", [])


def daylight_rows(rows: list[dict]) -> list[dict]:
    """Nur Zeilen zwischen Sonnenauf- und -untergang."""
    out = []
    for row in rows:
        sr = row.get("sunrise")
        ss = row.get("sunset")
        ts = row.get("timestamp")
        if not (sr and ss and ts):
            continue
        t  = dt.datetime.fromisoformat(ts)
        rise = dt.datetime.fromisoformat(sr)
        set_ = dt.datetime.fromisoformat(ss)
        if rise <= t <= set_:
            out.append(row)
    return out


def today_rows(rows: list[dict]) -> list[dict]:
    today = dt.date.today().isoformat()
    return [r for r in rows if r.get("timestamp", "").startswith(today)]


def print_table(rows: list[dict]) -> None:
    print(f"{'Zeit':<20} {'Wolken':>7} {'PoP':>5} {'Regen':>7}  Wetter")
    print("-" * 60)
    for r in rows:
        ts    = r.get("timestamp", "")[-8:-3]   # HH:MM
        cl    = r.get("cloudiness", 0)
        pop   = int(r.get("pop", 0) * 100)
        rain  = r.get("rain_3h", 0)
        wx    = r.get("weather", "")
        print(f"{ts:<20} {cl:>6}%  {pop:>4}%  {rain:>5.1f}mm  {wx}")


def avg_clouds(rows: list[dict]) -> float:
    if not rows:
        return 0.0
    return sum(r.get("cloudiness", 0) for r in rows) / len(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="Wetter-Forecast heissa.de")
    ap.add_argument("--hours", type=int, default=48)
    ap.add_argument("--json",  action="store_true", help="Roh-JSON ausgeben")
    args = ap.parse_args()

    rows = fetch(args.hours)

    if args.json:
        print(json.dumps(rows, indent=2, ensure_ascii=False))
        return

    today  = today_rows(rows)
    sun    = daylight_rows(today)

    print(f"\n=== Heute {dt.date.today()} — alle Slots ===")
    print_table(today)

    if sun:
        print(f"\n=== Nur Tageslichtstunden ===")
        print_table(sun)
        print(f"\nMittel Bewölkung (Tageslicht): {avg_clouds(sun):.0f}%")

    # Nächste 6h (für PRELOAD-Entscheidung)
    now  = dt.datetime.now()
    next6 = [r for r in rows
             if now <= dt.datetime.fromisoformat(r["timestamp"]) <= now + dt.timedelta(hours=6)]
    if next6:
        print(f"\n=== Nächste 6h (PRELOAD-Fenster) ===")
        print_table(next6)
        print(f"Mittel Bewölkung nächste 6h: {avg_clouds(next6):.0f}%")


if __name__ == "__main__":
    main()
