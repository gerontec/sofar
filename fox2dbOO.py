#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import datetime as _dt
import zoneinfo as _zoneinfo
from astral import LocationInfo as _LocationInfo
from astral.sun import sun as _astral_sun, elevation as _astral_elevation, azimuth as _astral_azimuth

import urllib.request
import pymysql
import paho.mqtt.client as mqtt

# ═══════════════════════════════════════════════════════════════════════════
# VERSION & KONSTANTEN
# ═══════════════════════════════════════════════════════════════════════════

VERSION = "v1.73-Py"
MAX_LOG_BYTES = 122 * 1024  # 122 kB, dann truncate

def _solar_noon(lat: float, lon: float) -> _dt.datetime:
    """Solarer Höchststand heute als timezone-aware datetime (Europe/Berlin)."""
    loc = _LocationInfo("Standort", "Germany", "Europe/Berlin", lat, lon)
    s = _astral_sun(loc.observer, date=_dt.date.today(), tzinfo=loc.timezone)
    return s["noon"]



STATE_POWER: dict[int, int] = {
    0: 0,
    1: 3000,
    2: 3650,
    3: 6650,
    4: 3900,
    5: 7100,
    6: 7800,
    7: 11400,
}


# ═══════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class Config:
    """Alle Konfigurations-Parameter. Defaults entsprechen fox2db.c."""

    # Regelwerk
    min_excess: int = 1010
    max_grid_draw: int = 400
    max_soc: int = 99
    hysteresis: int = 505
    stabilization_cycles: int = 2
    emergency_import: int = 1020
    bat_discharge_threshold: int = -220
    sweet_spot_pcc: int = 160
    sweet_spot_bat: int = -310
    max_drop_rate: int = -20
    midday_soc_threshold: int = 70      # SOC2-Schwelle für Mittagskapp
    midday_latitude: float = 47.6811    # Lenggries (PLZ 83661)
    midday_longitude: float = 11.5732
    feedin_dc_season_threshold_w: int = 12_000  # DC-Modell Mittag > X W → FeedIn/Midday aktiv
    feedin_limit_w: int = 19_000        # Einspeisung ab der geladen wird
    feedin_preload_w: int = 22_000      # Prognose-Schwelle für State-1-Vorschalten
    feedin_max_prognose_w: int = 35_000    # Prognose-Cap: gemessenes Allzeit-Maximum
    feedin_forecast_url: str = "https://web1.heissa.de/web1/forecast_api.php"
    feedin_forecast_hours: int = 4         # Stunden voraus für PRELOAD-Gate
    feedin_forecast_bad_clouds: float = 70.0  # Wolkenbedeckung % > Schwelle → kein PRELOAD
    feedin_clouds_no_peak: float = 60.0       # Wolken % > Schwelle → kein Cap-Risiko → CLOUD_FREE
    feedin_dc_cap_safe_w: int = 18_000        # dc_expected > X W → Risikofenster aktiv (Mittag/Bremse/DC_SAFE)
    bat2_capacity_wh: int = 30_000            # EBox bat2 Kapazität in Wh (für Ladeziel-Berechnung)
    deep_discharge_lower: int = 6
    deep_discharge_upper: int = 8
    deep_discharge_charge_target: int = 7

    # MQTT Empfang
    mqtt_broker: str = "kellertreppe.fritz.box"
    mqtt_port: int = 1883
    mqtt_topic: str = "inverter/power_grid_exchange/json"
    mqtt_timeout: int = 43

    # MQTT Zähler
    mqtt_zaehl_topic: str = "pv_zaehl2/#"
    mqtt_zaehl_timeout: int = 10        # kurzes Timeout – Zähler sendet häufig
    wirkleist_r4_threshold: int = -20_100  # W, negativ = Einspeisung

    # MQTT Publish
    mqtt_publish_broker: str = ""        # leer = gleich wie mqtt_broker
    mqtt_publish_port: int = 0           # 0 = gleich wie mqtt_port
    mqtt_publish_topic: str = "fox2db/state"
    mqtt_publish_retain: int = 1
    mqtt_publish_qos: int = 0

    # Dateipfade
    path_deep_discharge: str = "/tmp/deep_discharge_protection_active.txt"
    path_log: str = "/tmp/fox2db.log"
    path_relay_state: str = "/tmp/current_relay_state.txt"
    path_last_change: str = "/tmp/last_relay_change.txt"
    path_last_excess: str = "/tmp/last_excess.txt"
    path_ebox_data: str = "/tmp/ebox15k.txt"
    path_inverter_csv: str = "/tmp/inverter.csv"
    path_ebyte_script: str = "/home/pi/python/ebyte_ctrl.py"

    @classmethod
    def from_args(cls) -> "Config":
        """CLI-Parsing mit argparse – 1:1-Mapping zu den C-Optionen."""
        p = argparse.ArgumentParser(
            description=f"fox2db {VERSION} – SOYO Inverter Power Management Controller"
        )
        p.add_argument("--mqtt-broker", default="kellertreppe.fritz.box")
        p.add_argument("--mqtt-port", type=int, default=1883)
        p.add_argument("--mqtt-topic", default="inverter/power_grid_exchange/json")
        p.add_argument("--mqtt-timeout", type=int, default=43)
        p.add_argument("--mqtt-zaehl-topic", default="pv_zaehl2/#")
        p.add_argument("--mqtt-zaehl-timeout", type=int, default=10)
        p.add_argument("--wirkleist-r4-threshold", type=int, default=-20_100,
                       help="Zähler-Wirkleistung (W) unter der Relais-4 ausgelöst wird (negativ=Einspeisung)")
        p.add_argument("--min-excess", type=int, default=1010)
        p.add_argument("--max-grid-draw", type=int, default=400)
        p.add_argument("--max-soc", type=int, default=99)
        p.add_argument("--hysteresis", type=int, default=505)
        p.add_argument("--stabilization-cycles", type=int, default=2)
        p.add_argument("--emergency-import", type=int, default=1020)
        p.add_argument("--bat-discharge-threshold", type=int, default=-220)
        p.add_argument("--sweet-spot-pcc", type=int, default=160)
        p.add_argument("--sweet-spot-bat", type=int, default=-310)
        p.add_argument("--max-drop-rate", type=int, default=-20)
        p.add_argument("--deep-discharge-lower", type=int, default=6)
        p.add_argument("--deep-discharge-upper", type=int, default=8)
        p.add_argument("--deep-discharge-target", type=int, default=7)
        p.add_argument("--midday-soc-threshold", type=int, default=80)
        p.add_argument("--midday-lat", type=float, default=47.6811)
        p.add_argument("--midday-lon", type=float, default=11.5732)
        p.add_argument("--feedin-dc-season-threshold", type=int, default=12_000,
                       help="DC-Klarhimmel-Mittag > X W → FeedIn/Midday aktiv (default 12000)")
        p.add_argument("--feedin-limit", type=int, default=19_000,
                       help="Einspeisung (W) ab der FeedInLimiter lädt (default 19000)")
        p.add_argument("--feedin-preload", type=int, default=22_000,
                       help="Prognose-Schwelle (W) für State-1-Vorschalten (default 22000)")
        p.add_argument("--feedin-forecast-url",
                       default="https://web1.heissa.de/web1/forecast_api.php")
        p.add_argument("--feedin-forecast-hours", type=int, default=4,
                       help="Stunden voraus für PRELOAD-Gate (default 4)")
        p.add_argument("--feedin-forecast-bad-clouds", type=float, default=70.0,
                       help="Wolkenbedeckung %% > Schwelle → PRELOAD unterdrückt (default 70)")
        p.add_argument("--ebyte-script", default="/home/pi/python/ebyte_ctrl.py")
        p.add_argument("--mqtt-publish-broker", default="")
        p.add_argument("--mqtt-publish-port", type=int, default=0)
        p.add_argument("--mqtt-publish-topic", default="fox2db/state")
        p.add_argument("--mqtt-publish-retain", type=int, default=1)
        p.add_argument("--mqtt-publish-qos", type=int, default=0)
        p.add_argument("--version", action="version", version=VERSION)

        a = p.parse_args()
        return cls(
            min_excess=a.min_excess,
            max_grid_draw=a.max_grid_draw,
            max_soc=a.max_soc,
            hysteresis=a.hysteresis,
            stabilization_cycles=a.stabilization_cycles,
            emergency_import=a.emergency_import,
            bat_discharge_threshold=a.bat_discharge_threshold,
            sweet_spot_pcc=a.sweet_spot_pcc,
            sweet_spot_bat=a.sweet_spot_bat,
            max_drop_rate=a.max_drop_rate,
            deep_discharge_lower=a.deep_discharge_lower,
            deep_discharge_upper=a.deep_discharge_upper,
            deep_discharge_charge_target=a.deep_discharge_target,
            midday_soc_threshold=a.midday_soc_threshold,
            midday_latitude=a.midday_lat,
            midday_longitude=a.midday_lon,
            feedin_dc_season_threshold_w=a.feedin_dc_season_threshold,
            feedin_limit_w=a.feedin_limit,
            feedin_preload_w=a.feedin_preload,
            feedin_forecast_url=a.feedin_forecast_url,
            feedin_forecast_hours=a.feedin_forecast_hours,
            feedin_forecast_bad_clouds=a.feedin_forecast_bad_clouds,
            mqtt_broker=a.mqtt_broker,
            mqtt_port=a.mqtt_port,
            mqtt_topic=a.mqtt_topic,
            mqtt_timeout=a.mqtt_timeout,
            mqtt_zaehl_topic=a.mqtt_zaehl_topic,
            mqtt_zaehl_timeout=a.mqtt_zaehl_timeout,
            wirkleist_r4_threshold=a.wirkleist_r4_threshold,
            mqtt_publish_broker=a.mqtt_publish_broker,
            mqtt_publish_port=a.mqtt_publish_port,
            mqtt_publish_topic=a.mqtt_publish_topic,
            mqtt_publish_retain=a.mqtt_publish_retain,
            mqtt_publish_qos=a.mqtt_publish_qos,
            path_ebyte_script=a.ebyte_script,
        )


# ═══════════════════════════════════════════════════════════════════════════
# LOGGER
# ═══════════════════════════════════════════════════════════════════════════

class Logger:
    """Datei-Logger mit Auto-Truncate bei MAX_LOG_BYTES."""

    def __init__(self, path: str) -> None:
        self._path = Path(path)

    def log(self, msg: str) -> None:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts} {VERSION}] {msg}\n"
        mode = "w" if self._path.exists() and self._path.stat().st_size >= MAX_LOG_BYTES else "a"
        try:
            with self._path.open(mode) as f:
                f.write(line)
        except OSError as e:
            print(f"Log Error: {e}", file=sys.stderr)

    def __call__(self, msg: str) -> None:
        self.log(msg)


# ═══════════════════════════════════════════════════════════════════════════
# DATENTRANSFER-OBJEKTE
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class MeasuredValues:
    """Alle Messwerte eines Zyklus."""
    pcc: float = 0.0
    bat1: float = 0.0
    soc_bat1: float = 0.0
    bat_cur: float = 0.0       # EBox: Batterie-Strom in A
    soc: float = -1.0          # EBox: min SOC aller Packs (-1 = unbekannt)
    relay_st: int = 0
    stable: int = 0
    prot: int = 0
    last_excess: float = 0.0
    dc_pv: float = 0.0        # Sofar: Power_PV1 + Power_PV2 in kW
    wirkleist: float = 0.0    # Zähler: Wirkleistung in W (negativ = Einspeisung)
    wp_power: float = 0.0     # Wärmepumpe em0/54/power in W (nicht in Z2 enthalten)
    dc_expected: float = 0.0  # Klarhimmel-DC-Prognose in W (getdc-Modell)


@dataclass
class ControlDecision:
    """Ergebnis der Regellogik."""
    best_state: int = 0
    final_state: int = 0
    changed: bool = False
    excess: float = 0.0
    drop_rate: float = 0.0
    has_drop_rate: bool = False
    after_peak: bool = False
    trace: str = ""


# ═══════════════════════════════════════════════════════════════════════════
# MQTT CLIENT (Empfang)
# ═══════════════════════════════════════════════════════════════════════════

class MqttClient:
    """Subscribt einmalig auf den Inverter-Topic und wartet auf eine Nachricht."""

    def __init__(self, cfg: Config, log: Logger) -> None:
        self._cfg = cfg
        self._log = log

    def fetch(self) -> Optional[dict]:
        """Blockiert bis Nachricht eintrifft oder Timeout. Gibt geparsten dict zurück."""
        received: list[dict] = []

        def on_message(_client, _userdata, message):
            try:
                payload = message.payload.decode()
                data = json.loads(payload)
                received.append(data)
            except Exception as e:
                self._log(f"MQTT JSON Parse Error: {e}")

        client = mqtt.Client(client_id="fox2db_client", clean_session=True)
        client.on_message = on_message

        try:
            client.connect(self._cfg.mqtt_broker, self._cfg.mqtt_port, keepalive=60)
        except Exception as e:
            self._log(f"MQTT Connection failed: {e}")
            return None

        client.subscribe(self._cfg.mqtt_topic, qos=0)
        client.loop_start()

        deadline = time.monotonic() + self._cfg.mqtt_timeout
        while not received and time.monotonic() < deadline:
            time.sleep(0.1)

        client.loop_stop()
        client.disconnect()

        if not received:
            self._log(f"CRITICAL: MQTT Timeout after {self._cfg.mqtt_timeout}s – NO DATA AVAILABLE")
            return None

        raw = received[0]
        _pcc_raw = raw.get("ActivePower_PCC_Total")
        pcc = _pcc_raw * 1000.0 if _pcc_raw is not None else float("nan")  # fehlendes Feld ≠ 0W
        _bat1_raw = raw.get("Power_Bat1")
        bat1 = _bat1_raw * 1000.0 if _bat1_raw is not None else 0.0
        _soc_raw = raw.get("SOC_Bat1")
        soc_bat1 = float(_soc_raw) if _soc_raw is not None else 0.0
        _pv1_raw = raw.get("Power_PV1")
        _pv2_raw = raw.get("Power_PV2")
        dc_pv = (_pv1_raw if _pv1_raw is not None else 0.0) + (_pv2_raw if _pv2_raw is not None else 0.0)
        self._log(f"MQTT Received: PCC={pcc:.0f}W, Bat1={bat1:.0f}W, SOC_Bat1={soc_bat1:.1f}%, PV_DC={dc_pv:.2f}kW")
        return {"pcc": pcc, "bat1": bat1, "soc_bat1": soc_bat1, "dc_pv": dc_pv}

    def fetch_wirkleist(self) -> float:
        """
        Liest einmalig wirkleist aus pv_zaehl2/#.
        Gibt 0.0 bei Timeout oder Fehler zurück.
        Negativ = Einspeisung ins Netz.
        """
        received: list[float] = []

        def on_message(_client, _userdata, message):
            try:
                data = json.loads(message.payload.decode())
                wl = data.get("wirkleist")
                if wl is not None:
                    received.append(float(wl))
            except Exception as e:
                self._log(f"MQTT wirkleist JSON Parse Error: {e}")

        client = mqtt.Client(client_id="fox2db_zaehl_client", clean_session=True)
        client.on_message = on_message

        try:
            client.connect(self._cfg.mqtt_broker, self._cfg.mqtt_port, keepalive=60)
        except Exception as e:
            self._log(f"MQTT wirkleist Connection failed: {e}")
            return 0.0

        client.subscribe(self._cfg.mqtt_zaehl_topic, qos=0)
        client.loop_start()

        deadline = time.monotonic() + self._cfg.mqtt_zaehl_timeout
        while not received and time.monotonic() < deadline:
            time.sleep(0.1)

        client.loop_stop()
        client.disconnect()

        if not received:
            self._log(f"WARNING: wirkleist Timeout after {self._cfg.mqtt_zaehl_timeout}s – verwende 0.0")
            return 0.0

        wl = received[0]
        self._log(f"Zaehler wirkleist={wl:.0f}W")
        return wl


    def fetch_wp_power(self) -> float:
        """Liest em0/54/power (Waermepumpe, nicht in Z2 enthalten). Retained topic."""
        received: list[float] = []

        def on_message(_c, _u, msg):
            try:
                received.append(float(msg.payload.decode().strip()))
            except Exception:
                pass

        cl = mqtt.Client(client_id="fox2db_wp_client", clean_session=True)
        cl.on_message = on_message
        try:
            cl.connect(self._cfg.mqtt_broker, self._cfg.mqtt_port, keepalive=60)
        except Exception as e:
            self._log(f"MQTT wp_power failed: {e}")
            return 0.0
        cl.subscribe("em0/54/power", qos=0)
        cl.loop_start()
        deadline = time.monotonic() + 2
        while not received and time.monotonic() < deadline:
            time.sleep(0.1)
        cl.loop_stop()
        cl.disconnect()
        wp = received[0] if received else 0.0
        self._log(f"WP power={wp:.0f}W")
        return wp


# ═══════════════════════════════════════════════════════════════════════════
# MQTT PUBLISHER (Ausgabe)
# ═══════════════════════════════════════════════════════════════════════════

class MqttPublisher:
    """Publiziert den Zyklus-JSON direkt per MQTT."""

    def __init__(self, cfg: Config, log: Logger) -> None:
        self._cfg = cfg
        self._log = log

    def publish(self, payload: dict) -> None:
        broker = self._cfg.mqtt_publish_broker or self._cfg.mqtt_broker
        port = self._cfg.mqtt_publish_port or self._cfg.mqtt_port
        json_str = json.dumps(payload, separators=(",", ":"))

        client = mqtt.Client(client_id="fox2db_pub", clean_session=True)
        try:
            client.connect(broker, port, keepalive=10)
            result = client.publish(
                self._cfg.mqtt_publish_topic,
                json_str,
                qos=self._cfg.mqtt_publish_qos,
                retain=bool(self._cfg.mqtt_publish_retain),
            )
            result.wait_for_publish(timeout=4)
            self._log(f"MQTT-Publish: {len(json_str)} Bytes → {self._cfg.mqtt_publish_topic} "
                      f"(retain={self._cfg.mqtt_publish_retain})")
        except Exception as e:
            self._log(f"MQTT-Publish Error: {e}")
        finally:
            client.disconnect()


# ═══════════════════════════════════════════════════════════════════════════
# EBOX READER
# ═══════════════════════════════════════════════════════════════════════════

class EBoxReader:
    """Liest Batterie-Daten aus der EBox-Ausgabedatei."""

    def __init__(self, cfg: Config, log: Logger) -> None:
        self._cfg = cfg
        self._log = log

    def update(self) -> None:
        """Führt 'ebox pwr' aus und schreibt Ergebnis in Datei."""
        cmd = f"/usr/local/bin/ebox pwr > {self._cfg.path_ebox_data}"
        try:
            ret = subprocess.run(cmd, shell=True, timeout=10, capture_output=True)
            if ret.returncode != 0:
                self._log(f"EBox Update failed (rc={ret.returncode})")
        except subprocess.TimeoutExpired:
            self._log("EBox Update timeout")

    def _fetch_cells(self, pack: int, ts) -> list:
        """Fuehrt 'ebox bat <pack>' aus und gibt DB-Rows fuer pv_ebox_cells zurueck."""
        try:
            ret = subprocess.run(
                ["/usr/local/bin/ebox", "bat", str(pack)],
                capture_output=True, text=True, timeout=12, errors="replace"
            )
            output = ret.stdout
        except Exception as e:
            self._log(f"EBox bat {pack} error: {e}")
            return []

        rows = []
        for line in output.splitlines():
            parts = line.split()
            if not parts or not parts[0].isdigit():
                continue
            if len(parts) < 9:
                continue
            try:
                rows.append((
                    ts,
                    pack,
                    int(parts[0]),
                    int(parts[1]),
                    int(parts[3]),
                    int(parts[3]),
                    parts[4],
                    parts[5],
                    parts[6],
                    parts[7],
                    int(parts[8].replace("%", "")),
                ))
            except (ValueError, IndexError):
                continue
        return rows

    def _write_cells_to_db(self, rows: list) -> None:
        """Schreibt Einzelzell-Daten in MariaDB wagodb.pv_ebox_cells."""
        if not rows:
            return
        try:
            conn = pymysql.connect(
                host="192.168.178.218",
                database="wagodb",
                user="gh",
                password="a12345",
                connect_timeout=5,
            )
            cur = conn.cursor()
            cur.executemany(
                """INSERT INTO pv_ebox_cells
                   (ts, pack, cell, volt, curr, tempr,
                    base_st, volt_st, curr_st, temp_st, coulomb)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                rows,
            )
            conn.commit()
            cur.close()
            conn.close()
            self._log(f"EBox cells: {len(rows)} Zeilen in pv_ebox_cells geschrieben")
        except Exception as e:
            self._log(f"EBox cells DB write error: {e}")

    def _fetch_soh(self, pack: int) -> float:
        """Gibt SOH in % zurück (Real Coulomb / Total Coulomb). -1.0 bei Fehler."""
        try:
            ret = subprocess.run(
                ["/usr/local/bin/ebox", "pwr", str(pack)],
                capture_output=True, text=True, timeout=10, errors="replace"
            )
            real = total = None
            for line in ret.stdout.splitlines():
                if "Real Coulomb" in line:
                    parts = line.split()
                    real = float(parts[3])
                elif "Total Coulomb" in line:
                    parts = line.split()
                    total = float(parts[3])
            if real and total and total > 0:
                return round(real / total * 100, 1)
        except Exception as e:
            self._log(f"EBox pwr {pack} SOH error: {e}")
        return -1.0

    def collect_cells_if_needed(self, min_soc: float) -> None:
        """Einzelzell-Erfassung bei SOC > 99 % oder SOC < 20 %."""
        if min_soc < 0:
            return
        if not (min_soc > 99 or min_soc < 20):
            return
        ts = _dt.datetime.now()
        self._log(f"EBox cells: Einzelzell-Erfassung (SOC={min_soc:.1f}%)")
        time.sleep(4)  # Seriell-Port nach ebox pwr Abfrage entlasten
        all_rows: list = []
        for pack in (1, 2, 3):
            all_rows.extend(self._fetch_cells(pack, ts))
            time.sleep(4)  # Pause zwischen Pack-Abfragen
        self._write_cells_to_db(all_rows)
        if all_rows:
            worst = min(all_rows, key=lambda r: r[3])
            best  = max(all_rows, key=lambda r: r[3])
            self._log(
                f"EBox cells: MIN Pack{worst[1]} Zelle{worst[2]} {worst[3]}mV ({worst[3]/1000:.3f}V) SOC={worst[10]}%  |  "
                f"MAX Pack{best[1]} Zelle{best[2]} {best[3]}mV ({best[3]/1000:.3f}V) SOC={best[10]}%  |  "
                f"Delta {best[3]-worst[3]}mV")
        soh_vals = []
        for pack in (1, 2, 3):
            soh = self._fetch_soh(pack)
            if soh > 0:
                soh_vals.append((pack, soh))
            time.sleep(2)
        if soh_vals:
            worst_soh = min(soh_vals, key=lambda x: x[1])
            soh_str = "  ".join(f"Pack{p}={s}%" for p, s in soh_vals)
            self._log(f"EBox SOH: {soh_str}  |  MIN Pack{worst_soh[0]}={worst_soh[1]}%")

    def read(self, write_db: bool = True) -> tuple:
        """
        Liest ebox_data-Datei, gibt (bat_cur_A, min_soc) zurueck.
        write_db=True: INSERT in pv_ebox2 (wird vom Aufrufer gedrosselt).
        """
        path = Path(self._cfg.path_ebox_data)
        if not path.exists():
            self._log(f"EBox data file not found: {path}")
            return 0.0, -1.0

        total_current_ma = 0.0
        soc_values: list[float] = []
        db_rows: list[tuple] = []

        try:
            for raw_line in path.read_text().splitlines():
                line = raw_line.strip().lstrip("b'").rstrip("'")
                if not line or line.startswith("Power") or "$" in line or "#" in line:
                    continue
                if line[0] in "123":
                    parts = line.split()
                    if len(parts) < 13:
                        continue
                    if parts[8] == "Absent":
                        continue
                    try:
                        total_current_ma += float(parts[2])
                    except ValueError:
                        pass
                    for p in parts:
                        if "%" in p:
                            try:
                                soc_values.append(float(p.replace("%", "")))
                            except ValueError:
                                pass
                    try:
                        db_rows.append((
                            float(parts[0]),
                            float(parts[1]),
                            float(parts[2]),
                            float(parts[3]),
                            float(parts[4]),
                            float(parts[5]),
                            float(parts[6]),
                            float(parts[7]),
                            parts[8],
                            parts[9],
                            parts[10],
                            parts[11],
                            float(parts[12].replace("%", "")),
                            _dt.datetime.now(),
                        ))
                    except (ValueError, IndexError):
                        pass
        except OSError as e:
            self._log(f"EBox read error: {e}")
            return 0.0, -1.0

        if db_rows and write_db:
            try:
                conn = pymysql.connect(
                    host="192.168.178.218",
                    database="wagodb",
                    user="gh",
                    password="a12345",
                    connect_timeout=5,
                )
                cur = conn.cursor()
                cur.executemany(
                    """INSERT INTO pv_ebox2
                       (Power, Volt, Curr, Tempr, Tlow, Thigh, Vlow, Vhigh,
                        BaseSt, VoltSt, CurrSt, TempSt, Coulomb, ts)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    db_rows,
                )
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                self._log(f"EBox DB write error: {e}")

        bat_cur = total_current_ma / 1000.0
        min_soc = min(soc_values) if soc_values else -1.0
        return bat_cur, min_soc


# ═══════════════════════════════════════════════════════════════════════════
# RELAY CONTROLLER
# ═══════════════════════════════════════════════════════════════════════════

class RelayController:
    """Steuert das Relais via ebyte_ctrl-Skript und verwaltet die State-Datei."""

    def __init__(self, cfg: Config, log: Logger) -> None:
        self._cfg = cfg
        self._log = log

    def _build_cmd(self, args: str) -> str:
        script = self._cfg.path_ebyte_script
        prefix = "python3 " if script.endswith(".py") else ""
        return f"{prefix}{script} {args}"

    def db_log_decisions(self, state_from: int, state_to: int, trace: str,
                          pcc_w: int, bat1_w: int, soc: float, excess_w: int) -> None:
        """Schreibt jeden Trace-Token als eigene Zeile in pv_decision_log."""
        rows = []
        for token in trace.split(" | "):
            token = token.strip()
            if not token:
                continue
            if "(" in token:
                decision = token[:token.index("(")].strip()
                detail   = token[token.index("(")+1:token.rindex(")")] if ")" in token else ""
            else:
                decision, detail = token, ""
            rows.append((state_from, state_to, decision[:64], detail[:255],
                         pcc_w, bat1_w, soc, excess_w))
        if not rows:
            return
        try:
            conn = pymysql.connect(
                host="192.168.178.218", database="wagodb",
                user="gh", password="a12345", connect_timeout=5,
            )
            cur = conn.cursor()
            cur.executemany(
                "INSERT INTO pv_decision_log"
                " (ts, state_from, state_to, decision, detail, pcc_w, bat1_w, soc, excess_w)"
                " VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s)",
                rows,
            )
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            self._log(f"Decision DB log error: {e}")

    def _db_log(self, action: str, relay: str, duration_s: int | None,
                state_new: int | None, reason: str) -> None:
        try:
            conn = pymysql.connect(
                host="192.168.178.218",
                database="wagodb",
                user="gh",
                password="a12345",
                connect_timeout=5,
            )
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO pv_relay_events (ts, action, relay, duration_s, state_new, reason) "
                "VALUES (NOW(), %s, %s, %s, %s, %s)",
                (action, relay, duration_s, state_new, reason),
            )
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            self._log(f"Relay DB log error: {e}")

    def set_state(self, state: int, reason: str = "") -> bool:
        cmd = self._build_cmd(str(state))
        try:
            ret = subprocess.run(
                cmd, shell=True, timeout=10, capture_output=True, text=True
            )
            if ret.returncode == 0:
                self._write_state_file(state)
                self._log(f"Relay OK: → {state} | {ret.stdout.strip()}")
                self._db_log("set_state", "R1-R3", None, state, reason)
                return True
            else:
                self._log(f"Relay ERROR State {state}: [{ret.stdout.strip()}] – State file NOT updated")
                return False
        except subprocess.TimeoutExpired:
            self._log(f"Relay timeout: state {state}")
            return False

    def pulse_r4(self, duration: int = 3, reason: str = "") -> None:
        """Nicht-blockierender Relais-4-Puls (fork-ähnlich via Popen)."""
        script = self._cfg.path_ebyte_script
        cmd = ["python3", script, "r4", "pulse", str(duration)]
        try:
            proc = subprocess.Popen(cmd)
            self._log(f"Relay4 Puls ausgelöst (PID={proc.pid}, duration={duration}s)")
            self._db_log("pulse", "R4", duration, None, reason)
        except OSError as e:
            self._log(f"Relay4 fork fehlgeschlagen: {e}")

    def read_state(self) -> int:
        return int(_read_file(self._cfg.path_relay_state, 0))

    def _write_state_file(self, state: int) -> None:
        _write_file(self._cfg.path_relay_state, str(state))

    def keep_state(self, state: int) -> None:
        """Kein Relay-Wechsel, aber State-Datei aktuell halten."""
        self._write_state_file(state)


# ═══════════════════════════════════════════════════════════════════════════
# STATE POWER HILFSFUNKTIONEN
# ═══════════════════════════════════════════════════════════════════════════

def get_state_power(state: int) -> int:
    return STATE_POWER.get(state, 0)


def find_best_state(budget: int) -> int:
    best_state, best_power = 0, 0
    for s, p in STATE_POWER.items():
        if p <= budget and p > best_power:
            best_state, best_power = s, p
    return best_state


def find_min_covering_state(needed: int) -> int:
    """Kleinster State dessen Leistung >= needed (für FeedInLimiter: mindestens absorbieren)."""
    candidates = [(s, p) for s, p in STATE_POWER.items() if s > 0 and p >= needed]
    return min(candidates, key=lambda x: x[1])[0] if candidates else 7


def get_next_state_up(current_state: int) -> int:
    current_power = get_state_power(current_state)
    next_state, next_power = current_state, current_power
    for s, p in STATE_POWER.items():
        if p > current_power and (p < next_power or next_power == current_power):
            next_state, next_power = s, p
    return next_state


# ═══════════════════════════════════════════════════════════════════════════
# BLOCKING RULES – Strategy-Pattern
# ═══════════════════════════════════════════════════════════════════════════

class BlockingRule(ABC):
    """Abstrakte Basisklasse für alle Sperr-Regeln."""

    direction: str = "UP"   # "UP", "DOWN", "BOTH"

    @abstractmethod
    def is_blocked(self, pcc: float, bat1: float, stable: int,
                   drop_rate: float, pwr_diff: int) -> tuple[bool, str]:
        """Gibt (gesperrt, Begründung) zurück."""


class SweetSpotHold(BlockingRule):
    direction = "UP"

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg

    def is_blocked(self, pcc, bat1, stable, drop_rate, pwr_diff):
        if abs(pcc) < self._cfg.sweet_spot_pcc and bat1 > self._cfg.sweet_spot_bat:
            return True, f"SWEET_SPOT_HOLD (PCC={pcc:.0f}W, Bat1={bat1:.0f}W)"
        return False, ""


class TrendBlock(BlockingRule):
    direction = "UP"

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg

    def is_blocked(self, pcc, bat1, stable, drop_rate, pwr_diff):
        if drop_rate < self._cfg.max_drop_rate and drop_rate != 0:
            return True, f"TREND_BLOCK (Drop: {drop_rate:.1f}W/s)"
        return False, ""


class BatGuardBlock(BlockingRule):
    direction = "UP"

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg

    def is_blocked(self, pcc, bat1, stable, drop_rate, pwr_diff):
        if bat1 < self._cfg.bat_discharge_threshold:
            return True, f"BAT_GUARD_BLOCK (Bat1={bat1:.0f}W)"
        return False, ""


class StabilizingBlock(BlockingRule):
    direction = "DOWN"

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg

    def is_blocked(self, pcc, bat1, stable, drop_rate, pwr_diff):
        if stable < self._cfg.stabilization_cycles:
            return True, f"STABILIZING (stable={stable})"
        return False, ""


class HysteresisBlock(BlockingRule):
    direction = "DOWN"

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg

    def is_blocked(self, pcc, bat1, stable, drop_rate, pwr_diff):
        if pwr_diff < self._cfg.hysteresis:
            return True, f"HYSTERESIS ({pwr_diff}W)"
        return False, ""


# ═══════════════════════════════════════════════════════════════════════════
# WEATHER FORECAST (Wolkenbedeckungs-Prognose von heissa.de)
# ═══════════════════════════════════════════════════════════════════════════

class WeatherForecast:
    """Holt Forecast-JSON von web1.heissa.de und prüft Solar-Erwartung."""

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg
        self._cache: Optional[list] = None
        self._cache_ts: float = 0.0

    def _fetch(self) -> list:
        now = time.monotonic()
        if self._cache is not None and (now - self._cache_ts) < 1800:
            return self._cache
        try:
            url = (f"{self._cfg.feedin_forecast_url}"
                   f"?hours={self._cfg.feedin_forecast_hours}")
            with urllib.request.urlopen(url, timeout=5) as resp:
                data = json.loads(resp.read().decode())
            rows = data.get("rows", [])
            self._cache = rows
            self._cache_ts = now
        except Exception:
            self._cache = self._cache if self._cache is not None else []
        return self._cache

    def solar_expected(self) -> tuple[bool, str]:
        """True wenn mittlere Wolkenbedeckung < feedin_forecast_bad_clouds."""
        rows = self._fetch()
        if not rows:
            return True, "forecast_n/a→ok"
        avg_clouds = sum(r.get("cloudiness", 0) for r in rows) / len(rows)
        threshold = self._cfg.feedin_forecast_bad_clouds
        ok = avg_clouds < threshold
        cmp = "<" if ok else ">="
        return ok, f"forecast_clouds={avg_clouds:.0f}%{cmp}{threshold:.0f}%"

    def avg_cloudiness(self) -> float:
        """Mittlere Wolkenbedeckung % für das Forecast-Fenster (0 wenn n/a)."""
        rows = self._fetch()
        if not rows:
            return 0.0
        return sum(r.get("cloudiness", 0) for r in rows) / len(rows)


class DcForecast:
    """
    Klarhimmel-DC-Prognose (getdc-Modell, inline).
    Gibt erwartete DC-Watt für jetzt zurück (Meinel-Atmosphäre, 2-Array-Orientierung).
    """
    _LAT, _LON = 47.6811, 11.5732
    _TZ_NAME   = "Europe/Berlin"
    _ARRAYS    = [(25, 80, 27_854), (60, -5, 11_138)]
    _KT        = {1: 0.331, 2: 0.402, 3: 0.563, 4: 0.838,
                  5: 0.909, 6: 0.880, 7: 0.840, 8: 0.820,
                  9: 0.760, 10: 0.600, 11: 0.350, 12: 0.134}

    def __init__(self) -> None:
        self._loc = _LocationInfo("Lenggries", "Germany", self._TZ_NAME,
                                   self._LAT, self._LON)
        self._tz  = _zoneinfo.ZoneInfo(self._TZ_NAME)
        self._noon_cache: tuple[_dt.date, float] = (_dt.date.min, 0.0)

    def _cos_aoi(self, elev_deg: float, az_sun_N: float,
                 tilt_deg: float, az_panel_S: float) -> float:
        e  = math.radians(elev_deg)
        b  = math.radians(tilt_deg)
        da = math.radians((az_sun_N - 180.0) - az_panel_S)
        return math.sin(e) * math.cos(b) + math.cos(e) * math.sin(b) * math.cos(da)

    def at_noon(self) -> float:
        """DC-Watt Prognose für solaren Mittag heute (tagesweise gecacht)."""
        today = _dt.date.today()
        if self._noon_cache[0] != today:
            self._noon_cache = (today, self.at(_solar_noon(self._LAT, self._LON)))
        return self._noon_cache[1]

    def at(self, t: Optional[_dt.datetime] = None) -> float:
        """DC-Watt Prognose für Zeitpunkt t (default: jetzt, exakte Minute)."""
        if t is None:
            t = _dt.datetime.now(self._tz).replace(second=0, microsecond=0)
        t_tz = t.replace(tzinfo=self._tz) if t.tzinfo is None else t
        elev = _astral_elevation(self._loc.observer, t_tz)
        if elev <= 0:
            return 0.0
        az_N = _astral_azimuth(self._loc.observer, t_tz)
        am   = min(1.0 / math.sin(math.radians(elev)), 37.0)
        T    = 0.7 ** (am ** 0.678)
        kt   = self._KT.get(t.month, 0.60)
        total = 0.0
        for tilt, az_s, ppeak in self._ARRAYS:
            coi = max(0.0, self._cos_aoi(elev, az_N, tilt, az_s))
            total += ppeak * T * coi
        return total * kt


# ═══════════════════════════════════════════════════════════════════════════
# FEED-IN LIMITER (Sommer-Einspeisebegrenzung)
# ═══════════════════════════════════════════════════════════════════════════

class FeedInLimiter:
    """
    Sommer-Regelung: lädt die EBox nur um Grid-Einspeisung < feedin_limit_w zu halten.

    Drei Fälle:
      ON      — feed_in >= limit   → Proportionalregelung: Stufe direkt aus Überschuss
      PRELOAD — feed_in <  limit,  Prognose(60s) >= preload → State 1 vorschalten
      OFF     — sonst              → State 0
    """

    def __init__(self, cfg: Config, dc_forecast: "DcForecast") -> None:
        self._cfg = cfg
        self._forecast = WeatherForecast(cfg)
        self._dc_forecast = dc_forecast

    def _is_active_season(self) -> bool:
        return self._dc_forecast.at_noon() > self._cfg.feedin_dc_season_threshold_w

    def _feedin_forecast_clouds(self) -> float:
        """Mittlere Wolkenbedeckung % aus WeatherForecast (0 wenn n/a)."""
        return self._forecast.avg_cloudiness()

    def apply(self, vals: MeasuredValues, decision: ControlDecision) -> None:
        if not self._is_active_season() or vals.prot:
            return
        # Kein Peak-Risiko wenn wolken-korrigiertes Tagesmaximum unter Einspeiselimit:
        #   dc_noon × (1 − clouds/100) < feedin_limit_w
        # Deckt ab: Winter/Übergang (dc_noon < 19 kW), Sommer Schlechtwetter (Wolken ↑)
        _dc_noon_raw = self._dc_forecast.at_noon()
        _clouds      = self._feedin_forecast_clouds()
        _noon_eff    = _dc_noon_raw * max(0.0, 1.0 - _clouds / 100.0)
        if _noon_eff < self._cfg.feedin_limit_w:
            decision.trace += (f" | FeedInLimiter SKIP"
                               f" (noon={_dc_noon_raw:.0f}W"
                               f"×{1-_clouds/100:.2f}clouds={_noon_eff:.0f}W"
                               f"<{self._cfg.feedin_limit_w}W)")
            return
        # Batterie fast voll während Hochleistungsphase → Ladung deckeln (sanfter Übergang zu 100%)
        if vals.soc >= 88.0 and vals.dc_expected > self._cfg.feedin_dc_cap_safe_w:
            cap = 1
            if decision.final_state > cap:
                old = decision.final_state
                decision.final_state = cap
                decision.changed = (cap != vals.relay_st)
                decision.trace += (f" | FeedInLimiter SOC_BRAKE"
                                   f" (SOC={vals.soc:.0f}%≥88%"
                                   f", dc={vals.dc_expected:.0f}W>{self._cfg.feedin_dc_cap_safe_w}W)"
                                   f" State{old}→{cap}")
            else:
                decision.trace += (f" | FeedInLimiter SOC_BRAKE"
                                   f" (SOC={vals.soc:.0f}%≥88%"
                                   f", dc={vals.dc_expected:.0f}W, State{decision.final_state}≤{cap})")
            return
        cfg = self._cfg
        # PCC = direkte Messung der Netzeinspeisung am Wechselrichter (bevorzugt).
        # Z2 als Fallback wenn PCC=NaN (Mode-Switch). Z2 enthält WP bereits → kein Abzug.
        if vals.pcc > 500:
            feed_in_w = int(vals.pcc)
            feed_in_src = "PCC"
        else:
            feed_in_w = int(abs(min(0.0, vals.wirkleist)))
            feed_in_src = "Z2"
        prognose_w = min(
            feed_in_w + int(decision.drop_rate * 60) if decision.has_drop_rate else feed_in_w,
            cfg.feedin_max_prognose_w,
        )

        fc_ok, fc_reason = self._forecast.solar_expected()

        if feed_in_w >= cfg.feedin_limit_w:
            absorption_needed = feed_in_w - cfg.feedin_limit_w
            target = find_min_covering_state(absorption_needed)
            old_state = decision.final_state
            decision.final_state = target
            decision.changed = (target != vals.relay_st)
            decision.trace += (f" | FeedInLimiter ON [{feed_in_src}] ({feed_in_w}W >= {cfg.feedin_limit_w}W"
                               f", absorb={absorption_needed}W) State{old_state}→{target}")

        elif prognose_w >= cfg.feedin_preload_w:
            if fc_ok:
                old_state = decision.final_state
                decision.final_state = 1
                decision.changed = (1 != vals.relay_st)
                decision.trace += (f" | FeedInLimiter PRELOAD [{feed_in_src}]"
                                   f" ({feed_in_w}W + trend={decision.drop_rate:+.0f}W/s×60s"
                                   f" → prognose={prognose_w}W >= {cfg.feedin_preload_w}W"
                                   f", {fc_reason}) State{old_state}→1")
            else:
                old_state = decision.final_state
                decision.final_state = 0
                decision.changed = (0 != vals.relay_st)
                decision.trace += (f" | FeedInLimiter PRELOAD_FC_OFF [{feed_in_src}]"
                                   f" ({fc_reason}) State{old_state}→0")

        else:
            # Kein Cap-Risiko wenn:
            # A) Wolken > 60 % → CLOUD_FREE
            # B) Modell sinkt UND dc_expected < feedin_limit + max_ebox → POST_PEAK_FREE
            #    Schwellwert: 19 000 + 11 400 = 30 400 W — bei Ladung State 7 absorbiert
            #    EBox 11 400 W, daher PCC = dc − 11 400 − Haus < 19 000 W garantiert.
            avg_clouds = self._forecast.avg_cloudiness()
            _dc_now  = vals.dc_expected
            _peak_free_w = cfg.feedin_limit_w + max(STATE_POWER.values())
            _dc_30m  = self._dc_forecast.at(
                _dt.datetime.now() + _dt.timedelta(minutes=30))
            _past_peak = (_dc_now < _peak_free_w and _dc_30m <= _dc_now)

            if avg_clouds > cfg.feedin_clouds_no_peak:
                decision.trace += (f" | FeedInLimiter OFF→CLOUD_FREE [{feed_in_src}]"
                                   f" ({feed_in_w}W < {cfg.feedin_limit_w}W"
                                   f", clouds={avg_clouds:.0f}%>{cfg.feedin_clouds_no_peak:.0f}%)")
            elif _past_peak:
                decision.trace += (f" | FeedInLimiter OFF→POST_PEAK_FREE [{feed_in_src}]"
                                   f" (dc={_dc_now:.0f}W<{_peak_free_w:.0f}W"
                                   f", dc+30min={_dc_30m:.0f}W↓)")
            else:
                # Einspeisung zu niedrig → Sommer-Modus erzwingt State 0 (kein Laden)
                old_state = decision.final_state
                decision.final_state = 0
                decision.changed = (0 != vals.relay_st)
                decision.trace += (f" | FeedInLimiter OFF [{feed_in_src}]"
                                   f" ({feed_in_w}W < {cfg.feedin_limit_w}W"
                                   f", prognose={prognose_w}W, {fc_reason}) State{old_state}→0")


# ═══════════════════════════════════════════════════════════════════════════
# FB CONTROLLER (Kern-Regellogik)
# ═══════════════════════════════════════════════════════════════════════════

class FbController:
    """
    Implementiert die gesamte Entscheidungslogik aus fb_controller() + check_blocking_rules()
    in fox2db.c, jetzt mit Dependency Injection der BlockingRules.
    """

    def __init__(self, cfg: Config, blocking_rules: list[BlockingRule]) -> None:
        self._cfg = cfg
        self._rules = blocking_rules

    def decide(self, vals: MeasuredValues) -> ControlDecision:
        """Phase 1: Zustandsentscheidung ohne Blocking (kein drop_rate nötig)."""
        decision = ControlDecision()
        best, excess, trace = self._decide_state(vals)
        decision.best_state = best
        decision.excess = excess
        decision.trace = trace
        return decision

    def apply_blocking(self, vals: MeasuredValues, decision: ControlDecision) -> None:
        """Phase 2: Blocking anwenden. decision.drop_rate muss vorher gesetzt sein."""
        final, changed = self._apply_blocking(vals, decision)
        decision.final_state = final
        decision.changed = changed

    def _decide_state(self, v: MeasuredValues) -> tuple[int, float, str]:
        """Gibt (best_state, excess, trace) zurück – 1:1 mit fb_controller() in C."""
        cfg = self._cfg

        # SOC unbekannt
        if v.soc < 0:
            ebox_actual = (v.bat_cur * 2 * 53 + 1) if v.relay_st > 0 else 0.0
            pcc_eff = -v.wirkleist if math.isnan(v.pcc) else v.pcc
            excess = pcc_eff + ebox_actual + v.bat1
            return v.relay_st, excess, f"EBOX_SOC_UNKNOWN_HOLD (prot={v.prot})"

        ebox_eff = (max(v.bat_cur * 2 * 53 + 1, get_state_power(v.relay_st))
                    if v.relay_st > 0 else 0.0)
        # Z2-Fallback nur bei Lesefehler (NaN). PCC=0 ist gültiger Messwert (Sweet-Spot).
        pcc_eff = -v.wirkleist if math.isnan(v.pcc) else v.pcc
        excess = pcc_eff + ebox_eff + v.bat1

        # Emergency Charge mit Target (verhindert Ping-Pong)
        if v.prot:
            if v.soc < cfg.deep_discharge_charge_target:
                return 1, excess, (f"EMERGENCY_CHARGE_TO_{cfg.deep_discharge_charge_target}%"
                                   f" (current {v.soc:.1f}%)")
            else:
                return 0, excess, (f"CHARGE_TARGET_REACHED "
                                   f"({v.soc:.1f}% >= {cfg.deep_discharge_charge_target}%)")

        # Kritischer SOC ohne aktiven Schutz
        if v.soc < cfg.deep_discharge_lower:
            return 1, excess, (f"CRITICAL_SOC_PROTECTION_ACTIVATE "
                               f"({v.soc:.1f}% < {cfg.deep_discharge_lower}%)")

        # Batterie voll
        if v.soc > cfg.max_soc:
            return 0, excess, "BATTERY_FULL_STOP"

        # Ungenügender Überschuss
        if excess < cfg.min_excess:
            return 0, excess, f"INSUFFICIENT_EXCESS ({excess:.0f}W)"

        # Power Matching
        budget = int(excess + cfg.max_grid_draw)
        best = find_best_state(budget)
        trace = f"POWER_MATCHING (Excess: {excess:.0f}W, Budget: {budget}W)"

        # Ramp Limiting
        if best > v.relay_st:
            next_st = get_next_state_up(v.relay_st)
            if best > next_st:
                trace += f" | RAMP_LIMITED ({best}->{next_st})"
                best = next_st

        # Mittagskapp: SOC2 > Schwelle UND dc_expected im Risikofenster → max State 1
        if (v.dc_expected > cfg.feedin_dc_cap_safe_w
                and v.soc >= cfg.midday_soc_threshold
                and best > 1):
            trace += (f" | MIDDAY_SOC_CAP (SOC={v.soc:.0f}%≥{cfg.midday_soc_threshold}%"
                      f", dc={v.dc_expected:.0f}W>{cfg.feedin_dc_cap_safe_w}W)")
            best = 1

        return best, excess, trace

    def _apply_blocking(self, v: MeasuredValues,
                        dec: ControlDecision) -> tuple[int, bool]:
        """Gibt (final_state, changed) zurück. Emergency-Pfad überspringt Blocking."""
        best = dec.best_state
        relay_st = v.relay_st

        if best == relay_st:
            return relay_st, False

        pwr_diff = abs(get_state_power(best) - get_state_power(relay_st))
        direction = "UP" if get_state_power(best) > get_state_power(relay_st) else "DOWN"

        # Emergency: sofort schalten, keine Blocking-Prüfung
        if v.pcc < -self._cfg.emergency_import and direction == "DOWN":
            dec.trace += f" | EMERGENCY_FORCE (Import={v.pcc:.0f}W)"
            return best, True

        # Blocking-Regeln prüfen
        drop_rate = dec.drop_rate
        for rule in self._rules:
            if rule.direction not in (direction, "BOTH"):
                continue
            if dec.after_peak and isinstance(rule, TrendBlock):
                continue  # after solar peak: ramp freely to State 7
            blocked, reason = rule.is_blocked(v.pcc, v.bat1, v.stable, drop_rate, pwr_diff)
            if blocked:
                dec.trace += f" | {reason}"
                return relay_st, False

        return best, True


# ═══════════════════════════════════════════════════════════════════════════
# EXCESS TREND CALCULATOR
# ═══════════════════════════════════════════════════════════════════════════

class ExcessTrendCalc:
    """Berechnet Überschuss-Trend (drop_rate) zwischen Zyklen."""

    def __init__(self, path_last_excess: str) -> None:
        self._path = path_last_excess

    def write(self, excess: float) -> None:
        _write_file(self._path, f"{excess:.1f}")


# ═══════════════════════════════════════════════════════════════════════════
# DEEP DISCHARGE GUARD
# ═══════════════════════════════════════════════════════════════════════════

class DeepDischargeGuard:
    """Tiefentladeschutz mit Hysterese. Persistiert Status in Datei."""

    def __init__(self, cfg: Config, log: Logger) -> None:
        self._cfg = cfg
        self._log = log
        self._path = cfg.path_deep_discharge

    def read(self) -> int:
        return int(_read_file(self._path, 0))

    def update(self, soc: float) -> None:
        """Aktualisiert den Schutzstatus. Bei unbekanntem SOC (<0) keine Änderung."""
        cfg = self._cfg
        if soc < 0:
            self._log(f"WARNING: EBox SOC unbekannt – Tiefentladeschutz unverändert "
                      f"(prot={self.read()})")
            return

        old_prot = self.read()
        if soc < cfg.deep_discharge_lower:
            if not old_prot:
                self._log(f"DEEP_DISCHARGE_PROTECTION ACTIVATED at {soc:.1f}% "
                          f"(threshold: {cfg.deep_discharge_lower}%)")
            _write_file(self._path, "1")
        elif soc >= cfg.deep_discharge_upper:
            if old_prot:
                self._log(f"DEEP_DISCHARGE_PROTECTION DEACTIVATED at {soc:.1f}% "
                          f"(threshold: {cfg.deep_discharge_upper}%)")
            _write_file(self._path, "0")
        # Zwischen den Schwellen: unverändert (Hysterese)


# ═══════════════════════════════════════════════════════════════════════════
# OUTPUT WRITER
# ═══════════════════════════════════════════════════════════════════════════

class OutputWriter:
    """Schreibt alle Ausgabedateien und publiziert via MQTT."""

    def __init__(self, cfg: Config, publisher: MqttPublisher, log: Logger) -> None:
        self._cfg = cfg
        self._pub = publisher
        self._log = log

    def write_cycle(self, vals: MeasuredValues, dec: ControlDecision,
                    new_stable: int) -> None:
        ebox_w = vals.bat_cur * 2 * 53
        ts = time.strftime("%Y-%m-%dT%H:%M:%S")

        # MQTT JSON
        payload = {
            "ts": ts,
            "version": VERSION,
            "soc_bat2": round(vals.soc * 10) / 10,
            "soc_bat1": round(vals.soc_bat1 * 10) / 10,
            "pcc": round(vals.pcc) if math.isfinite(vals.pcc) else None,
            "bat1": round(vals.bat1),
            "ebox": round(ebox_w),
            "wirkleist": round(vals.wirkleist),
            "state": dec.final_state,
            "state_before": vals.relay_st,
            "stable": new_stable,
            "excess": round(dec.excess),
            "drop_rate": (round(dec.drop_rate * 10) / 10
                          if dec.has_drop_rate and dec.drop_rate != 0 else None),
            "trace": dec.trace,
            "deep_discharge_active": bool(vals.prot),
        }
        self._pub.publish(payload)

        # Dateien
        _write_file(self._cfg.path_last_change, str(new_stable))
        _write_file(self._cfg.path_inverter_csv,
                    f"{vals.pcc:.0f},{vals.bat1:.0f},{vals.soc:.1f},"
                    f"{vals.soc_bat1:.1f},{vals.bat_cur:.1f},{dec.final_state}\n")

        self._log(f"Data: SOC2={vals.soc:.1f}% SOC1={vals.soc_bat1:.1f}% "
                  f"PCC={vals.pcc:.0f}W Z2={vals.wirkleist:.0f}W "
                  f"Bat1={vals.bat1:.0f}W EBox={ebox_w:.0f}W "
                  f"(State={vals.relay_st}) Stable={new_stable}")
        if dec.has_drop_rate and dec.drop_rate != 0:
            self._log(f"Trend: Excess {vals.last_excess:.0f}W→{dec.excess:.0f}W "
                      f"({dec.drop_rate:+.1f}W/s)")
        self._log(f"Result: State {dec.final_state} (TRACE: {dec.trace})")


# ═══════════════════════════════════════════════════════════════════════════
# POWER CONTROLLER – Haupt-Orchestrator
# ═══════════════════════════════════════════════════════════════════════════

class PowerController:
    """
    Entspricht main_loop() in fox2db.c.
    Koordiniert alle Teilsysteme in der richtigen Reihenfolge.
    """

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg
        log = Logger(cfg.path_log)
        self._log = log

        # Aufrufer protokollieren
        ppid = os.getppid()
        try:
            parent_cmd = Path(f"/proc/{ppid}/cmdline").read_text().replace('\x00', ' ').strip()
        except OSError:
            parent_cmd = "unknown"
        log(f"START: {' '.join(sys.argv)} | caller(pid={ppid}): {parent_cmd}")

        self._mqtt = MqttClient(cfg, log)
        self._pub = MqttPublisher(cfg, log)
        self._ebox = EBoxReader(cfg, log)
        self._relay = RelayController(cfg, log)
        self._dd_guard = DeepDischargeGuard(cfg, log)
        self._trend = ExcessTrendCalc(cfg.path_last_excess)
        self._writer = OutputWriter(cfg, self._pub, log)

        blocking_rules: list[BlockingRule] = [
            SweetSpotHold(cfg),
            TrendBlock(cfg),
            BatGuardBlock(cfg),
            StabilizingBlock(cfg),
            HysteresisBlock(cfg),
        ]
        self._fb = FbController(cfg, blocking_rules)
        self._dc_forecast = DcForecast()
        self._feedin = FeedInLimiter(cfg, self._dc_forecast)

    def run_cycle(self) -> None:
        t0 = time.monotonic()
        self._log("--- Start Cycle ---")

        # ── INPUT LAYER ───────────────────────────────────────────────────
        self._ebox.update()
        t_ebox_upd = time.monotonic()

        mqtt_data = self._mqtt.fetch()
        t_mqtt = time.monotonic()
        if mqtt_data is None:
            self._log("EMERGENCY SHUTDOWN: MQTT failed")
            self._relay.set_state(0, reason="EMERGENCY: MQTT failed")
            return

        # Zähler-Wirkleistung parallel zum EBox-Read holen
        wirkleist = self._mqtt.fetch_wirkleist()
        wp_power = self._mqtt.fetch_wp_power()
        t_zaehl = time.monotonic()

        bat_cur, soc = self._ebox.read()
        t_ebox_read = time.monotonic()
        self._ebox.collect_cells_if_needed(soc)
        t_cells = time.monotonic()
        prot = self._dd_guard.read()

        vals = MeasuredValues(
            pcc=mqtt_data["pcc"],
            bat1=mqtt_data["bat1"],
            soc_bat1=mqtt_data["soc_bat1"],
            dc_pv=mqtt_data.get("dc_pv", 0.0),
            bat_cur=bat_cur,
            soc=soc,
            relay_st=self._relay.read_state(),
            stable=int(_read_file(self._cfg.path_last_change, 0)),
            prot=prot,
            last_excess=_read_file(self._cfg.path_last_excess, 0.0),
            wirkleist=wirkleist,
            wp_power=wp_power,
            dc_expected=self._dc_forecast.at(),
        )


        # ── LOGIC LAYER ───────────────────────────────────────────────────
        # Reihenfolge 1:1 wie C: decide → drop_rate berechnen → blocking

        # Phase 1: Zustandsentscheidung (excess wird dabei berechnet, drop_rate nicht nötig)
        decision = self._fb.decide(vals)

        # Phase 2: Trend mit jetzt bekanntem excess berechnen und persistieren
        has_drop_rate = vals.last_excess > 0
        decision.drop_rate = (decision.excess - vals.last_excess) / 30.0 if has_drop_rate else 0.0
        decision.has_drop_rate = has_drop_rate
        self._trend.write(decision.excess)

        # Phase 3: Blocking einmalig mit korrektem drop_rate (kein Doppellauf)
        # after_peak: TREND_BLOCK nur überspringen wenn Batterie noch nicht voll
        decision.after_peak = vals.dc_expected > self._cfg.feedin_dc_cap_safe_w and vals.soc < 88.0
        self._fb.apply_blocking(vals, decision)

        # ── FEED-IN LIMITER ────────────────────────────────────────────────
        self._feedin.apply(vals, decision)

        # ══ SAISON-GATE ══════════════════════════════════════════════════════════
        # Aktiver Tag (noon > 12 kW):
        #   Wolken > 60 % → kein Cap-Risiko → CLOUD_FREE: sofort best state
        #   Nach Peak (dc_expected < cap_safe UND >= 60% Noon) → POST_PEAK_FREE
        #   Sonst Vor/während Cap-Risiko → EBox wartet auf PCC > 19 kW
        # Winter/Wolken (noon < 12 kW): POWER_MATCHING lädt sofort aus Überschuss.
        if self._feedin._is_active_season() and vals.pcc < self._cfg.feedin_limit_w:
            _dc_noon    = self._dc_forecast.at_noon()
            _avg_clouds = self._feedin._forecast.avg_cloudiness()
            _cloud_free = _avg_clouds > self._cfg.feedin_clouds_no_peak
            _past_peak  = (vals.dc_expected < self._cfg.feedin_dc_cap_safe_w
                           and vals.dc_expected >= _dc_noon * 0.6)
            if _cloud_free:
                decision.trace += (f" | CLOUD_FREE"
                                   f" (clouds={_avg_clouds:.0f}%"
                                   f">{self._cfg.feedin_clouds_no_peak:.0f}%)")
            elif _past_peak:
                decision.trace += (f" | POST_PEAK_FREE"
                                   f" (dc={vals.dc_expected:.0f}W"
                                   f"<{self._cfg.feedin_dc_cap_safe_w}W"
                                   f", ≥60%noon={_dc_noon*0.6:.0f}W)")
            elif decision.final_state > 0:
                decision.final_state = 0
                decision.changed = (vals.relay_st != 0)
                decision.trace += (f" | SEASON_GATE→0"
                                   f" (PCC={vals.pcc:.0f}W<{self._cfg.feedin_limit_w}W"
                                   f", dc={vals.dc_expected:.0f}W"
                                   f", noon={_dc_noon:.0f}W)")

        # ══ RELAIS-4 / LADESTUFE-ERHOEHUNG ════════════════════
        # Trigger: PCC > 20.6 kW  oder  (wirkleist + WP) < Schwelle
        _r4_pcc = (not math.isnan(vals.pcc)) and vals.pcc > 20_600
        _r4_wl  = (vals.wirkleist + vals.wp_power) < self._cfg.wirkleist_r4_threshold
        if _r4_pcc or _r4_wl:
            _src = (f"PCC={vals.pcc:.0f}W" if _r4_pcc
                    else f"wirkleist_korr={(vals.wirkleist+vals.wp_power):.0f}W")
            if vals.soc < 100 and vals.relay_st < 7:
                # Batterie noch nicht voll: eine Stufe hoch statt DO4
                _next = get_next_state_up(vals.relay_st)
                if decision.final_state < _next:
                    _old_st = decision.final_state
                    decision.final_state = _next
                    decision.changed = (_next != vals.relay_st)
                    decision.trace += f" | DO4→STUFE{_next} ({_src}, SOC={vals.soc:.0f}%<100) State{_old_st}→{_next}"
                else:
                    decision.trace += f" | DO4→STUFE{_next} bereits abgedeckt durch State{decision.final_state} ({_src})"
            else:
                # SOC=100 oder State 7 aktiv → DO4 ausloesen
                if _r4_pcc and vals.dc_pv > 10.8:
                    self._relay.pulse_r4(60, reason=f"PCC={vals.pcc:.0f}W DC-PV={vals.dc_pv:.2f}kW")
                elif _r4_pcc:
                    self._relay.pulse_r4(reason=f"PCC={vals.pcc:.0f}W>20kW DC-PV={vals.dc_pv:.2f}kW<=10.8kW")
                    self._log(f"Relay4 3s: PCC={vals.pcc:.0f}W>20kW, DC-PV={vals.dc_pv:.2f}kW")
                else:
                    korr = vals.wirkleist + vals.wp_power
                    self._relay.pulse_r4(reason=f"wirkleist_korr={korr:.0f}W<{self._cfg.wirkleist_r4_threshold}W")
                    self._log(f"Relay4 3s: wirkleist={vals.wirkleist:.0f}W+WP={vals.wp_power:.0f}W={korr:.0f}W")


        # ── LADEZEIT-PROGNOSE ─────────────────────────────────────────────
        if vals.soc >= 0 and vals.soc < 100:
            _chg_w = get_state_power(decision.final_state)
            if _chg_w > 0:
                _remain_wh = (100.0 - vals.soc) / 100.0 * self._cfg.bat2_capacity_wh
                _h_to_full = _remain_wh / _chg_w
                _full_at   = _dt.datetime.now() + _dt.timedelta(hours=_h_to_full)
                self._log(
                    f"EBox Prognose: voll ~{_full_at.strftime('%H:%M')} Uhr"
                    f" ({_h_to_full*60:.0f} min"
                    f", {_remain_wh:.0f} Wh"
                    f" @ {_chg_w} W"
                    f", SOC={vals.soc:.0f}%)"
                )
            else:
                self._log(f"EBox Prognose: kein Laden (State=0, SOC={vals.soc:.0f}%)")

        # ── OUTPUT LAYER ──────────────────────────────────────────────────
        new_stable = 0 if decision.changed else vals.stable + 1
        self._writer.write_cycle(vals, decision, new_stable)

        # Solar Noon loggen
        try:
            _noon = _solar_noon(self._cfg.midday_latitude, self._cfg.midday_longitude)
            _now_tz = _dt.datetime.now(_noon.tzinfo)
            _diff_min = (_noon - _now_tz).total_seconds() / 60
            self._log(
                f"SolarNoon: {_noon.strftime('%H:%M')} CEST ({_diff_min:+.0f} min)"
                f" dc_expected={vals.dc_expected:.0f}W"
            )
        except Exception as e:
            self._log(f"SolarNoon error: {e}")

        # Tiefentladeschutz aktualisieren
        self._dd_guard.update(vals.soc)

        # Relais schalten oder halten
        if decision.changed:
            self._relay.set_state(decision.final_state, reason=decision.trace)
        else:
            self._relay.keep_state(decision.final_state)

        self._relay.db_log_decisions(
            vals.relay_st, decision.final_state, decision.trace,
            pcc_w=int(vals.pcc), bat1_w=int(vals.bat1),
            soc=vals.soc, excess_w=int(decision.excess),
        )

        t_end = time.monotonic()
        cells_s = f" cells={t_cells - t_ebox_read:.1f}s" if (t_cells - t_ebox_read) > 0.1 else ""
        self._log(
            f"Timing: ebox_pwr={t_ebox_upd-t0:.1f}s"
            f" mqtt={t_mqtt-t_ebox_upd:.1f}s"
            f" zaehl={t_zaehl-t_mqtt:.1f}s"
            f" ebox_read={t_ebox_read-t_zaehl:.1f}s"
            f"{cells_s}"
            f" total={t_end-t0:.1f}s"
        )


# ═══════════════════════════════════════════════════════════════════════════
# DATEI-HILFSFUNKTIONEN (Modul-Level, kein State)
# ═══════════════════════════════════════════════════════════════════════════

def _read_file(path: str, default):
    try:
        return type(default)(Path(path).read_text().strip())
    except (OSError, ValueError):
        return default


def _write_file(path: str, content: str) -> None:
    try:
        Path(path).write_text(content)
    except OSError as e:
        print(f"Write error {path}: {e}", file=sys.stderr)


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    cfg = Config.from_args()
    controller = PowerController(cfg)
    controller.run_cycle()


if __name__ == "__main__":
    main()
