#!/usr/bin/env python3
"""
fox2db.py  –  SOYO Inverter Power Management Controller
OO-Port von fox2db.c (v1.56-C) nach Python

Klassen:
    Config               – Konfiguration & CLI-Parsing
    Logger               – Datei-Logging mit Auto-Truncate
    MeasuredValues       – Datentransfer-Objekt für Messwerte
    ControlDecision      – Datentransfer-Objekt für Regelentscheid
    MqttClient           – MQTT Empfang (paho-mqtt)
    MqttPublisher        – MQTT Ausgabe
    EBoxReader           – EBox-Batterie-Daten lesen
    RelayController      – Relais-Steuerung via ebyte_ctrl
    BlockingRule         – Abstrakte Basisklasse für Sperr-Regeln
    SweetSpotHold / TrendBlock / BatGuardBlock /
    StabilizingBlock / HysteresisBlock  – konkrete Regeln
    FbController         – Kern-Regellogik (fb_controller)
    ExcessTrendCalc      – Überschuss-Trend-Berechnung
    DeepDischargeGuard   – Tiefentladeschutz mit Hysterese
    OutputWriter         – Datei- und CSV-Ausgabe
    PowerController      – Haupt-Orchestrator (main_loop)

Build/Run:
    pip install paho-mqtt
    python3 fox2dbOO.py [options]
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import subprocess
import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import datetime as _dt
import zoneinfo as _zoneinfo
from astral import LocationInfo as _LocationInfo
from astral.sun import elevation as _astral_elevation
from astral.sun import azimuth as _astral_azimuth

import paho.mqtt.client as mqtt
import pymysql

# ═══════════════════════════════════════════════════════════════════════════
# VERSION & KONSTANTEN
# ═══════════════════════════════════════════════════════════════════════════

VERSION = "v1.64-Py"
MAX_LOG_BYTES = 220 * 1024  # 220 kB, dann truncate

# Hardware-Zustandstabelle: state → Watt
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



def _solar_noon(lat: float, lon: float) -> _dt.datetime:
    from astral.sun import sun as _astral_sun
    loc = _LocationInfo("Standort", "Germany", "Europe/Berlin", lat, lon)
    s = _astral_sun(loc.observer, date=_dt.date.today(), tzinfo=loc.timezone)
    return s["noon"]


# ═══════════════════════════════════════════════════════════════════════════
# DC FORECAST — Klarhimmel-Modell WR1 + WR2
# ═══════════════════════════════════════════════════════════════════════════

class DcForecast:
    """Klarhimmel-DC-Prognose (Meinel-Atmosphäre, monatlicher KT-Faktor)."""

    _LAT, _LON = 47.6811, 11.5732
    _TZ_NAME   = "Europe/Berlin"
    _ARRAYS = [
        (25,  80, 27_854),
        (60,  -5, 11_138),
    ]
    _ARRAYS_EAST = [
        (41, -74, 19_852),
        (60,  90,  2_078),
        (32,  94,  2_378),
    ]
    _KT = {1: 0.331, 2: 0.402, 3: 0.563, 4: 0.838,
           5: 0.909, 6: 0.880, 7: 0.840, 8: 0.820,
           9: 0.760, 10: 0.600, 11: 0.350, 12: 0.134}

    def __init__(self) -> None:
        self._loc = _LocationInfo("Lenggries", "Germany", self._TZ_NAME,
                                   self._LAT, self._LON)
        self._tz  = _zoneinfo.ZoneInfo(self._TZ_NAME)

    def _cos_aoi(self, elev_deg: float, az_sun_N: float,
                 tilt_deg: float, az_panel_S: float) -> float:
        e  = math.radians(elev_deg)
        b  = math.radians(tilt_deg)
        da = math.radians((az_sun_N - 180.0) - az_panel_S)
        return math.sin(e) * math.cos(b) + math.cos(e) * math.sin(b) * math.cos(da)

    def at(self, t: '_dt.datetime | None' = None) -> float:
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

    def at_east(self, t: '_dt.datetime | None' = None) -> float:
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
        for tilt, az_s, ppeak in self._ARRAYS_EAST:
            coi = max(0.0, self._cos_aoi(elev, az_N, tilt, az_s))
            total += ppeak * T * coi
        return total * kt

    def peak_forecast_today(self, threshold_w: float = 20_000) -> tuple:
        """Gibt (peak_w, peak_time, expected) zurück — stündliche Auflösung."""
        today = _dt.date.today()
        best_w, best_t, window_end = 0.0, None, None
        for hour in range(5, 21):
            t = _dt.datetime(today.year, today.month, today.day, hour, 0,
                             tzinfo=self._tz)
            w = self.at(t) + self.at_east(t)
            if w > best_w:
                best_w, best_t = w, t
            if w > threshold_w:
                window_end = t
        return best_w, best_t, best_w > threshold_w, window_end


# ═══════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class Config:
    """Alle Konfigurations-Parameter. Defaults entsprechen fox2db.c."""

    # Regelwerk
    min_excess: int = 1010
    max_grid_draw: int = 1500
    max_soc: int = 99
    hysteresis: int = 505
    stabilization_cycles: int = 2
    emergency_import: int = 1020
    bat_discharge_threshold: int = -220
    sweet_spot_pcc: int = 160
    sweet_spot_bat: int = -310
    max_drop_rate: int = -20
    deep_discharge_lower: int = 6
    deep_discharge_upper: int = 8
    deep_discharge_charge_target: int = 7
    # MQTT Empfang
    mqtt_broker: str = "kellertreppe.fritz.box"
    mqtt_port: int = 1883
    mqtt_topic: str = "inverter/power_grid_exchange/json"
    mqtt_zaehl_topic: str = "pv_zaehl2/#"
    mqtt_timeout: int = 43

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
    path_ebox_script: str = "/home/pi/python/ebox1arg.py"
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
        p.add_argument("--min-excess", type=int, default=1010)
        p.add_argument("--max-grid-draw", type=int, default=1500)
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
        p.add_argument("--ebox-script", default="/home/pi/python/ebox1arg.py")
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
            mqtt_broker=a.mqtt_broker,
            mqtt_port=a.mqtt_port,
            mqtt_topic=a.mqtt_topic,
            mqtt_timeout=a.mqtt_timeout,
            mqtt_publish_broker=a.mqtt_publish_broker,
            mqtt_publish_port=a.mqtt_publish_port,
            mqtt_publish_topic=a.mqtt_publish_topic,
            mqtt_publish_retain=a.mqtt_publish_retain,
            mqtt_publish_qos=a.mqtt_publish_qos,
            path_ebyte_script=a.ebyte_script,
            path_ebox_script=a.ebox_script,
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
    dc_expected: float = 0.0
    wirkleist: float = 0.0     # Z2-Zähler: negativ = Export, positiv = Bezug


@dataclass
class ControlDecision:
    """Ergebnis der Regellogik."""
    best_state: int = 0
    final_state: int = 0
    changed: bool = False
    force: bool = False   # True → Blocking komplett überspringen
    excess: float = 0.0
    drop_rate: float = 0.0
    has_drop_rate: bool = False
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
        pcc = raw.get("ActivePower_PCC_Total", 0.0) * 1000.0   # kW → W
        bat1 = raw.get("Power_Bat1", 0.0) * 1000.0
        soc_bat1 = raw.get("SOC_Bat1", 0.0)
        self._log(f"MQTT Received: PCC={pcc:.0f}W, Bat1={bat1:.0f}W, SOC_Bat1={soc_bat1:.1f}%")
        return {"pcc": pcc, "bat1": bat1, "soc_bat1": soc_bat1}

    def fetch_z2(self) -> float:
        """Liest einmalig wirkleist aus pv_zaehl2/# (retained). 0.0 bei Fehler."""
        received: list[float] = []

        def on_message(_client, _userdata, message):
            try:
                data = json.loads(message.payload.decode())
                received.append(float(data.get("wirkleist", 0.0)))
            except Exception:
                pass

        client = mqtt.Client(client_id="fox2db_z2", clean_session=True)
        client.on_message = on_message
        try:
            client.connect(self._cfg.mqtt_broker, self._cfg.mqtt_port, keepalive=10)
        except Exception:
            return 0.0
        client.subscribe(self._cfg.mqtt_zaehl_topic, qos=0)
        client.loop_start()
        deadline = time.monotonic() + 3.0
        while not received and time.monotonic() < deadline:
            time.sleep(0.05)
        client.loop_stop()
        client.disconnect()
        return received[0] if received else 0.0


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

    def read(self) -> tuple[float, float]:
        """
        Liest ebox_data-Datei und gibt (bat_cur_A, min_soc) zurück.
        min_soc = -1.0 wenn unbekannt.
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
                    if len(parts) < 13 or parts[8] == "Absent":
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
                            float(parts[0]), float(parts[1]), float(parts[2]),
                            float(parts[3]), float(parts[4]), float(parts[5]),
                            float(parts[6]), float(parts[7]), parts[8], parts[9],
                            parts[10], parts[11],
                            float(parts[12].replace("%", "")), _dt.datetime.now(),
                        ))
                    except (ValueError, IndexError):
                        pass
        except OSError as e:
            self._log(f"EBox read error: {e}")
            return 0.0, -1.0

        if db_rows:
            try:
                conn = pymysql.connect(host="192.168.178.218", database="wagodb",
                                       user="gh", password="a12345", connect_timeout=3)
                cur = conn.cursor()
                cur.executemany(
                    "INSERT INTO pv_ebox2 "
                    "(Power,Volt,Curr,Tempr,Tlow,Thigh,Vlow,Vhigh,"
                    "BaseSt,VoltSt,CurrSt,TempSt,Coulomb,ts) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", db_rows)
                conn.commit()
                cur.close(); conn.close()
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

    def set_state(self, state: int) -> bool:
        cmd = self._build_cmd(str(state))
        try:
            ret = subprocess.run(
                cmd, shell=True, timeout=10, capture_output=True, text=True
            )
            if ret.returncode == 0:
                self._write_state_file(state)
                self._log(f"Relay OK: → {state} | {ret.stdout.strip()}")
                return True
            else:
                self._log(f"Relay ERROR State {state}: [{ret.stdout.strip()}] – State file NOT updated")
                return False
        except subprocess.TimeoutExpired:
            self._log(f"Relay timeout: state {state}")
            return False

    def pulse_r4(self, duration: int = 3) -> None:
        """Nicht-blockierender Relais-4-Puls (fork-ähnlich via Popen)."""
        script = self._cfg.path_ebyte_script
        cmd = ["python3", script, "r4", "pulse", str(duration)]
        try:
            proc = subprocess.Popen(cmd)
            self._log(f"Relay4 Puls ausgelöst (PCC>20kW, PID={proc.pid})")
            _db_relay_event(self._log, "pulse", "do4", 1, f"PCC>20kW PID={proc.pid}", duration)
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
# FB CONTROLLER (Kern-Regellogik)
# ═══════════════════════════════════════════════════════════════════════════

class FbController:
    """
    Implementiert die gesamte Entscheidungslogik aus fb_controller() + check_blocking_rules()
    in fox2db.c, jetzt mit Dependency Injection der BlockingRules.
    """

    def __init__(self, cfg: Config, blocking_rules: list[BlockingRule],
                 dc: DcForecast) -> None:
        self._cfg = cfg
        self._rules = blocking_rules
        self._dc = dc

    def run(self, vals: MeasuredValues) -> ControlDecision:
        """Vollständiger Lauf mit drop_rate=0. Nur wenn Trend irrelevant."""
        decision = self.decide(vals)
        self.apply_blocking(vals, decision)
        return decision

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
            excess = v.pcc + ebox_actual + v.bat1
            return v.relay_st, excess, f"EBOX_SOC_UNKNOWN_HOLD (prot={v.prot})"

        ebox_eff = (max(v.bat_cur * 2 * 53 + 1, get_state_power(v.relay_st))
                    if v.relay_st > 0 else 0.0)
        excess = v.pcc + ebox_eff + v.bat1

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

        return best, excess, trace

    def _apply_blocking(self, v: MeasuredValues,
                        dec: ControlDecision) -> tuple[int, bool]:
        """Gibt (final_state, changed) zurück."""
        if dec.force:
            return dec.best_state, dec.best_state != v.relay_st

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

    def read_last(self) -> float:
        """Letzten gespeicherten Überschuss lesen, ohne zu schreiben."""
        return _read_file(self._path, 0.0)

    def write(self, excess: float) -> None:
        """Neuen Überschuss persistieren."""
        _write_file(self._path, f"{excess:.1f}")

    def update(self, excess: float) -> tuple[float, bool]:
        """Gibt (drop_rate, has_drop_rate) zurück und schreibt neuen Wert."""
        last = self.read_last()
        has_drop_rate = last > 0
        drop_rate = (excess - last) / 30.0 if has_drop_rate else 0.0
        self.write(excess)
        return drop_rate, has_drop_rate


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
            "pcc": round(vals.pcc),
            "bat1": round(vals.bat1),
            "ebox": round(ebox_w),
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

        _dc_delta = vals.dc_expected - (vals.pcc + ebox_w + vals.bat1)
        self._log(f"Data: SOC2={vals.soc:.1f}% SOC1={vals.soc_bat1:.1f}% "
                  f"PCC={vals.pcc:.0f}W Z2={vals.wirkleist:.0f}W "
                  f"Bat1={vals.bat1:.0f}W EBox={ebox_w:.0f}W "
                  f"DC_exp={vals.dc_expected:.0f}W DC_delta={_dc_delta:+.0f}W "
                  f"(State={vals.relay_st}) Stable={new_stable}")
        if dec.has_drop_rate and dec.drop_rate != 0:
            self._log(f"Trend: Excess {vals.last_excess:.0f}W→{dec.excess:.0f}W "
                      f"({dec.drop_rate:+.1f}W/s)")
        self._log(f"Result: State {dec.final_state} (TRACE: {dec.trace})")
        self._write_decision_log(vals, dec)

    def _write_decision_log(self, vals: MeasuredValues, dec: ControlDecision) -> None:
        try:
            conn = pymysql.connect(host="192.168.178.218", database="wagodb",
                                   user="gh", password="a12345", connect_timeout=3)
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO pv_decision_log "
                "(ts, state_from, state_to, decision, detail, pcc_w, bat1_w, soc, excess_w) "
                "VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s)",
                (vals.relay_st, dec.final_state,
                 dec.trace.split("|")[0].split("(")[0].strip()[:64],
                 dec.trace[:255],
                 int(vals.pcc), int(vals.bat1), round(vals.soc, 1), int(dec.excess))
            )
            conn.commit()
            conn.close()
        except Exception:
            pass

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
        self._dc = DcForecast()
        self._fb = FbController(cfg, blocking_rules, self._dc)

    def run_cycle(self) -> None:
        self._log("--- Start Cycle ---")

        # ── INPUT LAYER ───────────────────────────────────────────────────
        self._ebox.update()

        mqtt_data = self._mqtt.fetch()
        if mqtt_data is None:
            self._log("EMERGENCY SHUTDOWN: MQTT failed")
            self._relay.set_state(0)
            return

        bat_cur, soc = self._ebox.read()
        prot = self._dd_guard.read()

        # PCC-Fallback: bei NaN (Lesefehler) Z2-Zähler verwenden
        import math as _math
        raw_pcc = mqtt_data["pcc"]
        wirkleist = self._mqtt.fetch_z2()
        if _math.isnan(raw_pcc):
            pcc = abs(min(0.0, wirkleist))   # Z2: negativ=Export → positiv
            self._log(f"PCC=NaN → Z2-Fallback: wirkleist={wirkleist:.0f}W → pcc={pcc:.0f}W")
        else:
            pcc = raw_pcc

        vals = MeasuredValues(
            pcc=pcc,
            bat1=mqtt_data["bat1"],
            soc_bat1=mqtt_data["soc_bat1"],
            bat_cur=bat_cur,
            soc=soc,
            relay_st=self._relay.read_state(),
            stable=int(_read_file(self._cfg.path_last_change, 0)),
            prot=prot,
            last_excess=_read_file(self._cfg.path_last_excess, 0.0),
            dc_expected=self._dc.at() + self._dc.at_east(),
            wirkleist=wirkleist,
        )

        # ── LOGIC — Ebene 1: Zielzustand ─────────────────────────────────
        decision = self._fb.decide(vals)

        # ── LOGIC — Trend (Metadaten für Blocking) ────────────────────────
        has_drop_rate = vals.last_excess > 0
        decision.drop_rate = (decision.excess - vals.last_excess) / 30.0 if has_drop_rate else 0.0
        decision.has_drop_rate = has_drop_rate
        self._trend.write(decision.excess)

        # ── LOGIC — Ebene 2: Stabilitätsprüfung ──────────────────────────
        self._fb.apply_blocking(vals, decision)

        # ── OUTPUT ────────────────────────────────────────────────────────
        new_stable = 0 if decision.changed else vals.stable + 1
        self._writer.write_cycle(vals, decision, new_stable)

        if decision.changed:
            _db_relay_event(self._log, "state_change", "ebox", decision.final_state, decision.trace)

        self._dd_guard.update(vals.soc)

        if decision.changed:
            self._relay.set_state(decision.final_state)
        else:
            self._relay.keep_state(decision.final_state)

        # PCC > 20 kW: zuerst State +1 wenn Akku nicht voll, sonst DO4-Puls
        if vals.pcc > 20_000:
            next_st = min(decision.final_state + 1, 7)
            if vals.soc < 100 and next_st > decision.final_state:
                reason = f"PCC>20kW SOC={vals.soc:.0f}%<100% State{decision.final_state}→{next_st}"
                self._log(f"PCC>20kW: {reason} (kein DO4)")
                _db_relay_event(self._log, "state_change", "ebox", next_st, reason)
                self._relay.set_state(next_st)
            else:
                self._relay.pulse_r4()


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

def _db_relay_event(log, action: str, relay: str,
                    state_new: int, reason: str, duration_s: int = None) -> None:
    try:
        conn = pymysql.connect(host="192.168.178.218", database="wagodb",
                               user="gh", password="a12345", connect_timeout=3)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO pv_relay_events (ts, action, relay, state_new, reason, duration_s) "
            "VALUES (NOW(), %s, %s, %s, %s, %s)",
            (action, relay, state_new, reason[:255], duration_s)
        )
        cur.execute(
            "DELETE FROM pv_relay_events WHERE relay=%s "
            "AND ts < NOW() - INTERVAL 30 DAY", (relay,)
        )
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        log(f"DB relay_event error: {e}")



# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    cfg = Config.from_args()
    controller = PowerController(cfg)
    ppid = os.getppid()
    try:
        raw = open(f"/proc/{ppid}/cmdline", "rb").read()
        parent_cmd = raw.replace(bytes([0]), b" ").decode(errors="replace").strip()
    except OSError:
        parent_cmd = "?"
    controller._log(f"Start: PID={os.getpid()} PPID={ppid} parent={parent_cmd}")
    controller.run_cycle()


if __name__ == "__main__":
    main()

