#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import subprocess
import sys
import time
import zoneinfo as _zoneinfo
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import datetime as _dt
from astral import LocationInfo as _LocationInfo
from astral.sun import elevation as _astral_elevation
from astral.sun import azimuth as _astral_azimuth
from astral.sun import sun as _astral_sun

import pymysql
import paho.mqtt.client as mqtt

# ═══════════════════════════════════════════════════════════════════════════
# VERSION & KONSTANTEN
# ═══════════════════════════════════════════════════════════════════════════

VERSION = "v2.4-Py"
MAX_LOG_BYTES = 220 * 1024  # 220 kB, dann truncate

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
    """Alle Konfigurations-Parameter."""

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
    deep_discharge_lower: int = 6
    deep_discharge_upper: int = 8
    deep_discharge_charge_target: int = 7

    # Noon Pacer: Laderate drosseln damit Batterie erst beim Peak voll wird
    peak_guard_threshold: int = 14_000   # W — dc_noon > X → NoonPacer aktiv
    bat2_capacity_wh: int = 30_000       # EBox Kapazität in Wh
    noon_latitude: float = 47.6811
    noon_longitude: float = 11.5732

    # MQTT Empfang
    mqtt_broker: str = "kellertreppe.fritz.box"
    mqtt_port: int = 1883
    mqtt_topic: str = "inverter/power_grid_exchange/json"
    mqtt_timeout: int = 43
    mqtt_zaehl_topic: str = "pv_zaehl2/#"
    mqtt_zaehl_timeout: int = 10

    # MQTT Publish
    mqtt_publish_broker: str = ""
    mqtt_publish_port: int = 0
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
        p = argparse.ArgumentParser(
            description=f"fox2db {VERSION} – SOYO Inverter Power Management Controller"
        )
        p.add_argument("--mqtt-broker", default="kellertreppe.fritz.box")
        p.add_argument("--mqtt-port", type=int, default=1883)
        p.add_argument("--mqtt-topic", default="inverter/power_grid_exchange/json")
        p.add_argument("--mqtt-timeout", type=int, default=43)
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
        p.add_argument("--peak-guard-threshold", type=int, default=20_000,
                       help="dc_noon > X W → NoonPacer aktiv (default 20000)")
        p.add_argument("--bat2-capacity-wh", type=int, default=30_000,
                       help="EBox Kapazität in Wh (default 30000)")
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
            peak_guard_threshold=a.peak_guard_threshold,
            bat2_capacity_wh=a.bat2_capacity_wh,
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
    pcc: float = 0.0
    bat1: float = 0.0
    soc_bat1: float = 0.0
    bat_cur: float = 0.0
    soc: float = -1.0
    relay_st: int = 0
    stable: int = 0
    prot: int = 0
    last_excess: float = 0.0
    dc_pv: float = 0.0         # Sofar: Power_PV1 + Power_PV2 in kW
    dc_expected: float = 0.0   # Klarhimmel-DC-Prognose in W (für Log)


@dataclass
class ControlDecision:
    best_state: int = 0
    final_state: int = 0
    changed: bool = False
    excess: float = 0.0
    drop_rate: float = 0.0
    has_drop_rate: bool = False
    trace: str = ""
    reason: str = ""


# ═══════════════════════════════════════════════════════════════════════════
# MQTT CLIENT (Empfang)
# ═══════════════════════════════════════════════════════════════════════════

class MqttClient:
    def __init__(self, cfg: Config, log: Logger) -> None:
        self._cfg = cfg
        self._log = log

    def fetch(self) -> Optional[dict]:
        received: list[dict] = []

        def on_message(_client, _userdata, message):
            try:
                data = json.loads(message.payload.decode())
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
        pcc_raw = raw.get("ActivePower_PCC_Total")
        pcc = float(pcc_raw) * 1000.0 if pcc_raw is not None else float("nan")
        bat1 = raw.get("Power_Bat1", 0.0) * 1000.0
        soc_bat1 = raw.get("SOC_Bat1", 0.0)
        dc_pv = raw.get("Power_PV1", 0.0) + raw.get("Power_PV2", 0.0)
        if math.isnan(pcc):
            wl = self.fetch_wirkleist()
            pcc = -wl if wl is not None else 0.0
            self._log(f"MQTT Received: PCC=NaN→Z2={pcc:.0f}W, Bat1={bat1:.0f}W, SOC_Bat1={soc_bat1:.1f}%, PV_DC={dc_pv:.2f}kW")
        else:
            self._log(f"MQTT Received: PCC={pcc:.0f}W, Bat1={bat1:.0f}W, SOC_Bat1={soc_bat1:.1f}%, PV_DC={dc_pv:.2f}kW")
        return {"pcc": pcc, "bat1": bat1, "soc_bat1": soc_bat1, "dc_pv": dc_pv}

    def fetch_wirkleist(self) -> Optional[float]:
        """Liest Wirkleistung aus pv_zaehl2/# — Z2-Fallback wenn PCC=NaN."""
        received: list[dict] = []

        def on_message(_client, _userdata, message):
            try:
                received.append(json.loads(message.payload.decode()))
            except Exception:
                pass

        client = mqtt.Client(client_id="fox2db_z2", clean_session=True)
        client.on_message = on_message
        try:
            client.connect(self._cfg.mqtt_broker, self._cfg.mqtt_port, keepalive=30)
        except Exception:
            return None
        client.subscribe(self._cfg.mqtt_zaehl_topic, qos=0)
        client.loop_start()
        deadline = time.monotonic() + self._cfg.mqtt_zaehl_timeout
        while not received and time.monotonic() < deadline:
            time.sleep(0.1)
        client.loop_stop()
        client.disconnect()
        if not received:
            self._log(f"WARNING: Z2 wirkleist Timeout – kein Fallback")
            return None
        wl = received[0].get("wirkleist")
        if wl is None:
            return None
        self._log(f"Z2 Fallback: wirkleist={wl:.0f}W")
        return float(wl)


# ═══════════════════════════════════════════════════════════════════════════
# MQTT PUBLISHER (Ausgabe)
# ═══════════════════════════════════════════════════════════════════════════

class MqttPublisher:
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
                self._cfg.mqtt_publish_topic, json_str,
                qos=self._cfg.mqtt_publish_qos,
                retain=bool(self._cfg.mqtt_publish_retain),
            )
            result.wait_for_publish(timeout=4)
            self._log(f"MQTT-Publish: {len(json_str)} Bytes → {self._cfg.mqtt_publish_topic}")
        except Exception as e:
            self._log(f"MQTT-Publish Error: {e}")
        finally:
            client.disconnect()


# ═══════════════════════════════════════════════════════════════════════════
# EBOX READER
# ═══════════════════════════════════════════════════════════════════════════

class EBoxReader:
    def __init__(self, cfg: Config, log: Logger) -> None:
        self._cfg = cfg
        self._log = log

    def update(self) -> None:
        cmd = f"/usr/local/bin/ebox pwr > {self._cfg.path_ebox_data}"
        try:
            ret = subprocess.run(cmd, shell=True, timeout=10, capture_output=True)
            if ret.returncode != 0:
                self._log(f"EBox Update failed (rc={ret.returncode})")
        except subprocess.TimeoutExpired:
            self._log("EBox Update timeout")

    def _fetch_cells(self, pack: int, ts) -> list:
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
            if not parts or not parts[0].isdigit() or len(parts) < 9:
                continue
            try:
                rows.append((ts, pack, int(parts[0]), int(parts[1]),
                              int(parts[3]), int(parts[3]),
                              parts[4], parts[5], parts[6], parts[7],
                              int(parts[8].replace("%", ""))))
            except (ValueError, IndexError):
                continue
        return rows

    def _write_cells_to_db(self, rows: list) -> None:
        if not rows:
            return
        try:
            conn = pymysql.connect(host="192.168.178.218", database="wagodb",
                                   user="gh", password="a12345")
            cur = conn.cursor()
            cur.executemany(
                "INSERT INTO pv_ebox_cells "
                "(ts,pack,cell,volt,curr,tempr,base_st,volt_st,curr_st,temp_st,coulomb) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", rows)
            conn.commit()
            cur.close(); conn.close()
            self._log(f"EBox cells: {len(rows)} Zeilen geschrieben")
        except Exception as e:
            self._log(f"EBox cells DB error: {e}")

    def _fetch_soh(self, pack: int) -> float:
        try:
            ret = subprocess.run(["/usr/local/bin/ebox", "pwr", str(pack)],
                                 capture_output=True, text=True, timeout=10, errors="replace")
            real = total = None
            for line in ret.stdout.splitlines():
                if "Real Coulomb" in line:
                    real = float(line.split()[3])
                elif "Total Coulomb" in line:
                    total = float(line.split()[3])
            if real and total and total > 0:
                return round(real / total * 100, 1)
        except Exception as e:
            self._log(f"EBox pwr {pack} SOH error: {e}")
        return -1.0

    def collect_cells_if_needed(self, min_soc: float) -> None:
        if min_soc < 0 or not (min_soc > 99 or min_soc < 20):
            return
        ts = _dt.datetime.now()
        self._log(f"EBox cells: Einzelzell-Erfassung (SOC={min_soc:.1f}%)")
        time.sleep(4)
        all_rows: list = []
        for pack in (1, 2, 3):
            all_rows.extend(self._fetch_cells(pack, ts))
            time.sleep(4)
        self._write_cells_to_db(all_rows)
        if all_rows:
            worst = min(all_rows, key=lambda r: r[3])
            best  = max(all_rows, key=lambda r: r[3])
            self._log(f"EBox cells: MIN Pack{worst[1]} Zelle{worst[2]} {worst[3]}mV  "
                      f"MAX Pack{best[1]} Zelle{best[2]} {best[3]}mV  "
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

    def read(self) -> tuple:
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
                                       user="gh", password="a12345")
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

        return total_current_ma / 1000.0, (min(soc_values) if soc_values else -1.0)


# ═══════════════════════════════════════════════════════════════════════════
# RELAY CONTROLLER
# ═══════════════════════════════════════════════════════════════════════════

class RelayController:
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
            ret = subprocess.run(cmd, shell=True, timeout=10, capture_output=True, text=True)
            if ret.returncode == 0:
                self._write_state_file(state)
                self._log(f"Relay OK: → {state} | {ret.stdout.strip()}")
                return True
            self._log(f"Relay ERROR State {state}: [{ret.stdout.strip()}]")
            return False
        except subprocess.TimeoutExpired:
            self._log(f"Relay timeout: state {state}")
            return False

    def pulse_r4(self, duration: int = 3) -> None:
        script = self._cfg.path_ebyte_script
        cmd = ["python3", script, "r4", "pulse", str(duration)]
        try:
            proc = subprocess.Popen(cmd)
            self._log(f"Relay4 Puls ausgelöst (PCC>20kW, PID={proc.pid}, {duration}s)")
        except OSError as e:
            self._log(f"Relay4 fork fehlgeschlagen: {e}")

    def read_state(self) -> int:
        return int(_read_file(self._cfg.path_relay_state, 0))

    def _write_state_file(self, state: int) -> None:
        _write_file(self._cfg.path_relay_state, str(state))

    def keep_state(self, state: int) -> None:
        self._write_state_file(state)


# ═══════════════════════════════════════════════════════════════════════════
# STATE POWER HILFSFUNKTIONEN
# ═══════════════════════════════════════════════════════════════════════════

def get_state_power(state: int) -> int:
    return STATE_POWER.get(state, 0)


def find_best_state(budget: int) -> int:
    """Höchster State dessen Leistung ins Budget passt (primäres Ziel: PCC≈0)."""
    best_state, best_power = 0, 0
    for s, p in STATE_POWER.items():
        if p <= budget and p > best_power:
            best_state, best_power = s, p
    return best_state


def find_min_covering_state(needed: int) -> int:
    """Kleinster State der mindestens `needed` Watt absorbiert."""
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
# DC FORECAST — Klarhimmel-Modell (passiv, keine Entscheidungen)
# ═══════════════════════════════════════════════════════════════════════════

class DcForecast:
    """
    Klarhimmel-DC-Prognose (Meinel-Atmosphäre, monatlicher KT-Faktor).
    Reine Berechnung — trifft keine Entscheidungen.
    Wird von PeakGuard und für Logging genutzt.
    """
    _LAT, _LON = 47.6811, 11.5732
    _TZ_NAME   = "Europe/Berlin"
    _ARRAYS    = [                                        # (tilt°, az_S°, ppeak_W) — WR1 (getdcwest)
        (25,  80, 27_854),   # PV1 – 25° Tilt, fast West
        (60,  -5, 11_138),   # PV2 – 60° Tilt, fast Süd
    ]
    _ARRAYS_EAST = [                                      # WR2 (getdceast) — drei Arrays
        (41, -74, 19_852),   # PV2 Ost  (SE, dominante Strings)
        (60,  90,  2_078),   # PV2 West-Anteil
        (32,  94,  2_378),   # PV1 West
    ]
    # KT validiert aus inverter_data Ratio-Verteilung — Klarhimmel-Peak bei ratio≈1.0 (10.5%)
    _KT        = {1: 0.331, 2: 0.402, 3: 0.563, 4: 0.838,
                  5: 0.909, 6: 0.880, 7: 0.840, 8: 0.820,
                  9: 0.760, 10: 0.600, 11: 0.350, 12: 0.134}

    def __init__(self) -> None:
        self._loc = _LocationInfo("Lenggries", "Germany", self._TZ_NAME,
                                   self._LAT, self._LON)
        self._tz  = _zoneinfo.ZoneInfo(self._TZ_NAME)

    def peak_forecast_today(self, threshold_w: int = 20_000) -> tuple[_dt.datetime, float, Optional[_dt.datetime]]:
        """Scannt 05–22 Uhr in 2-Min-Schritten.
        Gibt (peak_t, peak_w, window_end) zurück.
        window_end = letzter Zeitpunkt mit WR1+WR2 >= threshold_w, None wenn kein Peak-Tag."""
        today = _dt.date.today()
        t   = _dt.datetime(today.year, today.month, today.day, 5, 0, tzinfo=self._tz)
        end = _dt.datetime(today.year, today.month, today.day, 22, 0, tzinfo=self._tz)
        step = _dt.timedelta(minutes=2)
        best_w, best_t = 0.0, t
        window_end: Optional[_dt.datetime] = None
        while t <= end:
            w = self.at(t) + self.at_east(t)
            if w > best_w:
                best_w, best_t = w, t
            if w >= threshold_w:
                window_end = t
            t += step
        return best_t, best_w, window_end

    def _cos_aoi(self, elev_deg: float, az_sun_N: float,
                 tilt_deg: float, az_panel_S: float) -> float:
        e  = math.radians(elev_deg)
        b  = math.radians(tilt_deg)
        da = math.radians((az_sun_N - 180.0) - az_panel_S)
        return math.sin(e) * math.cos(b) + math.cos(e) * math.sin(b) * math.cos(da)

    def cloud_pct(self, dc_pv_kw: float) -> float:
        """Sofort-Bewölkung % aus gemessener DC vs. Klarhimmel (Fallback)."""
        dc_now = self.at()
        if dc_now < 500:
            return 0.0
        return max(0.0, min(100.0, (1.0 - dc_pv_kw * 1000.0 / dc_now) * 100.0))

    def avg_cloud_5min(self) -> Optional[float]:
        """5-Minuten-Durchschnitt Bewölkung % aus pv_decision_log (bevorzugt)."""
        try:
            conn = pymysql.connect(host="192.168.178.218", database="wagodb",
                                   user="gh", password="a12345", connect_timeout=3)
            cur = conn.cursor()
            cur.execute("""
                SELECT ROUND(AVG((1 - dc_pv_w / dc_expected_w) * 100), 1)
                FROM pv_decision_log
                WHERE ts >= NOW() - INTERVAL 5 MINUTE
                  AND dc_expected_w > 500
            """)
            row = cur.fetchone()
            conn.close()
            return max(0.0, min(100.0, float(row[0]))) if row and row[0] is not None else None
        except Exception:
            return None

    def at(self, t: Optional[_dt.datetime] = None) -> float:
        """DC-Watt Klarhimmel-Prognose für Zeitpunkt t (default: jetzt)."""
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

    def at_east(self, t: Optional[_dt.datetime] = None) -> float:
        """WR2-DC-Watt Klarhimmel (getdceast-Modell) für Zeitpunkt t (default: jetzt)."""
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

    def at_total(self) -> float:
        """Klarhimmel-Gesamt-DC (WR1+WR2) jetzt in Watt — reine Geometrie, kein Wolken-Check."""
        return self.at() + self.at_east()



# ═══════════════════════════════════════════════════════════════════════════
# NOON PACER — Laderate drosseln damit Batterie erst beim Peak voll wird
# ═══════════════════════════════════════════════════════════════════════════

class NoonPacer:
    """
    Drosselt die Ladeleistung vor dem Solarmittag, damit die Batterie
    beim Peak noch Absorptionskapazität hat.

    Formel:
        pace_w = (max_soc - soc) / 100 × bat_capacity_wh / time_to_noon_h

    Ziel: Batterie wird GENAU beim Solarmittag voll — nicht vorher.
    Ab Mittag übernimmt POWER_MATCHING (freie Absorption des Peak-Überschusses).

    Wirkt NUR als Deckel (Ceiling) — erhöht nie, POWER_MATCHING bleibt primär.
    Nur aktiv wenn dc_noon > peak_guard_threshold und vor dem Mittag.
    DO4 bleibt der Notnagel bei gemessenem PCC > 20 kW.
    """

    def __init__(self, cfg: Config, dc_forecast: DcForecast) -> None:
        self._cfg = cfg
        self._dc  = dc_forecast

    def apply(self, vals: MeasuredValues, decision: ControlDecision) -> None:
        # 5-min DB-Avg bevorzugt; Fallback: Momentanwert
        clouds_avg = self._dc.avg_cloud_5min()
        cloud_pct = clouds_avg if clouds_avg is not None else self._dc.cloud_pct(vals.dc_pv)
        peak_t, peak_w, _ = self._dc.peak_forecast_today()
        peak_eff = peak_w * max(0.0, 1.0 - cloud_pct / 100.0)
        if peak_eff <= self._cfg.peak_guard_threshold:
            return
        time_to_peak_h = (peak_t - _dt.datetime.now(peak_t.tzinfo)).total_seconds() / 3600.0
        if time_to_peak_h <= 0:
            return
        remaining_wh = max(0.0, (self._cfg.max_soc - vals.soc) / 100.0
                           * self._cfg.bat2_capacity_wh)
        pace_w = remaining_wh / time_to_peak_h
        paced_state = find_min_covering_state(int(pace_w))
        if decision.final_state > paced_state:
            old = decision.final_state
            decision.final_state = paced_state
            decision.changed = (paced_state != vals.relay_st)
            decision.trace += (f" | NOON_PACER (peak={peak_eff:.0f}W"
                               f"@{peak_t:%H:%M}"
                               f", clouds={cloud_pct:.0f}%"
                               f", +{time_to_peak_h*60:.0f}min"
                               f", pace={pace_w:.0f}W→State{paced_state}"
                               f", remain={remaining_wh:.0f}Wh"
                               f") State{old}→{paced_state}")


# ═══════════════════════════════════════════════════════════════════════════
# BLOCKING RULES – Strategy-Pattern
# ═══════════════════════════════════════════════════════════════════════════

class BlockingRule(ABC):
    direction: str = "UP"

    @abstractmethod
    def is_blocked(self, pcc: float, bat1: float, stable: int,
                   drop_rate: float, pwr_diff: int) -> tuple[bool, str]:
        pass


class SweetSpotHold(BlockingRule):
    direction = "UP"
    def __init__(self, cfg: Config) -> None: self._cfg = cfg
    def is_blocked(self, pcc, bat1, stable, drop_rate, pwr_diff):
        if abs(pcc) < self._cfg.sweet_spot_pcc and bat1 > self._cfg.sweet_spot_bat:
            return True, f"SWEET_SPOT_HOLD (PCC={pcc:.0f}W, Bat1={bat1:.0f}W)"
        return False, ""


class TrendBlock(BlockingRule):
    direction = "UP"
    def __init__(self, cfg: Config) -> None: self._cfg = cfg
    def is_blocked(self, pcc, bat1, stable, drop_rate, pwr_diff):
        if drop_rate < self._cfg.max_drop_rate and drop_rate != 0:
            return True, f"TREND_BLOCK (Drop: {drop_rate:.1f}W/s)"
        return False, ""


class BatGuardBlock(BlockingRule):
    direction = "UP"
    def __init__(self, cfg: Config) -> None: self._cfg = cfg
    def is_blocked(self, pcc, bat1, stable, drop_rate, pwr_diff):
        if bat1 < self._cfg.bat_discharge_threshold:
            return True, f"BAT_GUARD_BLOCK (Bat1={bat1:.0f}W)"
        return False, ""


class StabilizingBlock(BlockingRule):
    direction = "DOWN"
    def __init__(self, cfg: Config) -> None: self._cfg = cfg
    def is_blocked(self, pcc, bat1, stable, drop_rate, pwr_diff):
        if stable < self._cfg.stabilization_cycles:
            return True, f"STABILIZING (stable={stable})"
        return False, ""


class HysteresisBlock(BlockingRule):
    direction = "DOWN"
    def __init__(self, cfg: Config) -> None: self._cfg = cfg
    def is_blocked(self, pcc, bat1, stable, drop_rate, pwr_diff):
        if pwr_diff < self._cfg.hysteresis:
            return True, f"HYSTERESIS ({pwr_diff}W)"
        return False, ""


# ═══════════════════════════════════════════════════════════════════════════
# FB CONTROLLER (Kern-Regellogik — primäres Ziel: PCC≈0)
# ═══════════════════════════════════════════════════════════════════════════

class FbController:
    def __init__(self, cfg: Config, blocking_rules: list[BlockingRule]) -> None:
        self._cfg = cfg
        self._rules = blocking_rules

    def decide(self, vals: MeasuredValues) -> ControlDecision:
        decision = ControlDecision()
        best, excess, trace = self._decide_state(vals)
        decision.best_state = best
        decision.excess = excess
        decision.trace = trace
        return decision

    def apply_blocking(self, vals: MeasuredValues, decision: ControlDecision) -> None:
        final, changed = self._apply_blocking(vals, decision)
        decision.final_state = final
        decision.changed = changed

    def _decide_state(self, v: MeasuredValues) -> tuple[int, float, str]:
        cfg = self._cfg

        if v.soc < 0:
            ebox_actual = (v.bat_cur * 2 * 53 + 1) if v.relay_st > 0 else 0.0
            excess = v.pcc + ebox_actual + v.bat1
            return v.relay_st, excess, f"EBOX_SOC_UNKNOWN_HOLD (prot={v.prot})"

        ebox_eff = (max(v.bat_cur * 2 * 53 + 1, get_state_power(v.relay_st))
                    if v.relay_st > 0 else 0.0)
        excess = v.pcc + ebox_eff + v.bat1

        if v.prot:
            if v.soc < cfg.deep_discharge_charge_target:
                return 1, excess, (f"EMERGENCY_CHARGE_TO_{cfg.deep_discharge_charge_target}%"
                                   f" (current {v.soc:.1f}%)")
            return 0, excess, f"CHARGE_TARGET_REACHED ({v.soc:.1f}%)"

        if v.soc < cfg.deep_discharge_lower:
            return 1, excess, f"CRITICAL_SOC_PROTECTION_ACTIVATE ({v.soc:.1f}%)"

        if v.soc > cfg.max_soc:
            return 0, excess, "BATTERY_FULL_STOP"

        if excess < cfg.min_excess:
            return 0, excess, f"INSUFFICIENT_EXCESS ({excess:.0f}W)"

        budget = int(excess + cfg.max_grid_draw)
        best = find_best_state(budget)
        trace = f"POWER_MATCHING (Excess: {excess:.0f}W, Budget: {budget}W)"

        if best > v.relay_st:
            next_st = get_next_state_up(v.relay_st)
            if best > next_st:
                trace += f" | RAMP_LIMITED ({best}->{next_st})"
                best = next_st

        return best, excess, trace

    def _apply_blocking(self, v: MeasuredValues,
                        dec: ControlDecision) -> tuple[int, bool]:
        best = dec.best_state
        relay_st = v.relay_st

        if best == relay_st:
            return relay_st, False

        pwr_diff = abs(get_state_power(best) - get_state_power(relay_st))
        direction = "UP" if get_state_power(best) > get_state_power(relay_st) else "DOWN"

        if v.pcc < -self._cfg.emergency_import and direction == "DOWN":
            dec.trace += f" | EMERGENCY_FORCE (Import={v.pcc:.0f}W)"
            return best, True

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
    def __init__(self, path_last_excess: str) -> None:
        self._path = path_last_excess

    def read_last(self) -> float:
        return _read_file(self._path, 0.0)

    def write(self, excess: float) -> None:
        _write_file(self._path, f"{excess:.1f}")


# ═══════════════════════════════════════════════════════════════════════════
# DEEP DISCHARGE GUARD
# ═══════════════════════════════════════════════════════════════════════════

class DeepDischargeGuard:
    def __init__(self, cfg: Config, log: Logger) -> None:
        self._cfg = cfg
        self._log = log
        self._path = cfg.path_deep_discharge

    def read(self) -> int:
        return int(_read_file(self._path, 0))

    def update(self, soc: float) -> None:
        cfg = self._cfg
        if soc < 0:
            self._log(f"WARNING: EBox SOC unbekannt (prot={self.read()})")
            return
        old_prot = self.read()
        if soc < cfg.deep_discharge_lower:
            if not old_prot:
                self._log(f"DEEP_DISCHARGE_PROTECTION ACTIVATED at {soc:.1f}%")
            _write_file(self._path, "1")
        elif soc >= cfg.deep_discharge_upper:
            if old_prot:
                self._log(f"DEEP_DISCHARGE_PROTECTION DEACTIVATED at {soc:.1f}%")
            _write_file(self._path, "0")


# ═══════════════════════════════════════════════════════════════════════════
# OUTPUT WRITER
# ═══════════════════════════════════════════════════════════════════════════

class OutputWriter:
    def __init__(self, cfg: Config, publisher: MqttPublisher, log: Logger) -> None:
        self._cfg = cfg
        self._pub = publisher
        self._log = log

    def write_cycle(self, vals: MeasuredValues, dec: ControlDecision,
                    new_stable: int) -> None:
        ebox_w = vals.bat_cur * 2 * 53
        ts = time.strftime("%Y-%m-%dT%H:%M:%S")
        payload = {
            "ts": ts, "version": VERSION,
            "soc_bat2": round(vals.soc * 10) / 10,
            "soc_bat1": round(vals.soc_bat1 * 10) / 10,
            "pcc": round(vals.pcc), "bat1": round(vals.bat1),
            "ebox": round(ebox_w), "state": dec.final_state,
            "state_before": vals.relay_st, "stable": new_stable,
            "excess": round(dec.excess),
            "drop_rate": (round(dec.drop_rate * 10) / 10
                          if dec.has_drop_rate and dec.drop_rate != 0 else None),
            "dc_expected": round(vals.dc_expected),
            "trace": dec.trace,
            "deep_discharge_active": bool(vals.prot),
        }
        self._pub.publish(payload)
        _write_file(self._cfg.path_last_change, str(new_stable))
        _write_file(self._cfg.path_inverter_csv,
                    f"{vals.pcc:.0f},{vals.bat1:.0f},{vals.soc:.1f},"
                    f"{vals.soc_bat1:.1f},{vals.bat_cur:.1f},{dec.final_state}\n")
        _dc_delta = vals.dc_expected - vals.pcc
        self._log(f"Data: SOC2={vals.soc:.1f}% SOC1={vals.soc_bat1:.1f}% "
                  f"PCC={vals.pcc:.0f}W Bat1={vals.bat1:.0f}W EBox={ebox_w:.0f}W "
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
                "(ts, state_from, state_to, decision, detail, pcc_w, bat1_w, soc, excess_w, dc_pv_w, dc_expected_w) "
                "VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (vals.relay_st, dec.final_state,
                 dec.trace.split("|")[0].split("(")[0].strip()[:64],
                 dec.trace[:255],
                 int(vals.pcc), int(vals.bat1), round(vals.soc, 1),
                 int(dec.excess),
                 int(vals.dc_pv * 1000), int(vals.dc_expected))
            )
            conn.commit()
            conn.close()
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════════
# POWER CONTROLLER – Haupt-Orchestrator
# ═══════════════════════════════════════════════════════════════════════════

class PowerController:
    """
    Entscheidungsreihenfolge je Cycle:
      1. POWER_MATCHING  — bester State für PCC≈0  (primäres Ziel)
      2. Blocking        — Ramp/Trend/Stabilisierung
      3. PeakGuard       — Floor erhöhen wenn Klarhimmel > 20 kW (sekundäres Ziel)
      4. DO4             — Notnagel: gemessenes PCC > 20 kW → pulse_r4
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
        self._dc_forecast = DcForecast()
        self._noon_pacer = NoonPacer(cfg, self._dc_forecast)

        blocking_rules: list[BlockingRule] = [
            SweetSpotHold(cfg), TrendBlock(cfg), BatGuardBlock(cfg),
            StabilizingBlock(cfg), HysteresisBlock(cfg),
        ]
        self._fb = FbController(cfg, blocking_rules)

    def run_cycle(self) -> None:
        t0 = time.monotonic()
        self._log("--- Start Cycle ---")

        # ── INPUT ─────────────────────────────────────────────────────────
        self._ebox.update()
        t_ebox_upd = time.monotonic()

        mqtt_data = self._mqtt.fetch()
        t_mqtt = time.monotonic()
        if mqtt_data is None:
            self._log("EMERGENCY SHUTDOWN: MQTT failed")
            self._relay.set_state(0)
            return

        bat_cur, soc = self._ebox.read()
        t_ebox_read = time.monotonic()
        self._ebox.collect_cells_if_needed(soc)
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
            dc_expected=self._dc_forecast.at(),
        )

        # ── LOGIC ─────────────────────────────────────────────────────────
        # Phase 1: bester State (PCC≈0)
        decision = self._fb.decide(vals)

        # Phase 2: Trend berechnen
        last_excess = vals.last_excess
        has_drop_rate = last_excess > 0
        decision.drop_rate = (decision.excess - last_excess) / 30.0 if has_drop_rate else 0.0
        decision.has_drop_rate = has_drop_rate
        _write_file(self._cfg.path_last_excess, f"{decision.excess:.1f}")

        # Phase 3: Blocking
        self._fb.apply_blocking(vals, decision)

        # Phase 4: NoonPacer — Laderate deckeln damit Batterie erst beim Peak voll
        _peak_t, _peak_w, _window_end = self._dc_forecast.peak_forecast_today()
        _now     = _dt.datetime.now(_peak_t.tzinfo)
        _wend_str = _window_end.strftime("%H:%M") if _window_end else "—"
        self._log(f"DC-Forecast: peak={_peak_w/1000:.1f}kW@{_peak_t:%H:%M}"
                  f" window_end={_wend_str}"
                  f" after_window={_window_end is not None and _now >= _window_end}")
        self._noon_pacer.apply(vals, decision)

        # Phase 4b: Peak-Window-Gate — State > 1 gesperrt bis window_end; danach POWER_MATCHING frei
        if _window_end is not None and _now < _window_end and decision.final_state > 1:
            old_st = decision.final_state
            decision.final_state = 1
            decision.changed = (vals.relay_st != 1)
            decision.trace += (f" | PEAK_GATE (peak={_peak_w/1000:.1f}kW"
                               f"@{_peak_t:%H:%M}"
                               f" end={_window_end:%H:%M}"
                               f" State{old_st}→1)")

        # Phase 5: PCC > 20 kW — SOC zuerst prüfen, dann State 7, DO4 als letzter Ausweg
        if vals.pcc > 20_000:
            if vals.soc >= 100:
                # Batterie voll — State 7 hilft nicht, sofort DO4
                self._relay.pulse_r4()
                decision.trace += f" | DO4 (PCC={vals.pcc:.0f}W, SOC=100%)"
            elif decision.final_state < 7:
                # Sofort auf State 7 hochschalten, Rampe überspringen
                decision.final_state = 7
                decision.changed = (vals.relay_st != 7)
                decision.trace += f" | PEAK_FORCE_7 (PCC={vals.pcc:.0f}W)"
            else:
                decision.trace += f" | DO4 HOLD (State7, SOC={vals.soc:.0f}%<100%)"

        # ── OUTPUT ────────────────────────────────────────────────────────
        new_stable = 0 if decision.changed else vals.stable + 1
        self._writer.write_cycle(vals, decision, new_stable)
        self._dd_guard.update(vals.soc)

        if decision.changed:
            self._relay.set_state(decision.final_state)
        else:
            self._relay.keep_state(decision.final_state)

        t_end = time.monotonic()
        self._log(f"Timing: ebox_pwr={t_ebox_upd-t0:.1f}s"
                  f" mqtt={t_mqtt-t_ebox_upd:.1f}s"
                  f" ebox_read={t_ebox_read-t_mqtt:.1f}s"
                  f" total={t_end-t0:.1f}s"
                  f" dc_expected={vals.dc_expected:.0f}W")


# ═══════════════════════════════════════════════════════════════════════════
# DATEI-HILFSFUNKTIONEN
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
