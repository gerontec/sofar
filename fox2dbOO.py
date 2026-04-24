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
from astral import LocationInfo as _LocationInfo
from astral.sun import sun as _astral_sun

import paho.mqtt.client as mqtt

# ═══════════════════════════════════════════════════════════════════════════
# VERSION & KONSTANTEN
# ═══════════════════════════════════════════════════════════════════════════

VERSION = "v1.56-Py"
MAX_LOG_BYTES = 10 * 1024  # 10 kB, dann truncate

# Hardware-Zustandstabelle: state → Watt
# ─── Solar-Mittag-Berechnung ─────────────────────────────────────────────────

def _solar_noon(lat: float, lon: float) -> _dt.datetime:
    """Solarer Höchststand heute als timezone-aware datetime (Europe/Berlin)."""
    loc = _LocationInfo("Standort", "Germany", "Europe/Berlin", lat, lon)
    s = _astral_sun(loc.observer, date=_dt.date.today(), tzinfo=loc.timezone)
    return s["noon"]


def _in_midday_window(cfg) -> bool:
    """True wenn jetzt innerhalb ±midday_window_minutes um den solaren Mittag."""
    try:
        noon = _solar_noon(cfg.midday_latitude, cfg.midday_longitude)
        now_dt = _dt.datetime.now(noon.tzinfo)
        diff_min = abs((now_dt - noon).total_seconds() / 60)
        return diff_min <= cfg.midday_window_minutes
    except Exception:
        return False


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
    max_grid_draw: int = 1500
    max_soc: int = 99
    hysteresis: int = 505
    stabilization_cycles: int = 2
    emergency_import: int = 1020
    bat_discharge_threshold: int = -220
    sweet_spot_pcc: int = 160
    sweet_spot_bat: int = -310
    max_drop_rate: int = -20
    midday_soc_threshold: int = 80      # SOC2-Schwelle für Mittagskapp
    midday_window_minutes: int = 60     # ±Minuten um den solaren Mittag
    midday_latitude: float = 47.6811    # Lenggries (PLZ 83661)
    midday_longitude: float = 11.5732
    midday_season_start_month: int = 3  # März (Frühling)
    midday_season_end_month: int = 10   # Oktober (Ende Herbst, inklusiv)
    deep_discharge_lower: int = 6
    deep_discharge_upper: int = 8
    deep_discharge_charge_target: int = 7

    # MQTT Empfang
    mqtt_broker: str = "kellertreppe.fritz.box"
    mqtt_port: int = 1883
    mqtt_topic: str = "inverter/power_grid_exchange/json"
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
        p.add_argument("--midday-soc-threshold", type=int, default=80)
        p.add_argument("--midday-window", type=int, default=60,
                       help="±Minuten um solaren Mittag (default 60)")
        p.add_argument("--midday-lat", type=float, default=47.6811)
        p.add_argument("--midday-lon", type=float, default=11.5732)
        p.add_argument("--midday-season-start", type=int, default=3)
        p.add_argument("--midday-season-end", type=int, default=10)
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
            midday_soc_threshold=a.midday_soc_threshold,
            midday_window_minutes=a.midday_window,
            midday_latitude=a.midday_lat,
            midday_longitude=a.midday_lon,
            midday_season_start_month=a.midday_season_start,
            midday_season_end_month=a.midday_season_end,
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


@dataclass
class ControlDecision:
    """Ergebnis der Regellogik."""
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

        try:
            for raw_line in path.read_text().splitlines():
                line = raw_line.strip().lstrip("b'").rstrip("'")
                if not line or line.startswith("Power"):
                    continue
                if line[0] in "123":
                    parts = line.split()
                    if len(parts) > 2:
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
        except OSError as e:
            self._log(f"EBox read error: {e}")
            return 0.0, -1.0

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

    def __init__(self, cfg: Config, blocking_rules: list[BlockingRule]) -> None:
        self._cfg = cfg
        self._rules = blocking_rules

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

        # Mittagskapp: SOC2 > Schwelle UND innerhalb ±window um Sonnenmittag → max State 1
        _now = time.localtime()
        now_m = _now.tm_mon
        if (cfg.midday_season_start_month <= now_m <= cfg.midday_season_end_month
                and v.soc >= cfg.midday_soc_threshold
                and best > 1
                and _in_midday_window(cfg)):
            trace += (f" | MIDDAY_SOC_CAP (SOC={v.soc:.0f}%≥"
                      f"{cfg.midday_soc_threshold}%,"
                      f" ±{cfg.midday_window_minutes}min Sonnenmittag, Monat {now_m})")
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

        self._log(f"Data: SOC2={vals.soc:.1f}% SOC1={vals.soc_bat1:.1f}% "
                  f"PCC={vals.pcc:.0f}W Bat1={vals.bat1:.0f}W EBox={ebox_w:.0f}W "
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

        vals = MeasuredValues(
            pcc=mqtt_data["pcc"],
            bat1=mqtt_data["bat1"],
            soc_bat1=mqtt_data["soc_bat1"],
            bat_cur=bat_cur,
            soc=soc,
            relay_st=self._relay.read_state(),
            stable=int(_read_file(self._cfg.path_last_change, 0)),
            prot=prot,
            last_excess=_read_file(self._cfg.path_last_excess, 0.0),
        )

        # Relais-4-Puls bei PCC-Einspeisung > 20 kW
        if vals.pcc > 20_000:
            self._relay.pulse_r4()

        # ── LOGIC LAYER ───────────────────────────────────────────────────
        # Reihenfolge 1:1 wie C: decide → drop_rate berechnen → blocking
        # vals.last_excess wurde oben bereits aus Datei gelesen (vor jedem Schreiben)

        # Phase 1: Zustandsentscheidung (excess wird dabei berechnet, drop_rate nicht nötig)
        decision = self._fb.decide(vals)

        # Phase 2: Trend mit jetzt bekanntem excess berechnen und persistieren
        has_drop_rate = vals.last_excess > 0
        decision.drop_rate = (decision.excess - vals.last_excess) / 30.0 if has_drop_rate else 0.0
        decision.has_drop_rate = has_drop_rate
        self._trend.write(decision.excess)

        # Phase 3: Blocking einmalig mit korrektem drop_rate (kein Doppellauf)
        self._fb.apply_blocking(vals, decision)

        # ── OUTPUT LAYER ──────────────────────────────────────────────────
        new_stable = 0 if decision.changed else vals.stable + 1
        self._writer.write_cycle(vals, decision, new_stable)

        # Tiefentladeschutz aktualisieren
        self._dd_guard.update(vals.soc)

        # Relais schalten oder halten
        if decision.changed:
            self._relay.set_state(decision.final_state)
        else:
            self._relay.keep_state(decision.final_state)


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
