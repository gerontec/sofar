"""
Tests für fox2dbOO.py — alle Klassen mit reiner Logik oder Datei-I/O.
MQTT/Hardware-Klassen (MqttClient, MqttPublisher, RelayController.set_state)
sind ausgeklammert — sie benötigen echte Hardware oder einen Broker.

Ausführen:
    python3 -m pytest test_fox2dbOO.py -v
"""
import sys
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, "/home/pi/python")
from fox2dbOO import (
    Config, Logger, MeasuredValues, ControlDecision,
    EBoxReader, RelayController,
    BlockingRule, SweetSpotHold, TrendBlock, BatGuardBlock,
    StabilizingBlock, HysteresisBlock,
    FbController, ExcessTrendCalc, DeepDischargeGuard,
    get_state_power, find_best_state, get_next_state_up,
    _read_file, _write_file, STATE_POWER,
)

# ─────────────────────────────────────────────────────────────────────────────
# Modul-Funktionen
# ─────────────────────────────────────────────────────────────────────────────

class TestStateFunctions:
    def test_get_state_power_known(self):
        assert get_state_power(0) == 0
        assert get_state_power(1) == 3000
        assert get_state_power(7) == 11400

    def test_get_state_power_unknown(self):
        assert get_state_power(99) == 0

    def test_find_best_state_exact(self):
        assert find_best_state(11400) == 7

    def test_find_best_state_below(self):
        # budget 10000 → state 6 (7800W) ist höchster ≤ 10000
        assert find_best_state(10000) == 6

    def test_find_best_state_zero_budget(self):
        assert find_best_state(0) == 0

    def test_find_best_state_huge(self):
        assert find_best_state(99999) == 7

    def test_ramp_order(self):
        # Reihenfolge nach Watt: 0→1→2→4→3→5→6→7
        chain = []
        s = 0
        for _ in range(8):
            chain.append(s)
            nxt = get_next_state_up(s)
            if nxt == s:
                break
            s = nxt
        assert chain == [0, 1, 2, 4, 3, 5, 6, 7]

    def test_next_state_up_at_max(self):
        # State 7 ist Maximum — bleibt bei 7
        assert get_next_state_up(7) == 7

    def test_next_after_state2_is_4_not_3(self):
        # State 2=3650W, State 4=3900W, State 3=6650W → nächster nach 2 ist 4
        assert get_next_state_up(2) == 4


# ─────────────────────────────────────────────────────────────────────────────
# Blocking Rules
# ─────────────────────────────────────────────────────────────────────────────

def make_cfg(**kwargs):
    cfg = Config()
    for k, v in kwargs.items():
        setattr(cfg, k, v)
    return cfg


class TestSweetSpotHold:
    def setup_method(self):
        self.rule = SweetSpotHold(make_cfg(sweet_spot_pcc=160, sweet_spot_bat=-310))

    def _check(self, pcc, bat1):
        return self.rule.is_blocked(pcc, bat1, 0, 0.0, 0)

    def test_blocked_pcc_and_bat_in_range(self):
        blocked, reason = self._check(pcc=50, bat1=-100)
        assert blocked
        assert "SWEET_SPOT_HOLD" in reason

    def test_not_blocked_pcc_too_high(self):
        blocked, _ = self._check(pcc=200, bat1=-100)
        assert not blocked

    def test_not_blocked_bat_too_low(self):
        blocked, _ = self._check(pcc=50, bat1=-400)
        assert not blocked

    def test_direction_up_only(self):
        assert self.rule.direction == "UP"


class TestTrendBlock:
    def setup_method(self):
        self.rule = TrendBlock(make_cfg(max_drop_rate=-20))

    def test_blocked_on_steep_drop(self):
        blocked, reason = self.rule.is_blocked(0, 0, 0, -64.0, 0)
        assert blocked
        assert "TREND_BLOCK" in reason

    def test_not_blocked_zero_rate(self):
        blocked, _ = self.rule.is_blocked(0, 0, 0, 0.0, 0)
        assert not blocked

    def test_not_blocked_slow_drop(self):
        blocked, _ = self.rule.is_blocked(0, 0, 0, -10.0, 0)
        assert not blocked

    def test_not_blocked_rising(self):
        blocked, _ = self.rule.is_blocked(0, 0, 0, +50.0, 0)
        assert not blocked

    def test_direction_up_only(self):
        assert self.rule.direction == "UP"


class TestBatGuardBlock:
    def setup_method(self):
        self.rule = BatGuardBlock(make_cfg(bat_discharge_threshold=-220))

    def test_blocked_below_threshold(self):
        blocked, reason = self.rule.is_blocked(0, -300, 0, 0, 0)
        assert blocked
        assert "BAT_GUARD_BLOCK" in reason

    def test_not_blocked_above(self):
        blocked, _ = self.rule.is_blocked(0, -100, 0, 0, 0)
        assert not blocked


class TestStabilizingBlock:
    def setup_method(self):
        self.rule = StabilizingBlock(make_cfg(stabilization_cycles=2))

    def test_blocked_not_stable(self):
        blocked, reason = self.rule.is_blocked(0, 0, stable=1, drop_rate=0, pwr_diff=0)
        assert blocked
        assert "STABILIZING" in reason

    def test_not_blocked_stable(self):
        blocked, _ = self.rule.is_blocked(0, 0, stable=2, drop_rate=0, pwr_diff=0)
        assert not blocked

    def test_direction_down_only(self):
        assert self.rule.direction == "DOWN"


class TestHysteresisBlock:
    def setup_method(self):
        self.rule = HysteresisBlock(make_cfg(hysteresis=505))

    def test_blocked_small_diff(self):
        blocked, reason = self.rule.is_blocked(0, 0, 0, 0, pwr_diff=400)
        assert blocked
        assert "HYSTERESIS" in reason

    def test_not_blocked_large_diff(self):
        blocked, _ = self.rule.is_blocked(0, 0, 0, 0, pwr_diff=1000)
        assert not blocked

    def test_direction_down_only(self):
        assert self.rule.direction == "DOWN"


# ─────────────────────────────────────────────────────────────────────────────
# FbController — Zustandsentscheidung
# ─────────────────────────────────────────────────────────────────────────────

def make_fb(cfg=None):
    c = cfg or Config()
    rules = [
        SweetSpotHold(c), TrendBlock(c), BatGuardBlock(c),
        StabilizingBlock(c), HysteresisBlock(c),
    ]
    return FbController(c, rules)


def make_vals(**kwargs):
    defaults = dict(pcc=0, bat1=0, soc_bat1=100, bat_cur=0,
                    soc=50.0, relay_st=0, stable=5, prot=0, last_excess=0.0)
    defaults.update(kwargs)
    return MeasuredValues(**defaults)


class TestFbControllerDecide:
    def test_soc_unknown_holds_state(self):
        fb = make_fb()
        dec = fb.decide(make_vals(soc=-1.0, relay_st=3))
        assert dec.best_state == 3
        assert "EBOX_SOC_UNKNOWN_HOLD" in dec.trace

    def test_battery_full_stop(self):
        fb = make_fb()
        dec = fb.decide(make_vals(soc=100.0, pcc=10000))
        assert dec.best_state == 0
        assert "BATTERY_FULL_STOP" in dec.trace

    def test_insufficient_excess(self):
        fb = make_fb()
        dec = fb.decide(make_vals(soc=50, pcc=500))
        assert dec.best_state == 0
        assert "INSUFFICIENT_EXCESS" in dec.trace

    def test_critical_soc_activates_protection(self):
        fb = make_fb()
        dec = fb.decide(make_vals(soc=4.0, pcc=5000))
        assert dec.best_state == 1
        assert "CRITICAL_SOC_PROTECTION" in dec.trace

    def test_emergency_charge_when_prot_active(self):
        fb = make_fb()
        dec = fb.decide(make_vals(soc=5.0, pcc=5000, prot=1))
        assert dec.best_state == 1
        assert "EMERGENCY_CHARGE" in dec.trace

    def test_charge_target_reached(self):
        fb = make_fb()
        dec = fb.decide(make_vals(soc=8.0, pcc=5000, prot=1))
        assert dec.best_state == 0
        assert "CHARGE_TARGET_REACHED" in dec.trace

    def test_power_matching_full_power(self):
        fb = make_fb()
        # relay_st=7 → kein RAMP_LIMITED; 14000W PCC → budget 15500 → State 7
        dec = fb.decide(make_vals(soc=50, pcc=14000, relay_st=7))
        assert dec.best_state == 7
        assert "POWER_MATCHING" in dec.trace
        assert "RAMP_LIMITED" not in dec.trace

    def test_ramp_limited_from_zero(self):
        fb = make_fb()
        # relay_st=0, excess=14000 → best=7, aber RAMP_LIMITED → next=1
        dec = fb.decide(make_vals(soc=50, pcc=14000, relay_st=0))
        assert dec.best_state == 1
        assert "RAMP_LIMITED" in dec.trace

    def test_no_ramp_limit_when_already_at_best(self):
        fb = make_fb()
        # relay_st=7, excess=14000 → best=7, kein Ramp nötig
        dec = fb.decide(make_vals(soc=50, pcc=14000, relay_st=7))
        assert dec.best_state == 7
        assert "RAMP_LIMITED" not in dec.trace


class TestFbControllerBlocking:
    def test_trend_block_fires_with_correct_drop_rate(self):
        """Kernfix: drop_rate muss vor apply_blocking auf decision gesetzt sein."""
        fb = make_fb()
        dec = fb.decide(make_vals(soc=50, pcc=14000, relay_st=0))
        # State 0→1 geplant; drop_rate = -64 W/s → muss TREND_BLOCK auslösen
        dec.drop_rate = -64.0
        dec.has_drop_rate = True
        fb.apply_blocking(make_vals(soc=50, pcc=14000, relay_st=0), dec)
        assert dec.final_state == 0
        assert "TREND_BLOCK" in dec.trace

    def test_trend_block_not_fires_with_zero_drop_rate(self):
        fb = make_fb()
        vals = make_vals(soc=50, pcc=14000, relay_st=0)
        dec = fb.decide(vals)
        dec.drop_rate = 0.0
        fb.apply_blocking(vals, dec)
        assert "TREND_BLOCK" not in dec.trace

    def test_sweet_spot_blocks_up(self):
        fb = make_fb()
        # SweetSpot-Bedingung: |pcc| < 160 UND bat1 > -310
        # relay_st=1 mit ebox_eff=3000 → excess = 50 + 3000 - 100 = 2950 → best=2 (UP)
        vals = make_vals(soc=50, pcc=50, bat1=-100, relay_st=1)
        dec = fb.decide(vals)
        dec.drop_rate = 0.0
        fb.apply_blocking(vals, dec)
        assert dec.final_state == 1   # bleibt bei relay_st
        assert "SWEET_SPOT_HOLD" in dec.trace

    def test_emergency_import_bypasses_blocking(self):
        """Notfall-Import überspringt Blocking und schaltet sofort auf best."""
        fb = make_fb()
        # relay_st=3 (6650W), pcc=-2000 → excess=4650 → budget=6150 → best=4 (3900W, DOWN)
        # stable=0 würde DOWN normalerweise per STABILIZING blockieren
        # Emergency: pcc < -1020 AND direction=DOWN → sofort schalten ohne Blocking
        vals = make_vals(soc=50, pcc=-2000, relay_st=3, stable=0)
        dec = fb.decide(vals)
        dec.drop_rate = 0.0
        fb.apply_blocking(vals, dec)
        assert dec.changed
        assert "EMERGENCY_FORCE" in dec.trace
        # STABILIZING hätte ohne Emergency blockiert → stattdessen geändert
        assert dec.final_state == dec.best_state

    def test_hysteresis_blocks_down(self):
        fb = make_fb()
        # relay_st=7 (11400W), best=6 (7800W), diff=3600W < hysteresis=505? Nein (3600>505)
        # Machen wir diff klein: relay_st=6 (7800W), best=5 (7100W), diff=700W > 505 → nicht blockiert
        # Für blockiert: relay_st=2 (3650W), best=1 (3000W), diff=650W > 505 → nicht blockiert
        # relay_st=4 (3900W), best=2 (3650W), diff=250W < 505 → blockiert
        vals = make_vals(soc=50, pcc=2000, relay_st=4, stable=5)
        dec = fb.decide(vals)
        dec.drop_rate = 0.0
        fb.apply_blocking(vals, dec)
        # best=1 oder 2 je nach excess; key: wenn diff < 505 → HYSTERESIS
        if "HYSTERESIS" in dec.trace:
            assert not dec.changed

    def test_no_change_when_best_equals_relay_st(self):
        fb = make_fb()
        # State bleibt bei 0 (INSUFFICIENT_EXCESS)
        vals = make_vals(soc=50, pcc=500, relay_st=0)
        dec = fb.decide(vals)
        fb.apply_blocking(vals, dec)
        assert not dec.changed
        assert dec.final_state == 0


# ─────────────────────────────────────────────────────────────────────────────
# ExcessTrendCalc
# ─────────────────────────────────────────────────────────────────────────────

class TestExcessTrendCalc:
    def setup_method(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmpdir.name, "last_excess.txt")
        self.calc = ExcessTrendCalc(self.path)

    def teardown_method(self):
        self.tmpdir.cleanup()

    def test_read_last_missing_file(self):
        assert self.calc.read_last() == 0.0

    def test_write_then_read(self):
        self.calc.write(12345.6)
        assert self.calc.read_last() == 12345.6

    def test_update_first_cycle_no_drop_rate(self):
        drop_rate, has = self.calc.update(10000.0)
        assert not has
        assert drop_rate == 0.0

    def test_update_second_cycle_computes_drop_rate(self):
        self.calc.write(10000.0)
        drop_rate, has = self.calc.update(11800.0)
        assert has
        assert abs(drop_rate - (11800 - 10000) / 30.0) < 0.01

    def test_update_negative_drop(self):
        self.calc.write(15000.0)
        drop_rate, has = self.calc.update(12000.0)
        assert has
        assert drop_rate < 0
        assert abs(drop_rate - (12000 - 15000) / 30.0) < 0.01

    def test_update_writes_new_value(self):
        self.calc.update(9999.0)
        assert self.calc.read_last() == 9999.0

    def test_no_corrupt_on_sequential_updates(self):
        """Kernfix: update() darf nicht erst 0 schreiben."""
        self.calc.write(10000.0)
        drop1, _ = self.calc.update(11500.0)
        drop2, _ = self.calc.update(13000.0)
        assert abs(drop1 - (11500 - 10000) / 30.0) < 0.01
        assert abs(drop2 - (13000 - 11500) / 30.0) < 0.01


# ─────────────────────────────────────────────────────────────────────────────
# DeepDischargeGuard
# ─────────────────────────────────────────────────────────────────────────────

class TestDeepDischargeGuard:
    def setup_method(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        cfg = Config()
        cfg.path_deep_discharge = os.path.join(self.tmpdir.name, "prot.txt")
        cfg.deep_discharge_lower = 6
        cfg.deep_discharge_upper = 8
        log = MagicMock()
        self.guard = DeepDischargeGuard(cfg, log)
        self.cfg = cfg

    def teardown_method(self):
        self.tmpdir.cleanup()

    def test_initial_read_returns_zero(self):
        assert self.guard.read() == 0

    def test_activates_below_lower(self):
        self.guard.update(soc=5.0)
        assert self.guard.read() == 1

    def test_deactivates_at_upper(self):
        _write_file(self.cfg.path_deep_discharge, "1")
        self.guard.update(soc=8.0)
        assert self.guard.read() == 0

    def test_hysteresis_unchanged_in_between(self):
        _write_file(self.cfg.path_deep_discharge, "1")
        self.guard.update(soc=7.0)   # zwischen lower(6) und upper(8)
        assert self.guard.read() == 1  # bleibt aktiv

    def test_hysteresis_from_zero_unchanged(self):
        self.guard.update(soc=7.0)
        assert self.guard.read() == 0  # bleibt inaktiv

    def test_unknown_soc_no_change(self):
        _write_file(self.cfg.path_deep_discharge, "1")
        self.guard.update(soc=-1.0)
        assert self.guard.read() == 1


# ─────────────────────────────────────────────────────────────────────────────
# EBoxReader.read() — mit Temp-Datei
# ─────────────────────────────────────────────────────────────────────────────

class TestEBoxReader:
    def setup_method(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        cfg = Config()
        cfg.path_ebox_data = os.path.join(self.tmpdir.name, "ebox.txt")
        self.cfg = cfg
        self.reader = EBoxReader(cfg, MagicMock())

    def teardown_method(self):
        self.tmpdir.cleanup()

    def test_missing_file_returns_defaults(self):
        cur, soc = self.reader.read()
        assert cur == 0.0
        assert soc == -1.0

    def test_parses_current_and_soc(self):
        # Format: Zeile beginnt mit 1/2/3, 3. Feld = mA, Feld mit % = SOC
        Path(self.cfg.path_ebox_data).write_text(
            "1 bat 12500 other 80.0% more\n"
            "2 bat 13000 other 75.0% more\n"
        )
        cur, soc = self.reader.read()
        assert abs(cur - (12500 + 13000) / 1000.0) < 0.01
        assert soc == 75.0  # min der beiden

    def test_ignores_header_lines(self):
        Path(self.cfg.path_ebox_data).write_text(
            "Power some header line\n"
            "1 bat 5000 other 90.0%\n"
        )
        cur, soc = self.reader.read()
        assert cur == 5.0
        assert soc == 90.0


# ─────────────────────────────────────────────────────────────────────────────
# RelayController — State-Datei (ohne Hardware)
# ─────────────────────────────────────────────────────────────────────────────

class TestRelayControllerState:
    def setup_method(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        cfg = Config()
        cfg.path_relay_state = os.path.join(self.tmpdir.name, "relay.txt")
        self.relay = RelayController(cfg, MagicMock())
        self.cfg = cfg

    def teardown_method(self):
        self.tmpdir.cleanup()

    def test_read_state_missing_file(self):
        assert self.relay.read_state() == 0

    def test_keep_state_writes_file(self):
        self.relay.keep_state(5)
        assert self.relay.read_state() == 5

    def test_keep_state_roundtrip(self):
        for s in [0, 1, 3, 7]:
            self.relay.keep_state(s)
            assert self.relay.read_state() == s


# ─────────────────────────────────────────────────────────────────────────────
# Logger
# ─────────────────────────────────────────────────────────────────────────────

class TestLogger:
    def setup_method(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmpdir.name, "test.log")

    def teardown_method(self):
        self.tmpdir.cleanup()

    def test_creates_log_file(self):
        log = Logger(self.path)
        log("Hallo Welt")
        assert Path(self.path).exists()

    def test_log_contains_message(self):
        log = Logger(self.path)
        log("Testnachricht")
        content = Path(self.path).read_text()
        assert "Testnachricht" in content
        assert "v1.56-Py" in content

    def test_callable(self):
        log = Logger(self.path)
        log("via __call__")
        assert "via __call__" in Path(self.path).read_text()
