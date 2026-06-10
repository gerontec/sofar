#!/usr/bin/env python3
"""
fox2db_model.py — Mathematische Referenz des fox2db-Reglers, vereinfacht auf
2 analoge Eingänge (pcc, ebox) und 1 Zustands-Ausgang s ∈ {0..7}.

Modellklasse: zeitdiskreter, nichtlinearer hybrider Automat (Mealy-Maschine mit
Hysterese). Kein PID, kein LTI. Zustand x = (s, c):
    s ∈ {0..7}   Relais-/Last-State
    c ∈ ℕ        Stabilisierungs-Zähler (Takte seit letzter Änderung)

Rekursion pro 60-s-Takt:
    ebox_eff = max(ebox, P[s])      falls s>0, sonst 0          (Selbstkopplung)
    E        = pcc + ebox_eff                                   (Überschuss)
    target   = Q(E + MAX_GRID_DRAW)                             (Quantisierer; 0 = unzureichend)
    ramped   = ramp(target, s)                                  (Hochrampe ≤1 Stufe)
    s'       = blocking(ramped, s, c, pcc)                      (Hysterese/Schutz)

Zwei Varianten unterscheiden sich NUR in der Ramp-Stufe:
    step_literal : Vergleich über die State-NUMMER  (1:1 wie fox2db_logic.h)
    step_rank    : Vergleich über den Leistungs-RANG (sauber leistungsmonoton)

Run:  python3 fox2db_model.py   →  führt die Testsuite aus.
"""
from typing import Tuple, Callable

# ── Konstanten (1:1 aus fox2db_logic.h) ──────────────────────────────────────
P            = [0, 3000, 3650, 6650, 3900, 7100, 7800, 11400]   # State → Leistung [W]
SORTED       = [0, 1, 2, 4, 3, 5, 6, 7]                          # nach Leistung aufsteigend
RANK         = {s: i for i, s in enumerate(SORTED)}             # State → Leistungs-Rang
MAX_GRID_DRAW    = 900.0    # G — erlaubter Netzbezug ins Budget
HYSTERESIS       = 505.0    # H
STABILIZATION    = 2        # N
EMERGENCY_MARGIN = 120.0    # harter Abwurf erst bei G + Margin
EMERGENCY_IMPORT = MAX_GRID_DRAW + EMERGENCY_MARGIN   # = 1020 (an G gekoppelt)
# nur für das volle (pcc, ebox, bat1)-Modell:
SWEET_SPOT_PCC   = 160.0    # |pcc| darunter ⇒ Sweet-Spot (kein Hochschalten)
BAT_DISCHARGE_TH = -110.0   # bat1 darunter ⇒ Sofar entlädt ⇒ kein Hochschalten
MAX_DROP_RATE    = -20.0    # Excess-Steigung [W/s] darunter ⇒ Trendwende


# ── Quantisierer Q(B): höchster State mit P[s] ≤ B ───────────────────────────
def quantize(B: float) -> int:
    best_s, best_p = 0, -1
    for s in range(8):
        if P[s] <= B and P[s] > best_p:
            best_s, best_p = s, P[s]
    return best_s


# ── Memoryloses Ziel (decide ohne Ramp/Blocking) ─────────────────────────────
def target_state(s_prev: int, pcc: float, ebox: float) -> Tuple[int, float, str]:
    ebox_eff = max(ebox, float(P[s_prev])) if s_prev > 0 else 0.0
    E = pcc + ebox_eff
    t = quantize(E + MAX_GRID_DRAW)
    return t, E, ("INSUFFICIENT_EXCESS" if t == 0 else "POWER_MATCHING")


# ── Ramp-Varianten (einziger Unterschied der beiden Modelle) ─────────────────
def ramp_literal(target: int, s_prev: int) -> Tuple[int, bool]:
    """Hochrampe, Vergleich über State-NUMMER — 1:1 wie der C-Code."""
    if target > s_prev:                                   # State-Nummer!
        nxt = SORTED[min(RANK[s_prev] + 1, 7)]
        if target > nxt:                                  # State-Nummer!
            return nxt, True
    return target, False


def ramp_rank(target: int, s_prev: int) -> Tuple[int, bool]:
    """Hochrampe, Vergleich über Leistungs-RANG — sauber leistungsmonoton."""
    if RANK[target] > RANK[s_prev]:
        nxt = SORTED[min(RANK[s_prev] + 1, 7)]
        if RANK[target] > RANK[nxt]:
            return nxt, True
    return target, False


# ── Blocking (Schutz/Dämpfung beim Schalten) ─────────────────────────────────
def blocking(ramped: int, s_prev: int, c: int, pcc: float) -> Tuple[int, int, str]:
    if ramped == s_prev:
        return s_prev, c + 1, ""
    up = P[ramped] > P[s_prev]
    if pcc < -EMERGENCY_IMPORT and not up:
        return ramped, 0, "|EMERGENCY_FORCE"
    if not up and c < STABILIZATION:
        return s_prev, c + 1, "|STABILIZING"
    if not up and abs(P[ramped] - P[s_prev]) < HYSTERESIS:
        return s_prev, c + 1, "|HYSTERESIS"
    return ramped, 0, ""


# ── Gesamt-Schritt ───────────────────────────────────────────────────────────
def _step(s: int, c: int, pcc: float, ebox: float,
          ramp: Callable[[int, int], Tuple[int, bool]]) -> Tuple[int, int, str]:
    target, _E, trace = target_state(s, pcc, ebox)
    ramped, capped = ramp(target, s)
    if capped:
        trace += "|RAMP_LIMITED"
    s_new, c_new, t2 = blocking(ramped, s, c, pcc)
    return s_new, c_new, trace + t2


def step_literal(s: int, c: int, pcc: float, ebox: float) -> Tuple[int, int, str]:
    return _step(s, c, pcc, ebox, ramp_literal)


def step_rank(s: int, c: int, pcc: float, ebox: float) -> Tuple[int, int, str]:
    return _step(s, c, pcc, ebox, ramp_rank)


# ═══════════════════════════════════════════════════════════════════════════
#   VOLLES MODELL mit 3. Eingang bat1 (Sofar-Batterieleistung)
#   Zustand x = (s, c, last_excess). Reaktiviert SWEET_SPOT/TREND/BAT_GUARD und
#   macht STABILIZING/HYSTERESIS erreichbar. 1:1 zu fox2db_logic.h (ohne soc2-Guards).
# ═══════════════════════════════════════════════════════════════════════════
def blocking_full(ramped: int, s_prev: int, c: int, pcc: float, bat1: float,
                  drop_rate: float) -> Tuple[int, int, str]:
    if ramped == s_prev:
        return s_prev, c + 1, ""
    pwr_diff = abs(P[ramped] - P[s_prev])
    up = P[ramped] > P[s_prev]
    if pcc < -EMERGENCY_IMPORT and not up:
        return ramped, 0, "|EMERGENCY_FORCE"
    if up and abs(pcc) < SWEET_SPOT_PCC:
        return s_prev, c + 1, "|SWEET_SPOT_HOLD"
    if up and drop_rate < MAX_DROP_RATE and drop_rate != 0:
        return s_prev, c + 1, "|TREND_BLOCK"
    if up and bat1 < BAT_DISCHARGE_TH:
        return s_prev, c + 1, "|BAT_GUARD_BLOCK"
    if not up and c < STABILIZATION:
        return s_prev, c + 1, "|STABILIZING"
    if not up and pwr_diff < HYSTERESIS:
        return s_prev, c + 1, "|HYSTERESIS"
    return ramped, 0, ""


def _step_full(s: int, c: int, last_excess: float, pcc: float, ebox: float,
               bat1: float, ramp: Callable[[int, int], Tuple[int, bool]]
               ) -> Tuple[int, int, float, str]:
    ebox_eff = max(ebox, float(P[s])) if s > 0 else 0.0
    excess = pcc + ebox_eff + bat1
    target = quantize(excess + MAX_GRID_DRAW)
    trace = "INSUFFICIENT_EXCESS" if target == 0 else "POWER_MATCHING"
    ramped, capped = ramp(target, s)
    if capped:
        trace += "|RAMP_LIMITED"
    drop_rate = (excess - last_excess) / 30.0 if last_excess > 0 else 0.0
    s_new, c_new, t2 = blocking_full(ramped, s, c, pcc, bat1, drop_rate)
    return s_new, c_new, excess, trace + t2          # neuer last_excess = excess


def step_full_literal(s, c, last_excess, pcc, ebox, bat1):
    """ESP-treu (State-Nummer-Ramp), 3 Eingänge."""
    return _step_full(s, c, last_excess, pcc, ebox, bat1, ramp_literal)


def step_full_rank(s, c, last_excess, pcc, ebox, bat1):
    """leistungsmonotone Ramp, 3 Eingänge."""
    return _step_full(s, c, last_excess, pcc, ebox, bat1, ramp_rank)


# ── Totband-Analyse (Last an, ebox_eff = P[s]) ───────────────────────────────
def hold_band(s: int) -> Tuple[float, float]:
    """pcc-Intervall, in dem State s gehalten wird (untere, obere Grenze)."""
    higher = [P[x] for x in range(8) if P[x] > P[s]]
    upper = (min(higher) - P[s] - MAX_GRID_DRAW) if higher else float("inf")
    lower = -MAX_GRID_DRAW                       # darunter: Abregeln (EMERGENCY/down)
    return lower, upper


# ═══════════════════════════════════════════════════════════════════════════
#   PHYSIKALISCHE ANLAGEN-GRENZEN
#   Sicherung 3×50A, PV 35 kWp, Bat1 5 kWh (Sofar), Bat2 30 kWh (EBox).
# ═══════════════════════════════════════════════════════════════════════════
V_PHASE     = 230.0
FUSE_A      = 50.0
FUSE_PHASES = 3
FUSE_MAX_W  = FUSE_PHASES * V_PHASE * FUSE_A     # 3×230V×50A = 34500 W Netzanschluss (Im-/Export)
PV_KWP_W    = 35000.0                             # installierte PV-Spitzenleistung
BAT1_KWH    = 5.0                                 # Sofar-Batterie (autark; SOC1 ungenutzt)
BAT2_KWH    = 30.0                                # EBox-Batterie bat2 (SOC2 steuert die Guards)
EBOX_MAX_W  = float(P[7])                         # 11400 W max EBox-Last (State 7)
CYCLE_S     = 60.0                                # Takt [s]


def clamp_inputs(pcc: float, ebox: float, bat1: float) -> Tuple[float, float, float, bool]:
    """Begrenzt Eingänge auf physikalisch Mögliches: pcc auf ±Sicherung,
    ebox auf [0, EBox-Max]. bat1 wird durchgereicht (kein C-Rate gegeben)."""
    pcc_c  = max(-FUSE_MAX_W, min(FUSE_MAX_W, pcc))
    ebox_c = max(0.0, min(EBOX_MAX_W, ebox))
    return pcc_c, ebox_c, bat1, (pcc_c != pcc or ebox_c != ebox)


def check_limits(pcc: float, ebox: float, bat1: float) -> list:
    """Liste physikalischer Verletzungen (Diagnose, ohne zu verändern)."""
    v = []
    if abs(pcc) > FUSE_MAX_W:
        v.append(f"pcc {pcc:.0f}W überschreitet Sicherung ±{FUSE_MAX_W:.0f}W")
    if not (0.0 <= ebox <= EBOX_MAX_W):
        v.append(f"ebox {ebox:.0f}W außerhalb [0, {EBOX_MAX_W:.0f}]W")
    if pcc > PV_KWP_W:
        v.append(f"Export {pcc:.0f}W über PV-Peak {PV_KWP_W:.0f}W")
    return v


def dsoc_per_cycle(power_w: float, kwh: float) -> float:
    """SOC-Änderung [%/Takt] bei konstanter Lade-/Entladeleistung."""
    return power_w * (CYCLE_S / 3600.0) / (kwh * 1000.0) * 100.0


def dsoc2_per_cycle(state: int) -> float:
    """SOC2-Anstieg [%/Takt] beim Laden von bat2 (30 kWh) im EBox-State."""
    return dsoc_per_cycle(P[state], BAT2_KWH)


def limits_report() -> None:
    print(f"  Sicherung 3×{FUSE_A:.0f}A @ {V_PHASE:.0f}V  →  {FUSE_MAX_W:.0f} W Netzanschluss")
    print(f"  EBox-Max (State7) {EBOX_MAX_W:.0f} W  =  {EBOX_MAX_W/FUSE_MAX_W*100:.0f}% der Sicherung,"
          f"  {EBOX_MAX_W/PV_KWP_W*100:.0f}% der PV-Peak")
    print(f"  DO4-Schwellen 20/22 kW  <  Sicherung {FUSE_MAX_W/1000:.1f} kW  (Headroom "
          f"{(FUSE_MAX_W-22000)/1000:.1f} kW)")
    print(f"  bat2 30 kWh @ State7: +{dsoc2_per_cycle(7):.2f}%/min  →  voll in "
          f"{100/dsoc2_per_cycle(7):.0f} min ({100/dsoc2_per_cycle(7)/60:.1f} h)")


# ═══════════════════════════════════════════════════════════════════════════
#                                TESTS
# ═══════════════════════════════════════════════════════════════════════════
def _run_tests() -> None:
    n = 0

    def check(cond, msg):
        nonlocal n
        assert cond, "FAIL: " + msg
        n += 1
        print(f"  ok  {msg}")

    print("1) Quantisierer Q(B):")
    check(quantize(0) == 0,        "Q(0)=0")
    check(quantize(2999) == 0,     "Q(2999)=0")
    check(quantize(3000) == 1,     "Q(3000)=1")
    check(quantize(3899) == 2,     "Q(3899)=2")
    check(quantize(3900) == 4,     "Q(3900)=4  (State4=3900W vor State3=6650W)")
    check(quantize(6649) == 4,     "Q(6649)=4")
    check(quantize(6650) == 3,     "Q(6650)=3")
    check(quantize(11400) == 7,    "Q(11400)=7")
    check(quantize(1e9) == 7,      "Q(∞)=7")

    print("2) Totband / Sweet-Spot halten (State 6, Last an ebox=7800, stabil c=5):")
    for f in (step_literal, step_rank):
        nm = f.__name__
        check(f(6, 5,  -800, 7800)[0] == 6, f"{nm}: pcc=-800 hält 6")
        check(f(6, 5,  2000, 7800)[0] == 6, f"{nm}: pcc=+2000 hält 6")
        check(f(6, 5,  -900, 7800)[0] == 6, f"{nm}: pcc=-900 (Rand) hält 6")
        check(f(6, 5,  -901, 7800)[0] == 5, f"{nm}: pcc=-901 → runter (5)")
        check(f(6, 5,  2700, 7800)[0] == 7, f"{nm}: pcc=+2700 → hoch (7)")
        check(f(6, 5,  2699, 7800)[0] == 6, f"{nm}: pcc=+2699 hält 6")
    lo, hi = hold_band(6)
    check((lo, hi) == (-900.0, 2700.0), f"hold_band(6) = (-900, 2700)")

    print("3) Up-Rampe max. 1 Stufe (aus State 0, riesiger Überschuss):")
    check(step_literal(0, 0, 20000, 0)[0] == 1, "literal: 0 → 1 (nicht 7)")
    check(step_rank(0, 0, 20000, 0)[0] == 1,    "rank:    0 → 1 (nicht 7)")

    print("4) DIVERGENZ literal vs. rank (aus State 2, Ziel-Leistung 6650W):")
    sl = step_literal(2, 5, 2200, 3650)
    sr = step_rank(2, 5, 2200, 3650)
    print(f"     literal: {sl}")
    print(f"     rank:    {sr}")
    check(sl[0] == 3, "literal springt 2 → 3  (State-Nummer 3>2, Cap 4 greift nicht)")
    check(sr[0] == 4, "rank  rampt  2 → 4  (genau eine Leistungsstufe)")
    check(sl[0] != sr[0], "→ beide Modelle weichen hier ab")

    print("5) Lastsprung-Kaskade (Wärmepumpe: pcc=-2190 konstant, Last folgt State):")
    s, c, traj = 6, 5, [6]
    for k in range(5):
        s, c, tr = step_rank(s, c, -2190.0, float(P[s]))   # ebox folgt commandiertem State
        traj.append(s)
    print(f"     Trajektorie (rank): {traj}")
    powers = [P[x] for x in traj]
    check(all(powers[i] >= powers[i + 1] for i in range(len(powers) - 1)),
          "Leistung monoton fallend (Last wird abgeworfen)")
    check(traj[-1] == 0, "endet bei State 0")
    check(traj.index(0) <= 4, "State 0 in ≤4 Takten erreicht")

    print("6) EMERGENCY_FORCE vs. STABILIZING (harter Bezug schaltet sofort runter):")
    # Übergang nach unten bei Last an erfordert pcc<-1200 ⇒ immer < -1020 ⇒ EMERGENCY.
    s_new, c_new, tr = step_rank(6, 0, -2000.0, 7800.0)   # c=0 (NICHT stabil)
    check("EMERGENCY_FORCE" in tr and s_new != 6,
          "trotz c=0 sofort runter via EMERGENCY_FORCE (kein STABILIZING-Warten)")

    print("7) VOLLES Modell mit bat1 — reaktivierte Guards (step_full_literal):")
    # 7a SWEET_SPOT_HOLD: Hochschalt-Versuch, |pcc|<160 und bat1>-310 → halten
    s, c, le, tr = step_full_literal(1, 5, 0.0, 50.0, 3000.0, 0.0)
    check(s == 1 and "SWEET_SPOT_HOLD" in tr, "SWEET_SPOT_HOLD: pcc=50,bat1=0 hält State 1")
    # 7b Gegenprobe: pcc groß → Sweet-Spot aus → hoch
    s, c, le, tr = step_full_literal(1, 5, 0.0, 500.0, 3000.0, 0.0)
    check(s == 2 and "SWEET_SPOT_HOLD" not in tr, "ohne Sweet-Spot (pcc=500): 1 → 2")
    # 7c TREND_BLOCK: Excess fällt schnell (drop_rate=(3500-5000)/30=-50 < -20)
    s, c, le, tr = step_full_literal(1, 5, 5000.0, 500.0, 3000.0, 0.0)
    check(s == 1 and "TREND_BLOCK" in tr, "TREND_BLOCK: drop_rate=-50 blockt Hochschalten")
    # 7d BAT_GUARD_BLOCK: bat1<-220 (Sofar entlädt), pcc>160
    s, c, le, tr = step_full_literal(1, 5, 0.0, 500.0, 3000.0, -150.0)
    check(s == 1 and "BAT_GUARD_BLOCK" in tr, "BAT_GUARD_BLOCK: bat1=-150 < -110 blockt Hochschalten")
    # 7e STABILIZING jetzt erreichbar: down, pcc>-1020 (kein Emergency), c<2, gap≥H
    s, c, le, tr = step_full_literal(6, 0, 0.0, -1000.0, 7800.0, -500.0)
    check(s == 6 and "STABILIZING" in tr, "STABILIZING: down geblockt (c=0), pcc=-1000>-1020")
    # 7f HYSTERESIS jetzt erreichbar: down mit kleinem Sprung (7100→6650, gap 450<505), c≥2
    s, c, le, tr = step_full_literal(5, 5, 0.0, -1000.0, 7100.0, -200.0)
    check(s == 5 and "HYSTERESIS" in tr, "HYSTERESIS: down 5→3 geblockt (gap 450<505)")

    print("8) Physikalische Grenzen (Sicherung 3×50A, PV 35kWp, bat1 5kWh, bat2 30kWh):")
    limits_report()
    check(FUSE_MAX_W == 34500.0, "Sicherung 3×50A×230V = 34500 W")
    check(EBOX_MAX_W < FUSE_MAX_W, "EBox-Max (11400W) < Sicherung (34500W)")
    check(EBOX_MAX_W < PV_KWP_W, "EBox-Max < PV-Peak (35kWp)")
    check(22000.0 < FUSE_MAX_W, "DO4 regelt Export ab (22kW) deutlich unter Sicherung")
    # Eingangs-Clamping
    pc, eb, b1, cl = clamp_inputs(40000.0, 15000.0, 0.0)
    check(cl and pc == FUSE_MAX_W and eb == EBOX_MAX_W,
          "clamp_inputs: pcc 40k→34500, ebox 15k→11400")
    pc, eb, b1, cl = clamp_inputs(-50000.0, -100.0, 0.0)
    check(pc == -FUSE_MAX_W and eb == 0.0, "clamp_inputs: pcc -50k→-34500, ebox -100→0")
    check(clamp_inputs(5000.0, 7800.0, 0.0)[3] is False, "clamp_inputs: gültige Werte unverändert")
    # check_limits
    check(len(check_limits(40000.0, 0.0, 0.0)) >= 1, "check_limits flaggt pcc über Sicherung")
    check(check_limits(5000.0, 7800.0, 0.0) == [], "check_limits: gültige Werte ohne Befund")
    # SOC-Dynamik
    check(abs(dsoc2_per_cycle(7) - 0.6333) < 1e-3, "bat2 @ State7 = +0.633 %/min")
    check(dsoc2_per_cycle(0) == 0.0, "bat2 @ State0 = 0 %/min")
    check(abs(dsoc_per_cycle(P[7], BAT1_KWH) - 3.8) < 1e-2, "bat1 (5kWh) @ 11.4kW = +3.8 %/min")

    print(f"\nAlle {n} Checks bestanden. ✅")
    print("\nHinweis: MAX_GRID_DRAW=900 < EMERGENCY_IMPORT=1020 ⇒ die Down-Schwelle (-G=-900 W)")
    print("liegt jetzt ÜBER der Emergency-Schwelle (-1020 W). Im Fenster -1020..-900 W schaltet")
    print("der Regler gedämpft runter (STABILIZING/HYSTERESIS) statt per EMERGENCY_FORCE —")
    print("anders als bei G=1200. step_full_* bleibt 1:1 zu fox2db_logic.h (ohne soc2-Guards).")


if __name__ == "__main__":
    _run_tests()
