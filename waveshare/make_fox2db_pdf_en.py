#!/usr/bin/env python3
"""
make_fox2db_pdf_en.py — English edition of the fox2db controller model document.
Reuses text_page()/A4/simulate() from make_fox2db_pdf.py and fox2db_model.py.
Run:  python3 make_fox2db_pdf_en.py   ->  fox2db_model_en.pdf
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np

import fox2db_model as M
from make_fox2db_pdf import text_page, A4, simulate

OUT = "fox2db_model_en.pdf"


def build():
    pdf = PdfPages(OUT)

    # ── Page 1: overview ─────────────────────────────────────────────────────
    text_page(pdf, "fox2db — controller model (Sofar / Waveshare ESP32-S3)", [
        ("p", "Load-following PV-surplus controller for a solar plant with two battery "
              "stores. It switches a quantized electrical load (EBox) so that the grid "
              "exchange stays near zero (\"sweet spot\"), without disturbing the "
              "autonomous Sofar battery."),
        ("h", "Classification"),
        ("p", "Discrete-time, nonlinear hybrid automaton (Mealy machine with hysteresis) "
              "— NOT a PID and NOT an LTI system. The actuator is discrete (8 power "
              "levels via 3 relay bits), hence dead-band + rate limiting instead of a "
              "continuous control signal. Cycle: 60 s."),
        ("h", "Inputs and outputs"),
        ("mono",
         "Inputs :  pcc   grid exchange [W]   (+ = export)\n"
         "          ebox  measured EBox load [W]\n"
         "          bat1  Sofar battery power [W]  (autonomous, read-only)\n"
         "          (SOC1 is NOT used; SOC2 drives separate guards)\n"
         "State  :  s     relay state 0..7\n"
         "          c     stabilization counter\n"
         "          E_-1  previous cycle surplus (for trend)\n"
         "Output :  s     one state 0..7  ->  EBox power P[s]"),
        ("h", "Plant limits"),
        ("mono",
         f"fuse 3x50A @230V ......... {M.FUSE_MAX_W:.0f} W grid connection\n"
         f"PV peak power ............ {M.PV_KWP_W:.0f} W\n"
         f"EBox max (state 7) ....... {M.EBOX_MAX_W:.0f} W "
         f"(= {M.EBOX_MAX_W/M.FUSE_MAX_W*100:.0f}% fuse, {M.EBOX_MAX_W/M.PV_KWP_W*100:.0f}% PV)\n"
         f"Bat1 (Sofar) ............. {M.BAT1_KWH:.0f} kWh\n"
         f"Bat2 (EBox) .............. {M.BAT2_KWH:.0f} kWh "
         f"(@state7: +{M.dsoc2_per_cycle(7):.2f} %/min)"),
    ])

    # ── Page 2: math ─────────────────────────────────────────────────────────
    text_page(pdf, "Mathematical model", [
        ("h", "Power map and rank"),
        ("m", r"$P = (0,\,3000,\,3650,\,6650,\,3900,\,7100,\,7800,\,11400)\;\mathrm{W}$"),
        ("m", r"$\Pi\;\mathrm{(ascending)} = (0,3000,3650,3900,6650,7100,7800,11400)$"),
        ("h", "Step recursion  x_k=(s,c,E)"),
        ("m", r"$e_{\mathrm{eff}} = \max(e_k,\,P_{s_{k-1}})\quad (s_{k-1}>0)$"),
        ("m", r"$E_k = p_k + e_{\mathrm{eff}} + b_k$"),
        ("m", r"$\tau_k = Q(E_k+G)\;\;(0=\mathrm{insufficient})$"),
        ("m", r"$Q(B)=\mathrm{highest}\ \mathrm{state}\ s\ \mathrm{with}\ P_s \leq B$"),
        ("h", "Up-ramp (max. one power step per cycle)"),
        ("m", r"$\tilde\tau_k=\min(\tau_k,\;\hat{s}(\mathrm{rank}(s_{k-1})+1))$"),
        ("h", "Switching / protection logic  (order = priority)"),
        ("mono",
         "EMERGENCY_FORCE :  pcc < -1020 (=-(G+120)) & down -> switch now\n"
         "SWEET_SPOT_HOLD :  up & |pcc|<160              -> hold\n"
         "TREND_BLOCK     :  up & dE/dt < -20 W/s        -> hold\n"
         "BAT_GUARD_BLOCK :  up & bat1 < -110 W          -> hold\n"
         "STABILIZING     :  down & c < 2 cycles         -> hold\n"
         "HYSTERESIS      :  down & |dP| < 505 W         -> hold"),
        ("h", "Constants"),
        ("m", r"$G=900,\;H=505,\;N=2,\;\mathrm{EMERG}=G+120\;\mathrm{[W]}$"),
        ("h", "Dead-band (equilibrium, load on  e_eff=P_s)"),
        ("m", r"$-G \;<\; p \;<\; \left(\Pi^{+}(s)-P_s\right)-G$"),
    ])

    # ── Page 3: power map + quantizer ────────────────────────────────────────
    fig, (a1, a2) = plt.subplots(2, 1, figsize=A4)
    fig.subplots_adjust(left=0.12, right=0.95, top=0.93, bottom=0.07, hspace=0.30)
    a1.set_title("Power map P[s] (sorted by power)")
    order = M.SORTED
    a1.bar(range(8), [M.P[s] for s in order], color="#3b78c2")
    a1.set_xticks(range(8)); a1.set_xticklabels([f"S{s}" for s in order])
    for i, s in enumerate(order):
        a1.text(i, M.P[s] + 200, f"{M.P[s]}", ha="center", fontsize=8)
    a1.set_ylabel("power [W]"); a1.set_ylim(0, 12500); a1.grid(axis="y", alpha=0.3)
    a2.set_title("Quantizer  Q(B): highest state with P[s] <= B")
    B = np.linspace(0, 13000, 2000)
    a2.step(B, [M.P[M.quantize(b)] for b in B], where="post", color="#c0392b", lw=1.6)
    for s in range(8):
        a2.axvline(M.P[s], color="0.8", lw=0.6)
    a2.plot(B, B, "k--", lw=0.6, alpha=0.5, label="P = B (reference)")
    a2.set_xlabel("budget B = E + G  [W]"); a2.set_ylabel("chosen power P[Q(B)] [W]")
    a2.grid(alpha=0.3); a2.legend(loc="upper left", fontsize=8)
    pdf.savefig(fig); plt.close(fig)

    # ── Page 4: dead-band per state ──────────────────────────────────────────
    fig, ax = plt.subplots(figsize=A4)
    fig.subplots_adjust(left=0.12, right=0.95, top=0.90, bottom=0.10)
    ax.set_title("Dead-band (sweet spot): pcc range in which a state is held\n"
                 "(load on, ebox = P[s])", fontsize=13)
    states = [1, 2, 4, 3, 5, 6]
    for i, s in enumerate(states):
        lo, hi = M.hold_band(s)
        ax.barh(i, hi - lo, left=lo, height=0.55, color="#27ae60", alpha=0.75)
        ax.text(lo - 120, i, f"{lo:.0f}", va="center", ha="right", fontsize=8)
        ax.text(hi + 120, i, f"+{hi:.0f}", va="center", ha="left", fontsize=8)
    ax.axvline(0, color="k", lw=1.0)
    ax.set_yticks(range(len(states)))
    ax.set_yticklabels([f"State {s}  ({M.P[s]} W)" for s in states])
    ax.set_xlabel("grid exchange pcc [W]   (left = import, right = export)")
    ax.set_xlim(-2500, 4500); ax.grid(axis="x", alpha=0.3)
    ax.text(0.02, 0.02,
            "Lower edge always -G = -900 W (shed; hard force only at -1020 = -(G+120)).\n"
            "Upper edge = next power level - P[s] - G (then ramp up).",
            transform=ax.transAxes, fontsize=8.5, va="bottom",
            bbox=dict(boxstyle="round", fc="#f3f3f3", ec="0.7"))
    pdf.savefig(fig); plt.close(fig)

    # ── Page 5: simulation ───────────────────────────────────────────────────
    fig, (b1, b2) = plt.subplots(2, 1, figsize=A4)
    fig.subplots_adjust(left=0.12, right=0.90, top=0.93, bottom=0.07, hspace=0.32)
    n = 22
    pcc_up = np.concatenate([np.linspace(0, 16000, 14), np.full(n - 14, 16000)])
    s_up, _ = simulate(pcc_up, s0=0)
    t = np.arange(n)
    b1.set_title("(a) Ramp-up: rate limiting — max. one power step per cycle")
    b1.step(t, [M.P[s] for s in s_up], where="post", color="#c0392b", lw=1.6)
    b1.set_ylabel("EBox power [W]", color="#c0392b")
    b1b = b1.twinx(); b1b.plot(t, pcc_up, color="#3b78c2", lw=1.2)
    b1b.set_ylabel("pcc [W]", color="#3b78c2")
    b1.set_xlabel("cycle [min]"); b1.grid(alpha=0.3)

    pcc_step = np.concatenate([np.zeros(5), np.full(8, -2500.0), np.zeros(9)])
    s_st, _ = simulate(pcc_step, s0=6, c0=5)
    t2 = np.arange(len(pcc_step))
    b2.set_title("(b) Load step in the sweet spot: -2500 W from cycle 5 (heat pump)")
    b2.step(t2, [M.P[s] for s in s_st], where="post", color="#c0392b", lw=1.6)
    b2.set_ylabel("EBox power [W]", color="#c0392b")
    b2b = b2.twinx(); b2b.plot(t2, pcc_step, color="#3b78c2", lw=1.2)
    b2b.axhline(-M.MAX_GRID_DRAW, color="0.5", ls=":", lw=0.9)
    b2b.set_ylabel("pcc [W]", color="#3b78c2")
    b2.axvline(5, color="0.5", ls="--", lw=0.8)
    b2.set_xlabel("cycle [min]"); b2.grid(alpha=0.3)
    pdf.savefig(fig); plt.close(fig)

    # ── Page 6: validation ───────────────────────────────────────────────────
    text_page(pdf, "Validation against real data", [
        ("p", "The reference step_full_literal was checked one-step-ahead against the "
              "running ESP's actual decisions (table pv_decision_log, version="
              "'waveshare'): each cycle (state_from, pcc, bat1, ebox) is fed in and the "
              "predicted next state compared."),
        ("h", "Result (window 2026-06-10, midday; firmware before this tuning)"),
        ("mono",
         "one-step state match  : 55 / 55   (100.0 %)\n"
         "excess formula exact  : 54 / 55   ( 98.2 %)\n"
         "reason + modifier      : 54 / 55   ( 98.2 %)\n"
         "covered decisions: POWER_MATCHING, INSUFFICIENT_EXCESS,\n"
         "  RAMP_LIMITED, SWEET_SPOT_HOLD, TREND_BLOCK, BAT_GUARD_BLOCK,\n"
         "  EMERGENCY_FORCE, STABILIZING"),
        ("p", "Re-validate against post-tuning rows only (do not mix parameter sets)."),
        ("h", "Implementation note"),
        ("p", "The C code (fox2db_logic.h) compares by STATE NUMBER for the up-ramp "
              "rather than the power RANK. Since P is not monotone in the number "
              "(state4=3900W < state3=6650W), this deviates in edge cases from the "
              "rank-monotone model. step_full_literal mirrors the C code 1:1; "
              "step_full_rank is the clean form."),
        ("h", "Artifacts"),
        ("mono",
         "fox2db_logic.h     ESP control logic (C++)\n"
         "fox2dbEasy.py      1:1 Python port (shadow)\n"
         "fox2db_model.py    math reference + limits + self-tests\n"
         "validate_model.py  DB validation (one-step-ahead)"),
    ])

    d = pdf.infodict()
    d["Title"] = "fox2db controller model"
    d["Subject"] = "Mathematical model, plots, validation"
    pdf.close()
    print(f"written: {OUT}")


if __name__ == "__main__":
    build()
