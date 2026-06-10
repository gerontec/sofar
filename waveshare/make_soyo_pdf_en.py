#!/usr/bin/env python3
"""
make_soyo_pdf_en.py — English edition of the Soyo discharge-controller document.
Run:  python3 make_soyo_pdf_en.py   ->  soyo_model_en.pdf
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np

import soyo_model as S
from make_fox2db_pdf import text_page, A4
from make_soyo_pdf import wcurve

OUT = "soyo_model_en.pdf"


def build():
    pdf = PdfPages(OUT)

    # ── Page 1 ───────────────────────────────────────────────────────────────
    text_page(pdf, "Soyo discharge controller (bat2 -> grid)", [
        ("p", "The Soyo is a grid-tie micro-inverter that discharges the EBox battery "
              "(bat2) into the house to keep grid import near zero — the counterpart to "
              "the EBox charger (fox2db). Setpoint w in [0,900] W, cycle 60 s, sent over "
              "RS485 every 3 s."),
        ("h", "Classification"),
        ("p", "Gated proportional controller with night feed-forward and saturation. "
              "P-term on grid import (target pcc=0), no I/D term. Five gates force w=0 "
              "(safety / no simultaneous charge+discharge)."),
        ("h", "Inputs and output"),
        ("mono",
         "Inputs :  pcc   grid exchange [W]  (< 0 = import)\n"
         "          st    EBox state 0..7    (charger state)\n"
         "          soc2  bat2 SOC [%]\n"
         "          ebox  measured EBox load [W]\n"
         "          h     hour (day/night)\n"
         "Output :  w     Soyo setpoint 0..900 W  (RS485 frame)"),
        ("h", "Constants"),
        ("mono",
         f"k_p (P gain) ............. {S.KP}\n"
         f"B_n (night base load) .... {S.B_NIGHT} W   (feed-forward)\n"
         f"B_d (day idle) ........... {S.B_DAY_IDLE} W\n"
         f"w_max (saturation) ....... {S.W_MAX} W\n"
         f"import threshold ......... {S.PCC_IMPORT_TH:.0f} W\n"
         f"surplus gate ............. {S.PCC_SURPLUS_TH:.0f} W\n"
         f"deep-discharge guard ..... soc2 < {S.SOC_MIN:.0f} %\n"
         f"night .................... h < {S.NIGHT_END} or h >= {S.NIGHT_START}\n"
         f"TX watchdog .............. setpoint > {S.TX_WATCHDOG_MS//1000} s old -> 0"),
    ])

    # ── Page 2 ───────────────────────────────────────────────────────────────
    text_page(pdf, "Mathematical model", [
        ("h", "Gate cascade — w = 0 if any condition holds"),
        ("mono",
         "G1  stale        (inverter data > 3 min old)\n"
         "G2  st != 0      (EBox charging -> no simultaneous discharge)\n"
         "G3  0 <= soc2 < 9   (deep-discharge guard)\n"
         "G4  ebox > 200   (bat2 charging)\n"
         "G5  pcc  > 200   (PV surplus -> spare bat2)"),
        ("h", "Active law (gate open)"),
        ("m", r"$w=\mathrm{sat}_{[0,900]}\!\left(k_p\,(-\mathrm{pcc})+B_n\,\mathbf{1}[\mathrm{night}]\right)"
              r"\quad \mathrm{pcc}<-100$"),
        ("m", r"$w=B_n\,\mathbf{1}[\mathrm{night}]+B_d\,\mathbf{1}[\mathrm{day}]"
              r"\quad -100\leq \mathrm{pcc}\leq 200$"),
        ("p", "The P-term with k_p~1 is a unity feed-forward cancellation: inject as "
              "much as is currently being imported. B_n=468 W is the night base load as "
              "feed-forward. The dead-band [-100, 200] W prevents hunting around zero."),
        ("h", "Constants"),
        ("m", r"$k_p=1.01,\;B_n=468,\;B_d=10,\;w_{\max}=900\;\mathrm{[W]}$"),
        ("h", "TX watchdog (RS485, every 3 s)"),
        ("m", r"$w_{tx}=w\cdot\mathbf{1}[\Delta t \leq 90\,\mathrm{s}]$"),
        ("p", "If the setpoint is not refreshed in time (controller failure), the RS485 "
              "side sends 0 W -> the Soyo stops discharging (fail-safe)."),
        ("h", "Protocol frame (8 bytes)"),
        ("mono",
         "24 56 00 21  ph pl  80  crc      ph=(w>>8)&0xFF, pl=w&0xFF\n"
         "crc = (264 - ph - pl) & 0xFF"),
    ])

    # ── Page 3: static characteristic ────────────────────────────────────────
    fig, ax = plt.subplots(figsize=A4)
    fig.subplots_adjust(left=0.12, right=0.93, top=0.90, bottom=0.42)
    pcc = np.linspace(-1000, 400, 2000)
    ax.axvspan(-100, 200, color="0.92", label="dead-band")
    ax.axvspan(200, 400, color="#f6d7d7", alpha=0.6, label="PV gate (w=0)")
    ax.plot(pcc, wcurve(pcc, 2), color="#2c3e8c", lw=2.0, label="night (h=2)")
    ax.plot(pcc, wcurve(pcc, 12), color="#c0392b", lw=2.0, label="day (h=12)")
    ax.axhline(S.W_MAX, color="0.5", ls=":", lw=1.0)
    ax.text(-980, S.W_MAX + 12, f"saturation w_max = {S.W_MAX} W", fontsize=8, color="0.4")
    ax.axhline(S.B_NIGHT, color="#2c3e8c", ls=":", lw=0.8)
    ax.text(60, S.B_NIGHT + 12, f"B_n={S.B_NIGHT}", fontsize=8, color="#2c3e8c")
    ax.axvline(-100, color="0.6", lw=0.7); ax.axvline(200, color="0.6", lw=0.7)
    ax.set_title("Static characteristic  w(pcc): Soyo setpoint vs. grid exchange")
    ax.set_xlabel("grid exchange pcc [W]   (left = import, right = export)")
    ax.set_ylabel("Soyo setpoint w [W]")
    ax.set_xlim(-1000, 400); ax.set_ylim(-30, 980)
    ax.grid(alpha=0.3); ax.legend(loc="upper right", fontsize=9)
    fig.text(0.12, 0.34,
             "Regions (left to right):\n"
             "  - pcc < -100 W : proportional, w = k_p*|pcc| (+B_n at night), up to saturation 900 W\n"
             "  - -100..200 W  : dead-band - only base load (468 W night) resp. 10 W day\n"
             "  - pcc > 200 W  : PV surplus -> Soyo off (w=0), spare bat2\n\n"
             "The curve is the complete (memoryless) definition of the P-controller; the "
             "gates\nG1-G4 (stale / EBox on / SOC<9% / bat2 charging) additionally force it to 0.",
             fontsize=9.5, va="top", family="monospace")
    pdf.savefig(fig); plt.close(fig)

    # ── Page 4: complementarity + response ───────────────────────────────────
    fig, (c1, c2) = plt.subplots(2, 1, figsize=A4)
    fig.subplots_adjust(left=0.12, right=0.90, top=0.93, bottom=0.07, hspace=0.40)
    c1.set_title("(a) Complementarity: Soyo (discharge) vs. EBox charger")
    c1.axvspan(-1500, -100, color="#c0392b", alpha=0.25)
    c1.axvspan(-100, 200, color="0.85")
    c1.axvspan(1200, 4000, color="#2980b9", alpha=0.25)
    c1.axvline(0, color="k", lw=1.0)
    c1.text(-800, 0.5, "SOYO discharges bat2\nw = k_p*|pcc| (<=900 W)", ha="center", va="center", fontsize=9)
    c1.text(50, 0.5, "dead-band\n(idle)", ha="center", va="center", fontsize=8)
    c1.text(2600, 0.5, "EBOX charges bat2\nstate up (<=11400 W)", ha="center", va="center", fontsize=9)
    c1.set_xlim(-1500, 4000); c1.set_ylim(0, 1); c1.set_yticks([])
    c1.set_xlabel("grid exchange pcc [W]   (import < pcc=0 > export)")

    pcc_tr = np.array([-150, -150, -300, -800, -1500, -1500, -800, -300, 300, 300, -150, -150.0])
    w_tr = wcurve(pcc_tr, 2)
    t = np.arange(len(pcc_tr))
    c2.set_title("(b) Controller response to a measured import profile (night, h=2)")
    c2.step(t, w_tr, where="post", color="#2c3e8c", lw=1.8)
    c2.axhline(S.W_MAX, color="0.5", ls=":", lw=0.8)
    c2.set_ylabel("Soyo w [W]", color="#2c3e8c"); c2.set_ylim(-30, 980)
    c2b = c2.twinx()
    c2b.plot(t, pcc_tr, color="#7f8c8d", lw=1.2)
    c2b.axhline(0, color="0.7", lw=0.6); c2b.set_ylabel("pcc [W]", color="#7f8c8d")
    c2.set_xlabel("cycle [min]"); c2.grid(alpha=0.3)
    c2.text(0.02, 0.95,
            "small import -> proportional + B_n;  large import -> saturation 900 W;\n"
            "export (pcc>200) -> Soyo off.",
            transform=c2.transAxes, fontsize=8, va="top",
            bbox=dict(boxstyle="round", fc="#f3f3f3", ec="0.7"))
    pdf.savefig(fig); plt.close(fig)

    # ── Page 5 ───────────────────────────────────────────────────────────────
    text_page(pdf, "Classification & artifacts", [
        ("h", "Control-theoretic character"),
        ("p", "Pure P-controller on the import error e=-pcc (target pcc=0) with gain "
              "k_p~1 (quasi dead-beat per cycle), feed-forward night base load, "
              "saturation and dead-band. No integrator -> no windup; the controller "
              "relies on re-measuring pcc every 60 s cycle."),
        ("h", "Interplay with the EBox charger"),
        ("p", "Both controllers tile the pcc axis around 0 and are mutually exclusive: "
              "the gates st!=0, ebox>200 and pcc>200 prevent bat2 from charging and "
              "discharging at the same time. Soyo covers small/medium loads (base load, "
              "<=900 W); the EBox charger absorbs large PV surplus."),
        ("h", "Limits"),
        ("p", "Due to saturation at 900 W the Soyo covers only base load; larger "
              "consumers are carried by the Sofar battery (bat1) and the grid. The night "
              "feed-forward of 468 W damps hunting at the dead-band edge at night."),
        ("h", "Artifacts"),
        ("mono",
         "sofar_waveshare.yaml   Soyo calculation (ESPHome, interval 60s + RS485)\n"
         "soyo_model.py          math reference + self-tests\n"
         "make_soyo_pdf*.py      this document"),
    ])

    d = pdf.infodict()
    d["Title"] = "Soyo discharge controller - model"
    d["Subject"] = "Mathematical model, characteristic, complementarity"
    pdf.close()
    print(f"written: {OUT}")


if __name__ == "__main__":
    build()
