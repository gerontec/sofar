#!/usr/bin/env python3
"""
make_fox2db_pdf.py — erzeugt fox2db_model.pdf mit mathematischer Beschreibung,
Formeln, Plots und einer Simulation des fox2db-Reglers.

Nutzt fox2db_model.py als Referenz (keine externen LaTeX-Abhängigkeiten,
matplotlib-mathtext genügt).  Aufruf:  python3 make_fox2db_pdf.py
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np

import fox2db_model as M

A4 = (8.27, 11.69)          # Hochformat [Zoll]
OUT = "fox2db_model.pdf"


# ── Hilfen ───────────────────────────────────────────────────────────────────
def text_page(pdf, title, blocks):
    """Eine Textseite: title + Liste von (kind, text). kind: 'h','p','m','mono'."""
    from matplotlib.lines import Line2D
    fig = plt.figure(figsize=A4)
    y = 0.95
    fig.text(0.08, y, title, fontsize=17, weight="bold", va="top")
    y -= 0.045
    fig.add_artist(Line2D([0.08, 0.92], [y, y], color="0.6", lw=0.8,
                          transform=fig.transFigure))
    y -= 0.028
    for kind, txt in blocks:
        if kind == "h":
            y -= 0.012
            fig.text(0.08, y, txt, fontsize=12.5, weight="bold", va="top"); y -= 0.032
        elif kind == "m":                       # zentrierte Formel (mathtext)
            fig.text(0.5, y, txt, fontsize=13, va="top", ha="center"); y -= 0.046
        elif kind == "mono":
            for line in txt.split("\n"):
                fig.text(0.10, y, line, fontsize=9.5, va="top", family="monospace")
                y -= 0.0215
            y -= 0.010
        else:                                   # 'p' Fließtext
            for line in _wrap(txt, 92):
                fig.text(0.08, y, line, fontsize=10.5, va="top"); y -= 0.0235
            y -= 0.012
    pdf.savefig(fig); plt.close(fig)


def _wrap(s, n):
    out, line = [], ""
    for w in s.split():
        if len(line) + len(w) + 1 > n:
            out.append(line); line = w
        else:
            line = (line + " " + w).strip()
    if line:
        out.append(line)
    return out


def simulate(pcc_series, s0=0, c0=0, bat1=0.0):
    """Last folgt dem commandierten State (ebox = P[s]); gibt States + excess zurück."""
    s, c, le = s0, c0, 0.0
    states, excess = [], []
    for pcc in pcc_series:
        ebox = float(M.P[s])
        s, c, le, _tr = M.step_full_rank(s, c, le, pcc, ebox, bat1)
        states.append(s); excess.append(le)
    return np.array(states), np.array(excess)


# ═══════════════════════════════════════════════════════════════════════════
def build():
    pdf = PdfPages(OUT)

    # ── Seite 1: Titel + Beschreibung ────────────────────────────────────────
    text_page(pdf, "fox2db — Regler-Modell (Sofar / Waveshare ESP32-S3)", [
        ("p", "Lastfolge-Überschussregler für eine Photovoltaik-Anlage mit zwei "
              "Batteriespeichern. Der Regler schaltet eine quantisierte elektrische "
              "Last (EBox-Heizstäbe) so, dass der Netzaustausch nahe Null bleibt "
              "(\"Sweet-Spot\"), ohne die autarke Sofar-Batterie zu stören."),
        ("h", "Klassifikation"),
        ("p", "Zeitdiskreter, nichtlinearer hybrider Automat (Mealy-Maschine mit "
              "Hysterese) — KEIN PID und KEIN LTI-System. Der Aktuator ist diskret "
              "(8 Leistungsstufen über 3 Relais-Bits), daher Totband + Ratenbegrenzung "
              "statt kontinuierlicher Stellgröße. Takt: 60 s."),
        ("h", "Ein- und Ausgänge"),
        ("mono",
         "Eingänge :  pcc   Netzaustausch        [W]  (+ = Einspeisung)\n"
         "            ebox  gemessene EBox-Last   [W]\n"
         "            bat1  Sofar-Batterieleistung[W]  (autark, nur gelesen)\n"
         "            (SOC1 wird NICHT verwendet; SOC2 steuert separate Guards)\n"
         "Zustand  :  s     Relais-State 0..7\n"
         "            c     Stabilisierungs-Zähler\n"
         "            E_-1  Überschuss des Vortakts (für Trend)\n"
         "Ausgang  :  s     ein State 0..7  ->  EBox-Leistung P[s]"),
        ("h", "Anlagen-Grenzen"),
        ("mono",
         f"Sicherung 3x50A @230V .... {M.FUSE_MAX_W:.0f} W Netzanschluss\n"
         f"PV-Spitzenleistung ....... {M.PV_KWP_W:.0f} W\n"
         f"EBox-Max (State 7) ....... {M.EBOX_MAX_W:.0f} W "
         f"(= {M.EBOX_MAX_W/M.FUSE_MAX_W*100:.0f}% Sicherung, {M.EBOX_MAX_W/M.PV_KWP_W*100:.0f}% PV)\n"
         f"Bat1 (Sofar) ............. {M.BAT1_KWH:.0f} kWh\n"
         f"Bat2 (EBox) .............. {M.BAT2_KWH:.0f} kWh "
         f"(@State7: +{M.dsoc2_per_cycle(7):.2f} %/min)"),
    ])

    # ── Seite 2: Mathematik ──────────────────────────────────────────────────
    text_page(pdf, "Mathematisches Modell", [
        ("h", "Leistungs-Map und Rang"),
        ("m", r"$P = (0,\,3000,\,3650,\,6650,\,3900,\,7100,\,7800,\,11400)\;\mathrm{W}$"),
        ("m", r"$\Pi\;\mathrm{(aufsteigend)} = (0,3000,3650,3900,6650,7100,7800,11400)$"),
        ("h", "Schritt-Rekursion  x_k=(s,c,E)"),
        ("m", r"$e_{\mathrm{eff}} = \max(e_k,\,P_{s_{k-1}})\quad (s_{k-1}>0)$"),
        ("m", r"$E_k = p_k + e_{\mathrm{eff}} + b_k$"),
        ("m", r"$\tau_k = Q(E_k+G)\;\;(0=\mathrm{unzureichend})$"),
        ("m", r"$Q(B)=\mathrm{groesster}\ \mathrm{State}\ s\ \mathrm{mit}\ P_s \leq B$"),
        ("h", "Hochrampe (max. eine Leistungsstufe / Takt)"),
        ("m", r"$\tilde\tau_k=\min(\tau_k,\;\hat{s}(\mathrm{rang}(s_{k-1})+1))$"),
        ("h", "Schalt-/Schutzlogik  (Reihenfolge = Priorität)"),
        ("mono",
         "EMERGENCY_FORCE :  pcc < -1020 (=-(G+120)) & runter -> sofort schalten\n"
         "SWEET_SPOT_HOLD :  hoch & |pcc|<160              -> halten\n"
         "TREND_BLOCK     :  hoch & dE/dt < -20 W/s       -> halten\n"
         "BAT_GUARD_BLOCK :  hoch & bat1 < -110 W         -> halten\n"
         "STABILIZING     :  runter & c < 2 Takte         -> halten\n"
         "HYSTERESIS      :  runter & |dP| < 505 W        -> halten"),
        ("h", "Konstanten"),
        ("m", r"$G=900,\;H=505,\;N=2,\;\mathrm{EMERG}=G+120\;\mathrm{[W]}$"),
        ("h", "Totband (Gleichgewicht, Last an  e_eff=P_s)"),
        ("m", r"$-G \;<\; p \;<\; \left(\Pi^{+}(s)-P_s\right)-G$"),
    ])

    # ── Seite 3: Leistungs-Map + Quantisierer ────────────────────────────────
    fig, (a1, a2) = plt.subplots(2, 1, figsize=A4)
    fig.subplots_adjust(left=0.12, right=0.95, top=0.93, bottom=0.07, hspace=0.30)

    a1.set_title("Leistungs-Map P[s] (nach Leistung sortiert)")
    order = M.SORTED
    a1.bar(range(8), [M.P[s] for s in order], color="#3b78c2")
    a1.set_xticks(range(8)); a1.set_xticklabels([f"S{s}" for s in order])
    for i, s in enumerate(order):
        a1.text(i, M.P[s] + 200, f"{M.P[s]}", ha="center", fontsize=8)
    a1.set_ylabel("Leistung [W]"); a1.set_ylim(0, 12500)
    a1.grid(axis="y", alpha=0.3)

    a2.set_title("Quantisierer  Q(B): höchster State mit P[s] ≤ B")
    B = np.linspace(0, 13000, 2000)
    yp = np.array([M.P[M.quantize(b)] for b in B])
    a2.step(B, yp, where="post", color="#c0392b", lw=1.6)
    for s in range(8):
        a2.axvline(M.P[s], color="0.8", lw=0.6)
    a2.plot(B, B, "k--", lw=0.6, alpha=0.5, label="P = B (Referenz)")
    a2.set_xlabel("Budget B = E + G  [W]"); a2.set_ylabel("gewählte Leistung P[Q(B)] [W]")
    a2.grid(alpha=0.3); a2.legend(loc="upper left", fontsize=8)
    pdf.savefig(fig); plt.close(fig)

    # ── Seite 4: Totband je State ────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=A4)
    fig.subplots_adjust(left=0.12, right=0.95, top=0.90, bottom=0.10)
    ax.set_title("Totband (Sweet-Spot): pcc-Bereich, in dem ein State gehalten wird\n"
                 "(Last an, ebox = P[s])", fontsize=13)
    states = [1, 2, 4, 3, 5, 6]
    for i, s in enumerate(states):
        lo, hi = M.hold_band(s)
        ax.barh(i, hi - lo, left=lo, height=0.55, color="#27ae60", alpha=0.75)
        ax.text(lo - 120, i, f"{lo:.0f}", va="center", ha="right", fontsize=8)
        ax.text(hi + 120, i, f"+{hi:.0f}", va="center", ha="left", fontsize=8)
    ax.axvline(0, color="k", lw=1.0)
    ax.set_yticks(range(len(states)))
    ax.set_yticklabels([f"State {s}  ({M.P[s]} W)" for s in states])
    ax.set_xlabel("Netzaustausch pcc [W]   (links = Bezug, rechts = Einspeisung)")
    ax.set_xlim(-2500, 4500); ax.grid(axis="x", alpha=0.3)
    ax.text(0.02, 0.02,
            "Unterkante immer -G = -900 W (Abregeln; hart erst bei -1020 = -(G+120)).\n"
            "Oberkante = nächste Leistungsstufe - P[s] - G (dann Hochschalten).",
            transform=ax.transAxes, fontsize=8.5, va="bottom",
            bbox=dict(boxstyle="round", fc="#f3f3f3", ec="0.7"))
    pdf.savefig(fig); plt.close(fig)

    # ── Seite 5: Simulation ──────────────────────────────────────────────────
    fig, (b1, b2) = plt.subplots(2, 1, figsize=A4)
    fig.subplots_adjust(left=0.12, right=0.90, top=0.93, bottom=0.07, hspace=0.32)

    # (a) Morgen-Hochlauf: pcc rampt hoch -> State klettert 1 Stufe/Takt
    n = 22
    pcc_up = np.concatenate([np.linspace(0, 16000, 14), np.full(n - 14, 16000)])
    s_up, _ = simulate(pcc_up, s0=0)
    t = np.arange(n)
    b1.set_title("(a) Hochlauf: Rampenbegrenzung — max. eine Leistungsstufe pro Takt")
    b1.step(t, [M.P[s] for s in s_up], where="post", color="#c0392b", lw=1.6, label="EBox P[s]")
    b1.set_ylabel("EBox-Leistung [W]", color="#c0392b")
    b1b = b1.twinx()
    b1b.plot(t, pcc_up, color="#3b78c2", lw=1.2, label="pcc")
    b1b.set_ylabel("pcc [W]", color="#3b78c2")
    b1.set_xlabel("Takt [min]"); b1.grid(alpha=0.3)

    # (b) Lastsprung (Wärmepumpe) im Sweet-Spot
    pcc_step = np.concatenate([np.zeros(5), np.full(8, -2500.0), np.zeros(9)])
    s_st, _ = simulate(pcc_step, s0=6, c0=5)
    t2 = np.arange(len(pcc_step))
    b2.set_title("(b) Lastsprung im Sweet-Spot: -2500 W ab Takt 5 (Wärmepumpe)")
    b2.step(t2, [M.P[s] for s in s_st], where="post", color="#c0392b", lw=1.6)
    b2.set_ylabel("EBox-Leistung [W]", color="#c0392b")
    b2b = b2.twinx()
    b2b.plot(t2, pcc_step, color="#3b78c2", lw=1.2)
    b2b.axhline(-M.MAX_GRID_DRAW, color="0.5", ls=":", lw=0.9)
    b2b.set_ylabel("pcc [W]", color="#3b78c2")
    b2.axvline(5, color="0.5", ls="--", lw=0.8)
    b2.set_xlabel("Takt [min]"); b2.grid(alpha=0.3)
    pdf.savefig(fig); plt.close(fig)

    # ── Seite 6: Validierung ─────────────────────────────────────────────────
    text_page(pdf, "Validierung gegen Realdaten", [
        ("p", "Die Referenz step_full_literal wurde one-step-ahead gegen die echten "
              "Entscheidungen des laufenden ESP (Tabelle pv_decision_log, version="
              "'waveshare') geprüft: pro Takt wird (state_from, pcc, bat1, ebox) "
              "eingespeist und der vorhergesagte Folge-State verglichen."),
        ("h", "Ergebnis (Messfenster 2026-06-10, Mittag)"),
        ("mono",
         "STATE-Treffer (one-step) : 55 / 55   (100.0 %)\n"
         "excess-Formel exakt      : 54 / 55   ( 98.2 %)\n"
         "Grund + Modifier exakt   : 54 / 55   ( 98.2 %)\n"
         "abgedeckte Entscheidungen: POWER_MATCHING, INSUFFICIENT_EXCESS,\n"
         "  RAMP_LIMITED, SWEET_SPOT_HOLD, TREND_BLOCK, BAT_GUARD_BLOCK,\n"
         "  EMERGENCY_FORCE, STABILIZING"),
        ("p", "Die eine Abweichung (12:25) ist ein Quantisierer-Randfall: eine ~2 W "
              "Rundungsdifferenz im Überschuss verschob das Ziel um eine Stufe, die "
              "sofort durch STABILIZING zurückgehalten wurde -> identischer End-State."),
        ("h", "Hinweis zur Implementierung"),
        ("p", "Der C-Code (fox2db_logic.h) vergleicht bei der Hochrampe über die "
              "State-NUMMER statt den Leistungs-RANG. Da P nicht monoton in der Nummer "
              "ist (State4=3900W < State3=6650W), weicht das in Randfällen vom "
              "rang-monotonen Modell ab. step_full_literal bildet den C-Code 1:1 ab; "
              "step_full_rank ist die saubere Form."),
        ("h", "Artefakte"),
        ("mono",
         "fox2db_logic.h     ESP-Steuerlogik (C++)\n"
         "fox2dbEasy.py      1:1 Python-Port (Schatten)\n"
         "fox2db_model.py    math. Referenz + Grenzen + 49 Selbsttests\n"
         "validate_model.py  DB-Validierung (one-step-ahead)"),
    ])

    d = pdf.infodict()
    d["Title"] = "fox2db Regler-Modell"
    d["Author"] = "fox2db"
    d["Subject"] = "Mathematisches Modell, Plots, Validierung"
    pdf.close()
    print(f"geschrieben: {OUT}")


if __name__ == "__main__":
    build()
