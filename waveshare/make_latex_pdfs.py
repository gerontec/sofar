#!/usr/bin/env python3
"""
make_latex_pdfs.py — erzeugt die Modell-PDFs mit echtem LaTeX (pdflatex):
Text + Formeln via LaTeX/amsmath, Plots als eingebundene Vektor-PDFs.

Outputs (in diesem Verzeichnis):
  fox2db_model.pdf, fox2db_model_en.pdf, soyo_model.pdf, soyo_model_en.pdf

Run:  python3 make_latex_pdfs.py
Benötigt: pdflatex, matplotlib.
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import subprocess, shutil, os
from pathlib import Path

import fox2db_model as M
from make_fox2db_pdf import simulate
import soyo_model as S
from make_soyo_pdf import wcurve

HERE  = Path(__file__).parent
BUILD = Path("/tmp/latexbuild")


# ════════════════════════════════════════════════════════════════════════════
#  PLOTS  (matplotlib -> Vektor-PDF, schlichte Textbeschriftung)
# ════════════════════════════════════════════════════════════════════════════
def fx_maps(p):
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(9.2, 3.4))
    order = M.SORTED
    a1.bar(range(8), [M.P[s] for s in order], color="#3b78c2")
    a1.set_xticks(range(8)); a1.set_xticklabels([f"S{s}" for s in order], fontsize=8)
    a1.set_title("Power map P[s] (sorted by power)", fontsize=9)
    a1.set_ylabel("power [W]"); a1.grid(axis="y", alpha=0.3); a1.tick_params(labelsize=8)
    B = np.linspace(0, 13000, 2000)
    a2.step(B, [M.P[M.quantize(b)] for b in B], where="post", color="#c0392b", lw=1.5)
    a2.plot(B, B, "k--", lw=0.6, alpha=0.5)
    a2.set_title("Quantizer Q(B): highest state with P[s] <= B", fontsize=9)
    a2.set_xlabel("budget B = E + G  [W]"); a2.set_ylabel("P[Q(B)] [W]")
    a2.grid(alpha=0.3); a2.tick_params(labelsize=8)
    fig.tight_layout(); fig.savefig(p); plt.close(fig)


def fx_deadband(p):
    fig, ax = plt.subplots(figsize=(9.2, 3.6))
    states = [1, 2, 4, 3, 5, 6]
    for i, s in enumerate(states):
        lo, hi = M.hold_band(s)
        ax.barh(i, hi - lo, left=lo, height=0.55, color="#27ae60", alpha=0.78)
        ax.text(lo - 120, i, f"{lo:.0f}", va="center", ha="right", fontsize=7)
        ax.text(hi + 120, i, f"+{hi:.0f}", va="center", ha="left", fontsize=7)
    ax.axvline(0, color="k", lw=1.0)
    ax.set_yticks(range(len(states)))
    ax.set_yticklabels([f"State {s} ({M.P[s]}W)" for s in states], fontsize=8)
    ax.set_xlabel("grid exchange pcc [W]  (left = import, right = export)")
    ax.set_xlim(-2500, 4500); ax.grid(axis="x", alpha=0.3); ax.tick_params(labelsize=8)
    fig.tight_layout(); fig.savefig(p); plt.close(fig)


def fx_sim(p):
    fig, (b1, b2) = plt.subplots(1, 2, figsize=(9.2, 3.4))
    n = 22
    pcc_up = np.concatenate([np.linspace(0, 16000, 14), np.full(n - 14, 16000)])
    s_up, _ = simulate(pcc_up, s0=0); t = np.arange(n)
    b1.step(t, [M.P[s] for s in s_up], where="post", color="#c0392b", lw=1.5)
    b1.set_title("(a) ramp-up: one step per cycle", fontsize=9)
    b1.set_ylabel("EBox [W]", color="#c0392b"); b1.set_xlabel("cycle [min]")
    b1b = b1.twinx(); b1b.plot(t, pcc_up, color="#3b78c2", lw=1.0); b1b.set_ylabel("pcc [W]", color="#3b78c2")
    b1.grid(alpha=0.3); b1.tick_params(labelsize=8); b1b.tick_params(labelsize=8)
    pcc_step = np.concatenate([np.zeros(5), np.full(8, -2500.0), np.zeros(9)])
    s_st, _ = simulate(pcc_step, s0=6, c0=5); t2 = np.arange(len(pcc_step))
    b2.step(t2, [M.P[s] for s in s_st], where="post", color="#c0392b", lw=1.5)
    b2.set_title("(b) load step -2500W (heat pump)", fontsize=9)
    b2.set_ylabel("EBox [W]", color="#c0392b"); b2.set_xlabel("cycle [min]")
    b2b = b2.twinx(); b2b.plot(t2, pcc_step, color="#3b78c2", lw=1.0); b2b.set_ylabel("pcc [W]", color="#3b78c2")
    b2.axvline(5, color="0.5", ls="--", lw=0.8); b2.grid(alpha=0.3)
    b2.tick_params(labelsize=8); b2b.tick_params(labelsize=8)
    fig.tight_layout(); fig.savefig(p); plt.close(fig)


def so_char(p):
    fig, ax = plt.subplots(figsize=(9.2, 4.0))
    pcc = np.linspace(-1000, 400, 2000)
    ax.axvspan(-100, 200, color="0.92"); ax.axvspan(200, 400, color="#f6d7d7", alpha=0.6)
    ax.plot(pcc, wcurve(pcc, 2), color="#2c3e8c", lw=2.0, label="night (h=2)")
    ax.plot(pcc, wcurve(pcc, 12), color="#c0392b", lw=2.0, label="day (h=12)")
    ax.axhline(S.W_MAX, color="0.5", ls=":", lw=0.8); ax.axhline(S.B_NIGHT, color="#2c3e8c", ls=":", lw=0.7)
    ax.axvline(-100, color="0.6", lw=0.6); ax.axvline(200, color="0.6", lw=0.6)
    ax.set_title("Static characteristic w(pcc)", fontsize=9)
    ax.set_xlabel("grid exchange pcc [W]  (left=import)"); ax.set_ylabel("Soyo w [W]")
    ax.set_xlim(-1000, 400); ax.set_ylim(-30, 980); ax.grid(alpha=0.3)
    ax.legend(loc="upper right", fontsize=8); ax.tick_params(labelsize=8)
    fig.tight_layout(); fig.savefig(p); plt.close(fig)


def so_resp(p):
    fig, (c1, c2) = plt.subplots(1, 2, figsize=(9.2, 3.4))
    c1.axvspan(-1500, -100, color="#c0392b", alpha=0.25); c1.axvspan(-100, 200, color="0.85")
    c1.axvspan(1200, 4000, color="#2980b9", alpha=0.25); c1.axvline(0, color="k", lw=1.0)
    c1.text(-800, 0.5, "SOYO\ndischarge", ha="center", va="center", fontsize=8)
    c1.text(2600, 0.5, "EBOX\ncharge", ha="center", va="center", fontsize=8)
    c1.set_xlim(-1500, 4000); c1.set_ylim(0, 1); c1.set_yticks([])
    c1.set_title("(a) complementarity", fontsize=9); c1.set_xlabel("pcc [W]"); c1.tick_params(labelsize=8)
    pcc_tr = np.array([-150, -150, -300, -800, -1500, -1500, -800, -300, 300, 300, -150, -150.0])
    t = np.arange(len(pcc_tr))
    c2.step(t, wcurve(pcc_tr, 2), where="post", color="#2c3e8c", lw=1.6)
    c2.set_title("(b) response (night)", fontsize=9); c2.set_ylabel("Soyo w [W]", color="#2c3e8c")
    c2.set_xlabel("cycle [min]"); c2.set_ylim(-30, 980)
    c2b = c2.twinx(); c2b.plot(t, pcc_tr, color="#7f8c8d", lw=1.0); c2b.set_ylabel("pcc [W]", color="#7f8c8d")
    c2.grid(alpha=0.3); c2.tick_params(labelsize=8); c2b.tick_params(labelsize=8)
    fig.tight_layout(); fig.savefig(p); plt.close(fig)


# ════════════════════════════════════════════════════════════════════════════
#  LaTeX
# ════════════════════════════════════════════════════════════════════════════
def preamble(lang):
    return r"""\documentclass[11pt,a4paper]{article}
\usepackage[T1]{fontenc}
\usepackage[utf8]{inputenc}
\usepackage[%s]{babel}
\usepackage{amsmath,amssymb}
\usepackage{graphicx}
\usepackage{booktabs}
\usepackage{caption}
\usepackage[margin=2.2cm]{geometry}
\usepackage{parskip}
\usepackage{xcolor}
\usepackage[colorlinks=true,urlcolor=blue]{hyperref}
\setlength{\parindent}{0pt}
\renewcommand{\arraystretch}{1.15}
""" % ("ngerman" if lang == "de" else "english")


FX_DE = preamble("de") + r"""
\begin{document}
\begin{center}{\LARGE\bfseries fox2db --- Regler-Modell}\\[2pt]
{\large Sofar / Waveshare ESP32-S3 6CH}\end{center}\vspace{4pt}

Lastfolge-Überschussregler für eine PV-Anlage mit zwei Batteriespeichern. Er schaltet eine
\emph{quantisierte} elektrische Last (EBox) so, dass der Netzaustausch nahe Null bleibt
(``Sweet-Spot''), ohne die autarke Sofar-Batterie zu stören.

\section*{Klassifikation}
Zeitdiskreter, nichtlinearer \textbf{hybrider Automat} (Mealy-Maschine mit Hysterese) ---
\textbf{kein PID, kein LTI-System}. Der Aktuator ist diskret (8 Leistungsstufen über 3 Relais-Bits),
daher Totband + Ratenbegrenzung statt kontinuierlicher Stellgröße. Takt: 60\,s.

\section*{Mathematisches Modell}
Leistungs-Map und (leistungssortierter) Rang:
\[ P = (0,\,3000,\,3650,\,6650,\,3900,\,7100,\,7800,\,11400)\ \text{W} \]
\[ \Pi_{\text{aufst.}} = (0,3000,3650,3900,6650,7100,7800,11400) \]
Schritt-Rekursion, Zustand $x_k=(s,c,E)$:
\[ e_{\text{eff}} = \max\!\big(e_k,\ P_{s_{k-1}}\big)\quad (s_{k-1}>0), \qquad E_k = p_k + e_{\text{eff}} + b_k \]
\[ \tau_k = \begin{cases} 0 & E_k < E_{\min}\\[2pt] Q(E_k+G) & \text{sonst} \end{cases}
\qquad Q(B)=\max\{\, s : P_s \le B \,\} \]
Hochrampe (max.\ eine Leistungsstufe pro Takt):
\[ \tilde{\tau}_k = \min\nolimits_{\text{rang}}\big(\tau_k,\ \hat{s}(\operatorname{rang}(s_{k-1})+1)\big) \]
Schalt-/Schutzlogik (Reihenfolge = Priorität):
\begin{center}\small
\begin{tabular}{@{}lll@{}}
\toprule
Regel & Bedingung & Wirkung\\\midrule
\texttt{EMERGENCY\_FORCE} & $\mathrm{pcc} < -1020$ (= $-(G{+}120)$) und runter & sofort schalten\\
\texttt{SWEET\_SPOT\_HOLD} & hoch und $|\mathrm{pcc}|<160$ & halten\\
\texttt{TREND\_BLOCK}      & hoch und $\dot E < -20$\,W/s & halten\\
\texttt{BAT\_GUARD\_BLOCK} & hoch und $b_1 < -110$\,W & halten\\
\texttt{STABILIZING}       & runter und $c < 2$ Takte & halten\\
\texttt{HYSTERESIS}        & runter und $|\Delta P| < 505$\,W & halten\\
\bottomrule
\end{tabular}
\end{center}
Konstanten: $E_{\min}=2500$, $G=900$, $H=505$, $N=2$, $\mathrm{EMERG}=G+120$ \;[W].\\
Totband (Gleichgewicht, Last an, $e_{\text{eff}}=P_s$):
\[ \max(-G,\ E_{\min}-P_s) \;<\; p \;<\; \big(\Pi^{+}(s)-P_s\big)-G \]

\begin{figure}[h]\centering\includegraphics[width=\linewidth]{fx_maps.pdf}\end{figure}
\begin{figure}[h]\centering\includegraphics[width=\linewidth]{fx_deadband.pdf}
\caption*{\small Totband je State: untere Kante $=\max(-G,\,E_{\min}-P_s)$, obere Kante $=$ nächste Leistungsstufe $-P_s-G$.}\end{figure}

\section*{Simulation}
\begin{figure}[h]\centering\includegraphics[width=\linewidth]{fx_sim.pdf}\end{figure}
(a) Hochlauf: die Rampenbegrenzung lässt nur eine Leistungsstufe pro Takt zu.
(b) Lastsprung im Sweet-Spot: $-2500$\,W ab Takt 5 (Wärmepumpe) wird in wenigen Takten abgeworfen.

\section*{Anlagen-Grenzen}
Sicherung $3\times50$\,A @ 230\,V $= 34{,}5$\,kW; PV-Peak 35\,kWp; Bat1 (Sofar) 5\,kWh; Bat2 (EBox) 30\,kWh.
EBox-Max (State 7) 11{,}4\,kW $=$ 33\,\% der Sicherung. Bat2 @ State 7: $+0{,}63\,\%$/min ($\to$ voll in ${\sim}2{,}6$\,h).

\section*{Validierung}
\texttt{step\_full\_literal} wurde one-step-ahead gegen die Ist-Entscheidungen des ESP
(\texttt{pv\_decision\_log}) geprüft: \textbf{55/55 State-Treffer (100\,\%)}, excess-Formel 54/55.
Hinweis: Der C-Code vergleicht bei der Hochrampe über die State-\emph{Nummer} statt den Leistungs-\emph{Rang};
da $P$ nicht monoton in der Nummer ist (State4 $=3900 <$ State3 $=6650$\,W), weicht das in Randfällen
vom rang-monotonen Modell ab. \texttt{step\_full\_literal} bildet den C-Code 1:1 ab.

\end{document}
"""


FX_EN = preamble("en") + r"""
\begin{document}
\begin{center}{\LARGE\bfseries fox2db --- controller model}\\[2pt]
{\large Sofar / Waveshare ESP32-S3 6CH}\end{center}\vspace{4pt}

Load-following PV-surplus controller for a solar plant with two battery stores. It switches a
\emph{quantized} electrical load (EBox) so that the grid exchange stays near zero
(``sweet spot''), without disturbing the autonomous Sofar battery.

\section*{Classification}
Discrete-time, nonlinear \textbf{hybrid automaton} (Mealy machine with hysteresis) ---
\textbf{not a PID, not an LTI system}. The actuator is discrete (8 power levels via 3 relay bits),
hence dead-band + rate limiting instead of a continuous control signal. Cycle: 60\,s.

\section*{Mathematical model}
Power map and (power-sorted) rank:
\[ P = (0,\,3000,\,3650,\,6650,\,3900,\,7100,\,7800,\,11400)\ \text{W} \]
\[ \Pi_{\text{asc.}} = (0,3000,3650,3900,6650,7100,7800,11400) \]
Step recursion, state $x_k=(s,c,E)$:
\[ e_{\text{eff}} = \max\!\big(e_k,\ P_{s_{k-1}}\big)\quad (s_{k-1}>0), \qquad E_k = p_k + e_{\text{eff}} + b_k \]
\[ \tau_k = \begin{cases} 0 & E_k < E_{\min}\\[2pt] Q(E_k+G) & \text{otherwise} \end{cases}
\qquad Q(B)=\max\{\, s : P_s \le B \,\} \]
Up-ramp (max.\ one power step per cycle):
\[ \tilde{\tau}_k = \min\nolimits_{\text{rank}}\big(\tau_k,\ \hat{s}(\operatorname{rank}(s_{k-1})+1)\big) \]
Switching / protection logic (order = priority):
\begin{center}\small
\begin{tabular}{@{}lll@{}}
\toprule
Rule & Condition & Action\\\midrule
\texttt{EMERGENCY\_FORCE} & $\mathrm{pcc} < -1020$ (= $-(G{+}120)$) and down & switch now\\
\texttt{SWEET\_SPOT\_HOLD} & up and $|\mathrm{pcc}|<160$ & hold\\
\texttt{TREND\_BLOCK}      & up and $\dot E < -20$\,W/s & hold\\
\texttt{BAT\_GUARD\_BLOCK} & up and $b_1 < -110$\,W & hold\\
\texttt{STABILIZING}       & down and $c < 2$ cycles & hold\\
\texttt{HYSTERESIS}        & down and $|\Delta P| < 505$\,W & hold\\
\bottomrule
\end{tabular}
\end{center}
Constants: $E_{\min}=2500$, $G=900$, $H=505$, $N=2$, $\mathrm{EMERG}=G+120$ \;[W].\\
Dead-band (equilibrium, load on, $e_{\text{eff}}=P_s$):
\[ \max(-G,\ E_{\min}-P_s) \;<\; p \;<\; \big(\Pi^{+}(s)-P_s\big)-G \]

\begin{figure}[h]\centering\includegraphics[width=\linewidth]{fx_maps.pdf}\end{figure}
\begin{figure}[h]\centering\includegraphics[width=\linewidth]{fx_deadband.pdf}
\caption*{\small Dead-band per state: lower edge $=\max(-G,\,E_{\min}-P_s)$, upper edge $=$ next power level $-P_s-G$.}\end{figure}

\section*{Simulation}
\begin{figure}[h]\centering\includegraphics[width=\linewidth]{fx_sim.pdf}\end{figure}
(a) Ramp-up: rate limiting allows only one power step per cycle.
(b) Load step in the sweet spot: $-2500$\,W from cycle 5 (heat pump) is shed within a few cycles.

\section*{Plant limits}
Fuse $3\times50$\,A @ 230\,V $= 34.5$\,kW; PV peak 35\,kWp; Bat1 (Sofar) 5\,kWh; Bat2 (EBox) 30\,kWh.
EBox max (state 7) 11.4\,kW $=$ 33\,\% of the fuse. Bat2 @ state 7: $+0.63\,\%$/min ($\to$ full in ${\sim}2.6$\,h).

\section*{Validation}
\texttt{step\_full\_literal} was checked one-step-ahead against the ESP's actual decisions
(\texttt{pv\_decision\_log}): \textbf{55/55 state match (100\,\%)}, excess formula 54/55.
Note: the C code compares by state \emph{number} for the up-ramp rather than the power \emph{rank};
since $P$ is not monotone in the number (state4 $=3900 <$ state3 $=6650$\,W), this deviates in edge
cases from the rank-monotone model. \texttt{step\_full\_literal} mirrors the C code 1:1.

\end{document}
"""


SO_DE = preamble("de") + r"""
\begin{document}
\begin{center}{\LARGE\bfseries Soyo-Entladeregler}\\[2pt]
{\large bat2 $\to$ Netz (RS485)}\end{center}\vspace{4pt}

Der Soyo ist ein netzgekoppelter Mikro-Wechselrichter, der die EBox-Batterie (bat2) ins Haus
entlädt, um den Netzbezug nahe Null zu halten --- das Gegenstück zum EBox-Lader. Sollwert
$w\in[0,900]$\,W, Takt 60\,s, RS485-Frame alle 3\,s.

\section*{Klassifikation}
\textbf{Gated P-Regler} mit Nacht-Vorsteuerung und Sättigung. P-Anteil auf den Netzbezug
(Sollwert $\mathrm{pcc}=0$), kein I-/D-Anteil. Fünf Sperren erzwingen $w=0$.

\section*{Mathematisches Modell}
Sperrkaskade --- $w=0$, falls eine Bedingung greift:
\begin{center}\small
\begin{tabular}{@{}ll@{}}
\toprule
Gate & Bedingung\\\midrule
G1 & stale (WR-Daten $>3$\,min alt)\\
G2 & $\mathrm{st}\neq 0$ (EBox lädt $\to$ kein Doppelbetrieb)\\
G3 & $0 \le \mathrm{soc2} < 9$ (Tiefentladeschutz)\\
G4 & $\mathrm{ebox} > 200$ (bat2 lädt)\\
G5 & $\mathrm{pcc} > 200$ (PV-Überschuss)\\
\bottomrule
\end{tabular}
\end{center}
Aktives Gesetz (Gate offen):
\[ w = \begin{cases}
\operatorname{sat}_{[0,900]}\!\big(k_p\,(-\mathrm{pcc}) + B_n\,\mathbf{1}[\text{Nacht}]\big) & \mathrm{pcc} < -100\\[3pt]
B_n\,\mathbf{1}[\text{Nacht}] + B_d\,\mathbf{1}[\text{Tag}] & -100 \le \mathrm{pcc} \le 200
\end{cases} \]
Der P-Anteil mit $k_p\approx 1$ ist eine Einheits-Vorwärtskompensation: speise so viel ein, wie
gerade bezogen wird. $B_n=468$\,W ist die Nacht-Grundlast (Feed-Forward); das Totband
$[-100,200]$\,W verhindert Pendeln um Null.

Konstanten: $k_p=1{,}01$, $B_n=468$, $B_d=10$, $w_{\max}=900$ \;[W].\\
Sende-Watchdog (RS485, alle 3\,s): $\;w_{tx} = w\cdot\mathbf{1}[\Delta t \le 90\,\text{s}]$. Bleibt der
Sollwert aus (Reglerausfall), werden 0\,W gesendet (Fail-Safe).

Protokoll-Frame (8\,Byte): \texttt{24 56 00 21 PH PL 80 CRC} mit
\texttt{PH=(w>>8)\&0xFF}, \texttt{PL=w\&0xFF}, \texttt{CRC=(264-PH-PL)\&0xFF}.

\begin{figure}[h]\centering\includegraphics[width=0.92\linewidth]{so_char.pdf}
\caption*{\small Statische Kennlinie: Proportional-Rampe, Sättigung 900\,W, Nacht-Offset $B_n$, Totband, PV-Sperre.}\end{figure}
\begin{figure}[h]\centering\includegraphics[width=\linewidth]{so_resp.pdf}\end{figure}

\section*{Einordnung}
Reiner P-Regler auf den Bezugsfehler $e=-\mathrm{pcc}$ (quasi Deadbeat pro Takt), Feed-Forward,
Sättigung, Totband; kein Integrator $\to$ kein Windup. Lader und Entlader kacheln die pcc-Achse
um 0 und schließen sich gegenseitig aus (Sperren $\mathrm{st}\neq0$, $\mathrm{ebox}>200$,
$\mathrm{pcc}>200$). Wegen der Sättigung bei 900\,W deckt der Soyo nur Grundlast.

\end{document}
"""


SO_EN = preamble("en") + r"""
\begin{document}
\begin{center}{\LARGE\bfseries Soyo discharge controller}\\[2pt]
{\large bat2 $\to$ grid (RS485)}\end{center}\vspace{4pt}

The Soyo is a grid-tie micro-inverter that discharges the EBox battery (bat2) into the house to keep
grid import near zero --- the counterpart to the EBox charger. Setpoint $w\in[0,900]$\,W,
cycle 60\,s, RS485 frame every 3\,s.

\section*{Classification}
\textbf{Gated P-controller} with night feed-forward and saturation. P-term on grid import
(target $\mathrm{pcc}=0$), no I/D term. Five gates force $w=0$.

\section*{Mathematical model}
Gate cascade --- $w=0$ if any condition holds:
\begin{center}\small
\begin{tabular}{@{}ll@{}}
\toprule
Gate & Condition\\\midrule
G1 & stale (inverter data $>3$\,min old)\\
G2 & $\mathrm{st}\neq 0$ (EBox charging $\to$ no simultaneous discharge)\\
G3 & $0 \le \mathrm{soc2} < 9$ (deep-discharge guard)\\
G4 & $\mathrm{ebox} > 200$ (bat2 charging)\\
G5 & $\mathrm{pcc} > 200$ (PV surplus)\\
\bottomrule
\end{tabular}
\end{center}
Active law (gate open):
\[ w = \begin{cases}
\operatorname{sat}_{[0,900]}\!\big(k_p\,(-\mathrm{pcc}) + B_n\,\mathbf{1}[\text{night}]\big) & \mathrm{pcc} < -100\\[3pt]
B_n\,\mathbf{1}[\text{night}] + B_d\,\mathbf{1}[\text{day}] & -100 \le \mathrm{pcc} \le 200
\end{cases} \]
The P-term with $k_p\approx 1$ is a unity feed-forward cancellation: inject as much as is currently
imported. $B_n=468$\,W is the night base load (feed-forward); the dead-band $[-100,200]$\,W prevents
hunting around zero.

Constants: $k_p=1.01$, $B_n=468$, $B_d=10$, $w_{\max}=900$ \;[W].\\
TX watchdog (RS485, every 3\,s): $\;w_{tx} = w\cdot\mathbf{1}[\Delta t \le 90\,\text{s}]$. If the setpoint
is not refreshed (controller failure), 0\,W is sent (fail-safe).

Protocol frame (8\,bytes): \texttt{24 56 00 21 PH PL 80 CRC} with
\texttt{PH=(w>>8)\&0xFF}, \texttt{PL=w\&0xFF}, \texttt{CRC=(264-PH-PL)\&0xFF}.

\begin{figure}[h]\centering\includegraphics[width=0.92\linewidth]{so_char.pdf}
\caption*{\small Static characteristic: proportional ramp, saturation 900\,W, night offset $B_n$, dead-band, PV gate.}\end{figure}
\begin{figure}[h]\centering\includegraphics[width=\linewidth]{so_resp.pdf}\end{figure}

\section*{Classification \& notes}
Pure P-controller on the import error $e=-\mathrm{pcc}$ (quasi dead-beat per cycle), feed-forward,
saturation, dead-band; no integrator $\to$ no windup. Charger and discharger tile the pcc axis
around 0 and are mutually exclusive (gates $\mathrm{st}\neq0$, $\mathrm{ebox}>200$,
$\mathrm{pcc}>200$). Due to saturation at 900\,W the Soyo covers only base load.

\end{document}
"""


DOCS = {
    "fox2db_model.pdf":    FX_DE,
    "fox2db_model_en.pdf": FX_EN,
    "soyo_model.pdf":      SO_DE,
    "soyo_model_en.pdf":   SO_EN,
}


def build():
    BUILD.mkdir(exist_ok=True)
    # Plots erzeugen
    fx_maps(BUILD / "fx_maps.pdf")
    fx_deadband(BUILD / "fx_deadband.pdf")
    fx_sim(BUILD / "fx_sim.pdf")
    so_char(BUILD / "so_char.pdf")
    so_resp(BUILD / "so_resp.pdf")

    for out, tex in DOCS.items():
        stem = out[:-4]
        (BUILD / f"{stem}.tex").write_text(tex, encoding="utf-8")
        for _ in range(2):
            r = subprocess.run(["pdflatex", "-interaction=nonstopmode", "-halt-on-error",
                                f"{stem}.tex"], cwd=BUILD, capture_output=True, text=True)
        if (BUILD / f"{stem}.pdf").exists() and r.returncode == 0:
            shutil.copy(BUILD / f"{stem}.pdf", HERE / out)
            print(f"OK  -> {out}")
        else:
            print(f"FEHLER {out}:")
            print("\n".join(l for l in r.stdout.splitlines() if l.startswith("!"))[:1500] or r.stdout[-1500:])


if __name__ == "__main__":
    build()
