#!/usr/bin/env python3
"""
make_soyo_pdf.py — erzeugt soyo_model.pdf (Soyo-Entladeregler bat2 -> Netz):
Beschreibung, Formeln, statische Kennlinie, Komplementarität und Reglerantwort.

Nutzt soyo_model.py als Referenz und text_page() aus make_fox2db_pdf.py.
Aufruf:  python3 make_soyo_pdf.py
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np

import soyo_model as S
from make_fox2db_pdf import text_page, A4   # Textseiten-Helfer wiederverwenden

OUT = "soyo_model.pdf"


def wcurve(pcc_arr, hour):
    return np.array([S.soyo_setpoint(0, 50.0, 0.0, float(p), hour)[0] for p in pcc_arr])


def build():
    pdf = PdfPages(OUT)

    # ── Seite 1: Beschreibung ────────────────────────────────────────────────
    text_page(pdf, "Soyo-Entladeregler (bat2 → Netz)", [
        ("p", "Der Soyo ist ein netzgekoppelter Mikro-Wechselrichter, der die "
              "EBox-Batterie (bat2) ins Haus entlädt, um den Netzbezug nahe Null zu "
              "halten — das Gegenstück zum EBox-Lader (fox2db). Sollwert w in [0,900] W, "
              "Takt 60 s, Übertragung per RS485 alle 3 s."),
        ("h", "Klassifikation"),
        ("p", "Gated Proportional-Regler mit Nacht-Vorsteuerung und Sättigung. "
              "P-Anteil auf den Netzbezug (Sollwert pcc=0), kein I-/D-Anteil. Fünf "
              "Sperren erzwingen w=0 (Sicherheit / kein Doppelbetrieb mit dem Lader)."),
        ("h", "Ein- und Ausgänge"),
        ("mono",
         "Eingänge :  pcc   Netzaustausch [W]  (< 0 = Bezug)\n"
         "            st    EBox-State 0..7    (Lader-Zustand)\n"
         "            soc2  bat2-SOC [%]\n"
         "            ebox  gemessene EBox-Last [W]\n"
         "            h     Stunde (Tag/Nacht)\n"
         "Ausgang  :  w     Soyo-Sollwert 0..900 W  (RS485-Frame)"),
        ("h", "Konstanten"),
        ("mono",
         f"k_p (P-Verstärkung) ...... {S.KP}\n"
         f"B_n (Nacht-Grundlast) .... {S.B_NIGHT} W   (Vorsteuerung)\n"
         f"B_d (Tag-Leerlauf) ....... {S.B_DAY_IDLE} W\n"
         f"w_max (Sättigung) ........ {S.W_MAX} W\n"
         f"Bezugs-Schwelle .......... {S.PCC_IMPORT_TH:.0f} W\n"
         f"Überschuss-Sperre ........ {S.PCC_SURPLUS_TH:.0f} W\n"
         f"Tiefentladeschutz ........ soc2 < {S.SOC_MIN:.0f} %\n"
         f"Nacht .................... h < {S.NIGHT_END} oder h >= {S.NIGHT_START}\n"
         f"Sende-Watchdog ........... Sollwert > {S.TX_WATCHDOG_MS//1000} s alt -> 0"),
    ])

    # ── Seite 2: Mathematik ──────────────────────────────────────────────────
    text_page(pdf, "Mathematisches Modell", [
        ("h", "Sperrkaskade (Gate) — w = 0 falls eine Bedingung greift"),
        ("mono",
         "G1  stale        (Inverter-Daten > 3 min alt)\n"
         "G2  st != 0      (EBox lädt -> nicht gleichzeitig entladen)\n"
         "G3  0 <= soc2 < 9   (Tiefentladeschutz)\n"
         "G4  ebox > 200   (bat2 lädt)\n"
         "G5  pcc  > 200   (PV-Überschuss -> bat2 schonen)"),
        ("h", "Aktives Gesetz (Gate offen)"),
        ("m", r"$w=\mathrm{sat}_{[0,900]}\!\left(k_p\,(-\mathrm{pcc})+B_n\,\mathbf{1}[\mathrm{Nacht}]\right)"
              r"\quad \mathrm{pcc}<-100$"),
        ("m", r"$w=B_n\,\mathbf{1}[\mathrm{Nacht}]+B_d\,\mathbf{1}[\mathrm{Tag}]"
              r"\quad -100\leq \mathrm{pcc}\leq 200$"),
        ("p", "Der P-Anteil mit k_p≈1 ist eine Einheits-Vorwärtskompensation: speise "
              "so viel ein, wie gerade bezogen wird. B_n=468 W ist die Nacht-Grundlast "
              "als Feed-Forward. Das Totband [-100, 200] W verhindert Pendeln um Null."),
        ("h", "Konstanten"),
        ("m", r"$k_p=1.01,\;B_n=468,\;B_d=10,\;w_{\max}=900\;\mathrm{[W]}$"),
        ("h", "Sende-Watchdog (RS485, alle 3 s)"),
        ("m", r"$w_{tx}=w\cdot\mathbf{1}[\Delta t \leq 90\,\mathrm{s}]$"),
        ("p", "Wird der Sollwert nicht rechtzeitig erneuert (Reglerausfall), sendet "
              "der RS485-Teil 0 W -> der Soyo stoppt die Entladung (Fail-Safe)."),
        ("h", "Protokoll-Frame (8 Byte)"),
        ("mono",
         "24 56 00 21  ph pl  80  crc      ph=(w>>8)&0xFF, pl=w&0xFF\n"
         "crc = (264 - ph - pl) & 0xFF"),
    ])

    # ── Seite 3: Statische Kennlinie ─────────────────────────────────────────
    fig, ax = plt.subplots(figsize=A4)
    fig.subplots_adjust(left=0.12, right=0.93, top=0.90, bottom=0.42)
    pcc = np.linspace(-1000, 400, 2000)
    ax.axvspan(-100, 200, color="0.92", label="Totband")
    ax.axvspan(200, 400, color="#f6d7d7", alpha=0.6, label="PV-Sperre (w=0)")
    ax.plot(pcc, wcurve(pcc, 2), color="#2c3e8c", lw=2.0, label="Nacht (h=2)")
    ax.plot(pcc, wcurve(pcc, 12), color="#c0392b", lw=2.0, label="Tag (h=12)")
    ax.axhline(S.W_MAX, color="0.5", ls=":", lw=1.0)
    ax.text(-980, S.W_MAX + 12, f"Sättigung w_max = {S.W_MAX} W", fontsize=8, color="0.4")
    ax.axhline(S.B_NIGHT, color="#2c3e8c", ls=":", lw=0.8)
    ax.text(60, S.B_NIGHT + 12, f"B_n={S.B_NIGHT}", fontsize=8, color="#2c3e8c")
    ax.axvline(-100, color="0.6", lw=0.7); ax.axvline(200, color="0.6", lw=0.7)
    ax.set_title("Statische Kennlinie  w(pcc):  Soyo-Sollwert über Netzaustausch")
    ax.set_xlabel("Netzaustausch pcc [W]   (links = Bezug, rechts = Einspeisung)")
    ax.set_ylabel("Soyo-Sollwert w [W]")
    ax.set_xlim(-1000, 400); ax.set_ylim(-30, 980)
    ax.grid(alpha=0.3); ax.legend(loc="upper right", fontsize=9)
    fig.text(0.12, 0.34,
             "Bereiche (von links):\n"
             "  • pcc < -100 W : proportionaler Betrieb, w = k_p·|pcc| (+B_n nachts), bis Sättigung 900 W\n"
             "  • -100..200 W  : Totband — nur Grundlast (468 W nachts) bzw. 10 W tags\n"
             "  • pcc > 200 W  : PV-Überschuss -> Soyo aus (w=0), bat2 wird geschont\n\n"
             "Die Kurve ist die vollständige (memorylose) Definition des P-Reglers; die "
             "Gate-Sperren\nG1–G4 (stale / EBox an / SOC<9% / bat2 lädt) setzen sie zusätzlich auf 0.",
             fontsize=9.5, va="top", family="monospace")
    pdf.savefig(fig); plt.close(fig)

    # ── Seite 4: Komplementarität + Reglerantwort ────────────────────────────
    fig, (c1, c2) = plt.subplots(2, 1, figsize=A4)
    fig.subplots_adjust(left=0.12, right=0.90, top=0.93, bottom=0.07, hspace=0.40)

    # (a) Komplementaritäts-Schema auf der pcc-Achse
    c1.set_title("(a) Komplementarität: Soyo (entladen) ↔ EBox-Lader (laden)")
    c1.axvspan(-1500, -100, color="#c0392b", alpha=0.25)
    c1.axvspan(-100, 200, color="0.85")
    c1.axvspan(1200, 4000, color="#2980b9", alpha=0.25)
    c1.axvline(0, color="k", lw=1.0)
    c1.text(-800, 0.5, "SOYO entlädt bat2\nw = k_p·|pcc| (≤900 W)", ha="center", va="center", fontsize=9)
    c1.text(50, 0.5, "Totband\n(Leerlauf)", ha="center", va="center", fontsize=8)
    c1.text(2600, 0.5, "EBOX lädt bat2\nState ↑ (≤11400 W)", ha="center", va="center", fontsize=9)
    c1.set_xlim(-1500, 4000); c1.set_ylim(0, 1); c1.set_yticks([])
    c1.set_xlabel("Netzaustausch pcc [W]   (Bezug ◄ pcc=0 ► Einspeisung)")

    # (b) Reglerantwort (offen) auf einen Bezugsverlauf (Nacht)
    pcc_tr = np.array([-150, -150, -300, -800, -1500, -1500, -800, -300, 300, 300, -150, -150.0])
    w_tr = wcurve(pcc_tr, 2)
    t = np.arange(len(pcc_tr))
    c2.set_title("(b) Reglerantwort auf gemessenen Bezugsverlauf (Nacht, h=2)")
    c2.step(t, w_tr, where="post", color="#2c3e8c", lw=1.8, label="Soyo w")
    c2.axhline(S.W_MAX, color="0.5", ls=":", lw=0.8)
    c2.set_ylabel("Soyo w [W]", color="#2c3e8c"); c2.set_ylim(-30, 980)
    c2b = c2.twinx()
    c2b.plot(t, pcc_tr, color="#7f8c8d", lw=1.2, label="pcc (Eingang)")
    c2b.axhline(0, color="0.7", lw=0.6)
    c2b.set_ylabel("pcc [W]", color="#7f8c8d")
    c2.set_xlabel("Takt [min]"); c2.grid(alpha=0.3)
    c2.text(0.02, 0.95,
            "kleiner Bezug → proportional + B_n;  großer Bezug → Sättigung 900 W;\n"
            "Einspeisung (pcc>200) → Soyo aus.",
            transform=c2.transAxes, fontsize=8, va="top",
            bbox=dict(boxstyle="round", fc="#f3f3f3", ec="0.7"))
    pdf.savefig(fig); plt.close(fig)

    # ── Seite 5: Einordnung ──────────────────────────────────────────────────
    text_page(pdf, "Einordnung & Artefakte", [
        ("h", "Regelungstechnischer Charakter"),
        ("p", "Reiner P-Regler auf den Bezugsfehler e=-pcc (Sollwert pcc=0) mit "
              "Verstärkung k_p≈1 (quasi Deadbeat pro Takt), Feed-Forward-Nachtgrundlast, "
              "Sättigung und Totband. Kein Integrator -> kein Windup; der Regler stützt "
              "sich auf das Neu-Messen jedes 60-s-Takts."),
        ("h", "Zusammenspiel mit dem EBox-Lader"),
        ("p", "Beide Regler kacheln die pcc-Achse um 0 und schließen sich gegenseitig "
              "aus: die Sperren st!=0, ebox>200 und pcc>200 verhindern, dass bat2 "
              "gleichzeitig ge- und entladen wird. Soyo deckt kleine/mittlere Lasten "
              "(Grundlast, ≤900 W); der EBox-Lader nimmt großen PV-Überschuss auf."),
        ("h", "Grenzen"),
        ("p", "Durch die Sättigung bei 900 W deckt der Soyo nur Grundlast; größere "
              "Verbraucher tragen Sofar-Batterie (bat1) und Netz. Die Nacht-Vorsteuerung "
              "468 W dämpft das Pendeln um den Totband-Rand in der Nacht."),
        ("h", "Artefakte"),
        ("mono",
         "sofar_waveshare.yaml   Soyo-Kalkulation (ESPHome, interval 60s + RS485)\n"
         "soyo_model.py          math. Referenz + 20 Selbsttests\n"
         "make_soyo_pdf.py       dieses Dokument"),
    ])

    d = pdf.infodict()
    d["Title"] = "Soyo-Entladeregler — Modell"
    d["Subject"] = "Mathematisches Modell, Kennlinie, Komplementarität"
    pdf.close()
    print(f"geschrieben: {OUT}")


if __name__ == "__main__":
    build()
