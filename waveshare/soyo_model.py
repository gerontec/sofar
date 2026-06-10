#!/usr/bin/env python3
"""
soyo_model.py — math. Referenz des Soyo-Entladereglers (bat2 → Netz).

Der Soyo ist ein netzgekoppelter Mikro-Wechselrichter, der die EBox-Batterie
(bat2) ins Haus entlädt, um Netzbezug nahe Null zu halten — das Gegenstück zum
EBox-Lader (fox2db_model). Sollwert w ∈ [0, 900] W, Takt 60 s.

1:1 aus sofar_waveshare.yaml (Soyo-Kalkulation, interval 60s).

Charakter: GATED PROPORTIONAL-REGLER
    - 5 Sperren (Gate) erzwingen w=0 (Sicherheit / kein Doppelbetrieb mit EBox)
    - im Bezug: w = sat_[0,900]( k_p·(-pcc) + B_nacht·1[Nacht] )   k_p=1.01
    - im Totband: w = Nacht-Grundlast (468) bzw. Tag-Leerlauf (10)
    - Sende-Watchdog: Sollwert älter als 90 s ⇒ 0 (RS485-Sicherung)

Run:  python3 soyo_model.py   →  Selbsttests.
"""
from typing import Tuple

# ── Konstanten (1:1 aus YAML) ────────────────────────────────────────────────
KP            = 1.01      # Proportionalverstärkung auf den Bezug (≈ Einheitsverstärkung)
B_NIGHT       = 468       # Nacht-Grundlast-Vorsteuerung [W]
B_DAY_IDLE    = 10        # Tag-Leerlauf im Totband [W]
W_MAX         = 900       # Sättigung / Soyo-Maximum [W]
PCC_IMPORT_TH = -100.0    # darunter (Bezug) → proportionaler Betrieb
PCC_SURPLUS_TH = 200.0    # darüber (Einspeisung) → aus
EBOX_CHG_TH   = 200.0     # bat2 lädt → aus
SOC_MIN       = 9.0       # Tiefentladeschutz [%]
NIGHT_START   = 20        # h ≥ 20  → Nacht
NIGHT_END     = 6         # h < 6   → Nacht
STALE_MS      = 180_000   # Inverter-Daten älter → 0
TX_WATCHDOG_MS = 90_000   # Sollwert älter → 0 beim Senden


def is_night(hour: int) -> bool:
    return hour < NIGHT_END or hour >= NIGHT_START


def soyo_setpoint(st: int, soc2: float, ebox: float, pcc: float,
                  hour: int, stale: bool = False) -> Tuple[int, str]:
    """Soyo-Sollwert [W] + Grund. st=EBox-State, soc2[%], ebox[W], pcc[W] (<0=Bezug)."""
    if stale:
        return 0, "STALE"
    if st != 0:
        return 0, "EBOX_AKTIV"            # kein Doppelbetrieb: laden XOR entladen
    if 0.0 <= soc2 < SOC_MIN:
        return 0, "ENTLADESCHUTZ"
    if ebox > EBOX_CHG_TH:
        return 0, "BAT2_LAEDT"
    if pcc > PCC_SURPLUS_TH:
        return 0, "PV_UEBERSCHUSS"

    night = is_night(hour)
    if pcc < PCC_IMPORT_TH:               # echter Netzbezug → proportional ausregeln
        w = int(-pcc * KP) + (B_NIGHT if night else 0)
        if w > W_MAX:
            w = W_MAX
        return w, "PROPORTIONAL" + ("|NACHT" if night else "")
    # Totband (-100 ≤ pcc ≤ 200): ausgeglichen
    return (B_NIGHT if night else B_DAY_IDLE), "IDLE" + ("|NACHT" if night else "")


def soyo_tx(w: int, age_ms: int) -> int:
    """RS485-Sende-Watchdog: Sollwert älter als 90 s ⇒ 0."""
    return 0 if age_ms > TX_WATCHDOG_MS else w


def soyo_frame(w: int) -> bytes:
    """Protokoll-Frame (8 Byte) wie im YAML: 24 56 00 21 ph pl 80 crc."""
    w = max(0, min(W_MAX, w))
    ph, pl = (w >> 8) & 0xFF, w & 0xFF
    crc = (264 - ph - pl) & 0xFF
    return bytes([0x24, 0x56, 0x00, 0x21, ph, pl, 0x80, crc])


# ═══════════════════════════════════════════════════════════════════════════
def _run_tests() -> None:
    n = 0
    def check(c, m):
        nonlocal n
        assert c, "FAIL: " + m
        n += 1; print(f"  ok  {m}")

    print("1) Gate (erzwingt w=0, Prioritätsreihenfolge):")
    check(soyo_setpoint(0, 50, 0, -500, 12, stale=True)[0] == 0, "STALE → 0")
    check(soyo_setpoint(3, 50, 0, -500, 12)[1] == "EBOX_AKTIV", "EBox an (st=3) → 0")
    check(soyo_setpoint(0, 5, 0, -500, 12)[1] == "ENTLADESCHUTZ", "soc2=5% → 0")
    check(soyo_setpoint(0, -1, 0, -500, 12)[1] != "ENTLADESCHUTZ", "soc2 unbekannt (-1) → NICHT gesperrt")
    check(soyo_setpoint(0, 50, 500, -500, 12)[1] == "BAT2_LAEDT", "ebox=500 → 0")
    check(soyo_setpoint(0, 50, 0, 300, 12)[1] == "PV_UEBERSCHUSS", "pcc=+300 → 0")
    # Priorität: EBox schlägt Bezug
    check(soyo_setpoint(2, 50, 0, -500, 2)[0] == 0, "st≠0 schlägt Bezug (Doppelbetrieb verhindert)")

    print("2) Proportionaler Betrieb (Bezug, pcc < -100):")
    check(soyo_setpoint(0, 50, 0, -500, 12) == (505, "PROPORTIONAL"),
          "pcc=-500 Tag → 505 W (=int(500·1.01))")
    check(soyo_setpoint(0, 50, 0, -500, 2) == (900, "PROPORTIONAL|NACHT"),
          "pcc=-500 Nacht → 505+468=973 → sat 900")
    check(soyo_setpoint(0, 50, 0, -2000, 12) == (900, "PROPORTIONAL"),
          "pcc=-2000 → 2020 → sat 900")
    check(soyo_setpoint(0, 50, 0, -150, 2) == (151 + 468, "PROPORTIONAL|NACHT"),
          "pcc=-150 Nacht → 619 W")

    print("3) Totband (-100 ≤ pcc ≤ 200):")
    check(soyo_setpoint(0, 50, 0, 0, 12) == (10, "IDLE"), "pcc=0 Tag → 10 W Leerlauf")
    check(soyo_setpoint(0, 50, 0, 0, 23) == (468, "IDLE|NACHT"), "pcc=0 Nacht → 468 W Grundlast")
    check(soyo_setpoint(0, 50, 0, -50, 12) == (10, "IDLE"), "pcc=-50 (im Totband) Tag → 10 W")

    print("4) Sende-Watchdog + Frame:")
    check(soyo_tx(900, 30_000) == 900, "Sollwert frisch → unverändert")
    check(soyo_tx(900, 120_000) == 0, "Sollwert > 90 s alt → 0")
    f = soyo_frame(500)
    check(f[:4] == bytes([0x24, 0x56, 0x00, 0x21]) and f[4] == 1 and f[5] == 244,
          "Frame 500 W: ph=1 pl=244")
    check((f[6], f[7]) == (0x80, (264 - 1 - 244) & 0xFF), "Frame CRC korrekt")

    print("5) Komplementarität zum EBox-Lader (gemeinsame pcc-Achse):")
    # Soyo entlädt nur bei Bezug; EBox lädt nur bei Überschuss → nie gleichzeitig.
    check(soyo_setpoint(0, 50, 0,  3000, 12)[0] == 0, "großer Überschuss → Soyo aus (EBox-Revier)")
    check(soyo_setpoint(0, 50, 0, -800, 12)[0] > 0,   "Bezug → Soyo aktiv (Soyo-Revier)")

    print(f"\nAlle {n} Checks bestanden. ✅")


if __name__ == "__main__":
    _run_tests()
