#!/usr/bin/env python3
"""
validate_model.py — validiert fox2db_model.step_full_literal gegen echte
waveshare-Entscheidungen aus pv_decision_log.

Eingabe: TSV über stdin (ts, state_from, state_to, pcc_w, bat1_w, ebox_w,
         excess_w, soc, decision, detail), chronologisch (ORDER BY id).

Methode: One-step-ahead. Pro Zeile wird state_from als Ankerzustand genommen
(kein Modell-Drift), c (stable) und last_excess aus der Ist-Sequenz rekonstruiert.
Zeilen mit soc2-Guards / PCC_OVER / Ladesperre / Stale werden übersprungen
(die deckt das vereinfachte Modell bewusst nicht ab).
"""
import sys, re
from fox2db_model import step_full_literal

SKIP = ('GUARD:', 'EBOX_SOC', 'EMERGENCY_CHARGE', 'CHARGE_TARGET',
        'LADESPERRE', 'PCC_OVER', 'MQTT_STALE', 'BATTERY_FULL', 'CRITICAL_SOC')

def primary(detail):
    tok = re.split(r'[(|]', detail.strip())[0].strip() if detail else ''
    return tok[6:] if tok.startswith('GUARD:') else tok

def mods(detail):  # Menge der | MODIFIER | nach dem ersten Token
    return set(re.findall(r'\|\s*([A-Z][A-Z0-9_]+)', detail))

rows = []
for line in sys.stdin:
    p = line.rstrip('\n').split('\t')
    if len(p) < 10:
        continue
    try:
        rows.append((p[0], int(p[1]), int(p[2]), float(p[3]), float(p[4]),
                     float(p[5]), float(p[6]), p[8], p[9]))
    except ValueError:
        continue

c = 0
prev_excess = 0.0
prev_st = None
total = match = skipped = excess_ok = disc = 0
dec_match = 0
mism = []
rmism = []

for (ts, sf, st, pcc, bat1, ebox, excess, decision, detail) in rows:
    if prev_st is not None and sf != prev_st:
        disc += 1                      # Diskontinuität (Reboot/Lücke) → c unsicher
        c = 0
    s_pred, c_new, ex_model, trace = step_full_literal(sf, c, prev_excess, pcc, ebox, bat1)

    if abs(ex_model - excess) <= 2:
        excess_ok += 1

    pure = not any(m in detail for m in SKIP)
    if pure:
        total += 1
        if s_pred == st:
            match += 1
            if primary(trace) == decision and mods(trace) == mods(detail):
                dec_match += 1
            else:
                rmism.append((ts, sf, st, decision, mods(detail), primary(trace), mods(trace)))
        else:
            mism.append((ts, sf, st, s_pred, int(pcc), int(bat1), int(ebox),
                         detail[:46], trace))
    else:
        skipped += 1

    c = 0 if st != sf else c + 1       # an Ist-Transition ausrichten
    prev_excess = excess
    prev_st = st

print("="*72)
print(f"Zeilen gesamt:                {len(rows)}")
print(f"  davon reine Power-Logik:    {total}")
print(f"  übersprungen (Guards etc.): {skipped}")
print(f"  Diskontinuitäten (Reboot):  {disc}")
print("-"*72)
print(f"excess-Formel reproduziert:   {excess_ok}/{len(rows)} "
      f"({excess_ok/max(len(rows),1)*100:.1f}%)")
if total:
    print(f"STATE-Treffer (one-step):     {match}/{total} ({match/total*100:.1f}%)")
    print(f"  davon Grund+Modifier exakt: {dec_match}/{match}")
print("="*72)
for m in mism[:20]:
    print(f"  STATE-MISMATCH {m[0]}  from{m[1]}→ist{m[2]} pred{m[3]}  "
          f"pcc={m[4]} bat1={m[5]} ebox={m[6]}")
    print(f"           ist: {m[7]:48s} model: {m[8]}")

if rmism:
    print(f"\nGRUND-Diffs (State stimmt, Trace weicht ab) — {len(rmism)}:")
    for r in rmism[:25]:
        print(f"  {r[0]} S{r[1]}→{r[2]}  ist[{r[3]} {sorted(r[4])}]  "
              f"model[{r[5]} {sorted(r[6])}]")
