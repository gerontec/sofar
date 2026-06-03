#!/usr/bin/env python3
"""
gen_diagrams_v2.py — Flowcharts für fox2db.py v2.0 (prozedural)
Seite 1: Schichten-Architektur
Seite 2: main() Ablauf
Seite 3: Decision-Layer Detail
Ausgabe: fox2db_v2_flowchart.pdf (3 Seiten kombiniert)
"""
import subprocess
from pathlib import Path

OUT = Path(__file__).parent

# ═══════════════════════════════════════════════════════════════════════════
# SEITE 1 — SCHICHTEN-ARCHITEKTUR
# ═══════════════════════════════════════════════════════════════════════════

PAGE1 = """
digraph Architecture {
    graph [
        label="fox2db.py v2.0 — Schichten-Architektur\\n(prozedural, 568 Zeilen, 1 math. Hilfsklasse)"
        labelloc=t fontsize=20 fontname="Helvetica-Bold"
        rankdir=TB splines=ortho nodesep=0.8 ranksep=1.0
        bgcolor="#f8f9fa" size="11,17" ratio=fill
    ]
    node [fontname="Helvetica" fontsize=12 margin="0.25,0.12" style="filled,rounded"]
    edge [fontname="Helvetica" fontsize=10 penwidth=1.5]

    // ── Externe Systeme ──────────────────────────────────────────────────
    subgraph cluster_extern {
        label="Externe Systeme" style="dashed,filled" fillcolor="#eeeeee"
        fontname="Helvetica-Bold" fontsize=13 color="#aaaaaa"

        MQTT_IN  [shape=cylinder fillcolor="#dddddd"
                  label="MQTT-Broker\\nkellertreppe:1883\\ninverter/power_grid_exchange/json"]
        MQTT_Z2  [shape=cylinder fillcolor="#dddddd"
                  label="MQTT-Broker\\npv_zaehl2/#\\n(Z2-Zähler, retained)"]
        MQTT_PUB [shape=cylinder fillcolor="#dddddd"
                  label="MQTT-Broker\\nfox2db/state\\n(retain=True)"]
        MariaDB  [shape=cylinder fillcolor="#dddddd"
                  label="MariaDB wagodb\\npv_decision_log\\npv_relay_events"]
        Relay    [shape=component fillcolor="#dddddd"
                  label="ebyte_ctrl.py\\nRelais-Hardware\\nState 0..7 / DO4-Puls"]
        EBox     [shape=component fillcolor="#dddddd"
                  label="ebox1arg.py\\n/tmp/ebox15k.txt\\nSOC, Strom"]
    }

    // ── Input Layer ──────────────────────────────────────────────────────
    subgraph cluster_input {
        label="INPUT LAYER" style="filled" fillcolor="#d4edda"
        fontname="Helvetica-Bold" fontsize=13 color="#28a745"

        fetch_mqtt  [shape=box fillcolor="#b8dfc4"
                     label="fetch_mqtt()\\nMQTT empfangen\\nNone-Felder → 0"]
        fetch_z2    [shape=box fillcolor="#b8dfc4"
                     label="fetch_z2()\\nZ2-Zähler lesen\\n(retained MQTT, 3s)"]
        read_ebox   [shape=box fillcolor="#b8dfc4"
                     label="read_ebox()\\nEBox SOC + Strom\\nsoc=-1 wenn unbekannt"]
        z2_fallback [shape=diamond fillcolor="#fff3cd"
                     label="PCC = NaN?"]
        pcc_node    [shape=box fillcolor="#b8dfc4"
                     label="pcc = abs(min(0, wirkleist))\\n(Z2-Fallback)"]
    }

    // ── Decision Layer ───────────────────────────────────────────────────
    subgraph cluster_decision {
        label="DECISION LAYER" style="filled" fillcolor="#e2d9f3"
        fontname="Helvetica-Bold" fontsize=13 color="#6f42c1"

        decide_fn [shape=box fillcolor="#c9b8f0"
                   label="decide()\\n— Dominanzgeordnet (erste Regel gewinnt) —\\n\\n• EBOX_SOC_UNKNOWN_HOLD  (soc \\< 0)\\n• EMERGENCY_CHARGE_TO_7%  (prot + soc \\< 7)\\n• CHARGE_TARGET_REACHED   (prot + soc ≥ 7)\\n• INSUFFICIENT_EXCESS     (excess \\< min)\\n• POWER_MATCHING          (Default)\\n  └─ RAMP_LIMITED          (max +1 State/Zyklus)"]

        hard_guards [shape=box fillcolor="#f8d7da"
                     label="apply_guards()  ← HARD GUARDS\\n— Physikalische Invarianten, unüberwindbar —\\n\\n• BATTERY_FULL_STOP       (soc > 99)\\n• CRITICAL_SOC_PROTECTION (soc \\< 6)"]

        blocking [shape=box fillcolor="#d4c5f0"
                  label="apply_blocking()\\n— Verhindert zu schnelle Zustandsänderungen —\\n\\n• EMERGENCY_FORCE    (Bypass bei Import \\> 1020W)\\n\\nUP-Sperren:\\n  SWEET_SPOT_HOLD / TREND_BLOCK / BAT_GUARD_BLOCK\\n\\nDOWN-Sperren:\\n  STABILIZING / HYSTERESIS"]

        dc_calc [shape=box fillcolor="#c9b8f0"
                 label="_DcForecast.now()\\n(Meinel-Modell WR1+WR2\\nKlarhimmel-Prognose)"]
    }

    // ── Output Layer ────────────────────────────────────────────────────
    subgraph cluster_output {
        label="OUTPUT LAYER" style="filled" fillcolor="#cce5ff"
        fontname="Helvetica-Bold" fontsize=13 color="#004085"

        log_fn      [shape=box fillcolor="#a8d0f5" label="_log()\\nZeit + Version + Meldung"]
        publish     [shape=box fillcolor="#a8d0f5" label="publish_mqtt()\\nJSON → fox2db/state"]
        set_relay   [shape=box fillcolor="#a8d0f5" label="set_relay()\\nebyte_ctrl.py aufrufen"]
        db_log      [shape=box fillcolor="#a8d0f5"
                     label="_db_decision_log()\\n_db_relay_event()\\n→ MariaDB"]
        pcc_guard   [shape=box fillcolor="#f8d7da"
                     label="PCC_OVER_20KW Post-Guard\\n(prio 1: höchste Dominanz)\\n\\nSOC\\<100 → State+1\\nsonst → DO4 pulse_do4()"]
    }

    // ── Datenfluss ───────────────────────────────────────────────────────
    MQTT_IN  -> fetch_mqtt  [color="#28a745"]
    MQTT_Z2  -> fetch_z2   [color="#28a745"]
    EBox     -> read_ebox  [color="#28a745"]
    fetch_mqtt -> z2_fallback
    fetch_z2   -> z2_fallback
    z2_fallback -> pcc_node [label="JA" color="red"]

    fetch_mqtt -> decide_fn  [color="#6f42c1" label="pcc, bat1, soc_bat1"]
    read_ebox  -> decide_fn  [color="#6f42c1" label="bat_cur, soc"]
    dc_calc    -> log_fn     [color="#6f42c1" label="dc_expected"]
    decide_fn  -> hard_guards [color="#6f42c1"]
    hard_guards -> blocking  [color="#6f42c1"]

    blocking   -> set_relay  [color="#004085"]
    blocking   -> publish    [color="#004085"]
    blocking   -> db_log     [color="#004085"]
    blocking   -> pcc_guard  [color="#c0392b"]
    pcc_guard  -> set_relay  [color="#c0392b" label="State+1"]
    pcc_guard  -> Relay      [color="#c0392b" label="DO4"]

    set_relay  -> Relay   [color="#004085"]
    publish    -> MQTT_PUB [color="#004085"]
    db_log     -> MariaDB  [color="#004085"]
}
"""

# ═══════════════════════════════════════════════════════════════════════════
# SEITE 2 — main() ABLAUF
# ═══════════════════════════════════════════════════════════════════════════

PAGE2 = """
digraph MainFlow {
    graph [
        label="fox2db.py v2.0 — main() Ablauf"
        labelloc=t fontsize=20 fontname="Helvetica-Bold"
        rankdir=TB splines=polyline nodesep=0.5 ranksep=0.6
        bgcolor="#f8f9fa" size="11,17" ratio=fill
    ]
    node [fontname="Helvetica" fontsize=11 margin="0.2,0.1" style="filled,rounded"]
    edge [fontname="Helvetica" fontsize=10 penwidth=1.5]

    Start [shape=oval fillcolor="#cce5ff" label="main()\\nStart"]
    End   [shape=oval fillcolor="#cce5ff" label="Ende"]
    EndErr [shape=oval fillcolor="#f8d7da" label="Ende\\nEmergency"]

    s1 [shape=box fillcolor="#fff3cd" label="_log('--- Start Cycle ---')"]

    s2 [shape=box fillcolor="#d4edda"
        label="fetch_mqtt()\\n→ warte auf MQTT-Nachricht\\n   (Topic: inverter/power_grid_exchange/json)\\n→ None-Felder → 0"]

    d1 [shape=diamond fillcolor="#fce8e8" label="mqtt_data\\n= None?"]

    s3 [shape=box fillcolor="#f8d7da"
        label="set_relay(0)\\n_log('EMERGENCY SHUTDOWN')"]

    s4 [shape=box fillcolor="#d4edda"
        label="read_ebox()\\n→ ebox1arg.py pwr 1 → /tmp/ebox15k.txt\\n→ bat_cur_A, soc (min aller Packs)\\n   soc=-1 wenn unbekannt"]

    s5 [shape=box fillcolor="#d4edda"
        label="fetch_z2()\\n→ MQTT pv_zaehl2/# (retained, 3s)\\n→ wirkleist"]

    d2 [shape=diamond fillcolor="#fff3cd" label="math.isnan\\n(pcc)?"]

    s5b [shape=box fillcolor="#fff3cd"
         label="pcc = abs(min(0, wirkleist))\\n_log('PCC=NaN → Z2-Fallback')"]

    s6 [shape=box fillcolor="#d4edda"
        label="Zustandsvariablen lesen:\\nrelay_st, stable, prot,\\nlast_excess, dc_expected"]

    s7 [shape=box fillcolor="#e2d9f3"
        label="decide(soc, pcc, bat_cur, bat1, relay_st, prot)\\n→ best, trace, excess"]

    s8 [shape=box fillcolor="#f8d7da"
        label="apply_guards(best, soc, trace)\\n→ BATTERY_FULL_STOP  (soc > 99 → 0)\\n→ CRITICAL_SOC       (soc \\< 6  → 1)\\nPhysikalische Invarianten — unüberwindbar"]

    s9 [shape=box fillcolor="#e2d9f3"
        label="Trend berechnen:\\ndrop_rate = (excess - last_excess) / 30\\n_write(last_excess)"]

    s10 [shape=box fillcolor="#e2d9f3"
         label="apply_blocking(best, relay_st, pcc, bat1,\\n               stable, drop_rate, trace)\\n→ final, changed, trace"]

    s11 [shape=box fillcolor="#cce5ff"
         label="Data-Log schreiben:\\n_log(Data + DC_delta + State + Stable)\\n_log(Result + TRACE)"]

    s12 [shape=box fillcolor="#cce5ff"
         label="publish_mqtt()\\n→ JSON an fox2db/state (retain=True)"]

    s13 [shape=box fillcolor="#cce5ff"
         label="Dateien schreiben:\\n_write(last_change, inverter_csv)"]

    s14 [shape=box fillcolor="#e2d9f3"
         label="Deep Discharge Hysterese:\\nsoc \\< lower → prot=1\\nsoc ≥ upper → prot=0"]

    s15 [shape=box fillcolor="#cce5ff"
         label="_db_decision_log()\\n→ pv_decision_log INSERT"]

    d3 [shape=diamond fillcolor="#fff3cd" label="changed?"]

    s16a [shape=box fillcolor="#cce5ff"
          label="_db_relay_event()\\nset_relay(final)\\n→ ebyte_ctrl.py"]

    s16b [shape=box fillcolor="#d4edda"
          label="_write(relay_state, final)\\n(kein Relay-Wechsel)"]

    d4 [shape=diamond fillcolor="#fce8e8" label="pcc\\n> 20 000 W?"]

    d5 [shape=diamond fillcolor="#fce8e8" label="soc \\< 100\\nund State+1\\nmöglich?"]

    s17a [shape=box fillcolor="#f8d7da"
          label="set_relay(final+1)\\n_db_relay_event(state_change)\\n_log('PCC>20kW: State+1')"]

    s17b [shape=box fillcolor="#f8d7da"
          label="pulse_do4()\\n→ ebyte_ctrl r4 pulse 3\\n_db_relay_event(pulse, do4)"]

    Start -> s1 -> s2 -> d1
    d1 -> s3 [label="JA" color="red"]
    d1 -> s4 [label="NEIN" color="green"]
    s3 -> EndErr
    s4 -> s5 -> d2
    d2 -> s5b [label="JA" color="red"]
    d2 -> s6  [label="NEIN" color="green"]
    s5b -> s6
    s6 -> s7 -> s8 -> s9 -> s10 -> s11 -> s12 -> s13 -> s14 -> s15 -> d3
    d3 -> s16a [label="JA" color="green"]
    d3 -> s16b [label="NEIN" color="#888888"]
    s16a -> d4
    s16b -> d4
    d4 -> d5   [label="JA" color="red"]
    d4 -> End  [label="NEIN" color="green"]
    d5 -> s17a [label="JA" color="orange"]
    d5 -> s17b [label="NEIN" color="red"]
    s17a -> End
    s17b -> End
}
"""

# ═══════════════════════════════════════════════════════════════════════════
# SEITE 3 — DECISION-LAYER DETAIL
# ═══════════════════════════════════════════════════════════════════════════

PAGE3 = """
digraph DecisionDetail {
    graph [
        label="fox2db.py v2.0 — Decision Layer im Detail"
        labelloc=t fontsize=20 fontname="Helvetica-Bold"
        rankdir=TB splines=polyline nodesep=0.5 ranksep=0.65
        bgcolor="#f8f9fa" size="11,17" ratio=fill
    ]
    node [fontname="Helvetica" fontsize=11 margin="0.2,0.1" style="filled,rounded"]
    edge [fontname="Helvetica" fontsize=10 penwidth=1.5]

    Start [shape=oval fillcolor="#e2d9f3" label="decide()"]
    End   [shape=oval fillcolor="#e2d9f3" label="→ (best, trace, excess)"]
    GEnd  [shape=oval fillcolor="#f8d7da" label="→ (best, trace)\\nnach HARD GUARDS"]
    BEnd  [shape=oval fillcolor="#d4c5f0" label="→ (final, changed, trace)\\nnach BLOCKING"]

    // ── decide() ────────────────────────────────────────────────────────
    excess_calc [shape=box fillcolor="#c9b8f0"
                 label="excess = pcc + ebox_eff + bat1\\nebox_eff = max(bat_cur×106+1, STATE_POWER[relay_st])"]

    d_soc_unk [shape=diamond fillcolor="#fce8e8" label="soc \\< 0?\\n(SOC unbekannt)"]
    r_soc_unk [shape=box fillcolor="#fce8e8"
               label="return relay_st\\nEBOX_SOC_UNKNOWN_HOLD"]

    d_prot [shape=diamond fillcolor="#fce8e8" label="prot aktiv?\\n(Tiefentladeschutz)"]
    d_prot2 [shape=diamond fillcolor="#fce8e8" label="soc \\< charge_target\\n(7%)?"]
    r_emerg [shape=box fillcolor="#fce8e8"
             label="return State 1\\nEMERGENCY_CHARGE_TO_7%"]
    r_target [shape=box fillcolor="#fce8e8"
              label="return State 0\\nCHARGE_TARGET_REACHED"]

    d_excess [shape=diamond fillcolor="#fce8e8" label="excess \\< min_excess\\n(1010W)?"]
    r_insuf  [shape=box fillcolor="#fce8e8"
              label="return State 0\\nINSUFFICIENT_EXCESS"]

    pm [shape=box fillcolor="#c9b8f0"
        label="POWER_MATCHING\\nbudget = excess + max_grid_draw (1500W)\\nbest = höchster State mit STATE_POWER ≤ budget"]

    d_ramp [shape=diamond fillcolor="#fff3cd" label="best > relay_st\\nUND best > next_up?"]
    r_ramp [shape=box fillcolor="#fff3cd"
            label="best = get_next_state_up(relay_st)\\nRamp_LIMITED ({best}→{next})"]

    // ── apply_guards() ──────────────────────────────────────────────────
    StartG [shape=oval fillcolor="#f8d7da" label="apply_guards()"]

    g1 [shape=diamond fillcolor="#fce8e8" label="soc > 99?"]
    g1r [shape=box fillcolor="#f8d7da" label="best = 0\\nGUARD:BATTERY_FULL_STOP"]

    g2 [shape=diamond fillcolor="#fce8e8" label="0 ≤ soc \\< 6?"]
    g2r [shape=box fillcolor="#f8d7da" label="best = 1\\nGUARD:CRITICAL_SOC_PROTECTION"]

    // ── apply_blocking() ────────────────────────────────────────────────
    StartB [shape=oval fillcolor="#d4c5f0" label="apply_blocking()"]

    b_same [shape=diamond fillcolor="#d4c5f0" label="best == relay_st?"]
    b_same_r [shape=box fillcolor="#d4c5f0" label="return relay_st\\nchanged=False"]

    b_dir [shape=box fillcolor="#d4c5f0"
           label="direction = UP / DOWN\\npwr_diff = |P[best] - P[relay_st]|"]

    b_emf [shape=diamond fillcolor="#fce8e8" label="pcc \\< -1020W\\nund DOWN?"]
    b_emf_r [shape=box fillcolor="#f8d7da"
             label="EMERGENCY_FORCE\\nreturn best, changed=True"]

    b_rules [shape=box fillcolor="#d4c5f0"
             label="Blocking Rules prüfen:\\nUP:   SWEET_SPOT_HOLD\\n      TREND_BLOCK\\n      BAT_GUARD_BLOCK\\nDOWN: STABILIZING\\n      HYSTERESIS"]

    b_blocked [shape=diamond fillcolor="#d4c5f0" label="eine Regel\\ngreift?"]
    b_yes [shape=box fillcolor="#d4c5f0" label="return relay_st\\nchanged=False\\ntrace += | RULE"]
    b_no  [shape=box fillcolor="#d4c5f0" label="return best\\nchanged=True"]

    // ── decide() Flow ───────────────────────────────────────────────────
    Start       -> excess_calc
    excess_calc -> d_soc_unk
    d_soc_unk   -> r_soc_unk  [label="JA" color="red"]
    d_soc_unk   -> d_prot     [label="NEIN" color="green"]
    r_soc_unk   -> End
    d_prot      -> d_prot2    [label="JA" color="orange"]
    d_prot      -> d_excess   [label="NEIN" color="green"]
    d_prot2     -> r_emerg    [label="JA" color="red"]
    d_prot2     -> r_target   [label="NEIN" color="green"]
    r_emerg     -> End
    r_target    -> End
    d_excess    -> r_insuf    [label="JA" color="red"]
    d_excess    -> pm         [label="NEIN" color="green"]
    r_insuf     -> End
    pm          -> d_ramp
    d_ramp      -> r_ramp     [label="JA" color="orange"]
    d_ramp      -> End        [label="NEIN" color="green"]
    r_ramp      -> End

    // ── apply_guards() Flow ─────────────────────────────────────────────
    StartG -> g1
    g1 -> g1r [label="JA" color="red"]
    g1 -> g2  [label="NEIN" color="green"]
    g1r -> GEnd
    g2 -> g2r [label="JA" color="red"]
    g2 -> GEnd [label="NEIN" color="green"]
    g2r -> GEnd

    // ── apply_blocking() Flow ───────────────────────────────────────────
    StartB  -> b_same
    b_same  -> b_same_r [label="JA" color="#888888"]
    b_same  -> b_dir    [label="NEIN" color="green"]
    b_same_r -> BEnd
    b_dir   -> b_emf
    b_emf   -> b_emf_r  [label="JA" color="red"]
    b_emf   -> b_rules  [label="NEIN" color="green"]
    b_emf_r -> BEnd
    b_rules -> b_blocked
    b_blocked -> b_yes  [label="JA" color="orange"]
    b_blocked -> b_no   [label="NEIN" color="green"]
    b_yes -> BEnd
    b_no  -> BEnd
}
"""


def render(dot_src: str, out_path: Path) -> Path:
    dot_file = out_path.with_suffix(".dot")
    dot_file.write_text(dot_src)
    result = subprocess.run(
        ["dot", "-Tpdf", str(dot_file), "-o", str(out_path)],
        capture_output=True, text=True
    )
    dot_file.unlink()
    if result.returncode != 0:
        print(f"FEHLER {out_path.name}: {result.stderr[:200]}")
        return None
    print(f"OK  → {out_path}")
    return out_path


def main():
    pages = []
    for i, src in enumerate([PAGE1, PAGE2, PAGE3], 1):
        p = render(src, OUT / f"fox2db_v2_page{i}.pdf")
        if p:
            pages.append(str(p))

    if len(pages) == 3:
        out = OUT / "fox2db_v2_flowchart.pdf"
        result = subprocess.run(["pdfunite"] + pages + [str(out)],
                                capture_output=True, text=True)
        if result.returncode == 0:
            print(f"\nKombiniert → {out}")
            for p in pages:
                Path(p).unlink()
        else:
            print(f"pdfunite Fehler: {result.stderr}")
            print("Einzelne PDFs bleiben erhalten.")


if __name__ == "__main__":
    main()
