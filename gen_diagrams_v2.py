#!/usr/bin/env python3
# ════════════════════════════════════════════════════════════════════════════
#  Diagramm-Generator — NUR Waveshare-Version (fox2db v3.3.30, ESP32-S3 6CH)
#  Quelle: waveshare/fox2db_logic.h + waveshare/waveshare_6ch_esp32s3.md
#  Mehrseitiges PDF, Mindest-Schriftgröße 10.
# ════════════════════════════════════════════════════════════════════════════
import subprocess
from pathlib import Path

OUT = Path(__file__).parent

# ── Seite 1: Architektur / Schichten ─────────────────────────────────────────
PAGE1 = """
digraph WaveshareArch {
    graph [
        label="fox2db v3.3.30 — Waveshare ESP32-S3 6CH — Architektur (fox2db_logic.h, autonom auf dem ESP)"
        labelloc=t fontsize=14 fontname="Helvetica-Bold"
        rankdir=TB splines=ortho nodesep=0.6 ranksep=0.8
        bgcolor="#f8f9fa" size="11,17" ratio=fill
    ]
    node [fontname="Helvetica" fontsize=10 margin="0.2,0.08" style="filled,rounded"]
    edge [fontname="Helvetica" fontsize=10 penwidth=1.2]

    subgraph cluster_extern {
        label="Externe Systeme (MQTT 192.168.178.218:1883)" style="dashed,filled" fillcolor="#eeeeee"
        fontname="Helvetica-Bold" fontsize=10 color="#aaaaaa"

        MQTT_PCC [shape=cylinder fillcolor="#dddddd" label="WR-Daten (MQTT)\\npcc, bat1, soc1"]
        MQTT_EBOX [shape=cylinder fillcolor="#dddddd" label="ebox/pwr\\nsoc2, power_w (signiert!),\\ncurrent_a, packs"]
        MQTT_RATIO [shape=cylinder fillcolor="#dddddd" label="sofar/ratio\\nratio_th (default 0.5)\\nsofar/ladesperre, sofar/auto"]
        MQTT_OUT [shape=cylinder fillcolor="#dddddd" label="sofar/state · sofar/waveshare/status\\nsoyo/calc · soyo/sent (retain)"]
        Relays123 [shape=component fillcolor="#dddddd" label="CH1-CH3 (GPIO1/2/41)\\nEBox-State 0..7 binär"]
        Relay4 [shape=component fillcolor="#fce8e8" label="CH4 (GPIO42)\\nDO4 -> WR2 abregeln\\n3s-Puls"]
        Soyo [shape=component fillcolor="#dddddd" label="Soyo-Inverter (max 900W)\\nRS485 GPIO17 TX / GPIO18 RX\\nKeepalive alle 3s"]
    }

    subgraph cluster_input {
        label="INPUT (Inputs-Struct)" style="filled" fillcolor="#d4edda"
        fontname="Helvetica-Bold" fontsize=10 color="#28a745"

        in_struct [shape=box fillcolor="#b8dfc4" label="Inputs{pcc, bat1, soc1, soc2, ebox_w}\\nsoc2 < 0 = unbekannt"]
        state_ram [shape=box fillcolor="#b8dfc4" label="State (RAM, Cron-übergreifend)\\nrelay_st, stable, last_excess, prot,\\npeak_today, pcc_buf[10]\\n(Reset um Mitternacht, weg bei Reboot)"]
    }

    subgraph cluster_forecast {
        label="DC-FORECAST (Meinel-Klarhimmel)" style="filled" fillcolor="#fff3cd"
        fontname="Helvetica-Bold" fontsize=10 color="#b8860b"

        dc_now [shape=box fillcolor="#ffe69c" label="dc_now()  Süd+Ost-Arrays\\nNOAA-Sonnenstand, kt_month\\n47.6811N 11.5732E\\n-> dc_expected"]
        peakwin [shape=box fillcolor="#ffe69c" label="Peak-Fenster (Stunde 5..20)\\nbest_w, peak_h, win_end_h\\nratio_ist via pcc_buf-Mittel"]
    }

    subgraph cluster_decision {
        label="DECISION-PIPELINE (step())" style="filled" fillcolor="#e2d9f3"
        fontname="Helvetica-Bold" fontsize=10 color="#6f42c1"

        ladesperre [shape=box fillcolor="#d4c5f0" label="LADESPERRE-Zustandsmaschine\\nin_window && 0 <= ratio_ist <= ratio_th\\n(nur bei BELEGTEM Gutwetter)"]
        decide_fn [shape=box fillcolor="#c9b8f0" label="decide()  dominanzgeordnet\\nSOC_UNKNOWN / PCC_OVER_20KW /\\nEMERGENCY / INSUFFICIENT /\\nPOWER_MATCHING + RAMP_LIMITED"]
        guards [shape=box fillcolor="#f8d7da" label="apply_guards() — HARD\\nLADESPERRE / BATTERY_FULL /\\nCRITICAL_SOC (feuert -> Blocking skip)"]
        blocking [shape=box fillcolor="#d4c5f0" label="apply_blocking()\\nUP: SWEET_SPOT/TREND/BAT_GUARD\\nDOWN: STABILIZING/HYSTERESIS\\nEMERGENCY_FORCE bypass"]
    }

    subgraph cluster_output {
        label="OUTPUT (Result-Struct)" style="filled" fillcolor="#cce5ff"
        fontname="Helvetica-Bold" fontsize=10 color="#004085"

        set_relays [shape=box fillcolor="#a8d0f5" label="CH1-CH3 setzen\\nfinal_state als Bitmask"]
        do4_out [shape=box fillcolor="#f8d7da" label="DO4-Puls (CH4)\\npcc>22kW unbedingt ODER\\npcc>20kW kein State+ / ladesperre"]
        soyo_calc [shape=box fillcolor="#a8d0f5" label="Soyo-Kalkulation\\nEntladung wenn State==0\\nRS485-Frame alle 3s"]
        publish [shape=box fillcolor="#a8d0f5" label="MQTT publish\\nsofar/state, status, soyo/*"]
    }

    MQTT_PCC -> in_struct [color="#28a745"]
    MQTT_EBOX -> in_struct [color="#28a745"]
    MQTT_RATIO -> ladesperre [color="#28a745" label="ratio_th"]

    in_struct -> dc_now [color="#b8860b"]
    state_ram -> peakwin [color="#b8860b" label="pcc_buf"]
    dc_now -> peakwin [color="#b8860b"]

    peakwin -> ladesperre [color="#6f42c1" label="ratio_ist\\npeak_h"]
    in_struct -> decide_fn [color="#6f42c1"]
    state_ram -> decide_fn [color="#6f42c1" label="relay_st, prot"]
    ladesperre -> guards [color="#c0392b" label="ladesperre"]
    decide_fn -> guards [color="#6f42c1"]
    guards -> blocking [color="#6f42c1" label="kein Guard"]
    guards -> set_relays [color="#c0392b" style=dashed label="Guard feuert"]

    blocking -> set_relays [color="#004085"]
    blocking -> do4_out [color="#c0392b" style=dashed]
    blocking -> publish [color="#004085"]
    in_struct -> soyo_calc [color="#004085" style=dotted]

    set_relays -> Relays123 [color="#004085"]
    do4_out -> Relay4 [color="#c0392b"]
    soyo_calc -> Soyo [color="#004085" label="3s Keepalive"]
    publish -> MQTT_OUT [color="#004085"]
}
"""

# ── Seite 2: step() — Gesamtablauf ───────────────────────────────────────────
PAGE2 = """
digraph WaveshareStep {
    graph [
        label="fox2db v3.3.30 — step() Gesamtablauf (alle 60s, fox2db_logic.h)"
        labelloc=t fontsize=14 fontname="Helvetica-Bold"
        rankdir=TB splines=polyline nodesep=0.4 ranksep=0.5
        bgcolor="#f8f9fa" size="11,17" ratio=fill
    ]
    node [fontname="Helvetica" fontsize=10 margin="0.18,0.08" style="filled,rounded"]
    edge [fontname="Helvetica" fontsize=10 penwidth=1.2]

    Start [shape=oval fillcolor="#cce5ff" label="step(in, st, now_utc, ...)\\nESPHome-Intervall 60s"]
    End [shape=oval fillcolor="#cce5ff" label="return Result"]

    s1 [shape=diamond fillcolor="#fff3cd" label="local_yday != last_yday?\\n(Mitternacht)"]
    s1r [shape=box fillcolor="#d4edda" label="pcc_buf reset, peak_today=false\\nlast_yday = local_yday"]
    s2 [shape=box fillcolor="#fff3cd" label="dc_expected = dc_now(now_utc, month)\\nMeinel-Klarhimmel"]
    s3 [shape=box fillcolor="#d4edda" label="pcc_buf[i]=pcc (Ringpuffer 10)\\npcc_avg wenn n>=3"]
    s4 [shape=box fillcolor="#fff3cd" label="Peak-Fenster h=5..20\\nbest_w, peak_h_loc, win_end_loc\\npeak_h / win_end_h -> Result"]
    s5 [shape=box fillcolor="#fce8e8" label="if pcc>20kW: peak_today=true"]
    s6 [shape=box fillcolor="#fff3cd" label="ratio_ist = (dc_exp - (pcc_avg+ebox+bat1)) / dc_exp\\nnur wenn pcc_avg gültig und dc_exp>5000\\nsonst -1"]

    d_en [shape=diamond fillcolor="#fce8e8" label="ladesperre_enable?"]
    d_win [shape=diamond fillcolor="#fce8e8" label="in_window?\\nhas_peak && win_end>=0\\n&& !peak_today\\n&& local_hour<=peak_h"]
    d_rat [shape=diamond fillcolor="#fce8e8" label="0 <= ratio_ist\\n<= ratio_th?\\n(belegtes Gutwetter)"]
    r_lock [shape=box fillcolor="#f8d7da" label="ladesperre = true"]
    r_free [shape=box fillcolor="#d4edda" label="ladesperre = false"]

    s7 [shape=box fillcolor="#e2d9f3" label="best = decide(in, relay_st, prot, ...)\\n-> best, trace, excess"]
    s8 [shape=box fillcolor="#f8d7da" label="best = apply_guards(best, soc2, ladesperre)\\n-> guard_fired"]
    s9 [shape=box fillcolor="#e2d9f3" label="drop_rate = (excess - last_excess)/30\\nlast_excess = excess"]
    d_g [shape=diamond fillcolor="#fce8e8" label="guard_fired?"]
    s_byp [shape=box fillcolor="#f8d7da" label="final = best\\nchanged = (best != relay_st)\\n(Blocking übersprungen)"]
    s10 [shape=box fillcolor="#e2d9f3" label="final = apply_blocking(best, relay_st,\\n  pcc, bat1, stable, drop_rate)\\n-> final, changed"]
    s11 [shape=box fillcolor="#d4edda" label="stable = changed ? 0 : stable+1"]
    s12 [shape=box fillcolor="#e2d9f3" label="Deep-Discharge-Hysterese\\nsoc2<6 -> prot=true\\nsoc2>=8 -> prot=false"]
    d_do4 [shape=diamond fillcolor="#fce8e8" label="need_down?\\npcc>22kW ODER\\n(pcc>20kW &&\\n(kein PCC_OVER_20KW\\nim trace || ladesperre))"]
    s_do4 [shape=box fillcolor="#f8d7da" label="do4_pulse = true\\n(CH4 3s-Puls)"]
    s13 [shape=box fillcolor="#cce5ff" label="relay_st = final\\nResult füllen: final_state, changed,\\nexcess, dc_delta, ratio, peak_h, trace"]

    Start -> s1
    s1 -> s1r [label="JA" color="green"]
    s1 -> s2 [label="NEIN" color="#888888"]
    s1r -> s2
    s2 -> s3 -> s4 -> s5 -> s6 -> d_en
    d_en -> d_win [label="JA" color="green"]
    d_en -> r_free [label="NEIN" color="#888888"]
    d_win -> d_rat [label="JA" color="orange"]
    d_win -> r_free [label="NEIN" color="#888888"]
    d_rat -> r_lock [label="JA" color="red"]
    d_rat -> r_free [label="NEIN" color="green"]
    r_lock -> s7
    r_free -> s7
    s7 -> s8 -> s9 -> d_g
    d_g -> s_byp [label="JA" color="red"]
    d_g -> s10 [label="NEIN" color="green"]
    s_byp -> s11
    s10 -> s11 -> s12 -> d_do4
    d_do4 -> s_do4 [label="JA" color="red"]
    d_do4 -> s13 [label="NEIN" color="green"]
    s_do4 -> s13 -> End
}
"""

# ── Seite 3: decide / apply_guards / apply_blocking im Detail ─────────────────
PAGE3 = """
digraph WaveshareDecision {
    graph [
        label="fox2db v3.3.30 — decide() / apply_guards() / apply_blocking() im Detail"
        labelloc=t fontsize=14 fontname="Helvetica-Bold"
        rankdir=TB splines=polyline nodesep=0.4 ranksep=0.5
        bgcolor="#f8f9fa" size="11,17" ratio=fill
    ]
    node [fontname="Helvetica" fontsize=10 margin="0.18,0.08" style="filled,rounded"]
    edge [fontname="Helvetica" fontsize=10 penwidth=1.2]

    Start [shape=oval fillcolor="#e2d9f3" label="decide()"]
    End [shape=oval fillcolor="#e2d9f3" label="-> (best, trace, excess)"]

    excess_calc [shape=box fillcolor="#c9b8f0" label="ebox_eff = relay_st>0 ? max(ebox_w, state_power(relay_st)) : 0\\nexcess = pcc + ebox_eff + bat1"]
    d_unk [shape=diamond fillcolor="#fce8e8" label="soc2 < 0?\\n(unbekannt)"]
    r_unk [shape=box fillcolor="#fce8e8" label="return relay_st\\nEBOX_SOC_UNKNOWN_HOLD"]
    d_pcc [shape=diamond fillcolor="#f8d7da" label="pcc>20kW &&\\nsoc2<100?"]
    r_pcc [shape=box fillcolor="#f8d7da" label="return min(relay_st+1,7)\\nPCC_OVER_20KW"]
    d_prot [shape=diamond fillcolor="#fce8e8" label="prot aktiv?"]
    d_prot2 [shape=diamond fillcolor="#fce8e8" label="soc2 < 7%?"]
    r_emerg [shape=box fillcolor="#fce8e8" label="return 1\\nEMERGENCY_CHARGE_TO_7%"]
    r_target [shape=box fillcolor="#fce8e8" label="return 0\\nCHARGE_TARGET_REACHED"]
    pm [shape=box fillcolor="#c9b8f0" label="POWER_MATCHING\\nbudget = excess + 900W (MAX_GRID_DRAW)\\nbest = höchste state_power <= budget"]
    d_zero [shape=diamond fillcolor="#fce8e8" label="best == 0?\\n(Überschuss < State 1)"]
    r_insuf [shape=box fillcolor="#fce8e8" label="return 0\\nINSUFFICIENT_EXCESS"]
    d_ramp [shape=diamond fillcolor="#fff3cd" label="best > relay_st &&\\nbest > next_up\\n(SORTED_STATES)?"]
    r_ramp [shape=box fillcolor="#fff3cd" label="best = next_state_up\\nRAMP_LIMITED"]

    StartG [shape=oval fillcolor="#f8d7da" label="apply_guards(best, soc2, ladesperre)"]
    GEnd [shape=oval fillcolor="#f8d7da" label="-> best, guard_fired\\nfired=true -> Blocking skip"]
    g0 [shape=diamond fillcolor="#fce8e8" label="ladesperre?"]
    g0r [shape=box fillcolor="#f8d7da" label="best=0\\nGUARD:LADESPERRE_BIS_PCC_20KW"]
    g1 [shape=diamond fillcolor="#fce8e8" label="soc2 >= 100?"]
    g1r [shape=box fillcolor="#f8d7da" label="best=0\\nGUARD:BATTERY_FULL_STOP"]
    g2 [shape=diamond fillcolor="#fce8e8" label="0 <= soc2 < 6?"]
    g2r [shape=box fillcolor="#f8d7da" label="best=1\\nGUARD:CRITICAL_SOC_PROTECTION"]

    StartB [shape=oval fillcolor="#d4c5f0" label="apply_blocking()\\n(nur wenn !guard_fired)"]
    BEnd [shape=oval fillcolor="#d4c5f0" label="-> final, changed"]
    b_same [shape=diamond fillcolor="#d4c5f0" label="best == relay_st?"]
    b_same_r [shape=box fillcolor="#d4c5f0" label="return relay_st\\nchanged=false"]
    b_dir [shape=box fillcolor="#d4c5f0" label="up = power(best) > power(relay_st)\\npwr_diff = |power(best)-power(relay_st)|"]
    b_emf [shape=diamond fillcolor="#fce8e8" label="pcc<-1020W (=-(G+120))\\n&& !up?"]
    b_emf_r [shape=box fillcolor="#f8d7da" label="EMERGENCY_FORCE\\nreturn best, changed=true"]
    b_rules [shape=box fillcolor="#d4c5f0" label="UP:   SWEET_SPOT_HOLD (|pcc|<160)\\n      TREND_BLOCK (drop_rate<-20)\\n      BAT_GUARD_BLOCK (bat1<-110)\\nDOWN: STABILIZING (stable<2)\\n      HYSTERESIS (pwr_diff<505)"]
    b_blk [shape=diamond fillcolor="#d4c5f0" label="Regel greift?"]
    b_yes [shape=box fillcolor="#d4c5f0" label="return relay_st\\nchanged=false"]
    b_no [shape=box fillcolor="#d4c5f0" label="return best\\nchanged=true"]

    Start -> excess_calc -> d_unk
    d_unk -> r_unk [label="JA" color="red"]
    d_unk -> d_pcc [label="NEIN" color="green"]
    r_unk -> End
    d_pcc -> r_pcc [label="JA" color="red"]
    d_pcc -> d_prot [label="NEIN" color="green"]
    r_pcc -> End
    d_prot -> d_prot2 [label="JA" color="orange"]
    d_prot -> pm [label="NEIN" color="green"]
    d_prot2 -> r_emerg [label="JA" color="red"]
    d_prot2 -> r_target [label="NEIN" color="green"]
    r_emerg -> End
    r_target -> End
    pm -> d_zero
    d_zero -> r_insuf [label="JA" color="red"]
    d_zero -> d_ramp [label="NEIN" color="green"]
    r_insuf -> End
    d_ramp -> r_ramp [label="JA" color="orange"]
    d_ramp -> End [label="NEIN" color="green"]
    r_ramp -> End

    StartG -> g0
    g0 -> g0r [label="JA" color="red"]
    g0 -> g1 [label="NEIN" color="green"]
    g0r -> GEnd
    g1 -> g1r [label="JA" color="red"]
    g1 -> g2 [label="NEIN" color="green"]
    g1r -> GEnd
    g2 -> g2r [label="JA" color="red"]
    g2 -> GEnd [label="NEIN" color="green"]
    g2r -> GEnd

    StartB -> b_same
    b_same -> b_same_r [label="JA" color="#888888"]
    b_same -> b_dir [label="NEIN" color="green"]
    b_same_r -> BEnd
    b_dir -> b_emf
    b_emf -> b_emf_r [label="JA" color="red"]
    b_emf -> b_rules [label="NEIN" color="green"]
    b_emf_r -> BEnd
    b_rules -> b_blk
    b_blk -> b_yes [label="JA" color="orange"]
    b_blk -> b_no [label="NEIN" color="green"]
    b_yes -> BEnd
    b_no -> BEnd
}
"""

# ── Seite 4: LADESPERRE-Zustandsmaschine + Soyo-Entladung ─────────────────────
PAGE4 = """
digraph WaveshareSoyo {
    graph [
        label="fox2db v3.3.30 — LADESPERRE-Logik + Soyo-Entladung (RS485)"
        labelloc=t fontsize=14 fontname="Helvetica-Bold"
        rankdir=TB splines=polyline nodesep=0.4 ranksep=0.5
        bgcolor="#f8f9fa" size="11,17" ratio=fill
    ]
    node [fontname="Helvetica" fontsize=10 margin="0.18,0.08" style="filled,rounded"]
    edge [fontname="Helvetica" fontsize=10 penwidth=1.2]

    subgraph cluster_lade {
        label="LADESPERRE — Akku morgens leer halten für >20kW-Mittagspeak" style="filled" fillcolor="#fff3cd"
        fontname="Helvetica-Bold" fontsize=10 color="#b8860b"

        L0 [shape=oval fillcolor="#ffe69c" label="step() — pro Zyklus neu bewertet\\n(zustandslos, kein DB-Event)"]
        L1 [shape=diamond fillcolor="#fce8e8" label="has_peak?\\nbest_w > 20kW heute"]
        L2 [shape=diamond fillcolor="#fce8e8" label="local_hour <= peak_h?"]
        L3 [shape=diamond fillcolor="#fce8e8" label="!peak_today?\\n(pcc hat 20kW\\nnoch nicht erreicht)"]
        L4 [shape=diamond fillcolor="#fff3cd" label="ratio_ist gültig\\n(>= 0)?"]
        L5 [shape=diamond fillcolor="#fff3cd" label="ratio_ist <= ratio_th?\\n(default 0.5,\\nMQTT sofar/ratio)"]
        LON [shape=box fillcolor="#f8d7da" label="LADESPERRE AKTIV\\n-> Guard -> State 0\\nbelegtes Gutwetter"]
        LOFF [shape=box fillcolor="#d4edda" label="LADESPERRE OFF (Default)\\nSchlechtwetter / unbeurteilbar /\\nPeak gesehen / Stunde überschritten"]

        L0 -> L1
        L1 -> L2 [label="JA" color="green"]
        L1 -> LOFF [label="NEIN" color="#888888"]
        L2 -> L3 [label="JA" color="green"]
        L2 -> LOFF [label="NEIN\\npeak_h überschritten" color="#888888"]
        L3 -> L4 [label="JA" color="green"]
        L3 -> LOFF [label="NEIN\\npeak_today" color="#888888"]
        L4 -> L5 [label="JA" color="green"]
        L4 -> LOFF [label="NEIN\\nratio<0 unbeurteilbar" color="#888888"]
        L5 -> LON [label="JA" color="red"]
        L5 -> LOFF [label="NEIN\\nratio>th Schlechtwetter" color="orange"]
    }

    subgraph cluster_soyo {
        label="Soyo-Entladung (max 900W, alle 60s soyo/calc, RS485-TX alle 3s)" style="filled" fillcolor="#d4edda"
        fontname="Helvetica-Bold" fontsize=10 color="#28a745"

        S0 [shape=oval fillcolor="#b8dfc4" label="soyo/calc"]
        Sd1 [shape=diamond fillcolor="#fce8e8" label="State != 0?\\n(EBox lädt)"]
        Sd2 [shape=diamond fillcolor="#fce8e8" label="soc2 < 9%?\\n(Entladeschutz)"]
        Sd3 [shape=diamond fillcolor="#fce8e8" label="ebox > 200W?\\n(bat2 lädt)"]
        Sd4 [shape=diamond fillcolor="#fce8e8" label="pcc > 200W?\\n(PV-Überschuss)"]
        Sd5 [shape=diamond fillcolor="#fff3cd" label="pcc < -100W?\\n(Netzbezug)"]
        Sw0 [shape=box fillcolor="#dddddd" label="w = 0"]
        Swc [shape=box fillcolor="#b8dfc4" label="w = |pcc| * 1.01\\n(+ Nacht: +468W)"]
        Sws [shape=box fillcolor="#b8dfc4" label="w = 468W (Nacht)\\noder 10W (Tag, Standby)"]
        Stx [shape=box fillcolor="#a8d0f5" label="RS485-Frame\\n[24 56 00 21 PH PL 80 CRC]\\nCRC=(264-PH-PL)&0xFF\\nimmer alle 3s (Keepalive 4s)"]

        S0 -> Sd1
        Sd1 -> Sw0 [label="JA" color="#888888"]
        Sd1 -> Sd2 [label="NEIN" color="green"]
        Sd2 -> Sw0 [label="JA" color="#888888"]
        Sd2 -> Sd3 [label="NEIN" color="green"]
        Sd3 -> Sw0 [label="JA" color="#888888"]
        Sd3 -> Sd4 [label="NEIN" color="green"]
        Sd4 -> Sw0 [label="JA" color="#888888"]
        Sd4 -> Sd5 [label="NEIN" color="green"]
        Sd5 -> Swc [label="JA" color="orange"]
        Sd5 -> Sws [label="NEIN" color="green"]
        Sw0 -> Stx
        Swc -> Stx
        Sws -> Stx
    }
}
"""

PAGES = [PAGE1, PAGE2, PAGE3, PAGE4]


def render(dot_src: str, out_path: Path) -> Path:
    dot_file = out_path.with_suffix(".dot")
    dot_file.write_text(dot_src)
    result = subprocess.run(
        ["dot", "-Tpdf", str(dot_file), "-o", str(out_path)],
        capture_output=True, text=True
    )
    # .dot bleibt erhalten (wird mit eingecheckt)
    if result.returncode != 0:
        print(f"FEHLER {out_path.name}: {result.stderr[:200]}")
        return None
    print(f"OK  -> {out_path}")
    return out_path


def main():
    pages = []
    for i, src in enumerate(PAGES, 1):
        p = render(src, OUT / f"waveshare_v3_page{i}.pdf")
        if p:
            pages.append(str(p))

    if len(pages) == len(PAGES):
        out = OUT / "waveshare_v3_flowchart.pdf"
        result = subprocess.run(["pdfunite"] + pages + [str(out)],
                                capture_output=True, text=True)
        if result.returncode == 0:
            print(f"\nKombiniert -> {out}")
            # Einzelseiten bleiben erhalten (werden mit eingecheckt)
        else:
            print(f"pdfunite Fehler: {result.stderr}")


if __name__ == "__main__":
    main()
