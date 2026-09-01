#pragma once
// ════════════════════════════════════════════════════════════════════════════
//  fox2db v2.9 — Steuerlogik als C++-Port für ESPHome (Waveshare 6CH ESP32-S3)
//
//  Faithful port von fox2db.py:  decide() -> apply_guards() -> apply_blocking()
//  + DC-Klarhimmel-Forecast (NOAA-Sonnenstand) + Ladesperre-Zustandsmaschine.
//
//  Unterschiede zum Pi:
//   - KEIN DB-Logging  → Entscheidungen werden per MQTT publiziert (Pi schreibt DB)
//   - Tages-Flags (do4_today/weather/reblock) liegen im RAM (reset um Mitternacht,
//     gehen bei Reboot verloren — für ersten Test ok)
//   - Nur SOC2 steuert; SOC1 (Sofar) ist autark
// ════════════════════════════════════════════════════════════════════════════
#include <math.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

namespace fox {

// ── CONFIG (1:1 aus fox2db.py) ───────────────────────────────────────────────
constexpr float MIN_EXCESS       = 2500.0f;  // Mindest-Überschuss zum Laden (State 1)
constexpr float MAX_GRID_DRAW    = 900.0f;
constexpr float MAX_SOC          = 100.0f;
constexpr float HYSTERESIS       = 505.0f;
constexpr int   STABILIZATION    = 2;
constexpr int   PCC_AVG_N        = 3;         // Ringpuffer fuer den PCC-Ersatzwert (3 Min)
constexpr float RAMP_FREE_FACTOR = 1.3f;      // Ueberschuss >= Zielstufe * Faktor -> Rampe entfaellt
constexpr float EMERGENCY_MARGIN = 120.0f;                          // harter Abwurf erst bei G + Margin
constexpr float EMERGENCY_IMPORT = MAX_GRID_DRAW + EMERGENCY_MARGIN; // = 1020 W (an G gekoppelt)
constexpr float BAT_DISCHARGE_TH = -110.0f;
constexpr float SWEET_SPOT_PCC   = 160.0f;
constexpr float MAX_DROP_RATE    = -20.0f;
constexpr float LADESPERRE_HYST  = 0.25f;   // Hysterese-Band für Ladesperre-Ratio (Latch gegen Flattern); breit genug gegen rohes pcc-Rauschen (Release 0.62+0.25=0.87)
constexpr float LADESPERRE_NOW_RATIO = 0.20f;  // Momentan-Ratio, ab der Bewoelkung als belegt gilt
constexpr int   DD_LOWER         = 6;
constexpr int   DD_UPPER         = 8;
constexpr int   DD_CHARGE_TARGET = 7;
constexpr float PCC_PEAK_TH      = 20000.0f;
constexpr float PCC_HARD_TH      = 22000.0f;  // (nicht mehr im DO4-Pfad, s. need_down)
constexpr float DO4_EBOX_FULL_W  = 11000.0f;  // ab hier laedt die EBox wirklich voll
                                              // (Default; per MQTT sofar/do4_full setzbar)
// Ladesperre nur im Sommerhalbjahr. Ausserhalb wird ausschliesslich ueber DO4
// abgeregelt (>20 kW), der Akku laedt sofort. Harte Schranke, damit die Sperre
// nicht ueber geaenderte Feldleistungen wieder in Fruehjahr/Herbst rutscht.
constexpr int LADESPERRE_MONTH_FROM = 5;   // Mai
constexpr int LADESPERRE_MONTH_TO   = 8;   // August
// SOC1-Tor: Der Sofar-Hausakku hat Vorrang. Solange er heute nicht einmal
// ueber SOC1_FULL_TH stand, darf die EBox hoechstens SOC1_GATE_MAX_W ziehen —
// erst danach sind die grossen Stufen frei. Tages-Latch wie peak_today: einmal
// gesehen genuegt, ein spaeterer Abfall schliesst das Tor nicht wieder.
constexpr float SOC1_FULL_TH     = 99.0f;    // "einmal >99 %" gesehen
constexpr float SOC1_GATE_MAX_W  = 5000.0f;  // Deckel bis dahin
constexpr float NIGHT_DC_TH      = 100.0f;    // gemessene PV (Power_PV1+PV2) < 100W → "Nacht" (Soyo-Baseline 468W). PV-String statt pcc: batterieunabhängig & ehrlich

// STATE_TO_POWER {0:0,1:3000,2:3650,3:6650,4:3900,5:7100,6:7800,7:11400}
inline int state_power(int s) {
  static const int P[8] = {0, 3000, 3650, 6650, 3900, 7100, 7800, 11400};
  return (s >= 0 && s <= 7) ? P[s] : 0;
}
// SORTED_STATES nach Leistung sortiert, OHNE State 2 (ch2 nur in Kombination erlaubt): [0,1,4,3,5,6,7]
static const int N_SORTED = 7;
static const int SORTED_STATES[N_SORTED] = {0, 1, 4, 3, 5, 6, 7};
inline int sorted_index(int s) {
  for (int i = 0; i < N_SORTED; i++) if (SORTED_STATES[i] == s) return i;
  return 0;
}
// Hoechste Stufe, die den Leistungsdeckel nicht reisst. Gesucht wird in
// SORTED_STATES, damit State 2 auch hier aussen vor bleibt (wie in decide()).
inline int state_capped(int s, float max_w) {
  if ((float)state_power(s) <= max_w) return s;
  int best = 0, best_pow = -1;
  for (int i = 0; i < N_SORTED; i++) {
    int p = state_power(SORTED_STATES[i]);
    if ((float)p <= max_w && p > best_pow) { best = SORTED_STATES[i]; best_pow = p; }
  }
  return best;
}

// ── DC-Klarhimmel-Forecast (Meinel-Modell wie _DcForecast) ───────────────────
constexpr double LAT = 47.6811, LON = 11.5732;
struct Arr { double tilt, azS, power; };   // tilt, Azimut-Süd, Nennleistung
// Feldparameter je String gefittet (gen_pv_strings.py, 14 klare Tage):
// Sofar  PV1 West / PV2 Sued,  FoxESS pv2 Ost / pv1 West.
static const Arr ARRAYS[2]      = {{60, 33, 19430}, {68, -12, 7690}};
static const Arr ARRAYS_EAST[2] = {{59, -29, 22036}, {67, 32, 2781}};
// Standorthorizont: hoher Baumbestand im Ostsektor. Unter der Baumlinie bleibt
// nur Diffusstrahlung. Der Ertrag setzt dadurch ganzjaehrig rund zwei Stunden
// nach Sonnenaufgang ein (Messung: Kante bei 22-26 Grad, s. clearsky.tex).
constexpr double HOR_AZ_SPLIT = 120.0;   // Grenze Ost-/Suedsektor (Azimut von Nord)
constexpr double HOR_EAST     = 23.0;    // Elevation, unter der Ost verschattet ist
constexpr double HOR_SOUTH    = 15.0;    // dito Suedost
constexpr double HOR_DIFFUSE  = 0.25;    // Restanteil im Schatten
inline double kt_month(int m) {
  static const double K[13] = {0, .331, .402, .563, .838, .909, .880,
                               .840, .820, .760, .600, .350, .134};
  return (m >= 1 && m <= 12) ? K[m] : 0.60;
}
inline double d2r(double d) { return d * M_PI / 180.0; }

// NOAA-Sonnenstand: Elevation + Azimut (von Nord, im Uhrzeigersinn) für unix-UTC.
inline void sun_pos(time_t t_utc, double &elev_deg, double &az_deg) {
  struct tm g; gmtime_r(&t_utc, &g);
  double hour = g.tm_hour + g.tm_min / 60.0 + g.tm_sec / 3600.0;
  double gamma = 2.0 * M_PI / 365.0 * (g.tm_yday + (hour - 12) / 24.0);
  double eqtime = 229.18 * (0.000075 + 0.001868 * cos(gamma) - 0.032077 * sin(gamma)
                  - 0.014615 * cos(2 * gamma) - 0.040849 * sin(2 * gamma));
  double decl = 0.006918 - 0.399912 * cos(gamma) + 0.070257 * sin(gamma)
              - 0.006758 * cos(2 * gamma) + 0.000907 * sin(2 * gamma)
              - 0.002697 * cos(3 * gamma) + 0.00148 * sin(3 * gamma);
  double tst = hour * 60.0 + eqtime + 4.0 * LON;       // tz = UTC
  double ha = tst / 4.0 - 180.0;                        // Stundenwinkel (Grad)
  double har = d2r(ha), latr = d2r(LAT);
  double cosz = sin(latr) * sin(decl) + cos(latr) * cos(decl) * cos(har);
  cosz = fmax(-1.0, fmin(1.0, cosz));
  elev_deg = 90.0 - acos(cosz) * 180.0 / M_PI;
  double az_s = atan2(sin(har), cos(har) * sin(latr) - tan(decl) * cos(latr));
  az_deg = fmod(az_s * 180.0 / M_PI + 180.0 + 360.0, 360.0);  // 0=N
}

inline double cos_aoi(double elev, double azN, double tilt, double azS) {
  double e = d2r(elev), b = d2r(tilt), da = d2r((azN - 180.0) - azS);
  return sin(e) * cos(b) + cos(e) * sin(b) * cos(da);
}
inline double calc_arrays(const Arr *a, int n, time_t t, int month) {
  double elev, azN; sun_pos(t, elev, azN);
  if (elev <= 0) return 0.0;
  double am = fmin(1.0 / sin(d2r(elev)), 37.0);
  double T = pow(0.7, pow(am, 0.678));
  double kt = kt_month(month);
  // Horizont: steht die Sonne unter der Baumlinie, nur Diffusanteil.
  double hor = (azN < HOR_AZ_SPLIT) ? HOR_EAST : HOR_SOUTH;
  double shade = (elev >= hor) ? 1.0 : HOR_DIFFUSE;
  double sum = 0;
  for (int i = 0; i < n; i++)
    sum += a[i].power * T * kt * fmax(0.0, cos_aoi(elev, azN, a[i].tilt, a[i].azS));
  return sum * shade;
}
inline double dc_now(time_t t, int month) {
  return calc_arrays(ARRAYS, 2, t, month) + calc_arrays(ARRAYS_EAST, 2, t, month);
}

// Astronomischer lokaler Mittag (Sonnen-Meridiandurchgang, Stundenwinkel ha=0) als unix-UTC.
// NOAA-eqtime bei 12 UTC ausgewertet (Tagesvariation der eqtime vernachlässigbar).
inline time_t solar_noon_utc(time_t ref_utc) {
  time_t utc_midnight = (ref_utc / 86400) * 86400;
  time_t tmid = utc_midnight + 12 * 3600;
  struct tm g; gmtime_r(&tmid, &g);
  int yday0 = g.tm_yday;                              // (hour-12)/24 = 0 am Mittag
  double gamma = 2.0 * M_PI / 365.0 * yday0;
  double eqtime = 229.18 * (0.000075 + 0.001868 * cos(gamma) - 0.032077 * sin(gamma)
                            - 0.014615 * cos(2 * gamma) - 0.040849 * sin(2 * gamma));
  double hour_utc = (720.0 - eqtime - 4.0 * LON) / 60.0;  // tst = 720 min ⇒ ha = 0
  return utc_midnight + (time_t)(hour_utc * 3600.0);
}

// ── Temperatur-Derating (defensiv) ───────────────────────────────────────────
// Konservativer Datenblatt-Koeffizient (DAH 420W N-Type, -0.30 %/°C), NOCT-Zelltemp.
// Greift durch sin(Elevation) nur bei hohem Sonnenstand spürbar (dort gilt der
// Effekt real). Wird NUR angewendet, wenn eine gültige Außentemp vorliegt.
static const double TEMP_COEFF = -0.0030;
static const double NOCT       = 45.0;
static const double T_STC      = 25.0;
inline double dc_temp_factor(double elev_deg, double ambient) {
  double g = fmax(0.0, sin(d2r(elev_deg)));                 // POA-Anteil 0..1 (klar)
  double cell = ambient + (NOCT - 20.0) / 800.0 * 1000.0 * g;
  double f = 1.0 + TEMP_COEFF * (cell - T_STC);
  return fmax(0.85, fmin(1.0, f));                          // defensiv geklemmt
}

// ── ETA bis SOC 100 % (Port von expectsoc100.py, gleiche Formeln) ───────────
// Trend statt Modell: SOC-Steigung und Ladeleistung kommen aus dem Messfenster,
// der Restertrag aus dc_now() mal Bewoelkung. Teuer ist allein die Integration,
// deshalb laeuft sie nur jeden ETA_EVERY-ten Zyklus, dazwischen zaehlt der Cache.
constexpr float ETA_LOAD_W = 800.0f;    // Grundlast, die vor der EBox bedient wird
constexpr float ETA_K_DEF  = 0.32f;     // kWh(AC) je SOC-Prozent (30-kWh-EBox)
constexpr float ETA_K_MIN  = 0.20f;     // Plausibilitaetsfenster fuer den gemessenen
constexpr float ETA_K_MAX  = 0.50f;     // kWh/%-Wert; ausserhalb gilt ETA_K_DEF
constexpr int   ETA_WIN     = 30;       // Messfenster [min] = Ringpuffergroesse
constexpr int   ETA_MIN_N   = 10;       // Trend erst ab so vielen Stuetzstellen
constexpr int   ETA_STEP_S  = 300;      // Integrationsschritt [s]
constexpr int   ETA_END_H   = 21;       // Rechenhorizont (lokale Stunde)
constexpr int   ETA_EVERY   = 5;        // Neuberechnung nur jeden n-ten Zyklus

// ── Zustand (im RAM, über Cron-Zyklen hinweg) ────────────────────────────────
struct State {
  int   relay_st = 0;
  int   stable = 0;
  float last_excess = 0;
  bool  prot = false;               // Tiefentladeschutz (Hysterese)
  bool  peak_today = false;         // pcc hat heute PCC_PEAK_TH überschritten → Laden vor DO4
  bool  ladesperre_latched = false; // Ladesperre-Latch (Hysterese gegen Flattern)
  bool  badweather_today = false;   // Momentanleistung lag heute >= LADESPERRE_NOW_RATIO unter Klarhimmel
  bool  soc1_full_today = false;    // SOC1 stand heute einmal ueber SOC1_FULL_TH -> Deckel faellt
  int   last_yday = -1;
  // ETA-Trendfenster (siehe eta_soc100_h)
  float soc_hist[ETA_WIN] = {0};    // SOC-Ringpuffer, ein Wert je Zyklus
  int   soc_n = 0, soc_i = 0;
  float ebox_ema  = -1.0f;          // Ladeleistung, EMA ueber ETA_WIN
  float clear_ema = -1.0f;          // Bewoelkung (1-ratio), EMA ueber ETA_WIN
  float ebox_max_today = 0;         // Ladedeckel = Tagesmaximum der Ladeleistung
  float pcc3_buf[PCC_AVG_N] = {0};  // PCC-Ringpuffer fuer das 3-Min-Mittel
  int   pcc3_n = 0, pcc3_i = 0;
  int   eta_tick = 0;
  float exp_max_cache   = -1.0f;    // Prognose-Cache zwischen den Neuberechnungen
  float exp_max_h_cache = 0.0f;
};

// pcc_avg5 + bat1_avg5: 5-Min-Mittel aus pivot2db/MariaDB, nur für die Wetter-Ratio.
// Beide über dasselbe Fenster gemittelt → Akku-voll-Sprung (bat1→pcc) ist neutral.
struct Inputs { float pcc, bat1, soc1, soc2, ebox_w, bat1_avg5, pcc_avg5;
                float aussen_temp = 0; bool aussen_valid = false;
                bool  pcc_valid = true; };   // false = PCC kam als null (Lesefehler)

struct Result {
  int   final_state = 0;
  bool  changed = false;
  bool  do4_pulse = false;
  bool  ladesperre = false;
  float excess = 0, dc_expected = 0, dc_delta = 0;
  float ratio = -1.0f;  // Ist-Wetter-Ratio (Anteil fehlender Klarhimmel-Leistung; -1 = nicht berechenbar)
  float ratio_now = -1.0f; // dieselbe Groesse aus UNGEMITTELTEN Werten (Wolkenluecken)
  int   peak_h = -1;    // lokale Stunde des DC-Peaks (-1 = kein Peak heute)
  int   win_end_h = -1; // letzte lokale Stunde mit dc > PCC_PEAK_TH
  float noon_h = -1.0f; // astronomischer lokaler Mittag (Dezimalstunde, Reporting)
  float exp_max   = -1.0f; // hoechster heute erwarteter SOC in % (-1 = nicht beurteilbar)
  float exp_max_h = 0.0f;  // Uhrzeit dazu (lokale Dezimalstunde)
  bool  soc1_gate = false; // true = SOC1-Deckel aktiv (SOC1 heute noch nie >99 %)
  char  trace[160] = "";
};

inline void tcat(char *t, const char *s) { strncat(t, s, 159 - strlen(t)); }

// ── decide() ─────────────────────────────────────────────────────────────────
// pcc_excess = normalerweise der gueltige Minutenwert der Sofar (in.pcc). Nur
// wenn der PCC als null hereinkommt (Lesefehler, in.pcc_valid == false), tritt
// das 3-Min-Mittel der letzten gueltigen Werte an seine Stelle — statt der 0,
// die num() aus einem null macht. Gemittelt wird also nicht geglaettet: ein
// echter Einbruch schlaegt weiter sofort durch, sonst wird waehrend einer Wolke
// aus dem Netz geladen.
inline int decide(const Inputs &in, int relay_st, bool prot, float bat1_factor,
                  float pcc_excess, char *trace, float *excess_out) {
  float ebox_eff = (relay_st > 0) ? fmaxf(in.ebox_w, (float)state_power(relay_st)) : 0.0f;
  // Sofar-Entladung (bat1<0) zählt voll; von der Sofar-Ladung (bat1>0) nur der Anteil bat1_factor.
  float bat1_eff = (in.bat1 < 0.0f) ? in.bat1 : in.bat1 * bat1_factor;
  float excess = pcc_excess + ebox_eff + bat1_eff;
  if (in.soc2 < 0) {
    float ea = (relay_st > 0) ? in.ebox_w : 0.0f;
    *excess_out = pcc_excess + ea + bat1_eff;
    strcpy(trace, "EBOX_SOC_UNKNOWN_HOLD");
    return relay_st;
  }
  // DO4-Gefahr: sofort auf Stufe 7. Stufenweises Hochrampen kostet Minuten, in
  // denen die Einspeisung ueber 20 kW bleibt und DO4 den WR2 abwirft — die volle
  // Aufnahme ist immer billiger als die Abregelung.
  if (in.pcc > PCC_PEAK_TH && in.soc2 < MAX_SOC) {
    int next_st = 7;
    if (next_st > relay_st) {
      snprintf(trace, 80, "PCC_OVER_20KW (SOC=%.0f%% State%d->%d)", in.soc2, relay_st, next_st);
      *excess_out = excess; return next_st;
    }
  }
  if (prot) {
    *excess_out = excess;
    if (in.soc2 < DD_CHARGE_TARGET) { snprintf(trace, 80, "EMERGENCY_CHARGE_TO_7%% (%.1f%%)", in.soc2); return 1; }
    snprintf(trace, 80, "CHARGE_TARGET_REACHED (%.1f%%)", in.soc2); return 0;
  }
  if (excess < MIN_EXCESS) {
    snprintf(trace, 80, "INSUFFICIENT_EXCESS (%.0fW)", excess); *excess_out = excess; return 0;
  }
  float budget = excess + MAX_GRID_DRAW;
  int best = 0, best_pow = -1;
  for (int s = 0; s <= 7; s++) { if (s == 2) continue; int p = state_power(s); if (p <= budget && p > best_pow) { best = s; best_pow = p; } }
  snprintf(trace, 120, "POWER_MATCHING (Excess: %.0fW, Budget: %.0fW)", excess, budget);
  // Rampe nur, wenn der Ueberschuss die Zielstufe nicht klar traegt. Sie schuetzt
  // vor Netzbezug bei knappem Ueberschuss — bei 19 kW Sonne und 11,4 kW Zielstufe
  // gibt es nichts abzutasten, dann kostet jeder Zwischenschritt nur Ertrag.
  if (best > relay_st && excess >= (float)state_power(best) * RAMP_FREE_FACTOR) {
    char tmp[56]; snprintf(tmp, sizeof(tmp), " | RAMP_FREE (%.0fW traegt State%d)", excess, best);
    tcat(trace, tmp);
  } else if (best > relay_st) {                // Ramp-Limiting (State-Nr.-Vergleich, wie Python)
    int idx = sorted_index(relay_st);
    int next_st = SORTED_STATES[(idx + 1 < N_SORTED) ? idx + 1 : N_SORTED - 1];
    if (best > next_st) {
      char tmp[48]; snprintf(tmp, sizeof(tmp), " | RAMP_LIMITED (%d->%d)", best, next_st);
      tcat(trace, tmp); best = next_st;
    }
  }
  *excess_out = excess; return best;
}

// ── apply_guards() — feuert -> Blocking wird übersprungen ────────────────────
inline int apply_guards(int best, float soc2, float pcc, bool ladesperre, bool soc1_gate,
                        char *trace, bool *guard_fired) {
  // Reihenfolge: volle Batterie schlaegt alles (sie kann nichts mehr aufnehmen,
  // dann muss DO4 ran). Danach die DO4-Gefahr — sie schlaegt Ladesperre und
  // Tiefentladeschutz, weil beide durch Laden erfuellt statt verletzt werden,
  // und sie muss am Ramp-/Stabilize-Blocking vorbei: genau das hat bisher den
  // Hochlauf verzoegert (Trace "PCC_OVER_20KW ... | STABILIZING").
  if (soc2 >= MAX_SOC) { if (best != 0) tcat(trace, " | GUARD:BATTERY_FULL_STOP"); *guard_fired = true; return 0; }
  if (soc2 >= 0 && pcc > PCC_PEAK_TH) {
    if (best != 7) tcat(trace, " | GUARD:DO4_RISK_FORCE_STATE7");
    *guard_fired = true; return 7;
  }
  if (ladesperre) { if (best != 0) tcat(trace, " | GUARD:CHARGE_BLOCK_UNTIL_PCC_20KW"); *guard_fired = true; return 0; }
  if (soc2 >= 0 && soc2 < DD_LOWER) { if (best != 1) tcat(trace, " | GUARD:CRITICAL_SOC_PROTECTION_ACTIVATE"); *guard_fired = true; return 1; }
  // SOC1-Tor: Der Sofar-Akku hat Vorrang — mehr als SOC1_GATE_MAX_W erst, wenn
  // er heute einmal ueber SOC1_FULL_TH stand. Anders als die vier Regeln darueber
  // waehlt das Tor keine Stufe aus, es begrenzt nur die schon gewaehlte; deshalb
  // steht es am Ende und setzt guard_fired NICHT. Zwei Folgen, beide gewollt:
  // die vorherigen Guards kehren vorher zurueck, ihr erzwungener State bleibt
  // also unangetastet (bei pcc > 20 kW gewinnt Stufe 7 und DO4 bleibt intakt),
  // und apply_blocking() laeuft weiter — Rampe, Sweet-Spot- und Bat-Guard-Schutz
  // gelten auch fuer den gedeckelten Hochlauf.
  if (soc1_gate) {
    int capped = state_capped(best, SOC1_GATE_MAX_W);
    if (capped != best) {
      char t[64];
      snprintf(t, sizeof(t), " | SOC1_GATE (max %.0fW) State%d->%d",
               SOC1_GATE_MAX_W, best, capped);
      tcat(trace, t);
      best = capped;
    }
  }
  *guard_fired = false; return best;
}

// ── apply_blocking() ─────────────────────────────────────────────────────────
inline int apply_blocking(int best, int relay_st, float pcc, float bat1, int stable,
                          float drop_rate, char *trace, bool *changed) {
  if (best == relay_st) { *changed = false; return relay_st; }
  float pwr_diff = fabsf((float)(state_power(best) - state_power(relay_st)));
  bool up = state_power(best) > state_power(relay_st);
  if (pcc < -EMERGENCY_IMPORT && !up) {
    char t[48]; snprintf(t, 48, " | EMERGENCY_FORCE (Import=%.0fW)", pcc); tcat(trace, t);
    *changed = true; return best;
  }
  if (up && fabsf(pcc) < SWEET_SPOT_PCC)                        { tcat(trace, " | SWEET_SPOT_HOLD"); *changed = false; return relay_st; }
  if (up && drop_rate < MAX_DROP_RATE && drop_rate != 0)         { tcat(trace, " | TREND_BLOCK");     *changed = false; return relay_st; }
  if (up && bat1 < BAT_DISCHARGE_TH)                            { tcat(trace, " | BAT_GUARD_BLOCK"); *changed = false; return relay_st; }
  // STABILIZING daempft Pendeln zwischen Nachbarstufen und gilt nur abwaerts —
  // aufwaerts haelt es sonst 11 kW zurueck, waehrend 19 kW Ueberschuss anstehen.
  // (fox2dbEasy.py hatte hier schon immer "not up"; der Header war strenger.)
  if (!up && stable < STABILIZATION)                           { tcat(trace, " | STABILIZING");     *changed = false; return relay_st; }
  if (!up && pwr_diff < HYSTERESIS)                            { tcat(trace, " | HYSTERESIS");      *changed = false; return relay_st; }
  *changed = true; return best;
}

// ── Prognose: hoechster heute erreichbarer SOC und wann (1:1 wie expectsoc100.py)
// Ablauf je Zyklus: Fenster fortschreiben (ein Ringpuffer-Schreibzugriff und
// zwei EMA-Schritte). Nur jeden ETA_EVERY-ten Zyklus wird integriert:
//   P(t)     = clamp(dc_now(t) * Bewoelkung - Grundlast, 0, Ladedeckel)
//   soc(t)   = soc2 + Integral(P) / kWh_pro_Prozent   (aus dem Messfenster)
//   exp_max  = min(100, soc(Tagesende)),  exp_max_h = wann dieses Maximum steht
// Wird 100 % erreicht, ist exp_max_h die Uhrzeit dafuer; sonst die Uhrzeit, zu
// der der letzte Ertrag hereinkommt (Baumlinie/Sonnenuntergang) — danach steigt
// der SOC heute nicht mehr. exp_max = -1 heisst "nicht beurteilbar" (Nacht,
// SOC unbekannt, noch keine Bewoelkungsschaetzung).
inline void predict_max_soc(State &st, const Inputs &in, Result &r,
                            time_t now_utc, int local_sec_day, int month) {
  // Fenster fortschreiben — billig, laeuft jeden Zyklus.
  st.soc_hist[st.soc_i] = in.soc2;
  st.soc_i = (st.soc_i + 1) % ETA_WIN;
  if (st.soc_n < ETA_WIN) st.soc_n++;
  const float a = 1.0f / (float)ETA_WIN;
  st.ebox_ema = (st.ebox_ema < 0) ? in.ebox_w
                                  : st.ebox_ema + a * (in.ebox_w - st.ebox_ema);
  if (r.ratio >= 0.0f) {                       // Bewoelkung = 1 - Wetter-Ratio
    float cl = fmaxf(0.0f, fminf(1.0f, 1.0f - r.ratio));
    st.clear_ema = (st.clear_ema < 0) ? cl : st.clear_ema + a * (cl - st.clear_ema);
  }
  if (in.ebox_w > st.ebox_max_today) st.ebox_max_today = in.ebox_w;

  if (--st.eta_tick > 0) {                     // dazwischen: Cache, keine Rechnung
    r.exp_max = st.exp_max_cache; r.exp_max_h = st.exp_max_h_cache; return;
  }
  st.exp_max_cache = -1.0f; st.exp_max_h_cache = 0.0f;   // -1 = nicht beurteilbar
  const float now_h = (float)local_sec_day / 3600.0f;

  if (in.soc2 >= MAX_SOC) {                    // schon voll: Maximum steht jetzt
    st.exp_max_cache = MAX_SOC; st.exp_max_h_cache = now_h;
  } else if (in.soc2 >= 0 && r.dc_expected > 0 && st.clear_ema > 0.02f) {
    // kWh je Prozent aus dem Messfenster; nur uebernehmen wenn plausibel.
    float k = ETA_K_DEF;
    if (st.soc_n >= ETA_MIN_N) {
      int oldest = (st.soc_i + ETA_WIN - st.soc_n) % ETA_WIN;
      float d_soc_h = (in.soc2 - st.soc_hist[oldest]) * 60.0f / (float)(st.soc_n - 1);
      if (d_soc_h > 0.5f && st.ebox_ema > 200.0f) {
        float k_meas = (st.ebox_ema / 1000.0f) / d_soc_h;
        if (k_meas >= ETA_K_MIN && k_meas <= ETA_K_MAX) k = k_meas;
      }
    }
    float need = (MAX_SOC - in.soc2) * k;                                // kWh
    float pmax = (st.ebox_max_today > 1000.0f) ? st.ebox_max_today
                                               : (float)state_power(7);  // Deckel

    // Restertrag in ETA_STEP_S-Schritten aufintegrieren.
    int    sec  = local_sec_day;
    time_t t    = now_utc;
    float  e    = 0.0f;
    float  last = now_h;                       // letzter Zeitpunkt mit Ertrag
    const int end_sec = ETA_END_H * 3600;
    while (sec < end_sec) {
      float p = (float)dc_now(t, month) * st.clear_ema - ETA_LOAD_W;
      p = fmaxf(0.0f, fminf(pmax, p));
      sec += ETA_STEP_S; t += ETA_STEP_S;
      if (p > 0.0f) {
        e += p * ((float)ETA_STEP_S / 3600.0f) / 1000.0f;
        last = (float)sec / 3600.0f;
      }
      if (e >= need) {                         // 100 % erreicht → Uhrzeit dafuer
        st.exp_max_cache = MAX_SOC; st.exp_max_h_cache = (float)sec / 3600.0f;
        break;
      }
    }
    if (st.exp_max_cache < 0) {                // 100 % heute nicht erreichbar
      st.exp_max_cache   = fminf(MAX_SOC, in.soc2 + e / k);
      st.exp_max_h_cache = last;
    }
  }
  // Ein "nicht beurteilbar" nicht ueber ETA_EVERY Zyklen festhalten: direkt nach
  // Boot/OTA steht in_soc2 noch auf -1, der Cache wuerde die Prognose sonst fuenf
  // Minuten blockieren. Der -1-Pfad bricht vor der Integration ab, kostet also nichts.
  st.eta_tick = (st.exp_max_cache < 0.0f) ? 1 : ETA_EVERY;
  r.exp_max = st.exp_max_cache; r.exp_max_h = st.exp_max_h_cache;
}

// ── Gesamt-Pipeline (entspricht main()) ──────────────────────────────────────
//  now_utc:        unix-UTC der ESPHome-Zeit
//  local_sec_day:  Sekunden seit lokaler Mitternacht (hour*3600+min*60+sec)
//  month, hour, yday: lokale Zeitfelder
inline Result step(const Inputs &in, State &st, time_t now_utc, int local_sec_day,
                   int month, int local_hour, int local_yday, bool ladesperre_enable,
                   float ladesperre_ratio = 0.62f, float bat1_charge_factor = 0.5f,
                   float do4_ebox_full_w = DO4_EBOX_FULL_W) {
  Result r;
  if (local_yday != st.last_yday) {            // Mitternachts-Reset
    st.peak_today = false;
    st.ladesperre_latched = false;
    st.badweather_today = false;
    st.soc1_full_today = false;                // SOC1-Tor faellt taeglich neu zu
    st.last_yday = local_yday;
    st.pcc3_n = 0; st.pcc3_i = 0;              // PCC-Mittel faengt taeglich neu an
    st.ebox_max_today = 0;                     // ETA-Trend faengt taeglich neu an
    st.soc_n = 0; st.soc_i = 0;
    st.ebox_ema = -1.0f; st.clear_ema = -1.0f;
    st.eta_tick = 0; st.exp_max_cache = -1.0f; st.exp_max_h_cache = 0.0f;
  }
  r.dc_expected = dc_now(now_utc, month);
  // Temperatur-Derating nur bei gültiger, plausibler Außentemp; sonst unverändert.
  if (in.aussen_valid && in.aussen_temp > -40.0f && in.aussen_temp < 55.0f && r.dc_expected > 0) {
    double elev, azN; sun_pos(now_utc, elev, azN);
    r.dc_expected *= dc_temp_factor(elev, in.aussen_temp);
  }

  // Peak-Fenster immer berechnen (auch ohne ladesperre_enable) → für JSON-Reporting
  time_t midnight = now_utc - local_sec_day;
  double best_w = 0; int peak_h_loc = -1, win_end_loc = -1;
  for (int h = 5; h <= 20; h++) {
    double w = dc_now(midnight + (time_t)h * 3600, month);
    if (w > best_w) { best_w = w; peak_h_loc = h; }
    if (w > PCC_PEAK_TH) win_end_loc = h;
  }
  r.peak_h    = (best_w > PCC_PEAK_TH) ? peak_h_loc : -1;
  r.win_end_h = win_end_loc;

  // Astronomischer lokaler Mittag → harte Obergrenze für die Ladesperre:
  // Laden startet IMMER spätestens zum Sonnen-Meridiandurchgang.
  time_t noon_utc = solar_noon_utc(now_utc);
  r.noon_h = (float)((double)(noon_utc - midnight) / 3600.0);  // lokale Dezimalstunde

  // LADESPERRE: zeitbasiert bis win_end_h.
  // Freigabe: Schlechtwetter (ratio>ladesperre_ratio) ODER pcc>20kW ODER Peak-Stunde überschritten.
  // peak_today verhindert Oszillation nach Freigabe durch pcc-Abfall beim Laden.
  if (in.pcc > PCC_PEAK_TH) st.peak_today = true;

  // SOC1-Tor: einmal ueber der Schwelle gesehen reicht fuer den ganzen Tag.
  // Ein fehlender MQTT-Wert kommt als 0 herein (num(..., "SOC_Bat1", 0)) und
  // kann das Tor darum nie versehentlich oeffnen — bei stummem Broker bleibt
  // der Deckel liegen, was die sichere Richtung ist.
  if (in.soc1 > SOC1_FULL_TH) st.soc1_full_today = true;
  r.soc1_gate = !st.soc1_full_today;

  // Ist-Wetter-Ratio immer berechnen (für Reporting), -1 wenn DC ungültig.
  // ratio > ladesperre_ratio ⇒ Schlechtwetter. Proxy = pcc_avg5 + ebox + bat1_avg5
  // (beide 5-Min-Mittel aus pivot2db): bringt das Gutwetter-Signal der Akkuladung
  // ohne 0↔2500W-Flattern, und der Akku-voll-Sprung (bat1→pcc) hebt sich auf.
  if (r.dc_expected > 5000)
    r.ratio = (r.dc_expected - (in.pcc_avg5 + in.ebox_w + in.bat1_avg5)) / r.dc_expected;
  // Momentan-Ratio aus den UNGEMITTELTEN Werten. pcc_avg5 verschleift Wolken-
  // luecken zu Sonnenschein; der Rohwert zeigt die Wolke sofort. Nur mit
  // gueltigem PCC — ein null wuerde als volle Verschattung missdeutet.
  if (r.dc_expected > 5000 && in.pcc_valid)
    r.ratio_now = (r.dc_expected - (in.pcc + in.ebox_w + in.bat1)) / r.dc_expected;

  bool ladesperre = false;
  if (ladesperre_enable) {
    bool has_peak = best_w > PCC_PEAK_TH;
    // Zeitfenster: Peak vorhergesagt, vor/in Peak-Stunde, PCC hat 20kW noch nicht erreicht.
    // Harte Obergrenze astronomischer Mittag (now_utc < noon_utc) → Laden startet
    // spätestens zum lokalen Sonnenhöchststand, auch wenn die Peak-Stunde später läge.
    bool in_season = (month >= LADESPERRE_MONTH_FROM && month <= LADESPERRE_MONTH_TO);
    bool in_window = in_season && has_peak && win_end_loc >= 0 && !st.peak_today
                     && (local_hour <= peak_h_loc) && (now_utc < noon_utc);
    // Momentan-Wolkenerkennung: liegt die AKTUELLE Leistung >= 20 % unter dem
    // Klarhimmel-Modell, ist das Gutwetter fuer heute widerlegt -> sofort laden.
    // Tages-Latch wie peak_today, sonst schnappt die Sperre bei der naechsten
    // Wolkenluecke wieder zu und die Ladung flattert im Minutentakt.
    if (in_window && r.ratio_now >= LADESPERRE_NOW_RATIO)
      st.badweather_today = true;
    // LATCH mit Hysterese gegen Flattern an der Ratio-Schwelle:
    //   LOCK   bei belegtem Gutwetter (ratio <= ladesperre_ratio)
    //   RELEASE erst bei klarem Schlechtwetter (ratio >= ladesperre_ratio + LADESPERRE_HYST)
    //   dazwischen / ratio<0 (unbeurteilbar): Zustand halten; außerhalb Fenster: aus.
    if (!in_window) {
      st.ladesperre_latched = false;
    } else if (r.ratio >= 0.0f) {
      if (!st.ladesperre_latched && r.ratio <= ladesperre_ratio)
        st.ladesperre_latched = true;
      else if (st.ladesperre_latched && r.ratio >= ladesperre_ratio + LADESPERRE_HYST)
        st.ladesperre_latched = false;
    }
    ladesperre = in_window && st.ladesperre_latched && !st.badweather_today;
  }
  r.ladesperre = ladesperre;

  char trace[160]; float excess;
  // Ringpuffer nur mit GUELTIGEN PCC-Werten fuellen — ein null darf den
  // Ersatzwert nicht mit einer 0 verwaessern.
  float pcc_use = in.pcc;
  if (in.pcc_valid) {
    st.pcc3_buf[st.pcc3_i] = in.pcc;
    st.pcc3_i = (st.pcc3_i + 1) % PCC_AVG_N;
    if (st.pcc3_n < PCC_AVG_N) st.pcc3_n++;
  } else if (st.pcc3_n > 0) {                  // null → Mittel der letzten gueltigen
    float sum = 0.0f;
    for (int i = 0; i < st.pcc3_n; i++) sum += st.pcc3_buf[i];
    pcc_use = sum / (float)st.pcc3_n;
  }

  int best = decide(in, st.relay_st, st.prot, bat1_charge_factor, pcc_use, trace, &excess);
  if (!in.pcc_valid) {                         // im Trace sichtbar machen
    char t[40]; snprintf(t, sizeof(t), " | PCC_NULL_AVG3 (%.0fW)", pcc_use);
    tcat(trace, t);
  }
  bool guard_fired = false;
  best = apply_guards(best, in.soc2, in.pcc, ladesperre, r.soc1_gate, trace, &guard_fired);

  float drop_rate = (st.last_excess > 0) ? (excess - st.last_excess) / 30.0f : 0.0f;
  st.last_excess = excess;

  int final_state; bool changed;
  if (guard_fired) { final_state = best; changed = (best != st.relay_st); }   // Schutz unbypassbar
  else final_state = apply_blocking(best, st.relay_st, in.pcc, in.bat1, st.stable,
                                    drop_rate, trace, &changed);

  st.stable = changed ? 0 : st.stable + 1;

  if (in.soc2 >= 0) {                          // Deep-Discharge-Hysterese
    if (in.soc2 < DD_LOWER) st.prot = true;
    else if (in.soc2 >= DD_UPPER) st.prot = false;
  }

  // DO4 haengt an der GEMESSENEN Aufnahme, nicht mehr am Trace-Text und nicht an
  // der 22-kW-Schwelle: abgeregelt wird nur, wenn die EBox nachweislich nichts
  // mehr aufnehmen kann. Solange die Laderampe die volle Last noch nicht erreicht
  // hat, waere der Puls verfrueht — die Aufnahme ist im Anmarsch, und WR2
  // abzuwerfen kostet echten Ertrag.
  //   ebox_w >= do4_ebox_full_w -> Stufe 7 steht wirklich an, mehr geht nicht
  //   soc2 >= MAX_SOC           -> Speicher voll, die EBox kann grundsaetzlich nicht
  // Alles dazwischen (Rampe laeuft, Relais gerade zu, Messung noch niedrig) haelt
  // DO4 zurueck.
  bool ebox_at_max = (in.ebox_w >= do4_ebox_full_w);
  bool ebox_dead   = (in.soc2 >= MAX_SOC);
  bool need_down   = (in.pcc > PCC_PEAK_TH) && (ebox_at_max || ebox_dead);
  r.do4_pulse = need_down;

  st.relay_st = final_state;
  r.final_state = final_state; r.changed = changed; r.excess = excess;
  r.dc_delta = r.dc_expected - (in.pcc + in.ebox_w + in.bat1);
  strncpy(r.trace, trace, 159); r.trace[159] = 0;
  predict_max_soc(st, in, r, now_utc, local_sec_day, month);
  return r;
}

// Globaler Zustand (eine Instanz, von den ESPHome-Lambdas referenziert)
inline State g_state;

}  // namespace fox
