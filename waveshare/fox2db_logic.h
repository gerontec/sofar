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
constexpr float EMERGENCY_MARGIN = 120.0f;                          // harter Abwurf erst bei G + Margin
constexpr float EMERGENCY_IMPORT = MAX_GRID_DRAW + EMERGENCY_MARGIN; // = 1020 W (an G gekoppelt)
constexpr float BAT_DISCHARGE_TH = -110.0f;
constexpr float SWEET_SPOT_PCC   = 160.0f;
constexpr float MAX_DROP_RATE    = -20.0f;
constexpr float LADESPERRE_HYST  = 0.25f;   // Hysterese-Band für Ladesperre-Ratio (Latch gegen Flattern); breit genug gegen rohes pcc-Rauschen (Release 0.62+0.25=0.87)
constexpr int   DD_LOWER         = 6;
constexpr int   DD_UPPER         = 8;
constexpr int   DD_CHARGE_TARGET = 7;
constexpr float PCC_PEAK_TH      = 20000.0f;
constexpr float PCC_HARD_TH      = 22000.0f;  // bedingungsloser DO4-Trigger
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

// ── DC-Klarhimmel-Forecast (Meinel-Modell wie _DcForecast) ───────────────────
constexpr double LAT = 47.6811, LON = 11.5732;
struct Arr { double tilt, azS, power; };   // tilt, Azimut-Süd, Nennleistung
static const Arr ARRAYS[2]      = {{25, 80, 27854}, {60, -5, 11138}};
static const Arr ARRAYS_EAST[3] = {{41, -74, 19852}, {60, 90, 2078}, {32, 94, 2378}};
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
  double sum = 0;
  for (int i = 0; i < n; i++)
    sum += a[i].power * T * kt * fmax(0.0, cos_aoi(elev, azN, a[i].tilt, a[i].azS));
  return sum;
}
inline double dc_now(time_t t, int month) {
  return calc_arrays(ARRAYS, 2, t, month) + calc_arrays(ARRAYS_EAST, 3, t, month);
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

// ── Zustand (im RAM, über Cron-Zyklen hinweg) ────────────────────────────────
struct State {
  int   relay_st = 0;
  int   stable = 0;
  float last_excess = 0;
  bool  prot = false;               // Tiefentladeschutz (Hysterese)
  bool  peak_today = false;         // pcc hat heute PCC_PEAK_TH überschritten → Laden vor DO4
  bool  ladesperre_latched = false; // Ladesperre-Latch (Hysterese gegen Flattern)
  int   last_yday = -1;
};

// pcc_avg5 + bat1_avg5: 5-Min-Mittel aus pivot2db/MariaDB, nur für die Wetter-Ratio.
// Beide über dasselbe Fenster gemittelt → Akku-voll-Sprung (bat1→pcc) ist neutral.
struct Inputs { float pcc, bat1, soc1, soc2, ebox_w, bat1_avg5, pcc_avg5;
                float aussen_temp = 0; bool aussen_valid = false; };

struct Result {
  int   final_state = 0;
  bool  changed = false;
  bool  do4_pulse = false;
  bool  ladesperre = false;
  float excess = 0, dc_expected = 0, dc_delta = 0;
  float ratio = -1.0f;  // Ist-Wetter-Ratio (Anteil fehlender Klarhimmel-Leistung; -1 = nicht berechenbar)
  int   peak_h = -1;    // lokale Stunde des DC-Peaks (-1 = kein Peak heute)
  int   win_end_h = -1; // letzte lokale Stunde mit dc > PCC_PEAK_TH
  float noon_h = -1.0f; // astronomischer lokaler Mittag (Dezimalstunde, Reporting)
  char  trace[160] = "";
};

inline void tcat(char *t, const char *s) { strncat(t, s, 159 - strlen(t)); }

// ── decide() ─────────────────────────────────────────────────────────────────
inline int decide(const Inputs &in, int relay_st, bool prot, float bat1_factor, char *trace, float *excess_out) {
  float ebox_eff = (relay_st > 0) ? fmaxf(in.ebox_w, (float)state_power(relay_st)) : 0.0f;
  // Sofar-Entladung (bat1<0) zählt voll; von der Sofar-Ladung (bat1>0) nur der Anteil bat1_factor.
  float bat1_eff = (in.bat1 < 0.0f) ? in.bat1 : in.bat1 * bat1_factor;
  float excess = in.pcc + ebox_eff + bat1_eff;
  if (in.soc2 < 0) {
    float ea = (relay_st > 0) ? in.ebox_w : 0.0f;
    *excess_out = in.pcc + ea + bat1_eff;
    strcpy(trace, "EBOX_SOC_UNKNOWN_HOLD");
    return relay_st;
  }
  if (in.pcc > PCC_PEAK_TH && in.soc2 < MAX_SOC) {
    int next_st = relay_st + 1; if (next_st == 2) next_st = 3; if (next_st > 7) next_st = 7;
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
  if (best > relay_st) {                       // Ramp-Limiting (State-Nr.-Vergleich, wie Python)
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
inline int apply_guards(int best, float soc2, bool ladesperre, char *trace, bool *guard_fired) {
  if (ladesperre) { if (best != 0) tcat(trace, " | GUARD:CHARGE_BLOCK_UNTIL_PCC_20KW"); *guard_fired = true; return 0; }
  if (soc2 >= MAX_SOC) { if (best != 0) tcat(trace, " | GUARD:BATTERY_FULL_STOP"); *guard_fired = true; return 0; }
  if (soc2 >= 0 && soc2 < DD_LOWER) { if (best != 1) tcat(trace, " | GUARD:CRITICAL_SOC_PROTECTION_ACTIVATE"); *guard_fired = true; return 1; }
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
  if (stable < STABILIZATION)                                  { tcat(trace, " | STABILIZING");     *changed = false; return relay_st; }
  if (!up && pwr_diff < HYSTERESIS)                            { tcat(trace, " | HYSTERESIS");      *changed = false; return relay_st; }
  *changed = true; return best;
}

// ── Gesamt-Pipeline (entspricht main()) ──────────────────────────────────────
//  now_utc:        unix-UTC der ESPHome-Zeit
//  local_sec_day:  Sekunden seit lokaler Mitternacht (hour*3600+min*60+sec)
//  month, hour, yday: lokale Zeitfelder
inline Result step(const Inputs &in, State &st, time_t now_utc, int local_sec_day,
                   int month, int local_hour, int local_yday, bool ladesperre_enable,
                   float ladesperre_ratio = 0.62f, float bat1_charge_factor = 0.5f) {
  Result r;
  if (local_yday != st.last_yday) {            // Mitternachts-Reset
    st.peak_today = false;
    st.ladesperre_latched = false;
    st.last_yday = local_yday;
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

  // Ist-Wetter-Ratio immer berechnen (für Reporting), -1 wenn DC ungültig.
  // ratio > ladesperre_ratio ⇒ Schlechtwetter. Proxy = pcc_avg5 + ebox + bat1_avg5
  // (beide 5-Min-Mittel aus pivot2db): bringt das Gutwetter-Signal der Akkuladung
  // ohne 0↔2500W-Flattern, und der Akku-voll-Sprung (bat1→pcc) hebt sich auf.
  if (r.dc_expected > 5000)
    r.ratio = (r.dc_expected - (in.pcc_avg5 + in.ebox_w + in.bat1_avg5)) / r.dc_expected;

  bool ladesperre = false;
  if (ladesperre_enable) {
    bool has_peak = best_w > PCC_PEAK_TH;
    // Zeitfenster: Peak vorhergesagt, vor/in Peak-Stunde, PCC hat 20kW noch nicht erreicht.
    // Harte Obergrenze astronomischer Mittag (now_utc < noon_utc) → Laden startet
    // spätestens zum lokalen Sonnenhöchststand, auch wenn die Peak-Stunde später läge.
    bool in_window = has_peak && win_end_loc >= 0 && !st.peak_today
                     && (local_hour <= peak_h_loc) && (now_utc < noon_utc);
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
    ladesperre = in_window && st.ladesperre_latched;
  }
  r.ladesperre = ladesperre;

  char trace[160]; float excess;
  int best = decide(in, st.relay_st, st.prot, bat1_charge_factor, trace, &excess);
  bool guard_fired = false;
  best = apply_guards(best, in.soc2, ladesperre, trace, &guard_fired);

  float drop_rate = (st.last_excess > 0) ? (excess - st.last_excess) / 30.0f : 0.0f;
  st.last_excess = excess;

  int final_state; bool changed;
  if (guard_fired) { final_state = best; changed = (best != st.relay_st); }   // Schutz unbypassbar
  else final_state = apply_blocking(best, st.relay_st, in.pcc, in.bat1, st.stable, drop_rate, trace, &changed);

  st.stable = changed ? 0 : st.stable + 1;

  if (in.soc2 >= 0) {                          // Deep-Discharge-Hysterese
    if (in.soc2 < DD_LOWER) st.prot = true;
    else if (in.soc2 >= DD_UPPER) st.prot = false;
  }

  bool need_down = (in.pcc > PCC_HARD_TH) ||   // >22kW: bedingungslos
                   ((in.pcc > PCC_PEAK_TH) && (strncmp(trace, "PCC_OVER_20KW", 13) != 0 || ladesperre));
  r.do4_pulse = need_down;

  st.relay_st = final_state;
  r.final_state = final_state; r.changed = changed; r.excess = excess;
  r.dc_delta = r.dc_expected - (in.pcc + in.ebox_w + in.bat1);
  strncpy(r.trace, trace, 159); r.trace[159] = 0;
  return r;
}

// Globaler Zustand (eine Instanz, von den ESPHome-Lambdas referenziert)
inline State g_state;

}  // namespace fox
