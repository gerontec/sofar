// bm_esp.h: batmonitor on the pv_relay board (ESP32-S3-Relay-6CH). Runs the C logic of the victron repo unchanged
// (bm_logic.c: bm_step + bm_full_forecast, bm_parse.c: payload checks) on inputs it reads itself from the house broker
// .218, with the parameters of /data/batmonitor/env on Venus. This file is the shell batmonitor.c is on Venus, minus
// D-Bus:
//   inverter/power_grid_exchange/json, r290/heatpump/all, em0/power, aussen/temp, batmonitor/season,
//   batmonitor/forecast   as batmonitor.c (same parsing, same plausible ranges)
//   ebox/pwr              Stack2 (Pytes) as dbus-ebox-battery 1.4 puts it on the D-Bus: SoC, power, voltage, lowest
//                         cell, CCL/DCL incl. its "High" block and low-SoC latch; gone after 120 s like that service
//   not on .218           Stack1 (MUST, can0 on Venus) and vebus (AC-in/out, grid): bms_ok[0] = 0 -> Stack1 phase L1
//                         BMS_MISSING; ac_ok = 0 -> the measured own charge/discharge counts 0 (the surplus is then
//                         the one of idle Multis); grid_ok = 0 and no AC-out -> the AC-out DO4 rule never fires
// Setpoints are computed and published only: a second controller on the Multis would fight batmonitor.
// DO4 (D5): the ESP pulses its own relay when bm_step says so AND batmonitor is silent (no fresh
// batmonitor/peak_model) - fallback for Venus / Pi .119 down. A pulse from batmonitor (pv_relay/DO4 "1") sets the
// local lockout, so both never pulse within DO4_LOCKOUT.
#pragma once
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>
#include <strings.h>
#include <cJSON.h>
#include "esp_timer.h"
#include "esp_heap_caps.h"
#include "bm_logic.h"
extern "C" {                                 // bm_parse.h has no extern "C" of its own
#include "bm_parse.h"
}

namespace bmesp {

static const char *BM_VERSION = "0.53-c";   // batmonitor.c VERSION whose bm_logic.c / bm_parse.c this build carries
static const char *ESP_VERSION = "1.0";
static const int PYTES = 1;                  // BM_BANKS[1] = STACK2_PYTES
static const double EBOX_STALE_S = 120.0;    // dbus-ebox-battery STALE_SECONDS
static const double BM_ALIVE_S = 180.0;      // batmonitor/peak_model comes every 60 s
static const double ETA_WINDOW = 300.0, ETA_MIN_W = 50.0;
static const int ETA_MAX_N = 128;
static const double PUB_SECONDS = 60.0;

// dbus-ebox-battery 1.4 constants (CCL / DCL of the Pytes stack)
static const double EB_MAX_CHARGE_CURRENT = 586.0, EB_MAX_DISCHARGE_CURRENT = 586.0, EB_TAPER_CURRENT = 10.0;
static const int EB_CELL_TAPER_MV = 3600, EB_CELL_STOP_MV = 3650, EB_HIGH_RELEASE_MV = 3450;
static const double EB_FORCE_CHARGE_SOC = 5.0, EB_FORCE_CHARGE_RELEASE_SOC = 8.0;

static struct bm_cfg cfg;
static struct bm_state st;
static struct bm_out o_;
static struct bm_in in_last;
static struct bm_fc_out fc_out;
static bool ready = false;
static int cfg_set = 0, cfg_bad = 0;

static struct {
	int have_pcc, have_pv;
	double pcc, pv, bat1, pcc_avg5, bat1_avg5, inv_time;
	int have_soc_bat1;
	double soc_bat1;
	int r290_hz;
	double r290_time;
	double aussen, aussen_time;
	double wp, wp_time;
	int season;
	double season_ts;
	double fc_target, fc_ts, fc_corr;
	int fc_n;
	time_t fc_slot_t[BM_FC_SLOTS];
	double fc_slot_kt[BM_FC_SLOTS];
	double ebox_time;                // monotonic s of the last valid ebox/pwr
	double eb_soc, eb_power, eb_volt;
	int eb_volt_ok, eb_vlow_ok;
	double eb_vlow;                  // V
	double eb_ccl, eb_dcl;
	bool eb_high_block, eb_low_soc;
	double bm_peak_ts;               // unix ts in batmonitor/peak_model, 0 = never
} mq;

static struct { double t, p; } hist[NBANK][ETA_MAX_N];
static int hist_n[NBANK];
static time_t full_reached[NBANK];
static uint32_t cycles = 0, local_pulses = 0, remote_pulses = 0, inv_rejected = 0;
static uint32_t step_us = 0, fc_us = 0;

static double mono() { return esp_timer_get_time() / 1e6; }

static double jnum(const cJSON *o, const char *k, double dflt)
{
	const cJSON *x = cJSON_GetObjectItemCaseSensitive(o, k);
	return cJSON_IsNumber(x) && std::isfinite(x->valuedouble) ? x->valuedouble : dflt;
}

static void ensure_init()
{
	if (ready)
		return;
	bm_cfg_default(&cfg);
	bm_init(&st);
	memset(&mq, 0, sizeof(mq));
	mq.fc_target = -1;
	ready = true;
}

// parameters as /data/batmonitor/env: "NAME=value NAME=value ..." (names of BM_PARAMS without BATMONITOR_, plus
// LADESPERRE / FORECAST / PI = 0|1). Out of range or unknown: ignored and counted in cfg_bad
inline void set_params(const std::string &s)
{
	ensure_init();
	char buf[512];
	snprintf(buf, sizeof(buf), "%s", s.c_str());
	for (char *tok = strtok(buf, " ,;\n\t"); tok; tok = strtok(nullptr, " ,;\n\t")) {
		char *eq = strchr(tok, '=');
		if (!eq) {
			cfg_bad++;
			continue;
		}
		*eq = 0;
		char *end;
		double v = strtod(eq + 1, &end);
		if (end == eq + 1 || *end || !std::isfinite(v)) {
			cfg_bad++;
			continue;
		}
		if (!strcmp(tok, "LADESPERRE")) { cfg.ladesperre = v != 0; cfg_set++; continue; }
		if (!strcmp(tok, "FORECAST")) { cfg.forecast = v != 0; cfg_set++; continue; }
		if (!strcmp(tok, "PI")) { cfg.pi_armed = v != 0; cfg_set++; continue; }
		int i;
		for (i = 0; i < BM_NPARAMS; i++)
			if (!strcmp(tok, BM_PARAMS[i].name))
				break;
		if (i < BM_NPARAMS && bm_param_set(&cfg, i, v) == 0)
			cfg_set++;
		else
			cfg_bad++;
	}
}

// ---- inputs (batmonitor.c on_message) ----------------------------------------------------------------

inline void on_wp(const std::string &x)
{
	ensure_init();
	double v;
	if (bm_parse_number(x.c_str(), -1000.0, 30000.0, &v)) {
		mq.wp = v;
		mq.wp_time = mono();
	}
}

inline void on_aussen(const std::string &x)
{
	ensure_init();
	double v;
	if (bm_parse_number(x.c_str(), -40.0, 55.0, &v)) {
		mq.aussen = v;
		mq.aussen_time = mono();
	}
}

inline void on_inverter(const std::string &x)
{
	ensure_init();
	struct bm_inv_sample s;
	if (!bm_parse_inverter(x.c_str(), &s)) {
		inv_rejected++;
		return;
	}
	mq.pcc = s.pcc;
	mq.bat1 = s.bat1;
	mq.pcc_avg5 = s.pcc_avg5;
	mq.bat1_avg5 = s.bat1_avg5;
	mq.pv = s.pv;
	mq.have_pv = s.have_pv;
	mq.have_soc_bat1 = s.have_soc_bat1;
	mq.soc_bat1 = s.soc_bat1;
	mq.have_pcc = 1;
	mq.inv_time = mono();
}

inline void on_r290(const std::string &x)
{
	ensure_init();
	int hz;
	if (bm_parse_r290(x.c_str(), &hz)) {
		mq.r290_hz = hz;
		mq.r290_time = mono();
	}
}

inline void on_forecast(const std::string &x)
{
	ensure_init();
	cJSON *d = cJSON_Parse(x.c_str());
	if (cJSON_IsObject(d)) {
		mq.fc_target = jnum(d, "target_soc", -1);
		mq.fc_ts = jnum(d, "ts", 0);
		mq.fc_corr = jnum(d, "correction", 0);
		mq.fc_n = 0;
		const cJSON *sl, *slots = cJSON_GetObjectItemCaseSensitive(d, "kt_slots");
		cJSON_ArrayForEach(sl, slots) {
			const cJSON *st0 = cJSON_GetArrayItem(sl, 0), *kt = cJSON_GetArrayItem(sl, 1);
			if (mq.fc_n < BM_FC_SLOTS && cJSON_IsNumber(st0) && cJSON_IsNumber(kt)) {
				mq.fc_slot_t[mq.fc_n] = (time_t)st0->valuedouble;
				mq.fc_slot_kt[mq.fc_n++] = kt->valuedouble;
			}
		}
	}
	cJSON_Delete(d);
}

inline void on_season(const std::string &x)
{
	ensure_init();
	cJSON *d = cJSON_Parse(x.c_str());
	if (cJSON_IsObject(d)) {
		const cJSON *mo = cJSON_GetObjectItemCaseSensitive(d, "mode");
		mq.season = cJSON_IsString(mo) ? (!strcmp(mo->valuestring, "summer") ? BM_SUMMER
										  : !strcmp(mo->valuestring, "winter") ? BM_WINTER
										  : !strcmp(mo->valuestring, "transition") ? BM_TRANSITION : 0) : 0;
		mq.season_ts = jnum(d, "ts", 0);
	}
	cJSON_Delete(d);
}

// ebox/pwr (ebox_mqtt.py, every 30 s): what dbus-ebox-battery 1.4 update() makes of it
inline void on_ebox(const std::string &x)
{
	ensure_init();
	cJSON *d = cJSON_Parse(x.c_str());
	const cJSON *soc = cJSON_IsObject(d) ? cJSON_GetObjectItemCaseSensitive(d, "soc") : nullptr;
	const cJSON *cur = cJSON_IsObject(d) ? cJSON_GetObjectItemCaseSensitive(d, "current_a") : nullptr;
	const cJSON *pw = cJSON_IsObject(d) ? cJSON_GetObjectItemCaseSensitive(d, "power_w") : nullptr;
	if (cJSON_IsNumber(soc) && cJSON_IsNumber(cur) && cJSON_IsNumber(pw)) {   // the service raises on these
		double current = cur->valuedouble, power = pw->valuedouble;
		double volt = jnum(d, "voltage_v", NAN);
		if (std::isnan(volt) && current != 0)
			volt = power / current;
		double vhigh = jnum(d, "vhigh_mv", NAN), vlow = jnum(d, "vlow_mv", NAN);
		const cJSON *vs = cJSON_GetObjectItemCaseSensitive(d, "volt_st");
		const char *volt_st = cJSON_IsString(vs) && vs->valuestring[0] ? vs->valuestring : "Normal";
		if (!strncasecmp(volt_st, "high", 4))
			mq.eb_high_block = true;
		else if (mq.eb_high_block && !std::isnan(vhigh) && vhigh < EB_HIGH_RELEASE_MV)
			mq.eb_high_block = false;
		mq.eb_soc = round(soc->valuedouble * 10) / 10;
		if (mq.eb_soc < EB_FORCE_CHARGE_SOC)
			mq.eb_low_soc = true;
		else if (mq.eb_soc >= EB_FORCE_CHARGE_RELEASE_SOC)
			mq.eb_low_soc = false;
		double ccl = std::isnan(vhigh) ? EB_MAX_CHARGE_CURRENT : vhigh >= EB_CELL_STOP_MV ? 0.0
				   : vhigh >= EB_CELL_TAPER_MV ? EB_TAPER_CURRENT : EB_MAX_CHARGE_CURRENT;
		mq.eb_ccl = mq.eb_high_block ? 0.0 : ccl;
		mq.eb_dcl = mq.eb_low_soc ? 0.0 : EB_MAX_DISCHARGE_CURRENT;
		mq.eb_power = round(power);
		mq.eb_volt_ok = std::isfinite(volt) && volt != 0;
		mq.eb_volt = mq.eb_volt_ok ? round(volt * 100) / 100 : 0;
		mq.eb_vlow_ok = !std::isnan(vlow) && vlow != 0;
		mq.eb_vlow = mq.eb_vlow_ok ? vlow / 1000.0 : 0;
		mq.ebox_time = mono();
	}
	cJSON_Delete(d);
}

// batmonitor/peak_model (retained, every 60 s while batmonitor runs): its ts tells whether batmonitor is alive
inline void on_bm_peak(const std::string &x)
{
	ensure_init();
	cJSON *d = cJSON_Parse(x.c_str());
	if (cJSON_IsObject(d))
		mq.bm_peak_ts = jnum(d, "ts", 0);
	cJSON_Delete(d);
}

// pv_relay/DO4 "1" from batmonitor: one pulse, the local lockout starts with it
inline void on_do4_cmd(const std::string &x)
{
	ensure_init();
	if (x == "1") {
		st.do4_last = mono();
		remote_pulses++;
	}
}

inline bool batmonitor_alive(time_t t) { return mq.bm_peak_ts > 0 && t - (time_t)mq.bm_peak_ts < BM_ALIVE_S; }

// ---- cycle (batmonitor.c calc + sample_power + run_full_forecast) --------------------------------------

static bool ebox_fresh(double now) { return mq.ebox_time > 0 && now - mq.ebox_time < EBOX_STALE_S; }

static void sample_power(double now)
{
	for (int b = 0; b < NBANK; b++) {
		int k = 0;
		if (b == PYTES && ebox_fresh(now)) {
			if (hist_n[b] == ETA_MAX_N) {
				memmove(hist[b], hist[b] + 1, (ETA_MAX_N - 1) * sizeof(hist[b][0]));
				hist_n[b]--;
			}
			hist[b][hist_n[b]].t = now;
			hist[b][hist_n[b]].p = mq.eb_power;
			hist_n[b]++;
		}
		while (k < hist_n[b] && now - hist[b][k].t > ETA_WINDOW)
			k++;
		if (k) {
			memmove(hist[b], hist[b] + k, (hist_n[b] - k) * sizeof(hist[b][0]));
			hist_n[b] -= k;
		}
	}
}

static double hist_avg(int b, int *has)
{
	double s = 0;
	*has = hist_n[b] > 0;
	for (int i = 0; i < hist_n[b]; i++)
		s += hist[b][i].p;
	return *has ? s / hist_n[b] : 0;
}

static void run_full_forecast(const struct bm_in *in)
{
	struct bm_fc_in fi;
	memset(&fi, 0, sizeof(fi));
	fi.t = in->t;
	fi.have_pv = in->have_pv && in->now - in->inv_time < 180.0;
	fi.pv_sofar = in->pv;
	fi.lead = st.lead;
	for (int b = 0; b < NBANK; b++) {
		fi.volt_ok[b] = in->volt_ok[b];
		fi.volt[b] = in->volt[b];
		fi.soc_ok[b] = st.soc_ok[b];
		fi.soc[b] = st.soc[b];
		fi.avg[b] = hist_avg(b, &fi.has_avg[b]);
	}
	if (mq.fc_ts > 0 && in->t - (time_t)mq.fc_ts < 3 * 3600) {
		fi.corr = mq.fc_corr;
		fi.n_slots = mq.fc_n;
		memcpy(fi.slot_t, mq.fc_slot_t, sizeof(fi.slot_t));
		memcpy(fi.slot_kt, mq.fc_slot_kt, sizeof(fi.slot_kt));
	}
	bm_full_forecast(&cfg, &fi, &fc_out);
	for (int b = 0; b < NBANK; b++)
		if (!st.soc_ok[b])
			continue;
		else if (st.soc[b] < 100)
			full_reached[b] = 0;
		else if (!full_reached[b])
			full_reached[b] = in->t;
}

// batmonitor.c full_forecast: weather forecast by day, linear ETA at night, 0 = none
static long full_at(int b)
{
	int has;
	double avg = hist_avg(b, &has);
	if (full_reached[b])
		return (long)full_reached[b];
	if (fc_out.soc_sunset[b] >= 0)
		return (long)fc_out.full_at[b];
	if (has && st.soc_ok[b] && avg >= ETA_MIN_W && st.soc[b] < 100)
		return (long)(in_last.t + (100 - st.soc[b]) / 100 * BM_BANKS[b].capacity_wh / avg * 3600);
	return 0;
}

// one cycle every CYCLE_SECONDS (needs the wall clock: t = unix time after SNTP); returns 1 = pulse DO4 locally now
inline int cycle(time_t t)
{
	ensure_init();
	struct bm_in in;
	memset(&in, 0, sizeof(in));
	in.now = mono();
	in.t = t;
	sample_power(in.now);
	in.have_pcc = mq.have_pcc;
	in.have_pv = mq.have_pv;
	in.pcc = mq.pcc;
	in.pv = mq.pv;
	in.bat1 = mq.bat1;
	in.have_soc_bat1 = mq.have_soc_bat1;
	in.soc_bat1 = mq.soc_bat1;
	in.pcc_avg5 = mq.pcc_avg5;
	in.bat1_avg5 = mq.bat1_avg5;
	in.inv_time = mq.inv_time;
	in.r290_hz = mq.r290_hz;
	in.r290_time = mq.r290_time;
	in.aussen = mq.aussen;
	in.aussen_time = mq.aussen_time;
	in.wp = mq.wp;
	in.wp_time = mq.wp_time;
	in.season = mq.season;
	in.season_ts = (time_t)mq.season_ts;
	in.fc_target = mq.fc_ts > 0 ? mq.fc_target : -1;
	in.fc_ts = (time_t)mq.fc_ts;
	if (mq.fc_ts > 0 && in.t - (time_t)mq.fc_ts < 3 * 3600) {
		in.fc_corr = mq.fc_corr;
		in.fc_n = mq.fc_n;
		memcpy(in.fc_slot_t, mq.fc_slot_t, sizeof(in.fc_slot_t));
		memcpy(in.fc_slot_kt, mq.fc_slot_kt, sizeof(in.fc_slot_kt));
	}
	// Stack1 (MUST) and vebus: not on .218, stay 0 (see the header)
	if (ebox_fresh(in.now)) {
		in.bms_ok[PYTES] = 1;
		in.soc[PYTES] = mq.eb_soc;
		in.power_ok[PYTES] = 1;
		in.power[PYTES] = mq.eb_power;
		in.volt_ok[PYTES] = mq.eb_volt_ok;
		in.volt[PYTES] = mq.eb_volt;
		in.lim_ok[PYTES] = 1;
		in.ccl[PYTES] = mq.eb_ccl;
		in.dcl[PYTES] = mq.eb_dcl;
		in.cell_ok[PYTES] = mq.eb_vlow_ok && mq.eb_vlow > 1.5 && mq.eb_vlow < 4.5;
		in.cell_min[PYTES] = mq.eb_vlow;
	}
	int64_t t0 = esp_timer_get_time();
	bm_step(&cfg, &in, &st, &o_);
	int64_t t1 = esp_timer_get_time();
	run_full_forecast(&in);
	step_us = (uint32_t)(t1 - t0);
	fc_us = (uint32_t)(esp_timer_get_time() - t1);
	in_last = in;
	cycles++;
	if (o_.do4_pulse && !batmonitor_alive(t)) {
		local_pulses++;
		return 1;
	}
	return 0;
}

// ---- outputs ------------------------------------------------------------------------------------------

static void json_esc(char *dst, size_t n, const char *s)
{
	size_t o = 0;
	for (; *s && o + 2 < n; s++) {
		if (*s == '"' || *s == '\\')
			dst[o++] = '\\';
		dst[o++] = *s;
	}
	dst[o] = 0;
}

// batmonitor_esp/state: publish when a rule text changed or every PUB_SECONDS; "" = nothing to publish
inline std::string state_json(bool force = false)
{
	static char last_rules[NPH * WHY_LEN];
	static double last_pub;
	if (!cycles)
		return "";
	char rules[NPH * WHY_LEN] = "";
	for (int p = 0; p < NPH; p++) {
		strncat(rules, o_.why[p], sizeof(rules) - strlen(rules) - 2);
		strcat(rules, ";");
	}
	double now = mono();
	if (!force && !strcmp(rules, last_rules) && now - last_pub < PUB_SECONDS)
		return "";
	strcpy(last_rules, rules);
	last_pub = now;
	char why[NPH][2 * WHY_LEN], buf[1700];
	for (int p = 0; p < NPH; p++)
		json_esc(why[p], sizeof(why[p]), o_.why[p]);
	const struct bm_in &in = in_last;
	int has0, has1;
	double avg0 = hist_avg(0, &has0), avg1 = hist_avg(1, &has1);
	snprintf(buf, sizeof(buf),
			 "{\"ts\":%ld,\"version\":\"%s\",\"esp\":\"%s\",\"sp\":[%d,%d,%d],\"why\":[\"%s\",\"%s\",\"%s\"],"
			 "\"surplus\":%.0f,\"wp_eff\":%.0f,\"wp_why\":\"%s\",\"src_why\":\"%s\",\"season\":%d,\"season_measured\":%d,"
			 "\"fc_active\":%d,\"fc_bad\":%d,\"bat1_first\":%d,\"lead\":%d,"
			 "\"ls\":{\"active\":%d,\"cap\":%d,\"peak_h\":%d,\"win_end_h\":%d,\"ratio\":%.3f,\"ratio_now\":%.3f},"
			 "\"pcc\":%.0f,\"pv\":%.0f,\"bat1\":%.0f,\"soc_bat1\":%.1f,\"wp\":%.0f,\"r290_hz\":%d,"
			 "\"age\":{\"inv\":%.0f,\"wp\":%.0f,\"r290\":%.0f,\"ebox\":%.0f},"
			 "\"bms_ok\":[%d,%d],\"soc\":[%.1f,%.1f],\"power\":[%.0f,%.0f],\"ccl\":[%.0f,%.0f],\"dcl\":[%.0f,%.0f],"
			 "\"low\":[%d,%d],\"force\":[%d,%d],\"prot\":[%d,%d],\"avg5\":[%.0f,%.0f],\"full_at\":[%ld,%ld],"
			 "\"soc_sunset\":[%.1f,%.1f],\"kt_now\":%.3f,\"weather\":%d,\"pi_u\":%.0f,"
			 "\"do4\":{\"bm_alive\":%d,\"local\":%u,\"remote\":%u,\"last_ago\":%.0f},"
			 "\"cycles\":%u,\"step_us\":%u,\"fc_us\":%u,\"inv_rejected\":%u,\"cfg_set\":%d,\"cfg_bad\":%d,\"heap\":%u}",
			 (long)in.t, BM_VERSION, ESP_VERSION, o_.sp[0], o_.sp[1], o_.sp[2], why[0], why[1], why[2], o_.surplus,
			 o_.wp_eff, o_.wp_why, o_.src_why, o_.season, o_.season_measured, o_.fc_active, o_.fc_bad, o_.bat1_first,
			 st.lead, o_.ls.active, o_.ls.cap, o_.ls.peak_h, o_.ls.win_end_h, o_.ls.ratio, o_.ls.ratio_now,
			 in.pcc, in.pv, in.bat1, in.soc_bat1, in.wp, in.r290_hz,
			 in.inv_time > 0 ? in.now - in.inv_time : -1, in.wp_time > 0 ? in.now - in.wp_time : -1,
			 in.r290_time > 0 ? in.now - in.r290_time : -1, mq.ebox_time > 0 ? in.now - mq.ebox_time : -1,
			 in.bms_ok[0], in.bms_ok[1], in.soc[0], in.soc[1], in.power[0], in.power[1], in.ccl[0], in.ccl[1],
			 in.dcl[0], in.dcl[1], st.low[0], st.low[1], st.force[0], st.force[1], st.prot[0], st.prot[1],
			 has0 ? avg0 : 0, has1 ? avg1 : 0, full_at(0), full_at(1), fc_out.soc_sunset[0], fc_out.soc_sunset[1],
			 fc_out.kt_now, fc_out.weather, o_.pi.u, batmonitor_alive(in.t) ? 1 : 0, (unsigned)local_pulses,
			 (unsigned)remote_pulses, st.do4_last > 0 ? in.now - st.do4_last : -1, (unsigned)cycles,
			 (unsigned)step_us, (unsigned)fc_us, (unsigned)inv_rejected, cfg_set, cfg_bad,
			 (unsigned)heap_caps_get_free_size(MALLOC_CAP_DEFAULT));
	return std::string(buf);
}

// batmonitor_esp/peak_model (retained, every 60 s): the same fields as batmonitor/peak_model
inline std::string peak_model_json()
{
	if (!cycles)
		return "";
	char buf[512];
	snprintf(buf, sizeof(buf),
			 "{\"ts\":%ld,\"peak_h\":%d,\"win_end_h\":%d,\"ratio\":%.3f,\"ratio_now\":%.3f,\"dc_expected_w\":%.0f,"
			 "\"noon_h\":%.2f,\"peak_today\":%d,\"cap_active\":%d,\"ladesperre_active\":%d,\"version\":\"%s-esp\"}",
			 (long)in_last.t, o_.ls.peak_h, o_.ls.win_end_h, o_.ls.ratio, o_.ls.ratio_now, o_.ls.dc, o_.ls.noon_h,
			 st.peak_today, o_.ls.cap, o_.ls.active, BM_VERSION);
	return std::string(buf);
}

// batmonitor/do4_last after a local pulse (r290_boost and hantech read it, as after a batmonitor pulse)
inline std::string do4_last_json()
{
	char buf[96];
	snprintf(buf, sizeof(buf), "{\"ts\":%ld,\"pcc_w\":%.0f}", (long)in_last.t, in_last.pcc);
	return std::string(buf);
}

inline std::string params_json()
{
	ensure_init();
	std::string s = "{";
	char b[64];
	for (int i = 0; i < BM_NPARAMS; i++) {
		snprintf(b, sizeof(b), "%s\"%s\":%g", i ? "," : "", BM_PARAMS[i].name, *bm_param_ptr(&cfg, i));
		s += b;
	}
	snprintf(b, sizeof(b), ",\"LADESPERRE\":%d,\"FORECAST\":%d,\"PI\":%d}", cfg.ladesperre, cfg.forecast, cfg.pi_armed);
	return s + b;
}

}  // namespace bmesp
