// bm_esp.h: batmonitor ESP32 shadow (0.47-c) on the pv_relay board. Runs bm_step (the same bm_logic.c as Venus) on
// the inputs Venus publishes on batmonitor/shadow_in and compares the setpoints with venus_sp. Writes nothing to the
// Multis: a second controller would fight the first one.
#pragma once
#include <cstdio>
#include <cstring>
#include <string>
#include "bm_wire.h"

namespace bmshadow {

static struct bm_cfg cfg;
static struct bm_state st;
static struct bm_in in;
static struct bm_out out;
static bool ready = false;
static int cfg_keys = 0;
static uint32_t cycles = 0, matched = 0, mismatched = 0;

static void ensure_init()
{
	if (ready)
		return;
	bm_cfg_default(&cfg);
	bm_init(&st);
	ready = true;
}

// batmonitor/shadow_cfg (retained): the Venus parameters
inline void on_cfg(const std::string &s)
{
	ensure_init();
	cfg_keys = bm_wire_cfg_decode(s.c_str(), &cfg);
}

// batmonitor/shadow_in: one cycle; returns the JSON for batmonitor_esp/out
inline std::string on_in(const std::string &s)
{
	ensure_init();
	memset(&in, 0, sizeof(in));
	int keys = bm_wire_in_decode(s.c_str(), &in);
	int vsp[NPH] = {0, 0, 0};
	const char *p = strstr(s.c_str(), "venus_sp=");
	bool have_v = p && sscanf(p + 9, "%d,%d,%d", &vsp[0], &vsp[1], &vsp[2]) == 3;
	uint32_t t0 = micros();
	bm_step(&cfg, &in, &st, &out);
	uint32_t us = micros() - t0;
	cycles++;
	bool match = have_v && !memcmp(out.sp, vsp, sizeof(vsp));
	if (have_v)
		(match ? matched : mismatched)++;
	char buf[640];
	snprintf(buf, sizeof(buf),
			 "{\"sp\":[%d,%d,%d],\"venus_sp\":[%d,%d,%d],\"match\":%d,\"cycles\":%u,\"matched\":%u,\"mismatched\":%u,"
			 "\"us\":%u,\"in_keys\":%d,\"cfg_keys\":%d,\"heap\":%u,\"rule\":\"%s\"}",
			 out.sp[0], out.sp[1], out.sp[2], vsp[0], vsp[1], vsp[2], match ? 1 : 0, (unsigned)cycles, (unsigned)matched,
			 (unsigned)mismatched, (unsigned)us, keys, cfg_keys, (unsigned)ESP.getFreeHeap(), out.why[0]);
	return std::string(buf);
}

}  // namespace bmshadow
