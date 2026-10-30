#!/usr/bin/env python3
"""
pytes_discharge.py - discharge the Pytes (EBox, Multis on L2 + L3) with a given power, e.g. into the grid.

batmonitor is stopped (svc -d) for the run and started again at the end (svc -u), also on Ctrl-C / SIGTERM / errors.
Every CYCLE_S the Hub4 setpoints are written like batmonitor's send_setpoints (Hub4Mode 3, /Hub4/Lx/AcPowerSetpoint
is the AC-in power): L2 / L3 = AC-out load - power/2 (battery side: each Pytes Multi discharges power/2, what the
house on AC-out does not take goes into the grid), L1 (MUST) = AC-out load (stack idle).

Stops when
  - EBox SoC <= --until-soc
  - --minutes elapsed (default 120)
  - Pytes DCL (/Info/Pack/MaxDischargeCurrent) is 0, the EBox service or the vebus is missing, the grid is gone
  - EBox alarm HighDischargeCurrent or LowVoltage
Caveat: if this script is killed with SIGKILL, the last setpoints stay until batmonitor is started again:
  sudo nsenter -t $(machinectl show venus -p Leader --value) -a svc -u /service/batmonitor

Runs on the Venus host as pi (sudo for nsenter into the container 'venus').

Examples:
  pytes_discharge.py 3000 --until-soc 98
  pytes_discharge.py 2000 --minutes 30
"""
import argparse
import json
import signal
import subprocess
import sys
import time

EBOX = 'com.victronenergy.battery.ebox'
BATMONITOR = '/service/batmonitor'
PYTES_PHASES = ('L2', 'L3')
ALL_PHASES = ('L1', 'L2', 'L3')
MAX_POWER_W = 5000      # 2026-10-30 only 2 of 3 packs present, DOCAlm seen at about 45 A per pack
CYCLE_S = 5

# runs inside the Venus container: reads the values, writes the setpoints (if given), prints JSON
INNER = r'''
import dbus, json, sys
args = json.loads(sys.argv[1])
bus = dbus.SystemBus()
def get(s, p):
	try:
		v = bus.call_blocking(s, p, 'com.victronenergy.BusItem', 'GetValue', '', [], timeout=3)
	except dbus.exceptions.DBusException:
		return None
	return None if isinstance(v, dbus.Array) else float(v)
def put(s, p, v):
	bus.call_blocking(s, p, 'com.victronenergy.BusItem', 'SetValue', 'v', [dbus.Int32(v)], timeout=3)
vb = next((n for n in bus.list_names() if str(n).startswith('com.victronenergy.vebus.')), None)
out = {'vebus': vb}
if vb:
	out['grid'] = get(vb, '/Ac/ActiveIn/Connected')
	for ph in ('L1', 'L2', 'L3'):
		out['out_' + ph] = get(vb, '/Ac/Out/%s/P' % ph)
		out['in_' + ph] = get(vb, '/Ac/ActiveIn/%s/P' % ph)
	for ph, ac in (args.get('set') or {}).items():
		put(vb, '/Hub4/%s/MaxFeedInPower' % ph, (-ac if ac < 0 else 0) + 100)
		put(vb, '/Hub4/%s/AcPowerSetpoint' % ph, ac)
for k, p in (('soc', '/Soc'), ('dcl', '/Info/Pack/MaxDischargeCurrent'), ('current', '/Dc/0/Current'),
		('power', '/Dc/0/Power'), ('a_hdc', '/Alarms/HighDischargeCurrent'), ('a_lv', '/Alarms/LowVoltage')):
	out[k] = get('EBOX', p)
print(json.dumps(out))
'''.replace('EBOX', EBOX)

_leader = None


def run(cmd, timeout=15):
	global _leader
	if _leader is None:
		_leader = subprocess.run(['sudo', 'machinectl', 'show', 'venus', '-p', 'Leader', '--value'],
			capture_output=True, text=True, timeout=10, check=True).stdout.strip()
	return subprocess.run(['sudo', 'nsenter', '-t', _leader, '-a', *cmd], capture_output=True, text=True,
		timeout=timeout)


def cycle(setpoints=None):
	"""read (and write the setpoints {phase: AC-in W}); dict of the values"""
	r = run(['python3', '-c', INNER, json.dumps({'set': setpoints})])
	if r.returncode:
		raise RuntimeError('venus: ' + (r.stderr.strip().splitlines() or ['?'])[-1])
	return json.loads(r.stdout)


def batmonitor(up):
	r = run(['svc', '-u' if up else '-d', BATMONITOR])
	print('batmonitor %s%s' % ('started' if up else 'stopped', (': ' + r.stderr.strip()) if r.returncode else ''),
		flush=True)


class Stop(Exception):
	pass


def on_signal(sig, frame):
	raise Stop('signal %d' % sig)


def main():
	ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
	ap.add_argument('power', type=float, help='discharge power of the Pytes in W (sum L2 + L3)')
	ap.add_argument('--until-soc', type=float, default=None, help='stop at this EBox SoC (%%)')
	ap.add_argument('--minutes', type=float, default=120, help='stop after this time (default 120)')
	a = ap.parse_args()
	if not 0 < a.power <= MAX_POWER_W:
		sys.exit('power must be 1..%d W' % MAX_POWER_W)

	v = cycle()
	if not v.get('vebus') or v.get('soc') is None:
		sys.exit('vebus or EBox missing on dbus: %s' % v)
	if a.until_soc is not None and v['soc'] <= a.until_soc:
		sys.exit('EBox SoC %.1f %% already <= %.1f %%' % (v['soc'], a.until_soc))
	print('EBox SoC %.1f %%, discharging %.0f W (%.0f W each on %s) until %s, at most %.0f min' % (v['soc'], a.power,
		a.power / len(PYTES_PHASES), '+'.join(PYTES_PHASES),
		'SoC %.1f %%' % a.until_soc if a.until_soc is not None else '-', a.minutes), flush=True)

	for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
		signal.signal(s, on_signal)
	t0 = time.monotonic()
	reason = 'done'
	batmonitor(False)
	try:
		while True:
			if v.get('grid') != 1:
				raise Stop('grid not connected')
			if not v.get('dcl'):
				raise Stop('Pytes DCL %s' % v.get('dcl'))
			if v.get('a_hdc') or v.get('a_lv'):
				raise Stop('EBox alarm HighDischargeCurrent %s LowVoltage %s' % (v.get('a_hdc'), v.get('a_lv')))
			if a.until_soc is not None and v.get('soc') is not None and v['soc'] <= a.until_soc:
				raise Stop('SoC %.1f %% reached' % v['soc'])
			if time.monotonic() - t0 >= a.minutes * 60:
				raise Stop('%.0f min elapsed' % a.minutes)
			sp = {}
			for ph in ALL_PHASES:
				load = v.get('out_' + ph) or 0.0
				sp[ph] = int(round(load - (a.power / len(PYTES_PHASES) if ph in PYTES_PHASES else 0)))
			v = cycle(sp)
			if not v.get('vebus'):
				raise Stop('vebus missing')
			print('%s SoC %5.1f %%  EBox %6.0f W %6.1f A  setpoints %s  AC-in %s' % (time.strftime('%H:%M:%S'),
				v.get('soc') or 0, v.get('power') or 0, v.get('current') or 0,
				' '.join('%s %d' % kv for kv in sp.items()),
				' '.join('%s %.0f' % (ph, v.get('in_' + ph) or 0) for ph in ALL_PHASES)), flush=True)
			time.sleep(CYCLE_S)
	except Stop as e:
		reason = str(e)
	except Exception as e:
		reason = 'error: %s' % e
	finally:
		signal.signal(signal.SIGINT, signal.SIG_IGN)
		signal.signal(signal.SIGTERM, signal.SIG_IGN)
		batmonitor(True)
		print('end: %s' % reason, flush=True)


if __name__ == '__main__':
	main()
