#!/usr/bin/env python3
"""
vebus_mode.py - read and set the VE.Bus mode of the Multis through Venus (com.victronenergy.vebus.*, path /Mode).

Without an argument it only reads: current mode, state, active input, AC-in/AC-out power and voltage per phase,
plus ESS mode (Hub4Mode) and BatteryLife state for reference.
With a mode it reads first, asks for confirmation (unless --yes), writes /Mode, then reads back and follows /State
for --wait seconds.

Modes:  1 charger  = Charger only  (AC-out only from the grid, no inverter: no grid -> AC-out dead)
        2 inverter = Inverter only (AC-in disconnected, AC-out from the battery; 2026-10-30 house outage at Mode 2)
        3 on       = On (normal)
        4 off      = Off (AC-out dead)

Talks to the Venus MQTT broker (dbus-flashmq, port 1883) over the network, so it runs on any host with
python3-paho-mqtt (1.x or 2.x) that reaches the Venus host: N/<portal>/... to read (after R/<portal>/keepalive),
W/<portal>/vebus/<inst>/Mode to write. Broker: --host, else env VENUS_HOST, else 192.168.178.123
(Venus-Pi, nspawn container 'venus'; 2026-10-30 moved from sudo nsenter + dbus to MQTT).
Login: with Venus security profile 'secured' the broker wants the Venus (Remote Console) password for network
clients; it comes from env VENUS_MQTT_PASSWORD or from ~/.vebus_mode.cnf (chmod 600), lines
  host = 192.168.178.123
  password = ...
Without a password it connects anonymously (works on the Venus host itself).

Examples:
  vebus_mode.py              # read only
  vebus_mode.py on           # back to normal
  vebus_mode.py 2 --wait 20
  vebus_mode.py --host 10.8.0.x
"""
import argparse
import json
import os
import sys
import threading
import time

import paho.mqtt.client as mqtt

CNF = os.path.expanduser('~/.vebus_mode.cnf')


def read_cnf():
	cnf = {}
	try:
		with open(CNF) as f:
			for line in f:
				k, sep, v = line.partition('=')
				if sep and not line.lstrip().startswith('#'):
					cnf[k.strip()] = v.strip()
	except OSError:
		pass
	return cnf


_CNF = read_cnf()
DEFAULT_HOST = os.environ.get('VENUS_HOST') or _CNF.get('host') or '192.168.178.123'
PASSWORD = os.environ.get('VENUS_MQTT_PASSWORD') or _CNF.get('password')
MODES = {1: 'Charger only', 2: 'Inverter only', 3: 'On', 4: 'Off'}
NAMES = {'charger': 1, 'inverter': 2, 'on': 3, 'off': 4}
STATES = {0: 'Off', 1: 'Low power', 2: 'Fault', 3: 'Bulk', 4: 'Absorption', 5: 'Float', 6: 'Storage',
	7: 'Equalize', 8: 'Passthru', 9: 'Inverting', 10: 'Power assist', 11: 'Power supply', 244: 'Sustain', 252: 'External control'}
INPUTS = {0: 'AC-in 1', 1: 'AC-in 2', 240: 'disconnected'}
ESS_MODES = {1: 'Optimized + phase compensation', 2: 'Optimized without phase compensation', 3: 'External control'}
SETTINGS = ('/Settings/CGwacs/Hub4Mode', '/Settings/CGwacs/BatteryLife/State')
CORE = ('/Mode', '/State', '/Ac/ActiveIn/ActiveInput')


class Venus:
	"""value cache fed by the N/<portal>/... topics of the Venus MQTT broker"""

	def __init__(self, host, password=None, timeout=10):
		self.portal = None
		self.vebus = None                    # instance of the first vebus service seen
		self.values = {}                     # (service, path) -> value
		self.lock = threading.Lock()
		self.got_portal = threading.Event()
		try:
			self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id='vebus_mode-%d' % os.getpid())
		except AttributeError:               # paho-mqtt 1.x
			self.client = mqtt.Client(client_id='vebus_mode-%d' % os.getpid())
		self.client.on_message = self.on_message
		self.client.on_connect = self.on_connect
		self.connect_rc = None
		self.connected = threading.Event()
		if password:
			self.client.username_pw_set('vebus_mode', password)
		try:
			self.client.connect(host, 1883, 30)
		except OSError as e:
			sys.exit('MQTT %s:1883: %s' % (host, e))
		self.client.loop_start()
		if not self.connected.wait(timeout):
			sys.exit('no CONNACK from %s:1883 within %d s' % (host, timeout))
		if self.connect_rc:
			sys.exit('MQTT %s:1883 refused: %s%s' % (host, self.connect_rc,
				'' if password else ' (no password set, see ~/.vebus_mode.cnf / VENUS_MQTT_PASSWORD)'))
		self.client.subscribe('N/+/system/0/Serial')
		if not self.got_portal.wait(timeout):
			sys.exit('no N/+/system/0/Serial from %s within %d s' % (host, timeout))
		self.client.subscribe('N/%s/vebus/#' % self.portal)
		for p in SETTINGS:
			self.client.subscribe('N/%s/settings/0%s' % (self.portal, p))
		self.keepalive()
		self.last_keepalive = time.time()
		t0 = time.time()
		while time.time() - t0 < timeout and not all(self.get('vebus', p) is not None for p in CORE):
			time.sleep(0.1)
		if self.vebus is None:
			sys.exit('no com.victronenergy.vebus.* service on %s' % host)
		time.sleep(1)                        # let the rest of the full republish arrive

	def on_connect(self, client, userdata, flags, rc, *props):
		self.connect_rc = rc if (rc if isinstance(rc, int) else rc.value) else 0
		self.connected.set()

	def keepalive(self):
		self.client.publish('R/%s/keepalive' % self.portal, '')
		self.last_keepalive = time.time()

	def on_message(self, client, userdata, msg):
		p = msg.topic.split('/')            # N, <portal>, <service>, <instance>, <path...>
		if len(p) < 5:
			return
		try:
			v = json.loads(msg.payload).get('value')
		except (ValueError, AttributeError):
			return
		if p[2] == 'system' and p[4] == 'Serial' and self.portal is None:
			self.portal = p[1]
			self.got_portal.set()
			return
		service = p[2]
		if service == 'vebus':
			with self.lock:
				if self.vebus is None:
					self.vebus = p[3]
			if p[3] != self.vebus:
				return
		with self.lock:
			self.values[(service, '/' + '/'.join(p[4:]))] = v

	def get(self, service, path):
		if time.time() - getattr(self, 'last_keepalive', time.time()) > 30:
			self.keepalive()
		with self.lock:
			v = self.values.get((service, path))
		return None if isinstance(v, (list, dict)) else v

	def set_mode(self, mode):
		info = self.client.publish('W/%s/vebus/%s/Mode' % (self.portal, self.vebus), json.dumps({'value': mode}))
		info.wait_for_publish(5)
		return info.rc == mqtt.MQTT_ERR_SUCCESS

	def close(self):
		self.client.loop_stop()
		self.client.disconnect()


def fmt(v, unit=''):
	if v is None:
		return '-'
	return ('%.0f' % v if isinstance(v, float) else str(v)) + unit


def show(vn, host):
	get = lambda path: vn.get('vebus', path)
	mode = get('/Mode')
	state = get('/State')
	inp = get('/Ac/ActiveIn/ActiveInput')
	print('broker          %s  portal %s' % (host, vn.portal))
	print('service         com.victronenergy.vebus (instance %s)' % vn.vebus)
	print('mode            %s = %s%s' % (mode, MODES.get(mode, '?'),
		'' if get('/ModeIsAdjustable') == 1 else '  (ModeIsAdjustable != 1)'))
	print('state           %s = %s' % (state, STATES.get(state, '?')))
	print('active input    %s = %s' % (inp, INPUTS.get(inp, '?')))
	print('VebusError      %s' % fmt(get('/VebusError')))
	print('             AC-in V   AC-in W   AC-out V  AC-out W')
	for ph in ('L1', 'L2', 'L3'):
		print('  %s        %8s  %8s  %8s  %8s' % (ph,
			fmt(get('/Ac/ActiveIn/%s/V' % ph)), fmt(get('/Ac/ActiveIn/%s/P' % ph)),
			fmt(get('/Ac/Out/%s/V' % ph)), fmt(get('/Ac/Out/%s/P' % ph))))
	print('  total               %8s            %8s' % (fmt(get('/Ac/ActiveIn/P')), fmt(get('/Ac/Out/P'))))
	print('battery         %s V  %s A  SOC %s %%' % (fmt(get('/Dc/0/Voltage')), fmt(get('/Dc/0/Current')),
		fmt(get('/Soc'))))
	ess = vn.get('settings', SETTINGS[0])
	print('ESS Hub4Mode    %s = %s' % (ess, ESS_MODES.get(ess, '?')))
	print('BatteryLife     %s' % fmt(vn.get('settings', SETTINGS[1])))
	return mode


def parse_mode(s):
	s = s.lower()
	if s in NAMES:
		return NAMES[s]
	if s.isdigit() and int(s) in MODES:
		return int(s)
	raise argparse.ArgumentTypeError('mode must be 1-4 or one of %s' % ', '.join(NAMES))


def main():
	ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
	ap.add_argument('mode', nargs='?', type=parse_mode, help='1/charger, 2/inverter, 3/on, 4/off; omit to only read')
	ap.add_argument('--host', default=DEFAULT_HOST, help='Venus MQTT broker (default %s, env VENUS_HOST)' % DEFAULT_HOST)
	ap.add_argument('--yes', action='store_true', help='no confirmation prompt')
	ap.add_argument('--wait', type=float, default=10, help='seconds to follow /State after writing (default 10)')
	a = ap.parse_args()

	vn = Venus(a.host, PASSWORD)
	try:
		cur = show(vn, a.host)
		if a.mode is None:
			return
		if a.mode == cur:
			print('\nmode already %d = %s, nothing written' % (cur, MODES[cur]))
			return
		print('\nchange mode %s = %s  ->  %d = %s' % (cur, MODES.get(cur, '?'), a.mode, MODES[a.mode]))
		if a.mode in (1, 2, 4):
			print('WARNING: %s can switch AC-out (house) off; back with: vebus_mode.py on' % MODES[a.mode])
		if not a.yes and input('write? [y/N] ').strip().lower() not in ('y', 'j', 'yes', 'ja'):
			print('aborted')
			return
		print('SetValue: %s' % ('published' if vn.set_mode(a.mode) else 'publish FAILED'))
		t0 = time.time()
		last = None
		while True:
			mode, state = vn.get('vebus', '/Mode'), vn.get('vebus', '/State')
			if (mode, state) != last:
				print('%5.1f s  mode %s = %s  state %s = %s' % (time.time() - t0, mode, MODES.get(mode, '?'),
					state, STATES.get(state, '?')))
				last = (mode, state)
			if time.time() - t0 >= a.wait:
				break
			time.sleep(0.5)
		if mode != a.mode:
			sys.exit('mode reads back %s, expected %d' % (mode, a.mode))
	finally:
		vn.close()


if __name__ == '__main__':
	main()
