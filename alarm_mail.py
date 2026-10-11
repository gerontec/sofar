#!/usr/bin/env python3
"""
alarm_mail.py - alarm mails of the Lenggries system in two categories (user 2026-10-11: one service, one mail path):
  TEMP    a temperature sensor reads above its threshold (below)
  RELAIS  a relay of the Victron system drops, with the cause from the data (after 2026-10-11 04:05-08:20, when the
          cluster stood still without anybody noticing):
            GRID  vebus Ac/ActiveIn/Connected 1 -> 0: the MultiPlus grid (transfer) relay opened
            OFF   vebus Mode 3 -> other: switched off over D-Bus (e.g. systemcalc LG detection, 2026-10-11 04:05:57
                  with the wrong productid 0xB004 of dbus-ebox-battery)
            BMS   battery/<n>/Connected 1 -> 0 or the service vanished: BMS gone, its relay probably open
            DC    Devices/<n>/Diagnostics/UBatTerminal below DC_LOST_V: no battery at that unit's terminals
          EVENT_WAIT_S after the event the values are checked against the cause rules (cause()); the mail carries the
          causes, the values RING_S before and now and the last lines of the Venus service logs; all-clear mail when
          it is back.

TEMP: emergency mail when any single temperature sensor in the house reads above its threshold: batteries
(Pytes ebox/..., MUST venus:battery/...) above BAT_THRESHOLD_C (60 °C, BMS alarm range; Pytes board max so far 41 °C),
every other sensor above THRESHOLD_C (98 °C).

Runs as a systemd service on the Pi .119 (Venus host): from there it reaches the Venus MQTT broker (local only,
the BMS of Stack1 MUST) and the house broker .218 (everything else).
  .218        every numeric field whose name contains "temp" in any topic: Pytes ebox/pwr (temp_max_c, temps_c per
              pack), R290 (temp_exhaust, temp_outlet, ...), outdoor, rooms ... new sensors are picked up by
              themselves. Forecasts (wetter/...) are no sensors and skipped.
  Venus       battery/<n>/Dc/0/Temperature and System/MaxCellTemperature (MUST BMS), temperature/<n>/Temperature
Against false alarms (history: single Modbus reads of 16896 / 59136 / 65506 "°C"): only values in PLAUSIBLE_C count,
and a sensor has to be above the threshold in CONFIRM consecutive readings. Then one mail per sensor, a reminder
every REMIND_S while it stays above, and an all-clear once it is below RELEASE_C.

usage: alarm_mail.py            run (service)
       alarm_mail.py --test     one [TEST] mail with the hottest readings and the relay diagnosis after 20 s, then exit
       alarm_mail.py --dry-run  log instead of mailing
"""
import collections
import json
import re
import subprocess
import smtplib
import sys
import threading
import time
from email.message import EmailMessage
from email.utils import formatdate

import paho.mqtt.client as mqtt

THRESHOLD_C = 98.0
RELEASE_C = 90.0
BAT_THRESHOLD_C = 60.0          # batteries: early warning (user 2026-10-10)
BAT_RELEASE_C = 55.0
BATTERY = re.compile(r"^(ebox/|venus:battery/)")
PLAUSIBLE_C = (-40.0, 250.0)
CONFIRM = 2
REMIND_S = 3600
HOUSE_BROKER = "192.168.178.218"
VENUS_BROKER = "127.0.0.1"
VENUS_PORTAL = "e45f01114b2c"
VENUS_KEEPALIVE_S = 30
MAIL_TO = "gh@heissa.de"
MAIL_FROM = "alarm@heissa.de"
SMTP_HOSTS = ("heissa.de", "10.8.0.1")      # the MX of heissa.de accepts mail to gh@ directly (port 25)
TEMP_KEY = re.compile(r"temp", re.I)
SKIP_TOPIC = re.compile(r"^(wetter/|weather/)")

lock = threading.Lock()
sensors = {}        # name -> {"value", "ts", "over", "alarm", "mailed"}
DRY = "--dry-run" in sys.argv


def log(msg):
    print(time.strftime("%Y-%m-%d %H:%M:%S ") + msg, flush=True)


def send_mail(subject, body):
    if DRY:
        log("DRY mail: %s" % subject)
        return True
    msg = EmailMessage()
    msg["From"], msg["To"], msg["Subject"], msg["Date"] = MAIL_FROM, MAIL_TO, subject, formatdate(localtime=True)
    msg.set_content(body)
    for host in SMTP_HOSTS:
        try:
            with smtplib.SMTP(host, 25, timeout=20) as s:
                s.send_message(msg)
            log("mail via %s: %s" % (host, subject))
            return True
        except (OSError, smtplib.SMTPException) as e:
            log("mail via %s failed: %s" % (host, e))
    return False


def hottest(n=12):
    with lock:
        items = sorted(((v["value"], k) for k, v in sensors.items()), reverse=True)[:n]
    return "\n".join("  %6.1f °C  %s" % (val, name) for val, name in items)


def limits(name):
    """(alarm threshold, all-clear) of a sensor: batteries earlier"""
    return (BAT_THRESHOLD_C, BAT_RELEASE_C) if BATTERY.match(name) else (THRESHOLD_C, RELEASE_C)


def reading(name, value):
    """one plausible reading of one sensor: confirm, alarm, remind, all-clear"""
    if not (PLAUSIBLE_C[0] <= value <= PLAUSIBLE_C[1]):
        return
    threshold, release = limits(name)
    now = time.time()
    mail = None
    with lock:
        s = sensors.setdefault(name, {"value": value, "ts": now, "over": 0, "alarm": False, "mailed": 0.0})
        s["value"], s["ts"] = value, now
        if value > threshold:
            s["over"] += 1
            if s["over"] >= CONFIRM and (not s["alarm"] or now - s["mailed"] >= REMIND_S):
                mail = ("ALARM" if not s["alarm"] else "STILL HOT", value)
                s["alarm"], s["mailed"] = True, now
        else:
            s["over"] = 0
            if s["alarm"] and value < release:
                s["alarm"] = False
                mail = ("all clear", value)
    if mail:
        kind, v = mail
        subject = "%s TEMP Lenggries: %s %.1f °C (threshold %.0f °C)" % (kind, name, v, threshold)
        body = ("%s\n\nSensor:  %s\nValue:   %.1f °C\nTime:    %s\nRule:    above %.0f °C in %d consecutive readings; "
                "reminder every %d min; all clear below %.0f °C\n\nHottest sensors now:\n%s\n"
                % (subject, name, v, time.strftime("%Y-%m-%d %H:%M:%S"), threshold, CONFIRM, REMIND_S // 60,
                   release, hottest()))
        log(subject)
        send_mail(subject, body)


# ---- RELAIS ----------------------------------------------------------------------------------------
VEBUS = 290
BATTERIES = {512: "MUST (CAN-BMS, Stack1, L1 = VE.Bus master)", 513: "Pytes EBox (Stack2, L2+L3)"}
DC_LOST_V = 40.0            # V at a unit's battery terminals: below = no battery (LFP 16S never below 40 V)
EVENT_WAIT_S = 5            # s after the event: values settle, then the diagnosis
RING_S = 30                 # s: the "before" snapshot
REPEAT_S = 600              # s: at most one mail per event kind and source in this time
GRID_V = (184.0, 264.5)     # V: VDE-AR-N 4105 limits (0.8 / 1.15 Un)
GRID_F = (47.5, 51.5)       # Hz
LOGS = ("dbus-systemcalc-py", "hub4control", "mk2_dbus.ttyUSB2", "can-bus-bms.can0", "dbus-ebox-battery")
VEBUS_ERROR = {
    1: "a device switched off because another phase in the system switched off",
    2: "new and old MK2 types mixed in the system",
    3: "not all (or more than) the expected devices found in the system",
    4: "no other device detected",
    5: "overvoltage on AC-out",
    8: "ground relay test failed",
    10: "system time synchronisation problem",
    14: "device cannot transmit data",
    15: "VE.Bus communication problem",
    16: "dongle missing",
    17: "phase master missing",
    18: "AC overvoltage on the output of a slave",
    22: "device cannot be slave",
    24: "switch-over system protection initiated",
    25: "firmware incompatibility",
    26: "internal error",
}

rlock = threading.Lock()
values = {}                                  # path below N/<portal>/ -> value (None = removed)
ring = collections.deque()                   # (t, path, value) for the snapshot RING_S before
state = {"grid": None, "mode": None, "bms": {}, "dc": {}}
last_mail = {}


def v(path, default=None):
    with rlock:
        x = values.get(path)
    return default if x is None else x


def before(path, t_event):
    """the value of path RING_S before the event (the newest one older than that, else the oldest known)"""
    with rlock:
        old = [val for t, p, val in ring if p == path and t <= t_event - RING_S]
        if old:
            return old[-1]
        any_ = [val for t, p, val in ring if p == path]
    return any_[0] if any_ else None


def venus_logs(n=12):
    """the last lines of the Venus service logs (inside the nspawn container, tai64n -> local time)"""
    try:
        leader = subprocess.run(["machinectl", "show", "venus", "-p", "Leader", "--value"], capture_output=True,
                                text=True, timeout=5).stdout.strip()
        cmd = "; ".join("echo '== %s'; tail -n %d /var/log/%s/current" % (s, n, s) for s in LOGS)
        out = subprocess.run(["sudo", "-n", "nsenter", "-t", leader, "-a", "sh", "-c", cmd], capture_output=True,
                             text=True, timeout=10).stdout
    except (OSError, subprocess.SubprocessError) as e:
        return "(logs not readable: %s)" % e
    lines = []
    for line in out.splitlines():
        if line.startswith("@4") and len(line) > 26:
            try:
                t = int(line[1:17], 16) - 2 ** 62 - 37
                line = time.strftime("%m-%d %H:%M:%S", time.localtime(t)) + line[25:]
            except ValueError:
                pass
        lines.append(line[:220])
    return "\n".join(lines)


def batmonitor_rule():
    try:
        with open("/data/batmonitor/state.json") as f:
            s = json.load(f)
        return "batmonitor %s: rule %s, setpoints %s" % (s.get("version"), s.get("rule"), s.get("sp"))
    except (OSError, ValueError) as e:
        return "batmonitor state.json not readable: %s" % e


def cause(t_event):
    """the causes the data shows, most specific first"""
    vb = "vebus/%d/" % VEBUS
    found = []
    mode, mode_before = v(vb + "Mode"), before(vb + "Mode", t_event)
    if mode is not None and mode != 3:
        found.append("VE.Bus switched to Mode %s (%s) over D-Bus/MQTT, not by the unit itself; was %s. Who wrote it: "
                     "see the dbus-systemcalc-py / hub4control lines below (e.g. 'LG shutdown detected')"
                     % (mode, {1: "charger only", 2: "inverter only", 4: "OFF"}.get(mode, "?"), mode_before))
    err = v(vb + "VebusError", 0)
    if err:
        found.append("VE.Bus error %s: %s" % (err, VEBUS_ERROR.get(int(err), "see Victron VE.Bus error codes")))
    if v(vb + "Alarms/GridLost"):
        found.append("grid lost alarm")
    for ph in ("L1", "L2", "L3"):
        u, f = v(vb + "Ac/ActiveIn/%s/V" % ph), v(vb + "Ac/ActiveIn/%s/F" % ph)
        if u is not None and not GRID_V[0] <= u <= GRID_V[1]:
            found.append("grid voltage %s %.1f V outside %.0f-%.1f V (grid code)" % (ph, u, *GRID_V))
        if f is not None and f > 1 and not GRID_F[0] <= f <= GRID_F[1]:
            found.append("grid frequency %s %.2f Hz outside %.1f-%.1f Hz (grid code)" % (ph, f, *GRID_F))
    if v(vb + "Ac/State/IgnoreAcIn1") or v(vb + "Ac/Control/IgnoreAcIn1"):
        found.append("AC-in 1 is ignored (IgnoreAcIn1: ESS / assistant / control)")
    for n in range(3):
        d = vb + "Devices/%d/" % n
        if v(d + "ExtendStatus/IgnoreAcIn1") or v(d + "ExtendStatus/IgnoreAcIn1AssistantsVs"):
            found.append("unit %d ignores AC-in (assistant)" % n)
        fl = v(d + "ExtendStatus/SwitchoverInfo/ErrorFlags")
        if fl:
            found.append("unit %d switch-over error flags 0x%x (relay test / grid code)" % (n, int(fl)))
        for k in ("NSErrRelayTestKeepsFailing", "NSErrConnectFrustratedByRelayTest", "WarnRelayTestRecentlyFailed"):
            if v(d + "ErrorAndWarningFlags/" + k):
                found.append("unit %d %s" % (n, k))
        if v(d + "ExtendStatus/TemperatureHighForceBypass"):
            found.append("unit %d temperature high, forced bypass" % n)
        u = v(d + "Diagnostics/UBatTerminal")
        if u is not None and u < DC_LOST_V:
            found.append("unit %d has no battery at its terminals: %.2f V (BMS relay open / battery switched off)"
                         % (n, u))
    for inst, name in BATTERIES.items():
        if v("battery/%d/Connected" % inst) != 1:
            found.append("BMS %d %s missing (not connected / service gone)" % (inst, name))
        elif v("battery/%d/Info/MaxDischargeCurrent" % inst) == 0:
            found.append("BMS %d %s sends DCL 0 (discharge blocked)" % (inst, name))
    for a in ("Overload", "HighTemperature", "LowBattery", "PhaseRotation", "Ripple", "HighDcVoltage",
              "HighDcCurrent", "VoltageSensor", "TemperatureSensor"):
        if v(vb + "Alarms/" + a):
            found.append("VE.Bus alarm %s = %s" % (a, v(vb + "Alarms/" + a)))
    return found or ["no cause found in the VE.Bus / BMS data: see the values and logs below"]


SNAP = ["vebus/%d/%s" % (VEBUS, p) for p in (
    "State", "Mode", "VebusError", "Ac/ActiveIn/Connected", "Ac/ActiveIn/ActiveInput", "Ac/ActiveIn/L1/V",
    "Ac/ActiveIn/L2/V", "Ac/ActiveIn/L3/V", "Ac/ActiveIn/L1/F", "Ac/ActiveIn/L1/P", "Ac/ActiveIn/L2/P",
    "Ac/ActiveIn/L3/P", "Dc/0/Voltage", "Dc/0/Current", "Devices/0/Diagnostics/UBatTerminal",
    "Devices/1/Diagnostics/UBatTerminal", "Devices/2/Diagnostics/UBatTerminal", "Alarms/GridLost")] + \
    ["battery/%d/%s" % (i, p) for i in BATTERIES for p in (
        "Connected", "Soc", "Dc/0/Voltage", "Dc/0/Current", "System/MinCellVoltage", "Info/MaxChargeCurrent",
        "Info/MaxDischargeCurrent")]


def relay_report(kind, what, t_event, ok=False):
    key = (kind, what, ok)
    if not ok and time.time() - last_mail.get(key, 0) < REPEAT_S:
        log("%s %s: repeated within %d s, no mail" % (kind, what, REPEAT_S))
        return
    last_mail[key] = time.time()
    when = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t_event))
    if ok:
        subject = "OK RELAIS Lenggries: %s %s again (%s)" % (kind, what, when)
        body = "%s\n\n%s\n" % (subject, batmonitor_rule())
    else:
        causes = cause(t_event)
        subject = "ALARM RELAIS Lenggries: %s %s at %s - %s" % (kind, what, when, causes[0][:90])
        rows = "\n".join("  %-55s %14s  %14s" % (p, before(p, t_event), v(p)) for p in SNAP)
        body = ("%s\n\nCause (from the data, most specific first):\n%s\n\n%s\n\nValues %d s before / now:\n%s\n\n"
                "Venus logs (last lines):\n%s\n"
                % (subject, "\n".join("  - " + c for c in causes), batmonitor_rule(), RING_S, rows, venus_logs()))
    log(subject)
    send_mail(subject, body)


def relay_event(kind, what, ok=False):
    t = time.time()
    threading.Timer(EVENT_WAIT_S, relay_report, (kind, what, t, ok)).start()


def relay_check(path, val):
    """edge detection on the watched paths (the first value after start only sets the state)"""
    vb = "vebus/%d/" % VEBUS
    if path == vb + "Ac/ActiveIn/Connected" and val is not None:
        prev, state["grid"] = state["grid"], val
        if prev == 1 and val == 0:
            relay_event("GRID", "MultiPlus grid relay opened (AC-in disconnected)")
        elif prev == 0 and val == 1:
            relay_event("GRID", "MultiPlus grid relay closed", ok=True)
    elif path == vb + "Mode" and val is not None:
        prev, state["mode"] = state["mode"], val
        if prev == 3 and val != 3:
            relay_event("OFF", "VE.Bus Mode %s -> %s" % (prev, val))
        elif prev is not None and prev != 3 and val == 3:
            relay_event("OFF", "VE.Bus Mode 3 (on)", ok=True)
    elif path.startswith("battery/") and path.endswith("/Connected"):
        inst = int(path.split("/")[1])
        if inst not in BATTERIES:
            return
        now = 1 if val == 1 else 0
        prev, state["bms"][inst] = state["bms"].get(inst), now
        if prev == 1 and now == 0:
            relay_event("BMS", "%d %s lost" % (inst, BATTERIES[inst]))
        elif prev == 0 and now == 1:
            relay_event("BMS", "%d %s" % (inst, BATTERIES[inst]), ok=True)
    elif path.startswith(vb + "Devices/") and path.endswith("/Diagnostics/UBatTerminal") and val is not None:
        n = int(path.split("/")[3])
        low = 1 if val < DC_LOST_V else 0
        prev, state["dc"][n] = state["dc"].get(n), low
        if prev == 0 and low:
            relay_event("DC", "unit %d battery voltage lost (%.2f V)" % (n, val))
        elif prev == 1 and not low:
            relay_event("DC", "unit %d battery voltage (%.2f V)" % (n, val), ok=True)



# ---- TEMP: house broker ---------------------------------------------------------------------------

def walk(obj, path, out):
    """every numeric leaf below a key containing "temp" (lists too: temps_c[1][2])"""
    if isinstance(obj, dict):
        for k, v in obj.items():
            walk(v, "%s.%s" % (path, k), out)
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:64]):
            walk(v, "%s[%d]" % (path, i), out)
    elif isinstance(obj, (int, float)) and not isinstance(obj, bool):
        if TEMP_KEY.search(path.split("/", 1)[-1]):
            out.append((path, float(obj)))


def on_house_message(client, userdata, msg):
    if SKIP_TOPIC.match(msg.topic):
        return
    try:
        payload = msg.payload.decode("utf-8", "replace").strip()
        data = json.loads(payload)
    except ValueError:
        return
    found = []
    if isinstance(data, (dict, list)):
        walk(data, msg.topic, found)
    elif isinstance(data, (int, float)) and not isinstance(data, bool) and TEMP_KEY.search(msg.topic):
        found.append((msg.topic, float(data)))          # plain number topics like aussen/temp
    for name, value in found:
        if value == value:
            reading(name, value)


VENUS_TEMP = re.compile(r"^(battery/\d+/(Dc/0/Temperature|System/MaxCellTemperature)|temperature/\d+/Temperature)$")


def on_venus_message(client, userdata, msg):
    path = msg.topic.split("/", 2)[2]
    try:
        v = json.loads(msg.payload).get("value") if msg.payload else None
    except (ValueError, AttributeError):
        return
    if VENUS_TEMP.match(path):
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            reading("venus:" + path, float(v))
        return
    t = time.time()                                     # RELAIS: vebus / battery values
    with rlock:
        values[path] = v
        ring.append((t, path, v))
        while ring and ring[0][0] < t - 3 * RING_S:
            ring.popleft()
    relay_check(path, v)


def connect(host, on_message, on_connect):
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    c.on_message, c.on_connect = on_message, on_connect
    c.reconnect_delay_set(2, 60)
    c.connect_async(host, 1883, 60)
    c.loop_start()
    return c


def venus_on_connect(c, userdata, flags, rc, props):
    # subscribe before the keepalive: the full republish after it carries the current values
    for t in ("battery/+/Dc/0/Temperature", "battery/+/System/MaxCellTemperature", "temperature/+/Temperature"):
        c.subscribe("N/%s/%s" % (VENUS_PORTAL, t))
    c.subscribe("N/%s/vebus/%d/#" % (VENUS_PORTAL, VEBUS))          # RELAIS
    for inst in BATTERIES:
        c.subscribe("N/%s/battery/%d/#" % (VENUS_PORTAL, inst))
    c.publish("R/%s/keepalive" % VENUS_PORTAL, "")
    log("Venus broker connected (%s)" % rc)


def house_on_connect(c, userdata, flags, rc, props):
    c.subscribe("#")
    log("house broker connected (%s)" % rc)


def main():
    log("alarm_mail: TEMP threshold %.0f °C (batteries %.0f °C), confirm %d, release %.0f / %.0f °C; RELAIS GRID / OFF "
        "/ BMS / DC, diagnosis after %d s; mail %s%s" % (THRESHOLD_C, BAT_THRESHOLD_C, CONFIRM, RELEASE_C, BAT_RELEASE_C,
                                                        EVENT_WAIT_S, MAIL_TO, " (dry run)" if DRY else ""))
    connect(HOUSE_BROKER, on_house_message, house_on_connect)
    venus = connect(VENUS_BROKER, on_venus_message, venus_on_connect)
    if "--test" in sys.argv:
        time.sleep(20)
        with lock:
            n = len(sensors)
        rows = "\n".join("  %-55s %14s" % (p, v(p)) for p in SNAP)
        ok = send_mail("[TEST] alarm_mail Lenggries: TEMP %d sensors, RELAIS %d values watched" % (n, len(values)),
                       "Test mail of alarm_mail.py on the Pi .119, two categories.\n\n"
                       "TEMP: alarm when a battery sensor reads above %.0f °C or any other sensor above %.0f °C in %d "
                       "consecutive readings.\nHottest sensors now:\n%s\n\n"
                       "RELAIS: alarm when the MultiPlus grid relay opens (GRID), VE.Bus leaves Mode 3 (OFF), a BMS "
                       "disappears (BMS) or a unit loses its battery voltage (DC), with the cause from the data.\n"
                       "Diagnosis now:\n%s\n\n%s\n\nValues now:\n%s\n\nVenus logs:\n%s\n"
                       % (BAT_THRESHOLD_C, THRESHOLD_C, CONFIRM, hottest(25), "\n".join("  - " + x for x in cause(time.time())),
                          batmonitor_rule(), rows, venus_logs(4)))
        return 0 if ok else 1
    last_summary = time.time()
    while True:
        time.sleep(VENUS_KEEPALIVE_S)
        venus.publish("R/%s/keepalive" % VENUS_PORTAL, '{"keepalive-options": ["suppress-republish"]}')
        if time.time() - last_summary >= 3600:          # hourly log line: how many sensors, the hottest one
            last_summary = time.time()
            with lock:
                n = len(sensors)
                top = max(((v["value"], k) for k, v in sensors.items()), default=(0, "-"))
            log("%d sensors, hottest %.1f °C %s" % (n, top[0], top[1]))


if __name__ == "__main__":
    sys.exit(main())
