#!/usr/bin/env python3
"""
temp_alarm.py - emergency mail when any single temperature sensor in the house reads above THRESHOLD_C (98 °C).

Runs as a systemd service on the Pi .119 (Venus host): from there it reaches the Venus MQTT broker (local only,
the BMS of Stack1 MUST) and the house broker .218 (everything else).
  .218        every numeric field whose name contains "temp" in any topic: Pytes ebox/pwr (temp_max_c, temps_c per
              pack), R290 (temp_exhaust, temp_outlet, ...), outdoor, rooms ... new sensors are picked up by
              themselves. Forecasts (wetter/...) are no sensors and skipped.
  Venus       battery/<n>/Dc/0/Temperature and System/MaxCellTemperature (MUST BMS), temperature/<n>/Temperature
Against false alarms (history: single Modbus reads of 16896 / 59136 / 65506 "°C"): only values in PLAUSIBLE_C count,
and a sensor has to be above the threshold in CONFIRM consecutive readings. Then one mail per sensor, a reminder
every REMIND_S while it stays above, and an all-clear once it is below RELEASE_C.

usage: temp_alarm.py            run (service)
       temp_alarm.py --test     one [TEST] mail with the current hottest readings after 20 s, then exit
       temp_alarm.py --dry-run  log instead of mailing
"""
import json
import re
import smtplib
import sys
import threading
import time
from email.message import EmailMessage
from email.utils import formatdate

import paho.mqtt.client as mqtt

THRESHOLD_C = 98.0
RELEASE_C = 90.0
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


def reading(name, value):
    """one plausible reading of one sensor: confirm, alarm, remind, all-clear"""
    if not (PLAUSIBLE_C[0] <= value <= PLAUSIBLE_C[1]):
        return
    now = time.time()
    mail = None
    with lock:
        s = sensors.setdefault(name, {"value": value, "ts": now, "over": 0, "alarm": False, "mailed": 0.0})
        s["value"], s["ts"] = value, now
        if value > THRESHOLD_C:
            s["over"] += 1
            if s["over"] >= CONFIRM and (not s["alarm"] or now - s["mailed"] >= REMIND_S):
                mail = ("ALARM" if not s["alarm"] else "STILL HOT", value)
                s["alarm"], s["mailed"] = True, now
        else:
            s["over"] = 0
            if s["alarm"] and value < RELEASE_C:
                s["alarm"] = False
                mail = ("all clear", value)
    if mail:
        kind, v = mail
        subject = "%s Lenggries: %s %.1f °C (threshold %.0f °C)" % (kind, name, v, THRESHOLD_C)
        body = ("%s\n\nSensor:  %s\nValue:   %.1f °C\nTime:    %s\nRule:    above %.0f °C in %d consecutive readings; "
                "reminder every %d min; all clear below %.0f °C\n\nHottest sensors now:\n%s\n"
                % (subject, name, v, time.strftime("%Y-%m-%d %H:%M:%S"), THRESHOLD_C, CONFIRM, REMIND_S // 60,
                   RELEASE_C, hottest()))
        log(subject)
        send_mail(subject, body)


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


def on_venus_message(client, userdata, msg):
    try:
        v = json.loads(msg.payload).get("value")
    except (ValueError, AttributeError):
        return
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        reading("venus:" + msg.topic.split("/", 2)[2], float(v))


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
    c.publish("R/%s/keepalive" % VENUS_PORTAL, "")
    log("Venus broker connected (%s)" % rc)


def house_on_connect(c, userdata, flags, rc, props):
    c.subscribe("#")
    log("house broker connected (%s)" % rc)


def main():
    log("temp_alarm: threshold %.0f °C, confirm %d, release %.0f °C, mail %s%s"
        % (THRESHOLD_C, CONFIRM, RELEASE_C, MAIL_TO, " (dry run)" if DRY else ""))
    connect(HOUSE_BROKER, on_house_message, house_on_connect)
    venus = connect(VENUS_BROKER, on_venus_message, venus_on_connect)
    if "--test" in sys.argv:
        time.sleep(20)
        with lock:
            n = len(sensors)
        ok = send_mail("[TEST] temp_alarm Lenggries: %d sensors watched" % n,
                       "Test mail of temp_alarm.py on the Pi .119. An alarm goes out when a sensor reads above %.0f °C "
                       "in %d consecutive readings.\n\nHottest sensors now:\n%s\n" % (THRESHOLD_C, CONFIRM, hottest(25)))
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
