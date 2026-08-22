#!/usr/bin/env python3
"""Schickt MQTT-Nachrichten ueber den Dragino LA66 per LoRaWAN an 192.168.5.23.

Das ist der Notfallweg: faellt das Internet aus, laeuft die Strecke trotzdem,
weil sie weder Internet noch WireGuard braucht —

    LA66 (USB, hier)  ->  Gateway Lenggries  ->  192.168.5.23 (ChirpStack)
                                              ->  mosquitto  ->  Abonnent

Auf dem dell nimmt `dragino_rx.py` die Teile entgegen, setzt sie zusammen und
veroeffentlicht sie unter dem angegebenen Topic.

    dragino.py wetter/berg "Schneefall, Abstieg verzoegert"
    dragino.py --dr 0 notruf "Hilfe noetig am Brauneck"     # groesste Reichweite
    dragino.py --verify status/x "test"                     # Ankunft nachpruefen

Zwei Dinge, die man beim Funken nicht vergessen darf:

* **Nutzlast ist winzig.** EU868 erlaubt bei DR0 (SF12) nur 51 Byte je Rahmen,
  bei DR3 (SF9) 115. Laengere Nachrichten werden zerlegt und einzeln gesendet.
* **Sendezeit ist begrenzt.** 1 % Duty Cycle heisst: nach einem SF12-Rahmen von
  2,5 s Dauer muss das Band rund 4 Minuten ruhen. Der LA66 hat seine eigene
  Pruefung zwar abgeschaltet (`AT+DCS=0`), rechtlich gilt sie trotzdem —
  deshalb wird hier zwischen den Teilen bewusst gewartet.
"""
import argparse
import random
import sys
import time

import serial

PORT = "/dev/ttyUSB0"
BAUD = 9600
FPORT = 20                      # vereinbart mit dragino_rx.py auf dem dell
DELL = "192.168.5.23"

# Maximale Nutzlast je Datenrate in EU868, Wert N aus den LoRaWAN Regional
# Parameters (gilt, solange keine MAC-Kommandos in FOpts mitreisen).
MAX_PAYLOAD = {0: 51, 1: 51, 2: 51, 3: 115, 4: 242, 5: 242}
# Grobe Sendezeit eines vollen Rahmens, als Basis fuer die Pause bei 1 % Duty Cycle.
AIRTIME_S = {0: 2.5, 1: 1.4, 2: 0.7, 3: 0.7, 4: 0.7, 5: 0.4}


def at(ser, cmd, wait=1.0, collect=None):
    """Ein AT-Kommando abschicken und die Antwort einsammeln.

    `collect` ist ein Wort, auf das gewartet wird (z.B. txDone) — dann wird
    bis dahin gelesen statt nur eine feste Zeit.
    """
    ser.reset_input_buffer()
    ser.write((cmd + "\r\n").encode())
    ser.flush()
    if collect is None:
        time.sleep(wait)
        return ser.read(32768).decode("utf-8", "replace")
    buf = b""
    t0 = time.time()
    while time.time() - t0 < wait:
        buf += ser.read(4096)
        if collect.encode() in buf and time.time() - t0 > 3:
            break
    return buf.decode("utf-8", "replace")


def open_port(port, baud=BAUD):
    """baud ist nur fuer Fremdgeraete noetig: der LA66 laeuft mit 9600, der
    Pico-Knoten (gleicher AT-Dialekt, siehe la66chat.py) mit 115200."""
    ser = serial.Serial()
    ser.port, ser.baudrate, ser.timeout = port, baud, 0.2
    # Der CP2102 wuerde beim Oeffnen sonst DTR/RTS ziehen (LA66 = /dev/ttyUSB*).
    # Bei nativem USB-CDC (/dev/ttyACM*, z.B. der Pico) darf DTR nicht vorab
    # abgeschaltet werden: der Treiber wartet dann auf den Carrier und open()
    # blockiert endlos.
    if "ACM" not in port:
        ser.dtr = False
        ser.rts = False
    ser.open()
    time.sleep(0.5)
    return ser


def chunks(data, size):
    return [data[i:i + size] for i in range(0, len(data), size)]


def main():
    ap = argparse.ArgumentParser(
        description="MQTT-Nachricht ueber den LA66 an %s funken" % DELL)
    ap.add_argument("topic", help="MQTT-Topic, z.B. notruf oder wetter/berg")
    ap.add_argument("message", help="Nachrichtentext")
    ap.add_argument("--dr", type=int, default=3, choices=sorted(MAX_PAYLOAD),
                    help="Datenrate: 0=SF12 groesste Reichweite, 5=SF7 schnellste "
                         "Uebertragung (Vorgabe 3 = SF9)")
    ap.add_argument("--port", default=PORT, help=f"serieller Port (Vorgabe {PORT})")
    ap.add_argument("--confirm", action="store_true",
                    help="bestaetigte Uplinks anfordern (kostet Downlink-Kapazitaet)")
    ap.add_argument("--verify", action="store_true",
                    help="am Ende per MQTT auf dem dell nachsehen, ob es ankam "
                         "(braucht IP-Verbindung, im Krisenfall also nicht)")
    args = ap.parse_args()

    body = (args.topic + "\n" + args.message).encode("utf-8")
    # 2 Byte Kopf: Nachrichtenkennung und laufende Nummer mit Endemarke.
    per_frame = MAX_PAYLOAD[args.dr] - 2
    parts = chunks(body, per_frame)
    if len(parts) > 128:
        sys.exit(f"Nachricht zu lang: {len(parts)} Teile, hoechstens 128 moeglich")
    msg_id = random.randint(0, 255)

    print(f"{len(body)} Byte in {len(parts)} Rahmen bei DR{args.dr} "
          f"({MAX_PAYLOAD[args.dr]} Byte/Rahmen), Kennung {msg_id:02X}")

    ser = open_port(args.port)
    try:
        r = at(ser, f"AT+DR={args.dr}")
        if "OK" not in r:
            print(f"Warnung: Datenrate nicht bestaetigt — {r.strip()!r}")

        for i, part in enumerate(parts):
            last = 0x80 if i == len(parts) - 1 else 0x00
            frame = bytes([msg_id, last | i]) + part
            hexs = frame.hex().upper()
            cmd = f"AT+SENDB={1 if args.confirm else 0},{FPORT},{len(frame)},{hexs}"
            out = at(ser, cmd, wait=60, collect="txDone")
            ok = "txDone" in out
            print(f"  Rahmen {i + 1}/{len(parts)}: {len(frame)} Byte "
                  f"{'gesendet' if ok else 'UNBESTAETIGT'}")
            if not ok:
                print("   ", out.strip()[:200])
            if i < len(parts) - 1:
                # 1 % Duty Cycle: Sendezeit x 100 als Ruhezeit.
                pause = AIRTIME_S[args.dr] * 100
                print(f"    warte {pause:.0f}s (Duty Cycle)")
                time.sleep(pause)
    finally:
        ser.close()

    if args.verify:
        verify(args.topic, args.message)


def verify(topic, message, timeout=30):
    """Nachsehen, ob die Nachricht auf dem Broker des dell angekommen ist.

    Nur ein Testhilfsmittel — es braucht eine IP-Verbindung zum dell und ist
    damit genau in der Lage nutzlos, fuer die dieses Werkzeug gebaut ist.
    """
    try:
        import paho.mqtt.client as mqtt
    except ImportError:
        print("verify uebersprungen: paho-mqtt ist hier nicht installiert")
        return
    got = []

    def on_connect(c, u, f, rc, properties=None):
        c.subscribe(topic)

    def on_message(c, u, m):
        got.append(m.payload.decode("utf-8", "replace"))

    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    c.on_connect, c.on_message = on_connect, on_message
    try:
        c.connect(DELL, 1883, 30)
    except OSError as e:
        print(f"verify: {DELL} nicht erreichbar ({e})")
        return
    c.loop_start()
    t0 = time.time()
    while time.time() - t0 < timeout and not got:
        time.sleep(0.5)
    c.loop_stop()
    if got:
        print(f"verify: angekommen auf '{topic}': {got[-1]!r}")
    else:
        print(f"verify: binnen {timeout}s nichts auf '{topic}' gesehen")


if __name__ == "__main__":
    main()
