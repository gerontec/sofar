#!/usr/bin/env python3
"""Host-side bridge: DO4 of the Waveshare board over USB, without WiFi, for the Venus container.

The container must not see the tty itself: Venus serial-starter probes every tty and would write into it
(see /usr/local/sbin/venus-start). So this runs on the Pi host and listens on a Unix socket in /data, which the
container shares. The serial port is opened per command only, so esptool can always take it for recovery.

Request: one line "do4 1", "do4 0", "do4" or "ping". Reply: one line "OK <text>" or "ERR <text>".
"""
import os
import socket
import time

import serial

PORT = "/dev/serial/by-id/usb-Espressif_USB_JTAG_serial_debug_unit_30:ED:A0:D8:FD:44-if00"
SOCK = "/data/do4_usb/do4.sock"
PREFIX = "#K1# "
ALLOWED = {"do4 1", "do4 0", "do4", "ping"}
REPLY_TIMEOUT_S = 1.5
CLIENT_TIMEOUT_S = 3.0


def open_port():
    port = serial.Serial()
    port.port = PORT
    port.baudrate = 115200
    port.timeout = 0.1
    port.exclusive = True          # refuse while esptool holds the port
    # pyserial applies DTR first, then RTS. DTR low with RTS high resets the ESP32-S3 USB-JTAG,
    # so keep DTR high and bring RTS low.
    port.dtr = True
    port.rts = False
    port.open()
    return port


def ask_esp(command):
    try:
        port = open_port()
    except (serial.SerialException, OSError) as exc:
        return f"ERR port: {exc}"
    try:
        port.reset_input_buffer()
        port.write(command.encode() + b"\n")
        port.flush()
        answer = ""
        deadline = time.monotonic() + REPLY_TIMEOUT_S
        buffer = b""
        while time.monotonic() < deadline:
            buffer += port.read(256)
            while b"\n" in buffer:
                raw, buffer = buffer.split(b"\n", 1)
                text = raw.decode(errors="replace").strip()
                if not text.startswith(PREFIX):
                    continue               # log output on the same line
                body = text[len(PREFIX):]
                if body == "+OK":
                    return f"OK {answer}".strip()
                if body.startswith("-ERR"):
                    return f"ERR {body[5:]}"
                answer = body
        return "ERR no reply from ESP"
    except (serial.SerialException, OSError) as exc:
        return f"ERR port: {exc}"
    finally:
        port.close()


def serve():
    os.makedirs(os.path.dirname(SOCK), exist_ok=True)
    if os.path.exists(SOCK):
        os.unlink(SOCK)
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(SOCK)
    os.chmod(SOCK, 0o660)
    server.listen(4)
    while True:
        client, _ = server.accept()
        with client:
            client.settimeout(CLIENT_TIMEOUT_S)
            try:
                command = client.makefile("r").readline().strip()
                reply = ask_esp(command) if command in ALLOWED else "ERR command"
                client.sendall(reply.encode() + b"\n")
            except OSError:
                continue


if __name__ == "__main__":
    serve()
