#!/usr/bin/env python3
"""Venus side of DO4 over USB: talks to do4_usb_bridge.py on the Pi host through /data.

  do4_usb.py 1 | 0     switch DO4 (the ESP turns it off by itself after 10 s)
  do4_usb.py           read DO4
  do4_usb.py ping      firmware and uptime of the ESP
Exit code 0 only if the ESP confirmed the command.
"""
import socket
import sys

SOCK = "/data/do4_usb/do4.sock"
TIMEOUT_S = 4.0


def request(command):
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.settimeout(TIMEOUT_S)
    client.connect(SOCK)
    client.sendall(command.encode() + b"\n")
    reply = client.makefile("r").readline().strip()
    client.close()
    return reply


def main():
    arg = sys.argv[1] if len(sys.argv) > 1 else ""
    command = "ping" if arg == "ping" else ("do4 " + arg).strip()
    try:
        reply = request(command)
    except OSError as exc:
        print("ERR bridge: %s" % exc)
        return 1
    print(reply)
    return 0 if reply.startswith("OK") else 1


if __name__ == "__main__":
    sys.exit(main())
