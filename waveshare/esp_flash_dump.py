#!/usr/bin/env python3
"""Back up an ESP32 over the network through flash_dump.h (ESPHome OTA can only write).

Default: bootloader, partition table and every partition, one file each, plus manifest.json.
--full reads the whole flash instead. Every block is checked against the SHA-256 the ESP computes.

  ./esp_flash_dump.py 192.168.178.112 --yaml waveshare_do4.yaml --out /home/gh/esp_backups/waveshare_do4
"""
import argparse
import hashlib
import json
import pathlib
import re
import socket
import time

PORT = 8267
BOOTLOADER_ADDR = 0x0
PARTITION_TABLE_ADDR = 0x8000
PARTITION_TABLE_SIZE = 0x1000
TIMEOUT_S = 15


class Client:
    def __init__(self, host, password):
        self.sock = socket.create_connection((host, PORT), TIMEOUT_S)
        self.sock.settimeout(TIMEOUT_S)
        self.file = self.sock.makefile("rb")
        hello = self.line().split()
        if len(hello) != 2 or hello[0] != "FDUMP1":
            raise RuntimeError(f"unexpected greeting: {hello}")
        digest = hashlib.sha256((hello[1] + password).encode()).hexdigest()
        self.send(f"AUTH {digest}")
        if self.line() != "OK":
            raise RuntimeError("authentication failed")

    def send(self, text):
        self.sock.sendall(text.encode() + b"\n")

    def line(self):
        raw = self.file.readline()
        if not raw.endswith(b"\n"):
            raise RuntimeError("connection closed (wrong password?)")
        return raw.decode().strip()

    def info(self):
        self.send("INFO")
        return json.loads(self.line())

    def read(self, addr, size):
        self.send(f"READ {addr} {size}")
        head = self.line()
        if head != f"OK {size}":
            raise RuntimeError(f"READ 0x{addr:x}+{size}: {head}")
        data = self.file.read(size)
        if len(data) != size:
            raise RuntimeError(f"READ 0x{addr:x}: got {len(data)} of {size} bytes")
        tail = self.line()
        local = hashlib.sha256(data).hexdigest()
        if tail != f"SHA {local}":
            raise RuntimeError(f"READ 0x{addr:x}: checksum mismatch ({tail} / {local})")
        return data, local

    def close(self):
        try:
            self.send("QUIT")
        except OSError:
            pass
        self.sock.close()


def ota_password(yaml_path):
    match = re.search(r"^\s+ota_password:\s*(.+?)\s*$", pathlib.Path(yaml_path).read_text(), re.M)
    if match is None:
        raise SystemExit(f"no ota_password in {yaml_path}")
    value = match.group(1).strip("\"'")
    if value.startswith("!secret"):
        secrets = pathlib.Path(yaml_path).with_name("secrets.yaml").read_text()
        match = re.search(r"^ota_password:\s*(.+?)\s*$", secrets, re.M)
        if match is None:
            raise SystemExit("no ota_password in secrets.yaml")
        value = match.group(1).strip("\"'")
    return value


def regions(info, full):
    if full:
        return [("full", 0, info["flash_size"])]
    found = [("bootloader", BOOTLOADER_ADDR, PARTITION_TABLE_ADDR),
             ("partition_table", PARTITION_TABLE_ADDR, PARTITION_TABLE_SIZE)]
    found += [(p["label"], p["addr"], p["size"]) for p in info["partitions"]]
    return found


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("host")
    ap.add_argument("--yaml", default=str(pathlib.Path(__file__).with_name("waveshare_do4.yaml")))
    ap.add_argument("--out", required=True)
    ap.add_argument("--full", action="store_true", help="read the whole flash")
    args = ap.parse_args()

    client = Client(args.host, ota_password(args.yaml))
    info = client.info()
    print(f"flash {info['flash_size']} bytes, physical {info['physical_size']}, running {info['running']}")
    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    manifest = {"host": args.host, "time": time.strftime("%Y-%m-%dT%H:%M:%S"), "info": info, "files": []}
    total = 0
    start = time.monotonic()
    for label, addr, size in regions(info, args.full):
        data, digest = client.read(addr, size)
        name = f"{addr:08x}_{label}.bin"
        (out / name).write_bytes(data)
        manifest["files"].append({"file": name, "addr": addr, "size": size, "sha256": digest})
        total += size
        print(f"{name}: {size} bytes")
    client.close()
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n")
    elapsed = time.monotonic() - start
    print(f"done: {total} bytes in {elapsed:.1f} s, {total / 1024 / elapsed:.0f} kB/s, {out}")


if __name__ == "__main__":
    main()
