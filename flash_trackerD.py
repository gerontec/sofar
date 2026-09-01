#!/usr/bin/env python3
"""Den TrackerD-Fork bauen und nach app1 flashen.

Fasst zusammen, was am 31.08.2026 von Hand ging und jedes Mal gleich lief:
Quellen auf dem dell frisch auslegen, die Patches aus `devices/trackerd_stock148/`
anwenden, bauen, das Binary holen und mit `switch_app.py` nach app1 schreiben.

    ./flash_trackerD.py               # bauen, holen, flashen
    ./flash_trackerD.py --nur-flash   # letztes Binary vom dell holen und flashen
    ./flash_trackerD.py --bin x.bin   # eigenes Image flashen
    ./flash_trackerD.py --nur-bauen   # nur bauen, nichts anfassen
    ./flash_trackerD.py --app0        # zurueck auf die Werksfirmware, ohne Flash
    ./flash_trackerD.py --kein-fdr    # flashen, aber Puffer/Config nicht zuruecksetzen
    ./flash_trackerD.py --nur-fdr     # nur zuruecksetzen, nicht flashen

**Nie `pio run -t upload`.** Das schriebe nach app0 und wuerde die
Werksfirmware ueberschreiben -- die ist der Rueckfallweg, wenn ein Eigenbau
nicht laeuft. `switch_app.py` fasst ausschliesslich app1 (0x1F0000) und otadata
(0xE000) an; Bootloader, Partitionstabelle, app0 und vor allem `nvs` mit
DevEUI und AppKey bleiben unberuehrt.

Zwei Dinge passieren nach jedem Flash, beide sind normal:

* **Ampel-Zyklus Blau-Rot-Gruen.** Der Versionsstring wechselt (app0 meldet
  v1.4.8, der Eigenbau v1.4.6), die Firmware erkennt das als Versionswechsel
  und ruft `DATA_CLEAR()`. Alle DATA-Einstellungen stehen danach auf Werk --
  TDC, MTDC, FTIME. Sport und Datalog nicht, die kommen aus `fix_defaults_on`
  und stehen im Quelltext.
* **Ein neuer OTAA-Join.** Der RTC-Speicher ist weg, `RTC_LMIC.seqnoUp = 0`.

Deshalb schickt das Skript nach dem Flashen ein **`AT+FDR`** hinterher. Das
erzwingt `DATA_CLEAR()` und damit den `FDR_flag == 0`-Zweig, in dem
`fix_defaults_on.py` sitzt: Sport und Datalog an, und die drei Ringzeiger des
Spurpuffers auf 0.

Das ist kein Luxus, sondern noetig. Flasht man denselben Versionsstring erneut
(v1.4.6 auf v1.4.6), erkennt die Firmware **keinen** Versionswechsel, ruft kein
`DATA_CLEAR()` -- und der Patchblock laeuft nie. Der Spurpuffer behielte dann
den alten Inhalt. Das ist teuer: `loggpsdata_send` wird auf 1 gesetzt, sobald
der Ring 14 Eintraege traegt (`TrackerD.ino:288`), und dieses Flag steht in der
Bedingung der GPS-Suche (`TrackerD.ino:1286`). Ein verklemmter Ring schaltet
GPS also komplett ab -- gemessen am 31.08.2026: 80 Rahmen mit `Latitude=0` in
Folge, waehrend derselbe Stand mit leerem Ring 340 Rahmen mit 100 % Fixquote
lieferte.

`AT+FDR` raeumt nebenbei auch einen laufenden Alarm weg. Der wuerde den Flash
sonst ueberleben, weil `sys.alarm` im EEPROM liegt und beim Booten
zurueckgelesen wird.
"""
import argparse
import datetime
import os
import subprocess
import sys

HOST      = "gh@192.168.5.23"
BAUDIR    = "~/trackerd_build"
FORK      = "forkstock"
BINARY    = ".pio/build/trackerdstock/firmware.bin"
PIO       = "~/.platformio/penv/bin/pio"
SWITCH    = "/home/gh/python/lora/trackerd/switch_app.py"
PORT      = "/dev/ttyACM0"
# Neben die anderen Images, nicht nach /tmp: das ueberlebt einen Neustart
# und laesst sich spaeter noch flashen, wenn der dell mal nicht da ist.
# Jedes Image traegt Version und Deployzeit im Namen, damit spaeter feststellbar
# bleibt, was auf dem Geraet liegt -- die Firmware selbst kann das nicht sagen:
# `AT+VER` meldet nur den Quelltext-String (v1.4.6, bei Dragino seit v1.4.6
# nicht mehr nachgezogen) und ist damit fuer alle unsere Baustaende gleich.
IMGDIR    = "/home/gh/python/lora/trackerd"
ZIEL      = IMGDIR + "/firmware_stock148.bin"        # Zeiger auf den letzten Bau
LOG       = IMGDIR + "/deploy.log"

# Reihenfolge zaehlt: fix_holds zuerst, sonst patchen die anderen in eine
# Datei, die spaeter ohnehin neu ausgelegt wird.
PATCHES_SRC = ["fix_holds_stock.py", "fix_defaults_on.py", "fix_alarm_gpsfix_stop.py"]
PATCH_LIB   = "fix_aes_len.py"


def lauf(cmd, **kw):
    print("$", cmd if isinstance(cmd, str) else " ".join(cmd))
    return subprocess.run(cmd, shell=isinstance(cmd, str), check=True, **kw)


def bauen():
    """Quellen frisch auslegen, patchen, bauen -- alles auf dem dell."""
    schritte = [
        f"cd {BAUDIR}",
        f"rm -rf {FORK}",
        f"mkdir -p {FORK}/src {FORK}/lib",
        f"cp -a repo148/Example/LoRaWAN/examples/TrackerD/. {FORK}/src/",
        f"cp -a repo148/Library/arduino-lmic/arduino-lmic {FORK}/lib/arduino-lmic",
        f"cp platformio.ini partitions_trackerd.csv {FORK}/",
        f"cp -a variants_stock {FORK}/variants",
    ]
    schritte += [f"python3 {p} {FORK}/src" for p in PATCHES_SRC]
    schritte.append(f"python3 {PATCH_LIB} {FORK}/lib/arduino-lmic")
    schritte.append(f"cd {FORK} && {PIO} run")
    lauf(["ssh", HOST, " && ".join(schritte)])


def version_aus_quelle():
    """Den Versionsstring aus common.h holen, den die Firmware selbst meldet."""
    r = subprocess.run(["ssh", HOST,
                        f"grep -m1 'define Pro_version' {BAUDIR}/{FORK}/src/common.h"],
                       capture_output=True, text=True)
    teile = r.stdout.strip().split('"')
    return teile[1] if len(teile) > 1 else "unbekannt"


def holen():
    """Image holen, mit Version und Deployzeit ablegen, ZIEL darauf zeigen lassen."""
    ver = version_aus_quelle()
    stempel = datetime.datetime.now().strftime("%Y%m%d-%H%M")
    datei = f"{IMGDIR}/firmware_stock148_{ver}_{stempel}.bin"
    lauf(["scp", "-q", f"{HOST}:{BAUDIR.replace('~', '/home/gh')}/{FORK}/{BINARY}", datei])
    # ZIEL ist die Kopie, die --nur-flash und --bin ohne Argument finden.
    lauf(["cp", "-f", datei, ZIEL])
    md5 = subprocess.run(["md5sum", datei], capture_output=True, text=True).stdout.split()[0]
    with open(LOG, "a") as f:
        f.write(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S}  {ver}  {md5}  "
                f"{os.path.getsize(datei)}  {os.path.basename(datei)}\n")
    print(f"Binary: {datei} ({os.path.getsize(datei)} Byte, md5 {md5[:8]})")
    return datei


FEHLER = ("ERROR", "AT_PARAM_ERROR", "AT_BUSY_ERROR",
          "AT_TEST_PARAM_OVERFLOW", "AT_RX_ERROR")


def fdr(versuche=6):
    """AT+FDR: DATA_CLEAR erzwingen, damit der Patchblock laeuft.

**Auf ein "OK" zu warten waere falsch.** `at_fdr_run()` in `at.cpp:34` lautet

        sys.DATA_CLEAR();
        ESP.restart();
        return AT_OK;          // wird nie erreicht

Der Neustart kommt vor dem Rueckgabewert, also druckt `loop()` nichts mehr.
Der einzige gueltige Beweis ist das Boot-Banner `TrackerD ,v...`. Danach liest
`FDR_flag` aus dem geloeschten EEPROM als 0, und der Zweig in
`TrackerD.ino:1130` setzt die Vorgaben aus `fix_defaults_on.py`.

Wiederholt wird nur bei **Schweigen**, nicht blind. Eine Fehlermarke aus
`at.h` beendet den Versuch sofort:

      Boot-Banner     -> fertig, DATA_CLEAR ist gelaufen
      AT_BUSY_ERROR   -> beschaeftigt, gleich nochmal (das lohnt)
      andere Fehler   -> abbrechen, Wiederholen aendert daran nichts
      nichts          -> loop() laeuft noch nicht oder das Geraet schlaeft,
                         nochmal schicken -- der Normalfall vor dem Erfolg

    DTR bleibt unten -- das Signal haengt am Alarmknopf und wuerde ihn
    gedrueckt halten.
    """
    import serial, time
    print("$ AT+FDR  (DATA_CLEAR: Spurpuffer leeren, Vorgaben setzen)")
    p = serial.Serial()
    p.port, p.baudrate, p.timeout = PORT, 115200, 0.2
    p.dtr = p.rts = False
    p.open()
    try:
        for n in range(1, versuche + 1):
            p.reset_input_buffer()
            p.write(b"AT+FDR\r\n")
            antwort, ende = None, time.time() + 6
            while time.time() < ende:
                zeile = p.readline()
                if not zeile:
                    continue
                t = zeile.decode("utf-8", "replace").strip()
                if "TrackerD ," in t:
                    print(f"    {t}  -- neu gestartet, DATA_CLEAR gelaufen")
                    return True
                if t in FEHLER:
                    antwort = t
                    break
            if antwort in FEHLER and antwort != "AT_BUSY_ERROR":
                print(f"    {antwort} -- Wiederholen hilft hier nicht.", file=sys.stderr)
                return False
            print(f"    Versuch {n}/{versuche}: "
                  + (antwort if antwort else "kein Neustart (loop() laeuft noch nicht"
                                             " oder Geraet schlaeft)"))
    finally:
        p.close()
    print("    FEHLER: kein Neustart gesehen, AT+FDR kam nicht durch.\n"
          "    Erneut versuchen mit: ./flash_trackerD.py --nur-fdr", file=sys.stderr)
    return False


def geraet_da():
    if os.path.exists(PORT):
        return True
    print(f"FEHLER: {PORT} fehlt -- TrackerD nicht am USB.", file=sys.stderr)
    print("Kabel pruefen; das Geraet meldet sich als ttyACM ab, wenn es"
          " abgezogen wird.", file=sys.stderr)
    return False


def main():
    p = argparse.ArgumentParser(description="TrackerD-Fork bauen und nach app1 flashen.",
                                formatter_class=argparse.RawDescriptionHelpFormatter,
                                epilog=__doc__)
    p.add_argument("--nur-bauen", action="store_true", help="nur bauen, nicht flashen")
    p.add_argument("--nur-flash", action="store_true", help="nicht bauen, letztes Binary vom dell holen")
    p.add_argument("--bin", help="eigenes Image flashen, statt zu bauen oder zu holen")
    p.add_argument("--app0", action="store_true",
                   help="nur die Bootpartition auf app0 (Werksfirmware) stellen")
    p.add_argument("--kein-fdr", action="store_true",
                   help="nach dem Flashen KEIN AT+FDR schicken (Puffer bleibt, wie er ist)")
    p.add_argument("--nur-fdr", action="store_true",
                   help="nur AT+FDR schicken, nicht bauen und nicht flashen")
    a = p.parse_args()

    if a.nur_fdr:
        if not geraet_da():
            sys.exit(1)
        sys.exit(0 if fdr() else 2)

    if a.app0:
        if not geraet_da():
            sys.exit(1)
        lauf([SWITCH, "app0"])
        return

    if a.bin:
        image = a.bin
    else:
        if not a.nur_flash:
            bauen()
        if a.nur_bauen:
            print("gebaut, nicht geflasht.")
            return
        image = holen()

    if not geraet_da():
        sys.exit(1)
    lauf([SWITCH, "flash", "--bin", image])
    with open(LOG, "a") as f:
        f.write(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S}  GEFLASHT nach app1: "
                f"{os.path.basename(image)}\n")
    if a.kein_fdr:
        print("Geflasht, ohne AT+FDR. Achtung: beim gleichen Versionsstring gibt es"
              " kein DATA_CLEAR, der Spurpuffer behaelt seinen Inhalt.")
        return
    if not fdr():
        print("Geflasht, aber NICHT zurueckgesetzt -- der Spurpuffer kann noch"
              " Altlasten tragen.", file=sys.stderr)
        sys.exit(2)
    print("Geflasht und zurueckgesetzt: Spurpuffer leer, Sport und Datalog an.")


if __name__ == "__main__":
    main()
