# usbmux — mehrere Kanäle auf der einen USB-Leitung des ESP32-S3

Stand 10.08.2026, Testgerät `rs485defect-wav-esp32s3` (MAC `50:78:7D:07:FF:E4`,
lokal an `/dev/ttyACM0`), Firmware 1.5, ESPHome 2026.2.4.

## Das Problem

Der ESP32-S3 hat **ein** USB-Serial-Interface. Es gibt genau ein `/dev/ttyACM0`,
und wer es öffnet, hat es exklusiv:

* läuft `esphome logs`, ist keine Steuerung möglich,
* steuert man, sieht man kein Log,
* und ein zweites Werkzeug bekommt „Resource busy".

Beim Testboard kommt dazu: es bevorzugt offene WLANs (`open_wifi_join.h`) und
landet dabei regelmäßig in `192.168.4.x` (`OpenWrtDach`) — von dort ist weder
OTA noch MQTT-Steuerung aus dem Hausnetz erreichbar. USB ist dann der einzige
Rückweg, und der soll nicht dadurch blockiert sein, dass jemand mitloggt.

## Aufbau

```
   ESP32-S3                    Host
 ┌───────────┐         ┌──────────────────────────────────────────┐
 │ usb_mux.h │  USB    │  usbmux daemon   (hält /dev/ttyACM0)     │
 │  Kanal 1 ─┼──CDC────┤   ├─ Kanal 1  ctl-*.sock   tty-ctl-*     │
 │  Kanal 2 ─┤         │   ├─ Kanal 2  log-*.sock   tty-log-*     │
 │           │         │   └─ Kanal 0  (roh, 1:1)   tty-raw-*     │
 └───────────┘         └──────────────────────────────────────────┘
```

Die Trennung ist **logisch und zeilenweise**, denn ein zweites `ttyACM` kann es
nicht geben:

| Kanal | Inhalt | Erkennung auf der Leitung |
|---|---|---|
| 1 Steuerung | Kommando rein, Antwort raus | Antwortzeilen mit Präfix `#K1# `, Abschluss `+OK` / `-ERR …` |
| 2 Log | unveränderter ESPHome-Log | alles ohne dieses Präfix |
| 0 roh | 1:1-Durchreiche für esptool | eigener Kanal, kein Parsing (`raw on`) |

Jeder Kanal wird **zweifach** angeboten:

* **Unix-Socket** — für `usbmux cmd` / `usbmux mon` und eigene Skripte.
  Beliebig viele Monitore gleichzeitig, das kann `esphome logs` prinzipiell nicht.
* **PTY mit festem Symlink** — für Fremdwerkzeuge, die einen *Gerätepfad* wollen
  (`esphome logs`, `esp_idf_monitor`, `screen`, PlatformIO, esptool). Die merken
  nichts vom Mux; für sie ist es eine gewöhnliche serielle Schnittstelle.

Pfade liegen unter `$XDG_RUNTIME_DIR/usbmux/` (sonst `/tmp/usbmux-$UID/usbmux/`)
und tragen den Gerätenamen im Suffix, also z. B. `tty-log-ttyACM0`.

## Bedienung

```bash
make -C waveshare usbmux          # eine Binary, keine Abhängigkeiten

./usbmux daemon &                 # hält den Port, legt Sockets + PTYs an
./usbmux cmd status               # Kanal 1
./usbmux mon                      # Kanal 2, beliebig oft parallel
./usbmux state                    # Zustand + alle Pfade
./usbmux pause | resume           # Port ab-/wieder angeben
./usbmux boot | reset             # Download-Modus / Neustart (echte DTR/RTS)
./usbmux flash rs485defect.yaml   # Port frei, esphome run, Port zurück
./usbmux esptool flash_id         # esptool durch den Rohkanal

# Fremdwerkzeug direkt auf dem PTY:
esphome logs rs485defect.yaml --device $XDG_RUNTIME_DIR/usbmux/tty-log-ttyACM0
```

Ohne laufenden Daemon fallen `cmd` und `mon` auf den Direktmodus zurück (mit
Warnung) — dann wieder exklusiv.

### Kommandos, die der ESP kennt (Kanal 1)

`help` `ping` `status` `relay` `relay <1-6|all> <0|1>` `openwifi <0|1>` `scan`
`wifi` `loglevel <NONE|ERROR|WARN|INFO|DEBUG|VERBOSE>` `reboot`

`usbmux cmd help` fragt das Gerät direkt — die Liste kommt aus der Firmware,
nicht aus dieser Datei.

`status` liefert dasselbe JSON wie das MQTT-Topic `…/status`; beide gehen durch
`script: build_status`, damit es nicht zwei Wahrheiten gibt. Genauso teilen sich
MQTT und Kanal 1 die Scripts `set_relay` und `set_openwifi`.

## Grenzen: was passt durch die Leitung?

Drei Schichten reden mit, alle gemessen bzw. aus dem Quelltext belegt:

| Schicht | Grenze | Beleg |
|---|---|---|
| USB Full-Speed Bulk | 64 B je 1-ms-Frame | Hardware |
| RX-Ring des Treibers | 512 B | `usb_serial_jtag_config.rx_buffer_size`, ESPHome `logger_esp32.cpp:52` |
| TX-Ring des Treibers | 512 B | ebenda, Zeile 53 |
| ESPHome-Log-Zeile | 512 B, danach abgeschnitten | `logger: tx_buffer_size` Default, `logger/__init__.py:224` |

Daraus die effektiven Werte:

* **Kanal 1 Host → ESP:** 160 Zeichen je Kommandozeile (`UM_MAX_LINE`), längeres
  wird verworfen und mit `-ERR Zeile zu lang` quittiert. Harte Grenze wäre ein
  Burst > 512 B innerhalb eines 50-ms-Poll-Fensters; ein Kommando ist ~20 B.
* **Kanal 1 ESP → Host:** 448 B Nutztext + Präfix, eine Zeile = ein `fwrite`.
  Das Status-JSON ist mit 480 B die längste Antwort im System.
* **Kanal 2:** ESPHomes 512 B je Meldung.

### Der Burst-Effekt (im Stresstest gefunden)

Unter Last kann der TX-Ring überlaufen, und der Treiber **verwirft den Rest der
Zeile — inklusive Zeilenende**. Gemessen am 10.08.2026:

```
[I][open_wifi:217]: 0 offene APs, gewählt '-' (-127 dB#K1# pong fw=1.5 uptime=14
```

Die Log-Zeile bricht mitten im Wort ab, die nächste Antwort klebt daran. Mit
reinem `esphome logs` würde man das als harmlos abgeschnittene Zeile übersehen;
für einen Demux ist es ein Kanalleck. Zwei Gegenmaßnahmen, beide drin:

1. **Firmware:** jede Antwort beginnt mit einem führenden `\n` (`um_write_line_`).
   Damit startet sie garantiert auf einer frischen Zeile, egal was vorher
   verlorenging. Leerzeilen wirft der Host weg.
2. **Host:** steht das Präfix mitten in einer Zeile, wird getrennt statt beides
   zu verlieren — vorderer Teil ist Log, ab dem Präfix beginnt Kanal 1.

## Was ein PTY kann und was nicht

Gemessen mit einem eigenen Testprogramm, nicht geraten:

| Eigenschaft | durch PTY? | Ergebnis |
|---|---|---|
| Nutzdaten | ja | Passthrough funktioniert |
| **Baudrate** | **ja** | `tcgetattr(master)` liefert exakt, was der Slave gesetzt hat (Slave `B460800` → Master meldet `B460800`) |
| **DTR/RTS** | **nein** | `TIOCMSET`/`TIOCMGET` auf `/dev/pts/*` → `ENOTTY`. pyserial öffnet zwar klaglos, `setDTR()` wirft `OSError [Errno 25]` |
| `TIOCPKT` | ja | vorhanden, aber unnötig — Baud per `tcgetattr` reicht |

Konsequenzen:

* **Baudrate ist kein Problem.** Der Daemon liest die Einstellung des Werkzeugs
  vom PTY-Master und spiegelt sie auf den echten Port (`mirror_baud()`). Ob ein
  Tool mit 115200 (esptool-Default), 460800 oder 921600 kommt, ist ihm gleich —
  an USB-CDC ist die Baudrate ohnehin folgenlos, aber es soll ja nichts merken.
* **Reset kann ein PTY nicht.** Deshalb macht der Daemon die Modemleitungen
  selbst: er hält den echten Port und fährt dort die Sequenzen aus esptools
  `reset.py` (`seq_bootloader` = USBJTAGSerialReset, `seq_hard_reset` = HardReset).

### esptool durch den Mux

`usbmux esptool <args>` erledigt die Choreografie, der Anwender sieht nichts davon:

1. `!raw on` — Zeilenparser aus, es kommen SLIP-Frames
2. `!boot` — echte DTR/RTS-Sequenz auf `/dev/ttyACM0`, Chip geht in den Download-Modus
   (USB re-enumeriert dabei kurz, die Reopen-Schleife fängt das auf)
3. `esptool --port <tty-raw-…> --before no_reset --after no_reset <args>`
4. `!reset` — Anwendung startet wieder, `!raw off`

Verifiziert am 10.08.2026: Chip erkannt, Stub geladen, Flash ausgelesen
(`Manufacturer 46, Device 4018, 16MB`), danach meldete sich die Anwendung mit
`pong fw=1.5 uptime=5` zurück.

Zum reinen Flashen der eigenen Firmware ist `usbmux flash <yaml>` der kürzere
Weg: gibt den echten Port frei, ruft `esphome run`, nimmt ihn danach zurück.

## Stresstest

`./usbmux_stress.sh` (Daemon muss laufen). Prüft nicht „läuft durch", sondern
das, was beim Multiplexen wirklich schiefgeht. Ergebnis 10.08.2026, alle
bestanden:

| Prüfung | Ergebnis |
|---|---|
| 100× `status` nacheinander | 100/100 vollständig, 14–19,5 Kommandos/s |
| 20 gleichzeitige Kommandos | 20/20 korrekt zugeordnet, keine Vermischung |
| Kanaltrennung unter Last | 0 Steuerzeilen im Log, 0 Log-Zeilen in Antworten |
| 12 Monitore gleichzeitig | alle 12 versorgt |
| 300-Zeichen-Kommando | sauber abgewiesen (`-ERR Zeile zu lang`) |
| unbekanntes Kommando, `relay 9`, `loglevel VERBOSE` | jeweils `-ERR` |
| `pause` / `resume` | im Pausenzustand klarer Fehler statt Hänger, danach wieder erreichbar |

Der erste Durchlauf fand das Kanalleck oben (1 Steuerzeile im Log) — genau dafür
ist der Test da.

## Fallstricke, die Zeit gekostet haben

* **Öffnen des Ports kann das Board rebooten.** Die erste (Python-)Fassung setzte
  beim Öffnen DTR und RTS aktiv auf 0; das Board startete jedes Mal neu
  (`rst:0x15 USB_UART_CHIP_RESET`). Die C-Fassung fasst die Leitungen nicht an
  und löscht zusätzlich `HUPCL`, damit auch das *Schließen* (pause/flash) keinen
  Reset auslöst.
* **`on:` als YAML-Schlüssel ist ein Boolean.** Ein Script-Parameter `on: bool`
  verschwand spurlos, der Compiler meldete `'on' was not declared in this scope`
  (und schlug hilfsbereit `yn` aus `math.h` vor). Der Parameter heißt jetzt `st`.
* **`includes:` registriert keine Components.** Deshalb sind `usb_mux.h` und
  `open_wifi_join.h` reine Header mit freien Funktionen, die aus Lambdas gerufen
  werden — siehe Kommentar in beiden Dateien.
* **Der Kommando-Dispatch steht im YAML, nicht im Header.** `id(ch1)` & Co.
  werden erst *nach* dem Include deklariert; ein Dispatch im Header sähe sie nicht.
  Im Header liegt nur der Transport.
* **Logger-UART ist beim Prod-Board aus** (`baud_rate: 0`, Log läuft über MQTT).
  Am Testboard steht `hardware_uart: USB_SERIAL_JTAG`, deshalb funktioniert der
  Mux hier und dort nicht.

## Dateien

| Datei | Rolle |
|---|---|
| `usb_mux.h` | ESP-Seite: Transport (Zeilen lesen, Antworten schreiben, Parser-Helfer) |
| `rs485defect.yaml` | Kommando-Dispatch, gemeinsame Scripts `set_relay` / `set_openwifi` / `build_status` |
| `usbmux.c` | Host-Daemon + Clients, eine Binary ohne Abhängigkeiten |
| `Makefile` | `make usbmux` |
| `usbmux_stress.sh` | Belastungs- und Korrektheitstest |

Die frühere Python-Fassung (`usbmux.py`) ist entfallen — zwei Implementierungen
desselben Protokolls hätten unweigerlich auseinandergelebt.
