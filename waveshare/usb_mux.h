#pragma once
#include "esphome.h"
#include <driver/usb_serial_jtag.h>
#include <stdarg.h>
#include <stdio.h>
#include <string>

// ─────────────────────────────────────────────────────────────────────────────
//  usb_mux — zwei logische Kanäle auf der einen USB-CDC-Leitung (/dev/ttyACM0)
//
//  Der ESP32-S3 hat genau EIN USB-Serial-Interface, es kann also kein zweites
//  ttyACM geben. Die Trennung passiert deshalb logisch, zeilenweise:
//
//    Kanal 1 (Steuerung)  Host -> ESP: Kommandozeile im Klartext ("relay 1 1")
//                         ESP  -> Host: jede Antwortzeile mit Präfix "#K1# ",
//                         letzte Zeile immer "#K1# +OK" oder "#K1# -ERR <text>"
//    Kanal 2 (Log)        alles ohne dieses Präfix = unveränderter ESPHome-Log
//
//  Der Host (usbmux.py) filtert auf das Präfix und bedient daraus zwei Sockets.
//  Ein Log-Monitor sieht nie Steuerungsantworten, ein Steuerclient nie Log.
//
//  Warum Treiber-Ebene statt stdin:
//    ESPHomes Logger installiert in pre_setup() den IDF-Treiber
//    (usb_serial_jtag_driver_install) und hängt stdout/stdin über VFS daran.
//    Lesen über stdin wäre blockierend (der Logger setzt F_SETFL auf 0) und
//    würde die Hauptschleife anhalten. usb_serial_jtag_read_bytes(..., 0)
//    liest denselben RX-Puffer ohne zu warten.
//
//  Warum ein Header mit freien Funktionen und keine Component:
//    Wie bei open_wifi_join.h — per `includes:` eingebundene Components werden
//    nie registriert, setup() liefe nie. Aufruf hier aus einem Interval-Lambda.
//
//  Die Kommandos selbst stehen bewusst NICHT hier, sondern im YAML-Lambda:
//    `id(ch1)` & Co. werden erst nach diesem Header deklariert, ein Dispatch im
//    Header sähe sie nicht. Hier liegt nur der Transport.
// ─────────────────────────────────────────────────────────────────────────────

static const char *const UM_PREFIX = "#K1# ";
static const size_t UM_MAX_LINE = 160;  // längere Eingaben werden verworfen

// Eine komplette Zeile in einem Rutsch schreiben. Ein einzelner fwrite pro
// Zeile, damit sich Log-Ausgaben (gleicher stdout) nicht mitten in eine
// Antwortzeile schieben und der Host-Filter zuverlässig greift.
//
// Das führende \n ist kein Schönheitsfehler, sondern Absicht: der TX-Ring des
// USB-JTAG-Treibers fasst 512 B (ESPHome logger_esp32.cpp), und über USB Full
// Speed gehen nur 64 B je 1-ms-Frame hinaus. Ein Burst aus mehreren Log-Zeilen
// plus einer langen Antwort läuft über, der Treiber verwirft den Rest — am
// 10.08.2026 im Stresstest gemessen als abgeschnittene Log-Zeile, an der die
// nächste Antwort klebte ("...(-127 dB#K1# pong ..."), weil das Zeilenende mit
// verworfen wurde. Mit dem führenden \n beginnt jede Antwort garantiert auf
// einer frischen Zeile, egal was vorher verlorenging.
inline void um_write_line_(const char *body) {
  char line[512];
  const int n = snprintf(line, sizeof(line), "\n%s%s\n", UM_PREFIX, body);
  if (n > 0)
    fwrite(line, 1, (size_t) (n < (int) sizeof(line) ? n : (int) sizeof(line) - 1), stdout);
  fflush(stdout);
}

// Datenzeile auf Kanal 1 (beliebig viele pro Kommando).
inline void um_reply(const char *fmt, ...) {
  char body[448];
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(body, sizeof(body), fmt, ap);
  va_end(ap);
  um_write_line_(body);
}

// Abschlusszeile: Erfolg. Beendet die Antwort für den Host.
inline void um_ok() { um_write_line_("+OK"); }

// Abschlusszeile: Fehler. Beendet die Antwort für den Host.
inline void um_err(const char *fmt, ...) {
  char body[256];
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(body, sizeof(body), fmt, ap);
  va_end(ap);
  char line[288];
  snprintf(line, sizeof(line), "-ERR %s", body);
  um_write_line_(line);
}

// Nicht-blockierend eine Zeile vom Host holen.
// true = out enthält eine vollständige Zeile (ohne Zeilenende, getrimmt).
// Mehrfach in einer Schleife aufrufen, es kann mehr als eine Zeile im Puffer
// liegen. \r und \n zählen beide als Zeilenende (screen/minicom schicken \r).
inline bool um_read_line(std::string &out) {
  static std::string buf;
  static bool overflow = false;

  uint8_t rx[64];
  while (true) {
    const int n = usb_serial_jtag_read_bytes(rx, sizeof(rx), 0);
    if (n <= 0)
      break;
    for (int i = 0; i < n; i++) {
      const char c = (char) rx[i];
      if (c == '\n' || c == '\r') {
        if (buf.empty() && !overflow)
          continue;  // Leerzeile / CRLF-Rest
        if (overflow) {
          buf.clear();
          overflow = false;
          um_err("Zeile zu lang (max %u)", (unsigned) UM_MAX_LINE);
          continue;
        }
        out = buf;
        buf.clear();
        return true;
      }
      if (c == '\t')
        continue;
      if ((unsigned char) c < 0x20)
        continue;  // sonstige Steuerzeichen ignorieren
      if (buf.size() >= UM_MAX_LINE) {
        overflow = true;  // Rest bis zum Zeilenende wegwerfen
        continue;
      }
      buf += c;
    }
  }
  return false;
}

// ── kleine Parser-Helfer für den Dispatch im YAML ────────────────────────────

// Zerlegt an Leerzeichen; liefert max. max_parts Teile.
inline std::vector<std::string> um_split(const std::string &s, size_t max_parts = 8) {
  std::vector<std::string> parts;
  size_t i = 0;
  while (i < s.size() && parts.size() < max_parts) {
    while (i < s.size() && s[i] == ' ')
      i++;
    const size_t start = i;
    while (i < s.size() && s[i] != ' ')
      i++;
    if (i > start)
      parts.push_back(s.substr(start, i - start));
  }
  return parts;
}

// "1"/"on"/"true" -> true, alles andere false.
inline bool um_truthy(const std::string &s) {
  return s == "1" || s == "on" || s == "ON" || s == "true" || s == "TRUE";
}
