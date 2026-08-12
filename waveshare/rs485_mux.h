#pragma once
// ════════════════════════════════════════════════════════════════════════════
//  rs485_mux v1.0 — ein RS485-Bus, zwei Kanäle im Zeitmultiplex
//
//    Kanal A = Soyo    (4800 Baud, TX-only, Takt 4 s, hat immer Vorrang)
//    Kanal B = FoxESS  (Modbus RTU, Request/Response, läuft in A's Lücke)
//
//  Der Waveshare hat genau einen RS485-Transceiver. Kanal A ist reiner
//  Zuhörer (soyo_config.h: "RX not used, Soyo only receives") und antwortet
//  nie — es gibt also kein Antwortfenster zu schützen. Die gesamte Lücke
//  zwischen zwei A-Frames gehört Kanal B.
//
//  Aufteilung wie in fox2db_logic.h: hier steht nur Logik, alle UART-Zugriffe
//  bleiben in der Lambda. Grund: per `includes:` eingebundene Components
//  werden nie registriert, setup() läuft nie — nur freie inline-Funktionen.
//
//  tick() liefert je Aufruf EINE Aktion, die die Lambda ausführt; der Zustand
//  wandert dabei weiter. Empfangene Bytes reicht die Lambda per feed() herein.
// ════════════════════════════════════════════════════════════════════════════
#include <stdint.h>
#include <stdio.h>
#include <string.h>

namespace rs485_mux {

// ── Kanal A: Soyo ────────────────────────────────────────────────────────────
// 3800 statt 4000: das 50-ms-Raster der Lambda dehnte den Ist-Takt auf
// gemessene 4003..4047 ms — jeder Wert über dem 4-s-Timeout des Soyo.
constexpr uint32_t A_PERIOD_MS = 3800;   // Sendetakt (Gerät verliert nach 4 s)
constexpr int      A_W         = 10;     // Sollwert der Testfahrt (W)
constexpr uint32_t A_BAUD      = 4800;   // fest, nicht verhandelbar
constexpr uint32_t A_GUARD_MS  = 600;    // so lange vor A's Termin ist B tabu

// ── Kanal B: FoxESS-Modbus ───────────────────────────────────────────────────
// B_ENABLE=false: Kanal B sendet NICHTS auf den Bus — Stufe 1 der Inbetrieb-
// nahme, in der nur Kanal A im neuen Takt geprüft wird.
constexpr bool     B_ENABLE  = false;
constexpr uint32_t B_BAUD    = 9600;  // frei wählbar; == A_BAUD schaltet das Umschalten ab
constexpr uint8_t  B_SLAVE   = 1;     // PLATZHALTER: Slave-Adresse T20-G3 unbestätigt
constexpr uint16_t B_START   = 0;     // PLATZHALTER: Startregister unbestätigt
constexpr uint16_t B_COUNT   = 4;     // Registeranzahl (Lesen ist rückwirkungsfrei)
constexpr uint32_t B_TIMEOUT_MS = 500;  // ohne Antwort → Transaktion verworfen
constexpr uint32_t B_GAP_MS     = 6;    // Frame-Ende: 3,5 Zeichen @9600 ≈ 3,65 ms

// Ist ein Baudwechsel je Slot nötig? Bei gleicher Rate fällt der ganze
// load_settings()-Pfad weg (der intern uart_driver_delete/-install macht).
constexpr bool NEEDS_SWITCH = (B_BAUD != A_BAUD);

// ── Kanal A: Frame (1:1 aus der Lambda in sofar_waveshare.yaml) ──────────────
inline int a_frame(uint8_t out[8], int w) {
  uint8_t ph = (w >> 8) & 0xFF, pl = w & 0xFF;
  out[0] = 0x24; out[1] = 0x56; out[2] = 0x00; out[3] = 0x21;
  out[4] = ph;   out[5] = pl;   out[6] = 0x80;
  out[7] = (uint8_t) ((264 - ph - pl) & 0xFF);
  return 8;
}

// ── Kanal B: Modbus RTU ──────────────────────────────────────────────────────
inline uint16_t b_crc16(const uint8_t *d, int len) {
  uint16_t crc = 0xFFFF;
  for (int i = 0; i < len; i++) {
    crc ^= d[i];
    for (int b = 0; b < 8; b++)
      crc = (crc & 1) ? (uint16_t) ((crc >> 1) ^ 0xA001) : (uint16_t) (crc >> 1);
  }
  return crc;
}

inline int b_build_read(uint8_t out[8], uint8_t slave, uint16_t start, uint16_t count) {
  out[0] = slave; out[1] = 0x03;
  out[2] = start >> 8; out[3] = start & 0xFF;
  out[4] = count >> 8; out[5] = count & 0xFF;
  uint16_t c = b_crc16(out, 6);
  out[6] = c & 0xFF; out[7] = c >> 8;   // CRC low byte zuerst
  return 8;
}

inline int b_expected_len(uint16_t count) { return 5 + 2 * (int) count; }

// 0 = ok, 1 = zu kurz, 2 = falscher Slave, 3 = Exception, 4 = CRC
inline int b_check(const uint8_t *b, int len, uint8_t slave, uint16_t count) {
  if (len < 5) return 1;
  if (b[0] != slave) return 2;
  if (b[1] & 0x80) return 3;
  if (len < b_expected_len(count)) return 1;
  uint16_t c = b_crc16(b, len - 2);
  if ((c & 0xFF) != b[len - 2] || (c >> 8) != b[len - 1]) return 4;
  return 0;
}

inline uint16_t b_reg(const uint8_t *b, int i) {
  return (uint16_t) ((b[3 + 2 * i] << 8) | b[4 + 2 * i]);
}

// ── Zustandsautomat ──────────────────────────────────────────────────────────
enum Action : uint8_t {
  ACT_NONE = 0,
  ACT_A_TX,       // Soyo-Frame senden + flush()
  ACT_BAUD_B,     // auf B_BAUD stellen
  ACT_B_TX,       // Modbus-Request senden + flush()
  ACT_BAUD_A,     // zurück auf A_BAUD
  ACT_REPORT,     // B-Transaktion abgeschlossen (ok oder nicht) → publizieren
};

enum Phase : uint8_t { PH_IDLE, PH_B_BAUD, PH_B_TX, PH_B_WAIT, PH_B_BACK, PH_REPORT };

struct State {
  Phase    phase      = PH_IDLE;
  uint32_t last_a_ms  = 0;
  uint32_t phase_ms   = 0;
  uint32_t last_rx_ms = 0;
  int      rx_len     = 0;
  int      last_rc    = -1;   // Ergebnis der letzten B-Transaktion (b_check)
  uint32_t n_a = 0, n_ok = 0, n_timeout = 0, n_bad = 0, n_abort = 0;
};

// Von der Lambda für jedes empfangene Byte aufzurufen.
inline void feed(State &st, uint8_t b, uint32_t now, uint8_t *buf, int cap) {
  if (st.phase != PH_B_WAIT) return;   // ausserhalb des B-Slots ist alles Müll
  if (st.rx_len < cap) buf[st.rx_len++] = b;
  st.last_rx_ms = now;
}

// Liefert die nächste auszuführende Aktion. Genau eine pro Aufruf.
inline Action tick(State &st, uint32_t now, const uint8_t *buf) {
  // Notbremse: Kanal A hat immer Vorrang. Läuft eine B-Transaktion zu nah an
  // A's Termin heran, wird sie abgebrochen statt den Sendetakt zu reissen.
  if (st.phase != PH_IDLE && st.phase != PH_REPORT &&
      (now - st.last_a_ms) >= (A_PERIOD_MS - A_GUARD_MS)) {
    st.n_abort++;
    st.last_rc = -2;
    st.phase = PH_B_BACK;
  }

  switch (st.phase) {
    case PH_IDLE:
      if (now - st.last_a_ms >= A_PERIOD_MS) {
        st.last_a_ms = now;
        st.n_a++;
        st.phase    = !B_ENABLE ? PH_IDLE : (NEEDS_SWITCH ? PH_B_BAUD : PH_B_TX);
        st.phase_ms = now;
        return ACT_A_TX;   // flush() in der Lambda: erst dann ist der Bus frei
      }
      return ACT_NONE;

    case PH_B_BAUD:
      st.phase    = PH_B_TX;
      st.phase_ms = now;
      return ACT_BAUD_B;

    case PH_B_TX:
      st.rx_len     = 0;
      st.last_rx_ms = 0;
      st.phase      = PH_B_WAIT;
      st.phase_ms   = now;
      return ACT_B_TX;

    case PH_B_WAIT: {
      // Frame gilt als komplett, wenn die erwartete Länge da ist oder die
      // Leitung B_GAP_MS still war (Modbus-Zeichenlücke).
      bool voll   = st.rx_len >= b_expected_len(B_COUNT);
      bool luecke = st.rx_len > 0 && (now - st.last_rx_ms) >= B_GAP_MS;
      if (voll || luecke) {
        st.last_rc = b_check(buf, st.rx_len, B_SLAVE, B_COUNT);
        if (st.last_rc == 0) st.n_ok++; else st.n_bad++;
        st.phase = PH_B_BACK;
      } else if (now - st.phase_ms >= B_TIMEOUT_MS) {
        st.last_rc = -1;
        st.n_timeout++;
        st.phase = PH_B_BACK;
      }
      return ACT_NONE;
    }

    case PH_B_BACK:
      st.phase    = PH_REPORT;
      st.phase_ms = now;
      if (NEEDS_SWITCH) return ACT_BAUD_A;
      return ACT_NONE;

    case PH_REPORT:
      st.phase = PH_IDLE;
      return ACT_REPORT;
  }
  return ACT_NONE;
}

// ── Status als JSON (gleicher Stil wie die übrigen Topics) ───────────────────
inline void status_json(const State &st, const uint8_t *buf, char *out, int cap) {
  char hex[2 * 32 + 1];
  int n = st.rx_len > 32 ? 32 : st.rx_len;
  for (int i = 0; i < n; i++) sprintf(hex + 2 * i, "%02X", buf[i]);
  hex[2 * n] = 0;
  snprintf(out, cap,
           "{\"a_w\":%d,\"a_sends\":%lu,\"rc\":%d,\"rx_len\":%d,\"hex\":\"%s\","
           "\"ok\":%lu,\"timeout\":%lu,\"bad\":%lu,\"abort\":%lu,"
           "\"switch\":%d,\"b_baud\":%lu}",
           A_W, (unsigned long) st.n_a, st.last_rc, st.rx_len, hex,
           (unsigned long) st.n_ok, (unsigned long) st.n_timeout,
           (unsigned long) st.n_bad, (unsigned long) st.n_abort,
           NEEDS_SWITCH ? 1 : 0, (unsigned long) B_BAUD);
}

}  // namespace rs485_mux
