#pragma once
#include "esphome.h"
#include "esphome/components/wifi/wifi_component.h"
#include <esp_wifi.h>
#include <esp_attr.h>
#include "ping/ping_sock.h"
#include "lwip/ip_addr.h"
#include <algorithm>
#include <string>
#include <vector>

// ─────────────────────────────────────────────────────────────────────────────
//  open_wifi_join — offenes (passwortfreies) WLAN bevorzugen, f24 als Fallback
//
//  Warum als Funktions-Header und nicht als Component:
//    open_wifi_scan.h definierte eine esphome::Component, die per `includes:`
//    zwar instanziiert, aber nie mit App.register_component() registriert
//    wurde -> setup() lief nie. Hier sind es reine Funktionen, die aus
//    ESPHome-Lambdas gerufen werden; kein Registrierungsproblem.
//
//  Warum NICHT die External Component components/open_wifi (wie Gassensor):
//    Am 10.08.2026 als v3.6.0 versucht. Die Komponente scannt in setup() bei
//    Priority 350, also bevor ESPHomes WiFiComponent den ESP32-WLAN-Stack
//    initialisiert. Auf dem ESP8266 traegt das; auf dem ESP32-S3 kam das Board
//    nach dem Flash nicht mehr hoch — kein WLAN, kein MQTT, auch nach dem
//    reboot_timeout nicht, Rettung nur per USB. Der Laufzeit-Scan hier ist der
//    fuer diesen Chip belegte Weg.
//
//  Ablauf:
//    1. ow_scan()     blockierender Passiv-Scan (~1,3 s), bester offener AP
//    2. ow_prefer()   STA-Liste neu bauen: offener AP prio 10, f24 prio 0.
//                     Passwortloser AP -> ESPHome setzt threshold.authmode
//                     automatisch auf WIFI_AUTH_OPEN (min_auth_mode greift
//                     nur bei gesetztem Passwort).
//    3. Netz-Check    ow_net_check(): "verbunden" ist nicht "erreichbar".
//                     wifi.connected meldet nur Assoziation + DHCP-Lease; ein
//                     offener AP ohne funktionierenden Uplink gilt damit als
//                     verbunden, weshalb weder reboot_timeout noch der
//                     Watchdog unten je greifen. Deshalb ICMP gegen den
//                     Broker: 2 min ohne Echo -> zurück auf den Fallback.
//    4. Watchdog      Hängt das Board am offenen AP ohne MQTT-Verbindung
//                     (typisch: Geräte-Fallback-APs wie "ESP_xxxxxx",
//                     Captive Portals), wird die SSID nach Timeout auf eine
//                     Blacklist gesetzt und f24 übernimmt wieder.
//
//  Blacklist lebt im RAM; zusätzlich hält ow_hold die zuletzt aussortierte
//  SSID im RTC-Speicher fest (überlebt Reboot, nicht den Strom-Aus). Ohne das
//  landet das Board nach jedem Reboot wieder am selben toten AP.
// ─────────────────────────────────────────────────────────────────────────────

struct OpenApResult {
  std::string ssid;  // bester offener AP (höchster RSSI), "" = keiner
  int rssi;          // dessen RSSI, -127 wenn keiner
  int count;         // Anzahl gefundener offener APs (ohne Blacklist)
};

// ── Zustand ──────────────────────────────────────────────────────────────────
static std::vector<std::string> ow_blacklist;  // SSIDs ohne MQTT-Durchgang
static std::string ow_preferred;               // aktuell bevorzugter offener AP
static uint32_t ow_bad_since = 0;              // millis() seit MQTT weg (0 = ok)

// Macht einen String JSON-sicher (" und \ escapen, Steuerzeichen raus).
inline std::string json_escape(const std::string &in) {
  std::string out;
  for (char c : in) {
    if (c == '"' || c == '\\') {
      out += '\\';
      out += c;
    } else if ((unsigned char) c >= 0x20) {
      out += c;
    }
  }
  return out;
}

// Espressif-Default-SoftAPs ("ESP_A1B2C3", "ESP-xxx") und ESPHome-Fallback-APs
// sind Geräte-APs ohne Uplink -> nie als Ziel brauchbar, immer überspringen.
inline bool ow_is_device_ap(const std::string &ssid) {
  static const char *PREFIXES[] = {"ESP_", "ESP-", "ESPHome", "esphome"};
  for (const char *p : PREFIXES) {
    const size_t n = strlen(p);
    if (ssid.size() >= n && ssid.compare(0, n, p) == 0)
      return true;
  }
  return false;
}

// Schwächer als das hier wird nicht mehr als Ziel angenommen (wie beim
// Gassensor, components/open_wifi: min_rssi). Ohne diese Grenze stand
// 'OpenWrtDach' mit -93 dBm als Kandidat in der Liste — ein AP, der in ein
// unerreichbares 192.168.4.x führt und nur deshalb nie gewählt wurde, weil
// f7240 stärker ist. Fällt f7240 aus, wäre das Board dort gelandet.
static const int OW_MIN_RSSI = -85;

// ── Sperre über den Reboot hinweg (RTC) ─────────────────────────────────────
//
// Die RAM-Blacklist ist nach jedem Neustart leer. Genau das war die Falle: ein
// AP, der eine IP vergibt aber keinen Uplink hat, wurde aussortiert — dann kam
// aus irgendeinem Grund ein Reboot (MQTT-reboot_timeout, OTA, Watchdog), und
// das Board hing wieder am selben toten AP. Der RTC-Speicher überlebt einen
// Reboot (nicht Strom-Aus) und hält deshalb die zuletzt aussortierte SSID für
// OW_HOLD_CYCLES Scan-Durchläufe fern. Danach darf sie wieder mitspielen —
// vielleicht hat der Betreiber sein Netz ja repariert.
struct OwHold {
  uint32_t magic;
  uint16_t cycles;      // verbleibende scan_open-Durchläufe mit Sperre
  uint16_t wd_reboots;  // Neustarts durch den Loop-Watchdog (siehe unten)
  char ssid[33];        // SSID max. 32 Zeichen + NUL
};
RTC_NOINIT_ATTR static OwHold ow_hold;
// Die Zahl gehört zum Layout von OwHold: ändert sich das Struct, muss sie
// mitwandern, sonst liest der nächste Reboot den alten Inhalt falsch aus.
static const uint32_t OW_HOLD_MAGIC = 0x0F7240A6;
static const uint16_t OW_HOLD_CYCLES = 24;  // 24 x 5 min = 2 h

// Aus on_boot rufen: nach Strom-Aus steht im RTC-RAM Müll, den erkennt magic.
inline void ow_hold_init() {
  if (ow_hold.magic != OW_HOLD_MAGIC) {
    ow_hold.magic = OW_HOLD_MAGIC;
    ow_hold.cycles = 0;
    ow_hold.wd_reboots = 0;
    ow_hold.ssid[0] = '\0';
    return;
  }
  if (ow_hold.cycles > 0)
    ESP_LOGW("open_wifi", "'%s' bleibt noch %u Durchläufe gesperrt (RTC)", ow_hold.ssid,
             (unsigned) ow_hold.cycles);
}

inline bool ow_hold_blocks(const std::string &ssid) {
  return ow_hold.magic == OW_HOLD_MAGIC && ow_hold.cycles > 0 && !ssid.empty() &&
         ssid == ow_hold.ssid;
}

inline void ow_hold_set(const std::string &ssid) {
  if (ssid.empty())
    return;
  ow_hold.magic = OW_HOLD_MAGIC;
  strncpy(ow_hold.ssid, ssid.c_str(), sizeof(ow_hold.ssid) - 1);
  ow_hold.ssid[sizeof(ow_hold.ssid) - 1] = '\0';
  ow_hold.cycles = OW_HOLD_CYCLES;
  ESP_LOGW("open_wifi", "'%s' für %u Durchläufe gesperrt (überlebt Reboot)", ow_hold.ssid,
           (unsigned) OW_HOLD_CYCLES);
}

// Ein Tick je Scan-Durchlauf (ow_find_open), nicht je Sekunde: so hängt die
// Sperre am Takt der Auswahl und nicht an millis(), das der Reboot nullt.
inline void ow_hold_tick() {
  if (ow_hold.magic == OW_HOLD_MAGIC && ow_hold.cycles > 0)
    ow_hold.cycles--;
}

inline bool ow_is_blacklisted(const std::string &ssid) {
  if (ow_hold_blocks(ssid))
    return true;
  for (const auto &b : ow_blacklist)
    if (b == ssid)
      return true;
  return false;
}

inline void ow_blacklist_add(const std::string &ssid) {
  if (ssid.empty() || ow_is_blacklisted(ssid))
    return;
  ow_blacklist.push_back(ssid);
  ESP_LOGW("open_wifi", "'%s' auf Blacklist (kein MQTT)", ssid.c_str());
}

// Aktuell verbundene SSID ("" wenn nicht verbunden).
inline std::string ow_current_ssid() {
  auto *wc = esphome::wifi::global_wifi_component;
  if (wc == nullptr || !wc->is_connected())
    return "";
  return wc->wifi_ssid();
}

// Gefundene offene APs dieses Durchlaufs (SSID -> bester RSSI).
static std::vector<std::pair<std::string, int>> ow_cands;

// ── Aufnahme des Standorts (Survey) ─────────────────────────────────────────
//
// Der Scan laeuft regelmaessig, bevor eine Verbindung steht. Ein ESP_LOGI
// davon ist nur am seriellen Port zu sehen und im Nachhinein gar nicht — genau
// die Luecke, die das ESP8266-Bauteil mit seiner RAM-Aufnahme schliesst. Hier
// dasselbe ohne Component: die Aufnahme bleibt im RAM stehen, bis sie jemand
// abholt. Per MQTT, sobald der Broker wieder da ist (ow_survey_pending, die
// YAML-Seite meldet im Takt), oder sofort ueber Kanal 1 am USB.
//
// Sie enthaelt bewusst ALLE Netze, nicht nur die offenen: fuer die Auswahl
// zaehlt nur das offene Feld, fuer die Beurteilung eines Standorts das ganze
// Band. Der Mitschnitt sitzt deshalb VOR dem Filter in beiden Scan-Quellen und
// vor allen Ausschlusskriterien von ow_consider_ (Geraete-AP, OW_MIN_RSSI,
// Blacklist) — was die Auswahl verwirft, ist fuer die Ausleuchtung gerade
// interessant.
struct OwSurveyEntry {
  std::string ssid;
  int rssi;
  int channel;
  bool open;
};

static const size_t OW_SURVEY_MAX = 24;  // ~1,2 kB; Deckel gegen volle Baender
static std::vector<OwSurveyEntry> ow_survey;
static uint32_t ow_survey_ms = 0;        // millis() des Scans
static int ow_survey_scanned = 0;        // angebotene Records, mit Doppelten
static bool ow_survey_pending = false;   // noch nicht gemeldet

inline void ow_survey_reset() {
  ow_survey.clear();
  ow_survey_scanned = 0;
  ow_survey_ms = millis();
}

// Doppelte kommen vor: findet der eigene Scan zwar Netze, aber kein offenes,
// zieht ow_find_open zusaetzlich ESPHomes Liste heran, und beide Quellen
// schreiben hier hinein. Gleiche SSID auf gleichem Kanal -> staerkeren
// behalten. Deshalb zaehlt `scanned` die angebotenen Records und `recorded`
// die unterscheidbaren Netze; die beiden duerfen auseinanderliegen.
inline void ow_survey_add(const std::string &ssid, int rssi, int channel, bool open) {
  if (ssid.empty())
    return;
  ow_survey_scanned++;
  for (auto &e : ow_survey) {
    if (e.channel == channel && e.ssid == ssid) {
      if (rssi > e.rssi)
        e.rssi = rssi;
      return;
    }
  }
  if (ow_survey.size() >= OW_SURVEY_MAX)
    return;
  ow_survey.push_back(OwSurveyEntry{ssid, rssi, channel, open});
}

// Zeiger auf die Aufnahme, nach RSSI absteigend. Zeiger statt Kopien, weil der
// Aufrufer die Eintraege nur liest und der Heap hier knapp ist.
inline std::vector<const OwSurveyEntry *> ow_survey_sorted() {
  std::vector<const OwSurveyEntry *> s;
  s.reserve(ow_survey.size());
  for (const auto &e : ow_survey)
    s.push_back(&e);
  std::sort(s.begin(), s.end(),
            [](const OwSurveyEntry *a, const OwSurveyEntry *b) { return a->rssi > b->rssi; });
  return s;
}

// Aufnahme als JSON. age_ms macht eine retained Nachricht als alt erkennbar —
// ohne das sieht eine Aufnahme von gestern aus wie eine von eben.
inline std::string ow_survey_json() {
  char head[256];
  snprintf(head, sizeof(head),
           "{\"scan_ms\":%u,\"age_ms\":%u,\"scanned\":%d,\"recorded\":%u,"
           "\"connected\":\"%s\",\"aps\":[",
           (unsigned) ow_survey_ms, (unsigned) (millis() - ow_survey_ms), ow_survey_scanned,
           (unsigned) ow_survey.size(), json_escape(ow_current_ssid()).c_str());

  std::string out(head);
  bool first = true;
  for (const auto *e : ow_survey_sorted()) {
    char b[160];
    snprintf(b, sizeof(b), "%s{\"ssid\":\"%s\",\"rssi\":%d,\"ch\":%d,\"open\":%s}", first ? "" : ",",
             json_escape(e->ssid).c_str(), e->rssi, e->channel, e->open ? "true" : "false");
    out += b;
    first = false;
  }
  out += "]}";
  return out;
}

// Ein Kandidat prüfen und in die Kandidatenliste aufnehmen.
inline void ow_consider_(OpenApResult &r, const std::string &ssid, int rssi, const char *own_ap_ssid) {
  if (ssid.empty() || ssid == own_ap_ssid)
    return;
  if (ow_is_device_ap(ssid)) {
    ESP_LOGD("open_wifi", "  ignoriert (Geräte-AP): '%s' RSSI=%d", ssid.c_str(), rssi);
    return;
  }
  if (rssi < OW_MIN_RSSI) {
    ESP_LOGI("open_wifi", "  ignoriert (zu schwach): '%s' RSSI=%d < %d", ssid.c_str(), rssi,
             OW_MIN_RSSI);
    return;
  }
  if (ow_is_blacklisted(ssid))
    return;

  for (auto &c : ow_cands) {  // gleiche SSID auf mehreren APs: stärksten behalten
    if (c.first == ssid) {
      if (rssi > c.second)
        c.second = rssi;
      return;
    }
  }
  ow_cands.emplace_back(json_escape(ssid), rssi);
  r.count++;
  ESP_LOGI("open_wifi", "  offen: '%s' RSSI=%d", ssid.c_str(), rssi);
}

// Auswahl: schlicht der stärkste offene AP — unabhängig vom Subnetz.
inline void ow_pick_(OpenApResult &r) {
  for (const auto &c : ow_cands) {
    if (c.second > r.rssi) {
      r.rssi = c.second;
      r.ssid = c.first;
    }
  }
}

// Quelle 1: ESPHomes eigene Scan-Ergebnisse (get_with_auth() == false -> offen).
// Kostet nichts und stört die WiFi-Statemachine nicht. Als alleinige Quelle
// taugt sie nicht: im laufenden Betrieb ist die Liste leer (am 10.08.2026 als
// v3.5.4 gemessen -> "0 offene APs"), sie fuellt sich nur rund um einen
// (Re-)Connect. Deshalb nur als Rueckfallebene.
inline OpenApResult ow_from_esphome_scan(const char *own_ap_ssid) {
  OpenApResult r{"", -127, 0};
  auto *wc = esphome::wifi::global_wifi_component;
  if (wc == nullptr)
    return r;
  for (const auto &s : wc->get_scan_result()) {
    std::string ssid(s.get_ssid().c_str(), s.get_ssid().size());
    // Mitschnitt vor dem Filter — verschluesselte und versteckte Netze
    // gehoeren in die Ausleuchtung, nur nicht in die Auswahl.
    ow_survey_add(ssid, s.get_rssi(), s.get_channel(), !s.get_with_auth());
    if (s.get_with_auth() || s.get_is_hidden())
      continue;
    ow_consider_(r, ssid, s.get_rssi(), own_ap_ssid);
  }
  return r;
}

// Quelle 2: eigener blockierender Passiv-Scan (~1,3 s) für frische Daten,
// während die Verbindung steht. Liefert gelegentlich 0 Treffer, wenn ESPHome
// gerade selbst scannt — dann übernimmt der Aufrufer Quelle 1.
//
// Nebenwirkung, bekannt und in Kauf genommen: das SCAN_DONE-Event geht auch an
// ESPHome, dessen Handler die Records dann nicht mehr vorfindet und
// "esp_wifi_scan_get_ap_record failed: ESP_FAIL" loggt. Die STA-Liste setzen
// wir ohnehin selbst; der Versuch, das sauber zu loesen (v3.5.4 passiv, v3.6.0
// Component), hat einmal die Entscheidung unmoeglich gemacht und einmal das
// Board lahmgelegt.
inline OpenApResult ow_scan(const char *own_ap_ssid) {
  OpenApResult r{"", -127, 0};

  wifi_scan_config_t cfg = {};
  cfg.ssid = nullptr;
  cfg.bssid = nullptr;
  cfg.channel = 0;  // alle Kanäle
  cfg.show_hidden = false;
  cfg.scan_type = WIFI_SCAN_TYPE_PASSIVE;
  cfg.scan_time.passive = 100;  // ms pro Kanal -> ~1,3 s gesamt

  esp_err_t err = esp_wifi_scan_start(&cfg, true);
  if (err != ESP_OK) {
    ESP_LOGD("open_wifi", "Eigener Scan nicht möglich (%s)", esp_err_to_name(err));
    return r;
  }

  uint16_t n = 0;
  esp_wifi_scan_get_ap_num(&n);
  if (n == 0)
    return r;
  if (n > 30)
    n = 30;

  wifi_ap_record_t *aps = new wifi_ap_record_t[n];
  if (esp_wifi_scan_get_ap_records(&n, aps) == ESP_OK) {
    for (int i = 0; i < n; i++) {
      std::string ssid((const char *) aps[i].ssid);
      // Mitschnitt vor dem Filter, siehe ow_survey_add. primary ist der
      // Hauptkanal des AP.
      ow_survey_add(ssid, aps[i].rssi, aps[i].primary, aps[i].authmode == WIFI_AUTH_OPEN);
      if (aps[i].authmode != WIFI_AUTH_OPEN)
        continue;
      ow_consider_(r, ssid, aps[i].rssi, own_ap_ssid);
    }
  }
  delete[] aps;
  return r;
}

// Eigener Scan bevorzugt, sonst ESPHomes letzte Scan-Ergebnisse.
inline OpenApResult ow_find_open(const char *own_ap_ssid) {
  ow_cands.clear();
  ow_survey_reset();
  ow_hold_tick();
  OpenApResult r = ow_scan(own_ap_ssid);
  if (r.count == 0)
    r = ow_from_esphome_scan(own_ap_ssid);
  ow_pick_(r);
  // Nur melden, wenn wirklich etwas aufgenommen wurde. Ein leerer Durchlauf
  // (beide Quellen still) darf die vorige Aufnahme nicht als neue ausgeben —
  // deshalb wird pending hier gesetzt und nicht in ow_survey_reset().
  ow_survey_pending = !ow_survey.empty();
  ESP_LOGI("open_wifi", "%d offene APs, gewählt '%s' (%d dBm)", r.count,
           r.ssid.empty() ? "-" : r.ssid.c_str(), r.rssi);
  return r;
}

// Baut die STA-Liste neu: offener AP (Priorität 10) vor Fallback (Priorität 0).
// open_ssid == "" -> nur der Fallback bleibt.
// Weicht die aktuelle Verbindung vom Wunsch ab, wird ein Reconnect erzwungen
// (disable()/enable() -> ESPHome scannt neu und wählt nach Priorität).
inline void ow_prefer(const std::string &open_ssid, const char *fb_ssid, const char *fb_pass) {
  auto *wc = esphome::wifi::global_wifi_component;
  if (wc == nullptr)
    return;

  // Nur bei echter Änderung anfassen: clear_sta() setzt selected_sta_index_ auf
  // -1, ESPHome verwirft daraufhin die laufende Verbindung. Ohne diese Sperre
  // würde jeder Scan-Durchlauf einen Reconnect auslösen.
  static bool configured = false;
  if (configured && open_ssid == ow_preferred)
    return;
  configured = true;

  ow_preferred = open_ssid;

  wc->clear_sta();
  if (!open_ssid.empty()) {
    esphome::wifi::WiFiAP ap;
    ap.set_ssid(open_ssid);
    // kein set_password() -> passwortlos, threshold.authmode = WIFI_AUTH_OPEN
    // hidden=true: direkt verbinden statt erst auf ein Scan-Ergebnis zu warten.
    // Nach clear_sta() ist ESPHomes Scan-Liste leer ("No networks found"), der
    // Connect liefe sonst über den Hidden-Retry-Umweg.
    ap.set_hidden(true);
    ap.set_priority(10);
    wc->add_sta(ap);
  }
  esphome::wifi::WiFiAP fb;
  fb.set_ssid(fb_ssid);
  fb.set_password(fb_pass);
  fb.set_priority(0);
  wc->add_sta(fb);

  const std::string want = open_ssid.empty() ? std::string(fb_ssid) : open_ssid;
  const std::string have = ow_current_ssid();
  if (have == want)
    return;

  ESP_LOGI("open_wifi", "Wechsel '%s' -> '%s'", have.c_str(), want.c_str());
  ow_bad_since = 0;
  wc->disable();
  wc->enable();
}

// Watchdog: am offenen AP ohne MQTT -> nach timeout_ms blacklisten und zurück
// auf den Fallback. Gibt true zurück, wenn umgeschaltet wurde.
inline bool ow_watchdog(bool mqtt_connected, uint32_t timeout_ms, const char *fb_ssid,
                        const char *fb_pass) {
  if (ow_preferred.empty())
    return false;
  if (ow_current_ssid() != ow_preferred) {
    ow_bad_since = 0;
    return false;
  }
  if (mqtt_connected) {
    ow_bad_since = 0;
    return false;
  }

  const uint32_t now = millis();
  if (ow_bad_since == 0) {
    ow_bad_since = now;
    return false;
  }
  if (now - ow_bad_since < timeout_ms)
    return false;

  ow_blacklist_add(ow_preferred);
  ow_bad_since = 0;
  ow_prefer("", fb_ssid, fb_pass);
  return true;
}

// ═══════════════════════════════════════════════════════════════════════════
//  Netz-Check: erreichbar statt nur verbunden
// ═══════════════════════════════════════════════════════════════════════════
//
//  Der Kernfehler des bisherigen Aufbaus: wifi.connected (und damit ESPHomes
//  reboot_timeout) bedeutet nur Layer-2-Assoziation + DHCP-Lease. Ein offener
//  AP, der eine IP vergibt aber keinen Uplink hat — oder ein f7240, der nach
//  Tagen die Station stillschweigend fallenlässt — gilt aus ESPHome-Sicht als
//  "verbunden". Der reboot_timeout greift dann nie.
//
//  Deshalb hier ein eigener Beweis, dass Verkehr fließt:
//    a) MQTT verbunden -> Netz ist ok, kein Ping nötig (bester Beweis).
//    b) sonst ICMP gegen den Broker. Antwortet er, ist das Netz in Ordnung
//       und nur der Broker-Dienst weg — dann wäre ein AP-Wechsel Unsinn.
//
//  Takt: ohne MQTT jeder Aufruf (30 s), mit MQTT nur alle OW_PING_GAP_MS
//  (5 min) als Messwert für die Telemetrie.
//
//  Eskalation, sobald OW_NET_BAD_MS (2 min) ohne Beweis vergangen sind:
//    1. am offenen AP  -> SSID sperren (RAM + RTC) und zurück auf den Fallback
//    2. schon am Fallback -> Reconnect erzwingen (disable/enable)
//    3. hilft beides zweimal nicht -> Rückgabe 2, der Aufrufer rebootet
//  Nach jeder Stufe beginnt das 2-min-Fenster neu, damit die neue Verbindung
//  Zeit zum Aufbau hat.

static const uint32_t OW_PING_GAP_MS = 300000;  // 5 min Routinemessung
static const uint8_t OW_PING_COUNT = 3;         // Pakete je Messung
static const uint32_t OW_PING_TIMEOUT_MS = 1000;

static esp_ping_handle_t ow_ping_hdl = nullptr;
static volatile bool ow_ping_busy = false;
static volatile bool ow_ping_done = false;
static volatile uint32_t ow_ping_replies = 0;
static volatile uint32_t ow_ping_rtt = 0;   // ms der letzten Antwort
static uint32_t ow_ping_last_ms = 0;        // millis() des letzten Starts

static bool ow_net_ok = true;               // letzter Befund
static uint32_t ow_net_bad_since = 0;       // millis() seit erstem Fehlbefund
static uint32_t ow_net_last_ok = 0;         // millis() des letzten Beweises
static uint8_t ow_net_actions = 0;          // Eskalationsstufen ohne Erfolg

inline void ow_ping_on_success(esp_ping_handle_t hdl, void *args) {
  uint32_t t = 0;
  esp_ping_get_profile(hdl, ESP_PING_PROF_TIMEGAP, &t, sizeof(t));
  ow_ping_rtt = t;
}

// Läuft im Ping-Task. esp_ping_delete_session() wartet auf genau diesen Task
// und würde von hier aus verklemmen -> nur Flagge setzen, aufgeräumt wird im
// Hauptkontext (ow_ping_reap).
inline void ow_ping_on_end(esp_ping_handle_t hdl, void *args) {
  uint32_t received = 0;
  esp_ping_get_profile(hdl, ESP_PING_PROF_REPLY, &received, sizeof(received));
  ow_ping_replies = received;
  ow_ping_done = true;
}

inline bool ow_ping_start(const char *host) {
  if (ow_ping_busy || host == nullptr || host[0] == '\0')
    return false;

  ip_addr_t target;
  if (!ipaddr_aton(host, &target)) {
    ESP_LOGW("net", "Ping-Ziel '%s' ist keine IP", host);
    return false;
  }

  esp_ping_config_t cfg = ESP_PING_DEFAULT_CONFIG();
  cfg.target_addr = target;
  cfg.count = OW_PING_COUNT;
  cfg.timeout_ms = OW_PING_TIMEOUT_MS;
  cfg.interval_ms = 300;
  cfg.task_stack_size = 3072;

  esp_ping_callbacks_t cbs = {};
  cbs.cb_args = nullptr;
  cbs.on_ping_success = ow_ping_on_success;
  cbs.on_ping_timeout = nullptr;
  cbs.on_ping_end = ow_ping_on_end;

  ow_ping_replies = 0;
  ow_ping_done = false;
  if (esp_ping_new_session(&cfg, &cbs, &ow_ping_hdl) != ESP_OK) {
    ow_ping_hdl = nullptr;
    return false;
  }
  if (esp_ping_start(ow_ping_hdl) != ESP_OK) {
    esp_ping_delete_session(ow_ping_hdl);
    ow_ping_hdl = nullptr;
    return false;
  }
  ow_ping_busy = true;
  ow_ping_last_ms = millis();
  return true;
}

inline void ow_ping_reap() {
  if (ow_ping_hdl != nullptr) {
    esp_ping_delete_session(ow_ping_hdl);
    ow_ping_hdl = nullptr;
  }
  ow_ping_busy = false;
}

// Stufe wählen und ausführen. Rückgabe wie ow_net_check().
inline int ow_net_escalate_(esphome::wifi::WiFiComponent *wc, const char *fb_ssid,
                            const char *fb_pass) {
  ow_net_actions++;

  if (!ow_preferred.empty()) {  // wir hängen am offenen AP -> der ist verdächtig
    const std::string dead = ow_preferred;
    ESP_LOGE("net", "Netz seit 2 min tot an '%s' -> zurück auf %s", dead.c_str(), fb_ssid);
    ow_blacklist_add(dead);
    ow_hold_set(dead);
    ow_prefer("", fb_ssid, fb_pass);
    return 1;
  }

  if (ow_net_actions < 3) {  // schon am Fallback: Verbindung neu aufbauen
    ESP_LOGE("net", "Netz seit 2 min tot an '%s' -> Reconnect (%u.)", fb_ssid,
             (unsigned) ow_net_actions);
    if (wc != nullptr) {
      wc->disable();
      wc->enable();
    }
    return 1;
  }

  ESP_LOGE("net", "Netz tot, Reconnect hat nicht geholfen -> Reboot");
  return 2;
}

// Aus einem YAML-Interval (30 s) rufen.
//   host          Ping-Ziel, IP-Literal (der MQTT-Broker)
//   mqtt_connected  laufender MQTT-Verkehr zählt als Beweis
//   bad_ms        Geduld ohne Beweis, bevor eskaliert wird (120000)
// Rückgabe: 0 = nichts zu tun, 1 = Gegenmaßnahme lief, 2 = Aufrufer soll rebooten
inline int ow_net_check(const char *host, bool mqtt_connected, uint32_t bad_ms,
                        const char *fb_ssid, const char *fb_pass) {
  auto *wc = esphome::wifi::global_wifi_component;
  const uint32_t now = millis();
  const bool linked = (wc != nullptr) && wc->is_connected();

  // 1) Ein frisches Ping-Ergebnis ist der harte Beweis und schlägt alles andere.
  //    Insbesondere die MQTT-Behauptung: is_connected() sagt nur, dass der
  //    Client den Socket für offen hält. Genau so stand das Board am
  //    21.08.2026 da — ESTABLISHED auf 1883, ICMP beantwortet, seit Stunden
  //    keine einzige Nachricht mehr veröffentlicht.
  bool have = false, ok = false;
  if (ow_ping_busy && ow_ping_done) {
    have = true;
    ok = (ow_ping_replies > 0);
    ESP_LOGD("net", "Ping %s: %u/%u Antworten, %u ms", host, (unsigned) ow_ping_replies,
             (unsigned) OW_PING_COUNT, (unsigned) ow_ping_rtt);
    ow_ping_reap();
  } else if (mqtt_connected) {
    // 2) Kein frisches Echo: laufender MQTT-Verkehr genügt als Beweis, dann
    //    bleibt der Funk zwischen zwei Routinemessungen unbelastet.
    have = true;
    ok = true;
  }
  // 3) Ohne Link ist nichts erreichbar — hier zählt die Zeit ebenfalls
  if (!linked) {
    have = true;
    ok = false;
  }

  int action = 0;
  if (have) {
    ow_net_ok = ok;
    if (ok) {
      ow_net_last_ok = now;
      ow_net_bad_since = 0;
      ow_net_actions = 0;
    } else if (ow_net_bad_since == 0) {
      ow_net_bad_since = now;
    } else if (now - ow_net_bad_since >= bad_ms) {
      ow_net_bad_since = now;  // Fenster neu starten, die Maßnahme braucht Zeit
      action = ow_net_escalate_(wc, fb_ssid, fb_pass);
    }
  }

  // 4) Nächste Messung anstoßen: im Fehlerfall jeden Takt, sonst alle 5 min.
  //    Die 5-min-Routinemessung ist die Gegenprobe zur MQTT-Behauptung: länger
  //    als 5 min (+ 2 min Geduld) kann ein toter Socket nichts vortäuschen.
  if (linked && !ow_ping_busy) {
    const bool due = (ow_ping_last_ms == 0) || !ow_net_ok || (now - ow_ping_last_ms >= OW_PING_GAP_MS);
    if (due)
      ow_ping_start(host);
  }
  return action;
}

// Sekunden ohne Beweis (0 = Netz ok) — für die Telemetrie.
inline uint32_t ow_net_bad_s() {
  return ow_net_bad_since == 0 ? 0 : (millis() - ow_net_bad_since) / 1000;
}

// ═══════════════════════════════════════════════════════════════════════════
//  Loop-Watchdog: eigener Task, der den Stillstand der Hauptschleife überlebt
// ═══════════════════════════════════════════════════════════════════════════
//
//  Befund vom 21.08.2026 am laufenden Board (192.168.178.187): WLAN an f7240
//  assoziiert (-37 dBm), ICMP beantwortet, TCP nach 192.168.178.218:1883
//  ESTABLISHED — aber statt der 30-s-Telemetrie kam in vier Messfenstern zu je
//  25 s kein einziges Paket, dazwischen einmal ein Status. Die Hauptschleife
//  steht also über Minuten still und läuft nur schubweise; ICMP und der offene
//  Socket kommen derweil aus dem lwIP-Task und täuschen Gesundheit vor.
//  Steht die Schleife, hilft auch kein Netz-Check: dessen Interval-Lambda ist
//  ja selbst Teil der Schleife.
//
//  Deshalb ein Wächter außerhalb der Schleife: ein eigener FreeRTOS-Task
//  zählt mit, ob ow_beat_tick() (aus dem 30-s-Interval) noch hochzählt.
//  Bleibt der Herzschlag OW_WD_LIMIT_MS lang aus, wird neu gestartet. Die
//  Zahl der so ausgelösten Neustarts steht im RTC-Speicher und geht in die
//  Telemetrie — ohne sie wäre ein Watchdog-Reboot von einem Stromausfall
//  nicht zu unterscheiden.

static volatile uint32_t ow_beat = 0;  // Herzschlag der Hauptschleife
static volatile bool ow_wd_paused = false;
static uint32_t ow_wd_limit_ms = 180000;

inline void ow_beat_tick() { ow_beat++; }

// Während eines OTA läuft die Hauptschleife im Upload-Handler und kommt nicht
// zum 30-s-Interval. Über diese wacklige Funkstrecke kann das länger dauern
// als das Limit — der Wächter würde also ausgerechnet den Rettungsweg
// abschießen. Deshalb aus ota: on_begin/on_error stumm- und wieder scharf
// schalten.
inline void ow_wd_pause(bool paused) { ow_wd_paused = paused; }

inline uint16_t ow_wd_reboots() {
  return ow_hold.magic == OW_HOLD_MAGIC ? ow_hold.wd_reboots : 0;
}

inline void ow_wd_task_fn(void *arg) {
  uint32_t last = ow_beat;
  uint32_t last_ms = millis();
  for (;;) {
    vTaskDelay(pdMS_TO_TICKS(5000));
    if (ow_wd_paused) {
      last_ms = millis();  // OTA läuft: Uhr mitziehen, sonst schlägt sie danach zu
      continue;
    }
    if (ow_beat != last) {
      last = ow_beat;
      last_ms = millis();
      continue;
    }
    if (millis() - last_ms < ow_wd_limit_ms)
      continue;
    // Kein ESP_LOGx: das ist ESPHomes Logger, der aus einem fremden Task
    // heraus in die MQTT-Callbacks greifen würde. esp_rom_printf ist stumm
    // (baud_rate 0), tut aber niemandem weh.
    esp_rom_printf("[wd] Hauptschleife steht -> Neustart\n");
    if (ow_hold.magic == OW_HOLD_MAGIC && ow_hold.wd_reboots < 0xFFFF)
      ow_hold.wd_reboots++;
    esp_restart();
  }
}

// Aus on_boot rufen, nach ow_hold_init().
inline void ow_wd_start(uint32_t limit_ms) {
  ow_wd_limit_ms = limit_ms;
  ow_beat = 0;
  xTaskCreate(ow_wd_task_fn, "ow_wd", 2048, nullptr, 1, nullptr);
  ESP_LOGI("net", "Loop-Watchdog aktiv (%u s), bisher %u Neustarts",
           (unsigned) (limit_ms / 1000), (unsigned) ow_wd_reboots());
}
