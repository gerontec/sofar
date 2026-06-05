#pragma once
#include "esphome.h"
#include "esphome/components/wifi/wifi_component.h"
#include <esp_wifi.h>
#include <string.h>

// Scans for open (password-free) WiFi networks before ESPHome's WiFi component
// connects. Best open network (highest RSSI) is added to the configured
// network list. Falls back to configured ssid (f24) if no open network found.

static const char *TAG_WSCAN = "open_wifi";

class OpenWifiScanComponent : public esphome::Component {
public:
    // Run at priority 350 — after hardware (800) but before WiFiComponent (300)
    float get_setup_priority() const override { return 350.0f; }

    void setup() override {
        ESP_LOGI(TAG_WSCAN, "Scanning for open networks…");

        esp_wifi_set_mode(WIFI_MODE_STA);

        wifi_scan_config_t scan_cfg = {};
        scan_cfg.ssid     = nullptr;
        scan_cfg.bssid    = nullptr;
        scan_cfg.channel  = 0;
        scan_cfg.show_hidden = false;
        scan_cfg.scan_type = WIFI_SCAN_TYPE_ACTIVE;
        scan_cfg.scan_time.active.min = 100;
        scan_cfg.scan_time.active.max = 300;

        esp_err_t err = esp_wifi_scan_start(&scan_cfg, /*block=*/true);
        if (err != ESP_OK) {
            ESP_LOGW(TAG_WSCAN, "Scan failed: %s — falling back to f24", esp_err_to_name(err));
            return;
        }

        uint16_t ap_count = 0;
        esp_wifi_scan_get_ap_num(&ap_count);
        if (ap_count == 0) {
            ESP_LOGI(TAG_WSCAN, "No networks found — falling back to f24");
            return;
        }

        uint16_t max_aps = ap_count < 20 ? ap_count : 20;
        wifi_ap_record_t *aps = new wifi_ap_record_t[max_aps];
        esp_wifi_scan_get_ap_records(&max_aps, aps);

        int best_idx  = -1;
        int best_rssi = -120;
        for (int i = 0; i < max_aps; i++) {
            if (aps[i].authmode != WIFI_AUTH_OPEN) continue;
            if (strlen((char *)aps[i].ssid) == 0) continue;
            ESP_LOGI(TAG_WSCAN, "  Open: '%s'  RSSI=%d", (char *)aps[i].ssid, aps[i].rssi);
            if (aps[i].rssi > best_rssi) {
                best_rssi = aps[i].rssi;
                best_idx  = i;
            }
        }

        if (best_idx < 0) {
            ESP_LOGI(TAG_WSCAN, "No open networks — falling back to f24");
            delete[] aps;
            return;
        }

        std::string ssid((char *)aps[best_idx].ssid);
        ESP_LOGI(TAG_WSCAN, "Best open: '%s' RSSI=%d — adding to WiFi list", ssid.c_str(), best_rssi);

        esphome::wifi::WiFiAP ap;
        ap.set_ssid(ssid);
        // No password = open network
        esphome::wifi::global_wifi_component->add_sta(ap);

        delete[] aps;
    }
};

OpenWifiScanComponent *open_wifi_scanner{new OpenWifiScanComponent()};
