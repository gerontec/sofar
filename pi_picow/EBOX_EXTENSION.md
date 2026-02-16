# EBox RS232 Extension for Pico W

**Erweitert den Pico W MQTT Controller um Battery Management System Überwachung**

---

## 🎯 Was ist neu?

Der Pico W liest jetzt **zusätzlich** die EBox (Battery Management System) via RS232 aus und sendet den durchschnittlichen SOC (State of Charge) an MQTT.

**File:** `pico_w_ebox_mqtt.c`

---

## 🔌 Hardware

### RS232 Modul: MAX3232

**Warum MAX3232?**
- Konvertiert TTL (3.3V) ↔ RS232 (±12V)
- Pico W kann nur TTL (0V/3.3V)
- EBox nutzt RS232 (±12V)

**Pinout:**

| Pico W Pin | MAX3232 Pin | Funktion |
|------------|-------------|----------|
| GPIO 8 | T1IN (TTL TX) | Send data to EBox |
| GPIO 9 | R1OUT (TTL RX) | Receive data from EBox |
| 3.3V | VCC | Power |
| GND | GND | Ground |

**MAX3232 RS232 Side:**
- TX → EBox RX
- RX → EBox TX
- GND → EBox GND

---

## 📡 Komplette Hardware-Übersicht

```
┌────────────────────────────────────────┐
│           Pico W                       │
│                                        │
│  GPIO 0/1 + GPIO 2 → MAX485 #1 → SOYO │
│  GPIO 4/5 + GPIO 6 → MAX485 #2 → Relay│
│  GPIO 8/9          → MAX3232   → EBox │
│                                        │
│  WiFi              → MQTT Broker       │
└────────────────────────────────────────┘
```

**3 Serielle Schnittstellen:**
1. **UART0** (Hardware) → MAX485 → SOYO (RS485, 4800 baud)
2. **UART1** (Hardware) → MAX485 → Relay (RS485, 9600 baud)
3. **Software UART** (Bit-Bang) → MAX3232 → EBox (RS232, 115200 baud)

---

## 📋 EBox Protokoll

### Command Format

**Send (ASCII):**
```
"pwr\r\n"
```

**Bedeutung:** Read power status from all batteries

### Response Format

**Receive (ASCII, multiple lines):**
```
Power Volt   Curr   Tempr  ...  Coulomb  ...
1     51018  0      19000  ...  11%      ...
2     51034  0      19000  ...  8%       ...
3     51021  0      18000  ...  10%      ...
$
```

**Parsing:**
- Jede Zeile = eine Batterie
- Felder sind whitespace-separated
- **SOC:** Feld mit "%" (z.B. "11%")
- **Ende:** "$" oder Leerzeile

### Berechnung

```c
// Für jede Batterie:
parse_line() → extract SOC (z.B. "11%") → 11

// Average:
avg_soc = (11 + 8 + 10) / 3 = 9.67% ≈ 10%
```

---

## 📡 MQTT Topics

### Neue Topics:

| Topic | Direction | Type | Interval | Beschreibung |
|-------|-----------|------|----------|--------------|
| **`pico/ebox/soc`** | ← Pico W | `int` | 30s | Durchschnittlicher SOC (0-100%) |

### Bestehende Topics (erweitert):

**`pico/status`** (JSON, alle 5s):
```json
{
  "soyo_watts": 1500,
  "relay_state": 3,
  "relay_bits": "0x03",
  "ebox_soc": 10,
  "ebox_batteries": 3,
  "errors": "0x00"
}
```

---

## 🚀 Setup

### 1. Hardware

**Kaufen:**
- **MAX3232 Modul** (~1€)
- **RS232 Kabel** (Female D-Sub 9-Pin) oder Dupont Wires

**Verdrahten:**
```
Pico W GPIO 8 → MAX3232 T1IN
Pico W GPIO 9 → MAX3232 R1OUT
Pico W 3.3V   → MAX3232 VCC
Pico W GND    → MAX3232 GND

MAX3232 TX → EBox RX
MAX3232 RX → EBox TX
MAX3232 GND → EBox GND
```

### 2. Software

**Edit WiFi Credentials:**
```c
// In pico_w_ebox_mqtt.c Zeile 42-44:
#define WIFI_SSID       "DeinWiFi"
#define WIFI_PASSWORD   "DeinPassword"
#define MQTT_BROKER_IP  "192.168.1.100"
```

**Build:**
```bash
export PICO_SDK_PATH=$HOME/pico-sdk
mkdir build && cd build
cmake -DPICO_BOARD=pico_w ..
make -j4
```

**Flash:**
```
BOOTSEL gedrückt halten → USB → pico_w_ebox_mqtt.uf2 kopieren
```

### 3. Testen

**Monitor MQTT:**
```bash
mosquitto_sub -h 192.168.1.100 -t "pico/ebox/#" -v
```

**Output (alle 30s):**
```
pico/ebox/soc 10
```

**Full Status:**
```bash
mosquitto_sub -h 192.168.1.100 -t "pico/status" -v
```

```json
{"soyo_watts":1500,"relay_state":3,"relay_bits":"0x03","ebox_soc":10,"ebox_batteries":3,"errors":"0x00"}
```

---

## 🔧 Software UART Implementation

**Warum Software UART?**
- Pico hat nur **2 Hardware UARTs** (UART0 + UART1)
- Beide schon belegt (SOYO + Relay)
- **Software UART** (Bit-Bang) für EBox

**Implementierung:**
```c
// TX: Bit-Bang mit sleep_us()
static void ebox_uart_putc(uint tx_pin, uint baud, char c) {
    uint32_t bit_time_us = 1000000 / baud;

    gpio_put(tx_pin, 0);  // Start bit
    sleep_us(bit_time_us);

    for (int i = 0; i < 8; i++) {
        gpio_put(tx_pin, (c >> i) & 1);
        sleep_us(bit_time_us);
    }

    gpio_put(tx_pin, 1);  // Stop bit
    sleep_us(bit_time_us);
}

// RX: Polling mit Timeout
static int ebox_uart_getc_timeout(uint rx_pin, uint baud, uint timeout_ms) {
    // Wait for start bit (falling edge)
    // Read 8 data bits
    // Wait for stop bit
}
```

**Limitations:**
- ⚠️ **Blocking:** sleep_us() blocks Core 0
- ⚠️ **Timing:** Nicht exakt (Clock-Jitter)
- ⚠️ **Max Baud:** ~57600 (115200 funktioniert meist)

**Für Production:**
Nutze **PIO UART** (Pico SDK Examples):
- Hardware-timed (kein Jitter)
- Non-blocking
- Bis zu 8 zusätzliche UARTs!

**Example:**
```c
// https://github.com/raspberrypi/pico-examples/tree/master/pio/uart_rx
#include "uart_rx.pio.h"

PIO pio = pio0;
uint sm = pio_claim_unused_sm(pio, true);
uint offset = pio_add_program(pio, &uart_rx_program);
uart_rx_program_init(pio, sm, offset, EBOX_RX_PIN, EBOX_BAUD);
```

---

## ⏱️ Timing

| Funktion | Interval | Core | Blockiert |
|----------|----------|------|-----------|
| **SOYO** | 3s | Core 0 | Nein (ISR) |
| **Relay** | 100ms | Core 1 | Nein (Event) |
| **EBox** | **30s** | Core 0 | **Ja** (~200ms) |
| **MQTT** | 5s | Core 0 | Nein |

**⚠️ Wichtig:** EBox Read blockiert Core 0 für ~200ms alle 30s!
- SOYO Timer läuft weiter (separate ISR)
- WiFi/MQTT läuft weiter (lwIP background)
- Kein Problem in Praxis

---

## 🐛 Troubleshooting

### EBox Timeout

**Error:** `ERR_EBOX_TIMEOUT` (Bit 6 in error_flags)

**Ursachen:**
- ✓ MAX3232 Verkabelung korrekt?
- ✓ TX/RX vertauscht?
- ✓ EBox eingeschaltet?
- ✓ Baud Rate korrekt (115200)?
- ✓ RS232 Kabel OK?

**Debug:**
```bash
# USB Serial Console
screen /dev/ttyACM0 115200

# Output sollte zeigen:
EBox: Battery 1 SOC = 11%
EBox: Battery 2 SOC = 8%
EBox: Battery 3 SOC = 10%
EBox: Average SOC = 10% (3 batteries)
```

### Garbled Data

**Symptom:** Falsche Zeichen empfangen

**Lösung:**
- ✓ Baud Rate stimmt?
- ✓ Ground connected?
- ✓ MAX3232 Kapazitoren OK? (manche Module brauchen externe Caps!)

### SOC = -1

**Bedeutung:** Noch kein EBox Read erfolgt (startet nach 30s)

**Lösung:** Warten oder EBox Cycle auf 5s reduzieren für Testing:
```c
#define EBOX_CYCLE_MS 5000  // Test: 5 seconds
```

---

## 📊 Performance

| Metrik | Wert |
|--------|------|
| **EBox Read Time** | ~200ms (für 3 Batterien) |
| **MQTT Latenz** | +200ms alle 30s (tolerierbar) |
| **Accuracy** | ±1% (abhängig von EBox) |

---

## 🔬 Erweiterungen

### 1. PIO UART (empfohlen für Production)

**Vorteile:**
- ✅ Non-blocking
- ✅ Hardware-timed
- ✅ DMA support

**Implementation:**
```c
#include "hardware/pio.h"
#include "uart_rx.pio.h"

// Initialize PIO UART
PIO pio = pio0;
uint sm = 0;
uint offset = pio_add_program(pio, &uart_rx_program);
uart_rx_program_init(pio, sm, offset, EBOX_RX_PIN, EBOX_BAUD);

// Read char (non-blocking)
if (!pio_sm_is_rx_fifo_empty(pio, sm)) {
    char c = pio_sm_get(pio, sm) >> 24;
}
```

### 2. Per-Battery SOC Publishing

**Aktuell:** Nur Average SOC

**Erweiterung:** Publish jede Batterie einzeln
```c
mqtt_publish(mqtt_client, "pico/ebox/battery/1/soc", "11", ...);
mqtt_publish(mqtt_client, "pico/ebox/battery/2/soc", "8", ...);
mqtt_publish(mqtt_client, "pico/ebox/battery/3/soc", "10", ...);
```

### 3. Andere EBox Daten

**Aktuell:** Nur SOC

**Erweiterbar:**
- **Voltage** (Field 1)
- **Current** (Field 2)
- **Temperature** (Field 3)
- **Cycle Count**

```c
mqtt_publish(mqtt_client, "pico/ebox/voltage", "51018", ...);
mqtt_publish(mqtt_client, "pico/ebox/current", "0", ...);
mqtt_publish(mqtt_client, "pico/ebox/temperature", "19", ...);
```

---

## 📝 Vergleich

| Feature | pico_w_mqtt_modbus.c | pico_w_ebox_mqtt.c |
|---------|----------------------|--------------------|
| **SOYO** | ✅ | ✅ |
| **Relay** | ✅ | ✅ |
| **EBox** | ❌ | ✅ 30s interval |
| **UARTs** | 2 (Hardware) | 2 HW + 1 SW |
| **MQTT Topics** | 2 | 3 (+ ebox/soc) |

---

**Mit EBox hast du jetzt komplette Überwachung: Inverter + Relay + Batterien!** 🔋⚡
