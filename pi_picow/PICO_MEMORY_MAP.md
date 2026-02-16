# Pico Modbus Public RAM - Memory Map

**Speicheradressen für externes Steuerprogramm**

---

## 📍 Memory Layout

Das `g_public_ram` Struct hat folgende Memory-Organisation:

```c
// Base Address: &g_public_ram (zur Laufzeit bestimmt)
typedef struct {
    volatile uint16_t soyo_target_watts;    // Offset  0 (2 bytes)
    volatile bool     soyo_enable;          // Offset  2 (1 byte)
    uint8_t           _pad1;                // Offset  3 (padding)
    volatile uint8_t  relay_target_state;   // Offset  4 (1 byte)
    volatile bool     relay_needs_update;   // Offset  5 (1 byte)
    volatile bool     relay_status_request; // Offset  6 (1 byte)
    uint8_t           _pad2;                // Offset  7 (padding)
    volatile uint8_t  relay_current_state;  // Offset  8 (1 byte)
    volatile uint8_t  relay_current_bits;   // Offset  9 (1 byte)
    uint16_t          _pad3;                // Offset 10 (padding)
    volatile int16_t  soyo_last_watts;      // Offset 12 (2 bytes)
    uint16_t          _pad4;                // Offset 14 (padding)
    volatile uint32_t soyo_cycle_count;     // Offset 16 (4 bytes)
    volatile uint32_t relay_cycle_count;    // Offset 20 (4 bytes)
    volatile uint8_t  error_flags;          // Offset 24 (1 byte)
    // mutex_t at end (size varies by platform)
} PublicRAM;  // Total: ~28+ bytes (platform dependent)
```

**⚠️ WICHTIG:** Padding kann je nach Compiler-Flags variieren!

---

## ✍️ WRITE Interface (Steuerung → Pico)

**Diese Speicherpositionen MUSS das Steuerprogramm beschreiben:**

| Offset | Variable | Typ | Bytes | Beschreibung | Werte |
|--------|----------|-----|-------|--------------|-------|
| **0** | `soyo_target_watts` | `uint16_t` | 2 | SOYO Ziel-Leistung in Watt | 0-65535 |
| **2** | `soyo_enable` | `bool` | 1 | SOYO aktivieren/deaktivieren | 0=off, 1=on |
| **4** | `relay_target_state` | `uint8_t` | 1 | Relais Ziel-Zustand | 0-7, 11 |
| **5** | `relay_needs_update` | `bool` | 1 | **TRIGGER:** Relais-Update anfordern | 0→1 Flanke |
| **6** | `relay_status_request` | `bool` | 1 | **TRIGGER:** Status-Read anfordern | 0→1 Flanke |

### Beispiel-Code (C):
```c
// Setze SOYO auf 1500W
*(uint16_t*)(&g_public_ram + 0) = 1500;
*(bool*)(&g_public_ram + 2) = true;

// Setze Relais auf State 3
*(uint8_t*)(&g_public_ram + 4) = 3;
*(bool*)(&g_public_ram + 5) = true;  // TRIGGER!
```

**ODER besser (typsicher):**
```c
g_public_ram.soyo_target_watts = 1500;
g_public_ram.soyo_enable = true;

g_public_ram.relay_target_state = 3;
g_public_ram.relay_needs_update = true;  // TRIGGER!
```

---

## 📖 READ Interface (Pico → Status)

**Diese Speicherpositionen KANN das Steuerprogramm lesen:**

| Offset | Variable | Typ | Bytes | Beschreibung | Werte |
|--------|----------|-----|-------|--------------|-------|
| **8** | `relay_current_state` | `uint8_t` | 1 | Aktueller Relais-Zustand | 0-7, 11, 0xFF=unknown |
| **9** | `relay_current_bits` | `uint8_t` | 1 | **RAW Relais-Bits** | bit0=R1, bit1=R2, bit2=R3 |
| **12** | `soyo_last_watts` | `int16_t` | 2 | Zuletzt gesendeter SOYO-Wert | -1=none, sonst Watt |
| **16** | `soyo_cycle_count` | `uint32_t` | 4 | SOYO Heartbeat (3s Takt) | Zähler |
| **20** | `relay_cycle_count` | `uint32_t` | 4 | Relais Event-Counter | Zähler |
| **24** | `error_flags` | `uint8_t` | 1 | **Fehlerstatus** | Siehe unten |

### Beispiel-Code (C):
```c
// Prüfe Relais-Status
uint8_t state = *(uint8_t*)(&g_public_ram + 8);
uint8_t bits = *(uint8_t*)(&g_public_ram + 9);

bool r1_on = bits & 0x01;
bool r2_on = bits & 0x02;
bool r3_on = bits & 0x04;

// Prüfe Fehler
uint8_t errors = *(uint8_t*)(&g_public_ram + 24);
if (errors != 0) {
    printf("ERROR: 0x%02X\n", errors);
}
```

---

## 🚨 Error Flags (Offset 24)

| Bit | Flag | Bedeutung |
|-----|------|-----------|
| 0 | `ERR_SOYO_TIMEOUT` | SOYO antwortet nicht (>500ms) |
| 1 | `ERR_SOYO_CRC` | SOYO CRC-Fehler |
| 2 | `ERR_RELAY_TIMEOUT` | Relais antwortet nicht (>1000ms) |
| 3 | `ERR_RELAY_CRC` | Relais CRC-Fehler |

---

## 🔄 Kommunikations-Protokoll

### 1. SOYO Steuerung (automatisch, 3s Zyklus)
```c
// Schritt 1: Wert setzen
g_public_ram.soyo_target_watts = 2000;
g_public_ram.soyo_enable = true;

// Schritt 2: Warten (max 3 Sekunden)
// Timer-ISR sendet automatisch!

// Schritt 3: Status prüfen
if (g_public_ram.soyo_last_watts == 2000) {
    // Erfolgreich gesendet
}
```

### 2. Relais Steuerung (event-driven, <100ms)
```c
// Schritt 1: Wert + Trigger setzen
g_public_ram.relay_target_state = 5;
g_public_ram.relay_needs_update = true;

// Schritt 2: Warten auf Completion
while (g_public_ram.relay_needs_update) {
    sleep_ms(10);  // Core 1 cleared das Flag!
}

// Schritt 3: Verify
if (g_public_ram.relay_current_state == 5) {
    // Erfolgreich!
}
```

### 3. Status-Read anfordern
```c
// Trigger Status-Read
g_public_ram.relay_status_request = true;

// Warten
while (g_public_ram.relay_status_request) {
    sleep_ms(10);
}

// Lesen
uint8_t bits = g_public_ram.relay_current_bits;
```

---

## 🔌 Integration mit Raspberry Pi

**PROBLEM:** Pico und Raspberry Pi haben **separate Memory-Spaces!**

### Lösungen:

#### **Option A: Serial Protocol (EMPFOHLEN)**
Nutze USB Serial Console (bereits implementiert):
```bash
# Von Raspberry Pi
echo "s 1500" > /dev/ttyACM0  # SOYO 1500W
echo "r 3" > /dev/ttyACM0     # Relais State 3
echo "q" > /dev/ttyACM0       # Status Query
```

**Python Wrapper:**
```python
import serial

class PicoModbus:
    def __init__(self, port='/dev/ttyACM0'):
        self.ser = serial.Serial(port, 115200, timeout=1)

    def set_soyo(self, watts):
        self.ser.write(f"s {watts}\n".encode())

    def set_relay(self, state):
        self.ser.write(f"r {state}\n".encode())

    def get_status(self):
        self.ser.write(b"q\n")
        return self.ser.read(256).decode()
```

#### **Option B: Shared Memory über UART (Custom Protocol)**
Implementiere ein Binary Protocol:
```c
// Auf Raspberry Pi
struct PicoCommand {
    uint8_t cmd;  // 0x01=SOYO, 0x02=Relay, 0x03=Status
    uint16_t value;
} __attribute__((packed));

// Send
PicoCommand cmd = {0x01, 1500};
write(uart_fd, &cmd, sizeof(cmd));
```

**Pico muss erweitert werden:**
```c
// In pico_modbus_dual.c
void handle_binary_command(uint8_t* buf) {
    switch(buf[0]) {
        case 0x01:  // SOYO
            g_public_ram.soyo_target_watts = (buf[1] << 8) | buf[2];
            break;
        case 0x02:  // Relay
            g_public_ram.relay_target_state = buf[1];
            g_public_ram.relay_needs_update = true;
            break;
    }
}
```

#### **Option C: I2C/SPI Slave Mode**
Pico als I2C Slave, mapped Public RAM auf I2C Register:
```c
// I2C Address Map:
// 0x00-0x01: soyo_target_watts (r/w)
// 0x02:      soyo_enable (r/w)
// 0x04:      relay_target_state (r/w)
// 0x08:      relay_current_state (r)
// 0x09:      relay_current_bits (r)
```

---

## 🎯 Empfehlung für Integration

**Für `fox2db.c` / `soyo1min.c`:**

1. **Verwende Serial Protocol** (einfachste Integration)
2. **Erstelle Wrapper-Funktionen:**

```c
// In einer neuen Datei: pico_interface.c
#include <fcntl.h>
#include <termios.h>

int pico_fd = -1;

int pico_init(const char* port) {
    pico_fd = open(port, O_RDWR | O_NOCTTY);
    struct termios tty;
    tcgetattr(pico_fd, &tty);
    cfsetspeed(&tty, B115200);
    tcsetattr(pico_fd, TCSANOW, &tty);
    return pico_fd;
}

void pico_set_soyo(uint16_t watts) {
    char cmd[32];
    snprintf(cmd, sizeof(cmd), "s %u\n", watts);
    write(pico_fd, cmd, strlen(cmd));
}

void pico_set_relay(uint8_t state) {
    char cmd[32];
    snprintf(cmd, sizeof(cmd), "r %u\n", state);
    write(pico_fd, cmd, strlen(cmd));
}
```

3. **In `fox2db.c` nutzen:**
```c
#include "pico_interface.h"

int main() {
    pico_init("/dev/ttyACM0");

    // Statt direkt Modbus:
    pico_set_soyo(target_watts);
    pico_set_relay(relay_state);
}
```

---

## 📏 Größe des Public RAM

```c
sizeof(PublicRAM) ≈ 32 bytes (ohne Mutex)
```

**Alignment:**
- ARM Cortex-M0+ bevorzugt 4-byte alignment
- Nutze `__attribute__((packed))` um Padding zu vermeiden

---

**Fragen?** Soll ich ein Binary Protocol für UART implementieren?
