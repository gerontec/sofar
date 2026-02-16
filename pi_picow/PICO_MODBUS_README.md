# Raspberry Pi Pico Modbus Dual-Core Controller

**Bare-metal dual-core Modbus RTU controller** for SOYO inverters and relay boards.

---

## 🏗️ Architecture

```
┌───────────────────────────────────────────┐
│         SHARED PUBLIC RAM                 │
│  soyo_target_watts   → uint16_t           │
│  soyo_enable         → bool               │
│  relay_target_state  → uint8_t (0-7,11)   │
│  relay_needs_update  → bool (trigger)     │
│  relay_status_request→ bool (trigger)     │
├───────────────────────────────────────────┤
│  relay_current_state ← uint8_t (0-7,11)   │
│  relay_current_bits  ← uint8_t (R1/R2/R3) │
│  soyo_last_watts     ← int16_t (status)   │
│  soyo_cycle_count    ← uint32_t           │
│  relay_cycle_count   ← uint32_t           │
│  error_flags         ← uint8_t            │
└───────────────────────────────────────────┘
         ↑                    ↑
    ┌────┴─────┐         ┌────┴─────┐
    │ CORE 0   │         │ CORE 1   │
    │ SOYO     │         │ RELAY    │
    │ UART0    │         │ UART1    │
    │ 4800 bd  │         │ 9600 bd  │
    │ ISR 3s   │         │ event    │
    │ Priority 1│         │ Priority 2│
    └──────────┘         └──────────┘
```

---

## 📌 Hardware Connections

| Function | UART | TX Pin | RX Pin | Baud | Notes |
|----------|------|--------|--------|------|-------|
| **SOYO** | UART0 | GP0 | GP1 | 4800 | RS485 adapter required |
| **Relay** | UART1 | GP4 | GP5 | 9600 | Modbus RTU relay board |
| **LED** | - | GP25 | - | - | Onboard LED (status) |
| **USB** | - | - | - | - | Serial console + power |

**RS485 Wiring:**
- Connect a **MAX485/MAX3485** module to each UART
- SOYO: DE/RE → GP2, DI → GP0, RO → GP1
- Relay: DE/RE → GP6, DI → GP4, RO → GP5

---

## 🔧 Building

### Prerequisites
```bash
# Install Pico SDK
cd ~
git clone https://github.com/raspberrypi/pico-sdk.git
cd pico-sdk
git submodule update --init
export PICO_SDK_PATH=$HOME/pico-sdk

# Install toolchain (Ubuntu/Debian)
sudo apt install cmake gcc-arm-none-eabi libnewlib-arm-none-eabi build-essential
```

### Build Process
```bash
cd /path/to/sofar

# Rename CMakeLists.txt
mv pico_modbus_dual_CMakeLists.txt CMakeLists.txt

# Create build directory
mkdir build && cd build

# Configure
cmake ..

# Build
make -j4

# Result: build/pico_modbus_dual.uf2
```

### Flashing
1. Hold **BOOTSEL** button on Pico
2. Connect USB (appears as mass storage device)
3. Copy `pico_modbus_dual.uf2` to the drive
4. Pico reboots automatically

---

## 🎮 Usage

### Serial Console Commands

Connect via USB serial (115200 baud):
```bash
# Linux/Mac
screen /dev/ttyACM0 115200

# Windows
putty COM3 -serial -sercfg 115200,8,n,1,N
```

**Commands:**
```
s <watts>     - Set SOYO target watts (0-65535)
r <state>     - Set relay state (0-7, 11)
q             - Query status
e [0|1]       - Enable/disable SOYO
```

**Example Session:**
```
> s 1500        # Set SOYO to 1500W
SOYO target: 1500 W

> r 3           # Set relay state 3 (R1+R2 ON)
Relay target: 3

> q             # Query status
--- Status ---
SOYO:  1500 W (cycles: 42, errors: 0x00)
Relay: state 3, bits 0x03 [R1=1 R2=1 R3=0] (cycles: 1, errors: 0x00)

> e 0           # Disable SOYO
SOYO disabled
```

---

## 📡 Public RAM API

### External Controller Integration

The `PublicRAM` structure provides a **memory-mapped interface** for external control:

```c
typedef struct {
    // INPUTS (write these to control)
    volatile uint16_t soyo_target_watts;    // 0-65535 watts
    volatile bool     soyo_enable;          // true = active
    volatile uint8_t  relay_target_state;   // 0-7, 11
    volatile bool     relay_needs_update;   // set true to trigger
    volatile bool     relay_status_request; // set true to read

    // OUTPUTS (read these for status)
    volatile uint8_t  relay_current_state;  // Last confirmed state (0-7, 11)
    volatile uint8_t  relay_current_bits;   // Raw 3-bit pattern (bit0=R1, bit1=R2, bit2=R3)
    volatile int16_t  soyo_last_watts;      // Last sent value
    volatile uint32_t soyo_cycle_count;     // Heartbeat (increments every 3s)
    volatile uint32_t relay_cycle_count;    // Event counter
    volatile uint8_t  error_flags;          // See error bits below

    mutex_t mutex;  // Use for thread-safe access
} PublicRAM;

extern PublicRAM g_public_ram;  // Global instance
```

### Error Flags
```c
#define ERR_SOYO_TIMEOUT    (1 << 0)  // SOYO no response
#define ERR_SOYO_CRC        (1 << 1)  // SOYO CRC error
#define ERR_RELAY_TIMEOUT   (1 << 2)  // Relay no response
#define ERR_RELAY_CRC       (1 << 3)  // Relay CRC error
```

### Example: External Control
```c
// Set SOYO to 2000W
g_public_ram.soyo_target_watts = 2000;
g_public_ram.soyo_enable = true;
// SOYO command sent within 3 seconds by timer ISR

// Change relay to state 5 (R1+R3 ON)
g_public_ram.relay_target_state = 5;
g_public_ram.relay_needs_update = true;
// Relay updated within 100ms by Core 1

// Read status
if (g_public_ram.error_flags == 0) {
    printf("All OK, SOYO sent %d W\n", g_public_ram.soyo_last_watts);
    printf("Relay state: %d (bits 0x%02X)\n",
           g_public_ram.relay_current_state,
           g_public_ram.relay_current_bits);
}

// Check individual relay states
bool r1_on = (g_public_ram.relay_current_bits & 0x01) != 0;
bool r2_on = (g_public_ram.relay_current_bits & 0x02) != 0;
bool r3_on = (g_public_ram.relay_current_bits & 0x04) != 0;
```

---

## 🔄 Timing Behavior

### Core 0 - SOYO (Priority 1)
- **Cycle:** Fixed 3 seconds (timer interrupt)
- **Trigger:** Automatic (periodic)
- **Latency:** Max 3s from value change to command sent

### Core 1 - Relay (Priority 2)
- **Cycle:** Event-driven (100ms polling)
- **Trigger:** `relay_needs_update = true`
- **Latency:** Max 100ms from flag set to Modbus write
- **Optimization:** Only writes if state actually changed

**Status Reads:**
- Set `relay_status_request = true` to force a status read
- Result updated in `relay_current_state` within 100ms

---

## 🧪 Testing

### Bench Test (No Hardware)
```bash
# Build with mock UART
cmake -DMOCK_UART=1 ..
make
./pico_modbus_dual  # Runs simulator
```

### Live Test Checklist
1. ✅ SOYO responds to watt changes
2. ✅ Relay toggles match state commands
3. ✅ LED blinks every 3s (SOYO heartbeat)
4. ✅ `error_flags` stays 0x00
5. ✅ Status query returns correct values

---

## 📊 Relay State Mapping

| State | R1 | R2 | R3 | Description |
|-------|----|----|----|----|
| 0 | OFF | OFF | OFF | All off |
| 1 | ON | OFF | OFF | Only R1 |
| 2 | OFF | ON | OFF | Only R2 |
| 3 | ON | ON | OFF | R1 + R2 |
| 4 | OFF | OFF | ON | Only R3 |
| 5 | ON | OFF | ON | R1 + R3 |
| 6 | OFF | ON | ON | R2 + R3 |
| 7 | ON | ON | ON | All on |
| 11 | OFF | OFF | OFF | Special "state 11" |

---

## 🐛 Troubleshooting

### SOYO Errors
- **Timeout:** Check RS485 wiring, TX/RX swap?
- **CRC:** Verify baud rate (must be 4800)
- **No response:** Check device ID (`SOYO_DEVICE_ID`)

### Relay Errors
- **Timeout:** Check RS485 wiring
- **CRC:** Verify baud rate (must be 9600)
- **Wrong state:** Check slave ID (`RELAY_SLAVE_ID`)

### Build Errors
- **`PICO_SDK_PATH not set`:** Run `export PICO_SDK_PATH=...`
- **CMake errors:** Update to CMake 3.13+
- **Linker errors:** Run `git submodule update --init` in SDK

---

## 📝 Migration from Linux Version

| Linux (test_modbus_relay.c) | Pico (this version) |
|-----------------------------|---------------------|
| `libmodbus` library | Raw Modbus RTU implementation |
| Single UART + baud switching | Dual UART (no switching) |
| File-based state (`/run/user/...`) | Shared RAM (`g_public_ram`) |
| Command-line args | USB serial commands |
| pthread timers | Hardware timers + multicore |

**Key Advantages:**
- ✅ **No baud switching delays** (parallel UARTs)
- ✅ **Lower latency** (100ms vs 500ms for relays)
- ✅ **True concurrency** (dual-core)
- ✅ **No OS overhead** (bare-metal)
- ✅ **Deterministic timing** (hardware timers)

---

## 📜 License

Same as parent project (check repository root).

---

## 🤝 Contributing

1. Test on real hardware before committing
2. Keep SOYO cycle at 3s (inverter requirement)
3. Maintain state map compatibility (Python/C)
4. Document any Public RAM changes

**Built for production solar control systems.** 🔋☀️
