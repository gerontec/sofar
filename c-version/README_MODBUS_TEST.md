# Modbus Relay Testing - Python vs C Performance Comparison

## 📋 Overview

This test suite compares the performance of Python (pymodbus) vs C (libmodbus) for controlling the Ebyte Relay module via Modbus RTU.

## 🔧 Requirements

### Install libmodbus

```bash
# Debian/Raspbian
sudo apt-get install libmodbus-dev

# Verify installation
pkg-config --modversion libmodbus
```

### Existing Python dependencies
- pymodbus (already installed)

## 🏗️ Build

```bash
cd /home/user/sofar/c-version

# Build the C test tool
make test-modbus

# This creates: test_modbus_relay
```

## 🧪 Usage

### 1. Basic Commands

```bash
# Show help
sudo ./test_modbus_relay --help

# Show current status
sudo ./test_modbus_relay --status

# Set relay state (0-7, 11)
sudo ./test_modbus_relay 7    # All relays ON
sudo ./test_modbus_relay 0    # All relays OFF
sudo ./test_modbus_relay 5    # Custom pattern

# Read current state
sudo ./test_modbus_relay --read

# Run built-in benchmark
sudo ./test_modbus_relay --benchmark
```

### 2. Python Comparison Test

```bash
# Make benchmark script executable
chmod +x benchmark_modbus_comparison.sh

# Run full comparison (Python vs C)
sudo ./benchmark_modbus_comparison.sh
```

**This benchmark will:**
- Test multiple relay states (0, 7, 5, 3, 0)
- Run 10 iterations per state
- Measure timing for both Python and C
- Calculate speedup factor
- Analyze multiplexing overhead

### 3. Quick Single Test

```bash
# Python timing
time python3 ../ebyteserrequest.py 7

# C timing
time sudo ./test_modbus_relay 7
```

## 📊 Expected Results

### Python Performance
- **Average write time:** 50-80 ms
- **Overhead:** Interpreter startup, library imports
- **Pros:** Easy to modify, readable
- **Cons:** Slower, higher latency

### C Performance
- **Average write time:** 5-15 ms
- **Overhead:** Minimal, compiled binary
- **Pros:** Fast, low latency, predictable
- **Cons:** Requires compilation, less flexible

### Speedup Factor
**Expected:** C is **3-10x faster** than Python

## 🔄 Multiplexing Analysis

For integrating Modbus relay control into the Soyo 2-second loop:

```
2000 ms slot:
  - Soyo command @ 4800:     ~17 ms
  - Baudrate switch:         ~5 ms
  - Modbus write @ 9600:     ~10 ms (C) or ~60 ms (Python)
  - Baudrate switch back:    ~5 ms
  ----------------------------------------
  Total overhead:            ~37 ms (C) or ~87 ms (Python)
  Percentage:                1.85% (C) or 4.35% (Python)
```

**Conclusion:** Both are feasible, but C is more efficient.

## 🎯 Integration Example

### Option 1: Separate Ports (Recommended)

```c
// No multiplexing needed!
int fd_soyo = serial_open("/dev/ttyUSB0", 4800);
int fd_relay = serial_open("/dev/ttyAMA0", 9600);

while (running) {
    set_soyo_power(fd_soyo, power);

    if (relay_state_changed) {
        modbus_write_relay(fd_relay, new_state);
    }

    sleep(2);
}
```

### Option 2: Same Port with Multiplexing

```c
int fd = serial_open("/dev/ttyUSB0", 4800);

while (running) {
    // Soyo @ 4800
    set_baudrate(fd, B4800);
    set_soyo_power(fd, power);

    // Relay @ 9600
    set_baudrate(fd, B9600);
    modbus_write_relay(fd, relay_state);

    // Back to default
    set_baudrate(fd, B4800);

    sleep(2);
}
```

## 📝 State Mapping

| State | Relay 1 | Relay 2 | Relay 3 | Description |
|-------|---------|---------|---------|-------------|
| 0     | OFF     | OFF     | OFF     | All OFF     |
| 1     | OFF     | OFF     | ON      | R3 only     |
| 2     | OFF     | ON      | OFF     | R2 only     |
| 3     | OFF     | ON      | ON      | R2+R3       |
| 4     | ON      | OFF     | OFF     | R1 only     |
| 5     | ON      | OFF     | ON      | R1+R3       |
| 6     | ON      | ON      | OFF     | R1+R2       |
| 7     | ON      | ON      | ON      | All ON      |
| 11    | ON      | ON      | ON      | Alias for 7 |

## 🐛 Troubleshooting

### Permission Denied
```bash
sudo usermod -a -G dialout $USER
# Logout and login again
```

### Port Busy
```bash
# Check what's using the port
sudo lsof /dev/ttyAMA0

# Kill the process if needed
sudo systemctl stop <service>
```

### Modbus Read Fails
Some Ebyte modules only support **write operations**, not read. This is normal - use the state file instead.

## 📚 References

- [libmodbus documentation](https://libmodbus.org/)
- [Modbus RTU Protocol](https://www.modbus.org/docs/Modbus_Application_Protocol_V1_1b3.pdf)
- [pymodbus documentation](https://pymodbus.readthedocs.io/)
