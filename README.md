# Sofar Inverter Management System

[🇩🇪 Deutsche Version](README.de.md)

Battery management and monitoring system for Sofar inverters with comprehensive Modbus RTU integration.

## Overview

This project provides tools for managing and monitoring Sofar solar inverters:

- **Battery Management** (`soyo1min`) - Intelligent battery discharge control based on grid status, SOC, time of day, and heat pump integration
- **Register Reader** (`read_inverter`) - Comprehensive Modbus RTU register reader with CSV export

## Features

### Battery Management System (soyo1min)
- Real-time battery discharge control
- Grid status monitoring (feeding/buying)
- SOC-based protection (prevents over-discharge)
- Sunrise/sunset integration
- MQTT heat pump data integration
- Manual power override via file
- Priority-based decision system

### Register Reader (read_inverter)
- Reads Modbus RTU registers from Sofar inverter
- CSV-based register definitions (embedded at build time)
- Multiple data types: U16, I16, U32, I32, U64, BCD16, ASCII
- Filter by unit (kW/kWh/%) or read all registers
- **Full command-line configuration** (NEW!)
- CSV export of readings
- Production-ready with embedded register definitions

## Quick Start

```bash
# Install dependencies
sudo apt-get update
sudo apt-get install -y build-essential libmosquitto-dev libmodbus-dev python3 python3-pip

# Install Python dependencies
pip3 install astral pytz

# Build
cd c-version
make

# Run battery management
./soyo1min

# Read inverter registers with custom options
cd read-inverter
./read_inverter --help
./read_inverter --port /dev/ttyUSB0 --all-registers
```

## Documentation

- [C Version README](c-version/README.md) - Battery management system details
- [Installation Guide](c-version/INSTALL.md) - Detailed installation instructions
- [Register Reader](c-version/read-inverter/README.md) - Inverter register reader with CLI options

## Requirements

### Hardware
- Serial port (e.g., `/dev/ttyUSB32` or `/dev/ttyUSB0`)
- MQTT broker (for heat pump integration)
- Sofar inverter with Modbus RTU

### Software
- Linux (Raspberry Pi OS, Debian, Ubuntu, etc.)
- GCC compiler
- libmosquitto-dev (for MQTT)
- libmodbus-dev (for Modbus RTU)
- Python 3 with astral and pytz (for sunrise calculation)

### Permissions
User must be in the `dialout` group:
```bash
sudo usermod -a -G dialout $USER
# Logout and login again
```

## Architecture

The system consists of two main C programs:

### 1. soyo1min - Battery Management
Located in `c-version/`

**Decision Priority (highest to lowest):**
1. Discharge Protection: SOC < 9% → Power = 0W
2. Battery Charging: Battery charging (>2A) → Power = 0W
3. Manual Override: Power from `soyopower.txt`
4. Grid Feeding: PV surplus (Grid > 200W) → Power = 0W
5. Grid Buying: Drawing from grid (Grid < -100W) → Compensate with battery
6. Default: Base load support

**Key Files:**
- `config.h` - All configuration parameters
- `soyo1min.c` - Main program logic
- `sunrise.py` - Python script for sunrise/sunset calculation

### 2. read_inverter - Register Reader
Located in `c-version/read-inverter/`

**Features:**
- Embedded register definitions (no external CSV needed at runtime)
- Full command-line configuration support
- Mask-based register filtering for efficient reading
- Multiple output formats

**Key Files:**
- `read_config.h` - Default configuration values
- `read_inverter.c` - Main program with CLI parsing
- `sofarregister.csv` - Register definitions (embedded at build time)

## Configuration

### Battery Management (soyo1min)
Edit `c-version/config.h`:
```c
#define SERIAL_PORT "/dev/ttyUSB32"      // Serial port
#define MQTT_BROKER "192.168.178.218"    // MQTT broker IP
#define MQTT_TOPIC "em0/54"              // Heat pump power topic
#define BATTERY_CAPACITY_KWH 30          // Battery capacity
#define BAT2_SOC_MIN 9                   // Minimum SOC (%)
```

### Register Reader (read_inverter)
**NEW: Command-line configuration!** No recompilation needed:

```bash
# Serial communication
./read_inverter --port /dev/ttyUSB0 --baud 9600 --unit-id 1

# File paths
./read_inverter --csv custom_registers.csv --output /tmp/data.csv

# Register options
./read_inverter --all-registers  # Read all registers
./read_inverter --filter         # Only kW/kWh/% (default)

# Advanced options
./read_inverter --max-register 0x3000 --block-size 64
```

Or edit `c-version/read-inverter/read_config.h` for default values.

## Building

```bash
cd c-version

# Check system requirements (optional)
./configure

# Build all programs
make

# Build with debug symbols
make debug

# Install to /usr/local/bin
sudo make install
```

## Logging

- **soyo1min**: `/run/user/1000/soyo1min_c.log`
- **read_inverter**: stdout/stderr

Log levels: DEBUG, INFO, WARNING, ERROR

## Systemd Service

Example service for battery management:

```bash
# Install binary
sudo make install

# Create service file
sudo nano /etc/systemd/system/soyo1min.service
```

```ini
[Unit]
Description=Sofar Battery Management
After=network.target

[Service]
Type=simple
User=pi
WorkingDirectory=/home/pi/sofar
ExecStart=/usr/local/bin/soyo1min
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable soyo1min
sudo systemctl start soyo1min

# Check status
sudo systemctl status soyo1min
```

## Troubleshooting

### Serial Port Access
```bash
# Check permissions
ls -l /dev/ttyUSB32

# Add user to dialout group
sudo usermod -a -G dialout $USER
# Logout and login required!
```

### MQTT Connection Issues
```bash
# Test MQTT broker
mosquitto_sub -h 192.168.178.218 -t em0/54 -v
```

### Lock File Issues
```bash
# Check if another instance is running
ps aux | grep soyo1min

# Remove lock file (only if no instance is running)
rm /run/user/1000/soyo1min.lock
```

### Build Errors
```bash
# Install missing dependencies
sudo apt-get install -y build-essential libmosquitto-dev libmodbus-dev

# Clean and rebuild
make clean
make
```

## Performance

C implementation advantages:
- 10-20x faster startup
- 50% less memory usage (2-3 MB vs 15-20 MB Python)
- Same functionality as Python version
- Production-ready

## Project Structure

```
sofar/
├── README.md                    # This file (English)
├── README.de.md                 # German version
├── c-version/
│   ├── README.md                # Battery management (English)
│   ├── README.de.md             # Battery management (German)
│   ├── INSTALL.md               # Installation guide (English)
│   ├── INSTALL.de.md            # Installation guide (German)
│   ├── config.h                 # Configuration
│   ├── soyo1min.c               # Main battery management
│   ├── Makefile                 # Build system
│   └── read-inverter/
│       ├── README.md            # Register reader (English)
│       ├── README.de.md         # Register reader (German)
│       ├── read_config.h        # Default config
│       ├── read_inverter.c      # Main program with CLI
│       ├── csv_to_c.py          # Embeds CSV at build time
│       └── Makefile             # Build system
└── python/                      # Original Python version
```

## Contributing

This project was developed for production use on Raspberry Pi with Sofar inverters.

When making changes:
1. Test thoroughly on target hardware
2. Update both English and German documentation
3. Maintain backward compatibility with existing config files
4. Follow existing code style

## Version

**Current: v1.9_FIXED_GRID_SIGN_DATETIME_C**

Recent updates:
- Added comprehensive command-line argument support for read_inverter
- All configuration parameters now available via CLI
- Maintained backward compatibility with header file defaults
- Added detailed help text with examples

## License

Same as original Python version.

## Support

For issues, please check:
1. Log files in `/run/user/1000/`
2. Serial port permissions
3. MQTT broker connectivity
4. Configuration values

Ensure all dependencies are installed and user has proper permissions.
