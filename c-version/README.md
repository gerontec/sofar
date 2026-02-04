# Soyo1min - C Version

[🇩🇪 Deutsche Version](README.de.md)

C implementations for Sofar Inverter Management.

## Programs

### soyo1min
Battery Management System - manages battery discharge based on:
- Grid status (grid feeding/drawing)
- Battery SOC (State of Charge)
- Sunrise/sunset (via Python script `sunrise.py`)
- MQTT heat pump data
- Manual power settings (`soyopower.txt`)

### read-inverter
Inverter Register Reader - reads Modbus registers from Sofar inverter:
- CSV-based register definitions
- Modbus RTU via serial port
- Filter by kW/kWh/% or all registers
- **Full command-line configuration** (NEW!)
- CSV export of data

See `read-inverter/README.md` for details.

## Dependencies

### C Build Dependencies

**Debian/Ubuntu/Raspberry Pi OS:**
```bash
sudo apt-get update
sudo apt-get install -y build-essential libmosquitto-dev
```

**Other Distributions:**
- **Fedora/RHEL**: `sudo dnf install gcc make mosquitto-devel`
- **Arch**: `sudo pacman -S gcc make mosquitto`

### Python Dependencies (for sunrise.py)

The C program calls `sunrise.py` to calculate sunrise and sunset times:

```bash
# Install Python 3 and pip (if not present)
sudo apt-get install python3 python3-pip

# Install Python packages
pip3 install -r requirements.txt

# Or individually:
pip3 install astral pytz
```

## Build

```bash
cd c-version

# Optional: Check system requirements
./configure

# Compile
make
```

For debug build:
```bash
make debug
```

## Installation

```bash
sudo make install
```

This installs the binary to `/usr/local/bin/soyo1min`.

## Configuration

All configuration parameters are in `config.h`:

- **SERIAL_PORT**: Serial port for inverter communication (default: `/dev/ttyUSB32`)
- **MQTT_BROKER**: MQTT broker IP (default: `192.168.178.218`)
- **MQTT_TOPIC**: MQTT topic for heat pump power (default: `em0/54`)
- **BATTERY_CAPACITY_KWH**: Battery capacity in kWh (default: `30`)
- **NIGHT_STANDARD_POWER**: Base load during night time in W (default: `390`)
- **BAT2_SOC_MIN**: Minimum SOC for discharge protection (default: `9%`)

After changes, recompile:
```bash
make clean
make
```

## Usage

```bash
# Run directly
./soyo1min

# With systemd (recommended)
sudo systemctl start soyo1min
```

## Logging

Logs are written to: `/run/user/1000/soyo1min_c.log`

Log levels:
- **DEBUG**: Detailed information
- **INFO**: Normal operation messages
- **WARNING**: Warnings (e.g., invalid data)
- **ERROR**: Errors

## External Dependencies

The C program calls external scripts:
- **sunrise.py**: Must be in the same directory or in PATH

## Priority System

The system works with the following priorities (from highest to lowest):

1. **Discharge Protection**: SOC < 9% → Power = 0W
2. **Bat2 Charging**: Battery is charging (>2A) → Power = 0W
3. **Soyopower**: Manual setting from `soyopower.txt`
4. **Feeding to Grid**: PV surplus (Grid > 200W) → Power = 0W
5. **Buying from Grid**: Drawing from grid (Grid < -100W) → Compensate with battery
6. **Default**: Base load support

## Differences from Python Version

- Logging to separate file (`soyo1min_c.log`)
- Lower memory footprint
- Faster execution
- Same functionality

## Troubleshooting

### Program won't start
```bash
# Check lock file
cat /run/user/1000/soyo1min.lock

# Remove lock file (only if sure no instance is running)
rm /run/user/1000/soyo1min.lock
```

### Serial port errors
```bash
# Check permissions
ls -l /dev/ttyUSB32

# Add user to dialout group
sudo usermod -a -G dialout $USER
```

### MQTT connection problems
```bash
# Test MQTT broker
mosquitto_sub -h 192.168.178.218 -t em0/54 -v
```

## Systemd Service

Example service file (`/etc/systemd/system/soyo1min.service`):

```ini
[Unit]
Description=Soyo Battery Management (C version)
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

Enable:
```bash
sudo systemctl daemon-reload
sudo systemctl enable soyo1min
sudo systemctl start soyo1min
```

## License

Same as original Python version.

## Version

**v1.9_FIXED_GRID_SIGN_DATETIME_C**
