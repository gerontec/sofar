# Installation Guide - Soyo1min C Version

[🇩🇪 Deutsche Version](INSTALL.de.md)

## Quick Start

```bash
# 0. Switch branch (if not already done)
cd ~/sofar
git fetch origin
git checkout claude/python-to-c-conversion-fDGGt
git pull origin claude/python-to-c-conversion-fDGGt

# 1. Install dependencies
sudo apt-get update
sudo apt-get install -y build-essential libmosquitto-dev python3 python3-pip

# 2. Install Python packages for sunrise.py
pip3 install -r requirements.txt

# 3. Check system requirements
cd c-version
./configure

# 4. Compile
make

# 5. Test (as user with access to /dev/ttyUSB32)
./soyo1min

# 6. Installation (optional)
sudo make install

# 7. Set up systemd service (optional)
sudo cp soyo1min.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable soyo1min
sudo systemctl start soyo1min
```

## Prerequisites

### Hardware
- Serial port `/dev/ttyUSB32` (or adjust in `config.h`)
- MQTT broker accessible at `192.168.178.218`

### Software
- Linux (Raspberry Pi OS, Debian, Ubuntu, etc.)
- GCC compiler
- libmosquitto-dev (MQTT library)
- Python 3 (for sunrise.py)
- Python packages: astral, pytz (for sunrise.py)

### Permissions
User must be member of the `dialout` group:
```bash
sudo usermod -a -G dialout $USER
# Login again required!
```

## Detailed Installation

### 1. Clone Repository / Copy Files

The C version is located in the `c-version/` directory.

### 2. Install Dependencies

**Debian/Ubuntu/Raspberry Pi OS:**
```bash
sudo apt-get install -y build-essential libmosquitto-dev
```

**Fedora/RHEL:**
```bash
sudo dnf install gcc make mosquitto-devel
```

**Arch Linux:**
```bash
sudo pacman -S gcc make mosquitto
```

### 3. Adjust Configuration

Edit `config.h` and adjust the following values:

```c
#define SERIAL_PORT "/dev/ttyUSB32"      // Your serial port
#define MQTT_BROKER "192.168.178.218"    // Your MQTT broker IP
#define MQTT_TOPIC "em0/54"              // Your MQTT topic
#define BATTERY_CAPACITY_KWH 30          // Your battery capacity
```

### 4. Compile

```bash
cd c-version
make clean
make
```

On success, the executable `soyo1min` is created.

**Debug version (with debugging symbols):**
```bash
make debug
```

### 5. Manual Test

```bash
# Check if sunrise.py is in PATH or same directory
which sunrise.py
# or
ls ../sunrise.py

# Start program
./soyo1min

# Stop with Ctrl+C
```

### 6. Install as System Service

```bash
# Install binary
sudo make install

# Copy service file
sudo cp soyo1min.service /etc/systemd/system/

# Adjust paths in service file if needed
sudo nano /etc/systemd/system/soyo1min.service
# Change User, Group and WorkingDirectory

# Enable and start service
sudo systemctl daemon-reload
sudo systemctl enable soyo1min
sudo systemctl start soyo1min

# Check status
sudo systemctl status soyo1min

# View logs
sudo journalctl -u soyo1min -f
```

## Uninstallation

```bash
# Stop and disable service
sudo systemctl stop soyo1min
sudo systemctl disable soyo1min
sudo rm /etc/systemd/system/soyo1min.service
sudo systemctl daemon-reload

# Remove binary
sudo rm /usr/local/bin/soyo1min

# Remove lock file (if present)
rm /run/user/1000/soyo1min.lock
```

## Running in Parallel with Python Version

Both versions use different lock files:
- Python: `/run/user/1000/soyo1min.lock`
- C: `/run/user/1000/soyo1min.lock` (same!)

**IMPORTANT:** Only run one version at a time!

The C version writes logs to:
- `/run/user/1000/soyo1min_c.log`

The Python version writes to:
- `/run/user/1000/soyo1min.log`

## Troubleshooting

### Error: "mosquitto.h: No such file or directory"
```bash
sudo apt-get install libmosquitto-dev
```

### Error: "Permission denied" accessing /dev/ttyUSB32
```bash
# Add user to dialout group
sudo usermod -a -G dialout $USER
# Login again!
```

### Error: "Another instance is running"
```bash
# Check running instance
ps aux | grep soyo1min

# If none running, remove lock file
rm /run/user/1000/soyo1min.lock
```

### MQTT won't connect
```bash
# Test broker
mosquitto_sub -h 192.168.178.218 -t em0/54 -v

# Check MQTT_BROKER IP in config.h
```

### sunrise.py not found
```bash
# Find where sunrise.py is located
find ~ -name sunrise.py

# Create symlink or adjust path in config.h
ln -s /path/to/sunrise.py ./sunrise.py
```

## Performance

The C version is:
- ~10-20x faster at startup
- ~50% less memory usage (about 2-3 MB vs 15-20 MB)
- Same functionality as Python version

## Next Steps

After successful installation:
1. Monitor logs: `tail -f /run/user/1000/soyo1min_c.log`
2. Observe behavior (Grid values, Power settings)
3. If needed, adjust configuration in `config.h` and recompile
