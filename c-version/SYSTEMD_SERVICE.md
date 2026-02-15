# systemd Service Installation Guide

This guide explains how to set up `soyo1min` as a systemd service that starts automatically on boot.

## Overview

The systemd service ensures that:
- ✅ `soyo1min` starts automatically on system boot
- ✅ Automatic restart on crashes
- ✅ Proper logging via journald
- ✅ Security hardening with restricted permissions
- ✅ Runs as user `pi` with `dialout` group for serial port access

## Prerequisites

1. Build and install the binary:
```bash
cd ~/sofar/c-version
make clean && make
sudo make install
```

This installs `/usr/local/bin/soyo1min`.

2. Ensure user `pi` is in the `dialout` group:
```bash
sudo usermod -a -G dialout pi
```

## Service File Creation

Create the service file:
```bash
sudo nano /etc/systemd/system/soyo1min.service
```

Add the following content:

```ini
[Unit]
Description=Soyo Battery Management System (C version)
After=network.target mosquitto.service
Wants=mosquitto.service

[Service]
Type=simple
User=pi
Group=dialout

# Note: WorkingDirectory is not needed since all files are in /tmp
# If you uncomment it, you must also set ProtectHome=false
# WorkingDirectory=/home/pi/sofar

ExecStart=/usr/local/bin/soyo1min
Restart=always
RestartSec=10

# Logging
StandardOutput=journal
StandardError=journal

# Security hardening
NoNewPrivileges=true
PrivateTmp=false
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/tmp /home/pi/soyopower.txt

[Install]
WantedBy=multi-user.target
```

## Installation

Enable and start the service:

```bash
# Reload systemd to recognize the new service
sudo systemctl daemon-reload

# Enable service to start on boot
sudo systemctl enable soyo1min.service

# Start the service now
sudo systemctl start soyo1min.service

# Check status
sudo systemctl status soyo1min.service
```

## Service Management Commands

```bash
# Check service status
sudo systemctl status soyo1min.service

# Start service
sudo systemctl start soyo1min.service

# Stop service
sudo systemctl stop soyo1min.service

# Restart service
sudo systemctl restart soyo1min.service

# View logs (last 50 lines)
sudo journalctl -u soyo1min.service -n 50

# Follow logs in real-time
sudo journalctl -u soyo1min.service -f

# View logs from today
sudo journalctl -u soyo1min.service --since today

# Disable service from starting on boot
sudo systemctl disable soyo1min.service
```

## Logging

The service logs to:
1. **systemd journal**: View with `journalctl -u soyo1min.service`
2. **Log file**: `/tmp/soyo1min_c.log` (configured in `config.h`)

Example log output:
```
2026-02-15 10:00:36 - INFO - Starting soyo1min v1.13_CONFIGURABLE_LOCATION
2026-02-15 10:00:36 - INFO - Configuration: port=/dev/ttyUSB0, baud=4800, mqtt=localhost:1883, topic=warmepumpe/VorBetrieb_W
2026-02-15 10:00:36 - INFO - Location: lat=47.6811, lon=11.5732, sunrise_offset=60 min, sunset_offset=-60 min
2026-02-15 10:00:39 - DEBUG - Sun: Now=10:00:39, Sunrise=08:22:15, Sunset=16:36:03, IsNight=0
```

## Security Hardening

The service includes several security features:

- **NoNewPrivileges=true**: Prevents privilege escalation
- **ProtectHome=true**: Makes `/home` directories inaccessible (read-only)
- **ProtectSystem=strict**: Makes `/usr`, `/boot`, `/efi` read-only
- **PrivateTmp=false**: Allows access to `/tmp` for lock/log files
- **ReadWritePaths**: Explicitly allows writing to `/tmp` and `soyopower.txt`

### Important: ProtectHome vs WorkingDirectory

⚠️ **Do not use both `ProtectHome=true` and `WorkingDirectory=/home/pi/...`**

This will cause a **CHDIR error** (exit code 200) because `ProtectHome=true` makes `/home` invisible to the service.

**Solutions:**
1. Remove `WorkingDirectory` line (recommended, since all files are in `/tmp`)
2. Set `ProtectHome=false` (less secure)
3. Use `WorkingDirectory=/tmp`

## File Paths

Default file locations (from `config.h`):

```
Lock file:     /tmp/soyo1min.lock
Log file:      /tmp/soyo1min_c.log
Inverter data: /tmp/inverter.csv
Manual power:  /home/pi/soyopower.txt
```

All paths can be overridden via command-line options.

## Customization

### Custom Command-Line Options

To pass custom options to `soyo1min`, modify the `ExecStart` line:

```ini
ExecStart=/usr/local/bin/soyo1min --latitude 48.1351 --longitude 11.5820 --interval 5
```

### Custom Location (not Lenggries)

```ini
ExecStart=/usr/local/bin/soyo1min --latitude 52.5200 --longitude 13.4050  # Berlin
```

### Custom File Paths

```ini
ExecStart=/usr/local/bin/soyo1min --log-file /var/log/soyo1min.log --lock-file /var/lock/soyo1min.lock
```

**Note:** If you change file paths outside `/tmp` or `/home/pi`, you must also update `ReadWritePaths`:

```ini
ReadWritePaths=/var/log /var/lock /tmp /home/pi/soyopower.txt
```

After any change to the service file:
```bash
sudo systemctl daemon-reload
sudo systemctl restart soyo1min.service
```

## Troubleshooting

### Service fails with "CHDIR" error (exit code 200)

**Problem:** `ProtectHome=true` conflicts with `WorkingDirectory=/home/pi/...`

**Solution:** Remove the `WorkingDirectory` line or set `ProtectHome=false`

```bash
sudo nano /etc/systemd/system/soyo1min.service
# Comment out or remove: WorkingDirectory=/home/pi/sofar
sudo systemctl daemon-reload
sudo systemctl restart soyo1min.service
```

### Service fails with "Permission denied" on serial port

**Problem:** User `pi` is not in the `dialout` group

**Solution:**
```bash
sudo usermod -a -G dialout pi
# Reboot or re-login
```

### Service fails to start - binary not found

**Problem:** `/usr/local/bin/soyo1min` doesn't exist

**Solution:**
```bash
cd ~/sofar/c-version
sudo make install
```

### View detailed error messages

```bash
# Show full status with error messages
sudo systemctl status soyo1min.service -l

# Show recent logs
sudo journalctl -u soyo1min.service -n 100 --no-pager
```

### Service starts but immediately stops

Check the logs for errors:
```bash
sudo journalctl -u soyo1min.service -f
```

Common issues:
- MQTT broker not running: `sudo systemctl start mosquitto.service`
- Serial port not available: Check `ls -l /dev/ttyUSB*`
- Lock file exists from previous instance: `rm /tmp/soyo1min.lock`

## Updating the Service

When you update the code:

```bash
cd ~/sofar
git pull
cd c-version
make clean && make
sudo make install
sudo systemctl restart soyo1min.service
sudo systemctl status soyo1min.service
```

The service will automatically restart with the new binary.

## Complete Example

Full installation from scratch:

```bash
# 1. Clone repository
cd ~
git clone git@github.com:gerontec/sofar.git
cd sofar/c-version

# 2. Build and install
make clean && make
sudo make install

# 3. Add user to dialout group
sudo usermod -a -G dialout pi

# 4. Create service file
sudo nano /etc/systemd/system/soyo1min.service
# (paste service file content from above)

# 5. Enable and start service
sudo systemctl daemon-reload
sudo systemctl enable soyo1min.service
sudo systemctl start soyo1min.service

# 6. Verify it's running
sudo systemctl status soyo1min.service
tail -f /tmp/soyo1min_c.log

# 7. Check that it starts on boot
sudo reboot
# After reboot:
sudo systemctl status soyo1min.service
```

## Service Status

When properly configured, the status should show:

```
● soyo1min.service - Soyo Battery Management System (C version)
     Loaded: loaded (/etc/systemd/system/soyo1min.service; enabled; vendor preset: enabled)
     Active: active (running) since Sun 2026-02-15 10:00:36 CET; 5min ago
   Main PID: 829899 (soyo1min)
      Tasks: 3 (limit: 8755)
        CPU: 2.5s
     CGroup: /system.slice/soyo1min.service
             └─829899 /usr/local/bin/soyo1min
```

Key indicators:
- `Loaded: loaded` - Service file found and valid
- `enabled` - Will start on boot
- `Active: active (running)` - Currently running
- Shows the Main PID and CPU usage

---

**Version:** v1.13_CONFIGURABLE_LOCATION
**Last Updated:** 2026-02-15
