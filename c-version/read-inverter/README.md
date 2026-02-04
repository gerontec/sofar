# Read Inverter - C Version

[🇩🇪 Deutsche Version](README.de.md)

C implementation of the Sofar Inverter Register Reader (read.py).

## Overview

This program reads registers from a Sofar inverter via Modbus RTU:
- Reads register definitions from CSV file (embedded at build time)
- Connects via serial port (Modbus RTU)
- Reads various register ranges with mask filtering
- Decodes values (U16, I16, U32, I32, U64, BCD16, ASCII)
- Saves results to CSV files
- **Full command-line configuration** (NEW!)

## Dependencies

### Debian/Ubuntu
```bash
sudo apt-get update
sudo apt-get install -y build-essential libmodbus-dev
```

### Other Distributions
- **Fedora/RHEL**: `sudo dnf install gcc make libmodbus-devel`
- **Arch**: `sudo pacman -S gcc make libmodbus`

## Configuration

### NEW: Command-Line Arguments

All configuration can now be done via command-line without recompiling:

```bash
# View all options and current defaults
./read_inverter --help

# Serial Communication
./read_inverter --port /dev/ttyUSB0 --baud 9600 --unit-id 1
./read_inverter -p /dev/ttyUSB0 -b 9600 -u 1      # short form

# Timeout settings
./read_inverter --timeout 2                        # seconds
./read_inverter --timeout-usec 500000              # microseconds

# File Paths
./read_inverter --csv /path/to/custom_registers.csv
./read_inverter --output /tmp/my_output.csv
./read_inverter --pivoted-output /tmp/pivoted.csv

# Register Options
./read_inverter --all-registers                    # Read all registers
./read_inverter -a                                 # short form
./read_inverter --filter                           # Only kW/kWh/% (default)
./read_inverter -f                                 # short form

# Advanced Options
./read_inverter --max-register 0x3000              # Maximum register address (hex)
./read_inverter --block-size 64                    # Modbus block size (1-125)

# Combine options
./read_inverter -p /dev/ttyUSB0 -b 9600 -a -o /tmp/full_scan.csv
```

### Default Configuration

Most important settings are pre-configured for production use in `read_config.h`:

```c
#define SERIAL_PORT "/dev/ttyUSB32"                    // Serial port
#define BAUD_RATE 9600                                 // Baud rate
#define UNIT_ID 1                                      // Modbus Unit ID
#define CSV_FILE "/home/pi/python/sofarregister.csv"  // Register definitions
#define ALLREG 0                                       // 0=only kW/kWh/%, 1=all registers
```

**Embedded Register Definitions:**
- During build, all register definitions from `/home/pi/python/sofarregister.csv` are embedded directly into the binary
- The compiled program **does not need an external CSV file** at runtime
- The binary is standalone and fully portable
- If CSV is not available at build time, the program falls back to runtime CSV loading

**Standard Configuration:**
- Filter mode: only kW/kWh/% registers (faster, less data)
- These values are production-ready and normally don't need changes

**Optional:** Only if you need different values, edit `read_config.h` and recompile.

## Build

```bash
cd read-inverter

# Optional: Check system requirements
./configure

# Compile
make
```

For debug build:
```bash
make debug
```

**Note:** If `configure` doesn't exist, get the latest version:
```bash
git pull origin claude/python-to-c-conversion-fDGGt
```

## Usage

```bash
# Show help (displays all configuration values)
./read_inverter --help

# Show version
./read_inverter --version

# Run directly with defaults
./read_inverter

# Run with custom settings
./read_inverter --port /dev/ttyUSB0 --all-registers --output /tmp/data.csv

# As root if serial port permission required
sudo ./read_inverter
```

**Tip:** Use `--help` to see all currently compiled settings without looking at source files.

## Command-Line Options

### General Options
- `-h, --help` - Show help message and exit
- `-v, --version` - Show version information and exit

### Serial Communication
- `-p, --port <device>` - Serial port (default: /dev/ttyUSB32)
- `-b, --baud <rate>` - Baud rate (default: 9600)
- `-u, --unit-id <id>` - Modbus unit ID 0-247 (default: 1)
- `-t, --timeout <sec>` - Response timeout in seconds (default: 1)
- `--timeout-usec <usec>` - Response timeout microseconds (default: 0)

### File Paths
- `-c, --csv <file>` - Register definitions CSV (default: /home/pi/python/sofarregister.csv)
- `-o, --output <file>` - Raw output CSV file (default: /tmp/raw.csv)
- `--pivoted-output <file>` - Pivoted output CSV file (default: /tmp/pivoted_registers.csv)

### Register Options
- `-a, --all-registers` - Read all registers (default: no)
- `-f, --filter` - Filter kW/kWh/% only (opposite of --all-registers)
- `--max-register <addr>` - Maximum register address in hex (default: 0x203F)
- `--block-size <size>` - Modbus read block size 1-125 (default: 32)

## Examples

```bash
# Use different serial port and unit ID
./read_inverter -p /dev/ttyUSB0 -u 2

# Read all registers and save to custom location
./read_inverter --all-registers --output /home/user/inverter_data.csv

# Use custom register definitions
./read_inverter -c /path/to/my_registers.csv -o /tmp/output.csv

# Adjust timeout for slow connections
./read_inverter --timeout 3 --timeout-usec 0

# Full custom configuration
./read_inverter \
  --port /dev/ttyUSB0 \
  --baud 9600 \
  --unit-id 1 \
  --all-registers \
  --output /tmp/full_scan.csv \
  --max-register 0x3000 \
  --block-size 64
```

## Output Files

- Default: `/tmp/raw.csv` - Raw register data in CSV format
- Console output with all read registers

## Register Filter

By default (`ALLREG=0` or `--filter`), only registers with the following units are read:
- `kW` (Kilowatt)
- `kWh` (Kilowatt hours)
- `%` (Percent, for SOC)

Use `--all-registers` or `-a` to read all registers.

## CSV Format

The register definition CSV (`sofarregister.csv`) must have this format:
```
Section;Address;Name;Type;Accuracy;Unit
I General;0x0040-0x007F;Register Name;U16;1;kW
```

Columns (separated by `;`):
1. Section (empty for register lines)
2. Address (Hex, can be range: `0x0040-0x007F` or `0x0040____0x007F`)
3. Name
4. Type (U16, I16, U32, I32, U64, BCD16, ASCII)
5. Accuracy (Multiplier, e.g., 0.1)
6. Unit (e.g., kW, kWh, %, V, A)

## Permissions

User must have access to the serial port:
```bash
sudo usermod -a -G dialout $USER
# Then login again
```

## Troubleshooting

### Error: Cannot open serial port
```bash
# Check if port exists
ls -l /dev/ttyUSB32

# Check permissions
groups  # should contain "dialout"
```

### Error: Invalid option
```bash
# Make sure you're using the latest version
git pull

# Check available options
./read_inverter --help
```

### Error: Failed to create Modbus context
```bash
# Install libmodbus
sudo apt-get install libmodbus-dev
```

### Error: Cannot open CSV file
```bash
# Specify CSV file location
./read_inverter --csv /path/to/sofarregister.csv

# Or adjust path in read_config.h and recompile
```

### No registers read
```bash
# Use --all-registers flag
./read_inverter --all-registers

# Or ensure CSV contains registers with kW/kWh/% units
```

## Performance

- Extremely fast (C-native)
- Low memory usage (~2-5 MB)
- No Python dependencies
- Production-ready

## Differences from Python Version

✅ **Same Functionality:**
- CSV parsing
- Modbus RTU communication
- Register decoding (all types)
- Mask-based register filtering
- CSV output

➕ **New Features:**
- Full command-line configuration
- No recompilation needed for parameter changes
- Embedded register definitions (optional)

❌ **Not Implemented:**
- Pivoted CSV output (only raw.csv)
- Pandas integration
- File logging (only stdout/stderr)

## Installation (optional)

```bash
sudo make install
```

Installs to `/usr/local/bin/read_inverter`.

## Version

C port of read.py v1.0 with command-line configuration support.

## Changelog

### v1.1 (Latest)
- Added comprehensive command-line argument support
- All configuration parameters now available via CLI
- Maintained backward compatibility with header defaults
- Added detailed help text with examples
- Improved error messages for invalid parameters

### v1.0
- Initial C port from Python
- Embedded register definitions
- Production-ready configuration
