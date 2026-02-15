#!/bin/bash
# relay1_70s_soyo.sh
# Production script: Relay 1 ON for 70s, send Soyo 200W every 3s
#
# Usage: sudo ./relay1_70s_soyo.sh [POWER] [DURATION] [INTERVAL]
#   POWER:    Watts to send to Soyo (default: 200)
#   DURATION: Total time in seconds (default: 70)
#   INTERVAL: Seconds between Soyo commands (default: 3)
#
# Examples:
#   sudo ./relay1_70s_soyo.sh              # 200W, 70s, every 3s
#   sudo ./relay1_70s_soyo.sh 300          # 300W, 70s, every 3s
#   sudo ./relay1_70s_soyo.sh 250 60 2     # 250W, 60s, every 2s

set -e  # Exit on error

# Configuration (with defaults)
POWER=${1:-200}
DURATION=${2:-70}
INTERVAL=${3:-3}
PROG="./test_modbus_relay"

# Validation
if [ ! -f "$PROG" ]; then
    echo "Error: $PROG not found. Run 'make test-modbus' first."
    exit 1
fi

if [ "$EUID" -ne 0 ]; then
    echo "Error: This script must be run as root (use sudo)"
    exit 1
fi

if [ "$POWER" -lt 0 ] || [ "$POWER" -gt 3000 ]; then
    echo "Warning: Power $POWER W out of typical range (0-3000W)"
fi

# Calculate number of cycles
CYCLES=$((DURATION / INTERVAL))

echo "========================================================"
echo "PRODUCTION: Relay 1 ON + Soyo Control"
echo "========================================================"
echo "Configuration:"
echo "  Power:    ${POWER}W"
echo "  Duration: ${DURATION}s"
echo "  Interval: ${INTERVAL}s"
echo "  Cycles:   ${CYCLES}"
echo ""

# Step 1: Turn Relay 1 ON
echo "[$(date +%H:%M:%S)] Relay 1 ON..."
$PROG 1 > /dev/null
echo "✓ Relay 1 is ON"
echo ""

# Step 2: Send Soyo commands every INTERVAL seconds
echo "Starting Soyo control (${CYCLES} cycles)..."
echo "========================================================"

for i in $(seq 1 $CYCLES); do
    TIMESTAMP=$(date +%H:%M:%S)
    printf "[%s] Cycle %2d/%2d: " "$TIMESTAMP" "$i" "$CYCLES"

    # Send Soyo command only (fast, ~60ms)
    $PROG --soyo $POWER

    # Sleep until next cycle (unless it's the last one)
    if [ $i -lt $CYCLES ]; then
        sleep $INTERVAL
    fi
done

echo "========================================================"
echo ""

# Step 3: Turn Relay 1 OFF
echo "[$(date +%H:%M:%S)] Relay 1 OFF..."
$PROG 0 > /dev/null
echo "✓ Relay 1 is OFF"
echo ""
echo "========================================================"
echo "COMPLETED: ${CYCLES} cycles, ${DURATION}s total"
echo "========================================================"
