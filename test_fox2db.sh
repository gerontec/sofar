#!/bin/bash
# test_fox2db.sh - Test script for fox2db with C programs
#
# Usage on Raspberry Pi:
#   cd ~/sofar
#   chmod +x test_fox2db.sh
#   ./test_fox2db.sh

echo "=========================================="
echo "  fox2db C Integration Test"
echo "=========================================="
echo ""

# Check if programs are installed
echo "Checking installed programs..."
for prog in fox2db ebox fox2mqtt ebyte; do
    if command -v $prog &> /dev/null; then
        version=$($prog --version 2>&1 | head -1 || echo "unknown")
        echo "  ✓ $prog: $version"
    else
        echo "  ✗ $prog: NOT FOUND"
        echo "    Run: sudo make install"
        exit 1
    fi
done
echo ""

# Show which scripts will be used
echo "Configuration:"
echo "  EBox Script:         /home/pi/python/ebox1arg.py  (Python - battery data)"
echo "  EByte Script:        /usr/local/bin/ebyte         (C - relay control)"
echo "  MQTT Publish Script: /usr/local/bin/fox2mqtt      (C - MQTT publisher)"
echo ""

# Run fox2db with C programs
echo "Starting fox2db..."
echo "=========================================="
echo ""

time ./fox2db \
  --mqtt-broker "192.168.178.218" \
  --mqtt-port 1883 \
  --mqtt-topic "inverter/power_grid_exchange/json" \
  --mqtt-timeout 43 \
  --min-excess 1010 \
  --max-grid-draw 1500 \
  --max-soc 99 \
  --hysteresis 505 \
  --stabilization-cycles 2 \
  --emergency-import 1020 \
  --bat-discharge-threshold -220 \
  --sweet-spot-pcc 160 \
  --sweet-spot-bat -310 \
  --max-drop-rate -20 \
  --deep-discharge-lower 6 \
  --deep-discharge-upper 8 \
  --deep-discharge-target 7 \
  --ebox-script "/home/pi/python/ebox1arg.py" \
  --ebyte-script "/usr/local/bin/ebyte" \
  --mqtt-publish-script "/usr/local/bin/fox2mqtt"

exit_code=$?

echo ""
echo "=========================================="
echo "Test completed with exit code: $exit_code"
echo "=========================================="
echo ""

# Show log
if [ -f /run/user/1000/fox2db.log ]; then
    echo "Last 10 log entries:"
    tail -10 /run/user/1000/fox2db.log
else
    echo "No log file found at /run/user/1000/fox2db.log"
fi

exit $exit_code
