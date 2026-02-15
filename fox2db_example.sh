#!/bin/bash
#
# fox2db_example.sh - Example configuration script for fox2db
#
# This shows all available command-line options with custom values.
# Copy this file and modify according to your setup.

./fox2db \
  --mqtt-broker "192.168.178.50" \
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
  --ebyte-script "/home/pi/python/ebyteserrequest.py" \
  --mqtt-publish-script "/home/pi/python/fox2mqtt.py"
