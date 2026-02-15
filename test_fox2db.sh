#!/bin/bash
#
# test_fox2db.sh - Test-Suite für fox2db
#
# Testet verschiedene Funktionen ohne echten MQTT-Broker

set -e

BOLD='\033[1m'
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo "================================================"
echo "  fox2db Test Suite"
echo "================================================"
echo ""

# Helper functions
pass() {
    echo -e "${GREEN}✓${NC} $1"
}

fail() {
    echo -e "${RED}✗${NC} $1"
    exit 1
}

warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

section() {
    echo ""
    echo -e "${BOLD}$1${NC}"
    echo "----------------------------------------"
}

# Test 1: Binary exists
section "Test 1: Binary vorhanden"
if [ -f "./fox2db" ]; then
    pass "fox2db Binary gefunden"
else
    fail "fox2db Binary nicht gefunden! Bitte erst kompilieren: make"
fi

# Test 2: Version check
section "Test 2: Version Check"
VERSION=$(./fox2db --version 2>&1)
if [[ "$VERSION" == *"v1.50-C"* ]]; then
    pass "Version korrekt: $VERSION"
else
    fail "Version ungültig: $VERSION"
fi

# Test 3: Help output
section "Test 3: Help Output"
HELP=$(./fox2db --help 2>&1)
if [[ "$HELP" == *"--mqtt-broker"* ]]; then
    pass "Help-Text enthält --mqtt-broker"
else
    fail "Help-Text unvollständig"
fi

# Test 4: File creation (mock)
section "Test 4: Datei-Operationen"

TEST_DIR="/tmp/fox2db_test_$$"
mkdir -p "$TEST_DIR"

# Create mock state files
echo "0" > "$TEST_DIR/current_relay_state.txt"
echo "5" > "$TEST_DIR/last_relay_change.txt"
echo "2500.5" > "$TEST_DIR/last_excess.txt"
echo "0" > "$TEST_DIR/deep_discharge_protection_active.txt"

if [ -f "$TEST_DIR/current_relay_state.txt" ]; then
    pass "Test-Dateien erstellt in $TEST_DIR"
else
    fail "Konnte Test-Dateien nicht erstellen"
fi

# Test 5: Command-line parsing
section "Test 5: Command-Line Parsing"

# Test mit ungültigem Argument
if ./fox2db --invalid-option 2>&1 | grep -q "unrecognized option"; then
    pass "Ungültige Optionen werden erkannt"
else
    warn "Option-Parsing könnte verbessert werden"
fi

# Test 6: Dependencies check
section "Test 6: Library-Abhängigkeiten"

if ldd ./fox2db | grep -q "libpaho-mqtt3c"; then
    pass "libpaho-mqtt3c gelinkt"
else
    fail "libpaho-mqtt3c nicht gelinkt!"
fi

if ldd ./fox2db | grep -q "libcjson"; then
    pass "libcjson gelinkt"
else
    fail "libcjson nicht gelinkt!"
fi

# Test 7: Permissions
section "Test 7: Berechtigungen"

if [ -x ./fox2db ]; then
    pass "fox2db ist ausführbar"
else
    fail "fox2db nicht ausführbar! Führe aus: chmod +x fox2db"
fi

# Test 8: Config file syntax
section "Test 8: Beispiel-Config"

if [ -f "./fox2db_example.sh" ]; then
    if bash -n ./fox2db_example.sh; then
        pass "fox2db_example.sh Syntax korrekt"
    else
        fail "fox2db_example.sh Syntax-Fehler"
    fi
else
    warn "fox2db_example.sh nicht gefunden"
fi

# Test 9: Log directory writable
section "Test 9: Log-Verzeichnis beschreibbar"

LOG_DIR="/run/user/$(id -u)"
if [ -d "$LOG_DIR" ]; then
    if [ -w "$LOG_DIR" ]; then
        pass "Log-Verzeichnis beschreibbar: $LOG_DIR"
    else
        warn "Log-Verzeichnis nicht beschreibbar: $LOG_DIR"
    fi
else
    warn "Log-Verzeichnis existiert nicht: $LOG_DIR (wird automatisch erstellt)"
fi

# Test 10: MQTT broker reachable (optional)
section "Test 10: MQTT-Broker Erreichbarkeit (optional)"

MQTT_BROKER="kellertreppe.fritz.box"
MQTT_PORT=1883

if command -v nc > /dev/null 2>&1; then
    if timeout 2 nc -zv "$MQTT_BROKER" "$MQTT_PORT" 2>&1 | grep -q "succeeded\|open"; then
        pass "MQTT-Broker erreichbar: $MQTT_BROKER:$MQTT_PORT"
    else
        warn "MQTT-Broker nicht erreichbar: $MQTT_BROKER:$MQTT_PORT"
        warn "  → Passe --mqtt-broker an oder starte Mosquitto"
    fi
else
    warn "netcat (nc) nicht installiert, überspringe Broker-Test"
fi

# Test 11: Python scripts available
section "Test 11: Python-Skripte (optional)"

REQUIRED_SCRIPTS=(
    "/home/pi/python/ebox1arg.py"
    "/home/pi/python/ebyteserrequest.py"
    "/home/pi/python/fox2mqtt.py"
)

SCRIPTS_FOUND=0
for script in "${REQUIRED_SCRIPTS[@]}"; do
    if [ -f "$script" ]; then
        ((SCRIPTS_FOUND++))
        pass "Gefunden: $script"
    else
        warn "Nicht gefunden: $script (wird für Produktion benötigt)"
    fi
done

if [ $SCRIPTS_FOUND -eq ${#REQUIRED_SCRIPTS[@]} ]; then
    pass "Alle Python-Skripte vorhanden"
else
    warn "$SCRIPTS_FOUND/${#REQUIRED_SCRIPTS[@]} Skripte gefunden"
fi

# Cleanup
rm -rf "$TEST_DIR"

# Summary
section "Zusammenfassung"
echo ""
echo -e "${GREEN}✅ Alle Tests bestanden!${NC}"
echo ""
echo "Nächste Schritte:"
echo "  1. Starte MQTT-Broker: sudo systemctl start mosquitto"
echo "  2. Passe fox2db_example.sh an deine Config an"
echo "  3. Teste mit echten Daten: ./fox2db_example.sh"
echo "  4. Prüfe Log: tail -f /run/user/$(id -u)/fox2db.log"
echo ""
echo "Bei Problemen:"
echo "  - Prüfe MQTT-Verbindung: mosquitto_sub -h $MQTT_BROKER -t '#' -v"
echo "  - Prüfe Python-Skripte: ls -la /home/pi/python/"
echo "  - Siehe README_fox2db.md für Details"
echo ""
