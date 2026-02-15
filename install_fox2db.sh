#!/bin/bash
#
# install_fox2db.sh - Automatische Installation von fox2db auf Raspberry Pi
#
# Usage: ./install_fox2db.sh

set -e  # Exit on error

echo "================================================"
echo "  fox2db Installation Script"
echo "================================================"
echo ""

# Check if running on Raspberry Pi
if ! grep -q "Raspberry Pi" /proc/cpuinfo 2>/dev/null && ! grep -q "BCM" /proc/cpuinfo 2>/dev/null; then
    echo "⚠️  Warnung: Läuft nicht auf Raspberry Pi"
    echo "   Fortfahren auf eigene Gefahr..."
    echo ""
fi

# Update package list
echo "📦 Aktualisiere Package-Liste..."
sudo apt update

# Install dependencies
echo ""
echo "📥 Installiere Abhängigkeiten..."
sudo apt install -y \
    build-essential \
    git \
    pkg-config \
    libpaho-mqtt-dev \
    libcjson-dev

# Check dependencies
echo ""
echo "🔍 Prüfe Abhängigkeiten..."

if pkg-config --exists libpaho-mqtt3c; then
    echo "  ✓ libpaho-mqtt3c gefunden"
else
    echo "  ✗ libpaho-mqtt3c fehlt!"
    exit 1
fi

if pkg-config --exists libcjson; then
    echo "  ✓ libcjson gefunden"
else
    echo "  ✗ libcjson fehlt!"
    exit 1
fi

if which gcc > /dev/null 2>&1; then
    echo "  ✓ gcc gefunden ($(gcc --version | head -n1))"
else
    echo "  ✗ gcc fehlt!"
    exit 1
fi

# Build
echo ""
echo "🔨 Kompiliere fox2db..."
make clean 2>/dev/null || true
make

if [ ! -f "./fox2db" ]; then
    echo "  ✗ Build fehlgeschlagen!"
    exit 1
fi

echo "  ✓ Build erfolgreich"

# Test
echo ""
echo "🧪 Teste Binary..."
if ./fox2db --version; then
    echo "  ✓ Binary funktioniert"
else
    echo "  ✗ Binary-Test fehlgeschlagen!"
    exit 1
fi

# Optional: Install system-wide
echo ""
read -p "Möchtest du fox2db system-weit installieren? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "📦 Installiere nach /usr/local/bin..."
    sudo make install
    echo "  ✓ Installiert als: $(which fox2db)"
fi

# Create required directories
echo ""
echo "📁 Erstelle Runtime-Verzeichnisse..."
mkdir -p /run/user/$(id -u)
echo "  ✓ /run/user/$(id -u) bereit"

# Set permissions
echo ""
echo "🔐 Setze Berechtigungen..."
chmod +x fox2db
[ -f fox2db_example.sh ] && chmod +x fox2db_example.sh
echo "  ✓ Executable-Flags gesetzt"

# Summary
echo ""
echo "================================================"
echo "  ✅ Installation abgeschlossen!"
echo "================================================"
echo ""
echo "Verwendung:"
echo "  ./fox2db --help              # Hilfe anzeigen"
echo "  ./fox2db --version           # Version anzeigen"
echo "  ./fox2db                     # Mit Defaults starten"
echo "  ./fox2db_example.sh          # Mit Beispiel-Config starten"
echo ""
echo "Log-Datei:"
echo "  tail -f /run/user/$(id -u)/fox2db.log"
echo ""
echo "Nächste Schritte:"
echo "  1. Passe fox2db_example.sh an deine Umgebung an"
echo "  2. Teste mit: ./fox2db_example.sh"
echo "  3. Richte Cron-Job ein (siehe README_fox2db.md)"
echo ""
