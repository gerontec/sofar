# Makefile for fox2db

# Compiler
CC = gcc

# Compiler flags
CFLAGS = -Wall -Wextra -O2 -std=c11

# Libraries
LIBS = -lpaho-mqtt3c -lcjson -lm

# Targets
TARGET1 = fox2db
SRC1 = fox2db.c

TARGET2 = ebox
SRC2 = ebox.c

# Installation paths
PREFIX = /usr/local
BINDIR = $(PREFIX)/bin

# Build
all: $(TARGET1) $(TARGET2)

$(TARGET1): $(SRC1)
	$(CC) $(CFLAGS) -o $(TARGET1) $(SRC1) $(LIBS)

$(TARGET2): $(SRC2)
	$(CC) $(CFLAGS) -o $(TARGET2) $(SRC2)

# Install
install: $(TARGET1) $(TARGET2)
	install -D -m 0755 $(TARGET1) $(DESTDIR)$(BINDIR)/$(TARGET1)
	install -D -m 0755 $(TARGET2) $(DESTDIR)$(BINDIR)/$(TARGET2)

# Uninstall
uninstall:
	rm -f $(DESTDIR)$(BINDIR)/$(TARGET1)
	rm -f $(DESTDIR)$(BINDIR)/$(TARGET2)

# Clean
clean:
	rm -f $(TARGET1) $(TARGET2)

# Run with default config
run: $(TARGET1)
	./$(TARGET1)

run-ebox: $(TARGET2)
	./$(TARGET2) --help

# Test build dependencies
check-deps:
	@echo "Checking dependencies..."
	@pkg-config --exists libpaho-mqtt3c && echo "✓ paho-mqtt3c found" || echo "✗ paho-mqtt3c missing (install libpaho-mqtt3c-dev)"
	@pkg-config --exists libcjson && echo "✓ cJSON found" || echo "✗ cJSON missing (install libcjson-dev)"
	@which gcc > /dev/null && echo "✓ gcc found" || echo "✗ gcc missing"

# Help
help:
	@echo "Sofar Tools Makefile"
	@echo ""
	@echo "Programs:"
	@echo "  fox2db       - MQTT-based inverter power management"
	@echo "  ebox         - Serial battery communication tool"
	@echo ""
	@echo "Targets:"
	@echo "  all          - Build all programs (default)"
	@echo "  install      - Install to $(BINDIR)"
	@echo "  uninstall    - Remove from $(BINDIR)"
	@echo "  clean        - Remove built files"
	@echo "  run          - Build and run fox2db with default config"
	@echo "  run-ebox     - Build and show ebox help"
	@echo "  check-deps   - Check if dependencies are installed"
	@echo "  help         - Show this help"

.PHONY: all install uninstall clean run run-ebox check-deps help
