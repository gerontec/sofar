# Makefile for fox2db

# Compiler
CC = gcc

# Compiler flags
CFLAGS = -Wall -Wextra -O2 -std=c11

# Libraries
LIBS = -lpaho-mqtt3c -lcjson -lm

# Targets
TARGET = fox2db
SRC = fox2db.c

# Installation paths
PREFIX = /usr/local
BINDIR = $(PREFIX)/bin

# Build
all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) $(LIBS)

# Install
install: $(TARGET)
	install -D -m 0755 $(TARGET) $(DESTDIR)$(BINDIR)/$(TARGET)

# Uninstall
uninstall:
	rm -f $(DESTDIR)$(BINDIR)/$(TARGET)

# Clean
clean:
	rm -f $(TARGET)

# Run with default config
run: $(TARGET)
	./$(TARGET)

# Test build dependencies
check-deps:
	@echo "Checking dependencies..."
	@pkg-config --exists libpaho-mqtt3c && echo "✓ paho-mqtt3c found" || echo "✗ paho-mqtt3c missing (install libpaho-mqtt3c-dev)"
	@pkg-config --exists libcjson && echo "✓ cJSON found" || echo "✗ cJSON missing (install libcjson-dev)"
	@which gcc > /dev/null && echo "✓ gcc found" || echo "✗ gcc missing"

# Help
help:
	@echo "fox2db Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  all          - Build fox2db (default)"
	@echo "  install      - Install to $(BINDIR)"
	@echo "  uninstall    - Remove from $(BINDIR)"
	@echo "  clean        - Remove built files"
	@echo "  run          - Build and run with default config"
	@echo "  check-deps   - Check if dependencies are installed"
	@echo "  help         - Show this help"

.PHONY: all install uninstall clean run check-deps help
