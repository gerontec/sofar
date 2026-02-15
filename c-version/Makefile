CC = gcc
CFLAGS = -Wall -Wextra -O2 -std=c11
LDFLAGS = -lmosquitto -lm
TARGET = soyo1min
SOURCES = soyo1min.c
OBJECTS = $(SOURCES:.c=.o)

# Test tools
TEST_BAUDRATE = test_baudrate_switch

.PHONY: all clean install test-baudrate

all: $(TARGET)

test-baudrate: $(TEST_BAUDRATE)
	@echo "Run with: sudo ./$(TEST_BAUDRATE) [/dev/ttyUSB0]"

$(TEST_BAUDRATE): test_baudrate_switch.c
	$(CC) $(CFLAGS) -o $@ $^ -lm

$(TARGET): $(OBJECTS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.c config.h
	$(CC) $(CFLAGS) -c $<

clean:
	rm -f $(OBJECTS) $(TARGET) $(TEST_BAUDRATE)

install: $(TARGET)
	install -m 755 $(TARGET) /usr/local/bin/

# Development builds with debug symbols
debug: CFLAGS += -g -DDEBUG
debug: clean $(TARGET)

# Static analysis
check:
	cppcheck --enable=all --suppress=missingIncludeSystem $(SOURCES)
