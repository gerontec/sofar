CC = gcc
CFLAGS = -Wall -Wextra -O2 -std=c11
LDFLAGS = -lmodbus
TARGET = read_inverter
SOURCES = read_inverter.c
OBJECTS = $(SOURCES:.c=.o)

# CSV file to embed (set to empty to disable embedding)
CSV_SOURCE = /home/pi/python/sofarregister.csv
EMBEDDED_SOURCE = register_embedded.c
EMBEDDED_OBJECT = register_embedded.o

.PHONY: all clean install embedded

# Default: build with embedded registers if CSV exists
all: check_csv $(TARGET)

check_csv:
	@if [ -f "$(CSV_SOURCE)" ]; then \
		echo "CSV file found, building with embedded registers..."; \
		$(MAKE) embedded; \
	else \
		echo "CSV file not found at $(CSV_SOURCE)"; \
		echo "Building without embedded registers (will need CSV at runtime)"; \
	fi

embedded: $(EMBEDDED_SOURCE)

$(TARGET): $(OBJECTS) $(EMBEDDED_OBJECT)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

$(EMBEDDED_SOURCE): $(CSV_SOURCE) csv_to_c.py
	@echo "Generating embedded register data from CSV..."
	./csv_to_c.py $(CSV_SOURCE) > $(EMBEDDED_SOURCE)
	@echo "Generated $(EMBEDDED_SOURCE)"

%.o: %.c read_config.h
	$(CC) $(CFLAGS) -c $<

clean:
	rm -f $(OBJECTS) $(EMBEDDED_OBJECT) $(EMBEDDED_SOURCE) $(TARGET)

install: $(TARGET)
	install -m 755 $(TARGET) /usr/local/bin/

# Development builds with debug symbols
debug: CFLAGS += -g -DDEBUG
debug: clean $(TARGET)

# Static analysis
check:
	cppcheck --enable=all --suppress=missingIncludeSystem $(SOURCES)
