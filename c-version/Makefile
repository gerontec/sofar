CC = gcc
CFLAGS = -Wall -Wextra -O2 -std=c11
LDFLAGS = -lmosquitto -lm
TARGET = soyo1min
SOURCES = soyo1min.c
OBJECTS = $(SOURCES:.c=.o)

.PHONY: all clean install

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.c config.h
	$(CC) $(CFLAGS) -c $<

clean:
	rm -f $(OBJECTS) $(TARGET)

install: $(TARGET)
	install -m 755 $(TARGET) /usr/local/bin/

# Development builds with debug symbols
debug: CFLAGS += -g -DDEBUG
debug: clean $(TARGET)

# Static analysis
check:
	cppcheck --enable=all --suppress=missingIncludeSystem $(SOURCES)
