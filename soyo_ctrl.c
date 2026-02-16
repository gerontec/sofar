/**
 * SOYO Controller - Command-line interface for Pico Modbus
 *
 * Sends watt values to Raspberry Pi Pico via USB serial.
 * The Pico writes to the correct memory location (g_public_ram.soyo_target_watts)
 *
 * Usage:
 *   soyo_ctrl <watts>           # Set watts and enable
 *   soyo_ctrl <watts> on        # Set watts and enable explicitly
 *   soyo_ctrl <watts> off       # Set watts but disable SOYO
 *   soyo_ctrl status            # Query status
 *
 * Examples:
 *   soyo_ctrl 1500              # Set 1500W, enable
 *   soyo_ctrl 0 off             # Disable SOYO
 *   soyo_ctrl status            # Show current state
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <termios.h>
#include <errno.h>

#define PICO_DEVICE "/dev/ttyACM0"
#define BAUD_RATE B115200
#define TIMEOUT_MS 1000

// Open serial connection to Pico
static int open_pico(void) {
    int fd = open(PICO_DEVICE, O_RDWR | O_NOCTTY);
    if (fd < 0) {
        perror("Failed to open " PICO_DEVICE);
        fprintf(stderr, "Make sure Pico is connected and you have permissions.\n");
        fprintf(stderr, "Try: sudo chmod 666 %s\n", PICO_DEVICE);
        return -1;
    }

    // Configure serial port (115200 baud, 8N1)
    struct termios tty;
    if (tcgetattr(fd, &tty) != 0) {
        perror("tcgetattr");
        close(fd);
        return -1;
    }

    // Set baud rate
    cfsetospeed(&tty, BAUD_RATE);
    cfsetispeed(&tty, BAUD_RATE);

    // 8N1 mode
    tty.c_cflag &= ~PARENB;        // No parity
    tty.c_cflag &= ~CSTOPB;        // 1 stop bit
    tty.c_cflag &= ~CSIZE;
    tty.c_cflag |= CS8;            // 8 bits
    tty.c_cflag &= ~CRTSCTS;       // No hardware flow control
    tty.c_cflag |= CREAD | CLOCAL; // Enable receiver, ignore modem control

    // Raw mode
    tty.c_lflag &= ~(ICANON | ECHO | ECHOE | ISIG);
    tty.c_iflag &= ~(IXON | IXOFF | IXANY);
    tty.c_oflag &= ~OPOST;

    // Timeout
    tty.c_cc[VMIN] = 0;
    tty.c_cc[VTIME] = 10;  // 1 second timeout

    if (tcsetattr(fd, TCSANOW, &tty) != 0) {
        perror("tcsetattr");
        close(fd);
        return -1;
    }

    // Flush buffers
    tcflush(fd, TCIOFLUSH);

    return fd;
}

// Send command to Pico
static int send_command(int fd, const char *cmd) {
    ssize_t written = write(fd, cmd, strlen(cmd));
    if (written < 0) {
        perror("write");
        return -1;
    }

    // Wait for command to be processed
    usleep(50000);  // 50ms

    return 0;
}

// Read response from Pico (non-blocking)
static int read_response(int fd, char *buf, size_t len) {
    ssize_t total = 0;
    ssize_t n;

    // Read up to 1 second worth of data
    for (int i = 0; i < 10; i++) {
        n = read(fd, buf + total, len - total - 1);
        if (n > 0) {
            total += n;
        }
        if (total > 0) {
            usleep(100000);  // Wait for more data
        }
    }

    buf[total] = '\0';
    return total;
}

// Set SOYO watts
static int set_soyo(int watts, int enable) {
    int fd = open_pico();
    if (fd < 0) {
        return 1;
    }

    char cmd[64];

    // Send watts command
    snprintf(cmd, sizeof(cmd), "s %d\n", watts);
    if (send_command(fd, cmd) != 0) {
        close(fd);
        return 1;
    }

    // Send enable/disable command
    snprintf(cmd, sizeof(cmd), "e %d\n", enable);
    if (send_command(fd, cmd) != 0) {
        close(fd);
        return 1;
    }

    printf("SOYO: %d W (%s)\n", watts, enable ? "enabled" : "disabled");

    close(fd);
    return 0;
}

// Query status
static int query_status(void) {
    int fd = open_pico();
    if (fd < 0) {
        return 1;
    }

    // Send query command
    if (send_command(fd, "q\n") != 0) {
        close(fd);
        return 1;
    }

    // Read response
    char buf[1024];
    int len = read_response(fd, buf, sizeof(buf));
    if (len > 0) {
        printf("%s", buf);
    } else {
        printf("No response from Pico\n");
    }

    close(fd);
    return 0;
}

// Print usage
static void usage(const char *prog) {
    fprintf(stderr, "Usage:\n");
    fprintf(stderr, "  %s <watts>           # Set watts and enable\n", prog);
    fprintf(stderr, "  %s <watts> on        # Set watts and enable explicitly\n", prog);
    fprintf(stderr, "  %s <watts> off       # Set watts but disable SOYO\n", prog);
    fprintf(stderr, "  %s status            # Query current status\n", prog);
    fprintf(stderr, "\n");
    fprintf(stderr, "Examples:\n");
    fprintf(stderr, "  %s 1500              # Set 1500W, enable\n", prog);
    fprintf(stderr, "  %s 2000 on           # Set 2000W, enable\n", prog);
    fprintf(stderr, "  %s 0 off             # Disable SOYO\n", prog);
    fprintf(stderr, "  %s status            # Show current state\n", prog);
    fprintf(stderr, "\n");
    fprintf(stderr, "Note: Commands are sent via %s\n", PICO_DEVICE);
}

int main(int argc, char *argv[]) {
    // Check arguments
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    // Handle "status" command
    if (strcmp(argv[1], "status") == 0 || strcmp(argv[1], "q") == 0) {
        return query_status();
    }

    // Parse watts argument
    int watts = atoi(argv[1]);
    if (watts < 0 || watts > 65535) {
        fprintf(stderr, "ERROR: Watts must be 0-65535\n");
        return 1;
    }

    // Parse enable/disable argument
    int enable = 1;  // Default: enabled
    if (argc >= 3) {
        if (strcmp(argv[2], "off") == 0 || strcmp(argv[2], "0") == 0) {
            enable = 0;
        } else if (strcmp(argv[2], "on") == 0 || strcmp(argv[2], "1") == 0) {
            enable = 1;
        } else {
            fprintf(stderr, "ERROR: Second argument must be 'on' or 'off'\n");
            usage(argv[0]);
            return 1;
        }
    }

    // Set SOYO
    return set_soyo(watts, enable);
}
