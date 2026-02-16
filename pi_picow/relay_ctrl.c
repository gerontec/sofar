/**
 * Relay Controller - Command-line interface for Pico Modbus
 *
 * Sends relay state to Raspberry Pi Pico via USB serial.
 * The Pico writes to the correct memory location (g_public_ram.relay_target_state)
 *
 * Usage:
 *   relay_ctrl <state>          # Set relay state (0-7, 11)
 *   relay_ctrl status           # Query status
 *
 * Examples:
 *   relay_ctrl 0                # All relays OFF
 *   relay_ctrl 3                # R1+R2 ON
 *   relay_ctrl 7                # All relays ON
 *   relay_ctrl 11               # Special state 11
 *   relay_ctrl status           # Show current state
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

// State descriptions
static const char* state_desc[] = {
    "All OFF",
    "R1 ON",
    "R2 ON",
    "R1+R2 ON",
    "R3 ON",
    "R1+R3 ON",
    "R2+R3 ON",
    "All ON"
};

// Open serial connection to Pico
static int open_pico(void) {
    int fd = open(PICO_DEVICE, O_RDWR | O_NOCTTY);
    if (fd < 0) {
        perror("Failed to open " PICO_DEVICE);
        fprintf(stderr, "Make sure Pico is connected and you have permissions.\n");
        fprintf(stderr, "Try: sudo chmod 666 %s\n", PICO_DEVICE);
        return -1;
    }

    // Configure serial port
    struct termios tty;
    tcgetattr(fd, &tty);

    cfsetospeed(&tty, BAUD_RATE);
    cfsetispeed(&tty, BAUD_RATE);

    tty.c_cflag &= ~PARENB;
    tty.c_cflag &= ~CSTOPB;
    tty.c_cflag &= ~CSIZE;
    tty.c_cflag |= CS8;
    tty.c_cflag &= ~CRTSCTS;
    tty.c_cflag |= CREAD | CLOCAL;

    tty.c_lflag &= ~(ICANON | ECHO | ECHOE | ISIG);
    tty.c_iflag &= ~(IXON | IXOFF | IXANY);
    tty.c_oflag &= ~OPOST;

    tty.c_cc[VMIN] = 0;
    tty.c_cc[VTIME] = 10;

    tcsetattr(fd, TCSANOW, &tty);
    tcflush(fd, TCIOFLUSH);

    return fd;
}

// Send command to Pico
static int send_command(int fd, const char *cmd) {
    write(fd, cmd, strlen(cmd));
    usleep(50000);  // 50ms
    return 0;
}

// Read response
static int read_response(int fd, char *buf, size_t len) {
    ssize_t total = 0;
    ssize_t n;

    for (int i = 0; i < 10; i++) {
        n = read(fd, buf + total, len - total - 1);
        if (n > 0) {
            total += n;
        }
        if (total > 0) {
            usleep(100000);
        }
    }

    buf[total] = '\0';
    return total;
}

// Set relay state
static int set_relay(int state) {
    int fd = open_pico();
    if (fd < 0) {
        return 1;
    }

    char cmd[64];
    snprintf(cmd, sizeof(cmd), "r %d\n", state);

    if (send_command(fd, cmd) != 0) {
        close(fd);
        return 1;
    }

    // Print description
    if (state >= 0 && state <= 7) {
        printf("Relay: state %d (%s)\n", state, state_desc[state]);
    } else if (state == 11) {
        printf("Relay: state 11 (special)\n");
    } else {
        printf("Relay: state %d\n", state);
    }

    close(fd);
    return 0;
}

// Query status
static int query_status(void) {
    int fd = open_pico();
    if (fd < 0) {
        return 1;
    }

    if (send_command(fd, "q\n") != 0) {
        close(fd);
        return 1;
    }

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
    fprintf(stderr, "  %s <state>           # Set relay state (0-7, 11)\n", prog);
    fprintf(stderr, "  %s status            # Query current status\n", prog);
    fprintf(stderr, "\n");
    fprintf(stderr, "Relay States:\n");
    for (int i = 0; i <= 7; i++) {
        fprintf(stderr, "  %d = %s\n", i, state_desc[i]);
    }
    fprintf(stderr, "  11 = Special state 11\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "Examples:\n");
    fprintf(stderr, "  %s 0                 # All OFF\n", prog);
    fprintf(stderr, "  %s 3                 # R1+R2 ON\n", prog);
    fprintf(stderr, "  %s 7                 # All ON\n", prog);
    fprintf(stderr, "  %s status            # Show state\n", prog);
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    // Handle "status" command
    if (strcmp(argv[1], "status") == 0 || strcmp(argv[1], "q") == 0) {
        return query_status();
    }

    // Parse state argument
    int state = atoi(argv[1]);

    // Validate state
    if ((state < 0 || state > 7) && state != 11) {
        fprintf(stderr, "ERROR: State must be 0-7 or 11\n");
        usage(argv[0]);
        return 1;
    }

    // Set relay
    return set_relay(state);
}
