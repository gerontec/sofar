/*
 * ebyte.c - Modbus RTU relay control with Soyo inverter support
 * Replacement for ebyteserrequest.py
 *
 * Features:
 * - Modbus RTU relay control (3 coils, states 0-7, 11)
 * - Soyo inverter power control (4800 baud multiplexing)
 * - Fast C implementation for production use
 *
 * Usage:
 *   ebyte <state>                    # Set relay state (0-7, 11)
 *   ebyte --soyo <power>             # Send Soyo power command only
 *   ebyte --status                   # Show current status
 *
 * Compile:
 *   gcc -std=c11 -o ebyte ebyte.c -lmodbus
 */

#define _POSIX_C_SOURCE 199309L
#define _DEFAULT_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdbool.h>
#include <errno.h>
#include <time.h>
#include <fcntl.h>
#include <termios.h>
#include <getopt.h>
#include <modbus/modbus.h>

#define VERSION "1.0.0"
#define RTU_PORT "/dev/ttyAMA0"
#define BAUDRATE 9600
#define DEFAULT_SLAVE_ID 1
#define DEFAULT_SOYO_DEVICE_ID 0
#define SOYO_BAUDRATE 4800
#define COIL_START 0
#define NUM_COILS 3
#define STATE_FILE "/run/user/1000/current_relay_state.txt"

// Global configuration (can be overridden via command-line)
static int modbus_slave_id = DEFAULT_SLAVE_ID;
static int soyo_device_id = DEFAULT_SOYO_DEVICE_ID;

// Relay state to bits mapping
typedef struct {
    int state;
    uint8_t bits[3];
} StateMap;

static const StateMap state_map[] = {
    {0, {0, 0, 0}},
    {1, {0, 0, 1}},
    {2, {0, 1, 0}},
    {3, {0, 1, 1}},
    {4, {1, 0, 0}},
    {5, {1, 0, 1}},
    {6, {1, 1, 0}},
    {7, {1, 1, 1}},
    {11, {1, 1, 1}}  // Special case
};
static const int num_states = sizeof(state_map) / sizeof(state_map[0]);

// Get timestamp in microseconds
static long long get_timestamp_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000LL + ts.tv_nsec / 1000LL;
}

// Set baudrate on file descriptor
static int set_baudrate(int fd, speed_t speed) {
    struct termios tty;
    if (tcgetattr(fd, &tty) != 0) {
        return -1;
    }
    cfsetospeed(&tty, speed);
    cfsetispeed(&tty, speed);
    if (tcsetattr(fd, TCSANOW, &tty) != 0) {
        return -1;
    }
    return 0;
}

// Send Soyo power command
static int send_soyo_command(int fd, int power) {
    unsigned char cmd[8];
    unsigned char pu, pl, crc;

    // Build command packet
    pu = (power >> 8) & 0xFF;
    pl = power & 0xFF;
    crc = (264 - pu - pl) & 0xFF;

    cmd[0] = 0x24;  // Header
    cmd[1] = 0x56;  // Header
    cmd[2] = soyo_device_id & 0xFF;
    cmd[3] = 0x21;  // Command: Set power limit
    cmd[4] = pu;    // Power upper byte
    cmd[5] = pl;    // Power lower byte
    cmd[6] = 0x80;  // Fixed
    cmd[7] = crc;   // Checksum

    // Send command
    int n = write(fd, cmd, 8);
    if (n != 8) {
        fprintf(stderr, "Soyo write error: wrote %d/8 bytes\n", n);
        return -1;
    }

    // Optional: Read response (7 bytes expected)
    unsigned char response[100];
    usleep(50000); // 50ms for response
    n = read(fd, response, sizeof(response));

    return 0;
}

// Find bits for state
static const uint8_t* state_to_bits(int state) {
    for (int i = 0; i < num_states; i++) {
        if (state_map[i].state == state) {
            return state_map[i].bits;
        }
    }
    return NULL;
}

// Find state from bits
static int bits_to_state(const uint8_t *bits) {
    for (int i = 0; i < num_states; i++) {
        if (bits[0] == state_map[i].bits[0] &&
            bits[1] == state_map[i].bits[1] &&
            bits[2] == state_map[i].bits[2]) {
            return state_map[i].state;
        }
    }
    return -1;
}

// Read current relay status via Modbus
static int read_relay_status(modbus_t *ctx, int *state, uint8_t *bits) {
    uint8_t read_bits[3];
    if (modbus_read_bits(ctx, COIL_START, NUM_COILS, read_bits) == -1) {
        return -1;
    }
    memcpy(bits, read_bits, 3);
    *state = bits_to_state(bits);
    return 0;
}

// Write relay status via Modbus
static int write_relay_status(modbus_t *ctx, int state) {
    const uint8_t *bits = state_to_bits(state);
    if (!bits) {
        fprintf(stderr, "Invalid state: %d\n", state);
        return -1;
    }
    if (modbus_write_bits(ctx, COIL_START, NUM_COILS, bits) == -1) {
        fprintf(stderr, "Modbus write error: %s\n", modbus_strerror(errno));
        return -1;
    }
    return 0;
}

// Read state from file
static int read_state_file(void) {
    FILE *fp = fopen(STATE_FILE, "r");
    if (!fp) return -1;
    int state;
    if (fscanf(fp, "%d", &state) != 1) {
        fclose(fp);
        return -1;
    }
    fclose(fp);
    return state;
}

// Write state to file
static void write_state_file(int state) {
    FILE *fp = fopen(STATE_FILE, "w");
    if (!fp) return;
    fprintf(fp, "%d\n", state);
    fclose(fp);
}

// Show status
static void show_status(modbus_t *ctx) {
    int modbus_state = -1;
    uint8_t modbus_bits[3] = {0};
    int file_state;

    printf("=== EBYTE RELAY STATUS ===\n");

    // Try reading from Modbus
    if (read_relay_status(ctx, &modbus_state, modbus_bits) == 0) {
        printf("Modbus:     State %d → Bits [%d, %d, %d]\n",
               modbus_state, modbus_bits[0], modbus_bits[1], modbus_bits[2]);
    } else {
        printf("Modbus:     Read error (device may only support WRITE)\n");
    }

    // Read from state file
    file_state = read_state_file();
    if (file_state >= 0) {
        const uint8_t *bits = state_to_bits(file_state);
        if (bits) {
            printf("State-File: State %d → Bits [%d, %d, %d]\n",
                   file_state, bits[0], bits[1], bits[2]);
        }
    } else {
        printf("State-File: Not available\n");
    }

    printf("========================\n");
    printf("Port:      %s\n", RTU_PORT);
    printf("Baudrate:  %d\n", BAUDRATE);
    printf("Slave ID:  %d\n", modbus_slave_id);
    printf("Valid states: ");
    for (int i = 0; i < num_states; i++) {
        printf("%d%s", state_map[i].state, (i < num_states - 1) ? ", " : "\n");
    }
}

// Send Soyo command only (with baudrate switching)
static int send_soyo_only(modbus_t *ctx, int soyo_power) {
    int fd = modbus_get_socket(ctx);
    if (fd == -1) {
        fprintf(stderr, "Error: Could not get file descriptor from Modbus context\n");
        return 1;
    }

    long long t_start = get_timestamp_us();

    // Switch to 4800 baud
    if (set_baudrate(fd, B4800) != 0) {
        fprintf(stderr, "Failed to switch to 4800 baud\n");
        return 1;
    }
    usleep(10000); // 10ms settle time

    // Send Soyo command
    if (send_soyo_command(fd, soyo_power) != 0) {
        fprintf(stderr, "Failed to send Soyo command\n");
    }

    // Switch back to 9600 baud
    if (set_baudrate(fd, B9600) != 0) {
        fprintf(stderr, "Failed to switch back to 9600 baud\n");
        return 1;
    }
    usleep(10000); // 10ms settle time

    long long t_end = get_timestamp_us();
    printf("Soyo: %dW sent (Device %d) - %.2f ms\n",
           soyo_power, soyo_device_id, (t_end - t_start) / 1000.0);

    return 0;
}

// Print usage
static void print_usage(const char *prog) {
    printf("Usage: %s [OPTIONS] [STATE]\n", prog);
    printf("\n");
    printf("Commands:\n");
    printf("  <state>              Set relay state (0-7, 11)\n");
    printf("  --soyo <power>       Send Soyo power command only (W)\n");
    printf("  --status, -s         Show current status\n");
    printf("  --help, -h           Show this help\n");
    printf("  --version            Show version\n");
    printf("\n");
    printf("Configuration:\n");
    printf("  --slave-id <id>      Modbus RTU slave ID (default: %d)\n", DEFAULT_SLAVE_ID);
    printf("  --soyo-id <id>       Soyo device ID (default: %d)\n", DEFAULT_SOYO_DEVICE_ID);
    printf("\n");
    printf("Examples:\n");
    printf("  %s 7                     # Set all relays ON\n", prog);
    printf("  %s 0                     # Set all relays OFF\n", prog);
    printf("  %s --soyo 200            # Send 200W to Soyo\n", prog);
    printf("  %s --status              # Show status\n", prog);
    printf("  %s --slave-id 2 7        # Use slave ID 2, set state 7\n", prog);
}

int main(int argc, char *argv[]) {
    modbus_t *ctx;
    int rc = 0;

    // Parse configuration options first
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--slave-id") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "Error: --slave-id requires an argument\n");
                return 1;
            }
            modbus_slave_id = atoi(argv[i + 1]);
            if (modbus_slave_id < 1 || modbus_slave_id > 247) {
                fprintf(stderr, "Error: Invalid slave ID %d (must be 1-247)\n", modbus_slave_id);
                return 1;
            }
            // Remove these arguments
            memmove(&argv[i], &argv[i + 2], (argc - i - 2) * sizeof(char*));
            argc -= 2;
            i--;
        }
        else if (strcmp(argv[i], "--soyo-id") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "Error: --soyo-id requires an argument\n");
                return 1;
            }
            soyo_device_id = atoi(argv[i + 1]);
            if (soyo_device_id < 0 || soyo_device_id > 255) {
                fprintf(stderr, "Error: Invalid Soyo device ID %d (must be 0-255)\n", soyo_device_id);
                return 1;
            }
            // Remove these arguments
            memmove(&argv[i], &argv[i + 2], (argc - i - 2) * sizeof(char*));
            argc -= 2;
            i--;
        }
    }

    // Check for version
    if (argc >= 2 && strcmp(argv[1], "--version") == 0) {
        printf("ebyte v%s\n", VERSION);
        return 0;
    }

    // Check for help
    if (argc >= 2 && (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0)) {
        print_usage(argv[0]);
        return 0;
    }

    // Require at least one argument
    if (argc < 2) {
        fprintf(stderr, "Error: Missing argument\n\n");
        print_usage(argv[0]);
        return 1;
    }

    // Create Modbus RTU context
    ctx = modbus_new_rtu(RTU_PORT, BAUDRATE, 'N', 8, 1);
    if (!ctx) {
        fprintf(stderr, "Failed to create Modbus context: %s\n", strerror(errno));
        return 1;
    }

    // Set slave ID
    modbus_set_slave(ctx, modbus_slave_id);

    // Set timeouts
    modbus_set_response_timeout(ctx, 1, 0);
    modbus_set_byte_timeout(ctx, 0, 500000);

    // Connect
    if (modbus_connect(ctx) == -1) {
        fprintf(stderr, "Connection failed: %s\n", modbus_strerror(errno));
        modbus_free(ctx);
        return 1;
    }

    // Handle commands
    if (strcmp(argv[1], "-s") == 0 || strcmp(argv[1], "--status") == 0) {
        show_status(ctx);
    }
    else if (strcmp(argv[1], "--soyo") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Error: --soyo requires POWER argument\n");
            modbus_close(ctx);
            modbus_free(ctx);
            return 1;
        }
        int power = atoi(argv[2]);
        rc = send_soyo_only(ctx, power);
    }
    else {
        // Set relay state
        int state = atoi(argv[1]);
        const uint8_t *bits = state_to_bits(state);

        if (!bits) {
            fprintf(stderr, "Invalid state: %d\n", state);
            fprintf(stderr, "Valid states: ");
            for (int i = 0; i < num_states; i++) {
                fprintf(stderr, "%d%s", state_map[i].state,
                        (i < num_states - 1) ? ", " : "\n");
            }
            modbus_close(ctx);
            modbus_free(ctx);
            return 1;
        }

        printf("EbyteRTU-C: Request=%d → Relaisbits=[%d,%d,%d]\n",
               state, bits[0], bits[1], bits[2]);

        long long t1 = get_timestamp_us();
        rc = write_relay_status(ctx, state);
        long long t2 = get_timestamp_us();

        if (rc == 0) {
            printf("EbyteRTU-C: SUCCESS → Relays set to state %d [%d,%d,%d]\n",
                   state, bits[0], bits[1], bits[2]);
            printf("EbyteRTU-C: Operation took %lld µs (%.2f ms)\n",
                   t2 - t1, (t2 - t1) / 1000.0);
            write_state_file(state);
        } else {
            printf("EbyteRTU-C: FAILED - %s\n", modbus_strerror(errno));
        }
    }

    // Cleanup
    modbus_close(ctx);
    modbus_free(ctx);

    return rc;
}
