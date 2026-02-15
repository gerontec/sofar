#define _POSIX_C_SOURCE 199309L
#define _DEFAULT_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <fcntl.h>
#include <termios.h>
#include <modbus/modbus.h>

// Configuration defaults
#define RTU_PORT "/dev/ttyAMA0"
#define BAUDRATE 9600
#define DEFAULT_SLAVE_ID 1
#define DEFAULT_SOYO_DEVICE_ID 0
#define COIL_START 0
#define NUM_COILS 3
#define STATE_FILE "/run/user/1000/current_relay_state.txt"

// Global configuration (can be overridden via command-line)
static int modbus_slave_id = DEFAULT_SLAVE_ID;
static int soyo_device_id = DEFAULT_SOYO_DEVICE_ID;

// State to bits mapping (same as Python)
typedef struct {
    int state;
    uint8_t bits[3];
} StateMap;

static const StateMap state_map[] = {
    {0, {0, 0, 0}}, {1, {0, 0, 1}}, {2, {0, 1, 0}}, {3, {0, 1, 1}},
    {4, {1, 0, 0}}, {5, {1, 0, 1}}, {6, {1, 1, 0}}, {7, {1, 1, 1}},
    {11, {1, 1, 1}}
};
static const int num_states = sizeof(state_map) / sizeof(state_map[0]);

// Get timestamp in microseconds
long long get_timestamp_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000LL + ts.tv_nsec / 1000LL;
}

// Set baudrate on file descriptor
int set_baudrate(int fd, speed_t speed, const char *label) {
    struct termios tty;

    if (tcgetattr(fd, &tty) != 0) {
        fprintf(stderr, "tcgetattr error: %s\n", strerror(errno));
        return -1;
    }

    cfsetospeed(&tty, speed);
    cfsetispeed(&tty, speed);

    if (tcsetattr(fd, TCSANOW, &tty) != 0) {
        fprintf(stderr, "tcsetattr error (%s): %s\n", label, strerror(errno));
        return -1;
    }

    printf("  Baudrate switched to %s\n", label);
    return 0;
}

// Send Soyo power command (from soyo1min.c)
int send_soyo_command(int fd, int power) {
    unsigned char cmd[8];
    unsigned char pu, pl, crc;
    int n;

    // Build command packet
    pu = (power >> 8) & 0xFF;
    pl = power & 0xFF;
    crc = (264 - pu - pl) & 0xFF;

    cmd[0] = 0x24;  // Header
    cmd[1] = 0x56;  // Header
    cmd[2] = soyo_device_id & 0xFF;  // Device ID (configurable)
    cmd[3] = 0x21;  // Command: Set power limit
    cmd[4] = pu;    // Power upper byte
    cmd[5] = pl;    // Power lower byte
    cmd[6] = 0x80;  // Fixed
    cmd[7] = crc;   // Checksum

    // Send command
    n = write(fd, cmd, 8);
    if (n != 8) {
        fprintf(stderr, "Soyo write error: wrote %d/8 bytes\n", n);
        return -1;
    }

    printf("  Soyo command sent: %dW [%02X %02X %02X %02X %02X %02X %02X %02X]\n",
           power, cmd[0], cmd[1], cmd[2], cmd[3], cmd[4], cmd[5], cmd[6], cmd[7]);

    // Optional: Read response (7 bytes expected)
    unsigned char response[100];
    usleep(50000); // 50ms for response
    n = read(fd, response, sizeof(response));
    if (n > 0) {
        printf("  Soyo response: %d bytes [", n);
        for (int i = 0; i < n; i++) {
            printf("%02X%s", response[i], (i < n-1) ? " " : "");
        }
        printf("]\n");
    }

    return 0;
}

// Find bits for state
const uint8_t* state_to_bits(int state) {
    for (int i = 0; i < num_states; i++) {
        if (state_map[i].state == state) {
            return state_map[i].bits;
        }
    }
    return NULL;
}

// Find state from bits
int bits_to_state(const uint8_t *bits) {
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
int read_relay_status(modbus_t *ctx, int *state, uint8_t *bits) {
    uint8_t read_bits[3];

    if (modbus_read_bits(ctx, COIL_START, NUM_COILS, read_bits) == -1) {
        fprintf(stderr, "Modbus read error: %s\n", modbus_strerror(errno));
        return -1;
    }

    memcpy(bits, read_bits, 3);
    *state = bits_to_state(bits);

    return 0;
}

// Write relay status via Modbus
int write_relay_status(modbus_t *ctx, int state) {
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
int read_state_file(void) {
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
void write_state_file(int state) {
    FILE *fp = fopen(STATE_FILE, "w");
    if (!fp) return;

    fprintf(fp, "%d\n", state);
    fclose(fp);
}

// Show status (like Python's show_status)
void show_status(modbus_t *ctx) {
    int modbus_state = -1;
    uint8_t modbus_bits[3] = {0};
    int file_state;

    printf("============================================================\n");
    printf("RELAY STATUS (C Version)\n");
    printf("============================================================\n");

    // Try reading from Modbus
    if (read_relay_status(ctx, &modbus_state, modbus_bits) == 0) {
        printf("Modbus:     State %d → Bits [%d, %d, %d]\n",
               modbus_state, modbus_bits[0], modbus_bits[1], modbus_bits[2]);
    } else {
        printf("Modbus:     Read error - %s\n", modbus_strerror(errno));
        printf("            (Ebyte module may only support WRITE)\n");
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

    printf("============================================================\n");
    printf("Port:      %s\n", RTU_PORT);
    printf("Baudrate:  %d\n", BAUDRATE);
    printf("Slave ID:  %d (Modbus RTU)\n", modbus_slave_id);
    printf("Soyo ID:   %d (Non-Modbus)\n", soyo_device_id);
    printf("States:    ");
    for (int i = 0; i < num_states; i++) {
        printf("%d%s", state_map[i].state, (i < num_states - 1) ? ", " : "\n");
    }
    printf("============================================================\n");
}

// Multiplexing test: Soyo @ 4800 + Relay @ 9600
void multiplex_test(modbus_t *ctx, int soyo_power) {
    int fd;
    long long t_start, t1, t2, t3, t4, t5;

    printf("\n=== MULTIPLEXING TEST: Soyo @ 4800 + Relay @ 9600 ===\n\n");
    printf("Configuration:\n");
    printf("  Modbus Slave ID: %d (Ebyte Relay)\n", modbus_slave_id);
    printf("  Soyo Device ID:  %d (Soyo Inverter)\n", soyo_device_id);
    printf("  Target Power:    %dW\n\n", soyo_power);
    printf("Scenario: Alternating between Soyo inverter and Relay control\n");
    printf("  1. Relay command @ 9600 baud (Modbus RTU, Slave %d)\n", modbus_slave_id);
    printf("  2. Switch to 4800 baud\n");
    printf("  3. Soyo command @ 4800 baud (Device %d, %dW)\n", soyo_device_id, soyo_power);
    printf("  4. Switch back to 9600 baud\n");
    printf("  5. Relay command @ 9600 baud (Modbus RTU, Slave %d)\n", modbus_slave_id);
    printf("\n");

    // Get underlying file descriptor from modbus context
    fd = modbus_get_socket(ctx);
    if (fd == -1) {
        fprintf(stderr, "Error: Could not get file descriptor from Modbus context\n");
        return;
    }

    t_start = get_timestamp_us();

    // Step 1: Relay command @ 9600 (already connected)
    printf("Step 1: Relay @ 9600 baud (Modbus)\n");
    t1 = get_timestamp_us();
    if (write_relay_status(ctx, 7) == 0) {
        printf("  ✓ Relay set to state 7 (all ON)\n");
    }
    t2 = get_timestamp_us();
    printf("  Time: %lld µs (%.2f ms)\n\n", t2 - t1, (t2 - t1) / 1000.0);

    // Step 2: Switch to 4800 baud
    printf("Step 2: Switch to 4800 baud\n");
    t2 = get_timestamp_us();
    if (set_baudrate(fd, B4800, "4800") != 0) {
        fprintf(stderr, "Failed to switch to 4800 baud\n");
        return;
    }
    t3 = get_timestamp_us();
    printf("  Time: %lld µs (%.2f ms)\n\n", t3 - t2, (t3 - t2) / 1000.0);

    usleep(10000); // 10ms settle time

    // Step 3: Soyo command @ 4800
    printf("Step 3: Soyo @ 4800 baud\n");
    t3 = get_timestamp_us();
    if (send_soyo_command(fd, soyo_power) != 0) {
        fprintf(stderr, "Failed to send Soyo command\n");
    }
    t4 = get_timestamp_us();
    printf("  Time: %lld µs (%.2f ms)\n\n", t4 - t3, (t4 - t3) / 1000.0);

    // Step 4: Switch back to 9600 baud
    printf("Step 4: Switch back to 9600 baud\n");
    t4 = get_timestamp_us();
    if (set_baudrate(fd, B9600, "9600") != 0) {
        fprintf(stderr, "Failed to switch back to 9600 baud\n");
        return;
    }
    t5 = get_timestamp_us();
    printf("  Time: %lld µs (%.2f ms)\n\n", t5 - t4, (t5 - t4) / 1000.0);

    usleep(10000); // 10ms settle time

    // Step 5: Relay command @ 9600 again
    printf("Step 5: Relay @ 9600 baud (Modbus)\n");
    t5 = get_timestamp_us();
    if (write_relay_status(ctx, 0) == 0) {
        printf("  ✓ Relay set to state 0 (all OFF)\n");
    }
    long long t6 = get_timestamp_us();
    printf("  Time: %lld µs (%.2f ms)\n\n", t6 - t5, (t6 - t5) / 1000.0);

    // Summary
    long long total = t6 - t_start;
    printf("========================================\n");
    printf("TOTAL TIME: %lld µs (%.2f ms)\n", total, total / 1000.0);
    printf("========================================\n");
    printf("\nBreakdown:\n");
    printf("  Relay write 1:    %6.2f ms\n", (t2 - t1) / 1000.0);
    printf("  Baudrate to 4800: %6.2f ms\n", (t3 - t2) / 1000.0);
    printf("  Soyo command:     %6.2f ms\n", (t4 - t3) / 1000.0);
    printf("  Baudrate to 9600: %6.2f ms\n", (t5 - t4) / 1000.0);
    printf("  Relay write 2:    %6.2f ms\n", (t6 - t5) / 1000.0);
    printf("  --------------------------------\n");
    printf("  Total overhead:   %6.2f ms\n", total / 1000.0);
    printf("\nFor 2-second slot:\n");
    printf("  Overhead: %.1f%% of 2000ms\n", (total / 1000.0) / 20.0);
    printf("\n");
}

// Send Soyo command only (no relay control)
// Optimized for production: switches baud, sends Soyo, switches back
void send_soyo_only(modbus_t *ctx, int soyo_power) {
    int fd;
    long long t_start, t1, t2, t3;

    // Get file descriptor
    fd = modbus_get_socket(ctx);
    if (fd == -1) {
        fprintf(stderr, "Error: Could not get file descriptor from Modbus context\n");
        return;
    }

    t_start = get_timestamp_us();

    // Step 1: Switch to 4800 baud
    if (set_baudrate(fd, B4800, "4800") != 0) {
        fprintf(stderr, "Failed to switch to 4800 baud\n");
        return;
    }
    usleep(10000); // 10ms settle time
    t1 = get_timestamp_us();

    // Step 2: Send Soyo command @ 4800
    if (send_soyo_command(fd, soyo_power) != 0) {
        fprintf(stderr, "Failed to send Soyo command\n");
    }
    t2 = get_timestamp_us();

    // Step 3: Switch back to 9600 baud
    if (set_baudrate(fd, B9600, "9600") != 0) {
        fprintf(stderr, "Failed to switch back to 9600 baud\n");
        return;
    }
    usleep(10000); // 10ms settle time
    t3 = get_timestamp_us();

    // Print result (compact for production)
    long long total = t3 - t_start;
    printf("Soyo: %dW sent (Device %d) - %.2f ms\n",
           soyo_power, soyo_device_id, total / 1000.0);
}

// Benchmark: Compare C vs Python timing
void benchmark_mode(modbus_t *ctx) {
    printf("\n=== MODBUS RELAY BENCHMARK (C vs Python) ===\n\n");

    // Test write operations
    int test_states[] = {0, 7, 0, 5, 0};
    int num_tests = sizeof(test_states) / sizeof(test_states[0]);

    printf("Test 1: Single write operations\n");
    printf("--------------------------------\n");

    long long total_time = 0;

    for (int i = 0; i < num_tests; i++) {
        int state = test_states[i];
        const uint8_t *bits = state_to_bits(state);

        printf("  Write state %d [%d,%d,%d]... ",
               state, bits[0], bits[1], bits[2]);
        fflush(stdout);

        long long t1 = get_timestamp_us();
        int result = write_relay_status(ctx, state);
        long long t2 = get_timestamp_us();

        if (result == 0) {
            long long elapsed = t2 - t1;
            total_time += elapsed;
            printf("✓ %lld µs (%.2f ms)\n", elapsed, elapsed / 1000.0);
        } else {
            printf("✗ FAILED\n");
        }

        usleep(100000); // 100ms between tests
    }

    printf("\n  Average write time: %.2f ms\n", total_time / (num_tests * 1000.0));

    // Test rapid switching
    printf("\nTest 2: Rapid state switching (100 iterations)\n");
    printf("-----------------------------------------------\n");

    int iterations = 100;
    long long switch_time = 0;

    long long t_start = get_timestamp_us();

    for (int i = 0; i < iterations; i++) {
        int state = (i % 2 == 0) ? 0 : 7;
        write_relay_status(ctx, state);
    }

    long long t_end = get_timestamp_us();
    switch_time = t_end - t_start;

    printf("  Total time: %.2f ms\n", switch_time / 1000.0);
    printf("  Per switch: %.2f ms\n", switch_time / (iterations * 1000.0));
    printf("  Throughput: %.1f switches/second\n",
           (iterations * 1000000.0) / switch_time);

    // Compare with Python (rough estimate)
    printf("\n=== COMPARISON ===\n");
    printf("C implementation:      ~%.2f ms per write\n",
           total_time / (num_tests * 1000.0));
    printf("Python (estimated):    ~50-80 ms per write\n");
    printf("Speedup factor:        ~%.1fx faster\n",
           60.0 / (total_time / (num_tests * 1000.0)));

    printf("\nNote: Run same test with Python for exact comparison:\n");
    printf("      time python3 ebyteserrequest.py [STATE]\n");
}

// Print usage
void print_usage(const char *prog) {
    printf("Usage: %s [OPTIONS] [STATE]\n", prog);
    printf("\n");
    printf("Options:\n");
    printf("  -h, --help                Show this help\n");
    printf("  -s, --status              Show current status\n");
    printf("  -b, --benchmark           Run performance benchmark\n");
    printf("  -r, --read                Read current relay state\n");
    printf("      --soyo POWER          Send Soyo command only (no relay control)\n");
    printf("  -m, --multiplex POWER     Test multiplexing (Soyo @ 4800 + Relay @ 9600)\n");
    printf("\n");
    printf("Configuration:\n");
    printf("  --slave-id ID             Modbus RTU slave ID (default: %d)\n", DEFAULT_SLAVE_ID);
    printf("  --soyo-id ID              Soyo device ID (default: %d)\n", DEFAULT_SOYO_DEVICE_ID);
    printf("\n");
    printf("STATE: 0-7, 11 (relay state to set)\n");
    printf("\n");
    printf("Examples:\n");
    printf("  %s --status                      # Show current status\n", prog);
    printf("  %s 7                             # Set all relays ON\n", prog);
    printf("  %s 0                             # Set all relays OFF\n", prog);
    printf("  %s --benchmark                   # Performance test\n", prog);
    printf("  %s --soyo 200                    # Send 200W to Soyo (production)\n", prog);
    printf("  %s --multiplex 300               # Multiplex: Soyo 300W + Relay\n", prog);
    printf("  %s --slave-id 2 --status         # Status with slave ID 2\n", prog);
    printf("  %s --soyo-id 1 --soyo 500        # Soyo: Device 1, 500W\n", prog);
}

int main(int argc, char *argv[]) {
    modbus_t *ctx;
    int rc = 0;
    int i;

    // Parse configuration options first
    for (i = 1; i < argc; i++) {
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
            printf("Config: Modbus Slave ID set to %d\n", modbus_slave_id);
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
            printf("Config: Soyo Device ID set to %d\n", soyo_device_id);
            // Remove these arguments
            memmove(&argv[i], &argv[i + 2], (argc - i - 2) * sizeof(char*));
            argc -= 2;
            i--;
        }
    }

    // Check if we have at least one command argument left
    if (argc < 2) {
        print_usage(argv[0]);
        return 1;
    }

    // Create Modbus RTU context
    ctx = modbus_new_rtu(RTU_PORT, BAUDRATE, 'N', 8, 1);
    if (!ctx) {
        fprintf(stderr, "Failed to create Modbus context: %s\n", strerror(errno));
        return 1;
    }

    // Set slave ID (from config)
    modbus_set_slave(ctx, modbus_slave_id);

    // Set timeouts (response timeout: 1000ms, byte timeout: 500ms)
    modbus_set_response_timeout(ctx, 1, 0);
    modbus_set_byte_timeout(ctx, 0, 500000);

    // Connect
    printf("EbyteRTU-C: Connecting to %s @ %d baud...\n", RTU_PORT, BAUDRATE);

    if (modbus_connect(ctx) == -1) {
        fprintf(stderr, "Connection failed: %s\n", modbus_strerror(errno));
        modbus_free(ctx);
        return 1;
    }

    printf("EbyteRTU-C: Connected successfully\n");

    // Handle commands
    if (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0) {
        print_usage(argv[0]);
    }
    else if (strcmp(argv[1], "-s") == 0 || strcmp(argv[1], "--status") == 0) {
        show_status(ctx);
    }
    else if (strcmp(argv[1], "-b") == 0 || strcmp(argv[1], "--benchmark") == 0) {
        benchmark_mode(ctx);
    }
    else if (strcmp(argv[1], "--soyo") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Error: --soyo requires POWER argument\n");
            fprintf(stderr, "Usage: %s --soyo POWER\n", argv[0]);
            fprintf(stderr, "Example: %s --soyo 200\n", argv[0]);
            modbus_close(ctx);
            modbus_free(ctx);
            return 1;
        }

        int power = atoi(argv[2]);
        if (power < 0 || power > 3000) {
            fprintf(stderr, "Warning: Power %dW out of typical range (0-3000W)\n", power);
        }

        send_soyo_only(ctx, power);
    }
    else if (strcmp(argv[1], "-m") == 0 || strcmp(argv[1], "--multiplex") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Error: --multiplex requires POWER argument\n");
            fprintf(stderr, "Usage: %s --multiplex POWER\n", argv[0]);
            fprintf(stderr, "Example: %s --multiplex 300\n", argv[0]);
            modbus_close(ctx);
            modbus_free(ctx);
            return 1;
        }

        int power = atoi(argv[2]);
        if (power < 0 || power > 3000) {
            fprintf(stderr, "Warning: Power %dW out of typical range (0-3000W)\n", power);
        }

        multiplex_test(ctx, power);
    }
    else if (strcmp(argv[1], "-r") == 0 || strcmp(argv[1], "--read") == 0) {
        int state;
        uint8_t bits[3];

        if (read_relay_status(ctx, &state, bits) == 0) {
            printf("Current state: %d [%d, %d, %d]\n",
                   state, bits[0], bits[1], bits[2]);
        } else {
            printf("Read failed (device may not support reading)\n");
            rc = 1;
        }
    }
    else {
        // Write state
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
    printf("EbyteRTU-C: Connection closed\n");

    return rc;
}
