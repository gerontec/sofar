#define _POSIX_C_SOURCE 199309L
#define _DEFAULT_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <modbus/modbus.h>

// Configuration
#define RTU_PORT "/dev/ttyAMA0"
#define BAUDRATE 9600
#define SLAVE_ID 1
#define COIL_START 0
#define NUM_COILS 3
#define STATE_FILE "/run/user/1000/current_relay_state.txt"

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
    printf("Port:   %s\n", RTU_PORT);
    printf("Baud:   %d\n", BAUDRATE);
    printf("Slave:  %d\n", SLAVE_ID);
    printf("States: ");
    for (int i = 0; i < num_states; i++) {
        printf("%d%s", state_map[i].state, (i < num_states - 1) ? ", " : "\n");
    }
    printf("============================================================\n");
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
    printf("  -h, --help        Show this help\n");
    printf("  -s, --status      Show current status\n");
    printf("  -b, --benchmark   Run performance benchmark\n");
    printf("  -r, --read        Read current relay state\n");
    printf("\n");
    printf("STATE: 0-7, 11 (relay state to set)\n");
    printf("\n");
    printf("Examples:\n");
    printf("  %s --status          # Show current status\n", prog);
    printf("  %s 7                 # Set all relays ON\n", prog);
    printf("  %s 0                 # Set all relays OFF\n", prog);
    printf("  %s --benchmark       # Performance test\n", prog);
}

int main(int argc, char *argv[]) {
    modbus_t *ctx;
    int rc = 0;

    // Parse arguments
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

    // Set slave ID
    modbus_set_slave(ctx, SLAVE_ID);

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
