/**
 * Raspberry Pi Pico Dual-Core Modbus Controller
 *
 * Architecture:
 * - Core 0: SOYO inverter control (UART0, 4800 baud, 3s cycle, Priority 1)
 * - Core 1: Relay control (UART1, 9600 baud, event-driven, Priority 2)
 * - Public RAM: Shared variables for inter-core communication
 *
 * Hardware:
 * - UART0 (GP0/GP1): SOYO @ 4800 baud
 * - UART1 (GP4/GP5): Relay @ 9600 baud
 * - LED (GP25): Status indicator
 *
 * Build: Use Pico SDK CMake toolchain
 */

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include "pico/stdlib.h"
#include "pico/multicore.h"
#include "pico/mutex.h"
#include "hardware/uart.h"
#include "hardware/gpio.h"
#include "hardware/timer.h"
#include "hardware/watchdog.h"

// ============================================================================
// HARDWARE CONFIGURATION
// ============================================================================

// UART0 for SOYO (4800 baud)
#define SOYO_UART       uart0
#define SOYO_UART_TX    0
#define SOYO_UART_RX    1
#define SOYO_BAUD       4800

// UART1 for Relay (9600 baud)
#define RELAY_UART      uart1
#define RELAY_UART_TX   4
#define RELAY_UART_RX   5
#define RELAY_BAUD      9600

// Timing
#define SOYO_CYCLE_MS   3000    // 3 seconds
#define RELAY_CHECK_MS  100     // Fast polling for changes
#define LED_PIN         25      // Onboard LED

// Modbus defaults
#define RELAY_SLAVE_ID  1
#define SOYO_DEVICE_ID  1

// ============================================================================
// PUBLIC RAM - Shared Variables (Inter-Core Communication)
// ============================================================================

typedef struct {
    // SOYO control (written by external controller, read by Core 0)
    volatile uint16_t soyo_target_watts;    // Target power in watts
    volatile bool     soyo_enable;          // Enable/disable SOYO

    // Relay control (written by external controller, read by Core 1)
    volatile uint8_t  relay_target_state;   // Target relay state (0-7, 11)
    volatile bool     relay_needs_update;   // Flag: state changed
    volatile bool     relay_status_request; // Flag: read status

    // Status feedback (written by cores, read by external)
    volatile uint8_t  relay_current_state;  // Last confirmed state (0-7, 11)
    volatile uint8_t  relay_current_bits;   // Raw 3-bit pattern: bit0=R1, bit1=R2, bit2=R3
    volatile int16_t  soyo_last_watts;      // Last sent watts
    volatile uint32_t soyo_cycle_count;     // Heartbeat counter
    volatile uint32_t relay_cycle_count;    // Heartbeat counter
    volatile uint8_t  error_flags;          // Bit flags for errors

    // Mutual exclusion
    mutex_t mutex;
} PublicRAM;

// Global shared memory
static PublicRAM g_public_ram = {
    .soyo_target_watts = 0,
    .soyo_enable = false,
    .relay_target_state = 0,
    .relay_needs_update = false,
    .relay_status_request = false,
    .relay_current_state = 0xFF,
    .relay_current_bits = 0x00,
    .soyo_last_watts = -1,
    .soyo_cycle_count = 0,
    .relay_cycle_count = 0,
    .error_flags = 0
};

// Error flags
#define ERR_SOYO_TIMEOUT    (1 << 0)
#define ERR_SOYO_CRC        (1 << 1)
#define ERR_RELAY_TIMEOUT   (1 << 2)
#define ERR_RELAY_CRC       (1 << 3)

// ============================================================================
// STATE MAPPING (same as Python/original C)
// ============================================================================

typedef struct {
    uint8_t state;
    uint8_t relay1;
    uint8_t relay2;
    uint8_t relay3;
} StateMap;

static const StateMap STATE_MAP[] = {
    {0,  0, 0, 0},  // All OFF
    {1,  1, 0, 0},  // R1 ON
    {2,  0, 1, 0},  // R2 ON
    {3,  1, 1, 0},  // R1+R2 ON
    {4,  0, 0, 1},  // R3 ON
    {5,  1, 0, 1},  // R1+R3 ON
    {6,  0, 1, 1},  // R2+R3 ON
    {7,  1, 1, 1},  // All ON
    {11, 0, 0, 0}   // "State 11" (special)
};
#define STATE_MAP_SIZE (sizeof(STATE_MAP) / sizeof(StateMap))

// ============================================================================
// CRC CALCULATION (Modbus RTU)
// ============================================================================

static uint16_t crc16_modbus(const uint8_t *data, size_t len) {
    uint16_t crc = 0xFFFF;
    for (size_t i = 0; i < len; i++) {
        crc ^= data[i];
        for (int j = 0; j < 8; j++) {
            if (crc & 0x0001) {
                crc = (crc >> 1) ^ 0xA001;
            } else {
                crc >>= 1;
            }
        }
    }
    return crc;
}

// ============================================================================
// SOYO INTERFACE (UART0, Core 0, Priority 1)
// ============================================================================

// Send 8-byte Soyo command
static bool send_soyo_command(uint16_t watts) {
    uint8_t cmd[8];

    // Build command frame
    cmd[0] = SOYO_DEVICE_ID;
    cmd[1] = 0x06;  // Function code: Write Single Register
    cmd[2] = 0x0B;  // Register address high
    cmd[3] = 0xB8;  // Register address low (0x0BB8 = 3000)
    cmd[4] = (watts >> 8) & 0xFF;  // Data high
    cmd[5] = watts & 0xFF;         // Data low

    // Calculate CRC
    uint16_t crc = crc16_modbus(cmd, 6);
    cmd[6] = crc & 0xFF;        // CRC low
    cmd[7] = (crc >> 8) & 0xFF; // CRC high

    // Clear RX buffer
    while (uart_is_readable(SOYO_UART)) {
        uart_getc(SOYO_UART);
    }

    // Send command
    uart_write_blocking(SOYO_UART, cmd, 8);

    // Wait for echo (8 bytes response)
    uint8_t resp[8];
    absolute_time_t timeout = make_timeout_time_ms(500);
    for (int i = 0; i < 8; i++) {
        while (!uart_is_readable(SOYO_UART)) {
            if (time_reached(timeout)) {
                g_public_ram.error_flags |= ERR_SOYO_TIMEOUT;
                return false;
            }
            sleep_us(100);
        }
        resp[i] = uart_getc(SOYO_UART);
    }

    // Verify CRC
    uint16_t recv_crc = resp[6] | (resp[7] << 8);
    uint16_t calc_crc = crc16_modbus(resp, 6);
    if (recv_crc != calc_crc) {
        g_public_ram.error_flags |= ERR_SOYO_CRC;
        return false;
    }

    // Success
    g_public_ram.error_flags &= ~(ERR_SOYO_TIMEOUT | ERR_SOYO_CRC);
    return true;
}

// Timer callback for SOYO (runs every 3 seconds)
static bool soyo_timer_callback(struct repeating_timer *t) {
    (void)t;  // Unused

    // Read target from public RAM
    uint16_t target_watts = g_public_ram.soyo_target_watts;
    bool enable = g_public_ram.soyo_enable;

    if (!enable) {
        target_watts = 0;  // Force off if disabled
    }

    // Send command
    bool ok = send_soyo_command(target_watts);

    // Update status
    if (ok) {
        g_public_ram.soyo_last_watts = target_watts;
        gpio_put(LED_PIN, 1);  // Blink LED on success
        sleep_ms(50);
        gpio_put(LED_PIN, 0);
    }

    g_public_ram.soyo_cycle_count++;

    return true;  // Continue repeating
}

// ============================================================================
// RELAY INTERFACE (UART1, Core 1, Priority 2)
// ============================================================================

// Read relay status (Modbus Read Coils)
static bool read_relay_status(uint8_t *state) {
    uint8_t cmd[8];

    // Build Modbus request: Read Coils (function 0x01)
    cmd[0] = RELAY_SLAVE_ID;
    cmd[1] = 0x01;  // Read Coils
    cmd[2] = 0x00;  // Start address high
    cmd[3] = 0x00;  // Start address low
    cmd[4] = 0x00;  // Quantity high
    cmd[5] = 0x03;  // Quantity low (3 relays)

    uint16_t crc = crc16_modbus(cmd, 6);
    cmd[6] = crc & 0xFF;
    cmd[7] = (crc >> 8) & 0xFF;

    // Clear RX buffer
    while (uart_is_readable(RELAY_UART)) {
        uart_getc(RELAY_UART);
    }

    // Send command
    uart_write_blocking(RELAY_UART, cmd, 8);

    // Read response (5 bytes expected)
    uint8_t resp[8];
    absolute_time_t timeout = make_timeout_time_ms(1000);
    for (int i = 0; i < 5; i++) {
        while (!uart_is_readable(RELAY_UART)) {
            if (time_reached(timeout)) {
                g_public_ram.error_flags |= ERR_RELAY_TIMEOUT;
                return false;
            }
            sleep_us(100);
        }
        resp[i] = uart_getc(RELAY_UART);
    }

    // Verify CRC
    uint16_t recv_crc = resp[3] | (resp[4] << 8);
    uint16_t calc_crc = crc16_modbus(resp, 3);
    if (recv_crc != calc_crc) {
        g_public_ram.error_flags |= ERR_RELAY_CRC;
        return false;
    }

    // Decode state from 3-bit pattern
    uint8_t bits = resp[2];
    uint8_t r1 = (bits & 0x01) ? 1 : 0;
    uint8_t r2 = (bits & 0x02) ? 1 : 0;
    uint8_t r3 = (bits & 0x04) ? 1 : 0;

    // Store raw bits in public RAM (bit0=R1, bit1=R2, bit2=R3)
    g_public_ram.relay_current_bits = bits & 0x07;

    // Map to state number
    for (size_t i = 0; i < STATE_MAP_SIZE; i++) {
        if (STATE_MAP[i].relay1 == r1 &&
            STATE_MAP[i].relay2 == r2 &&
            STATE_MAP[i].relay3 == r3) {
            *state = STATE_MAP[i].state;
            g_public_ram.error_flags &= ~(ERR_RELAY_TIMEOUT | ERR_RELAY_CRC);
            return true;
        }
    }

    *state = 0xFF;  // Unknown state
    return false;
}

// Write relay status (Modbus Write Multiple Coils)
static bool write_relay_status(uint8_t state) {
    // Find state in map
    const StateMap *map = NULL;
    for (size_t i = 0; i < STATE_MAP_SIZE; i++) {
        if (STATE_MAP[i].state == state) {
            map = &STATE_MAP[i];
            break;
        }
    }

    if (map == NULL) {
        return false;  // Invalid state
    }

    // Build Modbus request: Write Multiple Coils (function 0x0F)
    uint8_t cmd[10];
    cmd[0] = RELAY_SLAVE_ID;
    cmd[1] = 0x0F;  // Write Multiple Coils
    cmd[2] = 0x00;  // Start address high
    cmd[3] = 0x00;  // Start address low
    cmd[4] = 0x00;  // Quantity high
    cmd[5] = 0x03;  // Quantity low (3 relays)
    cmd[6] = 0x01;  // Byte count
    cmd[7] = (map->relay1 ? 0x01 : 0x00) |
             (map->relay2 ? 0x02 : 0x00) |
             (map->relay3 ? 0x04 : 0x00);

    uint16_t crc = crc16_modbus(cmd, 8);
    cmd[8] = crc & 0xFF;
    cmd[9] = (crc >> 8) & 0xFF;

    // Clear RX buffer
    while (uart_is_readable(RELAY_UART)) {
        uart_getc(RELAY_UART);
    }

    // Send command
    uart_write_blocking(RELAY_UART, cmd, 10);

    // Read response (8 bytes expected)
    uint8_t resp[8];
    absolute_time_t timeout = make_timeout_time_ms(1000);
    for (int i = 0; i < 8; i++) {
        while (!uart_is_readable(RELAY_UART)) {
            if (time_reached(timeout)) {
                g_public_ram.error_flags |= ERR_RELAY_TIMEOUT;
                return false;
            }
            sleep_us(100);
        }
        resp[i] = uart_getc(RELAY_UART);
    }

    // Verify CRC
    uint16_t recv_crc = resp[6] | (resp[7] << 8);
    uint16_t calc_crc = crc16_modbus(resp, 6);
    if (recv_crc != calc_crc) {
        g_public_ram.error_flags |= ERR_RELAY_CRC;
        return false;
    }

    // Success
    g_public_ram.error_flags &= ~(ERR_RELAY_TIMEOUT | ERR_RELAY_CRC);
    return true;
}

// Helper: Convert state number to 3-bit pattern
static uint8_t state_to_bits(uint8_t state) {
    for (size_t i = 0; i < STATE_MAP_SIZE; i++) {
        if (STATE_MAP[i].state == state) {
            return (STATE_MAP[i].relay1 ? 0x01 : 0x00) |
                   (STATE_MAP[i].relay2 ? 0x02 : 0x00) |
                   (STATE_MAP[i].relay3 ? 0x04 : 0x00);
        }
    }
    return 0x00;  // Default to all OFF
}

// Core 1 main loop (event-driven relay control)
static void core1_relay_handler(void) {
    uint8_t last_state = 0xFF;

    // Read initial relay status at startup
    sleep_ms(500);  // Wait for hardware to settle
    uint8_t initial_state;
    if (read_relay_status(&initial_state)) {
        last_state = initial_state;
        g_public_ram.relay_current_state = initial_state;
        // relay_current_bits already set by read_relay_status()
    }

    while (true) {
        // Check for status read request
        if (g_public_ram.relay_status_request) {
            uint8_t current;
            if (read_relay_status(&current)) {
                g_public_ram.relay_current_state = current;
                last_state = current;
            }
            g_public_ram.relay_status_request = false;
        }

        // Check for state change request
        if (g_public_ram.relay_needs_update) {
            uint8_t target = g_public_ram.relay_target_state;

            // Only update if different from last known state
            if (target != last_state) {
                if (write_relay_status(target)) {
                    last_state = target;
                    g_public_ram.relay_current_state = target;
                    g_public_ram.relay_current_bits = state_to_bits(target);
                }
            }

            g_public_ram.relay_needs_update = false;
            g_public_ram.relay_cycle_count++;
        }

        // Sleep briefly to avoid busy-waiting
        sleep_ms(RELAY_CHECK_MS);
    }
}

// ============================================================================
// INITIALIZATION & MAIN
// ============================================================================

static void init_hardware(void) {
    // Initialize stdio (USB CDC)
    stdio_init_all();

    // Initialize LED
    gpio_init(LED_PIN);
    gpio_set_dir(LED_PIN, GPIO_OUT);
    gpio_put(LED_PIN, 0);

    // Initialize UART0 for SOYO (4800 baud)
    uart_init(SOYO_UART, SOYO_BAUD);
    gpio_set_function(SOYO_UART_TX, GPIO_FUNC_UART);
    gpio_set_function(SOYO_UART_RX, GPIO_FUNC_UART);
    uart_set_format(SOYO_UART, 8, 1, UART_PARITY_NONE);

    // Initialize UART1 for Relay (9600 baud)
    uart_init(RELAY_UART, RELAY_BAUD);
    gpio_set_function(RELAY_UART_TX, GPIO_FUNC_UART);
    gpio_set_function(RELAY_UART_RX, GPIO_FUNC_UART);
    uart_set_format(RELAY_UART, 8, 1, UART_PARITY_NONE);

    // Initialize mutex
    mutex_init(&g_public_ram.mutex);

    printf("\n=== Pico Modbus Dual-Core Controller ===\n");
    printf("SOYO:  UART0 @ %d baud (GP%d/GP%d)\n", SOYO_BAUD, SOYO_UART_TX, SOYO_UART_RX);
    printf("Relay: UART1 @ %d baud (GP%d/GP%d)\n", RELAY_BAUD, RELAY_UART_TX, RELAY_UART_RX);
}

int main(void) {
    // Initialize hardware
    init_hardware();

    // Enable watchdog (8s timeout)
    watchdog_enable(8000, true);

    // Launch Core 1 for relay control
    multicore_launch_core1(core1_relay_handler);
    printf("Core 1 launched (Relay handler)\n");

    // Setup SOYO timer on Core 0 (3 second cycle)
    struct repeating_timer soyo_timer;
    if (!add_repeating_timer_ms(SOYO_CYCLE_MS, soyo_timer_callback, NULL, &soyo_timer)) {
        printf("ERROR: Failed to create SOYO timer!\n");
        return 1;
    }
    printf("Core 0 SOYO timer started (3s cycle)\n");

    // Main loop on Core 0: Command interface via USB serial
    printf("\nReady! Use serial commands:\n");
    printf("  s <watts>     - Set SOYO target watts\n");
    printf("  r <state>     - Set relay state (0-7, 11)\n");
    printf("  q             - Query status\n");
    printf("  e [0|1]       - Enable/disable SOYO\n");

    char line[64];
    int line_idx = 0;

    while (true) {
        watchdog_update();

        // Read commands from USB serial (non-blocking)
        int c = getchar_timeout_us(0);
        if (c == PICO_ERROR_TIMEOUT) {
            sleep_ms(10);
            continue;
        }

        if (c == '\n' || c == '\r') {
            if (line_idx > 0) {
                line[line_idx] = '\0';

                // Parse command
                char cmd = line[0];
                int arg = 0;
                if (line_idx > 2) {
                    arg = atoi(&line[2]);
                }

                switch (cmd) {
                    case 's':  // Set SOYO watts
                        g_public_ram.soyo_target_watts = arg;
                        printf("SOYO target: %d W\n", arg);
                        break;

                    case 'r':  // Set relay state
                        g_public_ram.relay_target_state = arg;
                        g_public_ram.relay_needs_update = true;
                        printf("Relay target: %d\n", arg);
                        break;

                    case 'q':  // Query status
                        printf("\n--- Status ---\n");
                        printf("SOYO:  %d W (cycles: %u, errors: 0x%02X)\n",
                               g_public_ram.soyo_last_watts,
                               g_public_ram.soyo_cycle_count,
                               g_public_ram.error_flags & 0x03);
                        printf("Relay: state %d, bits 0x%02X [R1=%d R2=%d R3=%d] (cycles: %u, errors: 0x%02X)\n",
                               g_public_ram.relay_current_state,
                               g_public_ram.relay_current_bits,
                               (g_public_ram.relay_current_bits & 0x01) ? 1 : 0,
                               (g_public_ram.relay_current_bits & 0x02) ? 1 : 0,
                               (g_public_ram.relay_current_bits & 0x04) ? 1 : 0,
                               g_public_ram.relay_cycle_count,
                               (g_public_ram.error_flags >> 2) & 0x03);
                        // Request fresh relay status
                        g_public_ram.relay_status_request = true;
                        break;

                    case 'e':  // Enable/disable SOYO
                        g_public_ram.soyo_enable = (arg != 0);
                        printf("SOYO %s\n", arg ? "enabled" : "disabled");
                        break;

                    default:
                        printf("Unknown command: %c\n", cmd);
                }

                line_idx = 0;
            }
        } else if (line_idx < sizeof(line) - 1) {
            line[line_idx++] = c;
        }
    }

    return 0;
}
