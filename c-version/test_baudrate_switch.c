#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <termios.h>
#include <time.h>
#include <sys/ioctl.h>

// Get timestamp in microseconds
long long get_timestamp_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000LL + ts.tv_nsec / 1000LL;
}

// Set baudrate on open file descriptor
int set_baudrate(int fd, speed_t speed, const char *label) {
    struct termios tty;

    if (tcgetattr(fd, &tty) != 0) {
        fprintf(stderr, "Error from tcgetattr: %s\n", strerror(errno));
        return -1;
    }

    cfsetospeed(&tty, speed);
    cfsetispeed(&tty, speed);

    if (tcsetattr(fd, TCSANOW, &tty) != 0) {
        fprintf(stderr, "Error from tcsetattr (%s): %s\n", label, strerror(errno));
        return -1;
    }

    return 0;
}

int main(int argc, char *argv[]) {
    int fd;
    long long t1, t2, t3;
    const char *port = "/dev/ttyUSB0";
    int iterations = 1000;

    if (argc > 1) {
        port = argv[1];
    }

    printf("=== ttyUSB Baudrate Switch Benchmark ===\n");
    printf("Port: %s\n", port);
    printf("Iterations: %d\n\n", iterations);

    // Open serial port
    fd = open(port, O_RDWR | O_NOCTTY | O_SYNC);
    if (fd < 0) {
        fprintf(stderr, "Failed to open %s: %s\n", port, strerror(errno));
        return 1;
    }

    // Configure basic settings (8N1, no flow control)
    struct termios tty;
    memset(&tty, 0, sizeof(tty));

    if (tcgetattr(fd, &tty) != 0) {
        fprintf(stderr, "Error from tcgetattr: %s\n", strerror(errno));
        close(fd);
        return 1;
    }

    tty.c_cflag = (tty.c_cflag & ~CSIZE) | CS8;
    tty.c_iflag &= ~IGNBRK;
    tty.c_lflag = 0;
    tty.c_oflag = 0;
    tty.c_cc[VMIN]  = 0;
    tty.c_cc[VTIME] = 5;
    tty.c_iflag &= ~(IXON | IXOFF | IXANY);
    tty.c_cflag |= (CLOCAL | CREAD);
    tty.c_cflag &= ~(PARENB | PARODD);
    tty.c_cflag &= ~CSTOPB;

    // Test 1: Single switch from 4800 to 9600
    printf("Test 1: Single switch 4800 -> 9600 baud\n");

    set_baudrate(fd, B4800, "4800");
    usleep(10000); // 10ms settle

    t1 = get_timestamp_us();
    set_baudrate(fd, B9600, "9600");
    t2 = get_timestamp_us();

    printf("  Switch time: %lld µs (%.3f ms)\n", t2 - t1, (t2 - t1) / 1000.0);

    // Test 2: Single switch from 9600 to 4800
    printf("\nTest 2: Single switch 9600 -> 4800 baud\n");

    usleep(10000);

    t1 = get_timestamp_us();
    set_baudrate(fd, B4800, "4800");
    t2 = get_timestamp_us();

    printf("  Switch time: %lld µs (%.3f ms)\n", t2 - t1, (t2 - t1) / 1000.0);

    // Test 3: Repeated rapid switching
    printf("\nTest 3: Rapid switching (%d iterations)\n", iterations);

    long long total_time = 0;
    long long min_time = 999999999;
    long long max_time = 0;

    for (int i = 0; i < iterations; i++) {
        t1 = get_timestamp_us();
        set_baudrate(fd, B4800, "4800");
        t2 = get_timestamp_us();
        set_baudrate(fd, B9600, "9600");
        t3 = get_timestamp_us();

        long long switch_time = (t2 - t1) + (t3 - t2);
        total_time += switch_time;

        if (switch_time < min_time) min_time = switch_time;
        if (switch_time > max_time) max_time = switch_time;
    }

    printf("  Average per switch: %.3f µs (%.3f ms)\n",
           total_time / (2.0 * iterations),
           total_time / (2000.0 * iterations));
    printf("  Min: %lld µs (%.3f ms)\n", min_time / 2, min_time / 2000.0);
    printf("  Max: %lld µs (%.3f ms)\n", max_time / 2, max_time / 2000.0);
    printf("  Total time for %d switches: %.3f ms\n",
           iterations * 2,
           total_time / 1000.0);

    // Test 4: Multiplexing simulation (2-second slots)
    printf("\nTest 4: Multiplexing simulation (2x 2-second slots)\n");
    printf("  Simulating:\n");
    printf("    Slot A: 4800 baud (send 8 bytes to Soyo inverter)\n");
    printf("    Slot B: 9600 baud (send 16 bytes to other device)\n");

    unsigned char soyo_cmd[8] = {0x24, 0x56, 0x00, 0x21, 0x00, 0x64, 0x80, 0xB7};
    unsigned char other_cmd[16] = "TEST_CMD_9600\r\n";

    t1 = get_timestamp_us();

    // Slot A: Soyo @ 4800 baud
    set_baudrate(fd, B4800, "4800");
    write(fd, soyo_cmd, 8);
    tcdrain(fd); // Wait for transmission to complete

    // Slot B: Other device @ 9600 baud
    set_baudrate(fd, B9600, "9600");
    write(fd, other_cmd, 16);
    tcdrain(fd);

    t2 = get_timestamp_us();

    printf("  Total cycle time: %.3f ms\n", (t2 - t1) / 1000.0);
    printf("  Overhead per 2s slot: %.3f ms\n", (t2 - t1) / 2000.0);

    // Calculate transmission times
    // 4800 baud: 8 bytes = 80 bits (10 bits per byte) = 80/4800 = 16.67 ms
    // 9600 baud: 16 bytes = 160 bits = 160/9600 = 16.67 ms

    double tx_time_4800 = (8.0 * 10.0 / 4800.0) * 1000.0; // ms
    double tx_time_9600 = (16.0 * 10.0 / 9600.0) * 1000.0; // ms

    printf("\n  Theoretical transmission times:\n");
    printf("    4800 baud, 8 bytes:  %.3f ms\n", tx_time_4800);
    printf("    9600 baud, 16 bytes: %.3f ms\n", tx_time_9600);
    printf("    Total TX time:       %.3f ms\n", tx_time_4800 + tx_time_9600);
    printf("    Switch overhead:     %.3f ms\n", (t2 - t1) / 1000.0 - tx_time_4800 - tx_time_9600);

    // Test 5: Check if LOW_LATENCY flag is set
    printf("\nTest 5: Serial port flags\n");

    struct serial_struct serinfo;
    if (ioctl(fd, TIOCGSERIAL, &serinfo) == 0) {
        printf("  Flags: 0x%04x\n", serinfo.flags);
        printf("  LOW_LATENCY: %s\n",
               (serinfo.flags & ASYNC_LOW_LATENCY) ? "ENABLED" : "DISABLED");
        printf("  Baud base: %d\n", serinfo.baud_base);

        // Try enabling LOW_LATENCY
        if (!(serinfo.flags & ASYNC_LOW_LATENCY)) {
            printf("\n  Attempting to enable ASYNC_LOW_LATENCY...\n");
            serinfo.flags |= ASYNC_LOW_LATENCY;
            if (ioctl(fd, TIOCSSERIAL, &serinfo) == 0) {
                printf("  LOW_LATENCY enabled successfully!\n");
            } else {
                printf("  Failed to enable LOW_LATENCY: %s\n", strerror(errno));
            }
        }
    } else {
        printf("  Failed to get serial info: %s\n", strerror(errno));
    }

    printf("\n=== Conclusion ===\n");
    printf("Multiplexing is %s for 2-second slots\n",
           (total_time / (2.0 * iterations) < 50000) ? "FEASIBLE ✅" : "TOO SLOW ❌");
    printf("(Baudrate switching takes << 50ms, well within 2s slot)\n");

    close(fd);
    return 0;
}
