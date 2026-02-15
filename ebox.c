/*
 * ebox.c - Serial communication tool for battery management
 * Replacement for ebox.py and ebox1arg.py
 *
 * Usage:
 *   ebox bat                              # Simple mode: send "bat" and display output
 *   ebox bat 1                            # Simple mode: send "bat 1" and display output
 *   ebox bat --parse                      # Parse mode: parse battery data and call DB script
 *   ebox pwr 1 --device /dev/ttyUSB0      # With custom serial device
 *
 * Compile:
 *   gcc -std=c11 -o ebox ebox.c
 */

#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE  // For CRTSCTS and other BSD extensions

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <termios.h>
#include <errno.h>
#include <sys/select.h>
#include <sys/wait.h>
#include <getopt.h>
#include <stdbool.h>

#define VERSION "1.0.0"
#define MAX_LINES 100
#define MAX_LINE_LEN 512
#define MAX_FIELDS 20

typedef struct {
    char device[256];
    int baudrate;
    int timeout_sec;
    char db_script[512];
    char command[256];      // Increased size for multi-arg commands
    bool parse_mode;        // Parse and process data (ebox1arg.py mode)
    bool verbose;
} Config;

// Storage for all received lines
typedef struct {
    char lines[MAX_LINES][MAX_LINE_LEN];
    int count;
} LineBuffer;

static void init_config(Config *cfg) {
    strcpy(cfg->device, "/dev/ttyUSB23");
    cfg->baudrate = 115200;
    cfg->timeout_sec = 4;
    strcpy(cfg->db_script, "/home/pi/python/pv_ebox2.py");
    cfg->command[0] = '\0';
    cfg->parse_mode = false;
    cfg->verbose = false;
}

static void print_usage(const char *prog) {
    printf("Usage: %s <command> [args...] [OPTIONS]\n\n", prog);
    printf("Simple Mode (display output only):\n");
    printf("  %s bat                       # Send 'bat' command\n", prog);
    printf("  %s bat 1                     # Send 'bat 1' command\n", prog);
    printf("  %s pwr 1                     # Send 'pwr 1' command\n\n", prog);
    printf("Parse Mode (parse battery data and call DB script):\n");
    printf("  %s bat --parse               # Parse battery data from 'bat' command\n", prog);
    printf("  %s bat --parse --verbose     # With verbose output\n\n", prog);
    printf("Options:\n");
    printf("  -p, --parse              Parse battery data and call DB script (ebox1arg.py mode)\n");
    printf("  -d, --device <path>      Serial device path (default: /dev/ttyUSB23)\n");
    printf("  -b, --baudrate <rate>    Baud rate (default: 115200)\n");
    printf("  -t, --timeout <sec>      Read timeout in seconds (default: 4)\n");
    printf("  -s, --db-script <path>   Database script path (default: /home/pi/python/pv_ebox2.py)\n");
    printf("  -v, --verbose            Enable verbose output\n");
    printf("  -h, --help               Show this help\n");
    printf("      --version            Show version\n\n");
    printf("Examples:\n");
    printf("  %s bat                                    # Simple: send 'bat'\n", prog);
    printf("  %s bat 1 --device /dev/ttyUSB0            # Simple: custom device\n", prog);
    printf("  %s bat --parse --verbose                  # Parse mode with verbose output\n", prog);
}

static speed_t get_baudrate(int rate) {
    switch (rate) {
        case 9600: return B9600;
        case 19200: return B19200;
        case 38400: return B38400;
        case 57600: return B57600;
        case 115200: return B115200;
        case 230400: return B230400;
        default:
            fprintf(stderr, "Unsupported baudrate: %d\n", rate);
            return B115200;
    }
}

static int open_serial(const char *device, int baudrate) {
    int fd = open(device, O_RDWR | O_NOCTTY | O_NDELAY);
    if (fd < 0) {
        fprintf(stderr, "Error opening %s: %s\n", device, strerror(errno));
        return -1;
    }

    struct termios options;
    if (tcgetattr(fd, &options) < 0) {
        fprintf(stderr, "Error getting terminal attributes: %s\n", strerror(errno));
        close(fd);
        return -1;
    }

    speed_t speed = get_baudrate(baudrate);
    cfsetispeed(&options, speed);
    cfsetospeed(&options, speed);

    // 8N1, no flow control
    options.c_cflag &= ~PARENB;  // No parity
    options.c_cflag &= ~CSTOPB;  // 1 stop bit
    options.c_cflag &= ~CSIZE;
    options.c_cflag |= CS8;      // 8 data bits
    options.c_cflag &= ~CRTSCTS; // No hardware flow control
    options.c_cflag |= CREAD | CLOCAL;

    // Raw input
    options.c_lflag &= ~(ICANON | ECHO | ECHOE | ISIG);
    options.c_iflag &= ~(IXON | IXOFF | IXANY | ICRNL);
    options.c_oflag &= ~OPOST;

    // Timeout settings
    options.c_cc[VMIN] = 0;
    options.c_cc[VTIME] = 10; // 1 second timeout

    if (tcsetattr(fd, TCSANOW, &options) < 0) {
        fprintf(stderr, "Error setting terminal attributes: %s\n", strerror(errno));
        close(fd);
        return -1;
    }

    tcflush(fd, TCIOFLUSH);
    return fd;
}

static bool read_line_timeout(int fd, char *buffer, int max_len, int timeout_sec) {
    fd_set readfds;
    struct timeval timeout;
    int pos = 0;

    timeout.tv_sec = timeout_sec;
    timeout.tv_usec = 0;

    while (pos < max_len - 1) {
        FD_ZERO(&readfds);
        FD_SET(fd, &readfds);

        int ret = select(fd + 1, &readfds, NULL, NULL, &timeout);
        if (ret <= 0) {
            // Timeout or error
            buffer[pos] = '\0';
            return pos > 0;
        }

        char c;
        ssize_t n = read(fd, &c, 1);
        if (n <= 0) {
            buffer[pos] = '\0';
            return pos > 0;
        }

        buffer[pos++] = c;

        // Check for end of line
        if (pos >= 2 && buffer[pos-2] == '\r' && buffer[pos-1] == '\n') {
            buffer[pos] = '\0';
            return true;
        }
    }

    buffer[pos] = '\0';
    return true;
}

static void send_command_and_read(int fd, const char *cmd, LineBuffer *buf, int timeout_sec, bool verbose) {
    char cmd_with_newline[256];
    snprintf(cmd_with_newline, sizeof(cmd_with_newline), "%s\n", cmd);

    if (verbose) {
        printf("Sending: %s", cmd_with_newline);
    }

    buf->count = 0;
    char end_marker[] = "\r$$\r\n";
    bool done = false;

    while (!done && buf->count < MAX_LINES) {
        // Send command
        ssize_t written = write(fd, cmd_with_newline, strlen(cmd_with_newline));
        if (written < 0) {
            fprintf(stderr, "Error writing to serial port: %s\n", strerror(errno));
            return;
        }
        tcdrain(fd); // Wait for transmission to complete

        // Read response
        char line[MAX_LINE_LEN];
        if (read_line_timeout(fd, line, sizeof(line), timeout_sec)) {
            if (verbose) {
                printf("Received: %s", line);
            }

            // Store line
            if (buf->count < MAX_LINES) {
                strncpy(buf->lines[buf->count], line, MAX_LINE_LEN - 1);
                buf->lines[buf->count][MAX_LINE_LEN - 1] = '\0';
                buf->count++;
            }

            // Check for end marker
            if (strcmp(line, end_marker) == 0) {
                done = true;
            }
        } else {
            // Timeout or error - try sending command again
            usleep(100000); // 100ms delay
        }
    }
}

static bool is_numeric(const char *str) {
    if (!str || !*str) return false;
    char *endptr;
    strtod(str, &endptr);
    return *endptr == '\0';
}

static void parse_and_process_lines(const LineBuffer *buf, const char *db_script, bool verbose) {
    for (int i = 0; i < buf->count; i++) {
        const char *line = buf->lines[i];

        // Skip empty lines
        if (strlen(line) == 0) continue;

        // Skip lines with markers
        if (strstr(line, "$") || strstr(line, "#") || strstr(line, "Power")) {
            continue;
        }

        // Tokenize line
        char *copy = strdup(line);
        char *fields[MAX_FIELDS];
        int field_count = 0;

        char *token = strtok(copy, " \t\r\n");
        while (token && field_count < MAX_FIELDS) {
            fields[field_count++] = token;
            token = strtok(NULL, " \t\r\n");
        }

        // Need at least 13 fields (battery number + 12 data fields)
        if (field_count < 13) {
            free(copy);
            continue;
        }

        // Validate numeric fields
        if (!is_numeric(fields[0]) || !is_numeric(fields[1]) || !is_numeric(fields[2])) {
            free(copy);
            continue;
        }

        // Skip "Absent" batteries
        if (field_count > 8 && strcmp(fields[8], "Absent") == 0) {
            free(copy);
            continue;
        }

        // Found valid battery data - call database script
        int bat_num = atoi(fields[0]);

        if (verbose) {
            printf("→ Battery %d: Volt=%smV, Curr=%smA, SOC=%s\n",
                   bat_num, fields[1], fields[2], field_count > 12 ? fields[12] : "N/A");
        }

        // Fork and execute database script
        pid_t pid = fork();
        if (pid == 0) {
            // Child process
            char *args[MAX_FIELDS + 2];
            args[0] = (char *)db_script;
            for (int j = 0; j < 13 && j < field_count; j++) {
                args[j + 1] = fields[j];
            }
            args[14] = NULL;

            execv(db_script, args);
            // If execv returns, there was an error
            fprintf(stderr, "Error executing %s: %s\n", db_script, strerror(errno));
            exit(1);
        } else if (pid > 0) {
            // Parent process - wait for child
            int status;
            waitpid(pid, &status, 0);
            if (WIFEXITED(status) && WEXITSTATUS(status) != 0 && verbose) {
                fprintf(stderr, "Database script exited with status %d\n", WEXITSTATUS(status));
            }
        } else {
            fprintf(stderr, "Fork failed: %s\n", strerror(errno));
        }

        free(copy);
    }
}

int main(int argc, char *argv[]) {
    Config config;
    init_config(&config);

    if (argc < 2) {
        fprintf(stderr, "Error: command required\n\n");
        print_usage(argv[0]);
        return 1;
    }

    static struct option long_options[] = {
        {"parse",      no_argument,       0, 'p'},
        {"device",     required_argument, 0, 'd'},
        {"baudrate",   required_argument, 0, 'b'},
        {"timeout",    required_argument, 0, 't'},
        {"db-script",  required_argument, 0, 's'},
        {"verbose",    no_argument,       0, 'v'},
        {"help",       no_argument,       0, 'h'},
        {"version",    no_argument,       0, 'V'},
        {0, 0, 0, 0}
    };

    // First, collect positional arguments for command
    int i;
    for (i = 1; i < argc && argv[i][0] != '-'; i++) {
        if (i > 1) {
            strncat(config.command, " ", sizeof(config.command) - strlen(config.command) - 1);
        }
        strncat(config.command, argv[i], sizeof(config.command) - strlen(config.command) - 1);
    }

    // Now parse options starting from first option
    optind = i;
    int opt;
    while ((opt = getopt_long(argc, argv, "pd:b:t:s:vhV", long_options, NULL)) != -1) {
        switch (opt) {
            case 'p':
                config.parse_mode = true;
                break;
            case 'd':
                strncpy(config.device, optarg, sizeof(config.device) - 1);
                break;
            case 'b':
                config.baudrate = atoi(optarg);
                break;
            case 't':
                config.timeout_sec = atoi(optarg);
                break;
            case 's':
                strncpy(config.db_script, optarg, sizeof(config.db_script) - 1);
                break;
            case 'v':
                config.verbose = true;
                break;
            case 'V':
                printf("ebox v%s\n", VERSION);
                return 0;
            case 'h':
            default:
                print_usage(argv[0]);
                return opt == 'h' ? 0 : 1;
        }
    }

    // Validate command was provided
    if (config.command[0] == '\0') {
        fprintf(stderr, "Error: command required\n\n");
        print_usage(argv[0]);
        return 1;
    }

    if (config.verbose) {
        printf("Configuration:\n");
        printf("  Device: %s\n", config.device);
        printf("  Baudrate: %d\n", config.baudrate);
        printf("  Timeout: %d sec\n", config.timeout_sec);
        printf("  DB Script: %s\n", config.db_script);
        printf("  Command: %s\n", config.command);
        printf("\n");
    }

    // Open serial port
    int fd = open_serial(config.device, config.baudrate);
    if (fd < 0) {
        return 1;
    }

    // Send command and read response
    LineBuffer buffer;
    send_command_and_read(fd, config.command, &buffer, config.timeout_sec, config.verbose);

    // Close serial port
    close(fd);

    if (config.verbose) {
        printf("Received %d lines\n", buffer.count);
    }

    // Parse and process data only in parse mode
    if (config.parse_mode) {
        parse_and_process_lines(&buffer, config.db_script, config.verbose);
    } else {
        // Simple mode: just display output
        if (!config.verbose) {
            // If not verbose, print all lines (verbose already printed them during receive)
            for (int i = 0; i < buffer.count; i++) {
                printf("%s", buffer.lines[i]);
            }
        }
    }

    return 0;
}
