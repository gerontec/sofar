/* ════════════════════════════════════════════════════════════════════════════
 *  usbmux — zwei logische Kanäle auf der einen USB-CDC-Leitung des ESP32-S3
 *
 *  Der ESP32-S3 hat genau ein USB-Serial-Interface: es gibt nur ein
 *  /dev/ttyACM0, und wer es öffnet, hat es exklusiv — mit laufendem
 *  `esphome logs` ist keine Steuerung möglich und umgekehrt. Dieser Daemon
 *  hält den Port zentral und trennt ihn zeilenweise auf. Gegenstück auf dem
 *  ESP ist waveshare/usb_mux.h.
 *
 *      Kanal 1  Steuerung  Zeilen mit Präfix "#K1# ", Request/Response
 *      Kanal 2  Log        alles ohne dieses Präfix, unverändert
 *
 *  Jeder Kanal wird doppelt angeboten:
 *      - Unix-Socket   für dieses Programm (cmd/mon) und eigene Skripte
 *      - PTY + Symlink für Fremdwerkzeuge, die einen Gerätepfad wollen
 *        (esphome logs, esp_idf_monitor, screen, PlatformIO). Die merken
 *        nichts vom Mux, für sie ist es eine gewöhnliche serielle Leitung.
 *
 *  Ein PTY hat keine Modemleitungen — DTR/RTS laufen ins Leere, esptool kann
 *  darüber nicht in den Bootloader resetten. Zum Flashen deshalb
 *  `usbmux flash <yaml>`: gibt den echten Port frei, ruft esphome, nimmt ihn
 *  danach zurück.
 *
 *  Bauen:   make -C waveshare usbmux      (oder: cc -O2 -Wall -o usbmux usbmux.c)
 *
 *  Bedienung:
 *      ./usbmux daemon &                  Port halten, Sockets + PTYs anlegen
 *      ./usbmux cmd status                Kanal 1
 *      ./usbmux mon                       Kanal 2 (beliebig oft gleichzeitig)
 *      ./usbmux state | pause | resume | stop
 *      ./usbmux flash rs485defect.yaml
 *      esphome logs rs485defect.yaml --device $XDG_RUNTIME_DIR/usbmux/tty-log-ttyACM0
 *
 *  Warum C und nicht Python: eine Binary ohne pyserial/venv, läuft genauso auf
 *  dem Pi, poll()-basiert ohne Threads — der Daemon soll das Unauffälligste im
 *  System sein.
 * ════════════════════════════════════════════════════════════════════════════ */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <termios.h>
#include <time.h>
#include <unistd.h>

#define PREFIX        "#K1# "
#define PREFIX_LEN    5
#define LINE_MAX_     1024
#define BACKLOG_LINES 200      /* Zeilen, die ein neuer Monitor nachgereicht bekommt */
#define MAX_LOGCLI    16
#define MAX_CTLCLI    16
#define CMD_TIMEOUT_MS 6000    /* "scan" blockiert auf dem ESP ~1,3 s */
#define DEFAULT_DEVICE "/dev/ttyACM0"

/* ── globaler Zustand ─────────────────────────────────────────────────────── */

static const char *g_device = DEFAULT_DEVICE;
static char g_ctl_path[256], g_log_path[256];
static char g_ttyctl_link[256], g_ttylog_link[256], g_ttyraw_link[256];
static volatile sig_atomic_t g_stop = 0;

struct req {                     /* eine wartende Kanal-1-Anfrage */
	int fd;                      /* Socket-Client, oder -1 wenn vom Steuer-PTY */
	int from_pty;
	char cmd[LINE_MAX_];
	struct req *next;
};

struct daemon {
	int serfd;                   /* echter Port, -1 = zu */
	int paused;                  /* 1 = Port freigegeben (flashen) */
	int ctl_lfd, log_lfd;        /* Listen-Sockets */
	int pty_ctl_m, pty_ctl_s;    /* PTY Kanal 1 (Master hier, Slave offen halten) */
	int pty_log_m, pty_log_s;    /* PTY Kanal 2 */
	int pty_raw_m, pty_raw_s;    /* PTY Kanal 0: byte-transparent (esptool) */
	int raw_mode;                /* 1 = Zeilenparser aus, alles geht roh durch */
	speed_t raw_speed;           /* zuletzt vom Raw-PTY uebernommene Baudrate */
	int logcli[MAX_LOGCLI];
	int nlogcli;
	struct req *q_head, *q_tail; /* FIFO wartender Anfragen */
	struct req *active;          /* laufende Anfrage, NULL = frei */
	long long deadline_ms;
	char serbuf[8192];  size_t serlen;
	char ptybuf[LINE_MAX_]; size_t ptylen;
	char *backlog[BACKLOG_LINES];
	int bl_head, bl_count;
	time_t opened_at;
};

static struct daemon D;

/* ── Kleinkram ────────────────────────────────────────────────────────────── */

static void die(const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	vfprintf(stderr, fmt, ap);
	va_end(ap);
	fputc('\n', stderr);
	exit(1);
}

static long long now_ms(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void set_nonblock(int fd)
{
	int fl = fcntl(fd, F_GETFL, 0);
	if (fl >= 0)
		fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

/* Schreibt komplett, ignoriert EAGAIN-Reste (langsame Leser verlieren Zeilen,
 * bremsen aber nie den Daemon). */
static void write_all(int fd, const char *buf, size_t len)
{
	while (len > 0) {
		ssize_t n = write(fd, buf, len);
		if (n > 0) { buf += n; len -= (size_t)n; continue; }
		if (n < 0 && errno == EINTR)
			continue;
		return;
	}
}

static void write_line(int fd, const char *line)
{
	write_all(fd, line, strlen(line));
	write_all(fd, "\n", 1);
}

/* Kopiert ohne ANSI-Sequenzen. Der Logger färbt, unsere Antwortzeilen nicht —
 * ohne diesen Schritt würde ein farbiger Rest den Präfix-Test verfehlen. */
static void strip_ansi(const char *in, char *out, size_t outsz)
{
	size_t o = 0;
	for (size_t i = 0; in[i] && o + 1 < outsz; i++) {
		if (in[i] == 0x1b && in[i + 1] == '[') {
			i += 2;
			while (in[i] && !((in[i] >= 'A' && in[i] <= 'Z') || (in[i] >= 'a' && in[i] <= 'z')))
				i++;
			continue;
		}
		out[o++] = in[i];
	}
	out[o] = '\0';
}

static const char *runtime_dir(void)
{
	static char dir[200];
	const char *base = getenv("XDG_RUNTIME_DIR");
	char fallback[128];
	if (!base || !*base) {
		snprintf(fallback, sizeof(fallback), "/tmp/usbmux-%d", (int)getuid());
		base = fallback;
	}
	snprintf(dir, sizeof(dir), "%s/usbmux", base);
	mkdir(base, 0700);
	mkdir(dir, 0700);
	return dir;
}

static void build_paths(void)
{
	const char *dir = runtime_dir();
	const char *tag = strrchr(g_device, '/');
	tag = tag ? tag + 1 : g_device;
	snprintf(g_ctl_path,    sizeof(g_ctl_path),    "%s/ctl-%s.sock", dir, tag);
	snprintf(g_log_path,    sizeof(g_log_path),    "%s/log-%s.sock", dir, tag);
	snprintf(g_ttyctl_link, sizeof(g_ttyctl_link), "%s/tty-ctl-%s",  dir, tag);
	snprintf(g_ttylog_link, sizeof(g_ttylog_link), "%s/tty-log-%s",  dir, tag);
	snprintf(g_ttyraw_link, sizeof(g_ttyraw_link), "%s/tty-raw-%s",  dir, tag);
}

/* ── serieller Port ───────────────────────────────────────────────────────── */

/* DTR/RTS werden bewusst NICHT angefasst.
 * Auf dem USB-Serial-JTAG lösen Wechsel dieser Leitungen Reset bzw.
 * Bootloader aus (genau das nutzt esptool). Die Python-Vorfassung setzte beide
 * beim Öffnen aktiv auf 0 — das Board bootete daraufhin jedes Mal neu
 * (rst:0x15 USB_UART_CHIP_RESET im Log). Hier bleibt es bei dem, was der
 * cdc_acm-Treiber ohnehin gesetzt hat, und HUPCL wird gelöscht, damit auch das
 * Schließen (pause/flash) die Leitungen nicht fallen lässt. */
static int open_serial(const char *dev)
{
	int fd = open(dev, O_RDWR | O_NOCTTY | O_NONBLOCK);
	if (fd < 0)
		return -1;
	struct termios t;
	if (tcgetattr(fd, &t) == 0) {
		cfmakeraw(&t);
		t.c_cflag |= CLOCAL | CREAD;
		t.c_cflag &= ~HUPCL;     /* kein DTR-Drop beim Schließen -> kein Reboot */
		t.c_cc[VMIN] = 0;
		t.c_cc[VTIME] = 0;
		cfsetispeed(&t, B115200); /* bei USB-CDC belanglos, der Form halber */
		cfsetospeed(&t, B115200);
		tcsetattr(fd, TCSANOW, &t);
	}
	return fd;
}

/* ── Modemleitungen: das, was ein PTY nicht kann ──────────────────────────── */

/* Ein PTY-Slave beantwortet TIOCMSET/TIOCMGET mit ENOTTY (gemessen) — DTR/RTS
 * sind darüber grundsätzlich nicht zu haben. esptool bräuchte sie aber, um den
 * ESP32-S3 in den Download-Modus zu resetten. Lösung: der Daemon hält den
 * echten Port und fährt die Sequenz selbst; das Werkzeug am Raw-PTY bekommt
 * eine gewöhnliche Datenleitung und einen Chip, der schon im Bootloader steht. */
static void mctrl(int fd, int bits, int on)
{
	if (fd < 0)
		return;
	ioctl(fd, on ? TIOCMBIS : TIOCMBIC, &bits);
}

static void msleep(int ms)
{
	struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000L };
	nanosleep(&ts, NULL);
}

/* Sequenz aus esptool (reset.py, USBJTAGSerialReset): IO0 low halten, während
 * EN steigt -> Chip startet im Download-Modus. */
static void seq_bootloader(int fd)
{
	mctrl(fd, TIOCM_RTS, 0);
	mctrl(fd, TIOCM_DTR, 0);
	msleep(100);
	mctrl(fd, TIOCM_DTR, 1);
	mctrl(fd, TIOCM_RTS, 0);
	msleep(100);
	mctrl(fd, TIOCM_RTS, 1);
	mctrl(fd, TIOCM_DTR, 0);
	mctrl(fd, TIOCM_RTS, 1);
	msleep(100);
	mctrl(fd, TIOCM_DTR, 0);
	mctrl(fd, TIOCM_RTS, 0);
}

/* Sequenz aus esptool (HardReset): EN kurz low -> normaler Neustart. */
static void seq_hard_reset(int fd)
{
	mctrl(fd, TIOCM_RTS, 1);
	msleep(100);
	mctrl(fd, TIOCM_RTS, 0);
}

/* ── PTYs ─────────────────────────────────────────────────────────────────── */

/* Legt ein PTY an und verlinkt das Slave-Ende unter festem Namen.
 * Das Slave-Ende bleibt im Daemon offen: sonst liefert der Master EIO, sobald
 * der letzte Leser weg ist, und der Kanal wäre nach dem ersten `screen` tot. */
static int pty_make(const char *link, int *master, int *slave)
{
	int m = posix_openpt(O_RDWR | O_NOCTTY);
	if (m < 0 || grantpt(m) < 0 || unlockpt(m) < 0)
		return -1;
	const char *name = ptsname(m);
	if (!name)
		return -1;
	int s = open(name, O_RDWR | O_NOCTTY);
	if (s < 0)
		return -1;
	struct termios t;
	if (tcgetattr(s, &t) == 0) {
		cfmakeraw(&t);           /* kein Echo, keine Zeilenpufferung */
		tcsetattr(s, TCSANOW, &t);
	}
	set_nonblock(m);
	unlink(link);
	if (symlink(name, link) < 0)
		fprintf(stderr, "[usbmux] Symlink %s: %s\n", link, strerror(errno));
	*master = m;
	*slave = s;
	return 0;
}

/* Baudrate vom Raw-PTY auf den echten Port spiegeln.
 * tcgetattr(master) liefert die Einstellungen, die der Slave gesetzt hat
 * (gemessen: Slave B460800 -> Master meldet B460800). Damit übernimmt der
 * Service jede Baudrate, die ein Werkzeug wählt — an USB-CDC ist sie zwar
 * folgenlos, aber das Werkzeug soll nichts merken. */
static void mirror_baud(void)
{
	if (D.pty_raw_m < 0 || D.serfd < 0)
		return;
	struct termios pt;
	if (tcgetattr(D.pty_raw_m, &pt) != 0)
		return;
	speed_t sp = cfgetospeed(&pt);
	if (sp == D.raw_speed || sp == 0)
		return;
	D.raw_speed = sp;
	struct termios st;
	if (tcgetattr(D.serfd, &st) != 0)
		return;
	cfsetispeed(&st, sp);
	cfsetospeed(&st, sp);
	tcsetattr(D.serfd, TCSANOW, &st);
}

/* ── Unix-Sockets ─────────────────────────────────────────────────────────── */

static int listen_unix(const char *path)
{
	int fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd < 0)
		die("socket: %s", strerror(errno));
	struct sockaddr_un a = {0};
	a.sun_family = AF_UNIX;
	snprintf(a.sun_path, sizeof(a.sun_path), "%s", path);
	unlink(path);
	if (bind(fd, (struct sockaddr *)&a, sizeof(a)) < 0)
		die("bind %s: %s", path, strerror(errno));
	chmod(path, 0600);
	if (listen(fd, 8) < 0)
		die("listen: %s", strerror(errno));
	return fd;
}

static int connect_unix(const char *path)
{
	int fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd < 0)
		return -1;
	struct sockaddr_un a = {0};
	a.sun_family = AF_UNIX;
	snprintf(a.sun_path, sizeof(a.sun_path), "%s", path);
	if (connect(fd, (struct sockaddr *)&a, sizeof(a)) < 0) {
		close(fd);
		return -1;
	}
	return fd;
}

/* ── Kanal 2: Log verteilen ───────────────────────────────────────────────── */

static void backlog_push(const char *line)
{
	int slot = (D.bl_head + D.bl_count) % BACKLOG_LINES;
	if (D.bl_count == BACKLOG_LINES) {
		free(D.backlog[D.bl_head]);
		D.bl_head = (D.bl_head + 1) % BACKLOG_LINES;
		slot = (D.bl_head + D.bl_count - 1) % BACKLOG_LINES;
	} else {
		D.bl_count++;
	}
	D.backlog[slot] = strdup(line);
}

static void publish_log(const char *line)
{
	backlog_push(line);
	for (int i = 0; i < D.nlogcli; i++)
		write_line(D.logcli[i], line);
	if (D.pty_log_m >= 0) {
		/* PTY-Leser erwarten CRLF wie von einer echten Leitung */
		write_all(D.pty_log_m, line, strlen(line));
		write_all(D.pty_log_m, "\r\n", 2);
	}
}

static void note(const char *fmt, ...)
{
	char buf[512];
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);
	fprintf(stderr, "%s\n", buf);
	publish_log(buf);
}

static void logcli_drop(int idx)
{
	close(D.logcli[idx]);
	D.logcli[idx] = D.logcli[--D.nlogcli];
}

/* ── Kanal 1: Anfragen ────────────────────────────────────────────────────── */

static void reply_to(struct req *r, const char *line)
{
	if (r->from_pty) {
		if (D.pty_ctl_m >= 0) {
			write_all(D.pty_ctl_m, line, strlen(line));
			write_all(D.pty_ctl_m, "\r\n", 2);
		}
	} else if (r->fd >= 0) {
		write_line(r->fd, line);
	}
}

static void req_finish(void)
{
	struct req *r = D.active;
	if (!r)
		return;
	if (!r->from_pty && r->fd >= 0)
		close(r->fd);            /* ein Kommando pro Socket-Verbindung */
	free(r);
	D.active = NULL;
}

static void req_start_next(void);

static const char *state_word(void)
{
	if (D.paused)
		return "pausiert";
	return D.serfd >= 0 ? "offen" : "wartet auf Geraet";
}

/* Kommandos an den Daemon selbst (Präfix '!'), erreichen den ESP nie. */
static void local_command(struct req *r, const char *what)
{
	char buf[768];
	if (!strcmp(what, "stop")) {
		reply_to(r, "+OK usbmux beendet");
		g_stop = 1;
	} else if (!strcmp(what, "pause")) {
		D.paused = 1;
		if (D.serfd >= 0) { close(D.serfd); D.serfd = -1; }
		reply_to(r, "+OK Port freigegeben (flashen moeglich)");
	} else if (!strcmp(what, "resume")) {
		D.paused = 0;
		reply_to(r, "+OK Port wird wieder geoeffnet");
	} else if (!strcmp(what, "state")) {
		snprintf(buf, sizeof(buf), "device=%s state=%s raw=%s monitore=%d port_offen_s=%ld",
		         g_device, state_word(), D.raw_mode ? "an" : "aus", D.nlogcli,
		         D.opened_at ? (long)(time(NULL) - D.opened_at) : 0L);
		reply_to(r, buf);
		snprintf(buf, sizeof(buf), "kanal1 sock=%s pty=%s", g_ctl_path, g_ttyctl_link);
		reply_to(r, buf);
		snprintf(buf, sizeof(buf), "kanal2 sock=%s pty=%s", g_log_path, g_ttylog_link);
		reply_to(r, buf);
		snprintf(buf, sizeof(buf), "kanal0 raw pty=%s", g_ttyraw_link);
		reply_to(r, buf);
		reply_to(r, "+OK");
	} else if (!strcmp(what, "boot")) {
		/* Chip in den Download-Modus. Danach re-enumeriert USB, der Port
		 * verschwindet kurz — die Reopen-Schleife fängt das auf. */
		if (D.serfd < 0) {
			reply_to(r, "-ERR kein Port offen");
		} else {
			seq_bootloader(D.serfd);
			reply_to(r, "+OK Download-Modus (esptool jetzt mit --before no_reset)");
		}
	} else if (!strcmp(what, "reset")) {
		if (D.serfd < 0) {
			reply_to(r, "-ERR kein Port offen");
		} else {
			seq_hard_reset(D.serfd);
			reply_to(r, "+OK Neustart ausgeloest");
		}
	} else if (!strcmp(what, "raw on") || !strcmp(what, "raw off")) {
		D.raw_mode = (what[4] == 'o' && what[5] == 'n');
		if (D.raw_mode)
			D.serlen = 0;    /* halbe Zeile verwerfen, gleich kommen Binärdaten */
		snprintf(buf, sizeof(buf), "+OK raw %s", D.raw_mode ? "an" : "aus");
		reply_to(r, buf);
	} else {
		snprintf(buf, sizeof(buf), "-ERR unbekanntes Daemon-Kommando '%s'", what);
		reply_to(r, buf);
	}
	D.active = r;
	req_finish();
	req_start_next();
}

static void req_enqueue(int fd, int from_pty, const char *cmd)
{
	struct req *r = calloc(1, sizeof(*r));
	if (!r) {
		if (fd >= 0) close(fd);
		return;
	}
	r->fd = fd;
	r->from_pty = from_pty;
	snprintf(r->cmd, sizeof(r->cmd), "%s", cmd);
	if (D.q_tail)
		D.q_tail->next = r;
	else
		D.q_head = r;
	D.q_tail = r;
	if (!D.active)
		req_start_next();
}

static void req_start_next(void)
{
	if (D.active || !D.q_head)
		return;
	struct req *r = D.q_head;
	D.q_head = r->next;
	if (!D.q_head)
		D.q_tail = NULL;
	r->next = NULL;

	if (r->cmd[0] == '!') {
		local_command(r, r->cmd + 1);
		return;
	}
	if (D.paused) {
		D.active = r;
		reply_to(r, "-ERR usbmux pausiert (usbmux resume)");
		req_finish();
		req_start_next();
		return;
	}
	if (D.serfd < 0) {
		D.active = r;
		reply_to(r, "-ERR kein Port offen (Geraet abgezogen?)");
		req_finish();
		req_start_next();
		return;
	}
	D.active = r;
	D.deadline_ms = now_ms() + CMD_TIMEOUT_MS;
	write_all(D.serfd, r->cmd, strlen(r->cmd));
	write_all(D.serfd, "\n", 1);
}

/* ── Zeilen vom ESP verteilen ─────────────────────────────────────────────── */

static void handle_device_line(const char *raw)
{
	char clean[LINE_MAX_];
	strip_ansi(raw, clean, sizeof(clean));

	if (!clean[0])
		return;                  /* Leerzeile (u.a. das führende \n der Antworten) */

	/* Steht das Präfix mitten in der Zeile, hat der TX-Ring des ESP die
	 * vorangehende Log-Zeile samt Zeilenende verworfen (512-B-Burst-Grenze,
	 * siehe usb_mux.h). Dann wird hier getrennt statt beides zu verlieren:
	 * vorderer Teil ist Log, ab dem Präfix beginnt Kanal 1. */
	const char *mid = strstr(clean, PREFIX);
	if (mid && mid != clean) {
		char head[LINE_MAX_];
		size_t hl = (size_t)(mid - clean);
		if (hl >= sizeof(head))
			hl = sizeof(head) - 1;
		memcpy(head, clean, hl);
		head[hl] = '\0';
		publish_log(head);
		handle_device_line(mid);
		return;
	}

	if (strncmp(clean, PREFIX, PREFIX_LEN) == 0) {
		const char *body = clean + PREFIX_LEN;
		if (!D.active)
			return;              /* verspätete Antwort einer abgelaufenen Anfrage */
		reply_to(D.active, body);
		if (!strcmp(body, "+OK") || !strncmp(body, "-ERR", 4)) {
			req_finish();
			req_start_next();
		}
		return;
	}
	publish_log(raw);
}

static void feed_serial(const char *data, size_t len)
{
	/* Raw-PTY bekommt immer alles ungefiltert — es ist die 1:1-Leitung. */
	if (D.pty_raw_m >= 0)
		write_all(D.pty_raw_m, data, len);
	if (D.raw_mode)
		return;              /* Zeilenparser aus: es fliessen Binaerdaten */

	for (size_t i = 0; i < len; i++) {
		char c = data[i];
		if (c == '\n') {
			D.serbuf[D.serlen] = '\0';
			if (D.serlen && D.serbuf[D.serlen - 1] == '\r')
				D.serbuf[D.serlen - 1] = '\0';
			handle_device_line(D.serbuf);
			D.serlen = 0;
			continue;
		}
		if (D.serlen + 1 < sizeof(D.serbuf))
			D.serbuf[D.serlen++] = c;
		else
			D.serlen = 0;        /* Zeile ohne Ende: verwerfen */
	}
}

/* Zeilen, die eine App in das Steuer-PTY tippt/schreibt */
static void feed_pty_ctl(const char *data, size_t len)
{
	for (size_t i = 0; i < len; i++) {
		char c = data[i];
		if (c == '\n' || c == '\r') {
			if (D.ptylen == 0)
				continue;
			D.ptybuf[D.ptylen] = '\0';
			req_enqueue(-1, 1, D.ptybuf);
			D.ptylen = 0;
			continue;
		}
		if (D.ptylen + 1 < sizeof(D.ptybuf))
			D.ptybuf[D.ptylen++] = c;
	}
}

/* ── Daemon-Hauptschleife ─────────────────────────────────────────────────── */

static void on_signal(int sig) { (void)sig; g_stop = 1; }

static void cleanup(void)
{
	unlink(g_ctl_path);
	unlink(g_log_path);
	unlink(g_ttyctl_link);
	unlink(g_ttylog_link);
	unlink(g_ttyraw_link);
}

static int run_daemon(void)
{
	int probe = connect_unix(g_ctl_path);
	if (probe >= 0) {
		close(probe);
		die("usbmux läuft schon für %s (%s)", g_device, g_ctl_path);
	}

	memset(&D, 0, sizeof(D));
	D.serfd = -1;
	D.pty_ctl_m = D.pty_ctl_s = D.pty_log_m = D.pty_log_s = -1;
	D.pty_raw_m = D.pty_raw_s = -1;

	signal(SIGINT, on_signal);
	signal(SIGTERM, on_signal);
	signal(SIGPIPE, SIG_IGN);

	D.ctl_lfd = listen_unix(g_ctl_path);
	D.log_lfd = listen_unix(g_log_path);
	if (pty_make(g_ttyctl_link, &D.pty_ctl_m, &D.pty_ctl_s) < 0)
		fprintf(stderr, "[usbmux] Steuer-PTY nicht möglich: %s\n", strerror(errno));
	if (pty_make(g_ttylog_link, &D.pty_log_m, &D.pty_log_s) < 0)
		fprintf(stderr, "[usbmux] Log-PTY nicht möglich: %s\n", strerror(errno));
	if (pty_make(g_ttyraw_link, &D.pty_raw_m, &D.pty_raw_s) < 0)
		fprintf(stderr, "[usbmux] Raw-PTY nicht möglich: %s\n", strerror(errno));

	fprintf(stderr, "[usbmux] Kanal 1  %s  |  %s\n", g_ctl_path, g_ttyctl_link);
	fprintf(stderr, "[usbmux] Kanal 2  %s  |  %s\n", g_log_path, g_ttylog_link);
	fprintf(stderr, "[usbmux] Kanal 0  (roh, 1:1)  |  %s\n", g_ttyraw_link);

	long long next_open = 0;

	while (!g_stop) {
		if (D.serfd < 0 && !D.paused && now_ms() >= next_open) {
			D.serfd = open_serial(g_device);
			if (D.serfd >= 0) {
				D.opened_at = time(NULL);
				note("[usbmux] %s offen", g_device);
			} else {
				note("[usbmux] %s nicht da (%s) — neuer Versuch in 2 s",
				     g_device, strerror(errno));
				next_open = now_ms() + 2000;
			}
		}

		mirror_baud();

		struct pollfd p[8 + MAX_LOGCLI];
		int n = 0, i_ser = -1, i_ctl = -1, i_log = -1, i_pty = -1, i_raw = -1, i_log0 = -1;

		if (D.serfd >= 0) { p[n].fd = D.serfd; p[n].events = POLLIN; i_ser = n++; }
		p[n].fd = D.ctl_lfd; p[n].events = POLLIN; i_ctl = n++;
		p[n].fd = D.log_lfd; p[n].events = POLLIN; i_log = n++;
		if (D.pty_ctl_m >= 0) { p[n].fd = D.pty_ctl_m; p[n].events = POLLIN; i_pty = n++; }
		if (D.pty_raw_m >= 0) { p[n].fd = D.pty_raw_m; p[n].events = POLLIN; i_raw = n++; }
		i_log0 = n;
		for (int i = 0; i < D.nlogcli; i++) { p[n].fd = D.logcli[i]; p[n].events = 0; n++; }

		int timeout = 1000;
		if (D.active) {
			long long left = D.deadline_ms - now_ms();
			timeout = left < 0 ? 0 : (left < 1000 ? (int)left : 1000);
		}
		if (D.serfd < 0)
			timeout = 200;

		int rc = poll(p, (nfds_t)n, timeout);
		if (rc < 0 && errno != EINTR)
			break;

		/* Zeitüberschreitung einer laufenden Anfrage */
		if (D.active && now_ms() >= D.deadline_ms) {
			reply_to(D.active, "-ERR Zeitueberschreitung (keine Antwort vom ESP)");
			req_finish();
			req_start_next();
		}

		if (D.paused && D.serfd >= 0) {
			close(D.serfd);
			D.serfd = -1;
			note("[usbmux] Port freigegeben");
		}

		if (rc <= 0)
			continue;

		if (i_ser >= 0 && (p[i_ser].revents & (POLLIN | POLLERR | POLLHUP))) {
			char buf[2048];
			ssize_t k = read(D.serfd, buf, sizeof(buf));
			if (k > 0) {
				feed_serial(buf, (size_t)k);
			} else if (k == 0 || (k < 0 && errno != EAGAIN && errno != EINTR)) {
				note("[usbmux] Port weg (%s) — wird neu geöffnet",
				     k == 0 ? "EOF" : strerror(errno));
				close(D.serfd);
				D.serfd = -1;
				next_open = now_ms() + 1000;
			}
		}

		if (i_pty >= 0 && (p[i_pty].revents & POLLIN)) {
			char buf[512];
			ssize_t k = read(D.pty_ctl_m, buf, sizeof(buf));
			if (k > 0)
				feed_pty_ctl(buf, (size_t)k);
		}

		/* Raw-PTY: alles, was ein Werkzeug dort schreibt, geht unverändert auf
		 * den echten Port. Kein Zeilenbegriff, keine Übersetzung — hier laufen
		 * die SLIP-Frames von esptool durch. */
		if (i_raw >= 0 && (p[i_raw].revents & POLLIN)) {
			char buf[4096];
			ssize_t k = read(D.pty_raw_m, buf, sizeof(buf));
			if (k > 0 && D.serfd >= 0)
				write_all(D.serfd, buf, (size_t)k);
		}

		if (p[i_ctl].revents & POLLIN) {
			int c = accept(D.ctl_lfd, NULL, NULL);
			if (c >= 0) {
				/* Eine Zeile lesen — Clients schicken sie sofort, das ist kurz
				 * genug, um hier blockierend zu lesen. */
				char buf[LINE_MAX_];
				ssize_t k = read(c, buf, sizeof(buf) - 1);
				if (k <= 0) {
					close(c);
				} else {
					buf[k] = '\0';
					char *nl = strchr(buf, '\n');
					if (nl) *nl = '\0';
					req_enqueue(c, 0, buf);
				}
			}
		}

		if (p[i_log].revents & POLLIN) {
			int c = accept(D.log_lfd, NULL, NULL);
			if (c >= 0) {
				if (D.nlogcli >= MAX_LOGCLI) {
					write_line(c, "[usbmux] zu viele Monitore");
					close(c);
				} else {
					set_nonblock(c);
					for (int i = 0; i < D.bl_count; i++) {
						int slot = (D.bl_head + i) % BACKLOG_LINES;
						if (D.backlog[slot])
							write_line(c, D.backlog[slot]);
					}
					D.logcli[D.nlogcli++] = c;
				}
			}
		}

		/* Weggelaufene Monitore einsammeln */
		for (int i = 0; i < D.nlogcli; i++) {
			int idx = i_log0 + i;
			if (idx < n && (p[idx].revents & (POLLHUP | POLLERR))) {
				logcli_drop(i);
				break;   /* Array wurde umsortiert, Rest im nächsten Durchlauf */
			}
		}
	}

	if (D.serfd >= 0)
		close(D.serfd);
	cleanup();
	fprintf(stderr, "[usbmux] beendet\n");
	return 0;
}

/* ── Clients ──────────────────────────────────────────────────────────────── */

static int daemon_running(void)
{
	int fd = connect_unix(g_ctl_path);
	if (fd < 0)
		return 0;
	close(fd);
	return 1;
}

/* Kommando über den Daemon; gibt 1 zurück, wenn die Antwort ein -ERR enthielt. */
static int ask_daemon(const char *cmd)
{
	int fd = connect_unix(g_ctl_path);
	if (fd < 0)
		die("kein Daemon für %s", g_device);
	write_all(fd, cmd, strlen(cmd));
	write_all(fd, "\n", 1);
	char buf[1024];
	int err = 0;
	ssize_t k;
	while ((k = read(fd, buf, sizeof(buf) - 1)) > 0) {
		buf[k] = '\0';
		if (strstr(buf, "-ERR"))
			err = 1;
		write_all(1, buf, (size_t)k);
	}
	close(fd);
	return err;
}

/* Ohne Daemon: Port selbst öffnen, Kommando schicken, Kanal 1 herausfiltern. */
static int cmd_direct(const char *cmd)
{
	int fd = open_serial(g_device);
	if (fd < 0)
		die("%s: %s", g_device, strerror(errno));
	tcflush(fd, TCIFLUSH);
	write_all(fd, cmd, strlen(cmd));
	write_all(fd, "\n", 1);

	char line[LINE_MAX_], clean[LINE_MAX_];
	size_t len = 0;
	long long deadline = now_ms() + CMD_TIMEOUT_MS;
	int err = 0;
	while (now_ms() < deadline) {
		struct pollfd p = { .fd = fd, .events = POLLIN };
		if (poll(&p, 1, 200) <= 0)
			continue;
		char buf[512];
		ssize_t k = read(fd, buf, sizeof(buf));
		if (k <= 0)
			continue;
		for (ssize_t i = 0; i < k; i++) {
			if (buf[i] != '\n') {
				if (len + 1 < sizeof(line))
					line[len++] = buf[i];
				continue;
			}
			line[len] = '\0';
			if (len && line[len - 1] == '\r')
				line[len - 1] = '\0';
			len = 0;
			strip_ansi(line, clean, sizeof(clean));
			if (strncmp(clean, PREFIX, PREFIX_LEN) != 0)
				continue;               /* Kanal 2 hier verwerfen */
			const char *body = clean + PREFIX_LEN;
			printf("%s\n", body);
			if (!strcmp(body, "+OK")) { close(fd); return 0; }
			if (!strncmp(body, "-ERR", 4)) { close(fd); return 1; }
		}
	}
	printf("-ERR Zeitueberschreitung (keine Antwort vom ESP)\n");
	close(fd);
	return err ? 1 : 1;
}

static int mon_client(void)
{
	int fd = connect_unix(g_log_path);
	if (fd < 0) {
		fprintf(stderr, "[usbmux] kein Daemon — Direktmodus auf %s (exklusiv)\n", g_device);
		fd = open_serial(g_device);
		if (fd < 0)
			die("%s: %s", g_device, strerror(errno));
		/* Im Direktmodus zeilenweise filtern, damit Kanal 1 nicht mitläuft */
		char line[LINE_MAX_], clean[LINE_MAX_];
		size_t len = 0;
		for (;;) {
			char buf[512];
			ssize_t k = read(fd, buf, sizeof(buf));
			if (k < 0 && (errno == EAGAIN || errno == EINTR)) { usleep(20000); continue; }
			if (k <= 0)
				break;
			for (ssize_t i = 0; i < k; i++) {
				if (buf[i] != '\n') {
					if (len + 1 < sizeof(line))
						line[len++] = buf[i];
					continue;
				}
				line[len] = '\0';
				if (len && line[len - 1] == '\r')
					line[len - 1] = '\0';
				len = 0;
				strip_ansi(line, clean, sizeof(clean));
				if (strncmp(clean, PREFIX, PREFIX_LEN) == 0)
					continue;
				printf("%s\n", line);
				fflush(stdout);
			}
		}
		return 0;
	}
	char buf[4096];
	ssize_t k;
	while ((k = read(fd, buf, sizeof(buf))) > 0)
		write_all(1, buf, (size_t)k);
	close(fd);
	return 0;
}

static int do_flash(const char *yaml)
{
	int running = daemon_running();
	if (running) {
		ask_daemon("!pause");
		usleep(400000);
	}
	int rc = 1;
	pid_t pid = fork();
	if (pid == 0) {
		execlp("esphome", "esphome", "run", yaml, "--device", g_device, (char *)NULL);
		_exit(127);
	} else if (pid > 0) {
		int st = 0;
		waitpid(pid, &st, 0);
		rc = WIFEXITED(st) ? WEXITSTATUS(st) : 1;
	}
	if (running)
		ask_daemon("!resume");
	return rc;
}

/* esptool durch den Raw-Kanal fahren, ohne dass es den Mux bemerkt:
 * Daemon setzt den Chip per echter DTR/RTS-Sequenz in den Download-Modus,
 * schaltet den Zeilenparser ab und lässt esptool auf dem Raw-PTY arbeiten.
 * Die --before/--after-Flags setzt der Wrapper, nicht der Anwender. */
static int do_esptool(int argc, char **argv)
{
	if (!daemon_running())
		die("kein Daemon für %s — ohne Daemon einfach direkt esptool nutzen", g_device);

	ask_daemon("!raw on");
	ask_daemon("!boot");
	msleep(600);             /* USB re-enumeriert, Daemon öffnet neu */

	char **av = calloc((size_t)argc + 10, sizeof(char *));
	int i = 0;
	av[i++] = "esptool";
	av[i++] = "--port";
	av[i++] = g_ttyraw_link;
	av[i++] = "--before";
	av[i++] = "no_reset";
	av[i++] = "--after";
	av[i++] = "no_reset";
	for (int k = 0; k < argc; k++)
		av[i++] = argv[k];
	av[i] = NULL;

	int rc = 1;
	pid_t pid = fork();
	if (pid == 0) {
		execvp("esptool", av);
		_exit(127);
	} else if (pid > 0) {
		int st = 0;
		waitpid(pid, &st, 0);
		rc = WIFEXITED(st) ? WEXITSTATUS(st) : 1;
	}
	free(av);

	ask_daemon("!reset");    /* Anwendung wieder starten */
	ask_daemon("!raw off");
	return rc;
}

static void usage(void)
{
	fprintf(stderr,
	    "usbmux — mehrere Kanäle auf einer USB-CDC-Leitung\n\n"
	    "  usbmux [--device DEV] daemon        Port halten, Sockets + PTYs anlegen\n"
	    "  usbmux [--device DEV] cmd WORT...   Kanal 1: Kommando an den ESP\n"
	    "  usbmux [--device DEV] mon           Kanal 2: Log-Monitor\n"
	    "  usbmux [--device DEV] state|pause|resume|stop\n"
	    "  usbmux [--device DEV] boot|reset    Download-Modus / Neustart (echte DTR/RTS)\n"
	    "  usbmux [--device DEV] esptool ARG...  esptool über den Raw-Kanal\n"
	    "  usbmux [--device DEV] flash YAML    Port freigeben, esphome run, zurück\n\n"
	    "  Default-Gerät: %s (oder $USBMUX_DEVICE)\n"
	    "  Kanal-1-Kommandos kennt der ESP: help ping status relay openwifi scan\n"
	    "  wifi loglevel reboot — 'usbmux cmd help' fragt ihn direkt.\n",
	    DEFAULT_DEVICE);
}

int main(int argc, char **argv)
{
	const char *env = getenv("USBMUX_DEVICE");
	if (env && *env)
		g_device = env;

	int a = 1;
	while (a < argc && !strncmp(argv[a], "--", 2)) {
		if (!strcmp(argv[a], "--device") && a + 1 < argc) {
			g_device = argv[a + 1];
			a += 2;
		} else if (!strcmp(argv[a], "--help")) {
			usage();
			return 0;
		} else {
			usage();
			return 2;
		}
	}
	if (a >= argc) {
		usage();
		return 2;
	}
	build_paths();
	const char *what = argv[a++];

	if (!strcmp(what, "daemon"))
		return run_daemon();

	if (!strcmp(what, "cmd")) {
		if (a >= argc) { usage(); return 2; }
		char cmd[LINE_MAX_] = {0};
		for (int i = a; i < argc; i++) {
			if (i > a)
				strncat(cmd, " ", sizeof(cmd) - strlen(cmd) - 1);
			strncat(cmd, argv[i], sizeof(cmd) - strlen(cmd) - 1);
		}
		if (daemon_running())
			return ask_daemon(cmd);
		fprintf(stderr, "[usbmux] kein Daemon — Direktmodus auf %s\n", g_device);
		return cmd_direct(cmd);
	}

	if (!strcmp(what, "mon"))
		return mon_client();

	if (!strcmp(what, "flash")) {
		if (a >= argc) { usage(); return 2; }
		return do_flash(argv[a]);
	}

	if (!strcmp(what, "esptool"))
		return do_esptool(argc - a, argv + a);

	if (!strcmp(what, "state") || !strcmp(what, "pause") || !strcmp(what, "resume") ||
	    !strcmp(what, "stop") || !strcmp(what, "boot") || !strcmp(what, "reset")) {
		char c[64];
		snprintf(c, sizeof(c), "!%s", what);
		return ask_daemon(c);
	}

	usage();
	return 2;
}
