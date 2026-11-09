#pragma once
// flash_dump: read the flash over the network (ESPHome OTA can only write).
//
// Plain functions started from an on_boot lambda; a Component added through `includes:` is never registered.
// Runs as its own low-priority task, so the main loop and DO4 keep running during a dump.
//
// Protocol on TCP port FD_PORT, one client at a time, 10 s socket timeout:
//   server: "FDUMP1 <32 hex nonce>\n"
//   client: "AUTH <sha256 hex of nonce_hex + ota_password>\n"
//   server: "OK\n", or closes on a wrong digest
//   client: "INFO\n"             -> one JSON line (flash size, running app, partition list)
//   client: "READ <off> <len>\n" -> "OK <len>\n", <len> raw bytes, "SHA <sha256 hex of those bytes>\n"
//   client: "QUIT\n"
// Read only: nothing in here writes to the flash. The password is never sent, but the data is not encrypted.
// See esp_flash_dump.py for the client.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "lwip/sockets.h"
#include "esp_flash.h"
#include "esp_heap_caps.h"
#include "esp_ota_ops.h"
#include "esp_partition.h"
#include "esp_random.h"
#include "mbedtls/sha256.h"

static const uint16_t FD_PORT = 8267;
static const size_t FD_CHUNK = 4096;
static char fd_password[65];
static bool fd_started = false;

inline void fd_hex_(const uint8_t *in, size_t n, char *out) {
  static const char digits[] = "0123456789abcdef";
  for (size_t i = 0; i < n; i++) {
    out[2 * i] = digits[in[i] >> 4];
    out[2 * i + 1] = digits[in[i] & 0x0f];
  }
  out[2 * n] = 0;
}

inline bool fd_send_(int s, const void *data, size_t n) {
  const char *p = (const char *) data;
  while (n > 0) {
    int r = send(s, p, n, 0);
    if (r <= 0)
      return false;
    p += r;
    n -= r;
  }
  return true;
}

inline bool fd_send_str_(int s, const char *t) { return fd_send_(s, t, strlen(t)); }

// One line without the newline; false on timeout, close or overlong line.
inline bool fd_read_line_(int s, char *buf, size_t max) {
  size_t n = 0;
  for (;;) {
    char c;
    if (recv(s, &c, 1, 0) != 1)
      return false;
    if (c == '\n') {
      buf[n] = 0;
      return true;
    }
    if (n + 1 >= max)
      return false;
    buf[n++] = c;
  }
}

inline bool fd_auth_(int s) {
  uint8_t nonce[16], digest[32];
  char nonce_hex[33], expected[65], line[96], hello[48];
  esp_fill_random(nonce, sizeof(nonce));
  fd_hex_(nonce, sizeof(nonce), nonce_hex);
  snprintf(hello, sizeof(hello), "FDUMP1 %s\n", nonce_hex);
  if (!fd_send_str_(s, hello) || !fd_read_line_(s, line, sizeof(line)))
    return false;
  mbedtls_sha256_context ctx;
  mbedtls_sha256_init(&ctx);
  mbedtls_sha256_starts(&ctx, 0);
  mbedtls_sha256_update(&ctx, (const unsigned char *) nonce_hex, 32);
  mbedtls_sha256_update(&ctx, (const unsigned char *) fd_password, strlen(fd_password));
  mbedtls_sha256_finish(&ctx, digest);
  mbedtls_sha256_free(&ctx);
  fd_hex_(digest, sizeof(digest), expected);
  if (strncmp(line, "AUTH ", 5) != 0 || strlen(line) != 69)
    return false;
  uint8_t diff = 0;
  for (int i = 0; i < 64; i++)
    diff |= (uint8_t) (line[5 + i] ^ expected[i]);
  return diff == 0;
}

inline bool fd_info_(int s) {
  uint32_t size = 0, physical = 0;
  esp_flash_get_size(NULL, &size);
  esp_flash_get_physical_size(NULL, &physical);
  const esp_partition_t *run = esp_ota_get_running_partition();
  char line[192];
  snprintf(line, sizeof(line), "{\"flash_size\":%lu,\"physical_size\":%lu,\"running\":\"%s\",\"partitions\":[",
           (unsigned long) size, (unsigned long) physical, run != nullptr ? run->label : "");
  if (!fd_send_str_(s, line))
    return false;
  bool first = true;
  esp_partition_iterator_t it = esp_partition_find(ESP_PARTITION_TYPE_ANY, ESP_PARTITION_SUBTYPE_ANY, NULL);
  for (; it != NULL; it = esp_partition_next(it)) {
    const esp_partition_t *p = esp_partition_get(it);
    snprintf(line, sizeof(line), "%s{\"label\":\"%s\",\"type\":%d,\"subtype\":%d,\"addr\":%lu,\"size\":%lu}",
             first ? "" : ",", p->label, (int) p->type, (int) p->subtype, (unsigned long) p->address,
             (unsigned long) p->size);
    first = false;
    if (!fd_send_str_(s, line)) {
      esp_partition_iterator_release(it);
      return false;
    }
  }
  esp_partition_iterator_release(it);
  return fd_send_str_(s, "]}\n");
}

// false closes the connection; a client sees a short or missing SHA line.
inline bool fd_read_flash_(int s, uint32_t off, uint32_t len) {
  uint32_t size = 0;
  esp_flash_get_size(NULL, &size);
  if (len == 0 || off >= size || len > size - off)
    return fd_send_str_(s, "ERR range\n");
  uint8_t *buf = (uint8_t *) heap_caps_malloc(FD_CHUNK, MALLOC_CAP_INTERNAL | MALLOC_CAP_8BIT);
  if (buf == nullptr)
    return fd_send_str_(s, "ERR memory\n");
  char line[96];
  snprintf(line, sizeof(line), "OK %lu\n", (unsigned long) len);
  bool ok = fd_send_str_(s, line);
  mbedtls_sha256_context ctx;
  mbedtls_sha256_init(&ctx);
  mbedtls_sha256_starts(&ctx, 0);
  while (ok && len > 0) {
    size_t n = len < FD_CHUNK ? len : FD_CHUNK;
    ok = esp_flash_read(NULL, buf, off, n) == ESP_OK && fd_send_(s, buf, n);
    if (ok) {
      mbedtls_sha256_update(&ctx, buf, n);
      off += n;
      len -= n;
      vTaskDelay(1);   // leave the CPU to WiFi and the main loop
    }
  }
  if (ok) {
    uint8_t digest[32];
    char hex[65];
    mbedtls_sha256_finish(&ctx, digest);
    fd_hex_(digest, sizeof(digest), hex);
    snprintf(line, sizeof(line), "SHA %s\n", hex);
    ok = fd_send_str_(s, line);
  }
  mbedtls_sha256_free(&ctx);
  heap_caps_free(buf);
  return ok;
}

inline void fd_client_(int s) {
  if (!fd_auth_(s))
    return;
  if (!fd_send_str_(s, "OK\n"))
    return;
  char line[64];
  while (fd_read_line_(s, line, sizeof(line))) {
    if (strcmp(line, "QUIT") == 0)
      return;
    bool ok;
    if (strcmp(line, "INFO") == 0) {
      ok = fd_info_(s);
    } else if (strncmp(line, "READ ", 5) == 0) {
      char *end;
      uint32_t off = strtoul(line + 5, &end, 0);
      uint32_t len = strtoul(end, NULL, 0);
      ok = fd_read_flash_(s, off, len);
    } else {
      ok = fd_send_str_(s, "ERR command\n");
    }
    if (!ok)
      return;
  }
}

inline void fd_task_(void *arg) {
  int ls = ::socket(AF_INET, SOCK_STREAM, 0);   // :: because of the namespace esphome::socket
  int on = 1;
  setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(FD_PORT);
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  if (ls < 0 || bind(ls, (struct sockaddr *) &addr, sizeof(addr)) != 0 || listen(ls, 1) != 0) {
    if (ls >= 0)
      close(ls);
    vTaskDelete(NULL);
    return;
  }
  for (;;) {
    int s = accept(ls, NULL, NULL);
    if (s < 0) {
      vTaskDelay(pdMS_TO_TICKS(1000));
      continue;
    }
    struct timeval tv = {10, 0};
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    fd_client_(s);
    shutdown(s, SHUT_RDWR);
    close(s);
  }
}

// Call once the network is up.
inline void fd_start(const char *password) {
  if (fd_started || password == nullptr || password[0] == 0)
    return;
  strncpy(fd_password, password, sizeof(fd_password) - 1);
  fd_started = true;
  xTaskCreate(fd_task_, "flash_dump", 5632, NULL, 1, NULL);
}
