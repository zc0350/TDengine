/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Platform helpers, growable buffer, template renderer, JSON path lookup.
 */

#include "msInt.h"
#include "toolscJson.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdarg.h>
#include <time.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/time.h>
#endif

static int32_t gLogLevel = MS_LOG_INFO;

void msSetLogLevel(int32_t level) { gLogLevel = level; }

void msLog(int32_t level, const char *fmt, ...) {
  if (level < gLogLevel) return;
  static const char tag[] = {'D', 'I', 'W', 'E'};
  if (level < 0) level = 0;
  if (level > 3) level = 3;

  fprintf(stderr, "[mqttsrc][%c][%lld] ", tag[level], (long long)msNowMs());
  va_list ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);
  fputc('\n', stderr);
  fflush(stderr);
}

int64_t msNowMs(void) {
#ifdef _WIN32
  FILETIME ft;
  ULARGE_INTEGER ul;
  GetSystemTimeAsFileTime(&ft);
  ul.LowPart = ft.dwLowDateTime;
  ul.HighPart = ft.dwHighDateTime;
  /* 1601-01-01 -> 1970-01-01, in 100ns units */
  return (int64_t)(ul.QuadPart / 10000) - 11644473600000LL;
#else
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
#endif
}

void msSleepMs(int32_t ms) {
  if (ms <= 0) return;
#ifdef _WIN32
  Sleep((DWORD)ms);
#else
  usleep((useconds_t)ms * 1000);
#endif
}

/* ---------------- growable buffer ---------------- */

int32_t ssInit(SStr *s, int32_t cap) {
  if (cap < 64) cap = 64;
  s->data = (char *)malloc((size_t)cap);
  s->len = 0;
  s->cap = cap;
  s->failed = 0;
  if (s->data == NULL) {
    s->cap = 0;
    s->failed = 1;
    return -1;
  }
  s->data[0] = '\0';
  return 0;
}

void ssFree(SStr *s) {
  free(s->data);
  s->data = NULL;
  s->len = 0;
  s->cap = 0;
}

void ssClear(SStr *s) {
  s->len = 0;
  s->failed = 0;
  if (s->data != NULL) s->data[0] = '\0';
}

static int32_t ssReserve(SStr *s, int32_t extra) {
  if (s->failed) return -1;
  int64_t need = (int64_t)s->len + extra + 1;
  if (need <= s->cap) return 0;
  int64_t cap = s->cap ? (int64_t)s->cap : 64;
  while (cap < need) cap *= 2;
  if (cap > 64 * 1024 * 1024) {
    s->failed = 1;
    return -1;
  }
  char *p = (char *)realloc(s->data, (size_t)cap);
  if (p == NULL) {
    s->failed = 1;
    return -1;
  }
  s->data = p;
  s->cap = (int32_t)cap;
  return 0;
}

int32_t ssPut(SStr *s, const char *p, int32_t n) {
  if (n <= 0) return 0;
  if (p == NULL || ssReserve(s, n) != 0) return -1;
  memcpy(s->data + s->len, p, (size_t)n);
  s->len += n;
  s->data[s->len] = '\0';
  return 0;
}

int32_t ssPutStr(SStr *s, const char *p) { return p ? ssPut(s, p, (int32_t)strlen(p)) : 0; }

int32_t ssPutChar(SStr *s, char c) { return ssPut(s, &c, 1); }

int32_t ssPrintf(SStr *s, const char *fmt, ...) {
  /* Fixed scratch buffer: all call sites format short scalar values.
   * Avoids va_copy, which is not available on every supported toolchain. */
  char tmp[4096];
  va_list ap;
  va_start(ap, fmt);
  int n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
  va_end(ap);
  if (n < 0 || n >= (int)sizeof(tmp)) return -1;
  return ssPut(s, tmp, n);
}

/* Length-aware escaping. Control bytes (including an embedded NUL) are
 * replaced by spaces: a NUL would otherwise truncate the value, and raw
 * newlines/tabs would break the line-protocol framing. */
int32_t ssPutEscapedN(SStr *s, const char *p, int32_t n, int32_t mode) {
  if (p == NULL || n <= 0) return 0;
  for (int32_t i = 0; i < n; ++i) {
    unsigned char c = (unsigned char)p[i];
    if (c < 0x20 || c == 0x7f) {
      if (ssPutChar(s, ' ') != 0) return -1;
      continue;
    }
    bool esc = (mode == 1)
                   ? (c == ' ' || c == ',' || c == '=' || c == '"' || c == '\\')
                   : (c == '"' || c == '\\');
    if (esc && ssPutChar(s, '\\') != 0) return -1;
    if (ssPutChar(s, (char)c) != 0) return -1;
  }
  return 0;
}

int32_t ssPutEscaped(SStr *s, const char *p, int32_t mode) {
  if (p == NULL) return 0;
  return ssPutEscapedN(s, p, (int32_t)strlen(p), mode);
}

char *ssDetach(SStr *s) {
  if (s->failed) {
    ssFree(s);
    return NULL;
  }
  if (s->data == NULL) {
    if (ssInit(s, 64) != 0) return NULL;
  }
  s->data[s->len] = '\0';
  char *p = s->data;
  s->data = NULL;
  s->len = 0;
  s->cap = 0;
  s->failed = 0;
  return p;
}

/* ---------------- template rendering ---------------- */

int32_t msRender(const char *tpl, MsLookup lookup, void *arg, SStr *out) {
  if (tpl == NULL) return -1;
  const char *p = tpl;
  while (*p) {
    if (*p != '{') {
      if (ssPutChar(out, *p++) != 0) return -1;
      continue;
    }
    const char *end = strchr(p + 1, '}');
    if (end == NULL || end == p + 1) return -1;
    int32_t keyLen = (int32_t)(end - p - 1);
    if (keyLen >= MS_PATH_LEN) return -1;
    char key[MS_PATH_LEN];
    memcpy(key, p + 1, (size_t)keyLen);
    key[keyLen] = '\0';

    char val[MS_TEXT_LEN];
    if (lookup == NULL || lookup(arg, key, val, (int32_t)sizeof(val)) != 0) return -1;
    if (ssPutStr(out, val) != 0) return -1;
    p = end + 1;
  }
  return 0;
}

/* ---------------- JSON path ---------------- */

static tools_cJSON *childByPath(tools_cJSON *node, const char *seg, int32_t len) {
  if (node == NULL || len <= 0) return NULL;
  if (tools_cJSON_IsArray(node) && isdigit((unsigned char)seg[0])) {
    int idx = atoi(seg);
    return tools_cJSON_GetArrayItem(node, idx);
  }
  char key[MS_PATH_LEN];
  if (len >= MS_PATH_LEN) len = MS_PATH_LEN - 1;
  memcpy(key, seg, (size_t)len);
  key[len] = '\0';
  return tools_cJSON_GetObjectItemCaseSensitive(node, key);
}

int32_t msJsonPath(tools_cJSON *root, const char *path, SMsValue *val) {
  if (root == NULL || path == NULL || path[0] == '\0' || val == NULL) return -1;
  memset(val, 0, sizeof(*val));

  tools_cJSON *node = root;
  const char *p = path;
  while (*p) {
    const char *dot = strchr(p, '.');
    int32_t segLen = dot ? (int32_t)(dot - p) : (int32_t)strlen(p);
    node = childByPath(node, p, segLen);
    if (node == NULL) return -1;
    if (dot == NULL) break;
    p = dot + 1;
  }

  if (tools_cJSON_IsNumber(node)) {
    double d = node->valuedouble;
    if (d > -9.0e18 && d < 9.0e18 && d == (double)(long long)d) {
      val->type = MSV_INT;
      snprintf(val->text, sizeof(val->text), "%lld", (long long)d);
    } else {
      val->type = MSV_DOUBLE;
      snprintf(val->text, sizeof(val->text), "%.17g", d);
    }
  } else if (tools_cJSON_IsString(node)) {
    val->type = MSV_STRING;
    if (node->valuestring != NULL) {
      snprintf(val->text, sizeof(val->text), "%s", node->valuestring);
    }
  } else if (tools_cJSON_IsTrue(node) || tools_cJSON_IsFalse(node)) {
    val->type = MSV_BOOL;
    snprintf(val->text, sizeof(val->text), "%s",
             tools_cJSON_IsTrue(node) ? "true" : "false");
  } else if (tools_cJSON_IsNull(node)) {
    val->type = MSV_NULL;
  } else {
    val->type = MSV_OTHER;
    char *printed = tools_cJSON_PrintUnformatted(node);
    if (printed != NULL) {
      snprintf(val->text, sizeof(val->text), "%s", printed);
      free(printed);
    }
  }
  return 0;
}

int32_t msStrToInt(const char *p, int32_t def) {
  if (p == NULL || *p == '\0') return def;
  char *end = NULL;
  errno = 0;
  long v = strtol(p, &end, 10);
  if (end == p || *end != '\0' || errno == ERANGE || v < INT32_MIN || v > INT32_MAX) {
    return def;
  }
  return (int32_t)v;
}

void msStrCopy(char *dst, int32_t cap, const char *src) {
  if (cap <= 0) return;
  if (src == NULL) {
    dst[0] = '\0';
    return;
  }
  /* jstr() returns the default value, which is often dst itself. snprintf()
   * with overlapping source/destination is undefined behaviour. */
  if (src == dst) return;
  if (strlen(src) >= (size_t)cap) {
    msLog(MS_LOG_WARN, "config string truncated to %d bytes", cap - 1);
  }
  snprintf(dst, (size_t)cap, "%s", src);
}
