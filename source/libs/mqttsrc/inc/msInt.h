/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Internal declarations shared by the mqttsrc translation units.
 */

#ifndef TDENGINE_MQTTSRC_INT_H
#define TDENGINE_MQTTSRC_INT_H

#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mqttsrc.h"

/* cJSON is a private implementation detail of the mapper/config units;
 * keep the public internal header free of third-party includes. */
typedef struct tools_cJSON tools_cJSON;

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <intrin.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <errno.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netdb.h>
#include <unistd.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* Stop/handle publication between the signal/console thread and the loop.
 * Compiler builtins avoid a dependency on C11 <stdatomic.h> (MSVC would need
 * /std:c11) while still giving seq-cst ordering and signal-safe stores. */
#if defined(_MSC_VER)
#define MS_ATOMIC_STORE(p, v) _InterlockedExchange((volatile long *)(p), (long)(v))
#define MS_ATOMIC_LOAD(p)     _InterlockedCompareExchange((volatile long *)(p), 0, 0)
#elif defined(__GNUC__) || defined(__clang__)
#define MS_ATOMIC_STORE(p, v) __atomic_store_n((p), (v), __ATOMIC_SEQ_CST)
#define MS_ATOMIC_LOAD(p)     __atomic_load_n((p), __ATOMIC_SEQ_CST)
#else
#define MS_ATOMIC_STORE(p, v) (*(p) = (v))
#define MS_ATOMIC_LOAD(p)     (*(p))
#endif
#define MS_STOPPED(stop)      (MS_ATOMIC_LOAD(stop) != 0)

#define MS_LOG_DEBUG 0
#define MS_LOG_INFO  1
#define MS_LOG_WARN  2
#define MS_LOG_ERROR 3

void msSetLogLevel(int32_t level);
void msLog(int32_t level, const char *fmt, ...);

int64_t msNowMs(void);
void    msSleepMs(int32_t ms);

/* ---------------- growable string buffer ---------------- */
typedef struct {
  char   *data;
  int32_t len;
  int32_t cap;
  int32_t failed;
} SStr;

int32_t ssInit(SStr *s, int32_t cap);
void    ssFree(SStr *s);
void    ssClear(SStr *s);
int32_t ssPut(SStr *s, const char *p, int32_t n);
int32_t ssPutStr(SStr *s, const char *p);
int32_t ssPutChar(SStr *s, char c);
int32_t ssPrintf(SStr *s, const char *fmt, ...);
/* mode 1: measurement / tag key / tag value; mode 2: quoted field value */
int32_t ssPutEscaped(SStr *s, const char *p, int32_t mode);
/* Length-aware variant: also handles embedded NUL / control bytes, which a
 * NUL-terminated payload may legitimately contain. Control bytes are replaced
 * by spaces so they cannot terminate or break the line protocol. */
int32_t ssPutEscapedN(SStr *s, const char *p, int32_t n, int32_t mode);
/* append NUL and hand the buffer to the caller (caller frees with free()) */
char   *ssDetach(SStr *s);

/* ---------------- template rendering ---------------- */
/* Return 0 and fill buf when the key resolves; -1 when unknown. */
typedef int32_t (*MsLookup)(void *arg, const char *key, char *buf, int32_t bufLen);
int32_t msRender(const char *tpl, MsLookup lookup, void *arg, SStr *out);

/* ---------------- JSON value extraction ---------------- */
#define MSV_NULL   0
#define MSV_INT    1
#define MSV_DOUBLE 2
#define MSV_STRING 3
#define MSV_BOOL   4
#define MSV_OTHER  5

typedef struct {
  int32_t type;
  char    text[MS_TEXT_LEN];
} SMsValue;

/* Dot separated path lookup. Returns 0 on success, -1 when absent. */
int32_t msJsonPath(tools_cJSON *root, const char *path, SMsValue *val);

/* ---------------- MQTT client ---------------- */
/* Return 0 when the message is durably accepted, non-zero when it must NOT
 * be acknowledged: the broker is then expected to redeliver the PUBLISH.
 * Malformed messages should still return 0 (drop + count, never stall). */
typedef int32_t (*MsMsgCb)(void *arg, const char *topic, const uint8_t *payload,
                           int32_t len, int32_t qos, int32_t packetId);
typedef void (*MsTickCb)(void *arg);

typedef struct {
  const SMsBrokerCfg *cfg;
  volatile int32_t   *stop;
  MsMsgCb             onMessage;
  MsTickCb            onTick;
  void               *arg;
  uint64_t           *reconnects;
} SMsMqttOpts;

/* Blocking loop. Returns 0 on graceful stop, -1 on unrecoverable error. */
int32_t msMqttLoop(const SMsMqttOpts *opts);

/* ---------------- TDengine sink ---------------- */
typedef struct SMsSink SMsSink;

int32_t msSinkOpen(const SMsSinkCfg *cfg, const char *tbnameKey, SMsSink **pp);
int32_t msSinkWrite(SMsSink *sink, char **lines, int32_t num);
void    msSinkClose(SMsSink *sink);

/* ---------------- misc ---------------- */
int32_t msStrToInt(const char *p, int32_t def);
void    msStrCopy(char *dst, int32_t cap, const char *src);

#ifdef __cplusplus
}
#endif

#endif /* TDENGINE_MQTTSRC_INT_H */
