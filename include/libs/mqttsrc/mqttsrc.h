/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * mqttsrc - MQTT data source for TDengine.
 *
 * Direction: external MQTT broker -> subscribe -> parse JSON payload ->
 *            template mapping -> schemaless line protocol -> TDengine.
 *
 * This is the INGRESS direction. The built-in tmqtt/bnode broker is the
 * opposite (TDengine TMQ topic -> MQTT publish) and is not used here.
 */

#ifndef TDENGINE_MQTTSRC_H
#define TDENGINE_MQTTSRC_H

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MS_MAX_TOPICS   32
#define MS_MAX_COLUMNS  64
#define MS_MAX_TAGS     64
#define MS_MAX_TVARS    32
#define MS_MAX_RULES    8
#define MS_MAX_WHEN     8
#define MS_NAME_LEN     128
#define MS_TEXT_LEN     256
#define MS_PATH_LEN     256
#define MS_ERR_LEN      512

/* Timestamp precision of the incoming JSON timestamp field. */
typedef enum {
  MS_TS_SECOND = 1,
  MS_TS_MILLI  = 2,
  MS_TS_MICRO  = 3,
  MS_TS_NANO   = 4,
} EMsTsPrecision;

/* Value type hint for a mapped column.
 * MS_COL_NUMBER always emits DOUBLE: mixing JSON integers and floats in one
 * column then stays type-stable. Use MS_COL_INT to pin BIGINT (i64). */
typedef enum {
  MS_COL_AUTO   = 0,
  MS_COL_NUMBER = 1,
  MS_COL_STRING = 2,
  MS_COL_BOOL   = 3,
  MS_COL_INT    = 4,
} EMsColType;

typedef struct {
  char filter[MS_TEXT_LEN]; /* MQTT topic filter, may contain + and # */
  int  qos;                 /* 0, 1 or 2 */
} SMsTopic;

typedef struct {
  char     host[MS_TEXT_LEN];
  uint16_t port;
  char     clientId[MS_NAME_LEN];
  char     username[MS_NAME_LEN];
  char     password[MS_TEXT_LEN];
  int      keepalive;       /* seconds */
  bool     cleanSession;
  int      reconnectMs;     /* initial backoff */
  int      maxReconnectMs;  /* backoff cap */
  int      connectTimeoutMs;
  SMsTopic topics[MS_MAX_TOPICS];
  int      topicNum;
  /* One name per topic level; "_" ignores that level. Bound to {name}. */
  char     topicVars[MS_MAX_TVARS][MS_NAME_LEN];
  int      topicVarNum;
} SMsBrokerCfg;

typedef struct {
  char     host[MS_TEXT_LEN];
  uint16_t port;
  char     user[MS_NAME_LEN];
  char     password[MS_TEXT_LEN];
  char     db[MS_NAME_LEN];
  bool     createDb;      /* CREATE DATABASE IF NOT EXISTS before insert */
  int      batchSize;     /* flush after this many lines */
  int      flushMs;       /* or after this many milliseconds */
  int      maxRetries;    /* per batch, 0 disables retry */
  int      retryMs;
  int32_t  ttl;           /* 0 => server default */
  bool     dryRun;        /* print line protocol instead of writing */
} SMsSinkCfg;

typedef struct {
  char name[MS_NAME_LEN];       /* TDengine column name */
  char path[MS_PATH_LEN];       /* dot separated JSON path, e.g. "data.temp" */
  int  type;                    /* EMsColType */
} SMsColumn;

typedef struct {
  char name[MS_NAME_LEN];       /* TDengine tag name */
  char value[MS_TEXT_LEN];      /* template, e.g. "t_{dev}_{site}" */
} SMsTag;

/* Rule matcher. "path" resolves against JSON payload fields, topic vars and
 * the builtins {_topic}/{_topic_flat}/{_now}. All conditions are ANDed. */
typedef enum {
  MS_WHEN_EQ      = 0,
  MS_WHEN_NE      = 1,
  MS_WHEN_PREFIX  = 2,
  MS_WHEN_CONTAINS= 3,
  MS_WHEN_IN      = 4,
  MS_WHEN_EXISTS  = 5,
} EMsWhenOp;

typedef struct {
  char    path[MS_PATH_LEN];
  int32_t op;                    /* EMsWhenOp */
  char    value[MS_TEXT_LEN];    /* MS_WHEN_IN: comma separated list */
} SMsWhen;

/* One routing rule = its own table model. Columns/tags are slices of the
 * shared pools in SMsMappingCfg, so a config stays small enough to copy. */
typedef struct {
  char    name[MS_NAME_LEN];
  SMsWhen when[MS_MAX_WHEN];
  int32_t whenNum;
  char    stable[MS_NAME_LEN];
  char    table[MS_NAME_LEN];
  char    tbnameKey[MS_NAME_LEN];
  char    tsField[MS_PATH_LEN];
  int     tsPrecision;
  int32_t colStart, colNum;
  int32_t tagStart, tagNum;
} SMsRule;

typedef struct {
  char stable[MS_NAME_LEN];     /* super table name template */
  char table[MS_NAME_LEN];      /* child table name template */
  char tbnameKey[MS_NAME_LEN];  /* tag carrying child table name (default tbname) */
  char tsField[MS_PATH_LEN];    /* empty => broker receive time */
  int  tsPrecision;             /* EMsTsPrecision of tsField */
  /* Default model. Also the fallback when no rule matches. */
  SMsColumn columns[MS_MAX_COLUMNS];
  int       columnNum;
  SMsTag    tags[MS_MAX_TAGS];
  int       tagNum;
  /* columns[]/tags[] are shared pools: the default model occupies
   * [0, columnNum) / [0, tagNum), each rule owns a later slice. */
  SMsRule rules[MS_MAX_RULES];
  int     ruleNum;
} SMsMappingCfg;

typedef struct {
  SMsBrokerCfg  broker;
  SMsSinkCfg    sink;
  SMsMappingCfg mapping;
} SMsCfg;

typedef struct {
  uint64_t received;    /* PUBLISH packets */
  uint64_t parsed;      /* JSON records (one message may hold many) */
  uint64_t lines;       /* line protocol rows produced */
  uint64_t written;     /* rows accepted by the sink */
  uint64_t failed;      /* rows rejected after all retries */
  uint64_t parseErrors; /* malformed JSON / missing fields */
  uint64_t reconnects;
  uint64_t flushes;
} SMsStats;

typedef struct SMsSrc SMsSrc; /* opaque runtime handle */

/* Fill cfg with defaults. Always call before msLoadConfig(). */
void msCfgDefault(SMsCfg *cfg);

/* Load a JSON config file. Returns 0 on success, -1 on failure (err is set). */
int32_t msLoadConfig(const char *path, SMsCfg *cfg, char *err, int32_t errLen);

/* Validate a config. Returns 0 on success, -1 on failure (err is set). */
int32_t msValidateConfig(const SMsCfg *cfg, char *err, int32_t errLen);

/* Start the ingestion loop in the caller's thread (blocks until stopped). */
int32_t msSrcRun(const SMsCfg *cfg, SMsSrc **ppSrc);

/* Request a graceful stop. The stop flag uses an atomic store, so this is safe
 * from another thread. When called from a signal/console-control handler, the
 * caller must unregister that handler before calling msSrcFree(). */
void msSrcStop(SMsSrc *src);

/* Stats are updated without locks; only read after msSrcRun() has returned. */
void msSrcStats(SMsSrc *src, SMsStats *stats);

/* Release a handle returned by msSrcRun(). */
void msSrcFree(SMsSrc *src);

const char *msVersion(void);

/* ------------------------------------------------------------------ *
 * Helpers exported for unit tests. They are pure functions and do not
 * require an MQTT broker or a TDengine server.
 * ------------------------------------------------------------------ */

/* Parse one MQTT PUBLISH payload into line protocol lines.
 * topic: the concrete topic the message arrived on.
 * recvMs: broker receive time in milliseconds (used when tsField is empty).
 * out: receives NUL separated lines; *outNum is the number of lines.
 * Caller frees *out with free(). Returns 0 on success. */
int32_t msMapPayload(const SMsCfg *cfg, const char *payload, int32_t len,
                     const char *topic, int64_t recvMs, char **out,
                     int32_t *outNum, char *err, int32_t errLen);

/* MQTT remaining-length varint.
 * Encode returns the number of bytes written (1-4) or -1.
 * Decode returns 0 on success, 1 when more bytes are needed, -1 if malformed;
 * *used receives the varint byte count. */
int32_t msMqttEncodeRemainLen(uint32_t len, uint8_t *buf, int32_t bufLen);
int32_t msMqttDecodeRemainLen(const uint8_t *buf, int32_t bufLen,
                              uint32_t *value, int32_t *used);

#ifdef __cplusplus
}
#endif

#endif /* TDENGINE_MQTTSRC_H */
