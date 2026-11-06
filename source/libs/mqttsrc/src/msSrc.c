/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Orchestration: MQTT callback -> mapper -> batch -> TDengine sink.
 */

#include "msInt.h"

struct SMsSrc {
  SMsCfg         cfg;
  SMsSink       *sink;
  volatile int32_t stop;
  SMsStats       stats;
  char         **batch;
  int32_t        batchNum;
  int32_t        batchCap;
  int64_t        lastFlushMs;
};

static char *dupLine(const char *p) {
  size_t n = strlen(p) + 1;
  char *q = (char *)malloc(n);
  if (q != NULL) memcpy(q, p, n);
  return q;
}

static void freeBatch(SMsSrc *src) {
  for (int32_t i = 0; i < src->batchNum; ++i) free(src->batch[i]);
  src->batchNum = 0;
}

static void flushBatch(SMsSrc *src) {
  if (src->batchNum <= 0) return;
  int32_t n = src->batchNum;
  int32_t rc = msSinkWrite(src->sink, src->batch, n);
  ++src->stats.flushes;
  if (rc == 0) {
    src->stats.written += (uint64_t)n;
  } else {
    src->stats.failed += (uint64_t)n;
    msLog(MS_LOG_ERROR, "batch of %d row(s) dropped after retries", n);
  }
  freeBatch(src);
  src->lastFlushMs = msNowMs();
}

static void onMessage(void *arg, const char *topic, const uint8_t *payload,
                      int32_t len, int32_t qos, int32_t packetId) {
  (void)qos;
  (void)packetId;
  SMsSrc *src = (SMsSrc *)arg;
  ++src->stats.received;

  char *lines = NULL;
  int32_t num = 0;
  char err[MS_ERR_LEN];
  if (msMapPayload(&src->cfg, (const char *)payload, len, topic, msNowMs(),
                   &lines, &num, err, sizeof(err)) != 0) {
    ++src->stats.parseErrors;
    msLog(MS_LOG_WARN, "drop message on topic '%s': %s", topic, err);
    return;
  }

  const char *p = lines;
  for (int32_t i = 0; i < num; ++i) {
    char *line = dupLine(p);
    p += strlen(p) + 1;
    if (line == NULL) continue;
    src->batch[src->batchNum++] = line;
    ++src->stats.lines;
    ++src->stats.parsed;
    if (src->batchNum >= src->cfg.sink.batchSize) flushBatch(src);
  }
  free(lines);
}

static void onTick(void *arg) {
  SMsSrc *src = (SMsSrc *)arg;
  if (src->batchNum > 0 && msNowMs() - src->lastFlushMs >= src->cfg.sink.flushMs) {
    flushBatch(src);
  }
}

int32_t msSrcRun(const SMsCfg *cfg, SMsSrc **ppSrc) {
  if (ppSrc == NULL) return -1;
  *ppSrc = NULL;
  char err[MS_ERR_LEN];
  if (msValidateConfig(cfg, err, sizeof(err)) != 0) {
    msLog(MS_LOG_ERROR, "invalid config: %s", err);
    return -1;
  }

  SMsSrc *src = (SMsSrc *)calloc(1, sizeof(SMsSrc));
  if (src == NULL) return -1;
  src->cfg = *cfg;
  src->batchCap = cfg->sink.batchSize > 0 ? cfg->sink.batchSize : 1;
  src->batch = (char **)calloc((size_t)src->batchCap, sizeof(char *));
  if (src->batch == NULL) {
    free(src);
    return -1;
  }
  src->lastFlushMs = msNowMs();
  *ppSrc = src; /* publish handle before blocking so stop requests work */

  if (msSinkOpen(&cfg->sink, cfg->mapping.tbnameKey, &src->sink) != 0) {
    msLog(MS_LOG_ERROR, "cannot open TDengine sink");
    src->stop = 1;
    return -1;
  }

  SMsMqttOpts opts;
  memset(&opts, 0, sizeof(opts));
  opts.cfg = &src->cfg.broker;
  opts.stop = &src->stop;
  opts.onMessage = onMessage;
  opts.onTick = onTick;
  opts.arg = src;
  opts.reconnects = &src->stats.reconnects;

  int32_t rc = msMqttLoop(&opts);
  flushBatch(src); /* drain whatever is left before exit */
  msSinkClose(src->sink);
  src->sink = NULL;

  msLog(MS_LOG_INFO,
        "stopped: received=%llu parsed=%llu written=%llu failed=%llu "
        "parseErrors=%llu reconnects=%llu",
        (unsigned long long)src->stats.received, (unsigned long long)src->stats.parsed,
        (unsigned long long)src->stats.written, (unsigned long long)src->stats.failed,
        (unsigned long long)src->stats.parseErrors,
        (unsigned long long)src->stats.reconnects);
  return rc;
}

void msSrcStop(SMsSrc *src) {
  if (src != NULL) src->stop = 1;
}

void msSrcStats(SMsSrc *src, SMsStats *stats) {
  if (src == NULL || stats == NULL) return;
  *stats = src->stats;
}

void msSrcFree(SMsSrc *src) {
  if (src == NULL) return;
  freeBatch(src);
  free(src->batch);
  if (src->sink != NULL) msSinkClose(src->sink);
  free(src);
}

const char *msVersion(void) { return "1.0.0"; }
