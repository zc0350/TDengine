/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Orchestration: MQTT callback -> mapper -> bounded queue -> writer thread.
 *
 * The MQTT thread never blocks on TDengine. It maps a payload to line
 * protocol rows, enqueues them as one unit and returns. A dedicated writer
 * thread groups queued messages into batches (batch_size / flush_ms), writes
 * them, and only then reports the QoS1/2 packet ids back to the MQTT thread,
 * which sends the PUBACK/PUBREC. That keeps the "acknowledge only after
 * persistence" guarantee without flushing one message at a time.
 */

#include "msInt.h"

/* How long a producer may wait for queue space before giving up and forcing a
 * reconnect. Bounded so a wedged sink cannot pin the process forever. */
#define MS_ENQUEUE_WAIT_MS 5000

/* Size of the persisted-QoS2 ring used for cross-reconnect de-duplication. */
#define MS_PERSISTED_MAX 1024

typedef struct {
  char    *buf;   /* NUL separated lines, owned */
  char   **lines; /* pointers into buf */
  int32_t  num;
  int32_t  qos;
  uint16_t packetId;
} SMsMsg;

struct SMsSrc {
  SMsCfg    cfg;
  SMsSink  *sink;
  volatile int32_t stop;

  MsMutex lock;
  MsCond  hasData;
  MsCond  hasSpace;
  int32_t locksInited;

  SMsMsg *q;
  int32_t qCap, qHead, qCount;

  SMsAck *acks;
  int32_t ackCap, ackHead, ackCount;
  /* Recently persisted QoS2 ids. Kept across a resumed session so a
   * redelivered copy is acknowledged instead of inserted twice. */
  uint16_t persisted[MS_PERSISTED_MAX];
  int32_t  persistedPos;

  SMsMsg *held; /* writer thread only */
  int32_t heldNum;
  char  **batch; /* writer thread only */
  int32_t batchCap, batchNum;
  int64_t batchStartMs;

  volatile int32_t writeFailed;
  int32_t failedBatches;

  MsThread writer;
  int32_t  writerStarted;

  SMsStats stats;
  uint64_t lastParseErrors;
  int64_t  lastStatsMs;
};

static void freeMsg(SMsMsg *m) {
  free(m->buf);
  free(m->lines);
  m->buf = NULL;
  m->lines = NULL;
  m->num = 0;
}

static int32_t splitLines(char *buf, int32_t num, char ***out) {
  char **v = (char **)malloc(sizeof(char *) * (size_t)num);
  if (v == NULL) return -1;
  char *p = buf;
  for (int32_t i = 0; i < num; ++i) {
    v[i] = p;
    p += strlen(p) + 1;
  }
  *out = v;
  return 0;
}

static void pushAckLocked(SMsSrc *src, uint16_t id, int32_t qos) {
  if (qos <= 0 || id == 0) return;
  if (qos == 2) {
    src->persisted[src->persistedPos] = id;
    src->persistedPos = (src->persistedPos + 1) % MS_PERSISTED_MAX;
  }
  if (src->ackCount >= src->ackCap) {
    /* Cannot happen while the number of in-flight messages is bounded by the
     * queue, but never overwrite a live entry if it does. */
    src->ackHead = (src->ackHead + 1) % src->ackCap;
    --src->ackCount;
  }
  int32_t idx = (src->ackHead + src->ackCount) % src->ackCap;
  src->acks[idx].packetId = id;
  src->acks[idx].qos = (uint8_t)qos;
  ++src->ackCount;
}

/* Called by the MQTT thread once per loop iteration. */
static int32_t pollAcks(void *arg, SMsAck *out, int32_t max) {
  SMsSrc *src = (SMsSrc *)arg;
  if (src == NULL) return 0;
  if (max <= 0) {
    msMutexLock(&src->lock);
    src->ackHead = 0;
    src->ackCount = 0;
    /* A clean session restarts packet ids, so the cross-connection de-dup
     * ring would alias unrelated messages. */
    if (src->cfg.broker.cleanSession) {
      memset(src->persisted, 0, sizeof(src->persisted));
      src->persistedPos = 0;
    }
    msMutexUnlock(&src->lock);
    return 0;
  }
  msMutexLock(&src->lock);
  int32_t n = 0;
  while (n < max && src->ackCount > 0) {
    out[n++] = src->acks[src->ackHead];
    src->ackHead = (src->ackHead + 1) % src->ackCap;
    --src->ackCount;
  }
  src->stats.acksDeferred += (uint64_t)n;
  int32_t failed = MS_ATOMIC_LOAD(&src->writeFailed);
  msMutexUnlock(&src->lock);
  /* Report the failure only once every completed ACK has been handed over, so
   * a successful batch is never discarded just because a later one failed. */
  if (failed && n == 0) {
    MS_ATOMIC_STORE(&src->writeFailed, 0);
    return -1;
  }
  return n;
}

static int32_t wasPersisted(void *arg, uint16_t packetId) {
  SMsSrc *src = (SMsSrc *)arg;
  if (src == NULL || packetId == 0) return 0;
  int32_t found = 0;
  msMutexLock(&src->lock);
  for (int32_t i = 0; i < MS_PERSISTED_MAX; ++i) {
    if (src->persisted[i] == packetId) {
      found = 1;
      break;
    }
  }
  msMutexUnlock(&src->lock);
  return found;
}

static void forgetPersisted(void *arg, uint16_t packetId) {
  SMsSrc *src = (SMsSrc *)arg;
  if (src == NULL || packetId == 0) return;
  msMutexLock(&src->lock);
  for (int32_t i = 0; i < MS_PERSISTED_MAX; ++i) {
    if (src->persisted[i] == packetId) src->persisted[i] = 0;
  }
  msMutexUnlock(&src->lock);
}

static int32_t enqueue(SMsSrc *src, SMsMsg *m) {
  msMutexLock(&src->lock);
  int64_t deadline = msNowMs() + MS_ENQUEUE_WAIT_MS;
  while (src->qCount >= src->qCap) {
    if (MS_STOPPED(&src->stop)) {
      msMutexUnlock(&src->lock);
      return -1;
    }
    int64_t left = deadline - msNowMs();
    if (left <= 0) {
      msMutexUnlock(&src->lock);
      msLog(MS_LOG_WARN, "write queue full for %dms, forcing a reconnect",
            MS_ENQUEUE_WAIT_MS);
      return -1;
    }
    msCondWaitMs(&src->hasSpace, &src->lock, left > 200 ? 200 : (int32_t)left);
  }
  int32_t idx = (src->qHead + src->qCount) % src->qCap;
  src->q[idx] = *m;
  ++src->qCount;
  ++src->stats.queued;
  if ((uint64_t)src->qCount > src->stats.maxQueueDepth) {
    src->stats.maxQueueDepth = (uint64_t)src->qCount;
  }
  msCondSignal(&src->hasData);
  msMutexUnlock(&src->lock);
  return 0;
}

static int32_t onMessage(void *arg, const char *topic, const uint8_t *payload,
                         int32_t len, int32_t qos, int32_t packetId) {
  SMsSrc *src = (SMsSrc *)arg;

  char *lines = NULL;
  int32_t num = 0;
  char err[MS_ERR_LEN];
  if (msMapPayload(&src->cfg, (const char *)payload, len, topic, msNowMs(),
                   &lines, &num, err, sizeof(err)) != 0) {
    msMutexLock(&src->lock);
    ++src->stats.received;
    ++src->stats.parseErrors;
    msMutexUnlock(&src->lock);
    msLog(MS_LOG_WARN, "drop message on topic '%s': %s", topic, err);
    /* Malformed input is dropped deliberately: never stall the broker. */
    return 0;
  }

  SMsMsg m;
  memset(&m, 0, sizeof(m));
  m.buf = lines;
  m.num = num;
  m.qos = qos;
  m.packetId = (uint16_t)packetId;
  if (splitLines(lines, num, &m.lines) != 0) {
    free(lines);
    msMutexLock(&src->lock);
    ++src->stats.received;
    ++src->stats.parseErrors;
    msMutexUnlock(&src->lock);
    return 0;
  }

  if (enqueue(src, &m) != 0) {
    freeMsg(&m);
    return -1; /* no ACK: the broker redelivers after the reconnect */
  }

  msMutexLock(&src->lock);
  ++src->stats.received;
  src->stats.parsed += (uint64_t)num;
  src->stats.lines += (uint64_t)num;
  msMutexUnlock(&src->lock);
  return 0;
}

static int32_t batchReserve(SMsSrc *src, int32_t n) {
  if (n <= src->batchCap) return 0;
  int32_t cap = src->batchCap > 0 ? src->batchCap : 256;
  while (cap < n) {
    if (cap > 1024 * 1024) return -1;
    cap *= 2;
  }
  char **p = (char **)realloc(src->batch, sizeof(char *) * (size_t)cap);
  if (p == NULL) return -1;
  src->batch = p;
  src->batchCap = cap;
  return 0;
}

static void *writerMain(void *arg) {
  SMsSrc *src = (SMsSrc *)arg;
  const int32_t batchSize = src->cfg.sink.batchSize > 0 ? src->cfg.sink.batchSize : 1;
  const int32_t flushMs = src->cfg.sink.flushMs > 0 ? src->cfg.sink.flushMs : 1;

  for (;;) {
    msMutexLock(&src->lock);
    bool stopping = MS_STOPPED(&src->stop) != 0;

    if (src->qCount == 0 && src->heldNum == 0) {
      if (stopping) {
        msMutexUnlock(&src->lock);
        break;
      }
      msCondWaitMs(&src->hasData, &src->lock, 500);
      msMutexUnlock(&src->lock);
      continue;
    }

    /* Move whole messages out of the shared queue. A message is the unit of
     * acknowledgement, so it is never split across batches. */
    bool moved = false;
    while (src->qCount > 0 && src->heldNum < src->qCap && src->batchNum < batchSize) {
      SMsMsg *m = &src->q[src->qHead];
      src->qHead = (src->qHead + 1) % src->qCap;
      --src->qCount;
      src->held[src->heldNum++] = *m;
      src->batchNum += m->num;
      moved = true;
    }
    if (moved) {
      if (src->batchStartMs == 0) src->batchStartMs = msNowMs();
      msCondSignal(&src->hasSpace);
    }

    int64_t now = msNowMs();
    bool due = stopping || src->batchNum >= batchSize ||
               (src->batchStartMs != 0 && now - src->batchStartMs >= flushMs);
    if (!due) {
      int64_t wait = flushMs - (now - src->batchStartMs);
      if (wait > 200) wait = 200; /* keep stop responsive */
      if (wait < 1) wait = 1;
      msCondWaitMs(&src->hasData, &src->lock, (int32_t)wait);
      msMutexUnlock(&src->lock);
      continue;
    }

    int32_t total = 0;
    for (int32_t i = 0; i < src->heldNum; ++i) total += src->held[i].num;
    if (total <= 0) {
      for (int32_t i = 0; i < src->heldNum; ++i) freeMsg(&src->held[i]);
      src->heldNum = 0;
      src->batchNum = 0;
      src->batchStartMs = 0;
      msMutexUnlock(&src->lock);
      continue;
    }
    if (batchReserve(src, total) != 0) {
      src->stats.failed += (uint64_t)total;
      ++src->failedBatches;
      MS_ATOMIC_STORE(&src->writeFailed, 1);
      for (int32_t i = 0; i < src->heldNum; ++i) freeMsg(&src->held[i]);
      src->heldNum = 0;
      src->batchNum = 0;
      src->batchStartMs = 0;
      msMutexUnlock(&src->lock);
      msLog(MS_LOG_ERROR, "cannot grow the batch buffer, %d row(s) dropped", total);
      continue;
    }
    int32_t idx = 0;
    for (int32_t i = 0; i < src->heldNum; ++i) {
      for (int32_t j = 0; j < src->held[i].num; ++j) {
        src->batch[idx++] = src->held[i].lines[j];
      }
    }
    int32_t nMsgs = src->heldNum;
    int32_t nLines = total;
    msMutexUnlock(&src->lock);

    /* Retries happen here, on the writer thread; the MQTT thread keeps
     * reading until the bounded queue is full. */
    int32_t rc = msSinkWrite(src->sink, src->batch, nLines);

    msMutexLock(&src->lock);
    ++src->stats.flushes;
    if (rc == 0) {
      src->stats.written += (uint64_t)nLines;
      for (int32_t i = 0; i < nMsgs; ++i) {
        pushAckLocked(src, src->held[i].packetId, src->held[i].qos);
      }
    } else {
      src->stats.failed += (uint64_t)nLines;
      ++src->failedBatches;
      MS_ATOMIC_STORE(&src->writeFailed, 1);
      msLog(MS_LOG_ERROR, "batch of %d row(s) dropped after retries, broker will redeliver",
            nLines);
    }
    for (int32_t i = 0; i < nMsgs; ++i) freeMsg(&src->held[i]);
    src->heldNum = 0;
    src->batchNum = 0;
    src->batchStartMs = 0;
    if (rc != 0 && stopping) {
      /* Shutting down against an unavailable server: do not spend the whole
       * supervisor timeout replaying a queue that cannot be written. */
      for (int32_t i = 0; i < src->qCount; ++i) {
        freeMsg(&src->q[(src->qHead + i) % src->qCap]);
      }
      src->qCount = 0;
      msCondSignal(&src->hasSpace);
    }
    msMutexUnlock(&src->lock);
  }
  return NULL;
}

static void onTick(void *arg) {
  SMsSrc *src = (SMsSrc *)arg;
  int32_t interval = src->cfg.statsIntervalMs;
  if (interval <= 0) return;
  int64_t now = msNowMs();
  if (now - src->lastStatsMs < interval) return;
  src->lastStatsMs = now;

  msMutexLock(&src->lock);
  SMsStats s = src->stats;
  int32_t depth = src->qCount;
  msMutexUnlock(&src->lock);

  msLog(MS_LOG_INFO,
        "stats: received=%llu parsed=%llu lines=%llu written=%llu failed=%llu "
        "parseErrors=%llu reconnects=%llu queue=%d/%d flushes=%llu",
        (unsigned long long)s.received, (unsigned long long)s.parsed,
        (unsigned long long)s.lines, (unsigned long long)s.written,
        (unsigned long long)s.failed, (unsigned long long)s.parseErrors,
        (unsigned long long)s.reconnects, depth, src->qCap,
        (unsigned long long)s.flushes);
  /* A monotonically growing parse-error count is the only signal an operator
   * gets when a producer changes its payload schema. */
  if (s.parseErrors > src->lastParseErrors) {
    msLog(MS_LOG_WARN, "%llu message(s) could not be mapped since the last report",
          (unsigned long long)(s.parseErrors - src->lastParseErrors));
    src->lastParseErrors = s.parseErrors;
  }
}

static void destroySrc(SMsSrc *src) {
  if (src == NULL) return;
  for (int32_t i = 0; i < src->qCount; ++i) freeMsg(&src->q[(src->qHead + i) % src->qCap]);
  for (int32_t i = 0; i < src->heldNum; ++i) freeMsg(&src->held[i]);
  free(src->q);
  free(src->held);
  free(src->acks);
  free(src->batch);
  if (src->sink != NULL) msSinkClose(src->sink);
  if (src->locksInited) {
    msCondDestroy(&src->hasSpace);
    msCondDestroy(&src->hasData);
    msMutexDestroy(&src->lock);
  }
  free(src);
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
  int32_t qCap = cfg->sink.queueSize > 0 ? cfg->sink.queueSize : 1;
  src->q = (SMsMsg *)calloc((size_t)qCap, sizeof(SMsMsg));
  src->held = (SMsMsg *)calloc((size_t)qCap, sizeof(SMsMsg));
  src->acks = (SMsAck *)calloc((size_t)qCap, sizeof(SMsAck));
  if (src->q == NULL || src->held == NULL || src->acks == NULL) {
    destroySrc(src);
    return -1;
  }
  src->qCap = qCap;
  src->ackCap = qCap;
  msMutexInit(&src->lock);
  msCondInit(&src->hasData);
  msCondInit(&src->hasSpace);
  src->locksInited = 1;
  src->lastStatsMs = msNowMs();
  /* Publish the handle before blocking so a stop request can reach it. */
  MS_PTR_STORE(ppSrc, src);

  if (msSinkOpen(&cfg->sink, cfg->mapping.tbnameKey, &src->sink) != 0) {
    msLog(MS_LOG_ERROR, "cannot open TDengine sink");
    MS_PTR_STORE(ppSrc, NULL);
    destroySrc(src);
    return -1;
  }

  if (msThreadStart(&src->writer, writerMain, src) != 0) {
    msLog(MS_LOG_ERROR, "cannot start the writer thread");
    MS_PTR_STORE(ppSrc, NULL);
    destroySrc(src);
    return -1;
  }
  src->writerStarted = 1;

  SMsMqttOpts opts;
  memset(&opts, 0, sizeof(opts));
  opts.cfg = &src->cfg.broker;
  opts.stop = &src->stop;
  opts.onMessage = onMessage;
  opts.onTick = onTick;
  opts.pollAcks = pollAcks;
  opts.wasPersisted = wasPersisted;
  opts.forgetPersisted = forgetPersisted;
  opts.arg = src;
  opts.reconnects = &src->stats.reconnects;

  int32_t rc = msMqttLoop(&opts);

  /* Let the writer drain what is already queued, then stop it. */
  MS_ATOMIC_STORE(&src->stop, 1);
  msMutexLock(&src->lock);
  msCondBroadcast(&src->hasData);
  msCondBroadcast(&src->hasSpace);
  msMutexUnlock(&src->lock);
  msThreadJoin(src->writer);
  src->writerStarted = 0;

  msSinkClose(src->sink);
  src->sink = NULL;

  SMsStats s;
  msMutexLock(&src->lock);
  s = src->stats;
  int32_t failedBatches = src->failedBatches;
  msMutexUnlock(&src->lock);

  msLog(MS_LOG_INFO,
        "stopped: received=%llu parsed=%llu written=%llu failed=%llu "
        "parseErrors=%llu reconnects=%llu flushes=%llu maxQueue=%llu",
        (unsigned long long)s.received, (unsigned long long)s.parsed,
        (unsigned long long)s.written, (unsigned long long)s.failed,
        (unsigned long long)s.parseErrors, (unsigned long long)s.reconnects,
        (unsigned long long)s.flushes, (unsigned long long)s.maxQueueDepth);
  /* A non-zero exit code lets systemd / a supervisor notice lost rows. */
  if (rc != 0 || failedBatches != 0) {
    msLog(MS_LOG_ERROR, "stopped with %d failed batch(es); some data was not written",
          failedBatches);
    return -1;
  }
  return 0;
}

void msSrcStop(SMsSrc *src) {
  if (src != NULL) MS_ATOMIC_STORE(&src->stop, 1);
}

void msSrcStats(SMsSrc *src, SMsStats *stats) {
  if (src == NULL || stats == NULL) return;
  msMutexLock(&src->lock);
  *stats = src->stats;
  msMutexUnlock(&src->lock);
}

void msSrcFree(SMsSrc *src) { destroySrc(src); }

const char *msVersion(void) { return "1.1.0"; }
