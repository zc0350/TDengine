/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * TDengine sink: batch schemaless writes over the public C API.
 */

#include "msInt.h"
#include "taos.h"

struct SMsSink {
  SMsSinkCfg cfg;
  char       tbnameKey[MS_NAME_LEN];
  TAOS      *taos;
  int64_t    reqid;
};

int32_t msSinkOpen(const SMsSinkCfg *cfg, const char *tbnameKey, SMsSink **pp) {
  if (cfg == NULL || pp == NULL) return -1;
  *pp = NULL;

  SMsSink *s = (SMsSink *)calloc(1, sizeof(SMsSink));
  if (s == NULL) return -1;
  s->cfg = *cfg;
  snprintf(s->tbnameKey, sizeof(s->tbnameKey), "%s",
           (tbnameKey != NULL && tbnameKey[0] != '\0') ? tbnameKey : "tbname");
  s->reqid = 1;

  if (s->cfg.dryRun) {
    msLog(MS_LOG_INFO, "sink dry-run: line protocol will be printed, no TDengine connection");
    *pp = s;
    return 0;
  }

  s->taos = taos_connect(s->cfg.host, s->cfg.user, s->cfg.password, NULL, s->cfg.port);
  if (s->taos == NULL) {
    msLog(MS_LOG_ERROR, "taos_connect(%s:%u) failed", s->cfg.host, (unsigned)s->cfg.port);
    free(s);
    return -1;
  }

  if (s->cfg.createDb) {
    char sql[MS_NAME_LEN + 64];
    snprintf(sql, sizeof(sql), "CREATE DATABASE IF NOT EXISTS %s", s->cfg.db);
    TAOS_RES *res = taos_query(s->taos, sql);
    if (res == NULL || taos_errno(res) != 0) {
      msLog(MS_LOG_ERROR, "create database failed: %s",
            res != NULL ? taos_errstr(res) : "null result");
      if (res != NULL) taos_free_result(res);
      taos_close(s->taos);
      free(s);
      return -1;
    }
    taos_free_result(res);
  }

  if (taos_select_db(s->taos, s->cfg.db) != 0) {
    msLog(MS_LOG_ERROR, "taos_select_db(%s) failed", s->cfg.db);
    taos_close(s->taos);
    free(s);
    return -1;
  }

  msLog(MS_LOG_INFO, "sink connected to %s:%u, database=%s", s->cfg.host,
        (unsigned)s->cfg.port, s->cfg.db);
  *pp = s;
  return 0;
}

int32_t msSinkWrite(SMsSink *s, char **lines, int32_t num) {
  if (s == NULL || lines == NULL || num <= 0) return -1;

  if (s->cfg.dryRun) {
    for (int32_t i = 0; i < num; ++i) {
      fprintf(stdout, "%s\n", lines[i]);
    }
    fflush(stdout);
    return 0;
  }

  /* One reqid per batch, reused across all retries: the *_with_reqid* API
   * de-duplicates by this id, so changing it per attempt can double-insert
   * when the server committed but the client saw an error. */
  int64_t reqid = s->reqid++;
  if (s->reqid <= 0) s->reqid = 1;

  for (int32_t attempt = 0; attempt <= s->cfg.maxRetries; ++attempt) {
    if (attempt > 0) {
      msLog(MS_LOG_WARN, "retrying batch (%d/%d), rows=%d", attempt,
            s->cfg.maxRetries, num);
      msSleepMs(s->cfg.retryMs * (attempt > 3 ? 3 : attempt));
    }

    TAOS_RES *res = taos_schemaless_insert_ttl_with_reqid_tbname_key(
        s->taos, lines, num, TSDB_SML_LINE_PROTOCOL,
        TSDB_SML_TIMESTAMP_MILLI_SECONDS, s->cfg.ttl, reqid, s->tbnameKey);

    if (res != NULL) {
      int code = taos_errno(res);
      if (code == 0) {
        taos_free_result(res);
        return 0;
      }
      msLog(MS_LOG_ERROR, "schemaless insert failed, code=%d, msg=%s", code,
            taos_errstr(res));
      taos_free_result(res);
    } else {
      msLog(MS_LOG_ERROR, "schemaless insert returned NULL");
    }
  }
  return -1;
}

void msSinkClose(SMsSink *s) {
  if (s == NULL) return;
  if (s->taos != NULL) taos_close(s->taos);
  free(s);
}
