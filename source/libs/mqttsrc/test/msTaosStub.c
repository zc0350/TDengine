/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Controllable link-time stubs for the taos C API.
 *
 * taosmqttsrcDryRun links these so the protocol e2e test needs no TDengine
 * server. msSinkTest uses the injection hooks below to exercise the real
 * msSinkWrite() path: retry/backoff, reqid reuse and failure accounting.
 */

#include "taos.h"

static int32_t gFailFirstN = 0;
static int32_t gInsertCalls = 0;
static int32_t gLastCode = 0;
static int64_t gLastReqid = 0;

/* Any non-NULL pointer works: the stub never dereferences a TAOS/TAOS_RES. */
#define MS_FAKE_HANDLE ((void *)0x1)

void msStubReset(int32_t failFirstN) {
  gFailFirstN = failFirstN;
  gInsertCalls = 0;
  gLastCode = 0;
  gLastReqid = 0;
}

int32_t msStubInsertCalls(void) { return gInsertCalls; }
int64_t msStubLastReqid(void) { return gLastReqid; }

TAOS *taos_connect(const char *ip, const char *user, const char *pass,
                   const char *db, uint16_t port) {
  (void)ip;
  (void)user;
  (void)pass;
  (void)db;
  (void)port;
  return (TAOS *)MS_FAKE_HANDLE;
}

TAOS_RES *taos_query(TAOS *taos, const char *sql) {
  (void)taos;
  (void)sql;
  return (TAOS_RES *)MS_FAKE_HANDLE;
}

void taos_free_result(TAOS_RES *res) { (void)res; }
void taos_close(TAOS *taos) { (void)taos; }

int taos_errno(TAOS_RES *res) {
  (void)res;
  return gLastCode;
}

const char *taos_errstr(TAOS_RES *res) {
  (void)res;
  return "taos stub error";
}

int taos_select_db(TAOS *taos, const char *db) {
  (void)taos;
  (void)db;
  return 0;
}

TAOS_RES *taos_schemaless_insert_ttl_with_reqid_tbname_key(
    TAOS *taos, char *lines[], int numLines, int protocol, int precision,
    int32_t ttl, int64_t reqid, char *tbnameKey) {
  (void)taos;
  (void)lines;
  (void)numLines;
  (void)protocol;
  (void)precision;
  (void)ttl;
  (void)tbnameKey;
  ++gInsertCalls;
  gLastReqid = reqid;
  gLastCode = (gInsertCalls <= gFailFirstN) ? -1 : 0;
  return (TAOS_RES *)MS_FAKE_HANDLE;
}
