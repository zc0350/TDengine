/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Link-time stubs for the taos C API. Used only by the dry-run test binary
 * so that protocol / mapping tests can run without a TDengine server or
 * libtaos. The real taosmqttsrc executable links ${TAOS_LIB} instead.
 *
 * These functions are never called in dry-run mode.
 */

#include "taos.h"

TAOS *taos_connect(const char *ip, const char *user, const char *pass,
                   const char *db, uint16_t port) {
  (void)ip;
  (void)user;
  (void)pass;
  (void)db;
  (void)port;
  return NULL;
}

TAOS_RES *taos_query(TAOS *taos, const char *sql) {
  (void)taos;
  (void)sql;
  return NULL;
}

void taos_free_result(TAOS_RES *res) { (void)res; }
void taos_close(TAOS *taos) { (void)taos; }
int taos_errno(TAOS_RES *res) {
  (void)res;
  return -1;
}
const char *taos_errstr(TAOS_RES *res) {
  (void)res;
  return "taos stub";
}
int taos_select_db(TAOS *taos, const char *db) {
  (void)taos;
  (void)db;
  return -1;
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
  (void)reqid;
  (void)tbnameKey;
  return NULL;
}
