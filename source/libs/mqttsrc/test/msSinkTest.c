/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Exercises the real msSinkWrite() path with injectable taos stubs. The
 * dry-run e2e test never reaches this code, so retry/backoff and reqid
 * de-duplication would otherwise have zero coverage.
 */

#include <stdio.h>
#include <string.h>

#include "msInt.h"

void    msStubReset(int32_t failFirstN);
int32_t msStubInsertCalls(void);
int64_t msStubLastReqid(void);

static int gPass = 0;
static int gFail = 0;

#define CHECK(cond, msg)                                     \
  do {                                                      \
    if (cond) {                                             \
      ++gPass;                                              \
    } else {                                                \
      ++gFail;                                              \
      fprintf(stderr, "FAIL line %d: %s\n", __LINE__, msg); \
    }                                                       \
  } while (0)

int main(void) {
  SMsSinkCfg cfg;
  memset(&cfg, 0, sizeof(cfg));
  msStrCopy(cfg.host, sizeof(cfg.host), "127.0.0.1");
  msStrCopy(cfg.db, sizeof(cfg.db), "stub");
  cfg.port = 6030;
  cfg.createDb = false;
  cfg.maxRetries = 2;
  cfg.retryMs = 0; /* keep the test fast */
  cfg.dryRun = false;

  SMsSink *sink = NULL;
  msStubReset(0);
  CHECK(msSinkOpen(&cfg, "tbname", &sink) == 0 && sink != NULL,
        "sink opens against the stub driver");

  char *lines[1] = {"meters,tbname=t1 v=1 1730000000000"};

  /* Happy path: exactly one insert, reqid starts at 1. */
  CHECK(msSinkWrite(sink, lines, 1) == 0, "insert succeeds");
  CHECK(msStubInsertCalls() == 1, "one insert call");
  CHECK(msStubLastReqid() == 1, "first reqid is 1");

  /* Two transient failures then success. The whole retry cycle MUST reuse a
   * single reqid, otherwise the server-side de-dup can double-insert. */
  msStubReset(2);
  CHECK(msSinkWrite(sink, lines, 1) == 0, "retry eventually succeeds");
  CHECK(msStubInsertCalls() == 3, "three attempts (1 + 2 retries)");
  CHECK(msStubLastReqid() == 2, "one new reqid for the whole retry cycle");

  /* Permanent failure: the batch is reported as failed and the id is still
   * consumed so the next batch is not treated as a duplicate by the server. */
  msStubReset(99);
  CHECK(msSinkWrite(sink, lines, 1) == -1, "permanent failure is reported");
  CHECK(msStubInsertCalls() == 3, "retry budget exhausted");
  CHECK(msStubLastReqid() == 3, "failed batch consumes its reqid");

  msSinkClose(sink);
  fprintf(stdout, "msSinkTest: %d passed, %d failed\n", gPass, gFail);
  return gFail == 0 ? 0 : 1;
}
