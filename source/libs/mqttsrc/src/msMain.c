/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * taosmqttsrc - standalone MQTT -> TDengine ingestion daemon.
 */

#include "msInt.h"

#ifdef _WIN32
#include <windows.h>
#else
#include <signal.h>
#endif

static SMsSrc *gSrc = NULL;

#ifdef _WIN32
static BOOL WINAPI onConsoleCtrl(DWORD type) {
  if (type == CTRL_C_EVENT || type == CTRL_BREAK_EVENT || type == CTRL_CLOSE_EVENT) {
    msSrcStop(gSrc);
    return TRUE;
  }
  return FALSE;
}
#else
static void onSignal(int sig) {
  (void)sig;
  msSrcStop(gSrc);
}
#endif

static void usage(const char *exe) {
  fprintf(stderr,
          "Usage: %s -c <config.json> [options]\n"
          "  -c, --config <path>   JSON config file (required)\n"
          "  -n, --dry-run         print line protocol instead of writing to TDengine\n"
          "      --check           validate config and exit\n"
          "  -v, --verbose         debug logging\n"
          "  -h, --help            show this help\n"
          "      --version         show version\n",
          exe);
}

int main(int argc, char **argv) {
  const char *configPath = NULL;
  bool dryRun = false;
  bool checkOnly = false;
  bool verbose = false;

  for (int i = 1; i < argc; ++i) {
    const char *a = argv[i];
    if ((strcmp(a, "-c") == 0 || strcmp(a, "--config") == 0) && i + 1 < argc) {
      configPath = argv[++i];
    } else if (strcmp(a, "-n") == 0 || strcmp(a, "--dry-run") == 0) {
      dryRun = true;
    } else if (strcmp(a, "--check") == 0) {
      checkOnly = true;
    } else if (strcmp(a, "-v") == 0 || strcmp(a, "--verbose") == 0) {
      verbose = true;
    } else if (strcmp(a, "-h") == 0 || strcmp(a, "--help") == 0) {
      usage(argv[0]);
      return 0;
    } else if (strcmp(a, "--version") == 0) {
      fprintf(stdout, "taosmqttsrc %s\n", msVersion());
      return 0;
    } else {
      fprintf(stderr, "unknown option: %s\n", a);
      usage(argv[0]);
      return 1;
    }
  }

  if (configPath == NULL) {
    usage(argv[0]);
    return 1;
  }
  msSetLogLevel(verbose ? MS_LOG_DEBUG : MS_LOG_INFO);

  SMsCfg cfg;
  char err[MS_ERR_LEN];
  if (msLoadConfig(configPath, &cfg, err, sizeof(err)) != 0) {
    fprintf(stderr, "config error: %s\n", err);
    return 1;
  }
  if (dryRun) cfg.sink.dryRun = true;
  if (checkOnly) {
    fprintf(stdout, "config OK: %d topic(s), %d rule(s), %d default column(s), stable=%s\n",
            cfg.broker.topicNum, cfg.mapping.ruleNum, cfg.mapping.columnNum,
            cfg.mapping.stable);
    return 0;
  }

#ifdef _WIN32
  SetConsoleCtrlHandler(onConsoleCtrl, TRUE);
#else
  signal(SIGINT, onSignal);
  signal(SIGTERM, onSignal);
  signal(SIGPIPE, SIG_IGN);
#endif

  int32_t rc = msSrcRun(&cfg, &gSrc);
  SMsStats stats;
  if (gSrc != NULL) {
    msSrcStats(gSrc, &stats);
    msSrcFree(gSrc);
    gSrc = NULL;
  }
  return rc == 0 ? 0 : 1;
}
