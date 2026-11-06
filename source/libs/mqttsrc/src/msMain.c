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

/* Published by msSrcRun() before it blocks. Readers are the signal handler and
 * (on Windows) the console-control thread; both are unregistered before the
 * handle is released. Access goes through MS_PTR_LOAD/MS_PTR_STORE so the
 * pointer is neither optimized away nor qualified in a way that MSVC rejects
 * when passing &gSrc to msSrcRun(). */
static SMsSrc *gSrc = NULL;

static void requestStop(void) {
  SMsSrc *src = (SMsSrc *)MS_PTR_LOAD(&gSrc);
  if (src != NULL) msSrcStop(src);
}

#ifdef _WIN32
static BOOL WINAPI onConsoleCtrl(DWORD type) {
  if (type == CTRL_C_EVENT || type == CTRL_BREAK_EVENT || type == CTRL_CLOSE_EVENT) {
    requestStop();
    return TRUE;
  }
  return FALSE;
}
#else
static void onSignal(int sig) {
  (void)sig;
  requestStop();
}

static void uninstallSignals(void) {
  signal(SIGINT, SIG_DFL);
  signal(SIGTERM, SIG_DFL);
}
#endif

typedef struct {
  int32_t runMs;
} SMsRunTimer;

static volatile int32_t gTimerCancel = 0;

static void *timerMain(void *arg) {
  const SMsRunTimer *t = (const SMsRunTimer *)arg;
  /* Wait until msSrcRun() published the handle, then count down. Both loops
   * poll so a signal-triggered shutdown never waits for the full duration. */
  for (int32_t waited = 0; waited < 5000; waited += 50) {
    if (MS_PTR_LOAD(&gSrc) != NULL) break;
    msSleepMs(50);
  }
  int32_t left = t->runMs;
  while (left > 0 && !MS_STOPPED(&gTimerCancel) && MS_PTR_LOAD(&gSrc) != NULL) {
    msSleepMs(50);
    left -= 50;
  }
  if (!MS_STOPPED(&gTimerCancel) && MS_PTR_LOAD(&gSrc) != NULL) requestStop();
  return NULL;
}

static void usage(const char *exe) {
  fprintf(stderr,
          "Usage: %s -c <config.json> [options]\n"
          "  -c, --config <path>   JSON config file (required)\n"
          "  -n, --dry-run         print line protocol instead of writing to TDengine\n"
          "      --check           validate config and exit\n"
          "      --exit-after-ms <n>  stop gracefully after n milliseconds\n"
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
  int32_t exitAfterMs = 0;

  for (int i = 1; i < argc; ++i) {
    const char *a = argv[i];
    if ((strcmp(a, "-c") == 0 || strcmp(a, "--config") == 0) && i + 1 < argc) {
      configPath = argv[++i];
    } else if (strcmp(a, "-n") == 0 || strcmp(a, "--dry-run") == 0) {
      dryRun = true;
    } else if (strcmp(a, "--check") == 0) {
      checkOnly = true;
    } else if (strcmp(a, "--exit-after-ms") == 0 && i + 1 < argc) {
      exitAfterMs = msStrToInt(argv[++i], 0);
      if (exitAfterMs <= 0) {
        fprintf(stderr, "--exit-after-ms needs a positive number\n");
        return 1;
      }
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

  SMsRunTimer timer;
  timer.runMs = exitAfterMs;
  MsThread timerThread;
  memset(&timerThread, 0, sizeof(timerThread));
  int32_t timerStarted = 0;
  if (exitAfterMs > 0) {
    if (msThreadStart(&timerThread, timerMain, &timer) == 0) timerStarted = 1;
  }

  int32_t rc = msSrcRun(&cfg, &gSrc);

  if (timerStarted) {
    MS_ATOMIC_STORE(&gTimerCancel, 1);
    msThreadJoin(timerThread);
  }

  /* Unregister first: a signal/console event arriving between free() and the
   * NULL assignment used to dereference a freed handle (use-after-free). */
#ifdef _WIN32
  SetConsoleCtrlHandler(onConsoleCtrl, FALSE);
#else
  uninstallSignals();
#endif
  SMsSrc *s = (SMsSrc *)MS_PTR_LOAD(&gSrc);
  MS_PTR_STORE(&gSrc, NULL);
  msSrcFree(s);
  return rc == 0 ? 0 : 1;
}
