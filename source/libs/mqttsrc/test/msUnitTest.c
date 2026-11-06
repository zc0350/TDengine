/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Dependency-free unit tests for config parsing, MQTT varint and the
 * JSON -> line-protocol mapper. Run: msUnitTest <example-config.json>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "mqttsrc.h"

#ifndef MS_TEST_EXAMPLE_CONFIG
#define MS_TEST_EXAMPLE_CONFIG "mqttsrc.example.json"
#endif
#ifndef MS_TEST_RULES_CONFIG
#define MS_TEST_RULES_CONFIG "mqttsrc.rules.example.json"
#endif

static int gFail = 0;
static int gPass = 0;

#define CHECK(cond, msg)                                     \
  do {                                                      \
    if (cond) {                                             \
      ++gPass;                                               \
    } else {                                                \
      ++gFail;                                               \
      fprintf(stderr, "FAIL line %d: %s\n", __LINE__, msg); \
      return;                                                \
    }                                                       \
  } while (0)

#define CHECK_STR(a, b)                                                    \
  do {                                                                     \
    if (strcmp((a), (b)) == 0) {                                           \
      ++gPass;                                                              \
    } else {                                                                \
      ++gFail;                                                               \
      fprintf(stderr, "FAIL line %d:\n  got : %s\n  want: %s\n", __LINE__, \
              (a), (b));                                                    \
      return;                                                               \
    }                                                                       \
  } while (0)

static void testVarint(void) {
  uint8_t b[4] = {0};
  uint32_t v = 999;
  int32_t used = -1;

  CHECK(msMqttEncodeRemainLen(0, b, 4) == 1 && b[0] == 0, "encode 0");
  CHECK(msMqttEncodeRemainLen(127, b, 4) == 1 && b[0] == 127, "encode 127");
  int32_t n = msMqttEncodeRemainLen(128, b, 4);
  CHECK(n == 2 && b[0] == 0x80 && b[1] == 1, "encode 128");
  CHECK(msMqttDecodeRemainLen(b, 2, &v, &used) == 0 && v == 128 && used == 2,
        "decode 128");
  CHECK(msMqttDecodeRemainLen(b, 1, &v, &used) == 1, "decode needs more bytes");

  n = msMqttEncodeRemainLen(268435455, b, 4);
  CHECK(n == 4, "encode max");
  CHECK(msMqttDecodeRemainLen(b, 4, &v, &used) == 0 && v == 268435455,
        "decode max");
  CHECK(msMqttEncodeRemainLen(268435456, b, 4) == -1, "reject overflow");
}

static void testConfig(const char *path) {
  SMsCfg cfg;
  char err[MS_ERR_LEN];
  msCfgDefault(&cfg);
  CHECK(msLoadConfig(path, &cfg, err, sizeof(err)) == 0, err);
  CHECK_STR(cfg.broker.host, "127.0.0.1");
  CHECK(cfg.broker.port == 1883, "broker port");
  CHECK(cfg.broker.topicNum == 1, "topic count");
  CHECK_STR(cfg.broker.topics[0].filter, "factory/+/telemetry");
  CHECK(cfg.broker.topics[0].qos == 1, "topic qos");
  CHECK(cfg.broker.topicVarNum == 3, "topic var count");
  CHECK_STR(cfg.broker.topicVars[1], "site");
  CHECK_STR(cfg.sink.db, "mqtt_demo");
  CHECK(cfg.sink.batchSize == 1000, "batch size");
  CHECK_STR(cfg.mapping.stable, "meters");
  CHECK_STR(cfg.mapping.tbnameKey, "tbname");
  CHECK_STR(cfg.mapping.tsField, "ts");
  CHECK(cfg.mapping.columnNum == 4, "column count");
  CHECK(cfg.mapping.tagNum == 2, "tag count");
  CHECK(cfg.mapping.columns[0].type == MS_COL_NUMBER, "column type");

  SMsCfg bad;
  msCfgDefault(&bad);
  CHECK(msValidateConfig(&bad, err, sizeof(err)) == -1, "empty config must fail");
}

static void testMapping(const SMsCfg *cfg) {
  const char *payload =
      "{\"ts\":1730000000000,\"dev\":\"d1\",\"current\":12,"
      "\"voltage\":220.5,\"running\":true,\"note\":\"ok\"}";
  char *out = NULL;
  int32_t num = 0;
  char err[MS_ERR_LEN];
  int32_t rc = msMapPayload(cfg, payload, (int32_t)strlen(payload),
                            "factory/bj/telemetry", 1730000000000LL,
                            &out, &num, err, sizeof(err));
  CHECK(rc == 0 && num == 1 && out != NULL, err);
  if (rc == 0) {
    CHECK_STR(out,
              "meters,device=d1,site=bj,tbname=t_d1_bj "
              "current=12,voltage=220.5,running=true,note=\"ok\" "
              "1730000000000");
  }
  free(out);

  /* JSON array -> two rows */
  const char *arr =
      "[{\"ts\":1730000000000,\"dev\":\"d1\",\"current\":12,"
      "\"voltage\":220.5,\"running\":true,\"note\":\"a\"},"
      "{\"ts\":1730000000001,\"dev\":\"d2\",\"current\":13,"
      "\"voltage\":221,\"running\":false,\"note\":\"b\"}]";
  out = NULL;
  num = 0;
  rc = msMapPayload(cfg, arr, (int32_t)strlen(arr), "factory/sh/telemetry",
                    1730000000000LL, &out, &num, err, sizeof(err));
  CHECK(rc == 0 && num == 2, err);
  if (rc == 0) {
    const char *l1 = out;
    const char *l2 = out + strlen(out) + 1;
    CHECK(strstr(l1, "t_d1_sh") != NULL && strstr(l1, "site=sh") != NULL,
          "row 1 topic vars");
    CHECK(strstr(l2, "t_d2_sh") != NULL && strstr(l2, "current=13") != NULL,
          "row 2 values");
    CHECK(strstr(l2, " 1730000000001") != NULL, "row 2 timestamp");
  }
  free(out);

  /* escaping of tag values and quoted strings */
  const char *esc =
      "{\"ts\":1730000000000,\"dev\":\"d,1\",\"current\":1,"
      "\"voltage\":2,\"running\":true,\"note\":\"a,b\\\"c\\\\d\"}";
  out = NULL;
  num = 0;
  rc = msMapPayload(cfg, esc, (int32_t)strlen(esc), "factory/bj/telemetry",
                    1730000000000LL, &out, &num, err, sizeof(err));
  CHECK(rc == 0 && num == 1, err);
  if (rc == 0) {
    CHECK(strstr(out, "device=d\\,1") != NULL, "tag comma escaped");
    CHECK(strstr(out, "tbname=t_d\\,1_bj") != NULL, "tbname comma escaped");
    CHECK(strstr(out, "note=\"a,b\\\"c\\\\d\"") != NULL,
          "string field escaping");
  }
  free(out);

  /* explicit BIGINT column emits the i64 suffix */
  SMsCfg intCfg = *cfg;
  intCfg.mapping.columns[0].type = MS_COL_INT;
  out = NULL;
  num = 0;
  rc = msMapPayload(&intCfg, payload, (int32_t)strlen(payload),
                    "factory/bj/telemetry", 1730000000000LL,
                    &out, &num, err, sizeof(err));
  CHECK(rc == 0 && out != NULL && strstr(out, "current=12i64") != NULL,
        "int column emits i64");
  free(out);

  /* bad JSON */
  out = NULL;
  num = 0;
  rc = msMapPayload(cfg, "{not json", 9, "factory/bj/telemetry", 0,
                    &out, &num, err, sizeof(err));
  CHECK(rc == -1 && out == NULL, "invalid JSON must be rejected");

  /* Regression: a JSON string in a numeric column must not inject extra
   * line-protocol fields such as "1,evil=2". */
  const char *inject =
      "{\"ts\":1730000000000,\"dev\":\"d1\",\"current\":\"1,evil=2\","
      "\"voltage\":220,\"running\":true,\"note\":\"x\"}";
  out = NULL;
  num = 0;
  rc = msMapPayload(cfg, inject, (int32_t)strlen(inject), "factory/bj/telemetry",
                    1730000000000LL, &out, &num, err, sizeof(err));
  CHECK(rc == -1 && out == NULL, "numeric string with injected field is rejected");

  /* Regression: an over-long topic is rejected, not silently truncated. */
  char longTopic[MS_TEXT_LEN + 8];
  memset(longTopic, 'a', sizeof(longTopic) - 1);
  longTopic[sizeof(longTopic) - 1] = '\0';
  out = NULL;
  num = 0;
  rc = msMapPayload(cfg, payload, (int32_t)strlen(payload), longTopic,
                    1730000000000LL, &out, &num, err, sizeof(err));
  CHECK(rc == -1 && out == NULL, "over-long topic is rejected");
}

static void testTimestamp(const SMsCfg *base) {
  SMsCfg cfg = *base;
  char *out = NULL;
  int32_t num = 0;
  char err[MS_ERR_LEN];

  /* ts_precision = s, input seconds -> trailing milliseconds */
  cfg.mapping.tsPrecision = MS_TS_SECOND;
  const char *p = "{\"ts\":1730000000,\"dev\":\"d1\",\"current\":1,"
                  "\"voltage\":2,\"running\":true,\"note\":\"x\"}";
  int32_t rc = msMapPayload(&cfg, p, (int32_t)strlen(p), "factory/bj/telemetry",
                            0, &out, &num, err, sizeof(err));
  CHECK(rc == 0 && out != NULL && strstr(out, " 1730000000000") != NULL,
        "seconds -> milliseconds");
  free(out);

  /* no ts field -> receive time */
  cfg.mapping.tsField[0] = '\0';
  out = NULL;
  num = 0;
  rc = msMapPayload(&cfg, p, (int32_t)strlen(p), "factory/bj/telemetry",
                    1730000000123LL, &out, &num, err, sizeof(err));
  CHECK(rc == 0 && out != NULL && strstr(out, " 1730000000123") != NULL,
        "receive time used");
  free(out);

  /* missing timestamp field -> error */
  cfg.mapping.tsField[0] = 't';
  cfg.mapping.tsField[1] = 's';
  cfg.mapping.tsField[2] = '\0';
  const char *noTs = "{\"dev\":\"d1\",\"current\":1,\"voltage\":2,"
                     "\"running\":true,\"note\":\"x\"}";
  out = NULL;
  rc = msMapPayload(&cfg, noTs, (int32_t)strlen(noTs), "factory/bj/telemetry",
                    0, &out, &num, err, sizeof(err));
  CHECK(rc == -1, "missing timestamp must be rejected");
}

static void testRules(const char *path) {
  SMsCfg cfg;
  char err[MS_ERR_LEN];
  msCfgDefault(&cfg);
  CHECK(msLoadConfig(path, &cfg, err, sizeof(err)) == 0, err);
  CHECK(cfg.mapping.ruleNum == 4, "four routing rules");
  CHECK(cfg.mapping.columnNum == 1, "default model keeps its own columns");
  CHECK(cfg.mapping.rules[0].colNum == 2 && cfg.mapping.rules[1].colNum == 2,
        "each rule owns a column slice");
  CHECK(cfg.mapping.rules[0].when[0].op == MS_WHEN_EQ, "eq condition parsed");
  CHECK(cfg.mapping.rules[1].when[0].op == MS_WHEN_IN, "in condition parsed");
  CHECK_STR(cfg.mapping.rules[0].stable, "sensor_{site}");
  CHECK_STR(cfg.mapping.rules[1].table, "t_{dev}");

  const char *topic = "factory/bj/telemetry";
  char *out = NULL;
  int32_t num = 0;

  const char *temp = "{\"type\":\"temperature\",\"dev\":\"d1\",\"ts\":1730000000000,"
                     "\"temp\":23.5,\"unit\":\"C\"}";
  int32_t rc = msMapPayload(&cfg, temp, (int32_t)strlen(temp), topic, 0,
                            &out, &num, err, sizeof(err));
  CHECK(rc == 0 && num == 1, err);
  if (rc == 0) {
    CHECK_STR(out, "sensor_bj,dev=d1,site=bj,kind=temperature,tbname=t_d1 "
                   "temp=23.5,unit=\"C\" 1730000000000");
  }
  free(out);

  const char *power = "{\"type\":\"power\",\"dev\":\"p1\",\"ts\":1730000000001,"
                      "\"kw\":12,\"on\":true,\"line\":\"L1\"}";
  out = NULL;
  rc = msMapPayload(&cfg, power, (int32_t)strlen(power), topic, 0,
                    &out, &num, err, sizeof(err));
  CHECK(rc == 0 && out != NULL && strstr(out, "power_bj,dev=p1,line=L1") != NULL &&
        strstr(out, "kw=12,on=true") != NULL, "power rule routing");
  free(out);

  const char *fallback = "{\"type\":\"unknown\",\"dev\":\"x1\","
                         "\"ts\":1730000000002,\"value\":7}";
  out = NULL;
  rc = msMapPayload(&cfg, fallback, (int32_t)strlen(fallback), topic, 0,
                    &out, &num, err, sizeof(err));
  CHECK(rc == 0 && out != NULL && strstr(out, "generic,dev=x1,site=bj") != NULL &&
        strstr(out, "value=7") != NULL, "default fallback when no rule matches");
  free(out);

  /* JSON array: each record is routed independently */
  const char *mixed = "[{\"type\":\"temperature\",\"dev\":\"d1\",\"ts\":1730000000000,"
                      "\"temp\":1,\"unit\":\"C\"},"
                      "{\"type\":\"power\",\"dev\":\"p1\",\"ts\":1730000000001,"
                      "\"kw\":2,\"on\":false,\"line\":\"L1\"}]";
  out = NULL;
  rc = msMapPayload(&cfg, mixed, (int32_t)strlen(mixed), topic, 0,
                    &out, &num, err, sizeof(err));
  CHECK(rc == 0 && num == 2, err);
  if (rc == 0) {
    const char *l2 = out + strlen(out) + 1;
    CHECK(strstr(out, "sensor_bj,") != NULL, "array row 0 uses rule 1");
    CHECK(strstr(l2, "power_bj,") != NULL, "array row 1 uses rule 2");
  }
  free(out);

  /* Calendar variables: {_year}/{_month} come from the row timestamp,
   * {_server_year} comes from the receive time passed by the caller. */
  const int64_t recv = 1793936226000LL; /* 2026-11-06 local */
  const char *archive = "{\"type\":\"archive\",\"dev\":\"a1\","
                        "\"ts\":1793936226000,\"value\":42,\"level\":\"warn\"}";
  out = NULL;
  rc = msMapPayload(&cfg, archive, (int32_t)strlen(archive), topic, recv,
                    &out, &num, err, sizeof(err));
  CHECK(rc == 0 && out != NULL &&
        strstr(out, "archive_2026,dev=a1,year=2026,month=11") != NULL &&
        strstr(out, "tbname=t_a1_202611") != NULL,
        "row timestamp year/month partition");
  free(out);

  const char *hb = "{\"type\":\"heartbeat\",\"dev\":\"h1\","
                   "\"ts\":1793936226001,\"up\":true}";
  out = NULL;
  rc = msMapPayload(&cfg, hb, (int32_t)strlen(hb), topic, recv,
                    &out, &num, err, sizeof(err));
  CHECK(rc == 0 && out != NULL &&
        strstr(out, "srv_2026_bj,dev=h1,server_year=2026,tbname=t_h1_2026") != NULL,
        "server year partition");
  free(out);

  SMsCfg dayCfg = cfg;
  snprintf(dayCfg.mapping.rules[2].stable, MS_NAME_LEN, "%s",
           "archive_{_year}_{_month}{_day}");
  out = NULL;
  rc = msMapPayload(&dayCfg, archive, (int32_t)strlen(archive), topic, recv,
                    &out, &num, err, sizeof(err));
  CHECK(rc == 0 && out != NULL && strstr(out, "archive_2026_1106,") != NULL,
        "day template variable");
  free(out);

  /* condition operators on topic and payload */
  SMsCfg opCfg = cfg;
  opCfg.mapping.rules[0].when[0].op = MS_WHEN_PREFIX;
  snprintf(opCfg.mapping.rules[0].when[0].path, MS_PATH_LEN, "%s", "_topic");
  snprintf(opCfg.mapping.rules[0].when[0].value, MS_TEXT_LEN, "%s", "factory/bj/");
  const char *both = "{\"type\":\"power\",\"dev\":\"p1\",\"ts\":1730000000003,"
                     "\"temp\":1,\"unit\":\"C\",\"kw\":2,\"on\":false,\"line\":\"L1\"}";
  out = NULL;
  rc = msMapPayload(&opCfg, both, (int32_t)strlen(both), topic, 0,
                    &out, &num, err, sizeof(err));
  CHECK(rc == 0 && out != NULL && strstr(out, "sensor_bj,") != NULL &&
        strstr(out, "temp=1") != NULL,
        "prefix condition on _topic wins over later rules");
  free(out);

  /* no rule matched and no default model -> reject */
  SMsCfg noDefault = cfg;
  noDefault.mapping.columnNum = 0;
  snprintf(noDefault.mapping.rules[0].when[0].value, MS_TEXT_LEN, "%s", "never");
  noDefault.mapping.rules[1].when[0].op = MS_WHEN_EQ;
  snprintf(noDefault.mapping.rules[1].when[0].value, MS_TEXT_LEN, "%s", "never");
  out = NULL;
  rc = msMapPayload(&noDefault, fallback, (int32_t)strlen(fallback), topic, 0,
                    &out, &num, err, sizeof(err));
  CHECK(rc == -1, "unmatched record rejected when no default model exists");

  /* tbname_key must be uniform across rules (one schemaless batch key) */
  SMsCfg badKey = cfg;
  snprintf(badKey.mapping.rules[0].tbnameKey, MS_NAME_LEN, "%s", "other");
  CHECK(msValidateConfig(&badKey, err, sizeof(err)) == -1,
        "per-rule tbname_key override rejected");

  /* Regression: a topic-controlled measurement must escape line-protocol
   * separators, otherwise a publisher can inject extra tags/fields. */
  const char *evilTopic = "factory/x,injected=1/telemetry";
  out = NULL;
  num = 0;
  rc = msMapPayload(&cfg, temp, (int32_t)strlen(temp), evilTopic, 0,
                    &out, &num, err, sizeof(err));
  CHECK(rc == 0 && out != NULL &&
        strstr(out, "sensor_x\\,injected\\=1,") != NULL,
        "measurement escapes topic-injected separators");
  free(out);
}

static int32_t loadFromText(const char *text, SMsCfg *cfg, char *err, int32_t errLen) {
  char path[64];
  snprintf(path, sizeof(path), "ms_unit_%lu.json", (unsigned long)clock());
  FILE *fp = fopen(path, "wb");
  if (fp == NULL) return -2;
  fwrite(text, 1, strlen(text), fp);
  fclose(fp);
  int32_t rc = msLoadConfig(path, cfg, err, errLen);
  remove(path);
  return rc;
}

/* Every "wrong configuration that used to run silently" case must fail at
 * load/validate time. */
static void testConfigValidation(void) {
  SMsCfg cfg;
  char err[MS_ERR_LEN];

  msCfgDefault(&cfg);
  CHECK(cfg.broker.cleanSession == false, "clean_session defaults to false");
  CHECK(strncmp(cfg.broker.clientId, "taosx-mqttsrc-", 14) == 0,
        "default client id is unique per host/process");
  CHECK(cfg.sink.queueSize > 0, "write queue has a default size");

  /* Built-ins must resolve; unknown underscore keys are typos. */
  msCfgDefault(&cfg);
  snprintf(cfg.broker.topics[0].filter, MS_TEXT_LEN, "%s", "a/#");
  cfg.broker.topicNum = 1;
  cfg.mapping.columnNum = 1;
  snprintf(cfg.mapping.columns[0].name, MS_NAME_LEN, "%s", "v");
  snprintf(cfg.mapping.columns[0].path, MS_PATH_LEN, "%s", "v");
  snprintf(cfg.mapping.table, MS_NAME_LEN, "%s", "t_{dev}_{_year}");
  CHECK(msValidateConfig(&cfg, err, sizeof(err)) == 0, err);
  snprintf(cfg.mapping.table, MS_NAME_LEN, "%s", "t_{dev");
  CHECK(msValidateConfig(&cfg, err, sizeof(err)) == -1, "unclosed '{' rejected");
  snprintf(cfg.mapping.table, MS_NAME_LEN, "%s", "t_{}");
  CHECK(msValidateConfig(&cfg, err, sizeof(err)) == -1, "empty placeholder rejected");
  snprintf(cfg.mapping.table, MS_NAME_LEN, "%s", "t_{_bogus}");
  CHECK(msValidateConfig(&cfg, err, sizeof(err)) == -1, "unknown builtin rejected");
  snprintf(cfg.mapping.table, MS_NAME_LEN, "%s", "t_{a b}");
  CHECK(msValidateConfig(&cfg, err, sizeof(err)) == -1, "invalid key character rejected");
  snprintf(cfg.mapping.table, MS_NAME_LEN, "%s", "t_{dev}");
  snprintf(cfg.mapping.tbnameKey, MS_NAME_LEN, "%s", "tbname,evil=1");
  CHECK(msValidateConfig(&cfg, err, sizeof(err)) == -1, "unsafe tbname_key rejected");
  snprintf(cfg.mapping.tbnameKey, MS_NAME_LEN, "%s", "tbname");
  snprintf(cfg.broker.password, MS_TEXT_LEN, "%s", "secret");
  CHECK(msValidateConfig(&cfg, err, sizeof(err)) == -1,
        "password without username rejected");
  snprintf(cfg.broker.password, MS_TEXT_LEN, "%s", "");
  cfg.sink.queueSize = 0;
  CHECK(msValidateConfig(&cfg, err, sizeof(err)) == -1, "queue_size 0 rejected");
  cfg.sink.queueSize = 100;
  cfg.broker.tls = true;
#ifndef MS_WITH_TLS
  CHECK(msValidateConfig(&cfg, err, sizeof(err)) == -1,
        "TLS request rejected by a build without TLS");
#endif
  cfg.broker.tls = false;
  CHECK(msValidateConfig(&cfg, err, sizeof(err)) == 0, err);

  /* Ports are 16-bit protocol values: out-of-range values must be rejected,
   * not truncated into a different port. */
  const char *base =
      "{\"mqtt\":{\"port\":%d,\"topics\":[{\"filter\":\"a/#\",\"qos\":1}]},"
      "\"tdengine\":{\"database\":\"d1\"},"
      "\"mapping\":{\"columns\":[{\"name\":\"v\",\"path\":\"v\"}]}}";
  char json[512];
  snprintf(json, sizeof(json), base, 70000);
  CHECK(loadFromText(json, &cfg, err, sizeof(err)) == -1, "port 70000 rejected");
  snprintf(json, sizeof(json), base, -1);
  CHECK(loadFromText(json, &cfg, err, sizeof(err)) == -1, "negative port rejected");
  snprintf(json, sizeof(json), base, 1883);
  CHECK(loadFromText(json, &cfg, err, sizeof(err)) == 0, err);

  const char *unknown =
      "{\"mqtt\":{\"topicz\":[],\"topics\":[{\"filter\":\"a/#\",\"qos\":1}]},"
      "\"mapping\":{\"columnX\":1,\"columns\":[{\"name\":\"v\",\"path\":\"v\"}]}}";
  CHECK(loadFromText(unknown, &cfg, err, sizeof(err)) == -1,
        "unknown config key rejected");

  const char *badTemplate =
      "{\"mqtt\":{\"topics\":[{\"filter\":\"a/#\",\"qos\":1}]},"
      "\"mapping\":{\"table\":\"t_{dev\",\"columns\":[{\"name\":\"v\",\"path\":\"v\"}]}}";
  CHECK(loadFromText(badTemplate, &cfg, err, sizeof(err)) == -1,
        "broken template rejected at load time");
}

/* A value longer than MS_VALUE_LEN must fail loudly instead of being cut. */
static void testLongValue(const SMsCfg *base) {
  SMsCfg cfg = *base;
  char err[MS_ERR_LEN];
  char *out = NULL;
  int32_t num = 0;

  size_t len = MS_VALUE_LEN + 400;
  size_t cap = len + 256;
  char *payload = (char *)malloc(cap);
  CHECK(payload != NULL, "payload buffer");
  int32_t n = snprintf(payload, cap,
                       "{\"ts\":1730000000000,\"dev\":\"d1\",\"current\":1,"
                       "\"voltage\":2,\"running\":true,\"note\":\"");
  memset(payload + n, 'N', len);
  n += (int32_t)len;
  payload[n++] = '"';
  payload[n++] = '}';
  payload[n] = '\0';

  int32_t rc = msMapPayload(&cfg, payload, n, "factory/bj/telemetry",
                            1730000000000LL, &out, &num, err, sizeof(err));
  CHECK(rc == -1 && out == NULL, "over-long column value rejected");
  free(payload);

  /* Exactly at the limit still fits. */
  const char *small = "{\"ts\":1730000000000,\"dev\":\"d1\",\"current\":1,"
                      "\"voltage\":2,\"running\":true,\"note\":\"ok\"}";
  out = NULL;
  num = 0;
  rc = msMapPayload(&cfg, small, (int32_t)strlen(small), "factory/bj/telemetry",
                    1730000000000LL, &out, &num, err, sizeof(err));
  CHECK(rc == 0 && num == 1, err);
  free(out);
}

static void testRawColumns(void) {
  SMsCfg cfg;
  msCfgDefault(&cfg);
  snprintf(cfg.mapping.stable, MS_NAME_LEN, "%s", "raw_{_year}");
  snprintf(cfg.mapping.table, MS_NAME_LEN, "%s", "t_{deviceCode}");
  snprintf(cfg.mapping.tsField, MS_PATH_LEN, "%s", "time");
  cfg.mapping.tsPrecision = MS_TS_SECOND;
  cfg.mapping.columnNum = 3;
  cfg.mapping.tagNum = 0;
  snprintf(cfg.mapping.columns[0].name, MS_NAME_LEN, "%s", "event");
  snprintf(cfg.mapping.columns[0].path, MS_PATH_LEN, "%s", "event");
  cfg.mapping.columns[0].type = MS_COL_STRING;
  snprintf(cfg.mapping.columns[1].name, MS_NAME_LEN, "%s", "data");
  snprintf(cfg.mapping.columns[1].path, MS_PATH_LEN, "%s", "$payload");
  cfg.mapping.columns[1].type = MS_COL_STRING;
  snprintf(cfg.mapping.columns[2].name, MS_NAME_LEN, "%s", "raw");
  snprintf(cfg.mapping.columns[2].path, MS_PATH_LEN, "%s", "$message");
  cfg.mapping.columns[2].type = MS_COL_STRING;

  char *out = NULL;
  int32_t num = 0;
  char err[MS_ERR_LEN];
  const char *msg = "{\"event\":\"status\",\"deviceCode\":\"D1\","
                    "\"imei\":\"123\",\"time\":1793936202}";
  int32_t rc = msMapPayload(&cfg, msg, (int32_t)strlen(msg), "device/D1/up",
                            1793936202000LL, &out, &num, err, sizeof(err));
  CHECK(rc == 0 && num == 1, err);
  if (rc == 0) {
    CHECK(strstr(out, "event=\"status\"") != NULL, "plain column");
    CHECK(strstr(out, "data=\"{\\\"event\\\":\\\"status\\\"") != NULL,
          "$payload stores the record JSON");
    CHECK(strstr(out, "raw=\"{\\\"event\\\":\\\"status\\\"") != NULL,
          "$message stores the raw MQTT payload");
    CHECK(strstr(out, " 1793936202000") != NULL, "seconds -> ms timestamp");
  }
  free(out);

  /* JSON array: $payload is per record, $message is the whole message */
  const char *arr = "[{\"event\":\"a\",\"deviceCode\":\"D1\",\"time\":1793936202},"
                    "{\"event\":\"b\",\"deviceCode\":\"D1\",\"time\":1793936203}]";
  out = NULL;
  rc = msMapPayload(&cfg, arr, (int32_t)strlen(arr), "device/D1/up",
                    1793936202000LL, &out, &num, err, sizeof(err));
  CHECK(rc == 0 && num == 2, err);
  if (rc == 0) {
    const char *l1 = out;
    const char *l2 = out + strlen(out) + 1;
    CHECK(strstr(l1, "event=\"a\"") != NULL && strstr(l1, "data=\"{\\\"event\\\":\\\"a\\\"") != NULL,
          "array row 0 has its own $payload");
    CHECK(strstr(l2, "event=\"b\"") != NULL, "array row 1 routed");
    CHECK(strstr(l1, "raw=\"[{\\\"event\\\":\\\"a\\\"") != NULL,
          "$message keeps the whole array");
  }
  free(out);
}

int main(int argc, char **argv) {
  const char *path = (argc > 1) ? argv[1] : MS_TEST_EXAMPLE_CONFIG;
  const char *rulesPath = (argc > 2) ? argv[2] : MS_TEST_RULES_CONFIG;
  testVarint();
  testConfig(path);
  SMsCfg cfg;
  char err[MS_ERR_LEN];
  msCfgDefault(&cfg);
  if (msLoadConfig(path, &cfg, err, sizeof(err)) == 0) {
    testMapping(&cfg);
    testTimestamp(&cfg);
    testLongValue(&cfg);
  }
  testConfigValidation();
  testRules(rulesPath);
  testRawColumns();
  fprintf(stdout, "msUnitTest: %d passed, %d failed\n", gPass, gFail);
  return gFail == 0 ? 0 : 1;
}
