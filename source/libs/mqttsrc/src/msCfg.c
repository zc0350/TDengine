/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * JSON config loading / validation.
 *
 * Unknown keys are rejected instead of silently ignored: a typo such as
 * "column" for "columns" used to fall back to the default model and run with
 * a completely different schema.
 */

#include "msInt.h"
#include "toolscJson.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>

static void setErr(char *err, int32_t errLen, const char *fmt, ...) {
  if (err == NULL || errLen <= 0) return;
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(err, (size_t)errLen, fmt, ap);
  va_end(ap);
}

static tools_cJSON *jobj(tools_cJSON *o, const char *k) {
  if (o == NULL) return NULL;
  return tools_cJSON_GetObjectItemCaseSensitive(o, k);
}

static const char *jstr(tools_cJSON *o, const char *k, const char *def) {
  tools_cJSON *v = jobj(o, k);
  if (v == NULL || !tools_cJSON_IsString(v) || v->valuestring == NULL) return def;
  return v->valuestring;
}

static int32_t jint(tools_cJSON *o, const char *k, int32_t def) {
  tools_cJSON *v = jobj(o, k);
  if (v == NULL) return def;
  if (tools_cJSON_IsNumber(v)) {
    double d = v->valuedouble;
    /* An out-of-range double -> int32_t conversion is undefined behaviour. */
    if (!isfinite(d) || d < (double)INT32_MIN || d > (double)INT32_MAX) return def;
    return (int32_t)d;
  }
  if (tools_cJSON_IsString(v) && v->valuestring != NULL) return msStrToInt(v->valuestring, def);
  return def;
}

static bool streqCI(const char *a, const char *b) {
  while (*a != '\0' && *b != '\0') {
    if (tolower((unsigned char)*a) != tolower((unsigned char)*b)) return false;
    ++a;
    ++b;
  }
  return *a == *b;
}

/* Returns 1/0, or -1 (with err set) for a non-boolean value. Previously
 * "TRUE"/"yes" silently became false and overrode the default. */
static int32_t jbool(tools_cJSON *o, const char *k, bool def, char *err,
                     int32_t errLen) {
  tools_cJSON *v = jobj(o, k);
  if (v == NULL) return def ? 1 : 0;
  if (tools_cJSON_IsTrue(v)) return 1;
  if (tools_cJSON_IsFalse(v)) return 0;
  if (tools_cJSON_IsString(v) && v->valuestring != NULL) {
    const char *p = v->valuestring;
    if (streqCI(p, "true") || streqCI(p, "1") || streqCI(p, "yes") ||
        streqCI(p, "on")) return 1;
    if (streqCI(p, "false") || streqCI(p, "0") || streqCI(p, "no") ||
        streqCI(p, "off")) return 0;
  }
  setErr(err, errLen, "config field '%s' must be a boolean", k);
  return -1;
}

/* Copy a string field, rejecting values that do not fit. Silently truncating
 * a client id, password or template would change behaviour without a trace. */
static int32_t jcopy(char *dst, int32_t cap, tools_cJSON *o, const char *k,
                     const char *def, const char *path, char *err, int32_t errLen) {
  tools_cJSON *v = jobj(o, k);
  if (v == NULL) {
    msStrCopy(dst, cap, def);
    return 0;
  }
  if (!tools_cJSON_IsString(v) || v->valuestring == NULL) {
    setErr(err, errLen, "config field '%s' must be a string", path);
    return -1;
  }
  size_t n = strlen(v->valuestring);
  if (n >= (size_t)cap) {
    setErr(err, errLen, "config field '%s' exceeds %d bytes", path, cap - 1);
    return -1;
  }
  memcpy(dst, v->valuestring, n + 1);
  return 0;
}

/* A TCP port is a protocol-level 16-bit value. Casting an int straight to
 * uint16_t turned "port": 70000 into 4464 and connected somewhere else. */
static int32_t jport(tools_cJSON *o, const char *k, uint16_t def,
                     const char *path, uint16_t *out, char *err, int32_t errLen) {
  tools_cJSON *v = jobj(o, k);
  if (v == NULL) {
    *out = def;
    return 0;
  }
  double d = 0;
  if (tools_cJSON_IsNumber(v)) {
    d = v->valuedouble;
  } else if (tools_cJSON_IsString(v) && v->valuestring != NULL) {
    char *end = NULL;
    errno = 0;
    long lv = strtol(v->valuestring, &end, 10);
    if (end == v->valuestring || *end != '\0' || errno == ERANGE) {
      setErr(err, errLen, "config field '%s' must be an integer", path);
      return -1;
    }
    d = (double)lv;
  } else {
    setErr(err, errLen, "config field '%s' must be an integer", path);
    return -1;
  }
  if (!isfinite(d) || d != floor(d) || d < 1.0 || d > 65535.0) {
    setErr(err, errLen, "config field '%s' must be an integer in 1..65535", path);
    return -1;
  }
  *out = (uint16_t)d;
  return 0;
}

/* Reject unknown keys. "_"-prefixed keys are comments/annotations and are
 * always allowed. */
static int32_t jcheckKeys(tools_cJSON *o, const char *path,
                          const char *const *allowed, int32_t allowedNum,
                          char *err, int32_t errLen) {
  if (o == NULL || !tools_cJSON_IsObject(o)) return 0;
  for (tools_cJSON *it = o->child; it != NULL; it = it->next) {
    if (it->string == NULL || it->string[0] == '_') continue;
    bool ok = false;
    for (int32_t i = 0; i < allowedNum; ++i) {
      if (strcmp(it->string, allowed[i]) == 0) {
        ok = true;
        break;
      }
    }
    if (!ok) {
      setErr(err, errLen, "unknown config key '%s.%s'", path, it->string);
      return -1;
    }
  }
  return 0;
}

void msCfgDefault(SMsCfg *cfg) {
  if (cfg == NULL) return;
  memset(cfg, 0, sizeof(*cfg));

  SMsBrokerCfg *b = &cfg->broker;
  msStrCopy(b->host, sizeof(b->host), "127.0.0.1");
  b->port = 1883;
  /* Unique per process; a fixed id makes two instances evict each other and
   * makes a persistent session impossible. */
  msDefaultClientId(b->clientId, sizeof(b->clientId));
  b->keepalive = 60;
  /* Persistent session: QoS1/2 messages that were not acknowledged stay on
   * the broker while this client is disconnected and are redelivered. */
  b->cleanSession = false;
  b->reconnectMs = 1000;
  b->maxReconnectMs = 30000;
  b->connectTimeoutMs = 5000;
  b->tls = false;
  b->tlsInsecure = false;

  SMsSinkCfg *s = &cfg->sink;
  msStrCopy(s->host, sizeof(s->host), "127.0.0.1");
  s->port = 6030;
  msStrCopy(s->user, sizeof(s->user), "root");
  msStrCopy(s->password, sizeof(s->password), "taosdata");
  msStrCopy(s->db, sizeof(s->db), "mqtt");
  s->createDb = true;
  s->batchSize = 1000;
  s->flushMs = 200;
  s->maxRetries = 3;
  s->retryMs = 500;
  s->ttl = 0;
  s->dryRun = false;
  s->queueSize = 10000;

  SMsMappingCfg *m = &cfg->mapping;
  msStrCopy(m->stable, sizeof(m->stable), "mqtt_data");
  msStrCopy(m->table, sizeof(m->table), "t_{_topic_flat}");
  msStrCopy(m->tbnameKey, sizeof(m->tbnameKey), "tbname");
  m->tsPrecision = MS_TS_MILLI;

  cfg->statsIntervalMs = 60000;
}

static char *readFile(const char *path, int32_t *len) {
  FILE *fp = fopen(path, "rb");
  if (fp == NULL) return NULL;
  if (fseek(fp, 0, SEEK_END) != 0) {
    fclose(fp);
    return NULL;
  }
  long size = ftell(fp);
  if (size < 0 || size > 4 * 1024 * 1024 || fseek(fp, 0, SEEK_SET) != 0) {
    fclose(fp);
    return NULL;
  }
  char *buf = (char *)malloc((size_t)size + 1);
  if (buf == NULL) {
    fclose(fp);
    return NULL;
  }
  size_t n = fread(buf, 1, (size_t)size, fp);
  fclose(fp);
  buf[n] = '\0';
  *len = (int32_t)n;
  return buf;
}

static const char *const kRootKeys[] = {"mqtt", "tdengine", "mapping",
                                        "stats_interval_ms"};
static const char *const kMqttKeys[] = {
    "host",          "port",          "client_id",       "username",
    "password",      "keepalive",     "clean_session",   "reconnect_ms",
    "max_reconnect_ms", "connect_timeout_ms", "topics", "topic_vars", "tls"};
static const char *const kTlsKeys[] = {"enabled", "ca_file", "cert_file",
                                       "key_file", "insecure_skip_verify",
                                       "server_name"};
static const char *const kTopicKeys[] = {"filter", "qos"};
static const char *const kSinkKeys[] = {
    "host",     "port",        "user",       "password",  "database",
    "create_db", "batch_size", "flush_ms",   "max_retries", "retry_ms",
    "ttl",      "dry_run",     "queue_size"};
static const char *const kMappingKeys[] = {"stable",    "table",     "tbname_key",
                                           "ts_field",  "ts_precision",
                                           "columns",   "tags",      "rules"};
static const char *const kColumnKeys[] = {"name", "path", "type"};
static const char *const kTagKeys[] = {"name", "value"};
static const char *const kRuleKeys[] = {"name",   "when",         "stable",
                                        "table",  "tbname_key",   "ts_field",
                                        "ts_precision", "columns", "tags"};
static const char *const kWhenKeys[] = {"path", "op", "value"};

#define MS_ARRAY_LEN(a) ((int32_t)(sizeof(a) / sizeof((a)[0])))

static int32_t parseTopics(tools_cJSON *root, SMsBrokerCfg *b, char *err, int32_t errLen) {
  tools_cJSON *arr = jobj(root, "topics");
  if (arr == NULL) return 0;
  if (!tools_cJSON_IsArray(arr)) {
    setErr(err, errLen, "topics must be an array");
    return -1;
  }
  int32_t n = tools_cJSON_GetArraySize(arr);
  for (int32_t i = 0; i < n; ++i) {
    if (b->topicNum >= MS_MAX_TOPICS) {
      setErr(err, errLen, "too many topic filters (max %d)", MS_MAX_TOPICS);
      return -1;
    }
    tools_cJSON *it = tools_cJSON_GetArrayItem(arr, i);
    if (it == NULL) {
      setErr(err, errLen, "topics[%d] is empty", i);
      return -1;
    }
    SMsTopic *t = &b->topics[b->topicNum];
    int32_t qos = 0;
    const char *filter = NULL;

    if (tools_cJSON_IsString(it) && it->valuestring != NULL) {
      /* Shorthand: "factory/+/telemetry::1". "::" is legal inside a topic
       * filter (e.g. "home::kitchen/#"), so only a trailing ::0/1/2 is
       * treated as the QoS suffix. */
      char tmp[MS_TEXT_LEN];
      size_t n2 = strlen(it->valuestring);      if (n2 >= sizeof(tmp)) {
        setErr(err, errLen, "topics[%d] exceeds %d bytes", i, (int)sizeof(tmp) - 1);
        return -1;
      }
      memcpy(tmp, it->valuestring, n2 + 1);
      char *sep = strstr(tmp, "::");
      if (sep != NULL && sep[2] >= '0' && sep[2] <= '2' && sep[3] == '\0') {
        qos = sep[2] - '0';
        *sep = '\0';
      }
      filter = tmp;
      msStrCopy(t->filter, sizeof(t->filter), filter);
    } else if (tools_cJSON_IsObject(it)) {
      if (jcheckKeys(it, "mqtt.topics[]", kTopicKeys, MS_ARRAY_LEN(kTopicKeys),
                     err, errLen) != 0) {
        return -1;
      }
      filter = jstr(it, "filter", NULL);
      if (filter == NULL || *filter == '\0') {
        setErr(err, errLen, "topics[%d] has no filter", i);
        return -1;
      }
      if (strlen(filter) >= sizeof(t->filter)) {
        setErr(err, errLen, "topics[%d].filter exceeds %d bytes", i,
               (int)sizeof(t->filter) - 1);
        return -1;
      }
      msStrCopy(t->filter, sizeof(t->filter), filter);
      qos = jint(it, "qos", 0);
    } else {
      setErr(err, errLen, "topics[%d] must be a string or object", i);
      return -1;
    }

    if (t->filter[0] == '\0') {
      setErr(err, errLen, "topics[%d] has empty filter", i);
      return -1;
    }
    if (qos < 0 || qos > 2) {
      setErr(err, errLen, "topics[%d].qos must be 0, 1 or 2", i);
      return -1;
    }
    t->qos = qos;
    ++b->topicNum;
  }
  return 0;
}

static int32_t parseTopicVars(tools_cJSON *root, SMsBrokerCfg *b, char *err,
                              int32_t errLen) {
  tools_cJSON *arr = jobj(root, "topic_vars");
  if (arr == NULL) return 0;
  if (!tools_cJSON_IsArray(arr)) {
    setErr(err, errLen, "topic_vars must be an array");
    return -1;
  }
  int32_t n = tools_cJSON_GetArraySize(arr);
  for (int32_t i = 0; i < n; ++i) {
    if (b->topicVarNum >= MS_MAX_TVARS) {
      setErr(err, errLen, "too many topic_vars (max %d)", MS_MAX_TVARS);
      return -1;
    }
    tools_cJSON *it = tools_cJSON_GetArrayItem(arr, i);
    if (it == NULL || !tools_cJSON_IsString(it) || it->valuestring == NULL) {
      setErr(err, errLen, "topic_vars[%d] must be a string", i);
      return -1;
    }
    if (strlen(it->valuestring) >= MS_NAME_LEN) {
      setErr(err, errLen, "topic_vars[%d] exceeds %d bytes", i, MS_NAME_LEN - 1);
      return -1;
    }
    snprintf(b->topicVars[b->topicVarNum], MS_NAME_LEN, "%s", it->valuestring);
    ++b->topicVarNum;
  }
  return 0;
}

static int32_t parseColumns(tools_cJSON *obj, SMsColumn *pool, int32_t cap,
                            int32_t *num, char *err, int32_t errLen) {
  tools_cJSON *arr = jobj(obj, "columns");
  if (arr == NULL) return 0;
  if (!tools_cJSON_IsArray(arr)) {
    setErr(err, errLen, "columns must be an array");
    return -1;
  }
  int32_t n = tools_cJSON_GetArraySize(arr);
  for (int32_t i = 0; i < n; ++i) {
    if (*num >= cap) {
      setErr(err, errLen, "too many columns (max %d)", cap);
      return -1;
    }
    tools_cJSON *it = tools_cJSON_GetArrayItem(arr, i);
    if (it == NULL || !tools_cJSON_IsObject(it)) {
      setErr(err, errLen, "columns[%d] must be an object", i);
      return -1;
    }
    if (jcheckKeys(it, "mapping.columns[]", kColumnKeys, MS_ARRAY_LEN(kColumnKeys),
                   err, errLen) != 0) {
      return -1;
    }
    const char *name = jstr(it, "name", NULL);
    const char *path = jstr(it, "path", NULL);
    if (name == NULL || path == NULL || *name == '\0' || *path == '\0') {
      setErr(err, errLen, "columns[%d] needs name and path", i);
      return -1;
    }
    if (strlen(name) >= MS_NAME_LEN || strlen(path) >= MS_PATH_LEN) {
      setErr(err, errLen, "columns[%d] name or path is too long", i);
      return -1;
    }
    SMsColumn *c = &pool[*num];
    snprintf(c->name, sizeof(c->name), "%s", name);
    snprintf(c->path, sizeof(c->path), "%s", path);
    const char *type = jstr(it, "type", "auto");
    if (strcmp(type, "string") == 0)        c->type = MS_COL_STRING;
    else if (strcmp(type, "bool") == 0)     c->type = MS_COL_BOOL;
    else if (strcmp(type, "int") == 0 ||
             strcmp(type, "bigint") == 0 ||
             strcmp(type, "i64") == 0)       c->type = MS_COL_INT;
    else if (strcmp(type, "number") == 0 ||
             strcmp(type, "double") == 0 ||
             strcmp(type, "float") == 0)     c->type = MS_COL_NUMBER;
    else if (strcmp(type, "auto") == 0)     c->type = MS_COL_AUTO;
    else {
      setErr(err, errLen, "columns[%d] has unknown type '%s'", i, type);
      return -1;
    }
    ++(*num);
  }
  return 0;
}

static int32_t parseTags(tools_cJSON *obj, SMsTag *pool, int32_t cap,
                         int32_t *num, char *err, int32_t errLen) {
  tools_cJSON *arr = jobj(obj, "tags");
  if (arr == NULL) return 0;
  if (!tools_cJSON_IsArray(arr)) {
    setErr(err, errLen, "tags must be an array");
    return -1;
  }
  int32_t n = tools_cJSON_GetArraySize(arr);
  for (int32_t i = 0; i < n; ++i) {
    if (*num >= cap) {
      setErr(err, errLen, "too many tags (max %d)", cap);
      return -1;
    }
    tools_cJSON *it = tools_cJSON_GetArrayItem(arr, i);
    if (it == NULL || !tools_cJSON_IsObject(it)) {
      setErr(err, errLen, "tags[%d] must be an object", i);
      return -1;
    }
    if (jcheckKeys(it, "mapping.tags[]", kTagKeys, MS_ARRAY_LEN(kTagKeys),
                   err, errLen) != 0) {
      return -1;
    }
    const char *name = jstr(it, "name", NULL);
    const char *value = jstr(it, "value", NULL);
    if (name == NULL || *name == '\0' || value == NULL) {
      setErr(err, errLen, "tags[%d] needs name and value", i);
      return -1;
    }
    if (strlen(name) >= MS_NAME_LEN || strlen(value) >= MS_TEXT_LEN) {
      setErr(err, errLen, "tags[%d] name or value is too long", i);
      return -1;
    }
    snprintf(pool[*num].name, sizeof(pool[*num].name), "%s", name);
    snprintf(pool[*num].value, sizeof(pool[*num].value), "%s", value);
    ++(*num);
  }
  return 0;
}

/* Returns -1 for an unknown precision so the caller can reject the config
 * instead of silently assuming milliseconds. */
static int32_t parsePrecision(const char *prec) {
  if (prec == NULL || strcmp(prec, "ms") == 0) return MS_TS_MILLI;
  if (strcmp(prec, "s") == 0) return MS_TS_SECOND;
  if (strcmp(prec, "us") == 0) return MS_TS_MICRO;
  if (strcmp(prec, "ns") == 0) return MS_TS_NANO;
  return -1;
}

static int32_t parseWhen(tools_cJSON *obj, SMsRule *rule, char *err, int32_t errLen) {
  tools_cJSON *cond = jobj(obj, "when");
  if (cond == NULL) return 0;
  bool isArray = tools_cJSON_IsArray(cond);
  if (!isArray && !tools_cJSON_IsObject(cond)) {
    setErr(err, errLen, "when must be an object or an array of objects");
    return -1;
  }
  int32_t count = isArray ? tools_cJSON_GetArraySize(cond) : 1;
  if (count > MS_MAX_WHEN) {
    setErr(err, errLen, "too many when conditions (max %d)", MS_MAX_WHEN);
    return -1;
  }
  for (int32_t i = 0; i < count; ++i) {
    tools_cJSON *it = isArray ? tools_cJSON_GetArrayItem(cond, i) : cond;
    if (it == NULL || !tools_cJSON_IsObject(it)) {
      setErr(err, errLen, "when[%d] must be an object", i);
      return -1;
    }
    if (jcheckKeys(it, "when[]", kWhenKeys, MS_ARRAY_LEN(kWhenKeys), err, errLen) != 0) {
      return -1;
    }
    const char *path = jstr(it, "path", NULL);
    if (path == NULL || *path == '\0') {
      setErr(err, errLen, "when[%d] needs path", i);
      return -1;
    }
    if (strlen(path) >= MS_PATH_LEN) {
      setErr(err, errLen, "when[%d].path is too long", i);
      return -1;
    }
    SMsWhen *w = &rule->when[rule->whenNum];
    snprintf(w->path, sizeof(w->path), "%s", path);

    const char *op = jstr(it, "op", "eq");
    int32_t parsed;
    if (strcmp(op, "eq") == 0)            parsed = MS_WHEN_EQ;
    else if (strcmp(op, "ne") == 0)       parsed = MS_WHEN_NE;
    else if (strcmp(op, "prefix") == 0)   parsed = MS_WHEN_PREFIX;
    else if (strcmp(op, "contains") == 0) parsed = MS_WHEN_CONTAINS;
    else if (strcmp(op, "in") == 0)       parsed = MS_WHEN_IN;
    else if (strcmp(op, "exists") == 0)   parsed = MS_WHEN_EXISTS;
    else {
      setErr(err, errLen, "when[%d] has unknown op '%s'", i, op);
      return -1;
    }
    w->op = parsed;
    const char *val = jstr(it, "value", "");
    if (strlen(val) >= MS_TEXT_LEN) {
      setErr(err, errLen, "when[%d].value is too long", i);
      return -1;
    }
    snprintf(w->value, sizeof(w->value), "%s", val);
    /* An empty prefix/contains/in value matches everything and would turn a
     * rule into an unintended catch-all. */
    if (parsed != MS_WHEN_EXISTS && w->value[0] == '\0') {
      setErr(err, errLen, "when[%d] (op '%s') needs a non-empty value", i, op);
      return -1;
    }
    ++rule->whenNum;
  }
  return 0;
}

/* Rules inherit the top-level model, so a rule only declares its differences.
 * Explicitly declared columns/tags replace (not extend) the inherited ones. */
static int32_t parseRules(tools_cJSON *obj, SMsMappingCfg *m,
                          int32_t *poolCols, int32_t *poolTags,
                          char *err, int32_t errLen) {
  tools_cJSON *arr = jobj(obj, "rules");
  if (arr == NULL) return 0;
  if (!tools_cJSON_IsArray(arr)) {
    setErr(err, errLen, "rules must be an array");
    return -1;
  }
  int32_t n = tools_cJSON_GetArraySize(arr);
  for (int32_t i = 0; i < n; ++i) {
    if (m->ruleNum >= MS_MAX_RULES) {
      setErr(err, errLen, "too many rules (max %d)", MS_MAX_RULES);
      return -1;
    }
    tools_cJSON *it = tools_cJSON_GetArrayItem(arr, i);
    if (it == NULL || !tools_cJSON_IsObject(it)) {
      setErr(err, errLen, "rules[%d] must be an object", i);
      return -1;
    }
    if (jcheckKeys(it, "mapping.rules[]", kRuleKeys, MS_ARRAY_LEN(kRuleKeys),
                   err, errLen) != 0) {
      return -1;
    }
    SMsRule *r = &m->rules[m->ruleNum];

    const char *name = jstr(it, "name", "rule");
    const char *stable = jstr(it, "stable", m->stable);
    const char *table = jstr(it, "table", m->table);
    const char *tbnameKey = jstr(it, "tbname_key", m->tbnameKey);
    const char *tsField = jstr(it, "ts_field", m->tsField);
    if (strlen(name) >= MS_NAME_LEN || strlen(stable) >= MS_NAME_LEN ||
        strlen(table) >= MS_NAME_LEN || strlen(tbnameKey) >= MS_NAME_LEN ||
        strlen(tsField) >= MS_PATH_LEN) {
      setErr(err, errLen, "rules[%d] has an over-long name or template", i);
      return -1;
    }
    snprintf(r->name, sizeof(r->name), "%s", name);
    msStrCopy(r->stable, sizeof(r->stable), stable);
    msStrCopy(r->table, sizeof(r->table), table);
    msStrCopy(r->tbnameKey, sizeof(r->tbnameKey), tbnameKey);
    msStrCopy(r->tsField, sizeof(r->tsField), tsField);
    const char *prec = jstr(it, "ts_precision", NULL);
    r->tsPrecision = (prec != NULL) ? parsePrecision(prec) : m->tsPrecision;
    if (r->tsPrecision < 0) {
      setErr(err, errLen, "rules[%d].ts_precision must be s/ms/us/ns", i);
      return -1;
    }
    if (parseWhen(it, r, err, errLen) != 0) return -1;

    if (jobj(it, "columns") != NULL) {
      r->colStart = *poolCols;
      if (parseColumns(it, m->columns, MS_MAX_COLUMNS, poolCols, err, errLen) != 0) return -1;
      r->colNum = *poolCols - r->colStart;
    } else {
      r->colStart = 0;
      r->colNum = m->columnNum;
    }
    if (jobj(it, "tags") != NULL) {
      r->tagStart = *poolTags;
      if (parseTags(it, m->tags, MS_MAX_TAGS, poolTags, err, errLen) != 0) return -1;
      r->tagNum = *poolTags - r->tagStart;
    } else {
      r->tagStart = 0;
      r->tagNum = m->tagNum;
    }

    if (r->colNum <= 0) {
      setErr(err, errLen, "rules[%d] has no columns", i);
      return -1;
    }
    ++m->ruleNum;
  }
  return 0;
}

int32_t msLoadConfig(const char *path, SMsCfg *cfg, char *err, int32_t errLen) {
  if (err != NULL && errLen > 0) err[0] = '\0';
  if (path == NULL || cfg == NULL) {
    setErr(err, errLen, "null config path or cfg");
    return -1;
  }
  msCfgDefault(cfg);

  int32_t len = 0;
  char *text = readFile(path, &len);
  if (text == NULL) {
    setErr(err, errLen, "cannot read config file: %s", path);
    return -1;
  }

  tools_cJSON *root = tools_cJSON_Parse(text);
  free(text);
  if (root == NULL || !tools_cJSON_IsObject(root)) {
    setErr(err, errLen, "config is not a valid JSON object: %s", path);
    if (root != NULL) tools_cJSON_Delete(root);
    return -1;
  }

  int32_t rc = -1;
  SMsBrokerCfg *b = &cfg->broker;
  SMsSinkCfg *s = &cfg->sink;
  SMsMappingCfg *m = &cfg->mapping;

  if (jcheckKeys(root, "config", kRootKeys, MS_ARRAY_LEN(kRootKeys), err, errLen) != 0) {
    goto done;
  }

  cfg->statsIntervalMs = jint(root, "stats_interval_ms", cfg->statsIntervalMs);

  tools_cJSON *jb = jobj(root, "mqtt");
  if (jb != NULL) {
    if (!tools_cJSON_IsObject(jb)) {
      setErr(err, errLen, "'mqtt' must be an object");
      goto done;
    }
    if (jcheckKeys(jb, "mqtt", kMqttKeys, MS_ARRAY_LEN(kMqttKeys), err, errLen) != 0 ||
        jcopy(b->host, sizeof(b->host), jb, "host", b->host, "mqtt.host", err, errLen) != 0 ||
        jport(jb, "port", b->port, "mqtt.port", &b->port, err, errLen) != 0 ||
        jcopy(b->clientId, sizeof(b->clientId), jb, "client_id", b->clientId,
              "mqtt.client_id", err, errLen) != 0 ||
        jcopy(b->username, sizeof(b->username), jb, "username", "", "mqtt.username",
              err, errLen) != 0 ||
        jcopy(b->password, sizeof(b->password), jb, "password", "", "mqtt.password",
              err, errLen) != 0) {
      goto done;
    }
    b->keepalive = jint(jb, "keepalive", b->keepalive);
    int32_t cleanSession = jbool(jb, "clean_session", b->cleanSession, err, errLen);
    if (cleanSession < 0) goto done;
    b->cleanSession = (cleanSession != 0);
    b->reconnectMs = jint(jb, "reconnect_ms", b->reconnectMs);
    b->maxReconnectMs = jint(jb, "max_reconnect_ms", b->maxReconnectMs);
    b->connectTimeoutMs = jint(jb, "connect_timeout_ms", b->connectTimeoutMs);

    tools_cJSON *jt = jobj(jb, "tls");
    if (jt != NULL) {
      if (!tools_cJSON_IsObject(jt)) {
        setErr(err, errLen, "'mqtt.tls' must be an object");
        goto done;
      }
      if (jcheckKeys(jt, "mqtt.tls", kTlsKeys, MS_ARRAY_LEN(kTlsKeys), err, errLen) != 0) {
        goto done;
      }
      int32_t tls = jbool(jt, "enabled", false, err, errLen);
      int32_t insecure = jbool(jt, "insecure_skip_verify", false, err, errLen);
      if (tls < 0 || insecure < 0) goto done;
      b->tls = (tls != 0);
      b->tlsInsecure = (insecure != 0);
      if (jcopy(b->tlsCaFile, sizeof(b->tlsCaFile), jt, "ca_file", "",
                "mqtt.tls.ca_file", err, errLen) != 0 ||
          jcopy(b->tlsCertFile, sizeof(b->tlsCertFile), jt, "cert_file", "",
                "mqtt.tls.cert_file", err, errLen) != 0 ||
          jcopy(b->tlsKeyFile, sizeof(b->tlsKeyFile), jt, "key_file", "",
                "mqtt.tls.key_file", err, errLen) != 0 ||
          jcopy(b->tlsServerName, sizeof(b->tlsServerName), jt, "server_name", "",
                "mqtt.tls.server_name", err, errLen) != 0) {
        goto done;
      }
    }
    if (parseTopics(jb, b, err, errLen) != 0 ||
        parseTopicVars(jb, b, err, errLen) != 0) {
      goto done;
    }
  }

  tools_cJSON *js = jobj(root, "tdengine");
  if (js != NULL) {
    if (!tools_cJSON_IsObject(js)) {
      setErr(err, errLen, "'tdengine' must be an object");
      goto done;
    }
    if (jcheckKeys(js, "tdengine", kSinkKeys, MS_ARRAY_LEN(kSinkKeys), err, errLen) != 0 ||
        jcopy(s->host, sizeof(s->host), js, "host", s->host, "tdengine.host", err, errLen) != 0 ||
        jport(js, "port", s->port, "tdengine.port", &s->port, err, errLen) != 0 ||
        jcopy(s->user, sizeof(s->user), js, "user", s->user, "tdengine.user", err, errLen) != 0 ||
        jcopy(s->password, sizeof(s->password), js, "password", s->password,
              "tdengine.password", err, errLen) != 0 ||
        jcopy(s->db, sizeof(s->db), js, "database", s->db, "tdengine.database",
              err, errLen) != 0) {
      goto done;
    }
    int32_t createDb = jbool(js, "create_db", s->createDb, err, errLen);
    int32_t dryRun = jbool(js, "dry_run", s->dryRun, err, errLen);
    if (createDb < 0 || dryRun < 0) goto done;
    s->createDb = (createDb != 0);
    s->batchSize = jint(js, "batch_size", s->batchSize);
    s->flushMs = jint(js, "flush_ms", s->flushMs);
    s->maxRetries = jint(js, "max_retries", s->maxRetries);
    s->retryMs = jint(js, "retry_ms", s->retryMs);
    s->ttl = (int32_t)jint(js, "ttl", s->ttl);
    s->queueSize = jint(js, "queue_size", s->queueSize);
    s->dryRun = (dryRun != 0);
  }

  tools_cJSON *jm = jobj(root, "mapping");
  if (jm != NULL) {
    if (!tools_cJSON_IsObject(jm)) {
      setErr(err, errLen, "'mapping' must be an object");
      goto done;
    }
    if (jcheckKeys(jm, "mapping", kMappingKeys, MS_ARRAY_LEN(kMappingKeys),
                   err, errLen) != 0 ||
        jcopy(m->stable, sizeof(m->stable), jm, "stable", m->stable, "mapping.stable",
              err, errLen) != 0 ||
        jcopy(m->table, sizeof(m->table), jm, "table", m->table, "mapping.table",
              err, errLen) != 0 ||
        jcopy(m->tbnameKey, sizeof(m->tbnameKey), jm, "tbname_key", m->tbnameKey,
              "mapping.tbname_key", err, errLen) != 0 ||
        jcopy(m->tsField, sizeof(m->tsField), jm, "ts_field", "", "mapping.ts_field",
              err, errLen) != 0) {
      goto done;
    }
    m->tsPrecision = parsePrecision(jstr(jm, "ts_precision", "ms"));
    if (m->tsPrecision < 0) {
      setErr(err, errLen, "mapping.ts_precision must be s/ms/us/ns");
      goto done;
    }
    if (parseColumns(jm, m->columns, MS_MAX_COLUMNS, &m->columnNum, err, errLen) != 0 ||
        parseTags(jm, m->tags, MS_MAX_TAGS, &m->tagNum, err, errLen) != 0) {
      goto done;
    }
    int32_t poolCols = m->columnNum;
    int32_t poolTags = m->tagNum;
    if (parseRules(jm, m, &poolCols, &poolTags, err, errLen) != 0) {
      goto done;
    }
  }

  rc = msValidateConfig(cfg, err, errLen);

done:
  tools_cJSON_Delete(root);
  return rc;
}

static bool validIdent(const char *p) {
  if (*p == '\0') return false;
  if (!((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') || *p == '_')) {
    return false;
  }
  for (; *p != '\0'; ++p) {
    bool ok = (*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') ||
              (*p >= '0' && *p <= '9') || *p == '_';
    if (!ok) return false;
  }
  return true;
}

int32_t msValidateConfig(const SMsCfg *cfg, char *err, int32_t errLen) {
  if (err != NULL && errLen > 0) err[0] = '\0';
  if (cfg == NULL) {
    setErr(err, errLen, "null cfg");
    return -1;
  }
  const SMsBrokerCfg *b = &cfg->broker;
  const SMsSinkCfg *s = &cfg->sink;
  const SMsMappingCfg *m = &cfg->mapping;

  if (b->host[0] == '\0' || b->port == 0 || b->clientId[0] == '\0') {
    setErr(err, errLen, "mqtt.host / port / client_id must be set");
    return -1;
  }
  /* MQTT 3.1.1 3.1.2.9: the password flag may only be set together with the
   * user name flag; otherwise every broker rejects the CONNECT. */
  if (b->password[0] != '\0' && b->username[0] == '\0') {
    setErr(err, errLen, "mqtt.username is required when mqtt.password is set");
    return -1;
  }
  if (b->tls && ((b->tlsCertFile[0] == '\0') != (b->tlsKeyFile[0] == '\0'))) {
    setErr(err, errLen, "mqtt.tls.cert_file and mqtt.tls.key_file must be set together");
    return -1;
  }
  if (b->tls) {
#ifndef MS_WITH_TLS
    setErr(err, errLen,
           "mqtt.tls.enabled requires a build with TLS support "
           "(configure with -DMQTTSRC_ENABLE_TLS=ON); refusing to send "
           "credentials in clear text");
    return -1;
#endif
  }
  if (b->topicNum <= 0 || b->topicNum > MS_MAX_TOPICS ||
      b->topicVarNum < 0 || b->topicVarNum > MS_MAX_TVARS) {
    setErr(err, errLen, "mqtt.topics must contain 1..%d filters (and <=%d vars)",
           MS_MAX_TOPICS, MS_MAX_TVARS);
    return -1;
  }
  /* Keep every timeout/backoff bounded: the MQTT loop multiplies and adds
   * these values. */
  if (b->keepalive <= 0 || b->keepalive > 65535 ||
      b->reconnectMs <= 0 || b->reconnectMs > 600000 ||
      b->maxReconnectMs < b->reconnectMs || b->maxReconnectMs > 600000 ||
      b->connectTimeoutMs <= 0 || b->connectTimeoutMs > 600000) {
    setErr(err, errLen, "invalid keepalive / reconnect / connect_timeout settings");
    return -1;
  }
  for (int32_t i = 0; i < b->topicNum; ++i) {
    if (b->topics[i].filter[0] == '\0') {
      setErr(err, errLen, "mqtt.topics[%d] has empty filter", i);
      return -1;
    }
    if (b->topics[i].qos < 0 || b->topics[i].qos > 2) {
      setErr(err, errLen, "mqtt.topics[%d].qos must be 0, 1 or 2", i);
      return -1;
    }
  }
  if (s->host[0] == '\0' || s->port == 0 || s->db[0] == '\0' || s->batchSize <= 0 ||
      s->batchSize > 100000 || s->flushMs <= 0 || s->flushMs > 600000 ||
      s->maxRetries < 0 || s->maxRetries > 10 || s->retryMs < 0 ||
      s->retryMs > 60000 || s->ttl < 0 || s->queueSize <= 0 ||
      s->queueSize > 1000000) {
    setErr(err, errLen,
           "tdengine.host / port / database / batch_size / flush_ms / queue_size "
           "/ retry invalid");
    return -1;
  }
  /* db is interpolated into CREATE DATABASE; only allow safe identifiers. */
  if (!validIdent(s->db)) {
    setErr(err, errLen, "tdengine.database must be [A-Za-z0-9_]");
    return -1;
  }
  if (cfg->statsIntervalMs < 0 || cfg->statsIntervalMs > 3600000) {
    setErr(err, errLen, "stats_interval_ms must be 0..3600000");
    return -1;
  }
  if (m->stable[0] == '\0' || m->table[0] == '\0' || m->tbnameKey[0] == '\0') {
    setErr(err, errLen, "mapping.stable / table / tbname_key must be set");
    return -1;
  }
  /* The key is passed to the schemaless API as a tag name; a comma or space
   * would silently select the wrong tag (or none). */
  if (!validIdent(m->tbnameKey)) {
    setErr(err, errLen, "mapping.tbname_key must be [A-Za-z0-9_]");
    return -1;
  }
  if (m->columnNum < 0 || m->columnNum > MS_MAX_COLUMNS ||
      m->tagNum < 0 || m->tagNum > MS_MAX_TAGS ||
      m->ruleNum < 0 || m->ruleNum > MS_MAX_RULES) {
    setErr(err, errLen, "mapping pools exceed their configured limits");
    return -1;
  }
  if (m->columnNum <= 0 && m->ruleNum <= 0) {
    setErr(err, errLen, "mapping needs columns or at least one rule");
    return -1;
  }
  if (m->tsPrecision < MS_TS_SECOND || m->tsPrecision > MS_TS_NANO) {
    setErr(err, errLen, "mapping.ts_precision must be s/ms/us/ns");
    return -1;
  }
  /* A template that cannot render wastes every message: fail at startup. */
  if (msValidateTemplate(m->stable, "mapping.stable", b->topicVars, b->topicVarNum,
                         err, errLen) != 0 ||
      msValidateTemplate(m->table, "mapping.table", b->topicVars, b->topicVarNum,
                         err, errLen) != 0) {
    return -1;
  }
  for (int32_t i = 0; i < m->tagNum; ++i) {
    if (msValidateTemplate(m->tags[i].value, "mapping.tags[].value", b->topicVars,
                           b->topicVarNum, err, errLen) != 0) {
      return -1;
    }
  }
  for (int32_t i = 0; i < m->ruleNum; ++i) {
    const SMsRule *r = &m->rules[i];
    if (r->stable[0] == '\0' || r->table[0] == '\0' || r->tbnameKey[0] == '\0') {
      setErr(err, errLen, "rules[%d] needs stable / table / tbname_key", i);
      return -1;
    }
    /* One schemaless batch carries a single tbnameKey, so all rules must agree. */
    if (strcmp(r->tbnameKey, m->tbnameKey) != 0) {
      setErr(err, errLen, "rules[%d].tbname_key must equal mapping.tbname_key", i);
      return -1;
    }
    if (r->colNum <= 0 || r->colStart < 0 || r->colStart + r->colNum > MS_MAX_COLUMNS ||
        r->tagStart < 0 || r->tagStart + r->tagNum > MS_MAX_TAGS) {
      setErr(err, errLen, "rules[%d] has invalid column/tag slice", i);
      return -1;
    }
    if (r->tsPrecision < MS_TS_SECOND || r->tsPrecision > MS_TS_NANO) {
      setErr(err, errLen, "rules[%d].ts_precision invalid", i);
      return -1;
    }
    if (r->whenNum < 0 || r->whenNum > MS_MAX_WHEN) {
      setErr(err, errLen, "rules[%d].when has too many conditions", i);
      return -1;
    }
    for (int32_t j = 0; j < r->whenNum; ++j) {
      const SMsWhen *w = &r->when[j];
      if (w->op < MS_WHEN_EQ || w->op > MS_WHEN_EXISTS) {
        setErr(err, errLen, "rules[%d].when[%d] has an invalid op", i, j);
        return -1;
      }
      if (w->op != MS_WHEN_EXISTS && w->value[0] == '\0') {
        setErr(err, errLen, "rules[%d].when[%d] needs a non-empty value", i, j);
        return -1;
      }
    }
    if (msValidateTemplate(r->stable, "rules[].stable", b->topicVars, b->topicVarNum,
                           err, errLen) != 0 ||
        msValidateTemplate(r->table, "rules[].table", b->topicVars, b->topicVarNum,
                           err, errLen) != 0) {
      return -1;
    }
    for (int32_t j = 0; j < r->tagNum; ++j) {
      if (msValidateTemplate(m->tags[r->tagStart + j].value, "rules[].tags[].value",
                             b->topicVars, b->topicVarNum, err, errLen) != 0) {
        return -1;
      }
    }
  }
  return 0;
}
