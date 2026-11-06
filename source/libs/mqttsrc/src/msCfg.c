/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * JSON config loading / validation.
 */

#include "msInt.h"
#include "toolscJson.h"

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
  if (tools_cJSON_IsNumber(v)) return (int32_t)v->valuedouble;
  if (tools_cJSON_IsString(v) && v->valuestring != NULL) return msStrToInt(v->valuestring, def);
  return def;
}

static bool jbool(tools_cJSON *o, const char *k, bool def) {
  tools_cJSON *v = jobj(o, k);
  if (v == NULL) return def;
  if (tools_cJSON_IsTrue(v)) return true;
  if (tools_cJSON_IsFalse(v)) return false;
  if (tools_cJSON_IsString(v) && v->valuestring != NULL) {
    return (strcmp(v->valuestring, "true") == 0 || strcmp(v->valuestring, "1") == 0);
  }
  return def;
}

void msCfgDefault(SMsCfg *cfg) {
  if (cfg == NULL) return;
  memset(cfg, 0, sizeof(*cfg));

  SMsBrokerCfg *b = &cfg->broker;
  msStrCopy(b->host, sizeof(b->host), "127.0.0.1");
  b->port = 1883;
  msStrCopy(b->clientId, sizeof(b->clientId), "taosx-mqttsrc");
  b->keepalive = 60;
  b->cleanSession = true;
  b->reconnectMs = 1000;
  b->maxReconnectMs = 30000;
  b->connectTimeoutMs = 5000;

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

  SMsMappingCfg *m = &cfg->mapping;
  msStrCopy(m->stable, sizeof(m->stable), "mqtt_data");
  msStrCopy(m->table, sizeof(m->table), "t_{_topic_flat}");
  msStrCopy(m->tbnameKey, sizeof(m->tbnameKey), "tbname");
  m->tsPrecision = MS_TS_MILLI;
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

static int32_t parseTopics(tools_cJSON *root, SMsBrokerCfg *b, char *err, int32_t errLen) {
  tools_cJSON *arr = jobj(root, "topics");
  if (arr == NULL || !tools_cJSON_IsArray(arr)) return 0;
  int32_t n = tools_cJSON_GetArraySize(arr);
  for (int32_t i = 0; i < n && b->topicNum < MS_MAX_TOPICS; ++i) {
    tools_cJSON *it = tools_cJSON_GetArrayItem(arr, i);
    if (it == NULL) continue;
    SMsTopic *t = &b->topics[b->topicNum];
    int32_t qos = 0;

    if (tools_cJSON_IsString(it) && it->valuestring != NULL) {
      /* Shorthand: "factory/+/telemetry::1" */
      char tmp[MS_TEXT_LEN];
      snprintf(tmp, sizeof(tmp), "%s", it->valuestring);
      char *sep = strstr(tmp, "::");
      if (sep != NULL) {
        *sep = '\0';
        qos = msStrToInt(sep + 2, 0);
      }
      snprintf(t->filter, sizeof(t->filter), "%s", tmp);
    } else if (tools_cJSON_IsObject(it)) {
      const char *filter = jstr(it, "filter", NULL);
      if (filter == NULL || *filter == '\0') {
        setErr(err, errLen, "topics[%d] has no filter", i);
        return -1;
      }
      snprintf(t->filter, sizeof(t->filter), "%s", filter);
      qos = jint(it, "qos", 0);
    } else {
      setErr(err, errLen, "topics[%d] must be a string or object", i);
      return -1;
    }

    if (t->filter[0] == '\0') {
      setErr(err, errLen, "topics[%d] has empty filter", i);
      return -1;
    }
    t->qos = (qos < 0 || qos > 2) ? 0 : qos;
    ++b->topicNum;
  }
  return 0;
}

static int32_t parseTopicVars(tools_cJSON *root, SMsBrokerCfg *b) {
  tools_cJSON *arr = jobj(root, "topic_vars");
  if (arr == NULL || !tools_cJSON_IsArray(arr)) return 0;
  int32_t n = tools_cJSON_GetArraySize(arr);
  for (int32_t i = 0; i < n && b->topicVarNum < MS_MAX_TVARS; ++i) {
    tools_cJSON *it = tools_cJSON_GetArrayItem(arr, i);
    if (it != NULL && tools_cJSON_IsString(it) && it->valuestring != NULL) {
      snprintf(b->topicVars[b->topicVarNum], MS_NAME_LEN, "%s", it->valuestring);
      ++b->topicVarNum;
    }
  }
  return 0;
}

static int32_t parseColumns(tools_cJSON *obj, SMsColumn *pool, int32_t cap,
                            int32_t *num, char *err, int32_t errLen) {
  tools_cJSON *arr = jobj(obj, "columns");
  if (arr == NULL || !tools_cJSON_IsArray(arr)) return 0;
  int32_t n = tools_cJSON_GetArraySize(arr);
  for (int32_t i = 0; i < n && *num < cap; ++i) {
    tools_cJSON *it = tools_cJSON_GetArrayItem(arr, i);
    if (it == NULL || !tools_cJSON_IsObject(it)) continue;
    const char *name = jstr(it, "name", NULL);
    const char *path = jstr(it, "path", NULL);
    if (name == NULL || path == NULL || *name == '\0' || *path == '\0') {
      setErr(err, errLen, "columns[%d] needs name and path", i);
      return -1;
    }
    SMsColumn *c = &pool[*num];
    snprintf(c->name, sizeof(c->name), "%s", name);
    snprintf(c->path, sizeof(c->path), "%s", path);
    const char *type = jstr(it, "type", "auto");
    c->type = (strcmp(type, "string") == 0)  ? MS_COL_STRING
              : (strcmp(type, "bool") == 0)  ? MS_COL_BOOL
              : (strcmp(type, "int") == 0 ||
                 strcmp(type, "bigint") == 0 ||
                 strcmp(type, "i64") == 0)   ? MS_COL_INT
              : (strcmp(type, "number") == 0 ||
                 strcmp(type, "double") == 0 ||
                 strcmp(type, "float") == 0)  ? MS_COL_NUMBER
                                               : MS_COL_AUTO;
    ++(*num);
  }
  return 0;
}

static int32_t parseTags(tools_cJSON *obj, SMsTag *pool, int32_t cap,
                         int32_t *num, char *err, int32_t errLen) {
  tools_cJSON *arr = jobj(obj, "tags");
  if (arr == NULL || !tools_cJSON_IsArray(arr)) return 0;
  int32_t n = tools_cJSON_GetArraySize(arr);
  for (int32_t i = 0; i < n && *num < cap; ++i) {
    tools_cJSON *it = tools_cJSON_GetArrayItem(arr, i);
    if (it == NULL || !tools_cJSON_IsObject(it)) continue;
    const char *name = jstr(it, "name", NULL);
    const char *value = jstr(it, "value", NULL);
    if (name == NULL || *name == '\0' || value == NULL) {
      setErr(err, errLen, "tags[%d] needs name and value", i);
      return -1;
    }
    snprintf(pool[*num].name, sizeof(pool[*num].name), "%s", name);
    snprintf(pool[*num].value, sizeof(pool[*num].value), "%s", value);
    ++(*num);
  }
  return 0;
}

static int32_t parsePrecision(const char *prec) {
  return (strcmp(prec, "s") == 0)    ? MS_TS_SECOND
         : (strcmp(prec, "us") == 0) ? MS_TS_MICRO
         : (strcmp(prec, "ns") == 0) ? MS_TS_NANO
                                     : MS_TS_MILLI;
}

static int32_t parseWhen(tools_cJSON *obj, SMsRule *rule, char *err, int32_t errLen) {
  tools_cJSON *cond = jobj(obj, "when");
  if (cond == NULL) return 0;
  int32_t count = tools_cJSON_IsArray(cond) ? tools_cJSON_GetArraySize(cond) : 1;
  for (int32_t i = 0; i < count && rule->whenNum < MS_MAX_WHEN; ++i) {
    tools_cJSON *it = tools_cJSON_IsArray(cond) ? tools_cJSON_GetArrayItem(cond, i) : cond;
    if (it == NULL || !tools_cJSON_IsObject(it)) continue;
    const char *path = jstr(it, "path", NULL);
    if (path == NULL || *path == '\0') {
      setErr(err, errLen, "when[%d] needs path", i);
      return -1;
    }
    SMsWhen *w = &rule->when[rule->whenNum];
    snprintf(w->path, sizeof(w->path), "%s", path);
    const char *op = jstr(it, "op", "eq");
    w->op = (strcmp(op, "ne") == 0)       ? MS_WHEN_NE
            : (strcmp(op, "prefix") == 0) ? MS_WHEN_PREFIX
            : (strcmp(op, "contains") == 0) ? MS_WHEN_CONTAINS
            : (strcmp(op, "in") == 0)     ? MS_WHEN_IN
            : (strcmp(op, "exists") == 0) ? MS_WHEN_EXISTS
                                          : MS_WHEN_EQ;
    snprintf(w->value, sizeof(w->value), "%s", jstr(it, "value", ""));
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
  if (arr == NULL || !tools_cJSON_IsArray(arr)) return 0;
  int32_t n = tools_cJSON_GetArraySize(arr);
  for (int32_t i = 0; i < n && m->ruleNum < MS_MAX_RULES; ++i) {
    tools_cJSON *it = tools_cJSON_GetArrayItem(arr, i);
    if (it == NULL || !tools_cJSON_IsObject(it)) continue;
    SMsRule *r = &m->rules[m->ruleNum];

    snprintf(r->name, sizeof(r->name), "%s", jstr(it, "name", "rule"));
    msStrCopy(r->stable, sizeof(r->stable), jstr(it, "stable", m->stable));
    msStrCopy(r->table, sizeof(r->table), jstr(it, "table", m->table));
    msStrCopy(r->tbnameKey, sizeof(r->tbnameKey), jstr(it, "tbname_key", m->tbnameKey));
    msStrCopy(r->tsField, sizeof(r->tsField), jstr(it, "ts_field", m->tsField));
    r->tsPrecision = parsePrecision(jstr(it, "ts_precision", "ms"));
    if (r->tsPrecision == MS_TS_MILLI && jobj(it, "ts_precision") == NULL) {
      r->tsPrecision = m->tsPrecision;
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

  SMsBrokerCfg *b = &cfg->broker;
  SMsSinkCfg *s = &cfg->sink;
  SMsMappingCfg *m = &cfg->mapping;

  tools_cJSON *jb = jobj(root, "mqtt");
  if (jb != NULL) {
    msStrCopy(b->host, sizeof(b->host), jstr(jb, "host", b->host));
    b->port = (uint16_t)jint(jb, "port", b->port);
    msStrCopy(b->clientId, sizeof(b->clientId), jstr(jb, "client_id", b->clientId));
    msStrCopy(b->username, sizeof(b->username), jstr(jb, "username", ""));
    msStrCopy(b->password, sizeof(b->password), jstr(jb, "password", ""));
    b->keepalive = jint(jb, "keepalive", b->keepalive);
    b->cleanSession = jbool(jb, "clean_session", b->cleanSession);
    b->reconnectMs = jint(jb, "reconnect_ms", b->reconnectMs);
    b->maxReconnectMs = jint(jb, "max_reconnect_ms", b->maxReconnectMs);
    b->connectTimeoutMs = jint(jb, "connect_timeout_ms", b->connectTimeoutMs);
    if (parseTopics(jb, b, err, errLen) != 0 || parseTopicVars(jb, b) != 0) {
      tools_cJSON_Delete(root);
      return -1;
    }
  }

  tools_cJSON *js = jobj(root, "tdengine");
  if (js != NULL) {
    msStrCopy(s->host, sizeof(s->host), jstr(js, "host", s->host));
    s->port = (uint16_t)jint(js, "port", s->port);
    msStrCopy(s->user, sizeof(s->user), jstr(js, "user", s->user));
    msStrCopy(s->password, sizeof(s->password), jstr(js, "password", s->password));
    msStrCopy(s->db, sizeof(s->db), jstr(js, "database", s->db));
    s->createDb = jbool(js, "create_db", s->createDb);
    s->batchSize = jint(js, "batch_size", s->batchSize);
    s->flushMs = jint(js, "flush_ms", s->flushMs);
    s->maxRetries = jint(js, "max_retries", s->maxRetries);
    s->retryMs = jint(js, "retry_ms", s->retryMs);
    s->ttl = (int32_t)jint(js, "ttl", s->ttl);
    s->dryRun = jbool(js, "dry_run", s->dryRun);
  }

  tools_cJSON *jm = jobj(root, "mapping");
  if (jm != NULL) {
    msStrCopy(m->stable, sizeof(m->stable), jstr(jm, "stable", m->stable));
    msStrCopy(m->table, sizeof(m->table), jstr(jm, "table", m->table));
    msStrCopy(m->tbnameKey, sizeof(m->tbnameKey), jstr(jm, "tbname_key", m->tbnameKey));
    msStrCopy(m->tsField, sizeof(m->tsField), jstr(jm, "ts_field", ""));
    m->tsPrecision = parsePrecision(jstr(jm, "ts_precision", "ms"));
    if (parseColumns(jm, m->columns, MS_MAX_COLUMNS, &m->columnNum, err, errLen) != 0 ||
        parseTags(jm, m->tags, MS_MAX_TAGS, &m->tagNum, err, errLen) != 0) {
      tools_cJSON_Delete(root);
      return -1;
    }
    int32_t poolCols = m->columnNum;
    int32_t poolTags = m->tagNum;
    if (parseRules(jm, m, &poolCols, &poolTags, err, errLen) != 0) {
      tools_cJSON_Delete(root);
      return -1;
    }
  }

  tools_cJSON_Delete(root);
  return msValidateConfig(cfg, err, errLen);
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
  if (b->topicNum <= 0) {
    setErr(err, errLen, "mqtt.topics must contain at least one topic filter");
    return -1;
  }
  if (b->keepalive <= 0 || b->keepalive > 65535 || b->reconnectMs <= 0 ||
      b->maxReconnectMs < b->reconnectMs) {
    setErr(err, errLen, "invalid keepalive / reconnect settings");
    return -1;
  }
  for (int32_t i = 0; i < b->topicNum; ++i) {
    if (b->topics[i].filter[0] == '\0') {
      setErr(err, errLen, "mqtt.topics[%d] has empty filter", i);
      return -1;
    }
  }
  if (s->host[0] == '\0' || s->db[0] == '\0' || s->batchSize <= 0 || s->flushMs <= 0) {
    setErr(err, errLen, "tdengine.host / database / batch_size / flush_ms invalid");
    return -1;
  }
  /* db is interpolated into CREATE DATABASE; only allow safe identifiers. */
  for (const char *p = s->db; *p; ++p) {
    bool ok = (*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') ||
              (*p >= '0' && *p <= '9') || *p == '_';
    if (!ok) {
      setErr(err, errLen, "tdengine.database must be [A-Za-z0-9_]");
      return -1;
    }
  }
  if (m->stable[0] == '\0' || m->table[0] == '\0' || m->tbnameKey[0] == '\0') {
    setErr(err, errLen, "mapping.stable / table / tbname_key must be set");
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
  }
  return 0;
}
