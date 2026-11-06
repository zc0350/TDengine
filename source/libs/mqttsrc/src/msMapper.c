/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * JSON payload -> TDengine schemaless (InfluxDB line protocol) rows.
 *
 * One line looks like:
 *   meters,tbname=t_dev1,site=bj current=12i64,voltage=220.5,ok=true 1730000000000
 *
 * "tbname" is passed to taos_schemaless_insert_*_tbname_key() so the
 * rendered child-table template actually controls the subtable name.
 */

#include "msInt.h"
#include "toolscJson.h"

#include <time.h>

/* Effective table model for one record. Points into the config, never copies
 * the (large) SMsMappingCfg. */
typedef struct {
  const char     *stable;
  const char     *table;
  const char     *tbnameKey;
  const char     *tsField;
  int             tsPrecision;
  const SMsColumn *columns;
  int32_t         colNum;
  const SMsTag   *tags;
  int32_t         tagNum;
} SMsSpec;

typedef struct {
  const SMsCfg   *cfg;
  const SMsSpec  *spec;
  tools_cJSON    *rec;
  const char     *topic;
  char            topicFlat[MS_TEXT_LEN];
  char            varName[MS_MAX_TVARS][MS_NAME_LEN];
  char            varVal[MS_MAX_TVARS][MS_TEXT_LEN];
  int32_t         varNum;
  int64_t         recvMs;
  int64_t         tsMs;        /* final row timestamp, milliseconds */
  int             year, month, day;      /* derived from tsMs, local time */
  int             serverYear;            /* derived from recvMs, local time */
  const char     *message;     /* raw MQTT payload bytes */
  int32_t         messageLen;
} SMsCtx;

/* Special column paths: write JSON text into a VARCHAR column.
 * "$payload" = the current record (one row of a JSON array).
 * "$message" = the whole MQTT payload bytes, verbatim. */
#define MS_PATH_PAYLOAD "$payload"
#define MS_PATH_MESSAGE "$message"

static void setErr(char *err, int32_t errLen, const char *fmt, ...) {
  if (err == NULL || errLen <= 0) return;
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(err, (size_t)errLen, fmt, ap);
  va_end(ap);
}

/* Local-time calendar parts. Used by the {_year}/{_month}/{_day} and
 * {_server_year} template variables. */
static void breakTime(int64_t ms, int *year, int *month, int *day) {
  time_t sec = (time_t)(ms / 1000);
  struct tm tv;
  memset(&tv, 0, sizeof(tv));
#ifdef _WIN32
  if (localtime_s(&tv, &sec) != 0) {
#else
  if (localtime_r(&sec, &tv) == NULL) {
#endif
    *year = 1970;
    *month = 1;
    *day = 1;
    return;
  }
  *year = tv.tm_year + 1900;
  *month = tv.tm_mon + 1;
  *day = tv.tm_mday;
}

static void buildTopicVars(SMsCtx *ctx) {
  const SMsBrokerCfg *b = &ctx->cfg->broker;
  ctx->varNum = 0;
  {
    int y = 1970, m = 1, d = 1;
    breakTime(ctx->recvMs, &y, &m, &d);
    ctx->serverYear = y;
    /* Defaults before the record timestamp is resolved. A "when" condition
     * on {_year} therefore sees the receive year; templates see the row ts. */
    ctx->year = y;
    ctx->month = m;
    ctx->day = d;
    ctx->tsMs = ctx->recvMs;
  }

  snprintf(ctx->topicFlat, sizeof(ctx->topicFlat), "%s", ctx->topic ? ctx->topic : "");
  for (char *p = ctx->topicFlat; *p; ++p) {
    if (*p == '/' || *p == '+' || *p == '#' || *p == ' ' || *p == '.' || *p == ':') *p = '_';
  }

  if (ctx->topic == NULL) return;
  const char *p = ctx->topic;
  int32_t level = 0;
  while (*p && ctx->varNum < MS_MAX_TVARS) {
    const char *slash = strchr(p, '/');
    int32_t len = slash ? (int32_t)(slash - p) : (int32_t)strlen(p);
    if (level < b->topicVarNum && b->topicVars[level][0] != '_') {
      snprintf(ctx->varName[ctx->varNum], MS_NAME_LEN, "%s", b->topicVars[level]);
      if (len >= MS_TEXT_LEN) len = MS_TEXT_LEN - 1;
      memcpy(ctx->varVal[ctx->varNum], p, (size_t)len);
      ctx->varVal[ctx->varNum][len] = '\0';
      ++ctx->varNum;
    }
    ++level;
    if (slash == NULL) break;
    p = slash + 1;
  }
}

static int32_t lookup(void *arg, const char *key, char *buf, int32_t bufLen) {
  SMsCtx *ctx = (SMsCtx *)arg;
  if (strcmp(key, "_topic") == 0) {
    snprintf(buf, (size_t)bufLen, "%s", ctx->topic ? ctx->topic : "");
    return 0;
  }
  if (strcmp(key, "_topic_flat") == 0) {
    snprintf(buf, (size_t)bufLen, "%s", ctx->topicFlat);
    return 0;
  }
  if (strcmp(key, "_now") == 0 || strcmp(key, "_ts") == 0) {
    snprintf(buf, (size_t)bufLen, "%lld", (long long)ctx->recvMs);
    return 0;
  }
  /* Calendar parts of the final row timestamp (tsField or receive time). */
  if (strcmp(key, "_year") == 0) {
    snprintf(buf, (size_t)bufLen, "%d", ctx->year);
    return 0;
  }
  if (strcmp(key, "_month") == 0) {
    snprintf(buf, (size_t)bufLen, "%02d", ctx->month);
    return 0;
  }
  if (strcmp(key, "_day") == 0) {
    snprintf(buf, (size_t)bufLen, "%02d", ctx->day);
    return 0;
  }
  /* Calendar year of the receiving server, independent of device clocks. */
  if (strcmp(key, "_server_year") == 0) {
    snprintf(buf, (size_t)bufLen, "%d", ctx->serverYear);
    return 0;
  }
  for (int32_t i = 0; i < ctx->varNum; ++i) {
    if (strcmp(ctx->varName[i], key) == 0) {
      snprintf(buf, (size_t)bufLen, "%s", ctx->varVal[i]);
      return 0;
    }
  }
  SMsValue v;
  if (msJsonPath(ctx->rec, key, &v) != 0 || v.type == MSV_NULL) return -1;
  snprintf(buf, (size_t)bufLen, "%s", v.text);
  return 0;
}

static bool inList(const char *list, const char *val) {
  const char *p = list;
  while (*p) {
    const char *comma = strchr(p, ',');
    int32_t len = comma ? (int32_t)(comma - p) : (int32_t)strlen(p);
    while (len > 0 && p[len - 1] == ' ') --len;
    while (*p == ' ') {
      ++p;
      --len;
    }
    if (len > 0 && len == (int32_t)strlen(val) && strncmp(p, val, (size_t)len) == 0) return true;
    if (comma == NULL) break;
    p = comma + 1;
  }
  return false;
}

static bool whenMatch(const SMsWhen *w, SMsCtx *ctx) {
  char val[MS_TEXT_LEN];
  bool exists = (lookup(ctx, w->path, val, sizeof(val)) == 0);
  if (w->op == MS_WHEN_EXISTS) return exists;
  if (!exists) return false;
  switch (w->op) {
    case MS_WHEN_NE:       return strcmp(val, w->value) != 0;
    case MS_WHEN_PREFIX:   return strncmp(val, w->value, strlen(w->value)) == 0;
    case MS_WHEN_CONTAINS: return strstr(val, w->value) != NULL;
    case MS_WHEN_IN:       return inList(w->value, val);
    default:               return strcmp(val, w->value) == 0;
  }
}

static const SMsRule *selectRule(SMsCtx *ctx) {
  const SMsMappingCfg *m = &ctx->cfg->mapping;
  for (int32_t i = 0; i < m->ruleNum; ++i) {
    const SMsRule *r = &m->rules[i];
    bool matched = true;
    for (int32_t j = 0; j < r->whenNum; ++j) {
      if (!whenMatch(&r->when[j], ctx)) {
        matched = false;
        break;
      }
    }
    if (matched) return r; /* rule with no when acts as catch-all */
  }
  return NULL;
}

static void buildSpec(SMsSpec *spec, const SMsCfg *cfg, const SMsRule *rule) {
  const SMsMappingCfg *m = &cfg->mapping;
  if (rule == NULL) {
    spec->stable = m->stable;
    spec->table = m->table;
    spec->tbnameKey = m->tbnameKey;
    spec->tsField = m->tsField;
    spec->tsPrecision = m->tsPrecision;
    spec->columns = m->columns;
    spec->colNum = m->columnNum;
    spec->tags = m->tags;
    spec->tagNum = m->tagNum;
    return;
  }
  spec->stable = rule->stable;
  spec->table = rule->table;
  spec->tbnameKey = rule->tbnameKey;
  spec->tsField = rule->tsField;
  spec->tsPrecision = rule->tsPrecision;
  spec->columns = &m->columns[rule->colStart];
  spec->colNum = rule->colNum;
  spec->tags = &m->tags[rule->tagStart];
  spec->tagNum = rule->tagNum;
}

static int64_t toMilli(int64_t v, int32_t precision) {
  switch (precision) {
    case MS_TS_SECOND: return v * 1000;
    case MS_TS_MILLI:  return v;
    case MS_TS_MICRO:  return v / 1000;
    case MS_TS_NANO:   return v / 1000000;
    default:           return v;
  }
}

static int32_t renderTs(const SMsCtx *ctx, int64_t *ts, char *err, int32_t errLen) {
  const SMsSpec *spec = ctx->spec;
  if (spec->tsField[0] == '\0') {
    *ts = ctx->recvMs;
    return 0;
  }
  SMsValue v;
  if (msJsonPath(ctx->rec, spec->tsField, &v) != 0 || v.type == MSV_NULL) {
    setErr(err, errLen, "timestamp field '%s' missing", spec->tsField);
    return -1;
  }
  if (v.type == MSV_STRING && strcmp(v.text, "now") == 0) {
    *ts = ctx->recvMs;
    return 0;
  }
  if (v.type != MSV_INT && v.type != MSV_DOUBLE && v.type != MSV_STRING) {
    setErr(err, errLen, "timestamp field '%s' is not numeric", spec->tsField);
    return -1;
  }
  char *end = NULL;
  long long raw = strtoll(v.text, &end, 10);
  if (end == v.text) {
    setErr(err, errLen, "timestamp field '%s' is not an integer: %s", spec->tsField, v.text);
    return -1;
  }
  *ts = toMilli((int64_t)raw, spec->tsPrecision);
  return 0;
}

/* Append one column value in line-protocol syntax. */
static int32_t appendValue(SStr *line, const SMsColumn *col, const SMsValue *v,
                           char *err, int32_t errLen) {
  int32_t type = col->type;
  if (type == MS_COL_AUTO) {
    switch (v->type) {
      case MSV_INT:    /* plain numeric => DOUBLE, keeps int/float type-stable */
      case MSV_DOUBLE: type = MS_COL_NUMBER; break;
      case MSV_BOOL:   type = MS_COL_BOOL;   break;
      case MSV_NULL:   return 0;             /* omit -> TDengine stores NULL */
      default:         type = MS_COL_STRING; break;
    }
  }

  if (type == MS_COL_NUMBER || type == MS_COL_INT) {
    if (v->type == MSV_STRING) {
      char *end = NULL;
      strtod(v->text, &end);
      if (end == v->text) {
        setErr(err, errLen, "column '%s' expects a number, got '%s'", col->name, v->text);
        return -1;
      }
    } else if (v->type != MSV_INT && v->type != MSV_DOUBLE) {
      setErr(err, errLen, "column '%s' expects a number", col->name);
      return -1;
    }
    if (type == MS_COL_INT) {
      if (strchr(v->text, '.') != NULL || strchr(v->text, 'e') != NULL ||
          strchr(v->text, 'E') != NULL) {
        setErr(err, errLen, "column '%s' expects an integer, got '%s'", col->name, v->text);
        return -1;
      }
      return ssPrintf(line, "%si64", v->text);
    }
    return ssPrintf(line, "%s", v->text);
  }

  if (type == MS_COL_BOOL) {
    if (v->type == MSV_BOOL) {
      if (ssPutStr(line, v->text) != 0) return -1;
    } else if (v->type == MSV_INT || v->type == MSV_DOUBLE) {
      if (ssPutStr(line, strcmp(v->text, "0") == 0 ? "false" : "true") != 0) return -1;
    } else if (v->type == MSV_STRING) {
      bool t = (strcmp(v->text, "true") == 0 || strcmp(v->text, "1") == 0 ||
                strcmp(v->text, "T") == 0);
      if (ssPutStr(line, t ? "true" : "false") != 0) return -1;
    } else {
      setErr(err, errLen, "column '%s' expects a bool", col->name);
      return -1;
    }
    return 0;
  }

  /* string column */
  if (ssPutChar(line, '"') != 0) return -1;
  if (ssPutEscaped(line, v->text, 2) != 0) return -1;
  if (ssPutChar(line, '"') != 0) return -1;
  return 0;
}

static int32_t mapRecord(SMsCtx *ctx, SStr *out, char *err, int32_t errLen) {
  const SMsSpec *spec = ctx->spec;
  SStr line;
  char *pendingRaw = NULL;
  if (ssInit(&line, 1024) != 0) return -1;

  /* Resolve the timestamp first: {_year}/{_month}/{_day} in stable/table/tag
   * templates are derived from it, so it must be known before rendering. */
  int64_t ts = 0;
  if (renderTs(ctx, &ts, err, errLen) != 0) {
    ssFree(&line);
    return -1;
  }
  ctx->tsMs = ts;
  {
    int y = 1970, m = 1, d = 1;
    breakTime(ts, &y, &m, &d);
    ctx->year = y;
    ctx->month = m;
    ctx->day = d;
  }

  /* measurement */
  if (msRender(spec->stable, lookup, ctx, &line) != 0) {
    setErr(err, errLen, "cannot render stable template '%s'", spec->stable);
    ssFree(&line);
    return -1;
  }

  /* child table name, injected as a tag so tbnameKey can pick it up */
  SStr tableName;
  if (ssInit(&tableName, 128) != 0) {
    ssFree(&line);
    return -1;
  }
  if (msRender(spec->table, lookup, ctx, &tableName) != 0) {
    setErr(err, errLen, "cannot render table template '%s'", spec->table);
    ssFree(&tableName);
    ssFree(&line);
    return -1;
  }
  char tableNameZ[MS_NAME_LEN];
  snprintf(tableNameZ, sizeof(tableNameZ), "%s", tableName.data ? tableName.data : "");
  ssFree(&tableName);
  if (tableNameZ[0] == '\0') {
    setErr(err, errLen, "rendered table name is empty");
    ssFree(&line);
    return -1;
  }

  bool tbTagDone = false;
  for (int32_t i = 0; i < spec->tagNum; ++i) {
    const SMsTag *t = &spec->tags[i];
    if (ssPutChar(&line, ',') != 0 || ssPutEscaped(&line, t->name, 1) != 0 ||
        ssPutChar(&line, '=') != 0) goto fail;
    SStr v;
    if (ssInit(&v, 128) != 0) goto fail;
    if (msRender(t->value, lookup, ctx, &v) != 0) {
      ssFree(&v);
      setErr(err, errLen, "cannot render tag '%s' template '%s'", t->name, t->value);
      goto fail;
    }
    if (ssPutEscaped(&line, v.data ? v.data : "", 1) != 0) {
      ssFree(&v);
      goto fail;
    }
    ssFree(&v);
    if (strcmp(t->name, spec->tbnameKey) == 0) tbTagDone = true;
  }
  if (!tbTagDone) {
    if (ssPutChar(&line, ',') != 0 || ssPutEscaped(&line, spec->tbnameKey, 1) != 0 ||
        ssPutChar(&line, '=') != 0 || ssPutEscaped(&line, tableNameZ, 1) != 0) goto fail;
  }

  /* field set */
  if (ssPutChar(&line, ' ') != 0) goto fail;
  int32_t written = 0;
  for (int32_t i = 0; i < spec->colNum; ++i) {
    const SMsColumn *c = &spec->columns[i];
    SMsValue v;
    char *raw = NULL;

    if (strcmp(c->path, MS_PATH_PAYLOAD) == 0) {
      raw = tools_cJSON_PrintUnformatted(ctx->rec);
    } else if (strcmp(c->path, MS_PATH_MESSAGE) == 0) {
      raw = (char *)malloc((size_t)ctx->messageLen + 1);
      if (raw != NULL) {
        memcpy(raw, ctx->message, (size_t)ctx->messageLen);
        raw[ctx->messageLen] = '\0';
      }
    } else if (msJsonPath(ctx->rec, c->path, &v) != 0 || v.type == MSV_NULL) {
      if (c->type != MS_COL_AUTO) {
        setErr(err, errLen, "column '%s': JSON path '%s' missing", c->name, c->path);
        goto fail;
      }
      continue; /* AUTO + missing => omit, TDengine fills NULL */
    }
    if (strcmp(c->path, MS_PATH_PAYLOAD) == 0 || strcmp(c->path, MS_PATH_MESSAGE) == 0) {
      if (raw == NULL) goto fail;
      pendingRaw = raw;
    }

    int32_t mark = line.len;
    if (written > 0 && ssPutChar(&line, ',') != 0) goto fail;
    if (ssPutEscaped(&line, c->name, 1) != 0 || ssPutChar(&line, '=') != 0) goto fail;

    if (pendingRaw != NULL) {
      /* Escaping follows the line-protocol column-value rule: only " and \. */
      int32_t ok = (ssPutChar(&line, '"') == 0 &&
                    ssPutEscaped(&line, pendingRaw, 2) == 0 &&
                    ssPutChar(&line, '"') == 0);
      free(pendingRaw);
      pendingRaw = NULL;
      if (!ok) goto fail;
      ++written;
      continue;
    }

    int32_t before = line.len;
    if (appendValue(&line, c, &v, err, errLen) != 0) goto fail;
    if (line.len == before) {
      /* AUTO + JSON null produced nothing; roll back separator and name */
      line.len = mark;
      line.data[line.len] = '\0';
      continue;
    }
    ++written;
  }
  if (written == 0) {
    setErr(err, errLen, "no column values in record");
    goto fail;
  }

  if (ssPrintf(&line, " %lld", (long long)ctx->tsMs) != 0) goto fail;

  if (ssPut(out, line.data, line.len) != 0 || ssPutChar(out, '\0') != 0) {
    ssFree(&line);
    return -1;
  }
  ssFree(&line);
  return 0;

fail:
  free(pendingRaw);
  ssFree(&line);
  return -1;
}

int32_t msMapPayload(const SMsCfg *cfg, const char *payload, int32_t len,
                     const char *topic, int64_t recvMs, char **out,
                     int32_t *outNum, char *err, int32_t errLen) {
  if (err != NULL && errLen > 0) err[0] = '\0';
  if (out == NULL || outNum == NULL || cfg == NULL || payload == NULL) {
    setErr(err, errLen, "invalid map arguments");
    return -1;
  }
  *out = NULL;
  *outNum = 0;
  if (len <= 0) {
    setErr(err, errLen, "empty payload");
    return -1;
  }

  char *text = (char *)malloc((size_t)len + 1);
  if (text == NULL) return -1;
  memcpy(text, payload, (size_t)len);
  text[len] = '\0';

  tools_cJSON *root = tools_cJSON_Parse(text);
  free(text);
  if (root == NULL) {
    setErr(err, errLen, "payload is not valid JSON");
    return -1;
  }

  SStr buf;
  if (ssInit(&buf, 4096) != 0) {
    tools_cJSON_Delete(root);
    return -1;
  }

  SMsCtx ctx;
  memset(&ctx, 0, sizeof(ctx));
  ctx.cfg = cfg;
  ctx.topic = topic ? topic : "";
  ctx.recvMs = recvMs;
  ctx.message = payload;
  ctx.messageLen = len;
  buildTopicVars(&ctx);

  int32_t count = tools_cJSON_IsArray(root) ? tools_cJSON_GetArraySize(root) : 1;
  int32_t ok = 0;
  for (int32_t i = 0; i < count; ++i) {
    ctx.rec = tools_cJSON_IsArray(root) ? tools_cJSON_GetArrayItem(root, i) : root;
    if (ctx.rec == NULL || !tools_cJSON_IsObject(ctx.rec)) {
      setErr(err, errLen, "record[%d] is not a JSON object", i);
      continue;
    }
    const SMsRule *rule = selectRule(&ctx);
    if (rule == NULL && cfg->mapping.columnNum <= 0) {
      setErr(err, errLen, "record[%d] matched no rule and no default model exists", i);
      continue;
    }
    SMsSpec spec;
    buildSpec(&spec, cfg, rule);
    ctx.spec = &spec;
    if (mapRecord(&ctx, &buf, err, errLen) != 0) continue;
    ++ok;
  }

  tools_cJSON_Delete(root);
  if (ok == 0) {
    ssFree(&buf);
    if (err != NULL && errLen > 0 && err[0] == '\0') setErr(err, errLen, "no mappable record");
    return -1;
  }
  *out = ssDetach(&buf);
  if (*out == NULL) return -1;
  *outNum = ok;
  return 0;
}
