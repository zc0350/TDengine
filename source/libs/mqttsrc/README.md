# mqttsrc — MQTT → TDengine ingestion

`taosmqttsrc` subscribes to an external MQTT broker, maps JSON payloads to
TDengine line protocol with templates and writes them with the schemaless API.
It is the ingress counterpart of the built-in `tmqtt` broker.

```
broker ──subscribe──> taosmqttsrc ──schemaless──> taosd
                        │
                        ├── MQTT thread: parse + map + enqueue
                        └── writer thread: batch + retry + ACK
```

## Build

The repository's own wrapper keeps the CMake option list in step with the rest
of the tree:

```bash
TD_CONFIG=Release ./build.sh gen -DMQTTSRC_ENABLE_TLS=ON
TD_CONFIG=Release ./build.sh bld --target taosmqttsrc msUnitTest msSinkTest taosmqttsrcDryRun
ctest --test-dir debug -R '^ms' --output-on-failure
```

or configure CMake directly:

```bash
cmake -S . -B build -DBUILD_TEST=true -DMQTTSRC_ENABLE_TLS=ON
cmake --build build --target taosmqttsrc -j
```

`MQTTSRC_ENABLE_TLS=ON` requires OpenSSL. A build without it **rejects**
`mqtt.tls.enabled` at config load time — it never falls back to clear text.

Pushing a `v*` tag runs `.github/workflows/mqttsrc-release.yml`, which builds
and tests this component and publishes a relocatable Linux x64 bundle (binary,
`libtaos*`, the example configs, this README) on the GitHub release. The bundle
carries every dependency except the glibc core, so the target machine only needs
a glibc as new as the build image's (2.35 for `ubuntu-22.04`).

## Run

```bash
taosmqttsrc -c mqttsrc.example.json            # start
taosmqttsrc -c mqttsrc.example.json --check    # validate only
taosmqttsrc -c mqttsrc.example.json -n         # dry-run: print line protocol
taosmqttsrc -c mqttsrc.example.json -v         # debug logs
```

`--exit-after-ms <n>` stops gracefully after n milliseconds (used by the e2e
tests). `SIGINT`/`SIGTERM` (Ctrl-C / Ctrl-Break on Windows) also stop it.

Exit code is non-zero when any batch was dropped after its retries, so a
supervisor can notice lost rows.

## Delivery semantics

| Item | Behaviour |
|---|---|
| QoS 0 | Fire and forget; rows are batched, a failed batch is logged and counted. |
| QoS 1 | PUBACK is sent **after** the batch containing the message is accepted by TDengine. A sink failure leaves it unacknowledged, the client reconnects, the broker redelivers. At-least-once: a redelivery duplicates rows. |
| QoS 2 | PUBREC is deferred the same way. Persisted ids are remembered across a resumed session, so a redelivery after a reconnect is acknowledged without inserting twice (exactly-once within the queue window). |
| `clean_session` | Default `false`. With `true` the broker discards unacknowledged messages on disconnect, which turns QoS 1 into at-most-once. Only enable it when losing in-flight messages is acceptable. |
| Batch | QoS 1/2 messages are grouped up to `batch_size` rows or `flush_ms`, then written once and acknowledged together. |
| Backpressure | At most `queue_size` messages wait for the writer. A full queue blocks the reader; after 5 s the client disconnects without ACK so the broker keeps the data. |
| Memory bound | Rows are rejected when a single JSON value exceeds `MS_VALUE_LEN` (1024 bytes); nothing is silently truncated. |

`client_id` defaults to `taosx-mqttsrc-<hostname>-<pid>` so two instances on
one broker cannot evict each other. Set an explicit stable id if the broker
must keep the session across process restarts.

## Configuration

```jsonc
{
  "stats_interval_ms": 60000,        // periodic stats line, 0 disables
  "mqtt": {
    "host": "127.0.0.1",
    "port": 1883,                    // 1..65535
    "client_id": "taosx-mqttsrc-demo",
    "username": "",
    "password": "",                  // requires username
    "keepalive": 60,                 // seconds
    "clean_session": false,
    "reconnect_ms": 1000,
    "max_reconnect_ms": 30000,
    "connect_timeout_ms": 5000,
    "tls": {
      "enabled": false,
      "ca_file": "",                 // PEM bundle; empty = system trust store
      "cert_file": "",               // client certificate (mTLS)
      "key_file": "",
      "insecure_skip_verify": false, // never in production
      "server_name": ""              // SNI / verification name, default host
    },
    "topics": [
      { "filter": "factory/+/telemetry", "qos": 1 }
    ],
    "topic_vars": ["_", "site", "_"] // one name per topic level, "_" ignores
  },
  "tdengine": {
    "host": "127.0.0.1",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "database": "mqtt_demo",
    "create_db": true,
    "batch_size": 1000,
    "flush_ms": 200,
    "max_retries": 3,
    "retry_ms": 500,
    "ttl": 0,
    "queue_size": 10000,
    "dry_run": false
  },
  "mapping": {
    "stable": "meters",
    "table": "t_{dev}_{site}",
    "tbname_key": "tbname",
    "ts_field": "ts",
    "ts_precision": "ms",
    "columns": [ { "name": "current", "path": "current", "type": "number" } ],
    "tags":    [ { "name": "device",  "value": "{dev}" } ],
    "rules":   []
  }
}
```

Unknown keys are **rejected**, not ignored: a typo such as `"column"` instead
of `"columns"` used to fall back to the default model and run with a different
schema. `_comment` keys are allowed anywhere.

### Templates

`stable`, `table` and tag values are templates. `{path}` resolves a JSON path,
a topic variable, or a builtin:

`_topic`, `_topic_flat`, `_now`, `_ts`, `_year`, `_month`, `_day`,
`_server_year`.

Templates are validated at startup: unbalanced `{}`, an empty `{}`, invalid
key characters and unknown `_`-prefixed builtins fail `--check`.

### Types

| `type` | Output |
|---|---|
| `auto` (default) | JSON number → DOUBLE, bool → BOOL, string → VARCHAR, null → omitted |
| `number` | always DOUBLE (type-stable across int/float payloads) |
| `int` | BIGINT (`i64` suffix) |
| `bool` | BOOL |
| `string` | VARCHAR |

`path` may be `$payload` (the current record as JSON) or `$message` (the raw
MQTT payload, arrays included).

### Rules

`mapping.rules[]` route a record to its own table model; conditions are ANDed
(`eq`, `ne`, `prefix`, `contains`, `in`, `exists`). A rule without `when` is a
catch-all. `tbname_key` must be identical across rules because one schemaless
batch carries a single key.

## TLS

```json
"tls": { "enabled": true, "ca_file": "/etc/ssl/certs/broker-ca.pem" }
```

The server certificate and host name are verified (`server_name`, default
`mqtt.host`). `insecure_skip_verify` exists for lab setups only. Without
`ca_file` the OpenSSL system trust store is used. Client certificates are
optional (`cert_file` + `key_file`, both or neither).

## Observability

Every `stats_interval_ms` a line like

```
stats: received=... parsed=... lines=... written=... failed=... parseErrors=... reconnects=... queue=3/10000 flushes=...
```

is written to stderr. A growing `parseErrors` counter is the signal that a
producer changed its payload schema. At shutdown a final summary includes
`maxQueue`.

## Tests

```bash
ctest --test-dir build -R '^ms' --output-on-failure
```

* `msUnitTest` — config validation, templates, mapping, escaping, limits.
* `msSinkTest` — retry/backoff and reqid de-duplication with a stub driver.
* `msE2E*` — scripted MQTT broker (QoS 0/1/2 handshakes, PING, abrupt
  disconnect + DUP redelivery) with exact row-set assertions.
