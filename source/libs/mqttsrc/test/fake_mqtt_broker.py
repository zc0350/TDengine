#!/usr/bin/env python3
# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
# SPDX-License-Identifier: AGPL-3.0-or-later
"""
End-to-end test for taosmqttsrc in dry-run mode.

Implements the server side of MQTT 3.1.1 (CONNECT/SUBSCRIBE/PUBLISH/PUBACK),
publishes two JSON records and verifies the line protocol printed by the
client. No real MQTT broker and no TDengine server are needed.

Usage: python fake_mqtt_broker.py <taosmqttsrcDryRun.exe> <config.json> [scenario.json]

scenario.json: {"topic": "...", "payloads": [{...}], "qos": [0,1],
                "expected": ["<line protocol row>", ...]}
Without a scenario the built-in single-rule case is used.
"""

import datetime
import json
import os
import socket
import struct
import subprocess
import sys
import tempfile
import time


def enc_remaining(n):
    out = bytearray()
    while True:
        b = n % 128
        n //= 128
        if n > 0:
            b |= 0x80
        out.append(b)
        if n == 0:
            return bytes(out)


def packet(header, body):
    return bytes([header]) + enc_remaining(len(body)) + body


def read_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed")
        buf += chunk
    return buf


def read_packet(sock):
    header = read_exact(sock, 1)[0]
    multiplier, value, done = 1, 0, False
    while not done:
        b = read_exact(sock, 1)[0]
        value += (b & 0x7F) * multiplier
        multiplier *= 128
        done = (b & 0x80) == 0
    return header, read_exact(sock, value)


def publish(sock, topic, payload, qos=0, packet_id=1):
    body = struct.pack("!H", len(topic)) + topic.encode()
    if qos > 0:
        body += struct.pack("!H", packet_id)
    body += payload.encode()
    sock.sendall(packet(0x30 | (qos << 1), body))


def default_scenario():
    records = [
        {"ts": 1730000000000, "dev": "d1", "current": 12,
         "voltage": 220.5, "running": True, "note": "ok"},
        {"ts": 1730000000001, "dev": "d2", "current": 13,
         "voltage": 221, "running": False, "note": "hi"},
    ]
    expected = [
        "meters,device=d1,site=bj,tbname=t_d1_bj "
        "current=12,voltage=220.5,running=true,note=\"ok\" 1730000000000",
        "meters,device=d2,site=bj,tbname=t_d2_bj "
        "current=13,voltage=221,running=false,note=\"hi\" 1730000000001",
    ]
    return {"payloads": records, "expected": expected, "qos": [0, 1]}


def main():
    if len(sys.argv) not in (3, 4):
        print(__doc__)
        return 2
    exe, example = sys.argv[1], sys.argv[2]
    with open(example, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    if len(sys.argv) == 4:
        with open(sys.argv[3], "r", encoding="utf-8") as f:
            scenario = json.load(f)
    else:
        scenario = default_scenario()

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    port = srv.getsockname()[1]
    srv.listen(1)
    srv.settimeout(15)

    cfg["mqtt"]["host"] = "127.0.0.1"
    cfg["mqtt"]["port"] = port
    cfg["mqtt"]["reconnect_ms"] = 100
    cfg["tdengine"]["dry_run"] = True
    cfg["tdengine"]["batch_size"] = 1  # flush on every message
    cfg["tdengine"]["flush_ms"] = 50

    fd, cfg_path = tempfile.mkstemp(suffix=".json", prefix="mqttsrc-e2e-")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(cfg, f)

    proc = subprocess.Popen([exe, "-c", cfg_path, "-n"],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, bufsize=1)
    conn = None
    try:
        conn, _ = srv.accept()
        conn.settimeout(10)

        header, _ = read_packet(conn)
        assert header == 0x10, "expected CONNECT, got 0x%02x" % header
        conn.sendall(b"\x20\x02\x00\x00")  # CONNACK, accepted

        header, body = read_packet(conn)
        assert header == 0x82, "expected SUBSCRIBE, got 0x%02x" % header
        sub_id = body[:2]
        conn.sendall(b"\x90\x03" + sub_id + b"\x01")  # SUBACK, granted QoS 1

        topic = scenario.get("topic", "factory/bj/telemetry")
        payloads = scenario["payloads"]
        qos_list = scenario.get("qos", [0] * len(payloads))
        for i, rec in enumerate(payloads):
            qos = qos_list[i] if i < len(qos_list) else 0
            publish(conn, topic, json.dumps(rec), qos=qos, packet_id=7 + i)

        time.sleep(1.0)
        proc.terminate()
        stdout, stderr = proc.communicate(timeout=10)
    finally:
        if conn is not None:
            conn.close()
        srv.close()
        try:
            os.unlink(cfg_path)
        except OSError:
            pass
        if proc.poll() is None:
            proc.kill()

    lines = [x.strip() for x in stdout.splitlines() if x.strip()]
    year = str(datetime.datetime.now().year)
    expected = [e.replace("{server_year}", year) for e in scenario["expected"]]
    missing = [e for e in expected if e not in lines]
    if missing:
        print("FAIL: expected lines not produced")
        for m in missing:
            print("  missing:", m)
        print("--- stdout ---")
        print(stdout)
        print("--- stderr ---")
        print(stderr)
        return 1
    print("PASS: %d line protocol row(s) produced over MQTT" % len(expected))
    return 0


if __name__ == "__main__":
    sys.exit(main())
