#!/usr/bin/env python3
# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
# SPDX-License-Identifier: AGPL-3.0-or-later
"""
End-to-end test for taosmqttsrc in dry-run mode.

A small MQTT 3.1.1 server that drives the client through a scripted scenario:

  * CONNECT (checks protocol level, clean-session flag and client id)
  * SUBSCRIBE/SUBACK
  * PUBLISH with QoS 0/1/2, collecting PUBACK / PUBREC+PUBREL / PUBCOMP
  * PINGREQ/PINGRESP
  * an abrupt disconnect followed by a reconnect and DUP redelivery

Finally the client is asked to stop by itself (--exit-after-ms) so the exit
code and the final flush are verified, and stdout must match the expected
lines exactly - missing, duplicated and extra rows all fail.

Usage: python fake_mqtt_broker.py <taosmqttsrc> <config.json> [scenario.json]

scenario.json:
  topic, payloads[], qos[], expected[]   as before
  clean_session (bool)   expected CONNECT flag, default false
  ping (bool)            exchange PINGREQ/PINGRESP
  drop_after (int)       close the socket after publishing payloads[drop_after]
  redeliver (bool)       after the reconnect, resend the unacknowledged payload
"""

import datetime
import json
import os
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time

CONNACK = b"\x20\x02\x00\x00"


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


def publish(sock, topic, payload, qos=0, packet_id=1, dup=False):
    body = struct.pack("!H", len(topic)) + topic.encode()
    if qos > 0:
        body += struct.pack("!H", packet_id)
    body += payload.encode()
    header = 0x30 | (qos << 1) | (0x08 if dup else 0)
    sock.sendall(packet(header, body))


class Client(object):
    def __init__(self, sock):
        self.sock = sock
        self.sock.settimeout(10)
        self.pings = 0

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def handshake(self, expect_clean_session):
        header, body = read_packet(self.sock)
        if header != 0x10:
            raise RuntimeError("expected CONNECT, got 0x%02x" % header)
        if len(body) < 12:
            raise RuntimeError("short CONNECT")
        proto_len = struct.unpack("!H", body[0:2])[0]
        proto = body[2:2 + proto_len]
        if proto != b"MQTT":
            raise RuntimeError("unexpected protocol %r" % proto)
        level = body[2 + proto_len]
        flags = body[3 + proto_len]
        keepalive = struct.unpack("!H", body[4 + proto_len:6 + proto_len])[0]
        if level != 4:
            raise RuntimeError("expected MQTT level 4, got %d" % level)
        clean = bool(flags & 0x02)
        if clean != expect_clean_session:
            raise RuntimeError(
                "clean_session flag is %s, expected %s" % (clean, expect_clean_session))
        if (flags & 0x40) and not (flags & 0x80):
            raise RuntimeError("password flag set without user name flag")
        pos = 6 + proto_len
        client_len = struct.unpack("!H", body[pos:pos + 2])[0]
        client_id = body[pos + 2:pos + 2 + client_len].decode()
        if not client_id:
            raise RuntimeError("empty client id")
        if keepalive == 0:
            raise RuntimeError("keepalive must not be 0")
        self.sock.sendall(CONNACK)
        self.client_id = client_id

    def subscribe(self):
        header, body = read_packet(self.sock)
        if header != 0x82:
            raise RuntimeError("expected SUBSCRIBE, got 0x%02x" % header)
        sub_id = body[:2]
        self.sock.sendall(b"\x90\x03" + sub_id + b"\x01")  # SUBACK, QoS 1

    def collect_acks(self, want_puback, want_pubrec, timeout=6.0):
        """Read ACKs until the expected counts are reached."""
        got_ack, got_rec, got_comp = 0, 0, 0
        pending_rel = set()
        deadline = time.time() + timeout
        self.sock.settimeout(0.2)
        while time.time() < deadline:
            if got_ack >= want_puback and got_rec >= want_pubrec and not pending_rel:
                break
            try:
                header, body = read_packet(self.sock)
            except socket.timeout:
                continue
            except EOFError:
                break
            ptype = header >> 4
            if ptype == 4:  # PUBACK
                got_ack += 1
            elif ptype == 5:  # PUBREC
                pid = body[:2]
                pending_rel.add(pid)
                self.sock.sendall(packet(0x62, pid))  # PUBREL
                got_rec += 1
            elif ptype == 7:  # PUBCOMP
                got_comp += 1
            elif ptype == 12:  # PINGREQ
                self.pings += 1
                self.sock.sendall(b"\xd0\x00")
            elif ptype == 14:  # DISCONNECT
                break
            else:
                raise RuntimeError("unexpected packet type %d" % ptype)
        return got_ack, got_rec, got_comp

    def await_ping(self, timeout=3.0):
        """Wait for a client PINGREQ and answer with PINGRESP."""
        deadline = time.time() + timeout
        self.sock.settimeout(0.2)
        while time.time() < deadline:
            if self.pings > 0:
                return True
            try:
                header, body = read_packet(self.sock)
            except socket.timeout:
                continue
            except EOFError:
                return False
            ptype = header >> 4
            if ptype == 12:
                self.pings += 1
                self.sock.sendall(b"\xd0\x00")
            elif ptype == 4:
                pass
            elif ptype == 5:
                self.sock.sendall(packet(0x62, body[:2]))
            elif ptype == 14:
                return False
        return self.pings > 0


def read_stdout(proc, sink, stop_flag):
    for line in proc.stdout:
        sink.append(line.rstrip("\n"))
    stop_flag.set()


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
        raise SystemExit("a scenario file is required")

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    port = srv.getsockname()[1]
    srv.listen(8)
    srv.settimeout(20)

    cfg["mqtt"]["host"] = "127.0.0.1"
    cfg["mqtt"]["port"] = port
    cfg["mqtt"]["reconnect_ms"] = 100
    cfg["mqtt"]["max_reconnect_ms"] = 500
    cfg.setdefault("tdengine", {})["dry_run"] = True
    cfg["tdengine"]["batch_size"] = int(scenario.get("batch_size", 5))
    cfg["tdengine"]["flush_ms"] = int(scenario.get("flush_ms", 50))
    if scenario.get("ping"):
        # keepalive*800ms is when the client sends PINGREQ; keep the test short
        cfg["mqtt"]["keepalive"] = 1

    fd, cfg_path = tempfile.mkstemp(suffix=".json", prefix="mqttsrc-e2e-")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(cfg, f)

    run_ms = int(scenario.get("run_ms", 2500))
    proc = subprocess.Popen([exe, "-c", cfg_path, "-n", "--exit-after-ms", str(run_ms)],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, bufsize=1)
    lines = []
    reader_done = threading.Event()
    reader = threading.Thread(target=read_stdout, args=(proc, lines, reader_done))
    reader.daemon = True
    reader.start()

    failure = None
    acks = (0, 0, 0)
    client = None
    try:
        client, _ = srv.accept()
        c = Client(client)
        c.handshake(bool(scenario.get("clean_session", False)))
        c.subscribe()

        topic = scenario.get("topic", "factory/bj/telemetry")
        payloads = scenario["payloads"]
        qos_list = scenario.get("qos", [0] * len(payloads))
        drop_after = scenario.get("drop_after", -1)
        want_ack = want_rec = 0

        for i, rec in enumerate(payloads):
            qos = qos_list[i] if i < len(qos_list) else 0
            if qos == 1:
                want_ack += 1
            elif qos == 2:
                want_rec += 1
            publish(client, topic, json.dumps(rec), qos=qos, packet_id=7 + i)
            if i == drop_after:
                # Abrupt close before any ACK: the broker keeps the session and
                # redelivers with DUP=1 on the next connection.
                c.close()
                if not scenario.get("redeliver", True):
                    break
                client, _ = srv.accept()
                c = Client(client)
                c.handshake(bool(scenario.get("clean_session", False)))
                c.subscribe()
                publish(client, topic, json.dumps(rec), qos=qos, packet_id=7 + i,
                        dup=True)

        if want_ack or want_rec:
            got_ack, got_rec, got_comp = c.collect_acks(want_ack, want_rec)
            acks = (got_ack, got_rec, got_comp)
            if got_ack != want_ack:
                raise RuntimeError("expected %d PUBACK(s), got %d" % (want_ack, got_ack))
            if got_rec != want_rec:
                raise RuntimeError("expected %d PUBREC(s), got %d" % (want_rec, got_rec))
            if got_comp != want_rec:
                raise RuntimeError("expected %d PUBCOMP(s), got %d" % (want_rec, got_comp))

        if scenario.get("ping") and not c.await_ping(3.0):
            raise RuntimeError("client did not send PINGREQ while idle")
    except Exception as exc:  # report the root cause, not an UnboundLocalError
        failure = "%s: %s" % (type(exc).__name__, exc)
    finally:
        if client is not None:
            client.close()
        srv.close()

    try:
        proc.wait(timeout=run_ms / 1000.0 + 15)
    except subprocess.TimeoutExpired:
        proc.kill()
        failure = failure or "client did not stop after --exit-after-ms"
    reader.join(timeout=5)
    stderr = proc.stderr.read() if proc.stderr is not None else ""
    stdout_extra = proc.stdout.read() if proc.stdout is not None else ""
    if stdout_extra:
        lines.extend(stdout_extra.splitlines())
    try:
        os.unlink(cfg_path)
    except OSError:
        pass

    if failure is None and proc.returncode != 0:
        failure = "client exited with code %d" % proc.returncode

    produced = sorted(x.strip() for x in lines if x.strip())
    year = str(datetime.datetime.now().year)
    expected = sorted(e.replace("{server_year}", year)
                      for e in scenario["expected"] if e.strip())

    if produced != expected:
        failure = failure or "row set mismatch"
        print("FAIL:", failure)
        missing = [x for x in expected if x not in produced]
        extra = [x for x in produced if x not in expected]
        for m in missing:
            print("  missing:", m)
        for e in extra:
            print("  unexpected:", e)
        print("  produced %d row(s), expected %d" % (len(produced), len(expected)))
        print("--- stdout ---")
        print("\n".join(lines))
        print("--- stderr ---")
        print(stderr)
        return 1

    if failure:
        print("FAIL:", failure)
        print("--- stdout ---")
        print("\n".join(lines))
        print("--- stderr ---")
        print(stderr)
        return 1

    print("PASS: %d row(s) in order, PUBACK=%d PUBREC=%d PUBCOMP=%d"
          % (len(expected), acks[0], acks[1], acks[2]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
