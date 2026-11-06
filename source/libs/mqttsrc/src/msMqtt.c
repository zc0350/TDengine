/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Minimal MQTT 3.1.1 client (ingress direction only).
 *
 * Supported: CONNECT/CONNACK, SUBSCRIBE/SUBACK, PUBLISH (QoS 0/1/2),
 * PUBACK/PUBREC/PUBREL/PUBCOMP, PINGREQ/PINGRESP, DISCONNECT,
 * username/password, keepalive, exponential reconnect + re-subscribe.
 * Not supported: TLS, MQTT 5.0, will message, session persistence.
 *
 * contrib/libmqtt only ships a broker and client *headers*, so the wire
 * protocol is implemented here rather than linked from that library.
 */

#include "msInt.h"

#ifdef _WIN32
typedef SOCKET MsFd;
#define MS_BADFD INVALID_SOCKET
#else
typedef int MsFd;
#define MS_BADFD (-1)
#include <fcntl.h>
#endif

#define MS_MQTT_CONNECT     1
#define MS_MQTT_CONNACK     2
#define MS_MQTT_PUBLISH     3
#define MS_MQTT_PUBACK      4
#define MS_MQTT_PUBREC      5
#define MS_MQTT_PUBREL      6
#define MS_MQTT_PUBCOMP     7
#define MS_MQTT_SUBSCRIBE   8
#define MS_MQTT_SUBACK      9
#define MS_MQTT_PINGREQ     12
#define MS_MQTT_PINGRESP    13
#define MS_MQTT_DISCONNECT  14

typedef struct {
  MsFd              fd;
  SStr              rx;
  int64_t           lastSendMs;
  int64_t           lastRecvMs;
  uint16_t          nextId;
  uint16_t          qos2Seen[256];
  int32_t           qos2Pos;
  volatile int32_t *stop;
  uint16_t          pendingSubId;
  int32_t           subAckResult; /* -2 pending, 0 accepted, -1 rejected */
} SMsConn;

#define MS_SUBACK_PENDING (-2)
#define MS_SEND_TIMEOUT_MS 5000

static int32_t processPackets(SMsConn *c, const SMsMqttOpts *opts);

static void rxConsume(SStr *b, int32_t n) {
  if (n >= b->len) {
    ssClear(b);
    return;
  }
  memmove(b->data, b->data + n, (size_t)(b->len - n));
  b->len -= n;
  b->data[b->len] = '\0';
}

int32_t msMqttEncodeRemainLen(uint32_t len, uint8_t *buf, int32_t bufLen) {
  if (buf == NULL || len > 268435455) return -1;
  int32_t n = 0;
  uint32_t v = len;
  do {
    if (n >= bufLen || n >= 4) return -1;
    buf[n++] = (uint8_t)(v % 128);
    v /= 128;
    if (v > 0) buf[n - 1] |= 0x80;
  } while (v > 0);
  return n;
}

/* 0 = ok, 1 = need more bytes, -1 = malformed. *used = varint byte count. */
int32_t msMqttDecodeRemainLen(const uint8_t *buf, int32_t bufLen,
                              uint32_t *value, int32_t *used) {
  if (buf == NULL || value == NULL || used == NULL || bufLen <= 0) return -1;
  uint32_t v = 0;
  int32_t i = 0;
  do {
    if (i >= bufLen) return 1;
    if (i >= 4) return -1;
    v += (uint32_t)(buf[i] & 0x7f) << (7 * i);
    if ((buf[i] & 0x80) == 0) {
      *value = v;
      *used = i + 1;
      return 0;
    }
    ++i;
  } while (true);
}

static int32_t putU16(SStr *s, uint16_t v) {
  uint8_t b[2] = {(uint8_t)(v >> 8), (uint8_t)(v & 0xff)};
  return ssPut(s, (const char *)b, 2);
}

static int32_t putStr(SStr *s, const char *p) {
  int32_t len = p ? (int32_t)strlen(p) : 0;
  if (len > 65535) return -1;
  if (putU16(s, (uint16_t)len) != 0) return -1;
  return len > 0 ? ssPut(s, p, len) : 0;
}

static int32_t buildPacket(SStr *out, uint8_t typeFlags, const char *body, int32_t bodyLen) {
  uint8_t var[4];
  int32_t n = msMqttEncodeRemainLen((uint32_t)bodyLen, var, 4);
  if (n < 0) return -1;
  if (ssPutChar(out, (char)typeFlags) != 0) return -1;
  if (ssPut(out, (const char *)var, n) != 0) return -1;
  return bodyLen > 0 ? ssPut(out, body, bodyLen) : 0;
}

/* Bounded, stop-aware send. EINTR/EAGAIN used to spin forever with no way to
 * honour a stop request; now we wait for writability with a short timeout. */
static int32_t sendAll(MsFd fd, const char *p, int32_t len, volatile int32_t *stop) {
  int32_t sent = 0;
  int64_t deadline = msNowMs() + MS_SEND_TIMEOUT_MS;
  while (sent < len) {
    if (stop != NULL && MS_STOPPED(stop)) return -1;
    if (msNowMs() >= deadline) {
      msLog(MS_LOG_WARN, "send timed out after %d byte(s)", sent);
      return -1;
    }
#ifdef _WIN32
    int n = send(fd, p + sent, len - sent, 0);
#else
    int n = (int)send(fd, p + sent, (size_t)(len - sent), 0);
#endif
    if (n > 0) {
      sent += n;
      continue;
    }
#ifdef _WIN32
    if (n != SOCKET_ERROR) return -1;
    int err = WSAGetLastError();
    if (err != WSAEWOULDBLOCK && err != WSAEINTR) return -1;
#else
    if (n == 0 || (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)) return -1;
#endif
    fd_set wfds;
    FD_ZERO(&wfds);
    FD_SET(fd, &wfds);
    int64_t left = deadline - msNowMs();
    if (left <= 0) return -1;
    if (left > 100) left = 100; /* keep stop responsive */
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = (long)(left * 1000);
    int sel = select((int)fd + 1, NULL, &wfds, NULL, &tv);
#ifndef _WIN32
    if (sel < 0 && errno == EINTR) continue;
#endif
    if (sel < 0) return -1;
  }
  return 0;
}

static int32_t sendPacket(SMsConn *c, uint8_t typeFlags, const char *body, int32_t bodyLen) {
  SStr pkt;
  if (ssInit(&pkt, 256) != 0) return -1;
  if (buildPacket(&pkt, typeFlags, body, bodyLen) != 0) {
    ssFree(&pkt);
    return -1;
  }
  int32_t rc = sendAll(c->fd, pkt.data, pkt.len, c->stop);
  ssFree(&pkt);
  if (rc == 0) c->lastSendMs = msNowMs();
  return rc;
}

static void closeConn(SMsConn *c) {
  if (c->fd != MS_BADFD) {
#ifdef _WIN32
    closesocket(c->fd);
#else
    close(c->fd);
#endif
    c->fd = MS_BADFD;
  }
  ssClear(&c->rx);
}

static int32_t netConnect(SMsConn *c, const SMsBrokerCfg *cfg) {
  char port[16];
  snprintf(port, sizeof(port), "%u", (unsigned)cfg->port);

  struct addrinfo hints, *res = NULL, *it = NULL;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  if (getaddrinfo(cfg->host, port, &hints, &res) != 0 || res == NULL) return -1;

  MsFd fd = MS_BADFD;
  int cr = -1;
  for (it = res; it != NULL; it = it->ai_next) {
    fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
    if (fd == MS_BADFD) continue;

    /* non-blocking connect so the timeout is honored */
#ifdef _WIN32
    u_long on = 1;
    ioctlsocket(fd, FIONBIO, &on);
#else
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
#endif
    cr = connect(fd, it->ai_addr, (int)it->ai_addrlen);
#ifdef _WIN32
    bool inProgress = (cr == SOCKET_ERROR && WSAGetLastError() == WSAEWOULDBLOCK);
#else
    bool inProgress = (cr < 0 && errno == EINPROGRESS);
#endif
    if (cr == 0 || inProgress) break;

#ifdef _WIN32
    closesocket(fd);
#else
    close(fd);
#endif
    fd = MS_BADFD;
  }
  freeaddrinfo(res);
  if (fd == MS_BADFD) return -1;

  if (cr != 0) {
    fd_set wfds, efds;
    FD_ZERO(&wfds);
    FD_ZERO(&efds);
    FD_SET(fd, &wfds);
    FD_SET(fd, &efds);
    struct timeval tv;
    tv.tv_sec = cfg->connectTimeoutMs / 1000;
    tv.tv_usec = (cfg->connectTimeoutMs % 1000) * 1000;
    int sel = select((int)fd + 1, NULL, &wfds, &efds, &tv);
    if (sel <= 0) {
      goto fail;
    }
    int soerr = 0;
#ifdef _WIN32
    int slen = sizeof(soerr);
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, (char *)&soerr, &slen) != 0 || soerr != 0) goto fail;
#else
    socklen_t slen = sizeof(soerr);
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &soerr, &slen) != 0 || soerr != 0) goto fail;
#endif
  }

  /* back to blocking mode */
#ifdef _WIN32
  u_long off = 0;
  ioctlsocket(fd, FIONBIO, &off);
#else
  int flags = fcntl(fd, F_GETFL, 0);
  fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
#endif
#ifdef _WIN32
  DWORD sndTimeout = 5000;
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (const char *)&sndTimeout, sizeof(sndTimeout));
#else
  struct timeval sndTv;
  sndTv.tv_sec = 5;
  sndTv.tv_usec = 0;
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (const char *)&sndTv, sizeof(sndTv));
#endif

  c->fd = fd;
  c->lastSendMs = msNowMs();
  c->lastRecvMs = msNowMs();
  return 0;

fail:
#ifdef _WIN32
  closesocket(fd);
#else
  close(fd);
#endif
  return -1;
}

static uint16_t nextId(SMsConn *c) {
  if (++c->nextId == 0) c->nextId = 1;
  return c->nextId;
}

/* QoS2 de-duplication state is only committed after the message has been
 * persisted, is cleared on PUBREL, and is reset on every reconnect. */
static bool qos2Seen(SMsConn *c, uint16_t id) {
  if (id == 0) return false;
  for (int32_t i = 0; i < 256; ++i) {
    if (c->qos2Seen[i] == id) return true;
  }
  return false;
}

static void qos2Remember(SMsConn *c, uint16_t id) {
  if (id == 0) return;
  c->qos2Seen[c->qos2Pos] = id;
  c->qos2Pos = (c->qos2Pos + 1) % 256;
}

static void qos2Forget(SMsConn *c, uint16_t id) {
  for (int32_t i = 0; i < 256; ++i) {
    if (c->qos2Seen[i] == id) c->qos2Seen[i] = 0;
  }
}

static void qos2Reset(SMsConn *c) {
  memset(c->qos2Seen, 0, sizeof(c->qos2Seen));
  c->qos2Pos = 0;
}

static int32_t mqttConnect(SMsConn *c, const SMsBrokerCfg *cfg) {
  SStr body;
  if (ssInit(&body, 256) != 0) return -1;

  uint8_t flags = (uint8_t)(cfg->cleanSession ? 0x02 : 0x00);
  if (cfg->username[0] != '\0') flags |= 0x80;
  if (cfg->password[0] != '\0') flags |= 0x40;

  bool ok = (putStr(&body, "MQTT") == 0 && ssPutChar(&body, (char)4) == 0 &&
             ssPutChar(&body, (char)flags) == 0 && putU16(&body, (uint16_t)cfg->keepalive) == 0 &&
             putStr(&body, cfg->clientId) == 0);
  if (ok && cfg->username[0] != '\0') ok = (putStr(&body, cfg->username) == 0);
  if (ok && cfg->password[0] != '\0') ok = (putStr(&body, cfg->password) == 0);
  if (!ok || sendPacket(c, (uint8_t)(MS_MQTT_CONNECT << 4), body.data, body.len) != 0) {
    ssFree(&body);
    return -1;
  }
  ssFree(&body);

  /* CONNACK: 0x20 0x02 <flags> <return code> */
  int64_t deadline = msNowMs() + cfg->connectTimeoutMs;
  while (msNowMs() < deadline) {
    char buf[256];
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(c->fd, &rfds);
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 100000;
    int sel = select((int)c->fd + 1, &rfds, NULL, NULL, &tv);
#ifndef _WIN32
    if (sel < 0 && errno == EINTR) continue;
#endif
    if (sel < 0) return -1;
    if (sel == 0) continue;
#ifdef _WIN32
    int n = recv(c->fd, buf, (int)sizeof(buf), 0);
#else
    int n = (int)recv(c->fd, buf, sizeof(buf), 0);
#endif
#ifndef _WIN32
    if (n < 0 && errno == EINTR) continue;
#endif
    if (n <= 0) return -1;
    if (ssPut(&c->rx, buf, n) != 0) return -1;
    if (c->rx.len >= 4) {
      uint32_t rem = 0;
      int32_t used = 0;
      int32_t rc = msMqttDecodeRemainLen((const uint8_t *)c->rx.data + 1, c->rx.len - 1,
                                         &rem, &used);
      if (rc == 0 && (uint8_t)c->rx.data[0] == (MS_MQTT_CONNACK << 4) && rem == 2 &&
          c->rx.len >= 1 + used + (int32_t)rem) {
        int32_t total = 1 + used + (int32_t)rem;
        uint8_t code = (uint8_t)c->rx.data[total - 1];
        rxConsume(&c->rx, total); /* keep any bytes that followed CONNACK */
        if (code != 0) {
          msLog(MS_LOG_ERROR, "CONNECT refused by broker, code=%u", (unsigned)code);
          return -1;
        }
        return 0;
      }
    }
  }
  return -1;
}

static int32_t mqttSubscribe(SMsConn *c, const SMsBrokerCfg *cfg) {
  SStr body;
  if (ssInit(&body, 256) != 0) return -1;
  uint16_t id = nextId(c);
  bool ok = (putU16(&body, id) == 0);
  for (int32_t i = 0; ok && i < cfg->topicNum; ++i) {
    ok = (putStr(&body, cfg->topics[i].filter) == 0 &&
          ssPutChar(&body, (char)cfg->topics[i].qos) == 0);
  }
  c->pendingSubId = id;
  c->subAckResult = MS_SUBACK_PENDING;
  if (!ok || sendPacket(c, (uint8_t)((MS_MQTT_SUBSCRIBE << 4) | 0x02), body.data, body.len) != 0) {
    ssFree(&body);
    return -1;
  }
  ssFree(&body);
  msLog(MS_LOG_INFO, "SUBSCRIBE sent for %d topic filter(s), packetId=%u",
        cfg->topicNum, id);
  return 0;
}

/* SUBACK carries the packet id followed by one granted-QoS byte per filter.
 * 0x80 means the broker rejected that filter; previously this was silently
 * ignored, so a rejected subscription looked like a healthy connection. */
static int32_t handleSubAck(SMsConn *c, const uint8_t *body, int32_t bodyLen) {
  if (c->pendingSubId == 0 || c->subAckResult != MS_SUBACK_PENDING) return 0;
  if (bodyLen < 3) {
    msLog(MS_LOG_ERROR, "malformed SUBACK (len=%d)", bodyLen);
    return -1;
  }
  uint16_t id = (uint16_t)((body[0] << 8) | body[1]);
  if (id != c->pendingSubId) return 0; /* stale SUBACK for an earlier attempt */
  int32_t granted = bodyLen - 2;
  for (int32_t i = 0; i < granted; ++i) {
    if (body[2 + i] == 0x80) {
      msLog(MS_LOG_ERROR, "broker rejected topic filter %d (packetId=%u)", i,
            (unsigned)id);
      c->subAckResult = -1;
      c->pendingSubId = 0;
      return -1;
    }
  }
  c->subAckResult = 0;
  c->pendingSubId = 0;
  msLog(MS_LOG_INFO, "SUBACK accepted %d topic filter(s), packetId=%u", granted,
        (unsigned)id);
  return 0;
}

/* Wait for the SUBACK before entering the receive loop. Packets that arrive
 * in the meantime are still processed, so no broker data is lost. */
static int32_t waitSubAck(SMsConn *c, const SMsMqttOpts *opts) {
  int64_t timeout = opts->cfg->connectTimeoutMs > 0 ? opts->cfg->connectTimeoutMs : 5000;
  int64_t deadline = msNowMs() + timeout;
  while (!MS_STOPPED(opts->stop) && msNowMs() < deadline) {
    if (c->subAckResult != MS_SUBACK_PENDING) return c->subAckResult;

    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(c->fd, &rfds);
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 100000;
    int sel = select((int)c->fd + 1, &rfds, NULL, NULL, &tv);
#ifndef _WIN32
    if (sel < 0 && errno == EINTR) continue;
#endif
    if (sel < 0) return -1;
    if (sel == 0) continue;

    char buf[8192];
#ifdef _WIN32
    int n = recv(c->fd, buf, (int)sizeof(buf), 0);
#else
    int n = (int)recv(c->fd, buf, sizeof(buf), 0);
#endif
#ifndef _WIN32
    if (n < 0 && errno == EINTR) continue;
#endif
    if (n <= 0) return -1;
    if (ssPut(&c->rx, buf, n) != 0 || processPackets(c, opts) != 0) return -1;
  }
  if (c->subAckResult == MS_SUBACK_PENDING) {
    msLog(MS_LOG_ERROR, "timed out waiting for SUBACK (%lldms)", (long long)timeout);
    return -1;
  }
  return c->subAckResult;
}

static int32_t handlePublish(SMsConn *c, const SMsMqttOpts *opts, uint8_t flags,
                             const uint8_t *body, int32_t bodyLen) {
  if (bodyLen < 2) return -1;
  int32_t topicLen = (body[0] << 8) | body[1];
  if (topicLen < 0 || topicLen > 65535 || 2 + topicLen > bodyLen) return -1;
  /* Allocate the full topic: MQTT allows 65535 bytes and a 255-byte stack
   * buffer used to truncate it, corrupting topic variables and routing. */
  char *topic = (char *)malloc((size_t)topicLen + 1);
  if (topic == NULL) return -1;
  memcpy(topic, body + 2, (size_t)topicLen);
  topic[topicLen] = '\0';

  int32_t pos = 2 + topicLen;
  int32_t qos = (flags >> 1) & 0x03;
  if (qos == 3) { /* reserved by MQTT 3.1.1 */
    free(topic);
    return -1;
  }
  uint16_t packetId = 0;
  if (qos > 0) {
    if (pos + 2 > bodyLen) {
      free(topic);
      return -1;
    }
    packetId = (uint16_t)((body[pos] << 8) | body[pos + 1]);
    pos += 2;
  }

  bool dup = (qos == 2) ? qos2Seen(c, packetId) : false;
  int32_t delivered = 0;
  if (!dup && opts->onMessage != NULL) {
    delivered = opts->onMessage(opts->arg, topic, body + pos, bodyLen - pos, qos,
                                packetId);
  }
  if (delivered != 0) {
    /* No ACK: force a reconnect so the broker redelivers this PUBLISH. */
    msLog(MS_LOG_WARN, "message not persisted, withholding QoS ACK (packetId=%u)",
          (unsigned)packetId);
    free(topic);
    return -1;
  }
  if (qos == 2) qos2Remember(c, packetId);

  int32_t rc = 0;
  if (qos == 1) {
    char ack[2] = {(char)(packetId >> 8), (char)(packetId & 0xff)};
    rc = sendPacket(c, (uint8_t)(MS_MQTT_PUBACK << 4), ack, 2);
  } else if (qos == 2) {
    char rec[2] = {(char)(packetId >> 8), (char)(packetId & 0xff)};
    rc = sendPacket(c, (uint8_t)(MS_MQTT_PUBREC << 4), rec, 2);
  }
  free(topic);
  return rc;
}

static int32_t processPackets(SMsConn *c, const SMsMqttOpts *opts) {
  while (c->rx.len >= 2) {
    uint32_t rem = 0;
    int32_t used = 0;
    int32_t rc = msMqttDecodeRemainLen((const uint8_t *)c->rx.data + 1, c->rx.len - 1,
                                       &rem, &used);
    if (rc == 1) return 0; /* incomplete */
    if (rc != 0 || rem > 64 * 1024 * 1024) return -1;
    int32_t total = 1 + used + (int32_t)rem;
    if (c->rx.len < total) return 0;

    uint8_t hdr = (uint8_t)c->rx.data[0];
    uint8_t type = hdr >> 4;
    uint8_t flags = hdr & 0x0f;
    const uint8_t *body = (const uint8_t *)c->rx.data + 1 + used;
    int32_t rc2 = 0;

    switch (type) {
      case MS_MQTT_PUBLISH:
        rc2 = handlePublish(c, opts, flags, body, (int32_t)rem);
        break;
      case MS_MQTT_PUBREL: {
        if (rem >= 2) {
          uint16_t id = (uint16_t)(((uint8_t)body[0] << 8) | (uint8_t)body[1]);
          qos2Forget(c, id); /* QoS2 handshake complete: release the id */
          char comp[2] = {(char)body[0], (char)body[1]};
          rc2 = sendPacket(c, (uint8_t)(MS_MQTT_PUBCOMP << 4), comp, 2);
        }
        break;
      }
      case MS_MQTT_SUBACK:
        rc2 = handleSubAck(c, body, (int32_t)rem);
        break;
      case MS_MQTT_PINGRESP:
      case MS_MQTT_PUBACK:
      case MS_MQTT_PUBREC:
      case MS_MQTT_PUBCOMP:
        break;
      case MS_MQTT_CONNACK:
        rc2 = -1; /* unsolicited */
        break;
      default:
        msLog(MS_LOG_WARN, "ignoring MQTT packet type %u", type);
        break;
    }
    rxConsume(&c->rx, total);
    if (rc2 != 0) return -1;
    c->lastRecvMs = msNowMs();
  }
  return 0;
}

static void sleepInterruptible(int32_t ms, volatile int32_t *stop) {
  int64_t deadline = msNowMs() + ms;
  while (!MS_STOPPED(stop) && msNowMs() < deadline) msSleepMs(100);
}

int32_t msMqttLoop(const SMsMqttOpts *opts) {
  if (opts == NULL || opts->cfg == NULL || opts->stop == NULL) return -1;
#ifdef _WIN32
  WSADATA wsa;
  int wsaRc = WSAStartup(MAKEWORD(2, 2), &wsa);
  if (wsaRc != 0) {
    msLog(MS_LOG_ERROR, "WSAStartup failed: %d", wsaRc);
    return -1;
  }
#endif

  const SMsBrokerCfg *cfg = opts->cfg;
  SMsConn conn;
  memset(&conn, 0, sizeof(conn));
  conn.fd = MS_BADFD;
  conn.stop = opts->stop;
  conn.nextId = 1;
  conn.subAckResult = MS_SUBACK_PENDING;
  int32_t final = 0;
  int32_t backoff = cfg->reconnectMs;

  if (ssInit(&conn.rx, 8192) != 0) {
    msLog(MS_LOG_ERROR, "cannot allocate MQTT rx buffer");
    final = -1;
    goto done;
  }

  while (!MS_STOPPED(opts->stop)) {
    /* A fresh session invalidates QoS2 de-dup state, packet ids and any
     * pending SUBACK. Old state used to misclassify redelivered messages. */
    qos2Reset(&conn);
    conn.nextId = 1;
    conn.pendingSubId = 0;
    conn.subAckResult = MS_SUBACK_PENDING;

    if (netConnect(&conn, cfg) != 0 || mqttConnect(&conn, cfg) != 0 ||
        mqttSubscribe(&conn, cfg) != 0 || waitSubAck(&conn, opts) != 0) {
      msLog(MS_LOG_WARN, "MQTT connect/subscribe failed, retry in %dms", backoff);
      closeConn(&conn);
      if (opts->reconnects != NULL) ++(*opts->reconnects);
      sleepInterruptible(backoff, opts->stop);
      int64_t next = (int64_t)backoff * 2;
      backoff = (int32_t)(next > cfg->maxReconnectMs ? cfg->maxReconnectMs : next);
      continue;
    }
    backoff = cfg->reconnectMs;
    msLog(MS_LOG_INFO, "MQTT connected to %s:%u, clientId=%s", cfg->host,
          (unsigned)cfg->port, cfg->clientId);

    while (!MS_STOPPED(opts->stop)) {
      if (opts->onTick != NULL) opts->onTick(opts->arg);

      int64_t now = msNowMs();
      if (now - conn.lastSendMs > (int64_t)cfg->keepalive * 800) {
        if (sendPacket(&conn, (uint8_t)(MS_MQTT_PINGREQ << 4), NULL, 0) != 0) break;
      }
      /* Half-open detection: select() keeps timing out on a dead peer and
       * PINGRESP never arrives. Without this check the loop never reconnects. */
      if (now - conn.lastRecvMs > (int64_t)cfg->keepalive * 1500) {
        msLog(MS_LOG_WARN, "no data from broker for %lldms, reconnecting",
              (long long)(now - conn.lastRecvMs));
        break;
      }

      fd_set rfds;
      FD_ZERO(&rfds);
      FD_SET(conn.fd, &rfds);
      struct timeval tv;
      tv.tv_sec = 0;
      tv.tv_usec = 100000; /* 100ms keeps batch flush and stop responsive */
      int sel = select((int)conn.fd + 1, &rfds, NULL, NULL, &tv);
#ifndef _WIN32
      if (sel < 0 && errno == EINTR) continue;
#endif
      if (sel < 0) break;
      if (sel > 0) {
        char buf[8192];
#ifdef _WIN32
        int n = recv(conn.fd, buf, (int)sizeof(buf), 0);
#else
        int n = (int)recv(conn.fd, buf, sizeof(buf), 0);
#endif
#ifndef _WIN32
        if (n < 0 && errno == EINTR) continue;
#endif
        if (n <= 0) break;
        if (ssPut(&conn.rx, buf, n) != 0 || processPackets(&conn, opts) != 0) break;
      }
    }

    if (!MS_STOPPED(opts->stop)) {
      sendPacket(&conn, (uint8_t)(MS_MQTT_DISCONNECT << 4), NULL, 0);
    }
    closeConn(&conn);
    msLog(MS_LOG_WARN, "MQTT connection lost");
    if (opts->reconnects != NULL) ++(*opts->reconnects);
    if (!MS_STOPPED(opts->stop)) sleepInterruptible(backoff, opts->stop);
  }

  closeConn(&conn);
done:
  ssFree(&conn.rx);
#ifdef _WIN32
  WSACleanup();
#endif
  return final;
}
