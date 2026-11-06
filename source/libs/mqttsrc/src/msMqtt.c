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
  MsFd    fd;
  SStr    rx;
  int64_t lastSendMs;
  int64_t lastRecvMs;
  uint16_t nextId;
  uint16_t qos2Seen[256];
  int32_t  qos2Pos;
} SMsConn;

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

static int32_t sendAll(MsFd fd, const char *p, int32_t len) {
  int32_t sent = 0;
  while (sent < len) {
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
    if (n == SOCKET_ERROR && WSAGetLastError() == WSAEWOULDBLOCK) continue;
#else
    if (n < 0 && (errno == EINTR || errno == EAGAIN)) continue;
#endif
    return -1;
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
  int32_t rc = sendAll(c->fd, pkt.data, pkt.len);
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

static bool qos2Duplicate(SMsConn *c, uint16_t id) {
  for (int32_t i = 0; i < 256; ++i) {
    if (c->qos2Seen[i] == id && id != 0) return true;
  }
  c->qos2Seen[c->qos2Pos] = id;
  c->qos2Pos = (c->qos2Pos + 1) % 256;
  return false;
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
    if (sel < 0) return -1;
    if (sel == 0) continue;
#ifdef _WIN32
    int n = recv(c->fd, buf, (int)sizeof(buf), 0);
#else
    int n = (int)recv(c->fd, buf, sizeof(buf), 0);
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
  if (!ok || sendPacket(c, (uint8_t)((MS_MQTT_SUBSCRIBE << 4) | 0x02), body.data, body.len) != 0) {
    ssFree(&body);
    return -1;
  }
  ssFree(&body);
  msLog(MS_LOG_INFO, "subscribed to %d topic filter(s), packetId=%u", cfg->topicNum, id);
  return 0;
}

static int32_t handlePublish(SMsConn *c, const SMsMqttOpts *opts, uint8_t flags,
                             const uint8_t *body, int32_t bodyLen) {
  if (bodyLen < 2) return -1;
  int32_t topicLen = (body[0] << 8) | body[1];
  if (topicLen < 0 || 2 + topicLen > bodyLen) return -1;
  char topic[MS_TEXT_LEN];
  int32_t n = topicLen < (int32_t)sizeof(topic) - 1 ? topicLen : (int32_t)sizeof(topic) - 1;
  memcpy(topic, body + 2, (size_t)n);
  topic[n] = '\0';

  int32_t pos = 2 + topicLen;
  int32_t qos = (flags >> 1) & 0x03;
  uint16_t packetId = 0;
  if (qos > 0) {
    if (pos + 2 > bodyLen) return -1;
    packetId = (uint16_t)((body[pos] << 8) | body[pos + 1]);
    pos += 2;
  }

  bool dup = (qos == 2) ? qos2Duplicate(c, packetId) : false;
  if (!dup && opts->onMessage != NULL) {
    opts->onMessage(opts->arg, topic, body + pos, bodyLen - pos, qos, packetId);
  }

  if (qos == 1) {
    char ack[2] = {(char)(packetId >> 8), (char)(packetId & 0xff)};
    return sendPacket(c, (uint8_t)(MS_MQTT_PUBACK << 4), ack, 2);
  }
  if (qos == 2) {
    char rec[2] = {(char)(packetId >> 8), (char)(packetId & 0xff)};
    return sendPacket(c, (uint8_t)(MS_MQTT_PUBREC << 4), rec, 2);
  }
  return 0;
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
          char comp[2] = {(char)body[0], (char)body[1]};
          rc2 = sendPacket(c, (uint8_t)(MS_MQTT_PUBCOMP << 4), comp, 2);
        }
        break;
      }
      case MS_MQTT_PINGRESP:
      case MS_MQTT_SUBACK:
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
  while (!*stop && msNowMs() < deadline) msSleepMs(100);
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
  if (ssInit(&conn.rx, 8192) != 0) return -1;
  conn.nextId = 1;

  int32_t backoff = cfg->reconnectMs;
  int32_t final = 0;

  while (!*opts->stop) {
    if (netConnect(&conn, cfg) != 0 || mqttConnect(&conn, cfg) != 0 ||
        mqttSubscribe(&conn, cfg) != 0) {
      msLog(MS_LOG_WARN, "MQTT connect/subscribe failed, retry in %dms", backoff);
      closeConn(&conn);
      if (opts->reconnects != NULL) ++(*opts->reconnects);
      sleepInterruptible(backoff, opts->stop);
      backoff = backoff * 2 > cfg->maxReconnectMs ? cfg->maxReconnectMs : backoff * 2;
      continue;
    }
    backoff = cfg->reconnectMs;
    msLog(MS_LOG_INFO, "MQTT connected to %s:%u, clientId=%s", cfg->host,
          (unsigned)cfg->port, cfg->clientId);

    while (!*opts->stop) {
      if (opts->onTick != NULL) opts->onTick(opts->arg);

      int64_t idle = msNowMs() - conn.lastSendMs;
      if (idle > (int64_t)cfg->keepalive * 800) {
        if (sendPacket(&conn, (uint8_t)(MS_MQTT_PINGREQ << 4), NULL, 0) != 0) break;
      }

      fd_set rfds;
      FD_ZERO(&rfds);
      FD_SET(conn.fd, &rfds);
      struct timeval tv;
      tv.tv_sec = 0;
      tv.tv_usec = 100000; /* 100ms keeps batch flush and stop responsive */
      int sel = select((int)conn.fd + 1, &rfds, NULL, NULL, &tv);
      if (sel < 0) break;
      if (sel > 0) {
        char buf[8192];
#ifdef _WIN32
        int n = recv(conn.fd, buf, (int)sizeof(buf), 0);
#else
        int n = (int)recv(conn.fd, buf, sizeof(buf), 0);
#endif
        if (n <= 0) break;
        if (ssPut(&conn.rx, buf, n) != 0 || processPackets(&conn, opts) != 0) break;
      }
    }

    if (!*opts->stop) {
      sendPacket(&conn, (uint8_t)(MS_MQTT_DISCONNECT << 4), NULL, 0);
    }
    closeConn(&conn);
    msLog(MS_LOG_WARN, "MQTT connection lost");
    if (opts->reconnects != NULL) ++(*opts->reconnects);
    if (!*opts->stop) sleepInterruptible(backoff, opts->stop);
  }

  closeConn(&conn);
  ssFree(&conn.rx);
#ifdef _WIN32
  WSACleanup();
#endif
  final = 0;
  return final;
}
