/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Minimal MQTT 3.1.1 client (ingress direction only).
 *
 * Supported: CONNECT/CONNACK, SUBSCRIBE/SUBACK, PUBLISH (QoS 0/1/2),
 * PUBACK/PUBREC/PUBREL/PUBCOMP, PINGREQ/PINGRESP, DISCONNECT,
 * username/password, keepalive, exponential reconnect + re-subscribe,
 * optional TLS (OpenSSL, -DMQTTSRC_ENABLE_TLS=ON).
 * Not supported: MQTT 5.0, will message.
 *
 * QoS1/2 ACKs are *not* sent from handlePublish(): the message is queued for
 * the writer thread and the ACK is emitted once the batch has been accepted
 * by TDengine. A sink failure therefore leaves the message unacknowledged and
 * the broker redelivers it after the reconnect.
 *
 * contrib/libmqtt only ships a broker and client *headers*, so the wire
 * protocol is implemented here rather than linked from that library.
 */

#include "msInt.h"

#include <ctype.h>

#ifdef MS_WITH_TLS
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#ifndef _WIN32
#include <arpa/inet.h>
#endif
#endif

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
  void             *tlsCtx; /* SSL_CTX*, NULL without TLS */
  void             *tls;    /* SSL*, NULL without TLS */
  SStr              rx;
  int64_t           lastSendMs;
  int64_t           lastRecvMs;
  uint16_t          nextId;
  /* ids whose QoS2 PUBLISH has been persisted (PUBREC sent) */
  uint16_t          qos2Seen[256];
  /* ids queued but not yet persisted; a duplicate is ignored instead of
   * being written twice */
  uint16_t          qos2Pending[256];
  int32_t           qos2SeenPos;
  int32_t           qos2PendingPos;
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

/* ---------------- TLS ---------------- */

#ifdef MS_WITH_TLS
static bool isIpLiteral(const char *h) {
  unsigned char buf[16];
  if (h == NULL) return false;
#ifdef _WIN32
  return InetPtonA(AF_INET, h, buf) == 1 || InetPtonA(AF_INET6, h, buf) == 1;
#else
  return inet_pton(AF_INET, h, buf) == 1 || inet_pton(AF_INET6, h, buf) == 1;
#endif
}

static void tlsLogError(const char *what) {
  unsigned long e = ERR_get_error();
  char buf[256];
  if (e != 0) {
    ERR_error_string_n(e, buf, sizeof(buf));
    msLog(MS_LOG_ERROR, "%s failed: %s", what, buf);
  } else {
    msLog(MS_LOG_ERROR, "%s failed", what);
  }
}
#endif

static void tlsClose(SMsConn *c) {
#ifdef MS_WITH_TLS
  if (c->tls != NULL) {
    SSL_shutdown((SSL *)c->tls);
    SSL_free((SSL *)c->tls);
    c->tls = NULL;
  }
  if (c->tlsCtx != NULL) {
    SSL_CTX_free((SSL_CTX *)c->tlsCtx);
    c->tlsCtx = NULL;
  }
#else
  c->tls = NULL;
  c->tlsCtx = NULL;
#endif
}

static int32_t tlsHandshake(SMsConn *c, const SMsBrokerCfg *cfg) {
#ifdef MS_WITH_TLS
  SSL_CTX *ctx = SSL_CTX_new(TLS_client_method());
  if (ctx == NULL) {
    tlsLogError("SSL_CTX_new");
    return -1;
  }
  SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
  SSL_CTX_set_mode(ctx, SSL_MODE_AUTO_RETRY);
  if (cfg->tlsCaFile[0] != '\0') {
    if (SSL_CTX_load_verify_locations(ctx, cfg->tlsCaFile, NULL) != 1) {
      tlsLogError("SSL_CTX_load_verify_locations");
      SSL_CTX_free(ctx);
      return -1;
    }
  } else if (!cfg->tlsInsecure) {
    if (SSL_CTX_set_default_verify_paths(ctx) != 1) {
      tlsLogError("SSL_CTX_set_default_verify_paths");
      SSL_CTX_free(ctx);
      return -1;
    }
  }
  if (cfg->tlsCertFile[0] != '\0') {
    if (SSL_CTX_use_certificate_chain_file(ctx, cfg->tlsCertFile) != 1 ||
        SSL_CTX_use_PrivateKey_file(ctx, cfg->tlsKeyFile, SSL_FILETYPE_PEM) != 1 ||
        SSL_CTX_check_private_key(ctx) != 1) {
      tlsLogError("client certificate");
      SSL_CTX_free(ctx);
      return -1;
    }
  }
  SSL_CTX_set_verify(ctx, cfg->tlsInsecure ? SSL_VERIFY_NONE : SSL_VERIFY_PEER, NULL);

  SSL *ssl = SSL_new(ctx);
  if (ssl == NULL) {
    tlsLogError("SSL_new");
    SSL_CTX_free(ctx);
    return -1;
  }
  const char *name = cfg->tlsServerName[0] != '\0' ? cfg->tlsServerName : cfg->host;
  if (!isIpLiteral(name) && SSL_set_tlsext_host_name(ssl, name) != 1) {
    tlsLogError("SSL_set_tlsext_host_name");
    SSL_free(ssl);
    SSL_CTX_free(ctx);
    return -1;
  }
  if (SSL_set_fd(ssl, (int)c->fd) != 1) {
    tlsLogError("SSL_set_fd");
    SSL_free(ssl);
    SSL_CTX_free(ctx);
    return -1;
  }
  c->tlsCtx = ctx;
  c->tls = ssl;

  int rc = SSL_connect(ssl);
  if (rc != 1) {
    tlsLogError("SSL_connect");
    return -1;
  }
  if (!cfg->tlsInsecure) {
    long vr = SSL_get_verify_result(ssl);
    if (vr != X509_V_OK) {
      msLog(MS_LOG_ERROR, "TLS certificate verification failed: %s",
            X509_verify_cert_error_string(vr));
      return -1;
    }
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
    X509 *cert = SSL_get1_peer_certificate(ssl);
#else
    X509 *cert = SSL_get_peer_certificate(ssl);
#endif
    int match = (cert != NULL && X509_check_host(cert, name, 0, 0, NULL) == 1);
    if (cert != NULL) X509_free(cert);
    if (!match) {
      msLog(MS_LOG_ERROR, "TLS certificate does not match '%s'", name);
      return -1;
    }
  }
  msLog(MS_LOG_INFO, "TLS established with %s", name);
  return 0;
#else
  (void)c;
  (void)cfg;
  msLog(MS_LOG_ERROR, "TLS requested but this build has no TLS support");
  return -1;
#endif
}

/* ---------------- socket I/O ---------------- */

/* Returns: >0 bytes, 0 peer closed, -1 error, -2 retry (WANT_*, EINTR). */
static int32_t connRecv(SMsConn *c, char *buf, int32_t len) {
  if (c->tls != NULL) {
#ifdef MS_WITH_TLS
    int n = SSL_read((SSL *)c->tls, buf, len);
    if (n > 0) return n;
    int e = SSL_get_error((SSL *)c->tls, n);
    if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE) return -2;
    if (e == SSL_ERROR_ZERO_RETURN) return 0;
    return -1;
#else
    return -1;
#endif
  }
#ifdef _WIN32
  int n = recv(c->fd, buf, len, 0);
  if (n > 0) return n;
  if (n == 0) return 0;
  {
    int err = WSAGetLastError();
    if (err == WSAEWOULDBLOCK || err == WSAEINTR) return -2;
  }
  return -1;
#else
  int n = (int)recv(c->fd, buf, (size_t)len, 0);
  if (n > 0) return n;
  if (n == 0) return 0;
  if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) return -2;
  return -1;
#endif
}

static int32_t connPending(SMsConn *c) {
  if (c->tls == NULL) return 0;
#ifdef MS_WITH_TLS
  return SSL_pending((SSL *)c->tls);
#else
  return 0;
#endif
}

static int32_t waitFd(MsFd fd, bool forWrite, int32_t timeoutMs) {
  fd_set fds;
  FD_ZERO(&fds);
  FD_SET(fd, &fds);
  struct timeval tv;
  tv.tv_sec = timeoutMs / 1000;
  tv.tv_usec = (long)(timeoutMs % 1000) * 1000;
  int sel = forWrite ? select((int)fd + 1, NULL, &fds, NULL, &tv)
                     : select((int)fd + 1, &fds, NULL, NULL, &tv);
#ifndef _WIN32
  if (sel < 0 && errno == EINTR) return 0;
#endif
  return sel;
}

/* Bounded, stop-aware send. EINTR/EAGAIN used to spin forever with no way to
 * honour a stop request; now we wait for writability with a short timeout. */
static int32_t connSendAll(SMsConn *c, const char *p, int32_t len) {
  int32_t sent = 0;
  int64_t deadline = msNowMs() + MS_SEND_TIMEOUT_MS;
  while (sent < len) {
    if (c->stop != NULL && MS_STOPPED(c->stop)) return -1;
    if (msNowMs() >= deadline) {
      msLog(MS_LOG_WARN, "send timed out after %d byte(s)", sent);
      return -1;
    }
    if (c->tls != NULL) {
#ifdef MS_WITH_TLS
      int n = SSL_write((SSL *)c->tls, p + sent, len - sent);
      if (n > 0) {
        sent += n;
        continue;
      }
      int e = SSL_get_error((SSL *)c->tls, n);
      if (e == SSL_ERROR_WANT_READ) {
        if (waitFd(c->fd, false, 100) < 0) return -1;
        continue;
      }
      if (e == SSL_ERROR_WANT_WRITE) {
        if (waitFd(c->fd, true, 100) < 0) return -1;
        continue;
      }
      return -1;
#else
      return -1;
#endif
    }
#ifdef _WIN32
    int n = send(c->fd, p + sent, len - sent, 0);
#else
    int n = (int)send(c->fd, p + sent, (size_t)(len - sent), 0);
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
    int64_t left = deadline - msNowMs();
    if (left <= 0) return -1;
    if (left > 100) left = 100; /* keep stop responsive */
    int sel = waitFd(c->fd, true, (int32_t)left);
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
  int32_t rc = connSendAll(c, pkt.data, pkt.len);
  ssFree(&pkt);
  if (rc == 0) c->lastSendMs = msNowMs();
  return rc;
}

static void closeConn(SMsConn *c) {
  tlsClose(c);
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

static void setSocketTimeout(MsFd fd, int32_t ms) {
#ifdef _WIN32
  DWORD tv = (DWORD)ms;
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (const char *)&tv, sizeof(tv));
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof(tv));
#else
  struct timeval tv;
  tv.tv_sec = ms / 1000;
  tv.tv_usec = (ms % 1000) * 1000;
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (const char *)&tv, sizeof(tv));
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof(tv));
#endif
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
  /* Also covers the TLS handshake, which uses blocking calls. */
  setSocketTimeout(fd, cfg->connectTimeoutMs > 0 ? cfg->connectTimeoutMs : 5000);

  c->fd = fd;
  c->lastSendMs = msNowMs();
  c->lastRecvMs = msNowMs();

  if (cfg->tls && tlsHandshake(c, cfg) != 0) goto fail;
  return 0;

fail:
#ifdef _WIN32
  closesocket(fd);
#else
  close(fd);
#endif
  c->fd = MS_BADFD;
  tlsClose(c);
  return -1;
}

static uint16_t nextId(SMsConn *c) {
  if (++c->nextId == 0) c->nextId = 1;
  return c->nextId;
}

/* QoS2 de-duplication state is only committed after the message has been
 * persisted, is cleared on PUBREL, and survives a reconnect while a persistent
 * session is in use (otherwise packet ids from the old session would alias). */
static bool qos2Contains(const uint16_t *set, uint16_t id) {
  if (id == 0) return false;
  for (int32_t i = 0; i < 256; ++i) {
    if (set[i] == id) return true;
  }
  return false;
}

static void qos2Insert(uint16_t *set, int32_t *pos, uint16_t id) {
  if (id == 0) return;
  set[*pos] = id;
  *pos = (*pos + 1) % 256;
}

static void qos2Remove(uint16_t *set, uint16_t id) {
  for (int32_t i = 0; i < 256; ++i) {
    if (set[i] == id) set[i] = 0;
  }
}

static bool qos2Seen(SMsConn *c, uint16_t id) { return qos2Contains(c->qos2Seen, id); }
static void qos2Remember(SMsConn *c, uint16_t id) { qos2Insert(c->qos2Seen, &c->qos2SeenPos, id); }
static void qos2Forget(SMsConn *c, uint16_t id) { qos2Remove(c->qos2Seen, id); }
static bool qos2Pending(SMsConn *c, uint16_t id) { return qos2Contains(c->qos2Pending, id); }
static void qos2PendingAdd(SMsConn *c, uint16_t id) { qos2Insert(c->qos2Pending, &c->qos2PendingPos, id); }
static void qos2PendingForget(SMsConn *c, uint16_t id) { qos2Remove(c->qos2Pending, id); }

static void qos2Reset(SMsConn *c, bool cleanSession) {
  memset(c->qos2Pending, 0, sizeof(c->qos2Pending));
  c->qos2PendingPos = 0;
  if (cleanSession) {
    memset(c->qos2Seen, 0, sizeof(c->qos2Seen));
    c->qos2SeenPos = 0;
  }
}

/* Send the ACKs the writer thread has completed. Returns -1 when a batch
 * failed and the connection must be dropped so the broker redelivers. */
static int32_t drainAcks(SMsConn *c, const SMsMqttOpts *opts) {
  if (opts->pollAcks == NULL) return 0;
  for (;;) {
    SMsAck acks[16];
    int32_t n = opts->pollAcks(opts->arg, acks, (int32_t)(sizeof(acks) / sizeof(acks[0])));
    if (n < 0) return -1;
    for (int32_t i = 0; i < n; ++i) {
      uint8_t type = MS_MQTT_PUBACK;
      if (acks[i].qos == 2) {
        qos2PendingForget(c, acks[i].packetId);
        qos2Remember(c, acks[i].packetId);
        type = MS_MQTT_PUBREC;
      } else if (acks[i].qos != 1) {
        continue;
      }
      char body[2] = {(char)(acks[i].packetId >> 8), (char)(acks[i].packetId & 0xff)};
      if (sendPacket(c, (uint8_t)(type << 4), body, 2) != 0) return -1;
    }
    if (n < (int32_t)(sizeof(acks) / sizeof(acks[0]))) return 0;
  }
}

static int32_t mqttConnect(SMsConn *c, const SMsBrokerCfg *cfg) {
  SStr body;
  if (ssInit(&body, 256) != 0) return -1;

  uint8_t flags = (uint8_t)(cfg->cleanSession ? 0x02 : 0x00);
  /* MQTT 3.1.1 3.1.2.9: User Name Flag must be 1 when Password Flag is set. */
  if (cfg->username[0] != '\0') {
    flags |= 0x80;
    if (cfg->password[0] != '\0') flags |= 0x40;
  }

  bool ok = (putStr(&body, "MQTT") == 0 && ssPutChar(&body, (char)4) == 0 &&
             ssPutChar(&body, (char)flags) == 0 && putU16(&body, (uint16_t)cfg->keepalive) == 0 &&
             putStr(&body, cfg->clientId) == 0);
  if (ok && cfg->username[0] != '\0') ok = (putStr(&body, cfg->username) == 0);
  if (ok && cfg->password[0] != '\0' && cfg->username[0] != '\0') {
    ok = (putStr(&body, cfg->password) == 0);
  }
  if (!ok || sendPacket(c, (uint8_t)(MS_MQTT_CONNECT << 4), body.data, body.len) != 0) {
    ssFree(&body);
    return -1;
  }
  ssFree(&body);

  /* CONNACK: 0x20 0x02 <flags> <return code> */
  int64_t deadline = msNowMs() + cfg->connectTimeoutMs;
  while (msNowMs() < deadline) {
    int sel = waitFd(c->fd, false, 100);
#ifndef _WIN32
    if (sel < 0 && errno == EINTR) continue;
#endif
    if (sel < 0) return -1;
    if (sel == 0) continue;
    char buf[256];
    int n = connRecv(c, buf, (int32_t)sizeof(buf));
    if (n == -2) continue;
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

    int sel = waitFd(c->fd, false, 100);
#ifndef _WIN32
    if (sel < 0 && errno == EINTR) continue;
#endif
    if (sel < 0) return -1;
    if (sel == 0) continue;

    char buf[8192];
    int n = connRecv(c, buf, (int32_t)sizeof(buf));
    if (n == -2) continue;
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

  if (qos == 2) {
    if (qos2Seen(c, packetId)) {
      /* Persisted before the connection dropped: acknowledge again instead of
       * inserting the redelivered copy a second time. */
      char rec[2] = {(char)(packetId >> 8), (char)(packetId & 0xff)};
      int32_t rc = sendPacket(c, (uint8_t)(MS_MQTT_PUBREC << 4), rec, 2);
      free(topic);
      return rc;
    }
    /* Same, but the ACK never made it onto the wire before the disconnect. */
    if (!opts->cfg->cleanSession && opts->wasPersisted != NULL &&
        opts->wasPersisted(opts->arg, packetId)) {
      char rec[2] = {(char)(packetId >> 8), (char)(packetId & 0xff)};
      qos2Remember(c, packetId);
      int32_t rc = sendPacket(c, (uint8_t)(MS_MQTT_PUBREC << 4), rec, 2);
      free(topic);
      return rc;
    }
    if (qos2Pending(c, packetId)) {
      /* Already queued; its PUBREC follows once the batch is persisted. */
      free(topic);
      return 0;
    }
  }

  int32_t delivered = 0;
  if (opts->onMessage != NULL) {
    delivered = opts->onMessage(opts->arg, topic, body + pos, bodyLen - pos, qos,
                                packetId);
  }
  if (delivered != 0) {
    /* No ACK: force a reconnect so the broker redelivers this PUBLISH. */
    msLog(MS_LOG_WARN, "message not queued, withholding QoS ACK (packetId=%u)",
          (unsigned)packetId);
    free(topic);
    return -1;
  }
  if (qos == 2) qos2PendingAdd(c, packetId);
  /* QoS1/2 ACKs are sent by drainAcks() after the rows are persisted. */
  free(topic);
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
          uint16_t id = (uint16_t)(((uint8_t)body[0] << 8) | (uint8_t)body[1]);
          qos2Forget(c, id); /* QoS2 handshake complete: release the id */
          if (opts->forgetPersisted != NULL) opts->forgetPersisted(opts->arg, id);
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
    /* A fresh session invalidates pending QoS2 state, packet ids and any
     * SUBACK. Persisted QoS2 ids survive only when the broker keeps the
     * session (clean_session=false), because packet ids are session-scoped. */
    qos2Reset(&conn, cfg->cleanSession);
    conn.nextId = 1;
    conn.pendingSubId = 0;
    conn.subAckResult = MS_SUBACK_PENDING;
    if (opts->pollAcks != NULL) opts->pollAcks(opts->arg, NULL, 0);

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
      /* ACKs completed by the writer thread; -1 means a batch failed and the
       * unacknowledged messages must be redelivered on a new connection. */
      if (drainAcks(&conn, opts) != 0) {
        msLog(MS_LOG_WARN, "persistence failed, reconnecting to get messages redelivered");
        break;
      }

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

      /* TLS keeps decrypted bytes inside OpenSSL: select() would sleep even
       * though a complete packet is already buffered. */
      if (connPending(&conn) > 0) {
        char buf[8192];
        int n = connRecv(&conn, buf, (int32_t)sizeof(buf));
        if (n == -2) continue;
        if (n <= 0) break;
        if (ssPut(&conn.rx, buf, n) != 0 || processPackets(&conn, opts) != 0) break;
        continue;
      }

      int sel = waitFd(conn.fd, false, 100);
#ifndef _WIN32
      if (sel < 0 && errno == EINTR) continue;
#endif
      if (sel < 0) break;
      if (sel > 0) {
        char buf[8192];
        int n = connRecv(&conn, buf, (int32_t)sizeof(buf));
        if (n == -2) continue;
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
