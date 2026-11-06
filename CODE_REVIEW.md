# TDengine 代码审查报告

| 项 | 内容 |
|---|---|
| 日期 | 2026-11-06 |
| 基线 | `main @ 1f1282c`（fork `zc0350/TDengine`），上游 TDengine 3.0 社区版 |
| 审查对象 | 全仓 `source/` `include/` `packaging/` `cmake/` `test/`，重点为提交 `1f1282c` 新增的 `mqttsrc` 模块 |
| 方法 | 静态审查：分层架构分析 + 自有改动逐文件精读 + 上游高风险面抽样 + 全仓量化统计 + 关键结论逐条回读源码核实 |
| 限制 | 本机无 C 编译器（`gcc`/`cl`/`clang` 均不可用），未编译、未运行单测、未做动态/fuzz 验证；上游 137 万行代码为抽样，非逐行 |

---

## 0. 修复记录（2026-11-06 同日）

下文第 1~6 节保留**首次审查时的历史基线**；本节记录随后完成的修复。共改动 25 个既有文件 + 新增 1 个测试文件。

### 已修复

| 原编号 | 修复内容 |
|---|---|
| P0-1 | `MsMsgCb` 改为返回 `int32_t`；QoS1/2 消息强制 flush **成功落库后**才发 PUBACK/PUBREC，失败不 ACK 并断连让 broker 重投；退出时 flush 失败或有失败批次即以非零码退出 |
| P0-2 | 数值/整数列改为 `strtod`/`strtoll` 严格全串解析（`*end`/`ERANGE`/`isfinite`）后由 `%.17g`/`%lldi64` 重新格式化，不再回显原始 payload |
| P0-3 | measurement 渲染到临时缓冲后经 `ssPutEscaped(...,1)` 转义；超长 topic 在 `msMapPayload` 明确拒绝 |
| P1-1 | `sendAll` 改为 5s 总超时 + `select` 等可写 + 循环内检查 stop，EINTR/EAGAIN 不再无限忙循环 |
| P1-2 | 退出前先注销信号/控制台处理器，再 `gSrc = NULL` 后 free；stop 标志改用编译器原子内建（`__atomic_*` / `_Interlocked*`）；删除未使用变量 |
| P1-3 | QoS2 去重状态拆为 seen/remember/forget/reset，仅在落库成功后记录，PUBREL 清除，每次重连重置 |
| P1-4 | 新增 `handleSubAck` + `waitSubAck`：校验 packetId 与每个 granted QoS，`0x80` 或超时触发重连 |
| P1-5 | 主循环按 `lastRecvMs` 做半开检测（1.5×keepalive 无任何入站数据即重连） |
| P1-6 | `msSinkWrite` 每批次取一个 reqid，整个重试周期复用；函数结束（成功或失败）才递增 |
| P1-7 | rpm/deb/tar 三种清单补 `taosmqttsrc`；`preun.sh`/`prerm`/spec 卸载段补软链清理；示例配置与 `mqttsrc.h` 随包分发（`make_install.sh/.bat`、spec、makedeb、makepkg） |
| P1-8 | `taosmqttsrc` 在 Darwin 下调用 `td_fix_macos_client_libtaos_dep()` |
| P1-9 | `msStrCopy` 加 `src == dst` 短路，并对截断打 WARN |
| P2-1~2、6 | `jint`/`msStrToInt` 加范围与尾随字符校验；枚举/布尔/数组类型与上限全部改为硬失败；`when` 空值不再产生 catch-all |
| P2-3~5、7 | 新增长度感知的 `ssPutEscapedN`（控制字节含 NUL 替换为空格）；`toMilli` 饱和运算；时间戳/数值严格解析；64MB 超限明确报错 |
| P2-8 | stop 原子化；公共头收紧 `msSrcStop`/`msSrcStats` 的线程与生命周期约定 |
| P2-9~11 | 新增 `msSinkTest`（可注入 stub，覆盖重试次数/reqid 幂等/失败统计）；三个 e2e 场景接入 CTest；`msUnitTest` 默认路径改编译期宏、断言失败即中止、新增注入/转义/超长 topic 回归用例；`fake_mqtt_broker.py` 去掉 assert、初始化 stdout/stderr，设备场景年份改为占位符 |
| P2-12 | cJSON 改为复用 `toolscJson` target（`add_subdirectory` + `target_link_libraries`），`tools/taosBenchmark/deps` 加 target guard，不再重复编译源码 |
| P2-13~15 | 新增 `MQTTSRC_STRICT_WARNINGS` 开关（`-Wall -Wextra`）；Windows 安装检查 errorlevel；错误日志不再回显 payload 原值 |
| 上游 P1 | `transComm.c` 两处 realloc 改用临时指针判空、增长前做加法与上界校验、收包入口补负长度/下界校验 |
| 上游 P1 | `walChangeWrite` 六个错误分支改为 `TAOS_RETURN(terrno ? terrno : TSDB_CODE_FAILED)`，错误码来源不再失真 |
| 上游 P2 | `osMemory.c` 修正未声明标识符、calloc/realloc 的整数溢出与窄化、`fopen` 判空 |

### 部分缓解 / 未处理（需长期治理）

- **P2-13**：严格告警开关默认 OFF（项目全局 `-Werror`，本机无编译器无法验证开启后是否零告警）。CI 可用 `-DMQTTSRC_STRICT_WARNINGS=ON` 开启。
- **P2-12**：cJSON 已复用 target，但 `source/` 仍引用 `tools/taosBenchmark/deps/` 私有路径；彻底解耦需把该库上移到 `contrib/`，涉及更多构建文件。
- **P1-2 理论 UB**：POSIX 信号处理器仍访问 `volatile` 句柄指针（已通过注销处理器消除 UAF 窗口、stop 用原子写）。彻底合规需自管道或 `sigwait` 转发线程。
- **上游工程性风险**（第 4.2 节）：438 处 `strcpy`/219 处 `sprintf` 存量、147 个 >2000 行巨型文件、新旧错误约定并存、双轨依赖版本一致性、WAL 恢复静默降级、内存池分流设计风险 —— 属跨版本治理项，本轮未改。
- **动态验证**：本机无 C 编译器，未执行 `cmake`/编译/CTest/fuzz。已完成的静态验证：`git diff --check`、`bash -n`（6 个脚本）、`python -m py_compile`、全部 JSON 解析、C/CMake 括号平衡、符号引用一致性复核。

---

## 1. 结论摘要

> 本节为**修复前基线**，修复状态见第 0 节。

| 严重度 | 数量 | 主要主题 |
|---|---|---|
| P0 阻断上线 | 3 | QoS 确认早于落库导致丢数据；行协议注入（数值列、表名模板） |
| P1 高 | 9 | 无法优雅停止、UAF/信号竞态、QoS2 重连误丢、SUBACK 未校验、keepalive 僵死、reqid 幂等破坏、打包清单遗漏、macOS 不可用、snprintf 自拷贝 UB |
| P2 中 | 15 | 配置校验缺失、静默降级、NUL 截断、测试覆盖不足、构建告警与依赖结构、上游局部缺陷 |
| 工程性风险 | 6 | 危险函数存量、巨型文件、新旧错误约定并存、长度校验分层不一致等 |

**一句话结论**：上游主体工程质量属中上（分层、错误码、内存治理、CI 均有体系化约束），问题集中在局部旧代码未跟上新约定；**新增的 `mqttsrc` 模块不建议按现状上线**——功能完成度高、代码整洁，但存在 3 个数据可靠性/安全类 P0 缺陷，且打包链路实际未打通。

---

## 2. 项目概览

### 2.1 架构分层

`source/CMakeLists.txt:16-21` 的 `add_subdirectory` 顺序即单向依赖方向，`include/` 与 `source/` 镜像对应：

```
client/    对外 API（include/client/taos.h）+ wrapper/
   ↑
dnode/     mnode / vnode / qnode / snode / bnode / xnode / mgmt（6 类节点角色 + 管理面）
   ↑
libs/      ~40 个子系统：存储 wal/tdb/tfs/sync/index；查询 parser/planner/executor/
           function/nodes/qworker/scheduler/scalar；通信 transport/qcom/catalog/wlr；
           运维 monitor/audit/metrics  ← 本次新增 mqttsrc 位于此层
   ↑
common/    跨节点消息结构与编解码（tmsg.c / streamMsg.c）
util/      容器/算法/日志/编码 + 统一错误宏（tutil.h / taoserror.h）
os/        平台抽象 osMemory / osThread / osSocket / osFile / osSemaphore
```

### 2.2 量化数据（已复核）

| 指标 | 数值 |
|---|---|
| `source/` + `include/` C/C++/H | 1,354,709 行 / 1,424 文件 |
| 全仓文件 | source 1,408、include 186、test 4,598、tools 680、packaging 123、utils 54 |
| 本次新增 `mqttsrc` | 3,476 行 / 24 文件（全部为新增，无上游文件被改动逻辑） |
| TODO/FIXME/XXX/HACK | 301 |
| 危险函数 | `strcpy` 438、`sprintf` 219、`strcat` 9、`gets` 1（唯一命中是注释文字，非调用） |
| 单文件 > 2000 行 | 147 个；最大 `parTranslater.c` 43,446 行、`streamTriggerTaskTest.cpp` 39,018 行 |
| 注释样式行 | 约 93,001 行 ≈ 6.9%（仅统计以 `//`/`/*`/`*` 开头，低估行尾注释） |
| 错误约定规模 | `terrno` 13,515、`TAOS_CHECK_EXIT` 9,192、`TAOS_RETURN` 2,991；对照裸 `return -1;` 1,469 处 |
| 平台条件编译 | 708 行，os/src 20 个文件 |

---

## 3. 自有改动 `mqttsrc` 审查（逐文件精读）

模块定位：自研 MQTT 3.1.1 客户端，订阅外部 Broker，将 JSON payload 按模板/规则映射为 InfluxDB 行协议，经 schemaless 接口批量写入 TDengine。共 7 个 `.c`、1 个内部头、1 个公共头。

**总体评价**：模块边界清晰（`msMqtt` 协议 / `msMapper` 映射 / `msCfg` 配置 / `msSink` 写入 / `msSrc` 编排），错误分支释放覆盖完整，三平台（Windows/POSIX）分支齐全，cJSON 对象所有权无泄漏，唯一 SQL 拼接点 `db` 有白名单校验，SPDX 许可头齐全。缺陷集中在**协议语义、外部输入信任、生命周期与集成**四处，而非编码基本功。

### 3.1 P0 — 阻断上线

#### P0-1 QoS 确认早于持久化，写入失败静默丢数据

```c
// source/libs/mqttsrc/src/msMqtt.c:370-380
if (!dup && opts->onMessage != NULL) {
  opts->onMessage(opts->arg, topic, body + pos, bodyLen - pos, qos, packetId);
}
if (qos == 1) { ... return sendPacket(c, MS_MQTT_PUBACK << 4, ack, 2); }
if (qos == 2) { ... return sendPacket(c, MS_MQTT_PUBREC << 4, rec, 2); }
```

`onMessage` 返回 `void`（`msInt.h`），仅把消息塞进内存 batch（`msSrc.c:70`，未达 `batchSize`/`flushMs` 不落库）就立即 ACK；`flushBatch` 失败时 `msSrc.c:42` 只打日志后 `freeBatch` 丢弃；`msSink.c:81-105` 重试耗尽返回 -1 后同样丢弃。退出路径 `msSrc.c:122` 的 `flushBatch` 失败亦无补救。

- **影响**：进程崩溃/重启窗口内丢数据；写失败后 broker 已收 ACK 不再重投；**QoS1/2 实际退化为 at-most-once**，与配置承诺不符。
- **修复**：`onMessage` 改为返回写入结果，落库成功后才发 PUBACK/PUBREC；失败不 ACK（让 broker 重投）或引入本地持久化队列；退出前确认 flush 成功，失败以非零码退出。

#### P0-2 数值型列只校验首字符，把整串原样拼进行协议

```c
// source/libs/mqttsrc/src/msMapper.c:293-313
if (v->type == MSV_STRING) {
  char *end = NULL;
  strtod(v->text, &end);
  if (end == v->text) { ...return -1; }   // 只判断"能解析出前缀"
}
...
return ssPrintf(line, "%s", v->text);      // 原样输出整串
```

- **触发**：按 `conf/mqttsrc.example.json`（`current` 为 `number`），发布 `{"current":"1,evil=2", ...}` → 输出 `current=1,evil=2,...`，凭空多出字段；`int` 型列 `"100,foo=1"` → `100,foo=1i64`。
- **影响**：外部 MQTT 发布者可注入任意字段/tag、污染数据、构造语法错误使整批写入失败（拒绝服务）。**这不是 SQL 注入**（走 schemaless 行协议），但等价的数据注入。
- **修复**：整体数值解析（检查 `*end == '\0'` 与 `errno != ERANGE`），再用 `%.17g` / `%lld` 统一格式化；绝不回显原始 `v->text`。

#### P0-3 measurement（超级表名）模板渲染完全不转义

```c
// source/libs/mqttsrc/src/msMapper.c:362  —— 直接写入 line
if (msRender(spec->stable, lookup, ctx, &line) != 0) { ... }
// 对比 :401 / :410 / :443，tag 名/值、列名均先 ssPutEscaped(..., 1)
```

`lookup` 的 `_topic` 及 topic 层级变量来自外部可任意构造的 topic（`msMapper.c:100-121` 只把 `/ + # . :` 和空格替换为下划线，**不处理逗号与等号**）。

- **触发**：规则配置为 `stable:"sensor_{site}"`，发布到 topic `factory/x,injected=1/telemetry` → measurement 变成 `sensor_x,injected=1`，凭空多出 tag；含空格可提前开启字段集。
- **修复**：measurement 渲染进临时 `SStr` 后同样 `ssPutEscaped(&line, ..., 1)`；topic 变量入库前拒绝或转义 `, = 空格 " \` 等行协议分隔符。

### 3.2 P1 — 高优先级

| # | 问题 | 位置与证据 | 影响 / 修复 |
|---|---|---|---|
| P1-1 | `sendAll` 对 `EINTR`/`EAGAIN` 无限重试且不检查 stop | `msMqtt.c:126-129`：`if (... == WSAEWOULDBLOCK) continue;` / `if (errno == EINTR \|\| errno == EAGAIN) continue;` | 对端不读或信号打断时忙循环占 CPU、无视 `*opts->stop`，优雅停止失效。改用 `poll/select` 等可写 + 总超时 + 循环内检查 stop |
| P1-2 | `gSrc` 先 free 后置 NULL，信号处理器全程未注销 | `msMain.c:16-31` 的 `onSignal/onConsoleCtrl` 读 `gSrc`；`msMain.c:106-107`：`msSrcFree(gSrc); gSrc = NULL;`；`msSrc.c:150` `free(src)` | 退出窗口收到 SIGINT/CTRL_C 触发 UAF；信号上下文中调用非 async-signal-safe 函数本身也是 UB。先置 NULL 再 free，退出前恢复 `SIG_DFL`，或改用自管道/信号只写 `volatile sig_atomic_t` |
| P1-3 | QoS2 去重状态重连不重置、PUBREL 不清除 | `msMqtt.c:451` `memset(&conn,0,...)` 仅在循环外一次；`:265-268` `qos2Seen[]` 只写不删；`:406-410` PUBREL 分支只回 PUBCOMP | `cleanSession=true` 重连后 packetId 从 1 重新分配，与残留 256 项相撞，新消息被判重复静默丢弃。重连时清空 `qos2Seen/qos2Pos/nextId`，PUBCOMP 时移除对应 id |
| P1-4 | 订阅不等待、不校验 SUBACK | `msMqtt.c:341-347` 发完即 `return 0`；`:413-418` `case MS_MQTT_SUBACK: break;` | broker 拒绝订阅（granted QoS `0x80`）时日志仍显示成功，进程静默空转不收数据。同步等待并逐个校验 granted QoS，失败断连重连 |
| P1-5 | keepalive 无对端存活检测 | `msMqtt.c:428` `lastRecvMs` 全文件只写不读；`:476-479` 仅按 `lastSendMs` 发 PINGREQ，从不判断 PINGRESP | TCP 半开/网络黑洞时 `select` 持续超时返回 0，连接僵死永不重连。记录未确认 PINGREQ 时间，超过 `1.5 × keepalive` 未收到任何数据即断连重连 |
| P1-6 | 重试时递增 reqid，破坏幂等 | `msSink.c:81-90` 重试循环内传 `s->reqid++` | 首次写入服务端成功但客户端收到错误时，换 reqid 重试绕过 `*_with_reqid*` API 的去重，可能重复写入。同一批次整个重试周期复用一个 reqid |
| P1-7 | rpm/deb/tar 三种包均未收录 `taosmqttsrc` | `packaging/rpm/tdengine.spec:121`、`packaging/deb/makedeb.sh:157`、`packaging/tools/makepkg.sh:117` 三处清单只有 `taosmqtt`；本次提交只改了 `make_install.sh/.bat`、`post.sh`、`preun.sh` | 三条发行包路径安装后无该命令，`post.sh:314` 的 `-x` 判断恒假；而 `cmake --install` 走 `install.cmake:41-56` 能装上，**两条安装路径结果不一致**。卸载侧 `deb/DEBIAN/prerm:53`、`spec:302` 也漏删软链。另 3 个 `*.example.json` 与 `include/libs/mqttsrc/mqttsrc.h` 完全未随包分发，而 `msMain.c:74-76` 强制要求 `-c <config>`，装完无法开箱使用 |
| P1-8 | macOS 缺 libtaos 的 RPATH 修复 | `source/libs/mqttsrc/CMakeLists.txt:38-39` 无条件构建并链接 `${TAOS_LIB}`；全仓 `td_fix_macos_client_libtaos_dep()`（`cmake/define.cmake:471`）只被 shell/taosBenchmark/taosdump 调用；`source/libs/tmqtt/CMakeLists.txt:17` 显式排除 macOS | macOS 安装后无法定位 `driver/libtaos.dylib`，启动即失败。补 `if(TD_DARWIN) td_fix_macos_client_libtaos_dep(taosmqttsrc) endif()` 或按 tmqtt 限定平台 |
| P1-9 | `snprintf` 源/目标同一缓冲区（UB） | `msUtil.c:278` `snprintf(dst, cap, "%s", src);`；`msCfg.c:24-28` 的 `jstr` 在字段缺失时返回默认值；`msCfg.c:331` `msStrCopy(b->host, sizeof(b->host), jstr(jb, "host", b->host))` → `src == dst`，同类共 9 处（`:331/333/349/351/352/353/365/366/367`） | C 标准下 `snprintf` 的 restrict 语义使重叠为未定义行为，glibc 常恰好无碍、MSVCRT/`_FORTIFY` 下不保证。`msStrCopy` 开头加 `if (src == dst) return;`；对截断返回实际长度并报错 |

### 3.3 P2 — 中优先级

| # | 问题 | 位置 | 影响 / 修复 |
|---|---|---|---|
| P2-1 | 数值配置解析无范围/尾随字符/ERANGE 校验 | `msCfg.c:30-33` `jint` 直接 `(int32_t)valuedouble`；`msUtil.c:264-269` `strtol` 后直接窄化 | `batch_size:1e300`、`port:"1883abc"`、`ttl:99999999999` 均为 UB 或静默错误；`backoff*2`（`msMqtt.c:466`）亦可溢出成负退避。先校验 double 落在 int32 范围、`*end`/`errno` 全检，再按字段做上下界校验 |
| P2-2 | 非法配置静默降级，无失败路径 | `msCfg.c:218-223`（未知精度→ms）、`:112/170/229/260`（数组超限静默丢弃）、`:230-231`（`when` 类型错误 `continue`） | `{"when":"oops"}` 会得到 `whenNum=0` 的 catch-all 规则，**劫持全部消息**；`"create_db":"yes"` 被判 false。枚举/布尔解析失败应报错，数组超限应报错 |
| P2-3 | `$message`/`$payload` 内嵌 NUL 被静默截断 | `msMapper.c:424-427` 按长度 `memcpy` 后补 `'\0'`；`msUtil.c:143` `for (...; *c; ++c)` | 二进制/NUL payload 的原文被截断、其它控制字符未转义。按长度遍历并显式编码或拒绝控制字节 |
| P2-4 | topic 静默截断到 255 字节 | `msMqtt.c:355-358`（`MS_TEXT_LEN`） | MQTT 协议允许 65535 字节 topic；截断后 `{_topic}`、`topicVars`、规则匹配基于错误字符串，数据可能路由到错误子表。动态分配或按长度传递并明确拒绝超长 |
| P2-5 | 时间戳 `strtoll` 不校验尾随字符，`toMilli` 乘法可溢出 | `msMapper.c:269-275` | `"1700000000000junk"` 被接受；超大秒级时间戳 ×1000 有符号溢出 UB。检查 `*end`/`errno`/范围，饱和运算 |
| P2-6 | `prefix`/`contains` 条件空值恒真 | `msMapper.c:192-193` `strncmp(val, w->value, strlen(w->value))`；`msCfg.c:246` 默认 `value=""` | 配置漏写 `value` 即匹配所有消息，错误路由。要求这些 op 的 `value` 非空 |
| P2-7 | 单条大 payload 触发 64MB 上限后整条消息全丢 | `msUtil.c:102-104` 置 `failed`；`msMapper.c:549-557` `ssDetach` 返回 NULL 且未设置 err | 已成功映射的行一并丢弃、错误信息为空。分块 flush 或返回已成功部分 |
| P2-8 | stop 标志与句柄发布未用原子类型 | `msSrc.c:13` `volatile int32_t stop;`；`msInt.h:101`；`msMain.c:16` 普通 `SMsSrc *gSrc`；公共头 `:184-185` 却承诺"可从信号处理器/其它线程安全调用" | C 内存模型下为数据竞争/UB（Windows 控制台处理器确在独立线程）；`stats` 计数器无锁。改用 `_Atomic` / `volatile sig_atomic_t`，或收紧接口文档 |
| P2-9 | e2e 与自研 MQTT 协议栈零 CI 覆盖 | `test/CMakeLists.txt:8-13` 仅注册 `msUnitTest`；`taosmqttsrcDryRun`、`fake_mqtt_broker.py`、两个 scenario 均无 `add_test`；`mqttsrc/CMakeLists.txt:41` `BUILD_TEST` 默认 OFF（`cmake/options.cmake:18`） | CONNECT/SUBSCRIBE/PUBLISH/PUBACK、QoS1、重连、keepalive 全部未被 CI 执行；默认构建连 DryRun 都不产出。补 `add_test` 并注册两个 scenario |
| P2-10 | stub + 强制 dry-run 掩盖 sink 真实路径 | `test/msTaosStub.c:16-58` 8 个 stub 全返回 NULL/-1；`fake_mqtt_broker.py:113` 强制 `dry_run=True` | `taos_schemaless_insert_ttl_with_reqid_tbname_key`（`msSink.c:88-90`）、重试退避、批量 flush、写入统计从未被执行。改为可注入返回码的 fake sink 并断言重试次数与统计 |
| P2-11 | 单测可独立运行性与断言失败后的行为 | `msUnitTest.c:388-389` 默认用相对文件名；`CHECK` 宏失败仅累加不中断；`fake_mqtt_broker.py:159` `stdout` 在异常路径未初始化 → `UnboundLocalError` 掩盖真实原因，`assert` 在 `python -O` 下失效 | 用编译期宏注入绝对路径；失败即中止用例；显式初始化 `stdout/stderr=''` 并用显式检查替代 `assert` |
| P2-12 | 重复编译 cJSON，且 `source/` 反向依赖 `tools/` 私有目录 | `mqttsrc/CMakeLists.txt:10-11,20` 直接把 `tools/taosBenchmark/deps/toolscJson/src/toolscJson.c` 加入源码；该目录已有独立 target | 同一源码编译两份、绕过原 target 编译选项；`toolscJson.c:27` 的 `#pragma GCC visibility push(default)` 会把符号全局导出；taosBenchmark 被裁剪即编译失败。收敛到 `contrib/` 独立 target 并用 `target_link_libraries` |
| P2-13 | 编译告警未真正覆盖新代码 | `cmake/define.cmake:246` 全局 `-Wno-unused-result`；`:413/:449` 有 `-Werror` 但 Linux/macOS 未开 `-Wall/-Wextra`；`:166` Windows Debug 用 `/w` 关闭全部告警；`:200` `/WX` 伴随 30 余项 `/wd` | 实例：`msMain.c:103-106` 的 `SMsStats stats` 写入后从未读取，任何配置下都不告警。对 `mqttsrc` target 单独加 `-Wall -Wextra` 并删除死代码 |
| P2-14 | `WSAStartup` 泄漏 / `EINTR` 被当断连 | `msMqtt.c:451-453`（`ssInit` 失败直接 return，跳过 `:513` 的 `WSACleanup`）；`:487-496` `select`/`recv` 返回负值即 `break` | Winsock 引用计数泄漏；偶发信号导致不必要重连、统计虚高。统一单出口清理；`errno==EINTR` 时 continue |
| P2-15 | 日志回显 payload 原值 / Windows 安装无错误检查 | `msMapper.c:272,298` 把 `v->text` 写入 err，`msSrc.c:61` WARN 打印；`packaging/tools/make_install.bat:142` `copy ... > nul` 不检查 errorlevel | 敏感字段可能进日志（建议截断脱敏）；exe 缺失时安装静默成功（Linux 侧有 `[ -f ]` 判断，两端不一致） |

### 3.4 安全性专项结论

- **无传统 SQL 注入**：全模块唯一 SQL 文本拼接是 `msSink.c:44` 的 `CREATE DATABASE IF NOT EXISTS %s`，`db` 在 `msCfg.c:420-428` 被 `[A-Za-z0-9_]` 白名单严格校验，且 `msValidateConfig` 确实在 `msSrc.c:89` 与 `msCfg.c:384` 被调用（已核实调用链）。其余写入走 schemaless 行协议 API。
- **无 RCE / 格式化字符串漏洞 / 数组越界写**：无 shell 调用；`msLog`/`setErr` 的 fmt 均为字面量；所有数组循环受 `MS_MAX_*` 与 `cap` 约束；`SStr` 增长有 64MB 上限。
- **存在行协议数据注入**（P0-2、P0-3），影响面为数据污染与写入拒绝服务，非权限提升。
- **未发现密码/token 入日志**（`msSink.c` 仅打 host/port/db，MQTT 密码只在 CONNECT 报文中）；无路径遍历（配置路径来自 CLI，不与外部数据拼接）。

---

## 4. 上游主体抽样审查

### 4.1 确认的代码缺陷

| 级别 | 问题 | 证据 | 影响 / 修复 |
|---|---|---|---|
| P1 | `realloc` 失败时覆盖原指针 | `source/libs/transport/src/transComm.c:428`、`:456`：`p->buf = taosMemoryRealloc(p->buf, p->cap);` 失败返回 NULL 时原缓冲丢失；同文件 `:496-500` 用临时 `newBuf` 判空后再赋值，是正确写法 | 原缓冲区泄漏 + 连接缓冲不可用。对齐 `:496-500` 写法 |
| P1 | WAL 错误码来源失真 | `source/libs/wal/src/walWrite.c:116-168` 的 6 个错误分支裸 `return -1`，不写 `terrno`；调用方 `:186` 使用 `TAOS_CHECK_EXIT_SET_CODE`，该宏（`include/util/tutil.h:454-462`）在 `code < 0` 时执行 `code = terrno`，**丢弃函数返回值改用全局 terrno** | 诊断信息失真、错误码不受控；`terrno` 残留 0 时存在条件性误报成功风险。错误分支统一 `TAOS_RETURN(...)` 或调整宏语义 |
| P2 | `USE_TD_MEMORY` 路径无法编译 | `source/os/src/osMemory.c:339` 声明 `pTdMemoryInfo`，`:340` 却使用未声明的 `tpTdMemoryInfo`；连带 `:300` `int32_t memorySize = num * size;` 两个 int64 乘积窄化、`:267/:348` 加头无溢出检查、`:172-174` `fopen("/proc/self/maps")` 未判空即传给 `fscanf` | 调试/内存追踪路径实际腐化不可用。为 `USE_TD_MEMORY`/`USE_ADDR2LINE` 加一个 CI 编译目标 |

### 4.2 工程性风险（非确认缺陷）

- **危险函数存量与告警放宽叠加**：438 处 `strcpy`、219 处 `sprintf`；`cmake/define.cmake:413` 同时带 `-Wno-format-truncation -Wno-format-overflow`，ASan 构建 `:355` 用 `-Wformat=0`。建议分批替换为 `snprintf`/定长 `memcpy` 并逐步收紧。
- **长度校验分层不一致**：`transComm.c:477` 只校验 `msgLen >= TRANS_MSG_LIMIT` 与版本，`ntohl` 高位值先变负数可绕过上界（由 `:382 total <= 0` 兜底，暂无直接越界）；`:494-495` `p->len + len` 无加法溢出检查。应把上下界与符号检查前移到收包入口，对齐压缩路径 `:187-215`。顺带修正 `transComm.h:251` 宏体尾随分号。
- **巨型文件**：147 个 >2000 行，最大 43,446 行；约 6.9% 注释样式行。建议优先拆分 `parTranslater.c` / `planOptimizer.c` / `tmsg.c` 并补接口注释。
- **新旧错误约定并存**：1,469 处裸 `return -1;` vs 9,192 处 `TAOS_CHECK_EXIT`。仓库已有 clang AST matcher（`test/ci/filter_for_return_values`）可纳入 CI 门禁逐步收敛。
- **双轨依赖版本各写一份**：`cmake/external.cmake` 的 `URL_HASH SHA256` 与 `conanfile.py` 精确版本（zlib 1.3.1 / lz4 1.10.0 等）需加一致性校验。
- **WAL 恢复静默降级**：`walMgmt.c:226-242` 中 `.txn` 打开失败降级禁用并仅 warn、恢复失败标 non-fatal，属有意的可用性优先设计，但可能掩盖恢复后数据不完整。建议改为可配置策略并纳入启动健康检查。
- **内存池分流依赖运行时状态**（`include/os/osMemPool.h:181-189`）：分配与释放时若 `threadPoolEnabled/Session` 变化会走不同分配器，属设计风险，**当前无证据表明已发生错配**。

### 4.3 值得肯定的工程实践

1. **分层单向且带强制约束**：`include/os/osMemory.h:27-51` 在非 WINDOWS 且非 `ALLOW_FORBID_FUNC` 时把 `malloc/calloc/realloc/free/strdup` 宏替换为非法标识符，编译期禁用裸 libc 分配；统一走 `taosMemoryMalloc/Free` + 会话内存池 + `BUILD_WITH_RAND_ERR` 故障注入（`osMemory.c:278-293`）。
2. **成体系的错误码与清理范式**：1,200 个负数错误码（`taoserror.h:37-43`）、`terrno` 线程局部、`TAOS_CHECK_GOTO/EXIT` 记录 `__LINE__` 到 `lino` 形成统一 `goto _exit` 清理；`walWrite.c:829-853` 是泄漏治理与 `ftruncate` 回滚的正面范例。
3. **CI/测试成熟**：19 个 GitHub workflow、`test/ci` 下 ASan/覆盖率/返回值静态检查脚本、4,069 个用例、259 个 C++ 单测。
4. **编译与安全加固**：Linux 生产构建 `-Werror -Wformat=2 -Werror=format` + 探测式 `-fstack-protector-strong`/PIE/Full RELRO；`ExternalProject_Add` 全部带 `URL_HASH SHA256`。
5. **平台抽象完整**：`osThread.h:48-73` 把 pthread/CRITICAL_SECTION/SRWLOCK 统一 typedef，708 行条件编译集中管理。

---

## 5. 修复路线图

| 阶段 | 动作 | 验收标准 |
|---|---|---|
| 阶段一（上线前必须） | 修 P0-1 / P0-2 / P0-3；补 P1-7 打包清单与示例配置 | QoS1 写入在进程 kill -9 后不丢已 ACK 数据；注入 payload 无法产生额外字段；三种安装包均含 `taosmqttsrc` 与 example 配置 |
| 阶段二 | 修 P1-1 ~ P1-6、P1-8、P1-9 | SIGINT 后 1s 内干净退出；半开连接 1.5×keepalive 内重连；SUBACK 拒绝时报错而非空转；macOS 可启动 |
| 阶段三 | 修 P2 配置/映射健壮性；把 e2e 接入 CTest 并改造 fake sink；收紧新代码编译告警 | `ctest` 覆盖 CONNECT/QoS1/重连/重试统计；`-Wall -Wextra` 零新增告警 |
| 阶段四（上游） | 修 `transComm.c:428/456`、`walWrite.c` 错误码、`osMemory.c:340`；为危险函数与巨型文件设长期收敛计划 | 三个 CI 编译目标（含 `USE_TD_MEMORY`）全绿 |

**测试建议**：优先补三类用例——① 恶意 payload/topic 注入（数值列、measurement、`$message` 含 NUL）；② kill -9 / 断网 / broker 拒绝订阅下的数据不丢与重连；③ 打包产物在 rpm/deb/tar/macOS 上的安装与启动冒烟。

---

## 6. 审查局限声明

- 全仓 137 万行代码**未逐行审查**；上游部分为架构级分析 + 高风险面抽样（每个方向 2-4 个代表文件）。
- 本机无 C 编译器，**未编译、未运行任何单测或 e2e、未做动态验证与 fuzz**；所有结论基于源码事实与 C 语言语义推演，已在第 3 节对关键条目逐条回读核实。
- 未做性能剖析、并发压力测试、内存泄漏运行时检测（ASan/Valgrind）；P1-5 等时序类问题建议以集成测试复现后再定最终严重度。
- 上游缺陷（第 4.1 节）多为非默认编译宏或错误分支路径，默认构建是否触发需实测确认，报告已与"工程性风险"明确区分。
