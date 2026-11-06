# TDengine 源码二次审查报告（Round 2）

| 项 | 内容 |
|---|---|
| 日期 | 2026-11-06 |
| 基线 | `main @ 7b4e79e`（首轮修复提交），上游 TDengine 3.0 社区版 |
| 范围 | ① 首轮 25 个修复文件的复验；② `mqttsrc` 全模块逐行精读 + 对抗性测试；③ 上游抽样换向：client schemaless 行协议解析、transport 解压、容器（hash/array）、scheduler |
| 方法 | 与首轮不同：本轮**本机有 MSVC 14.51（VS BuildTools 18）**，补齐了真实编译、静态分析、单测、e2e 与对抗性探测；不再是纯静态推演 |
| 工具链 | `cl.exe` /W4 + `/analyze`；Python 3.13 驱动假 Broker；`vcvars64.bat` 因沙箱屏蔽 `reg.exe` 不可用，改为手工设置 `INCLUDE/LIB` |
| 限制 | 上游 137 万行仍为定向抽样；未跑 TDengine 服务端与真实 Broker；未做 ASan/Valgrind（Windows 侧未启用） |

---

## 0. 结论摘要

| 严重度 | 数量 | 主题 |
|---|---|---|
| P1 | 2 | `clean_session=true` 抵消“不 ACK 等重投”；QoS1 逐条 flush 使批处理失效并阻塞收包 |
| P2 | 6 | 端口静默截断；仅密码无用户名导致 CONNECT 非法；模板语法不校验导致全量静默丢数据；字符串 255 字节静默截断；无 TLS/可观测性不足；e2e 覆盖面被高估 |
| P3 | 3 | tag/measurement 过度转义 `"`；API 契约与 C4090 告警；未知配置键静默忽略 |
| 上游 | 1 | `transDecompressMsg` 未清 `comp` 标志（一致性，当前无触发路径） |

**一句话结论**：首轮 P0 修复在**功能上确实生效**（注入被拒、measurement 已转义、QoS 确认在落库后），但 `mqttsrc` 仍不适合直接上生产——最关键的可靠性补丁被默认配置（`clean_session=true`）抵消，QoS1 路径的批处理被彻底旁路；另有 3 处“配置写错也不报错、运行时静默丢数据”的失败模式。

**本轮正面结论**：`mqttsrc` 六个源文件在 MSVC `/W4` 与 `/analyze` 下**零告警**；单测 73/73、sink 测试 10/10、三个 e2e 场景全部通过；首轮修复未引入回归。

---

## 1. 首轮修复的实测复验

### 1.1 编译与测试

| 项 | 结果 |
|---|---|
| 6 个源文件 `/W4 /std:c11` | 0 warning / 0 error（仅 `msMain.c:110` 一条 C4090，见 N-9） |
| `/analyze`（微软静态分析器） | 无指向 `mqttsrc` 的告警，输出仅 `winnt.h`/`ws2tcpip.h` SDK 噪音 |
| `msUnitTest` | 73 passed / 0 failed |
| `msSinkTest` | 10 passed / 0 failed（重试 3 次、reqid 复用、失败计数均符合预期） |
| e2e 默认场景 | PASS，2 行 |
| e2e 设备场景 | PASS，2 行 |
| e2e 规则场景 | PASS，5 行 |

### 1.2 逐项复验

| 首轮编号 | 复验结论 | 证据 |
|---|---|---|
| P0-1 QoS 确认晚于落库 | **部分生效** | `msSrc.c:86-91` 确实在 flush 成功后才 return 0；`msMqtt.c:510-516` 失败不 ACK 并断连。但默认 `clean_session=true` 使 broker 在断连时丢弃会话，重投承诺不成立（N-1） |
| P0-2 数值列注入 | **已修复（实测）** | 发布 `"current":"1,evil=2"`、`"voltage":"3,evil=4"` 均被拒，日志 `column 'current' expects a finite number`，无输出行 |
| P0-3 measurement 注入 | **已修复（实测）** | topic `factory/x,injected=1/telemetry` + 规则 `stable:"sensor_{site}"` → `sensor_x\,injected\=1`，未产生额外 tag |
| P1-1 sendAll 忙循环 | 已修复（代码） | `msMqtt.c:123-164` 5s 总超时 + select 等可写 + 循环内检查 stop |
| P1-2 信号 UAF | 已修复（代码） | `msMain.c:114-121` 先注销处理器再 free；stop 用 `_InterlockedExchange`/`__atomic_store_n` |
| P1-3 QoS2 去重 | 已修复（实测） | 同一 packetId 连发两次 PUBLISH，收两次 PUBREC、仅 1 行输出、PUBCOMP 正常 |
| P1-4 SUBACK 校验 | 已修复（实测） | e2e 假 Broker 回 0x90 0x00…，客户端 `SUBACK accepted 1 topic filter(s)` 后才进入收包循环 |
| P1-5 keepalive 半开检测 | 已修复（代码） | `msMqtt.c:646` 1.5×keepalive 无入站数据即断连 |
| P1-6 reqid 幂等 | 已修复（实测） | msSinkTest：3 次尝试均为 reqid=2 |
| P1-7 打包清单 | 已修复（核对） | rpm/deb/tar/make_install.sh/.bat 均含二进制、头文件、`examples/mqttsrc/*.example.json`；卸载侧补软链清理 |
| P1-8 macOS rpath | 已修复（代码） | `mqttsrc/CMakeLists.txt:46-48` |
| P1-9 snprintf 自拷贝 | 已修复（代码） | `msUtil.c:291` `src == dst` 短路 |

**结论**：首轮修复质量整体可信，未见回归；未闭环的两项（P0-1 的会话语义、P2-7 的整批丢弃）由 N-1 与下文说明接续。

---

## 2. 新发现缺陷

### N-1（P1）`clean_session=true` 抵消“不 ACK 催重投”，P0-1 的修复在默认配置下失效

```c
// msCfg.c:85
b->cleanSession = true;
// msMqtt.c:325
uint8_t flags = (uint8_t)(cfg->cleanSession ? 0x02 : 0x00);
```

- 机制：写入失败时 `msSrc.c:90` 返回 -1 → `msMqtt.c:510-516` 不发 PUBACK 并断连，注释承诺“broker 将重投”。
- 事实：MQTT 3.1.1 §3.1.2.4 规定 `CleanSession=1` 的会话仅存活于本次网络连接，**断连即丢弃会话状态，包含未确认的 QoS1/2 消息**。因此重连后 broker 不会重投，消息仍然丢失。
- 影响：TDengine 不可用期间，QoS1 依然退化为 at-most-once，与首轮修复目标相反；且这是**默认配置**，示例配置也未显式写 `clean_session`。
- 另：默认 `client_id` 固定为 `taosx-mqttsrc`（`msCfg.c:83`），同 Broker 上跑两个实例会互踢，也无法支撑持久会话。
- 建议：默认改 `clean_session=false`；`client_id` 默认拼接主机名/PID；或在本地做失败批次落盘（WAL/spool），把“不丢”从 broker 语义中解耦。

### N-2（P1）QoS1/2 每条消息强制 flush：批处理完全失效，且阻塞收包线程

```c
// msSrc.c:86-91
if (qos > 0) {
  if (flushBatch(src) != 0) failed = -1;
  return failed;
}
```

- `batch_size`（默认 1000）与 `flush_ms` 对 QoS1 **永不生效**：每条消息一次同步 `taos_schemaless_insert`。
- 失败时 `msSink.c:87-110` 最多 4 次尝试、退避 0.5/1/1.5s，单条消息可阻塞主循环约 3s；期间不读 socket、不发 PINGREQ，broker 侧接收缓冲堆积直至断连，形成“重连风暴”。
- 吞吐上限 ≈ 1 / 插入 RTT，批量写入的性能优势（TDengine 最核心的优势之一）被抹平。
- 建议：引入有界队列 + 后台写线程；按 `flush_ms` 聚合一个窗口内的 QoS1 消息，整批提交成功后**批量回 ACK**，既保留批处理又保留“落库后才确认”的语义。

### N-3（P2）端口字段静默截断，错误配置变成“另一个端口”

```c
// msCfg.c:448 / :472
b->port = (uint16_t)jint(jb, "port", b->port);
s->port = (uint16_t)jint(js, "port", s->port);
```

- `jint` 允许到 `INT32_MAX`，强制转 `uint16_t` 直接截断；校验仅判 `port == 0`（`msCfg.c:530`）。
- 实测（`--check`）：`mqtt.port=70000` → 通过，实际连 **4464**；`mqtt.port=-1` → 通过，实际连 **65535**；`tdengine.port=0` 也通过（运行时才连接失败）。
- 建议：转换前校验 `1..65535`，越界即报错。

### N-4（P2）只配密码不配用户名 → 生成非法 CONNECT，被 Broker 拒绝后无限重连

```c
// msMqtt.c:326-327
if (cfg->username[0] != '\0') flags |= 0x80;
if (cfg->password[0] != '\0') flags |= 0x40;   // 与 username 无关联
```

- MQTT 3.1.1 §3.1.2.9：User Name Flag 为 0 时 Password Flag **必须**为 0。
- 实测抓包：仅配 `password` 时 CONNECT flags = **0x42**（password 置位、username 未置位）。
- 影响：EMQX/Mosquitto 等会以 CONNACK 0x04 或直接断链拒绝；客户端只打 WARN 并按退避无限重连，表现为“连不上但配置检查通过”。
- 建议：`msValidateConfig` 增加配对校验；或密码非空时自动置 username 位并告警。

### N-5（P2）模板语法不做校验：写错一个 `{` 即 100% 丢数据，且进程仍 ACK、退出码 0

- 校验只查非空：`msCfg.c:571-574`；渲染失败发生在每条消息上（`msMapper.c:383-388`），随后 `msSrc.c:61-67` 仅打 WARN 并 `return 0`（照常 ACK）。
- 实测：`mapping.table = "t_{dev"` 时 `--check` 返回 **rc=0 / config OK**；运行时每条消息丢弃，进程无异常退出。
- 同类：`{unknown_key}`、`{}`、超长 key 均通过配置检查。
- 影响：最典型的人为故障（少写一个右括号）表现为“管道健康但零数据”，且统计只在退出时打印（`msSrc.c:143-149`），运维难以发现。
- 建议：加载时做模板语法扫描（括号配对、key 字符集、已知内置变量白名单）；渲染失败按配置错误在启动阶段失败。

### N-6（P2）字符串字段超 255 字节静默截断

- 数据结构决定了上限：`mqttsrc.h:32` `MS_TEXT_LEN 256`，`SMsValue.text[MS_TEXT_LEN]`（`msInt.h:108`），`msUtil.c:253`、`msMapper.c:167` 两处 `snprintf` 截断，无任何告警。
- 实测：`note` 给 300 个 `N` → 输出 255 个，行内无提示。
- 影响：长文本/序列号/JSON 片段被静默改写；`$payload`/`$message` 走 raw 路径不受影响，普通列与 tag 模板受影响。
- 建议：值改为 `(payload 偏移, 长度)` 引用或动态缓冲；至少在一次运行内对每个字段告警一次并写入文档。

### N-7（P2）无 TLS、无 will、仅 MQTT 3.1.1：凭据与数据明文过网

- `msMqtt.c:10` 自述 “Not supported: TLS, MQTT 5.0, will message”。
- `username/password` 直接进 CONNECT 明文（`msMqtt.c:332-333`），Broker 侧 1883 端口即明文。
- 影响：生产环境只能部署在可信内网或前置 stunnel；文档缺失（`source/libs/mqttsrc` 无 README，`docs/` 无对应页面），使用者不知道这条边界。
- 建议：至少支持 `tls: true` + CA 路径（或明确要求前置代理），并补一页配置说明。

### N-8（P2）可观测性不足：数据丢失只能靠日志与退出码发现

- 统计只在退出时打印（`msSrc.c:143-149`）；运行中无周期指标、无健康端口。
- `parseErrors`、`failedBatches` 仅累加；`onMessage` 的配置类错误同样只 WARN。
- 加上 N-5，运维看到的是“进程在跑、CPU 很低、库里没数据”。
- 建议：每 N 秒输出一次统计行（或暴露 `/metrics`）；把 `parseErrors` 连续增长作为可告警指标。

### N-9（P3）接口契约、告警与配置宽松

| 项 | 位置 | 说明 |
|---|---|---|
| 失败仍发布句柄 | `msSrc.c:121` | `*ppSrc = src` 在 `msSinkOpen` 之前；失败返回 -1 但 `*ppSrc` 非空，调用方必须记得 free。建议失败时回写 NULL 或在文档中写明 |
| volatile 丢弃告警 | `msMain.c:110` | `&gSrc`（`SMsSrc *volatile *`）传给 `SMsSrc **`，MSVC `/W4` 报 **C4090**；Linux 侧无告警但语义同样被丢弃 |
| 未知配置键静默忽略 | `msCfg.c` 全文 | 顶层与节内均无白名单；`"column"` 拼错会静默退回默认模型 |
| `::` 简写与合法 topic 冲突 | `msCfg.c:160` | 过滤器本身允许含 `::`，会被误拆成 `filter` + `qos` |
| 固定 client_id | `msCfg.c:83` | 多实例互踢（与 N-1 同源） |

### N-10（P2）e2e 覆盖面被注释高估，关键路径仍无自动化

- `test/CMakeLists.txt:40-41` 声称覆盖 “CONNECT/SUBSCRIBE/SUBACK/PUBLISH/PUBACK, QoS1, reconnect and keepalive”。
- 实际 `fake_mqtt_broker.py`：`listen(1)` 只 accept 一次（`:107`），发两条消息后 `sleep(1)` 即终止（`:145-150`）——**没有重连、没有 PINGREQ/PINGRESP、没有 QoS2、从不读取 PUBACK**，断言只检查“期望行存在”，重复行/多余行不会失败。
- Windows 上 `proc.terminate()` 是 TerminateProcess（硬杀），优雅退出、最终退出码、flush 收尾均未验证。
- 未覆盖本轮发现的回归点：长字符串截断、端口越界、密码/用户名配对、模板语法、tag 内引号转义。
- 建议：假 Broker 改成可脚本化的状态机（断链/拒绝订阅/延迟 ACK/重复投递），断言改为“输出行集合完全相等”。

---

## 3. 上游抽样（本轮换向）

### 3.1 首轮修复复核（均正确）

| 位置 | 结论 |
|---|---|
| `transComm.c:424-436 / 455-475` | realloc 改用临时指针，失败保留原缓冲；增长前做加法与上界校验，正确 |
| `transComm.c:486-497` | 收包入口补 `msgLen < sizeof(STransMsgHead)` 下界，`ntohl` 负值不再绕过上界，正确 |
| `walWrite.c:117-168` | 6 个错误分支改 `TAOS_RETURN(terrno ? terrno : TSDB_CODE_FAILED)`；`TSDB_CODE_FAILED == -1`（`taoserror.h:71`），`TAOS_RETURN` 同时写 `terrno` 并返回，语义与调用方 `TAOS_CHECK_EXIT_SET_CODE` 匹配，正确 |
| `osMemory.c:300-360` | `calloc` 乘法与 `INT32_MAX` 上界、`realloc` 负数/上界、`fopen` 判空，正确 |

### 3.2 新观察

| 级别 | 问题 | 证据 | 影响 |
|---|---|---|---|
| P3 | 解压后未清 `comp` 标志 | `transComm.c:252-255` 只重写 `msgLen`；对照 `transDecompressMsgExt:303` 显式 `pNewHead->comp = 0` | 解压后的报文头部仍自称“已压缩”。当前无消费方在解压后再读 `comp`（`transSvr.c:937` 之后不再判断），**暂无触发路径**；但任何新增的转发/重入路径按该标志判断就会二次解压垃圾数据。建议与 Ext 版本对齐清零 |

### 3.3 本轮定向抽查未发现新缺陷的区域

- `clientSmlLine.c`（592 行，逐行）：measurement/tag/field 三段解析的转义配对、引号计数、长度上界均自洽；`*(sql-1)` 模式因 `&&` 短路不会越过行首（仅在当前字符为分隔符时求值）。
- `clientSml.c`（schemaless 主干抽样）：`smlBuildInvalidDataMsgLen` 的 `strncat` 长度预算正确；`smlParseTableName` 的 var-data 类型检查、`tstrncpy` 上界正确。代码中多处新注释（“same union restriction…”、“never read past its declared end”）显示该区域近期已做过安全加固。
- `thash.c`：`taosHashCapacity` 的 1GB 上限、扩容时 realloc/calloc 失败分支、`refCount` 延迟释放逻辑均正确。
- `tarray.c`：容量倍增与 `BOUNDARY` 分支无溢出；`taosArrayAddBatch` 传负数会先被容量检查拦下（极端下可能长循环，非内存破坏）。
- `scheduler.c`：job 引用与 `smlProcess` 生命周期无异常。

**说明**：本轮上游为定向抽样（约 5 个文件、逐行约 5 千行 + 交叉引用），不构成对 137 万行的完整结论；首轮已列出的工程性风险（危险函数存量、巨型文件、新旧错误约定并存）依然成立，本轮未重复统计。

---

## 4. 建议修复顺序

| 阶段 | 动作 | 验收 |
|---|---|---|
| 立即（上线前） | N-1 改默认 `clean_session=false` + 唯一 client_id；N-2 引入队列 + 批量 ACK | 断开 Broker 10s 后重连，落库失败的消息可重投；QoS1 吞吐与 batch_size 正相关 |
| 高优 | N-3 端口范围校验；N-4 用户名/密码配对；N-5 模板语法校验 | 4 类错误配置在 `--check` 阶段全部失败并给出字段路径 |
| 中优 | N-6 字符串长度治理；N-10 假 Broker 状态机化；N-8 周期统计 | 长字符串不截断或有告警；e2e 覆盖重连/拒绝订阅/重复投递；运行中可见指标 |
| 低优 | N-7 TLS 或文档化限制；N-9 契约与告警清理 | 有 README/文档页；`/W4` 零告警 |
| 上游 | `transDecompressMsg` 清 `comp` | 与 Ext 版本行为一致 |

---

## 5. 本轮验证的可复现命令

```bat
REM 编译（vcvars 被沙箱屏蔽 reg.exe，手工注入 INCLUDE/LIB）
cl /nologo /c /W4 /std:c11 /D_CRT_SECURE_NO_WARNINGS ^
   /Isource\libs\mqttsrc\inc /Iinclude\libs\mqttsrc /Iinclude\client /Iinclude ^
   /Itools\taosBenchmark\deps\toolscJson\inc ^
   /Fo:.review_build\ source\libs\mqttsrc\src\*.c

REM 测试
.review_build\msUnitTest.exe source/libs/mqttsrc/conf/mqttsrc.example.json ^
                                source/libs/mqttsrc/conf/mqttsrc.rules.example.json
.review_build\msSinkTest.exe
python source/libs/mqttsrc/test/fake_mqtt_broker.py .review_build\taosmqttsrcDryRun.exe ^
       source/libs/mqttsrc/conf/mqttsrc.example.json
```

对抗性探测（本轮临时脚本，未入库）：数值/时间戳注入、topic 注入、字符串内换行、tag 内逗号/等号/引号、300 字节 topic、300 字节字符串、QoS2 重投、JSON 数组、`clean_session` 关闭场景、12 组配置边界。

---

## 6. 局限

- 未连接真实 MQTT Broker 与 TDengine 集群：N-1 的“不重投”依据协议规范与代码路径推演，未在真实 Broker 上做断链重投复现。
- 未做性能压测：N-2 的吞吐结论来自代码路径（每条消息一次同步插入 + 阻塞重试），非实测 QPS。
- 上游为定向抽样；`clientSml*.c` 之外的客户端路径、执行器、存储层本轮未展开。
- 未启用 ASan/UBSan（Windows 工具链），内存类问题仅靠静态分析与 `/analyze`。
