# Apache Iceberg 每日动态分析报告

> **日期：** 2026-05-27（UTC）
> **Fork 同步状态：** ✅ 已同步至 `apache/iceberg` 最新 `main` 分支（ebd0100）
> **数据范围：** 2026-05-27 00:00:00 UTC — 23:59:59 UTC

---

## 概览统计

| 类别 | 数量 |
|------|------|
| 🔀 合并 PR | 4 |
| 🆕 新增 Issue | 2 |
| 📬 新增 PR | 13 |

---

## 一、Fork 同步详情

本次同步将 `vinlee19/iceberg` 的 `claude/happy-ride-ftCIi` 分支从 `f12e0cf` 快进合并至 `ebd0100`：

```
Updating f12e0cf..ebd0100
Fast-forward
 docs/docs/spark-configuration.md                   |  2 +
 flink/v2.0/…/OperatorTestBase.java                 | 19 ++++--
 flink/v2.1/…/OperatorTestBase.java                 | 15 +++--
 flink/v2.0-hadoop/…/OperatorTestBase.java          | 15 +++--
 open-api/rest-catalog-open-api.py                  | 13 ++++
 open-api/rest-catalog-open-api.yaml                | 74 +++++++
 spark-extensions/…/RESTCatalogServer.java          |  2 +-
 spark-extensions/v3.5/…/SparkRowLevelOperations…   | 70 ----
 spark-extensions/v3.5/…/TestRewriteTablePath…      |  7 --
 spark-extensions/v3.5/…/TestStructuredStreaming…   | 39 ----
 spark-extensions/v4.0/…/SparkRowLevelOperations…   | 58 ----
 spark-extensions/v4.0/…/TestRewriteTablePath…      |  7 --
 spark-extensions/v4.0/…/TestStructuredStreaming…   | 39 ----
 spark-extensions/v4.1/…/SparkRowLevelOperations…   | 58 ----
 spark-extensions/v4.1/…/TestRewriteTablePath…      |  7 --
 spark-extensions/v4.1/…/TestStructuredStreaming…   | 39 ----
 16 files changed, 137 insertions(+), 327 deletions(-)
```

---

## 二、已合并 PR 深度分析

### 🔀 PR #16548 — Flink: Fix flaky `TestMonitorSource.testStateRestore`

- **作者：** [@wombatu-kun](https://github.com/wombatu-kun)
- **合并时间：** 2026-05-27 14:20 UTC
- **标签：** `flink`
- **关联 Issue：** [#16546](https://github.com/apache/iceberg/issues/16546)

#### 问题背景

`TestMonitorSource.testStateRestore` 是 Flink v2.0 / v2.1 中的测试，间歇性地因 `CollectingSink.poll` 超时（5 秒）而失败，CI 日志中可见 `TimeoutException`。

#### 根本原因

```
原有逻辑（有缺陷）：

closeJobClient(JobClient, File)
  ├── 调用 stopWithSavepoint(...)  ← 丢弃返回的 Future
  ├── 等待 savepointDir.listFiles(File::isDirectory).length == 1
  └── ← 目录创建 ≠ savepoint 写完！

测试 Phase 2 还原 job
  └── clusterClient.submitJob(...)  ← 可能在 savepoint 未完成时恢复，导致空输出
```

Flink 的 savepoint 流程分两步：
1. 创建 savepoint **目录**（早于 `_metadata`/state 文件写入完毕）
2. 写入所有 state 文件（晚于目录创建）

原代码通过检测目录出现来判断 savepoint 完成，但这只能说明进程开始了 savepoint，而不是完成了写入。Phase 2 恢复时若 state 文件尚未写完，作业无法正确恢复，`CollectingSink` 永远收不到数据，5 秒后 poll 超时。

#### 修复方案

```java
// 修复前：丢弃 Future，靠目录出现来判断完成
jobClient.stopWithSavepoint(false, savepointDir.getPath(), SavepointFormatType.CANONICAL);
Awaitility.await().until(() -> savepointDir.listFiles(File::isDirectory).length == 1);
return savepointDir.listFiles(File::isDirectory)[0].getAbsolutePath();

// 修复后：await Future，savepoint 路径由 Future 直接返回
try {
  return jobClient
      .stopWithSavepoint(false, savepointDir.getPath(), SavepointFormatType.CANONICAL)
      .get();  // 阻塞至 savepoint 完全写入
} catch (InterruptedException | ExecutionException e) {
  throw new RuntimeException(e);
}
```

#### 变更文件

| 文件 | 变更 |
|------|------|
| `flink/v2.0/…/OperatorTestBase.java` | +8 / -4 |
| `flink/v2.1/…/OperatorTestBase.java` | +8 / -4 |
| `flink/v2.0-hadoop/…/OperatorTestBase.java` | +8 / -4 |

> **影响范围：** 仅测试基础设施，不影响生产代码。修复方式参照了同仓库 `TestIcebergSourceFailover.testBoundedWithSavepoint` 中已有的 `.get()` 惯用法。

---

### 🔀 PR #16549 — Spark: Trim row-level test parameter rows from 6 to 3

- **作者：** [@stevenzwu](https://github.com/stevenzwu)
- **合并时间：** 2026-05-27 18:34 UTC
- **标签：** `spark`, `OPENAPI`

#### 背景

`SparkRowLevelOperationsTestBase` 是 9 个子测试类的参数化基类，包含 `TestCopyOnWriteMerge`、`TestMergeOnReadMerge`、`TestCopyOnWriteDelete` 等。原来每个 Spark 版本运行 6（v4.x）或 7（v3.5）行参数组合，CI 时间成本高。

#### 参数矩阵优化

**优化前（6 行，v4.0/v4.1）：**

| # | Catalog | Format | Distribution | Branch | Planning | fmtV |
|---|---------|--------|-------------|--------|----------|------|
| 1 | testhive | ORC | NONE | MAIN | LOCAL | v2 |
| 2 | testhive | PARQUET | NONE | test | DISTRIBUTED | v2 |
| 3 | testhadoop | PARQUET | HASH | null | LOCAL | v2 |
| 4 | spark_catalog (Hive) | AVRO | RANGE | test | DISTRIBUTED | v2 |
| 5 | testhadoop | PARQUET | HASH | null | LOCAL | **v3** |
| 6 | spark_catalog (Hive) | AVRO | RANGE | test | DISTRIBUTED | **v3** |

*问题：行 3+5、行 4+6 仅在 `formatVersion` 上有区别，属于重复覆盖*

**优化后（3 行）：**

| # | Catalog | Format | Distribution | Branch | Planning | fmtV |
|---|---------|--------|-------------|--------|----------|------|
| 1 | testhive | ORC | NONE | MAIN | LOCAL | v2 |
| 2 | testhive | PARQUET | HASH | null | DISTRIBUTED | v2 |
| 3 | **spark_catalog (REST)** | AVRO | RANGE | test | DISTRIBUTED | **v3** |

**覆盖率验证：** 所有轴的每个取值至少保留一行覆盖。`testhadoop` 被移除（HadoopCatalog 非生产推荐），`spark_catalog` 从 Hive 改为 REST（REST 是 OSS 战略目录）。

#### 附带修复：`RESTCatalogServer` 路径 scheme 问题

```java
// 修复前（无 URI scheme）
new File(warehouseDir).getAbsolutePath()
// → "/tmp/iceberg_warehouse/..."  ← Paths.get(URI.create(...)) 抛 IllegalArgumentException

// 修复后（带 file:// scheme）
new File(warehouseDir).toURI().toString()
// → "file:///tmp/iceberg_warehouse/..."  ← 与 HiveCatalog/HadoopCatalog 行为一致
```

#### 预期效果

每个子类测试调用量减少 **50%**（v3.5 减少约 57%），CI 每个 `extensions` 矩阵单元节省约 **6-7 分钟**。

#### 变更文件

| 文件 | 变更 |
|------|------|
| `SparkRowLevelOperationsTestBase.java` (v3.5/v4.0/v4.1) | 大幅删减参数行 |
| `TestRewriteTablePathProcedure.java` (v3.5/v4.0/v4.1) | -7 行 |
| `rest/RESTCatalogServer.java` | 1 行修复 |
| `open-api/rest-catalog-open-api.yaml` | +74 行 |
| `open-api/rest-catalog-open-api.py` | +13 行 |

---

### 🔀 PR #16559 — Spark: Trim `TestStructuredStreamingRead3` parameter rows from 8 to 2

- **作者：** [@stevenzwu](https://github.com/stevenzwu)
- **合并时间：** 2026-05-27 18:35 UTC
- **标签：** `spark`

#### 背景

`TestStructuredStreamingRead3` 是 Spark core CI 中 **CPU 消耗最高的测试类**，占总 CPU 的 20.3%（约 931 CPU-秒，出自 `(17, 4.1, 2.13, core)` 矩阵）。原来每个 Spark 版本有 8 行参数组合（4 catalog × async {true, false}），每次 CI 运行 264 个测试调用。

#### 参数矩阵优化

**优化前（8 行）：**

| # | Catalog | Async |
|---|---------|-------|
| 1 | testhive | false |
| 2 | testhive | true |
| 3 | testhadoop | false |
| 4 | testhadoop | true |
| 5 | testrest | false |
| 6 | testrest | true |
| 7 | spark_catalog (Hive) | false |
| 8 | spark_catalog (Hive) | true |

**优化后（2 行）：**

| # | Catalog | Async |
|---|---------|-------|
| 1 | testhive | **true** |
| 2 | testrest | **false** |

#### 设计理由

- **Streaming read 语义与 catalog 无关。** 一旦表被加载，streaming read 经过 Spark micro-batch source → DSv2 read path → `IncrementalScan`，catalog backend 只在表解析时参与。
- 保留 testhive（生产目标）和 testrest（OSS 战略 catalog），覆盖两个 async 值。
- 移除 testhadoop（非生产推荐）和 spark_catalog（wrapper 差异在 DDL/表解析路径，非 streaming read）。

#### 效果

| 指标 | 优化前 | 优化后 |
|------|--------|--------|
| 每类调用数 | 264 | **66** |
| 削减比例 | — | **75%** |

#### 变更文件

| 文件 | 变更 |
|------|------|
| `TestStructuredStreamingRead3.java` (v3.5/v4.0/v4.1) | 各 -39 行 |

---

### 🔀 PR #16557 — Docs: Document adaptive split sizing configurations

- **作者：** [@pratham76](https://github.com/pratham76)
- **合并时间：** 2026-05-27 19:23 UTC
- **标签：** `docs`
- **关联 Issue：** [#16556](https://github.com/apache/iceberg/issues/16556)

#### 背景

PR #16088 为 Spark 读取路径引入了 **Adaptive Split Sizing（自适应分片大小）** 功能，但当时遗漏了配置文档。本 PR 补充文档。

#### 新增文档内容

在 `docs/docs/spark-configuration.md` 的 Spark 配置表中新增两行：

```diff
+ | spark.sql.iceberg.read.adaptive-split-size.enabled     | Table default |
+   Enables adaptive split sizing for read operations. When enabled, split size is
+   automatically adjusted based on scan size and parallelism                       |
+
+ | spark.sql.iceberg.read.adaptive-split-size.parallelism | max(spark.default.parallelism,
+   spark.sql.shuffle.partitions) | Overrides the parallelism used for adaptive split sizing.
+   Must be greater than 0                                                          |
```

#### 功能说明

**Adaptive Split Sizing** 的工作原理：
- 根据扫描总大小和 Spark 并行度自动调整每个 split 的大小
- 避免小表产生过多小 split（overhead 大）或大表 split 过少（并行度不足）
- `parallelism` 参数默认取 `max(spark.default.parallelism, spark.sql.shuffle.partitions)`

#### 变更文件

| 文件 | 变更 |
|------|------|
| `docs/docs/spark-configuration.md` | +2 行 |

---

## 三、新增 Issue 分析

### 🐛 Issue #16581 — SchemaUpdate 无法在同一更新中按新名称移动列

- **提交者：** [@TwinklerG](https://github.com/TwinklerG)
- **时间：** 2026-05-27 11:12 UTC
- **标签：** `question`
- **链接：** https://github.com/apache/iceberg/issues/16581

#### 问题描述

用户发现在同一个 `UpdateSchema` 操作中，先 `renameColumn` 再 `moveFirst` 时，`moveFirst` 无法使用新名称：

```java
// 失败示例：
Schema newSchema = new SchemaUpdate(schema, SCHEMA_LAST_COLUMN_ID)
    .renameColumn("data", "data_renamed")
    .moveFirst("data_renamed")   // ← 抛出：Cannot move missing column: data_renamed
    .apply();

// 必须用原名：
.moveFirst("data")  // 原始名称才能工作
```

#### 分析

这是一个 API 语义问题：`SchemaUpdate` 的各操作是否应该在同一 update 链中共享中间状态（rename 的结果）？目前的行为是：move 操作在 rename 执行之前无法感知新名称，需要使用原始名称。是否应该让 SchemaUpdate 在同一批次内解析 pending rename 名称，这是一个设计决策。

---

### 💡 Issue #16573 — Core: Add table-level filtering for MetricsReporter implementations

- **提交者：** [@moomindani](https://github.com/moomindani)
- **时间：** 2026-05-27 00:49 UTC
- **标签：** `feature request`
- **链接：** https://github.com/apache/iceberg/issues/16573

#### 功能需求

在大型部署中，用户希望限制哪些表的 `ScanReport`/`CommitReport` 被转发给 `MetricsReporter`，但现有机制不够完善：

- OTel 的 `iceberg.otel.metrics.attributes` allowlist 只控制属性维度，不能阻止指标被发出
- 每个 reporter 实现不同的过滤逻辑会导致不一致

#### 提案

在 catalog 层面引入跨 reporter 的统一过滤：

```properties
# 仅上报 prod 库的表
metrics-reporter.table-name.include=prod\..*

# 排除 tmp 前缀的表
metrics-reporter.table-name.exclude=.*\.tmp_.*
```

**过滤规则：**

```
┌──────────────────────────────────────────┐
│  include 匹配 AND exclude 不匹配 → 转发   │
│  exclude 匹配 → 丢弃（优先于 include）    │
│  均未设置 → 全部转发（当前行为）           │
└──────────────────────────────────────────┘
```

> 该 Issue 已有对应实现 PR #16574（当日新增）。

---

## 四、新增 PR 速览

### 📬 PR #16574 — Core: Add table-name filter for MetricsReporter

- **作者：** [@moomindani](https://github.com/moomindani)
- **时间：** 2026-05-27 01:24 UTC
- **标签：** `core`, `docs`
- **关联 Issue：** #16573

实现 Issue #16573 的功能。在 `CatalogUtil.loadMetricsReporter` 中，当配置了 `include`/`exclude` 属性时，将用户的 reporter 包装在 `FilteringMetricsReporter` 中。对于 REST catalog，还在 `RESTSessionCatalog.metricsReporter()` 内对 `RESTMetricsReporter` 同样应用过滤层。

---

### 📬 PR #16575 — Data: Add TCK for Writer builder in FileFormat API

- **作者：** [@Guosmilesmile](https://github.com/Guosmilesmile)
- **时间：** 2026-05-27 05:36 UTC
- **标签：** `data`

为 `FileFormat` API 中 Writer builder 补充 TCK（Technology Compatibility Kit）测试覆盖，包括 `set()`、`setAll()`、`meta(key, value)`、`meta(Map)`、`overwrite()` 方法。加密相关方法 `withFileEncryptionKey`/`withAADPrefix` 将在后续 PR 单独覆盖。

---

### 📬 PR #16576 — Docs: Document Kafka Connect control topic purpose and retention

- **作者：** [@wombatu-kun](https://github.com/wombatu-kun)
- **时间：** 2026-05-27 07:16 UTC
- **标签：** `docs`
- **关联 Issue：** [#15844](https://github.com/apache/iceberg/issues/15844)

解决用户反馈的 Kafka Connect control topic 无限增长问题。在 `docs/docs/kafka-connect.md` 中新增：
- control topic 的作用和每次 commit 的事件流（`StartCommit` → `DataWritten` → `DataComplete` → `CommitToTable` → `CommitComplete`）
- 建议配置 `retention.ms=3600000` 并说明如何根据 commit 间隔调整
- 明确使用 `cleanup.policy=delete` 而非 compaction

---

### 📬 PR #16577 — Avro: Encode non-zone timestamps with local-timestamp logical types

- **作者：** [@Shekharrajak](https://github.com/Shekharrajak)
- **时间：** 2026-05-27 08:30 UTC
- **标签：** `spark`, `core`, `data`, `flink`, `Specification`
- **关联 Issue：** [#12751](https://github.com/apache/iceberg/issues/12751)

修复 Avro 时间戳编码不符合规范的问题：

```
Avro 规范：
  timestamp-{micros,nanos}        → UTC instant（带时区）
  local-timestamp-{micros,nanos}  → 本地时间（无时区）

Iceberg 现状（有问题）：
  timestamptz / timestamptz_ns → timestamp-* with adjust-to-utc=true   ✅
  timestamp   / timestamp_ns   → timestamp-* with adjust-to-utc=false  ❌（Iceberg私有惯例）

修复后：
  timestamp / timestamp_ns → local-timestamp-{micros,nanos}           ✅ 符合 Avro 规范
```

---

### 📬 PR #16578 — Core: Retry transient REST response-body read failures

- **作者：** [@wombatu-kun](https://github.com/wombatu-kun)
- **时间：** 2026-05-27 09:34 UTC
- **标签：** `core`
- **关联 Issue：** [#15030](https://github.com/apache/iceberg/issues/15030)

修复 REST catalog 客户端偶发性的 `MalformedChunkCodingException` 导致请求失败的问题。

**问题根因：**

```
Apache HttpClient5 重试链：
  HttpRequestRetryExec  ← 只覆盖连接和响应头读取
  ...
  ResponseHandler       ← 响应体在此处读取，在重试链之外！
```

响应体读取失败时 `IOException` 绕过了重试策略，直接抛出 `RESTException`。

**修复方案：** 在 `HttpRequestRetryExec` 内部注册一个 exec-chain interceptor，使用 `BufferedHttpEntity` 完全缓冲响应体。这样响应体读取错误会被现有重试策略捕获并重试（仅对幂等方法，非幂等方法如 commit 不受影响）。

---

### 📬 PR #16579 — Build: Replace deprecated Groovy space-assignment property syntax

- **作者：** [@wombatu-kun](https://github.com/wombatu-kun)
- **时间：** 2026-05-27 10:11 UTC
- **标签：** `spark`, `flink`, `MR`, `INFRA`, `build`, `AWS`, `GCP`, `AZURE`
- **关联 Issue：** [#15976](https://github.com/apache/iceberg/issues/15976)

Gradle 已废弃 Groovy DSL 的空格赋值语法（如 `exceptionFormat "full"`），计划在 Gradle 10 移除。将 13 个构建脚本中的 22 处旧语法迁移为 `name = value` 格式：

```groovy
// 修复前
exceptionFormat "full"
zip64 true
maxHeapSize '4g'

// 修复后
exceptionFormat = "full"
zip64 = true
maxHeapSize = '4g'
```

---

### 📬 PR #16580 — Flink: Lazy initial bulk scan for `TABLE_SCAN_THEN_INCREMENTAL`

- **作者：** [@sauliusvl](https://github.com/sauliusvl)
- **时间：** 2026-05-27 10:32 UTC
- **标签：** `flink`, `docs`
- **关联 Issue：** [#14463](https://github.com/apache/iceberg/issues/14463)

**这是本日最重磅的 PR，针对超大表的生产级别问题。**

#### 问题背景

`ContinuousSplitPlannerImpl` 将整个初始扫描物化为单个 `List<IcebergSourceSplit>`，在首次 checkpoint 时序列化为 `byte[]`。在约 800k 个 split 时，JSON 大小超过 Java 2GB 数组上限，导致崩溃。在约 187 万文件的生产表上已复现。

#### 解决方案：Lazy 分页初始扫描

```
配置方式：
  .lazyInitialBulkScanPageSize(N)
  connector.iceberg.lazy-initial-bulk-scan-page-size=N
  （N=0 保持原有 eager 行为，向后兼容）
```

**三阶段枚举器状态机：**

```
Phase 1（bulk 进行中）  position = empty() sentinel  cursor = (snapshot, count, hash)
Phase 2（bulk 刚完成）  position = real snapshot id   cursor = null（原子清除）
Phase 3（稳定状态）     position 随 commit 推进        cursor = null
```

**确定性保证（FNV-1a 滚动哈希）：**

```
恢复时验证 = hash(file.location + start + length + delete_files)
                 前 N 个 CombinedScanTask 重新计算
               若不匹配 → loud abort（不是静默重扫）
```

**响应式分页（避免 800k 文件花 30 分钟耗尽 bulk）：**

```
当 assigner.pendingSplitCount() < pageSize/2 时
  → 触发额外 planSplits 调用（不等待下一个 monitor interval）
单飞锁（AtomicBoolean）防止并发 planSplits 重叠
```

**效果：**

| 指标 | 优化前 | 优化后 |
|------|--------|--------|
| 首次 checkpoint 大小 | 超过 2GB（崩溃） | **~108 MB**（1.87M 文件，pageSize=10000）|
| 支持的最大表规模 | ~800k 文件 | **无上限** |

---

### 📬 PR #16582 — API, Core, ORC: Implement `filter()` for partition statistics scan API

- **作者：** [@gaborkaszab](https://github.com/gaborkaszab)
- **时间：** 2026-05-27 14:48 UTC
- **标签：** `API`, `core`, `ORC`

为 partition statistics scan API 实现 `filter()` 和 `caseSensitive()` 方法，使 partition statistics 扫描可以在 ORC 层面进行谓词下推。测试由 Claude Opus 4.7 辅助生成（已人工审查）。

---

### 📬 PR #16583 — API, Core, Parquet: Add filter hint support to `InternalData.ReadBuilder`

- **作者：** [@kamcheungting-db](https://github.com/kamcheungting-db)
- **时间：** 2026-05-27 19:09 UTC
- **标签：** `API`, `core`, `parquet`

`InternalData.ReadBuilder` 原先没有传递过滤表达式的方式，导致 Parquet 的行组跳过（row-group skipping）在内部元数据读取（如 partition statistics 扫描）中无法生效。

新增 `InternalData.read(format, file, filterHint)` 入口点，filter 作为 best-effort I/O 优化（Parquet 用于行组跳过；Avro 忽略），调用方负责通过 residual filter 保证正确性。

---

### 📬 PR #16584 — Core: Add unregister table to REST Catalog Kit (RCK)

- **作者：** [@rambleraptor](https://github.com/rambleraptor)
- **时间：** 2026-05-27 21:06 UTC
- **标签：** `core`, `OPENAPI`

跟进 PR #16400 中新增的 `unregisterTable` REST endpoint，将该 endpoint 添加到 REST Fixture（RCK）中，方便测试套件使用。

---

### 📬 PR #16585 — Parquet: Fix variant metrics crash when value column has no stats *(Draft)*

- **作者：** [@nssalian](https://github.com/nssalian)
- **时间：** 2026-05-27 22:32 UTC
- **标签：** `parquet`
- **关联 Issue：** [#16567](https://github.com/apache/iceberg/issues/16567)

修复 Spark 写入含 shredded VARIANT 列的 Iceberg 表时，若 variant `value` 子列没有可用的 Parquet Statistics，`DataWriter.close()` 会崩溃并抛出 `NoSuchElementException`。

修复方案：`MetricsVariantVisitor.value()` 在 `valueResult` 为空时返回空 bounds，而不是调用 `Iterables.getOnlyElement()`。

---

### 📬 PR #16586 — Core: Validate non-string elements in `JsonUtil.getStringArray`

- **作者：** [@stevenzwu](https://github.com/stevenzwu)
- **时间：** 2026-05-27 23:42 UTC
- **标签：** `core`

`JsonUtil.getStringArray` 直接对每个元素调用 `asText()`，会静默地将非字符串值（如整数 `45` → `"45"`，布尔 `true` → `"true"`）强制转换。

修复方案：增加 `isTextual()` 检查，与 `JsonStringArrayIterator`（被 `getStringList`、`getStringSet` 使用）的验证逻辑保持一致。

**影响的 3 个调用点均不依赖强制转换行为：**

| 调用位置 | 用途 |
|---------|------|
| `ViewVersionParser.fromJson` | 解析 view 版本的 `default-namespace` |
| `RESTSerializers.NamespaceDeserializer` | REST payload 中的 Namespace 反序列化 |
| `RemoteSignRequestParser.headersFromJson` | HTTP header 值解析 |

---

### 📬 PR #16587 — OpenAPI: Require at least one level in `CatalogObjectIdentifier`

- **作者：** [@stevenzwu](https://github.com/stevenzwu)
- **时间：** 2026-05-27 23:42 UTC
- **标签：** `OPENAPI`

在 OpenAPI spec 的 `CatalogObjectIdentifier` schema 中增加 `minItems: 1`，使规范与 Java 实现（`CatalogObjectIdentifier.of` 拒绝空数组）保持一致。

---

## 五、活动趋势分析

### 按模块分布

```
Spark      ████████████ 4 件（2 合并 + 2 新 PR）
Flink      ████████     3 件（1 合并 + 2 新 PR）
Core       ████████████████ 6 件（新 PR 为主）
Docs       ████████     3 件（1 合并 + 2 新 PR）
Parquet    ████         2 件（新 PR）
OpenAPI    ████         2 件（新 PR）
Build/Infra ██          1 件（新 PR）
Avro       ██           1 件（新 PR）
```

### 关键趋势

1. **CI 优化专项推进：** 本日合并的 2 个 Spark PR（#16549, #16559）都是测试矩阵瘦身，合计减少约 75% 的测试 CPU，显示社区对 CI 效率的持续关注。

2. **生产级大表支持：** Flink PR #16580（Lazy bulk scan）是响应真实生产场景（187 万文件表）的重要功能，解决了 2GB checkpoint 上限的根本问题。

3. **REST Catalog 生态完善：** 多个 PR 围绕 REST catalog（#16549 fixture 修复、#16574 metrics 过滤、#16578 重试、#16584 RCK）协同推进。

4. **规范一致性修复：** Avro 时间戳（#16577）和 JSON 解析（#16586）都是协议层面的正确性修复，属于长期遗留的规范偏差清理。

---

## 六、关注建议

| 优先级 | PR/Issue | 理由 |
|--------|----------|------|
| ⭐⭐⭐ 高 | PR #16580 | 大规模 Flink streaming 用户的生产阻塞问题 |
| ⭐⭐⭐ 高 | PR #16578 | REST client 重试修复，影响所有 REST catalog 用户 |
| ⭐⭐ 中 | PR #16577 | Avro 规范符合性修复，可能影响跨系统数据交换 |
| ⭐⭐ 中 | PR #16574 | 大规模部署中 metrics 控制的实用功能 |
| ⭐ 低 | Issue #16581 | API 语义问题，影响面小但值得讨论设计决策 |

---

*本报告由 Claude Code 自动生成 · 数据来源 apache/iceberg GitHub · 生成时间 2026-05-28*
