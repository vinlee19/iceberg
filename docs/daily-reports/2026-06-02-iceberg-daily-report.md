# Apache Iceberg 每日动态报告

**报告日期：** 2026-06-02
**数据来源：** https://github.com/apache/iceberg
**Fork 同步状态：** ✅ 已同步（vinlee19/iceberg main 分支已更新至最新 upstream 提交 `c958fcf`）

---

## 目录

1. [Fork 同步状态](#1-fork-同步状态)
2. [已合并 PR 分析（2026-06-02）](#2-已合并-pr-分析2026-06-02)
3. [新增 Issue 汇总](#3-新增-issue-汇总)
4. [新增 PR 汇总](#4-新增-pr-汇总)
5. [趋势总结](#5-趋势总结)

---

## 1. Fork 同步状态

| 项目 | 详情 |
|------|------|
| 同步时间 | 2026-06-03 |
| 同步前最新 commit | `67cbc1a` Build: Bump com.azure:azure-sdk-bom |
| 同步后最新 commit | `c958fcf` iceberg-go 0.6.0 release blog (#16649) |
| 新增提交数 | **9 个** |
| 变更文件数 | **116 个文件**（+3594 行 / -707 行）|

**同步新增的提交（按时间倒序）：**

```
c958fcf  iceberg go 0.6.0 release blog (#16649)
f5349db  Arrow: Fix vectorized reads of decimal columns with default values (#16501)
26a5771  Parquet: Fix timestamp_ns and timestamptz_ns predicate pushdown (#16619)
b88addf  Core: Support pluggable executor service for manifest writing (#16108)
687c58f  Core: v4 table metadata location should be optional (#16572)
1172b10  Spark 4.1: Upgrade to Spark 4.1.2 (#16365)
7857238  Flink: Backport DynamicCommitter jobId fix to v1.20 and v2.0 (#16648)
696b93d  Arrow: Fix truncation of decimals with precision larger than 18 (#16627)
c11404c  Flink: Fix duplicate commits in DynamicCommitter when Flink jobId changes (#16011)
```

---

## 2. 已合并 PR 分析（2026-06-02）

本日共合并 **3 个 PR**，涵盖 Bug 修复与文档更新。

---

### PR #16619 — Parquet: Fix timestamp_ns and timestamptz_ns predicate pushdown

| 属性 | 详情 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16619 |
| **作者** | wombatu-kun |
| **标签** | `parquet` |
| **合并时间** | 2026-06-02 10:48:51 UTC |
| **影响模块** | `iceberg-parquet` |

#### 问题描述

在使用 `ReadSupport` 读取路径（即不设置 `createReaderFunc` 时）对 `timestamp_ns` 或 `timestamptz_ns` 列进行谓词下推过滤时，**过滤结果错误**：无论过滤边界值如何，所有行都会被匹配返回，子微秒级过滤实际上被忽略，且不抛出任何异常。

#### 根本原因

存在两个互补的缺陷：

**缺陷 1：** `MessageTypeToType` 在类型转换时忽略时间戳单位，将所有 Parquet `INT64 TIMESTAMP(NANOS)` 列都映射回 Iceberg 微秒 `TimestampType`（而非 `TimestampNanoType`）。

```
nanosecond 列数据：  ~1.7 × 10¹⁸（纳秒值）
错误绑定后的边界值：~1.7 × 10¹⁵（微秒值）
结果：每一行都满足谓词 → 过滤失效
```

**缺陷 2：** `ParquetFilters` 中缺少 `TIMESTAMP_NANO` 的 case 分支。一旦 Schema 返回了正确的纳秒类型，过滤器会直接抛出 `UnsupportedOperationException`。

#### 修复方案

```
修复点 1：MessageTypeToType
  - 读取 TimestampLogicalTypeAnnotation.getUnit()
  - NANOS → TimestampNanoType (带/不带时区)
  - MICROS/其他 → 保持原有 TimestampType

修复点 2：ParquetFilters
  - 在 long 列谓词组中新增 case TIMESTAMP_NANO
  - 直接用 long 对比原始纳秒值（精确，无单位转换）
```

#### 新增测试

| 测试方法 | 验证内容 |
|---------|---------|
| `TestParquetSchemaUtil.testTimestampNanoConversionPreservesUnit` | Parquet NANOS schema 正确转换为 Iceberg timestamp_ns/timestamptz_ns |
| `TestParquet.timestampNanoFilterRespectsNanoseconds` | 5 行仅纳秒不同的数据能精确过滤到正确的 id |
| `TestParquet.timestamptzNanoFilterAcrossTimezones` | 多时区下纳秒边界过滤正确 |

#### 影响范围

此修复是 ORC 修复（#16609）的 Parquet 对应版本。主要影响使用 Parquet `ReadSupport` 路径并对纳秒时间戳列进行精确过滤的场景。

---

### PR #16501 — Arrow: Fix vectorized reads of decimal columns with default values

| 属性 | 详情 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16501 |
| **作者** | harperjiang |
| **标签** | `spark`, `arrow` |
| **合并时间** | 2026-06-02 19:26:59 UTC |
| **影响模块** | `iceberg-arrow` |
| **讨论评论数** | 4 |

#### 问题描述

向量化 Arrow 读取器（`VectorizedTableScanIterable`）对带有 `initialDefault` 或 `writeDefault` 的 Decimal 列分配向量时失败，抛出以下异常：

```
java.lang.IllegalArgumentException: Cannot cast default value to FIXED: <default>
  at org.apache.iceberg.types.Types$NestedField.castDefault(Types.java:892)
  at org.apache.iceberg.arrow.vectorized.VectorizedArrowReader.getPhysicalType(VectorizedArrowReader.java:255)
  at org.apache.iceberg.arrow.vectorized.VectorizedArrowReader.allocateFieldVector(VectorizedArrowReader.java:228)
```

#### 根本原因

调用链如下：

```
VectorizedArrowReader#getPhysicalType
  ↓ 将 Decimal 逻辑字段重写为底层物理类型 fixed[N]
  ↓ 使用 Types.NestedField.from(logicalType).ofType(type).build()
  ↓ 此操作将 initialDefault/writeDefault 一并复制到物理类型
  ↓ NestedField 构造器调用 castDefault(literal, type)
  ↓ DecimalLiteral.to(FixedType) 未定义此转换 → 返回 null
  ↓ Preconditions.checkArgument 断言失败 → 抛出异常
```

**关键点：** Default 值在语义上属于逻辑（Decimal）视图，不应流向物理表示。物理类型只是用来确定 Arrow 向量大小的实现细节。

#### 修复方案

```java
// 修复前：复用现有字段，导致 default 值被一并复制
Types.NestedField.from(logicalType).ofType(type).build()

// 修复后：用全新 builder 构造，仅复制 id/name/optionality/doc，不含 defaults
Types.NestedField.builder()
    .withId(field.fieldId())
    .withName(field.name())
    .asOptional(field.isOptional())
    .withDoc(field.doc())
    .ofType(type)
    .build()
```

#### 影响说明

- 此 Bug 仅在**非字典编码**列时触发（`allocateDictEncodedVector` 不调用 `getPhysicalType`）
- 新增测试 `TestArrowReader#testDecimalWithDefaultIsReadByVectorizedReader` 禁用了字典编码以使回归测试确定性可复现

#### 新增测试

| 测试方法 | 验证内容 |
|---------|---------|
| `TestArrowReader#testDecimalWithDefaultIsReadByVectorizedReader` | v3 表 DECIMAL(5,2) 列含 initialDefault+writeDefault，禁用字典编码写 Parquet，VectorizedTableScanIterable 读取成功并验证 INT32 原始值 |
| `TestVectorizedDefaultValues`（新建测试类） | 向量化读取含默认值字段的全面回归测试 |

---

### PR #16649 — Site: Add Iceberg-Go 0.6.0 release blog post

| 属性 | 详情 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16649 |
| **作者** | nssalian |
| **标签** | `docs` |
| **合并时间** | 2026-06-02 21:24:42 UTC |
| **影响模块** | `site/docs/blog` |

#### 内容说明

新增 iceberg-go 0.6.0 版本发布的博客文章，文件路径：

```
site/docs/blog/posts/2026-06-01-iceberg-go-0.6.0-release.md
```

完整变更日志：https://github.com/apache/iceberg-go/compare/v0.5.0...v0.6.0

此 PR 为纯文档更新，不涉及代码逻辑修改。

---

## 3. 新增 Issue 汇总

2026-06-02 共新增 **5 个 Issue**：

---

### Issue #16667 — AWS: restCredentialsProvider 应在设置 AssumeRole 时返回 StsAssumeRoleCredentialsProvider

| 属性 | 详情 |
|------|------|
| **Issue 链接** | https://github.com/apache/iceberg/issues/16667 |
| **类型** | `improvement` |
| **提交者** | NathanCYee |
| **状态** | Open |

**问题背景：**

当用户同时配置了 `client.factory=AssumeRoleAwsClientFactory` 和 REST Catalog SigV4 认证时，`RESTSigV4AuthSession` 调用 `restCredentialsProvider` 获取凭据，但此函数的决策链**不考虑 AssumeRole 配置**，导致 REST Catalog 请求使用默认凭据链，而 S3FileIO/Glue/KMS/DynamoDB 操作使用 AssumeRole 凭据，两者权限不一致。

**当前决策链：**
```
1. 有 accessKeyId/secretAccessKey → StaticCredentialsProvider
2. 有 clientCredentialsProvider → 反射构建自定义 Provider
3. 否则 → DefaultCredentialsProvider（未考虑 AssumeRole）
```

**提案：** 在步骤 2 和 3 之间检查 `clientAssumeRoleArn`，若已设置则返回 `StsAssumeRoleCredentialsProvider`。

---

### Issue #16663 — Spark 4.1: 支持 Iceberg time 类型

| 属性 | 详情 |
|------|------|
| **Issue 链接** | https://github.com/apache/iceberg/issues/16663 |
| **类型** | `improvement` |
| **提交者** | Benjamin0313 |
| **状态** | Open（已有对应 PR #16665）|

**背景：** Spark 4.1 引入了原生 `TimeType`（SPARK-51162），但 Iceberg Spark 模块仍抛出 `UnsupportedOperationException: Spark does not support time fields`。

**提案范围（仅 spark/v4.1）：**
- 类型映射：Iceberg `time` ⇄ Spark `TimeType`（微秒精度）
- 值转换：Iceberg 存储微秒，Spark 4.1 存储纳秒（×1000 读，÷1000 写）
- 向量化读取暂不支持（ColumnarBatch 无法暴露 TimeType 值）

---

### Issue #16662 — `rewrite_table_path` 对已删除的 position delete 清单条目抛出 FileNotFoundException

| 属性 | 详情 |
|------|------|
| **Issue 链接** | https://github.com/apache/iceberg/issues/16662 |
| **类型** | `bug` |
| **提交者** | leeyam24 |
| **状态** | Open |

**复现步骤：**
1. 创建含 position delete 文件的 v2 表
2. 运行 `rewrite_position_delete_files`（创建新快照，旧 delete 文件标记为 DELETED）
3. 运行 `expire_snapshots`（GC 旧 delete 文件）
4. 运行 `rewrite_table_path`（完整重写，无 `start_version`）

**预期：** 仅重写活跃的 position delete 文件
**实际：** `FileNotFoundException`（封装在 `UncheckedIOException` 中）

**根本原因：** `RewriteTablePathUtil.writeDeleteFileEntry` 的 `POSITION_DELETES` case 中无条件将所有文件加入 `result.toRewrite()`，不检查 manifest entry 的状态（DELETED 条目也被加入）：

```java
// 有问题的代码
result.toRewrite().add(file.copy()); // 无条件添加，含 DELETED 条目
```

---

### Issue #16661 — Core: 向 MetricsReporter 上报失败的 Scan 和 Commit

| 属性 | 详情 |
|------|------|
| **Issue 链接** | https://github.com/apache/iceberg/issues/16661 |
| **类型** | Feature Request |
| **提交者** | moomindani |
| **状态** | Open |

**现状：** `MetricsReporter` 只上报成功的操作：
- `ScanReport`：仅在 `planFiles` 成功时发出
- `CommitReport`：仅在 commit 成功后发出

**提案：** 在失败时也发出报告，使运维可以观察失败率、重试/退避行为、冲突频率等关键生产指标。

**设计决策讨论：**

| 方案 | 新建 `CommitFailureReport`/`ScanFailureReport`（推荐）| 复用现有报告 + 失败标记 |
|------|---|---|
| 向后兼容 | 不影响现有聚合逻辑 | 现有消费者静默混入失败指标 |
| API 表面 | 新增类型（additive/revapi-safe）| 不新增类型 |
| REST 传输 | 仅进程内（不修改 REST spec）| 需要新的 schema + server 支持 |

---

### Issue #16659 — Commit-validation failures（Kafka Connect + Databricks）

| 属性 | 详情 |
|------|------|
| **Issue 链接** | https://github.com/apache/iceberg/issues/16659 |
| **类型** | `question` |
| **提交者** | martinskeem |
| **状态** | Open（2 条评论）|

**问题：** 使用 Kafka Connect Iceberg Sink 向 Databricks Iceberg Catalog 写入时，Databricks 在 commit 验证失败时返回 HTTP 400，而非 Iceberg 期望的 HTTP 409。导致连接器无法识别为可重试的乐观锁冲突，进入循环重试但无法成功。

**提问：**
1. HTTP 409 是否是 Iceberg 规范对 commit 验证失败的要求？是否有正式文档？
2. 是否接受类似 #16644 的兼容性修复 PR？

---

## 4. 新增 PR 汇总

2026-06-02 共新增 **12 个 PR**，其中 3 个已合并（见上文），以下为仍处于 Open 状态的 9 个 PR：

---

### PR #16668 — Core: Fix for global equality deletes applied to partitioned tables

| 属性 | 详情 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16668 |
| **作者** | mderoy |
| **标签** | `core` |
| **状态** | Open |

**问题：** `PartitionSpec.unpartitioned()` 是 specId = 0 的单例常量，而表的第一个分区 spec 也被赋予 specId = 0。当对一个第一个 spec 就是分区 spec 的表写入全局相等删除（使用 `PartitionSpec.unpartitioned()`）时，删除文件存储 specId = 0。读取时 `specsById.get(0)` 解析为**分区 spec** 而非 unpartitioned 常量，导致全局删除被误分类为分区范围删除，实际上不生效，被删除的行仍被返回。

**修复：** 将空 partition data 的条目视为全局删除。

---

### PR #16666 — Core: Relative paths for data files [Draft]

| 属性 | 详情 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16666 |
| **作者** | rambleraptor |
| **标签** | `spark`, `core`, `data` |
| **状态** | Draft |

新增对相对路径数据文件的读取支持，修复先前 PR 中无 scheme 的"绝对"路径处理问题。

---

### PR #16665 — Spark 4.1: Support time type

| 属性 | 详情 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16665 |
| **作者** | Benjamin0313 |
| **标签** | `spark` |
| **状态** | Open（对应 Issue #16663）|

在 Spark 4.1 模块中添加 Iceberg `time` 类型支持，映射到 Spark `TimeType`。

**变更范围：**
- **类型转换：** `TypeToSparkType`（time → TimeType）、`SparkTypeToType`（TimeType → time）
- **值转换：** Parquet/ORC/Avro 的读写路径均处理微秒↔纳秒转换
- **行级：** `SparkValueConverter`、`InternalRowWrapper`
- **向量化：** 暂不支持（SparkBatch 回退到行读取）

---

### PR #16664 — Data: Add metrics reporter to generic scan builder

| 属性 | 详情 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16664 |
| **作者** | alexandrefimov |
| **标签** | `data` |
| **状态** | Open（对应 Issue #14875）|

为 `IcebergGenerics.ScanBuilder` 添加委托式 `metricsReporter` 方法，允许调用方配置 Scan 指标上报而无需直接访问底层 `TableScan`。

---

### PR #16660 — CI: Retry Trivy scanner image pull to absorb transient Docker Hub timeouts

| 属性 | 详情 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16660 |
| **作者** | wombatu-kun |
| **标签** | `INFRA` |
| **状态** | Open |

**问题：** CVE 扫描 workflow 间歇性因拉取 Trivy 扫描器镜像超时而失败（`context deadline exceeded`，exit code 125），阻塞无关 PR。

**修复：** 在扫描前预先拉取镜像，加入最多 5 次重试和线性退避。镜像版本通过 job 级 `TRIVY_IMAGE` 环境变量统一管理，确保预拉取和扫描使用同一镜像。

---

### PR #16658 — Kafka Connect: Single-pass array uniformity checks in JsonToMapUtils

| 属性 | 详情 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16658 |
| **作者** | wombatu-kun |
| **标签** | `KAFKACONNECT` |
| **状态** | Open |

**性能优化：** 将 `JsonToMapUtils` 中使用 `HashSet` 的数组均匀性检查替换为单次遍历，遇第一个不同元素即提前退出。

**性能提升（基准测试结果）：**

| 数组 | 大小 | 优化前 | 优化后 | 提速 |
|------|------|--------|--------|------|
| uniform（完整扫描）| 10 | 110.3 ns | 12.0 ns | **89%** |
| uniform | 100 | 902.3 ns | 68.8 ns | **92%** |
| mixed@2（第2个不同）| 10 | 119.8 ns | 6.7 ns | **94%** |
| mixed@2 | 100 | 1069.4 ns | 6.7 ns | **99%** |

---

### PR #16657 — Kafka Connect: Pre-size collections in RecordConverter list/map conversion

| 属性 | 详情 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16657 |
| **作者** | wombatu-kun |
| **标签** | `KAFKACONNECT` |
| **状态** | Open |

**性能优化：** 将 `convertListValue` 从 Stream+collect 改为预分配 `ArrayList`（`newArrayListWithCapacity`），`convertMapValue` 使用预估大小的 HashMap（`newHashMapWithExpectedSize`），并将字段 ID/类型提取移到循环外。

**性能提升：**

| 集合 | 大小 | 优化前 | 优化后 | 提速 |
|------|------|--------|--------|------|
| list | 10 | 202.6 ns | 44.7 ns | **78%** |
| list | 100 | 1098 ns | 382 ns | **65%** |
| map | 10 | 190.3 ns | 164.5 ns | **14%** |
| map | 100 | 2568 ns | 1581 ns | **38%** |

---

### PR #16656 — S3FileIO: Always normalize listPrefix key to end with trailing slash

| 属性 | 详情 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16656 |
| **作者** | bolma-lila |
| **标签** | `AWS` |
| **状态** | Open |

**问题：** `S3FileIO.listPrefix()` 仅对 S3 Directory Bucket 添加尾部斜杠，标准 S3 桶不规范化，导致两个问题：

1. **前缀匹配泄漏：** `s3://bucket/warehouse/ns/table` 会匹配 `warehouse/ns/table_archive/...` 等相邻路径
2. **STS 权限 403：** Lakekeeper 等 REST catalog 通过 STS vending 限定权限为 `warehouse/ns/table/*`，无尾部斜杠时 `StringLike` 匹配失败

**修复：** 在 `listPrefix()` 中无条件调用 `toDirectoryPath()`（与 Trino 的 `directoryKey()` 逻辑一致）。

---

### PR #16655 — Kafka Connect: Split route field path once instead of per record

| 属性 | 详情 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16655 |
| **作者** | wombatu-kun |
| **标签** | `KAFKACONNECT` |
| **状态** | Open |

**性能优化：** `SinkWriter` 每次路由时调用 `Splitter.on('.').splitToList(routeField)` 重新解析路由字段路径。由于路由字段在连接器生命周期内固定，此解析是纯粹的逐记录开销。

**修复：** 在构造函数中一次性解析路径，新增接受 `List<String>` 的重载，现有 `String` 重载委托给它。

**性能提升：**

| 记录类型 | 路由字段 | 优化前 | 优化后 | 提速 |
|---------|---------|--------|--------|------|
| struct | `key` | 52.8 ns | 5.7 ns | **89%** |
| struct | `data.id.key` | 162.2 ns | 32.0 ns | **80%** |
| map | `key` | 54.3 ns | 6.1 ns | **89%** |
| map | `data.id.key` | 144.3 ns | 21.6 ns | **85%** |

---

### PR #16654 — Kafka Connect: Precompute UUID-as-bytes flag in RecordConverter

| 属性 | 详情 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16654 |
| **作者** | wombatu-kun |
| **标签** | `KAFKACONNECT` |
| **状态** | Open |

**性能优化：** `convertUUID` 每次处理 UUID 类型值时都重新计算写入文件格式是否为 Parquet（`FileFormat.PARQUET.name().toLowerCase().equals(...)`），该 boolean 在连接器生命周期内恒定，却在每次调用时分配新字符串和 Map 查找。

**修复：** 在构造函数中一次性计算 `writeUuidAsBytes`，`convertUUID` 方法简化为字段读取。

**性能提升：**

| 输入类型 | 文件格式 | 优化前 | 优化后 | 提速 |
|---------|---------|--------|--------|------|
| String | parquet | 53.6 ns | 32.5 ns | **39%** |
| String | orc | 46.1 ns | 26.1 ns | **43%** |
| UUID | parquet | 32.8 ns | 5.9 ns | **82%** |
| UUID | orc | 22.3 ns | 2.4 ns | **89%** |

---

### PR #16653 — Docs: Add no-emdash / ASCII-only rule to AGENTS.md

| 属性 | 详情 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16653 |
| **作者** | wombatu-kun |
| **状态** | Open |

在 `AGENTS.md` 中新增规范：禁止在源代码中使用全角破折号（em-dash U+2014）及其他非 ASCII Unicode 字符（测试数据或专门测试 Unicode 处理的场景除外）。

---

### PR #16652 — Spec: Add spec for expressions

| 属性 | 详情 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16652 |
| **作者** | rdblue（Iceberg PMC 核心成员）|
| **标签** | `Specification` |
| **状态** | Open（已获 1 个 🚀 reaction）|

**重大提案！** 新增 Iceberg 表达式（Expressions）规范文档，基于 [Extending Iceberg Expressions 设计文档](https://docs.google.com/document/d/1VthBz0S2I39TeQM8oiF9_gSPQu_gHAjWXvdFpv0QqDk/edit?tab=t.0#heading=h.54u4q3416qx8)。这将使 Iceberg 表达式可以跨引擎标准化，为 REST Catalog 谓词下推等场景奠定规范基础。

---

## 5. 趋势总结

### 本日活动概览

```
合并 PR：  3 个
新增 Issue：5 个
新增 PR：  12 个
```

### 重点关注领域

#### Bug 修复（高优先级）

| 模块 | 问题 | 严重性 |
|------|------|--------|
| Parquet | timestamp_ns 谓词下推失效 | ⚠️ 高 — 静默返回错误结果 |
| Arrow | Decimal 含默认值时向量化读崩溃 | ⚠️ 高 — 抛出异常，读取完全失败 |
| Core | 全局相等删除在已分区表上不生效 | ⚠️ 高 — 被删除数据仍被返回 |
| S3FileIO | listPrefix 无尾部斜杠导致路径泄漏或 403 | ⚠️ 中 — 影响 STS vending 场景 |

#### 性能优化（Kafka Connect 集中发力）

本日贡献者 **wombatu-kun** 提交了 4 个针对 Kafka Connect 的性能优化 PR（#16658、#16657、#16655、#16654），聚焦逐记录热路径，累计提速 **40%～99%** 不等。

#### 重大规范提案

**PR #16652** 由 Iceberg PMC 核心成员 `rdblue` 提交，新增 Iceberg 表达式规范，属于规范级别的重大变更，值得持续关注。

#### 新特性方向

- **Spark 4.1 time 类型支持**（Issue #16663 + PR #16665）：随 Spark 4.1 引入 `TimeType`，Iceberg 有望支持完整的 time 列读写
- **MetricsReporter 失败事件上报**（Issue #16661）：提升生产可观测性
- **相对路径数据文件**（PR #16666 Draft）：存储路径灵活性增强

---

*报告自动生成 | Apache Iceberg Daily Report | 2026-06-02*
