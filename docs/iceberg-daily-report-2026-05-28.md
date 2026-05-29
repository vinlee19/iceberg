# Apache Iceberg 每日动态报告

**报告日期：** 2026-05-28  
**数据范围：** 2026-05-28 00:00:00 UTC — 2026-05-28 23:59:59 UTC  
**Fork 同步状态：** ✅ 已同步至 upstream `apache/iceberg` main 分支（合并 38 个文件变更）

---

## 目录

1. [概览统计](#1-概览统计)
2. [已合并 PR 深度分析](#2-已合并-pr-深度分析)
3. [新增 Issue 分析](#3-新增-issue-分析)
4. [新增 PR 概览](#4-新增-pr-概览)
5. [技术趋势总结](#5-技术趋势总结)

---

## 1. 概览统计

```
┌─────────────────────────────────────────────────────────────┐
│              Apache Iceberg 2026-05-28 日报                  │
├──────────────────────┬──────────────────────────────────────┤
│  已合并 PR           │  5 个                                 │
│  新增 Issue          │  3 个                                 │
│  新增 PR             │  9 个（含 1 个 Draft）                │
│  Fork 同步           │  ✅ 38 个文件变更已合并               │
└──────────────────────┴──────────────────────────────────────┘
```

### 模块分布

```
已合并 PR 模块分布
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Core (核心)     ██████████████  2 个  (40%)
Flink           ██████████████  2 个  (40%)
Docs (文档)     ███████         1 个  (20%)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

新增 PR 模块分布
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Docs (文档)       ████████████   3 个  (33%)
Flink             ████████       2 个  (22%)
Core (核心)       ████████       2 个  (22%)
Parquet           ████           1 个  (11%)
Kafka Connect     ████           1 个  (11%)
REST              ████           1 个  (11%)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

---

## 2. 已合并 PR 深度分析

### PR #16208 · Core: Cache PartitionData template in PartitionsTable

| 字段 | 内容 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16208 |
| **作者** | [@Wenjun7J](https://github.com/Wenjun7J) |
| **标签** | `core` |
| **合并时间** | 2026-05-28T14:27:58Z |
| **类型** | 性能优化 🚀 |

#### 问题背景

在扫描 `PartitionsTable`（Iceberg 分区元数据表）时，每处理一个分区行都需要创建一个全新的 `PartitionData(partitionType)` 对象，这会触发一次完整的 Avro schema 转换流程：

```
每个分区 → PartitionData(partitionType)
         → PartitionData.partitionDataSchema()
         → AvroSchemaUtil.convert()
         → TypeToSchema$WithTypeToName.struct()
```

当表拥有大量分区时（如 20,000 个分区 × 4 个分区列），这种重复构建造成严重的内存分配压力。

#### 修复方案

在每次扫描启动时只创建 **一个** `PartitionData` 模板，之后通过 `copyFor(key)` 复用该模板，避免反复执行 Avro schema 转换：

```
扫描启动 → 创建 1 个 PartitionData 模板
分区 1   → template.copyFor(key1)  ← 无 schema 重建
分区 2   → template.copyFor(key2)  ← 无 schema 重建
...
```

#### 性能数据对比

测试环境：20,000 个分区值，4 个分区列，多次全量扫描

```
指标                 修复前              修复后          提升幅度
─────────────────────────────────────────────────────────────
Wall Clock Time     12.71s              5.24s           ↓ 58.8%
最大内存 (RSS)      5,938 MB (~5.7 GiB) 1,483 MB (~1.4 GiB) ↓ 75.0%
```

> **内存消耗从 ~5.7 GiB 降低到 ~1.4 GiB，时间从 12.7s 降低到 5.2s，效果显著！**

#### 关键代码变更

- 在 `PartitionsTable` 中引入 per-scan 的 `PartitionData` 模板
- 添加回归测试，验证同一扫描内的分区行复用相同 Avro schema 实例

---

### PR #16243 · Flink: Honor schema identifier fields in dynamic-sink record routing

| 字段 | 内容 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16243 |
| **作者** | [@jordepic](https://github.com/jordepic) |
| **标签** | `flink`, `docs` |
| **合并时间** | 2026-05-28T05:07:58Z |
| **类型** | Bug 修复 🐛 |

#### 问题背景

Flink 动态 Sink 在处理 equality delete（等值删除）时存在两处路由逻辑 Bug：

**Bug 1：`HashKeyGenerator` 路由错误**

当用户未显式指定 equality fields 时，系统应回退使用 schema 的 `identifierFieldIds`。但 `HashKeyGenerator` 对这类"仅含 identifier 字段"的记录采用了**轮询（round-robin）分发**，导致具有相同 identifier key 的两行数据可能被路由到**不同的 Writer 子任务**，而 Writer 端却仍然在用 identifier fields 生成 equality delete，从而破坏了等值删除的正确性。

**Bug 2：`DynamicRecordProcessor` 的 distribution mode 判断缺失**

当记录的 `distributionMode` 为 `null` 时，`DynamicRecordProcessor` 会直接将记录转发给 Writer，即使该记录实际上解析出了非空的 equality-field 集合。这同样导致共享同一 equality key 的记录被分散到多个 Writer，遗留重复数据。

#### 数据正确性影响

```
修复前：
  Row(id=1, A) ──── Writer-0 ← 产生 equality delete (id=1)
  Row(id=1, B) ──── Writer-1 ← 产生 equality delete (id=1)
  结果：两个 equality delete 相互独立，数据重复！

修复后：
  Row(id=1, A) ──┐
  Row(id=1, B) ──┴── Writer-0 ← 正确产生合并的 equality delete
```

#### 修复方案

在 `DynamicSinkUtil` 中集中实现 `resolveEqualityFieldNames()` 方法，统一 distribution 和 write 端的 equality field 解析逻辑，并在两处调用点复用：

- `HashKeyGenerator`（distribution 路由）
- `DynamicRecordProcessor`（record 转发）

同时在 `flink-writes.md` 中补充文档说明，并添加覆盖 v1.20 / v2.0 / v2.1 三个版本的单元测试。

---

### PR #16539 · Core: Fix flaky test by ensuring generateContentLength returns positive value

| 字段 | 内容 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16539 |
| **作者** | [@lilei1128](https://github.com/lilei1128) |
| **标签** | `core` |
| **合并时间** | 2026-05-28T03:12:50Z |
| **类型** | 测试稳定性修复 🔧 |

#### 问题根因

`generateContentLength()` 使用了 `random().nextInt(10_000)`，该方法**可以返回 0**。

当 DV（Deletion Vector）的 `contentSizeInBytes` 为 0 时：

```java
// SnapshotSummary.java 中的条件判断：
setIf(addedSize > 0, ADDED_FILE_SIZE_PROP, ...)
// 当 addedSize == 0 时，"added-files-size" 字段不写入快照摘要
```

测试 `testFileSizeSummaryWithDVs` 期望 summary 包含 19 个属性，但实际只有 18 个，导致随机性失败（Flaky Test）。

#### 修复方式

```java
// 修复前：
return random().nextInt(10_000);  // 可能返回 0

// 修复后：
return 1 + random().nextInt(10_000);  // 保证 >= 1
```

---

### PR #16597 · Flink: Honor schema identifier fields in dynamic-sink record routing (backport)

| 字段 | 内容 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16597 |
| **作者** | [@jordepic](https://github.com/jordepic) |
| **标签** | `flink`, `docs` |
| **合并时间** | 2026-05-28T15:56:33Z |
| **类型** | Backport 🔙 |

#### 说明

这是对 PR #16243（Flink identifier fields 路由修复）的 **Backport**，将修复向后移植到 Flink v1.20 和 v2.0 分支。原始修复已提交到 v2.1。

新增文件：
- `flink/v1.20/flink/src/test/java/org/apache/iceberg/flink/sink/dynamic/TestDynamicRecordProcessor.java`
- `flink/v2.0/flink/src/test/java/org/apache/iceberg/flink/sink/dynamic/TestDynamicRecordProcessor.java`
- `flink/v2.1/flink/src/test/java/org/apache/iceberg/flink/sink/dynamic/TestDynamicRecordProcessor.java`

---

### PR #16538 · Docs: Publish Iceberg security model

| 字段 | 内容 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16538 |
| **作者** | [@sungwy](https://github.com/sungwy) |
| **标签** | `docs` |
| **合并时间** | 2026-05-28T18:03:50Z |
| **类型** | 文档/安全 📄 |

#### 变更内容

发布了 Iceberg 项目正式的**安全模型文档**，新增文件 `SECURITY-THREAT-MODEL.md`，内容包括：

- **安全边界定义**：明确 Iceberg 区分安全漏洞与普通 Bug 的判断标准
- **信任模型（Trust Model）**：描述各类角色（数据管理员、查询引擎、REST Catalog 客户端等）的信任等级
- **威胁模型（Threat Model）**：枚举具体的威胁场景及 Iceberg 对每类威胁的处理策略
- **安全角色说明**：包括 Catalog 端、存储端、引擎端的安全职责划分

**背景**：该文档同时从 `AGENTS.md` 引用，为项目提供清晰的安全参考基准，旨在减少自动化安全扫描的误报，并帮助社区正确识别真正的安全问题。

---

## 3. 新增 Issue 分析

### Issue #16589 · Flink: Expose scan planning metrics on ContinuousIcebergEnumerator

| 字段 | 内容 |
|------|------|
| **Issue 链接** | https://github.com/apache/iceberg/issues/16589 |
| **提出者** | [@2dmurali](https://github.com/2dmurali) |
| **标签** | `improvement` |
| **关联 PR** | #16590（已提交对应实现） |

**问题描述：**  
Flink 流式作业在扫描大型 Iceberg 表时，`ContinuousIcebergEnumerator` 缺乏可观测性：
- 无法知道 Partition Pruning 是否有效（跳过了多少 manifest/文件）
- 无法监控扫描规划延迟
- `BaseIncrementalScan.planFiles()` 不像 `SnapshotScan`（批处理）那样通过 `metricsReporter()` 上报指标

**影响：** 难以诊断流式管道变慢的原因、验证表维护效果，或设置合理的 SLO。

**提案：**
1. 为 `BaseIncrementalScan.planFiles()` 接入 `metricsReporter()` 支持
2. 将 `ScanMetricsResult` 的所有字段作为 Flink Gauge 暴露到 Coordinator metric group

---

### Issue #16596 · `initial-default`+`write-default` `{}` 结构体默认值表示问题

| 字段 | 内容 |
|------|------|
| **Issue 链接** | https://github.com/apache/iceberg/issues/16596 |
| **提出者** | [@Tishj](https://github.com/Tishj) |
| **标签** | `bug` |

**问题描述：**  
Iceberg Spec 明确说明 struct 类型列的非 null 默认值应用空 struct `{}` 表示，使字段值从各自的 `initial-default` 或 `write-default` 推导：

```
spec 原文：A non-null default is stored by setting initial-default or write-default
           to an empty struct ({}) that will use field values set from each field's
           initial-default or write-default, respectively.
```

但 REST API spec 中，`initial-default` 和 `write-default` 被定义为 `PrimitiveTypeValue`，无法表达 `{}`。

**复现步骤：**  
向 `iceberg-rest-fixture` catalog 的 create table 端点 POST 含 `"initial-default": {}` 的请求时，触发：

```
Cannot create expression literal from org.apache.iceberg.data.GenericRecord: Record(null, null)
```

**影响范围：** REST API spec 规范不完整，影响所有使用 REST Catalog 创建含 struct 列默认值的表。

---

### Issue #16599 · Per-column dictionary encoding control in Parquet.WriteBuilder

| 字段 | 内容 |
|------|------|
| **Issue 链接** | https://github.com/apache/iceberg/issues/16599 |
| **提出者** | [@Gerrrr](https://github.com/Gerrrr) |
| **标签** | `improvement` |
| **意愿** | ✅ 作者愿意独立贡献 |

**问题描述：**  
对于包含高基数二进制列（如序列化消息载荷）的表，Parquet 会为每个值尝试字典编码。由于每个值都唯一，字典页会在几千行后就达到 2 MiB 上限（`write.parquet.dict-size-bytes`），随后 parquet-java 对所有已缓冲行重新编码为 PLAIN 格式。这带来了额外的：
- 构建字典的 CPU 开销
- 缓冲期间的内存开销
- 超限时重新编码所有数据的开销

**现状：**  
`parquet-java` 已通过 `ParquetProperties.Builder.withDictionaryEncoding(String columnPath, boolean)` 支持按列字典编码控制，但 Iceberg 的 `Parquet.WriteBuilder.build()` 只调用全局布尔形式，`Context.dataContext()` 只读取单一的 `write.parquet.dict-enabled` 值。

**提案：**  
新增 `write.parquet.dict-enabled.column.<columnPath>` 属性，与现有 `write.parquet.bloom-filter-enabled.column.*` 和 `write.parquet.stats-enabled.column.*` 的约定保持一致，列级设置优先于全局设置。

---

## 4. 新增 PR 概览

> 以下 PR 于 2026-05-28 创建，截止报告生成时仍处于 Open 或 Draft 状态。

| PR # | 标题 | 模块 | 状态 | 作者 |
|------|------|------|------|------|
| [#16598](https://github.com/apache/iceberg/pull/16598) | Kafka Connect: Follow-up identifier field validations and error contract alignment | `KAFKACONNECT` | 🟢 Open | @Elbehery |
| [#16595](https://github.com/apache/iceberg/pull/16595) | Core: Ensure trailing path separator in FileSystemWalker.listDirRecursivelyWithHadoop | `core` | 🟢 Open | @nnguyen168 |
| [#16594](https://github.com/apache/iceberg/pull/16594) | REST: Fix schema of data-access object in REST spec | `OPENAPI` | 🟢 Open | @adutra |
| [#16593](https://github.com/apache/iceberg/pull/16593) | Spark: Compact IcebergSortCompactionBenchmark to use base compaction class | `spark` | 🟢 Open | @varun-lakhyani |
| [#16592](https://github.com/apache/iceberg/pull/16592) | Docs: Refresh FileIO concepts page | `docs` | 🟢 Open | @wombatu-kun |
| [#16591](https://github.com/apache/iceberg/pull/16591) | Docs: Document default-namespace limitation with SparkSessionCatalog | `docs` | 🟢 Open | @wombatu-kun |
| [#16590](https://github.com/apache/iceberg/pull/16590) | Flink: Expose scan planning metrics on ContinuousIcebergEnumerator | `flink`, `core` | 🟢 Open | @2dmurali |
| [#16588](https://github.com/apache/iceberg/pull/16588) | Parquet: Add reader and writer without Hadoop dependency | `parquet` | ⚪ Draft | @ebyhr |

### 重点关注 PR

#### PR #16588 · Parquet: Add reader and writer without Hadoop dependency（Draft）

这是一个很有价值的重大改动。它为 Iceberg 添加了无 Hadoop 依赖的原生 Parquet 读写器：

```
当前依赖链：
  iceberg-parquet → parquet-hadoop → hadoop-common → ...

目标（NATIVE 模式）：
  iceberg-parquet → parquet-format + parquet-column （无 parquet-hadoop）
```

**新增核心类：**
- `NativeParquetFileReader` — 使用 `InputFile` 读取 Parquet，无需 `parquet-hadoop`
- `NativeParquetFileWriter` — 使用 `OutputFile` 写入 Parquet
- `ParquetFileReaderFactory` / `ParquetFileWriterFactory` — 通过 JVM 属性路由实现

**切换方式：**
```bash
# 通过 JVM 属性切换到原生实现
-Diceberg.parquet-client=NATIVE

# 默认仍为 HADOOP（向后兼容）
-Diceberg.parquet-client=HADOOP
```

**目标用户：** 主要面向 Trino 等禁止使用 Hadoop 依赖的系统。

#### PR #16590 · Flink: Expose scan planning metrics（对应 Issue #16589）

实现了 Issue #16589 提出的全套指标暴露方案：
- `BaseIncrementalScan.planFiles()` 接入 `metricsReporter()`
- 新增 `IcebergSourceEnumeratorMetrics` 类（17 个 AtomicLong 支撑的 Gauge）
- Flink 指标路径：`coordinator.enumerator.IcebergSourceEnumerator.table.<tableName>.<metric>`
- 支持通过 Prometheus、Datadog、JMX、Slf4j 上报

---

## 5. 技术趋势总结

### 本日重点方向

```
1. 🚀 性能优化
   PR #16208 大幅降低 PartitionsTable 扫描的 CPU 和内存消耗（时间 -58.8%，内存 -75%）

2. 🐛 Flink 等值删除正确性
   PR #16243 + #16597 修复了 Flink 动态 Sink 中 equality delete 的数据正确性问题
   影响：使用 Flink + Iceberg v2 upsert 的用户需关注，建议升级

3. 📄 安全与合规
   PR #16538 正式发布 Iceberg 安全模型文档（SECURITY-THREAT-MODEL.md）
   对依赖 Iceberg 的大型系统做安全评估时可参考

4. 🔧 去 Hadoop 依赖趋势
   PR #16588（Draft）提出原生 Parquet 读写器，延续 Iceberg 去 Hadoop 化趋势
   关注 Trino、DuckDB 等轻量级引擎集成场景

5. 📊 Flink 可观测性增强
   Issue #16589 + PR #16590 推动 Flink 流式扫描的 metrics 支持
   有助于生产环境的 Flink + Iceberg 监控建设
```

### 需要关注的潜在风险

| 风险项 | 相关 Issue/PR | 建议 |
|--------|--------------|------|
| Flink 等值删除数据正确性 | PR #16243 | 使用 Flink upsert 的用户须升级 |
| REST API struct 默认值表示缺陷 | Issue #16596 | 避免在 REST Catalog 中使用 struct 类型列的非 null 默认值，等待修复 |
| S3 路径不含尾斜杠可能匹配兄弟目录 | PR #16595 | 已有修复 PR，待合并 |

---

*报告生成时间：2026-05-29 | 数据来源：apache/iceberg GitHub 仓库*
