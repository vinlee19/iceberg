# Apache Iceberg 每日动态报告 — 2026-05-29

> **生成时间**: 2026-05-30  
> **分析范围**: 2026-05-29 00:00 ~ 23:59 (UTC)  
> **数据来源**: [apache/iceberg](https://github.com/apache/iceberg)  
> **Fork 同步**: ✅ 已完成同步 `vinlee19/iceberg` → `upstream/main`

---

## 目录

1. [Fork 同步状态](#fork-同步状态)
2. [今日数据概览](#今日数据概览)
3. [合并的 PR 深度分析](#合并的-pr-深度分析)
4. [新增 Issue 分析](#新增-issue-分析)
5. [新提交 PR 分析](#新提交-pr-分析)
6. [总结与趋势](#总结与趋势)

---

## Fork 同步状态

```
远程仓库: https://github.com/apache/iceberg
本地 Fork: vinlee19/iceberg
同步分支: main
同步结果: ✅ 成功 (fast-forward)
同步提交: f12e0cf → 8f28a86
```

| 状态 | 说明 |
|------|------|
| ✅ 已拉取 | `git fetch upstream main` |
| ✅ 已合并 | `git merge upstream/main --ff-only` |
| ✅ 已推送 | `git push origin main` |

---

## 今日数据概览

```
┌─────────────────────────────┬───────┐
│ 指标                         │  数量  │
├─────────────────────────────┼───────┤
│ 合并的 PR (已合并入 main)     │   7   │
│ 新增 Issue                  │   4   │
│ 新提交 PR (Open)             │  10   │
│ 关闭但未合并的 PR            │   1   │
└─────────────────────────────┴───────┘
```

**模块分布图**

```
Flink         ████████░░ 2 个合并 PR
Core          ████████░░ 2 个合并 PR
API + ORC     ████░░░░░░ 1 个合并 PR
Spark         ████░░░░░░ 1 个合并 PR
Docs          ████░░░░░░ 1 个合并 PR
```

---

## 合并的 PR 深度分析

> 时间范围: 2026-05-29 00:00 ~ 23:59 (UTC)，共 **7 个 PR** 合并入 main

---

### PR #16593 — Spark: 重构 IcebergSortCompactionBenchmark 继承基类

| 字段 | 内容 |
|------|------|
| **标题** | Spark: Compact IcebergSortCompactionBenchmark to use base compaction class |
| **作者** | varun-lakhyani |
| **合并时间** | 2026-05-29 14:36 UTC |
| **标签** | `spark` |
| **关联 PR** | 跟进 #16219 (引入了基础压缩基类) |
| **变更文件** | `IcebergSortCompactionBenchmark.java` |
| **变更规模** | +8 / -90 行 |

#### 核心改动

```
变更前: IcebergSortCompactionBenchmark 独立实现所有逻辑 (约 100 行)
变更后: 继承 BaseCompactionBenchmark，复用公共逻辑 (仅 ~18 行核心)
```

**改动示意图**

```
改动前:
IcebergSortCompactionBenchmark
├── 所有 setup 逻辑 (重复代码)
├── 所有 teardown 逻辑 (重复代码)
└── sortCompaction() benchmark 方法

改动后:
BaseCompactionBenchmark (父类 #16219 引入)
    └── IcebergSortCompactionBenchmark (子类)
            └── sortCompaction() benchmark 方法 (仅业务逻辑)
```

**意义**: 减少 ~82 行冗余代码，与 `IcebergCopyOnWriteCompactionBenchmark` 等兄弟类保持一致的结构。功能行为完全不变。

---

### PR #16569 — API/Core/ORC: 实现 `project()` for 分区统计扫描 API

| 字段 | 内容 |
|------|------|
| **标题** | API, Core, Orc: Implement project() for partition statistics scan API |
| **作者** | gaborkaszab |
| **合并时间** | 2026-05-29 12:22 UTC |
| **标签** | `core`, `ORC` |
| **变更文件** | 5 个文件 |
| **变更规模** | +183 / -7 行 |

#### 问题背景

`PartitionStatisticsScan.project()` 之前抛出 `UnsupportedOperationException`，无法对分区统计数据进行列投影，用户无法只读取部分列。

#### 核心改动

**1. `BasePartitionStatisticsScan.java` — 实现 `project()` 方法**

```java
// 改动前：直接抛出异常
@Override
public PartitionStatisticsScan project(Schema newSchema) {
    throw new UnsupportedOperationException("Projection is not supported");
}

// 改动后：保存投影 schema，读取时应用
@Override
public PartitionStatisticsScan project(Schema newSchema) {
    Preconditions.checkArgument(newSchema != null, "Invalid projection schema: null");
    this.projection = newSchema;
    return this;
}

// 读取时：
Schema readSchema = projection == null ? schema
    : TypeUtil.select(schema, TypeUtil.getProjectedIds(projection));
```

**2. `BasePartitionStatistics.java` — 引入静态 BASE_TYPE，支持索引投影**

```java
// 新增：完整的字段类型定义，使 SupportsIndexProjection 能正确工作
private static final Types.StructType BASE_TYPE = Types.StructType.of(
    EMPTY_PARTITION_FIELD, SPEC_ID, DATA_RECORD_COUNT,
    DATA_FILE_COUNT, TOTAL_DATA_FILE_SIZE_IN_BYTES,
    POSITION_DELETE_RECORD_COUNT, POSITION_DELETE_FILE_COUNT,
    EQUALITY_DELETE_RECORD_COUNT, EQUALITY_DELETE_FILE_COUNT,
    TOTAL_RECORD_COUNT, LAST_UPDATED_AT, LAST_UPDATED_SNAPSHOT_ID, DV_COUNT
);
```

**3. `PartitionStatsHandler.java` — V2 表向后兼容修复**

```java
// 修复：V2 表不包含 dv_count 字段，需先检查 size
if (inputStats.dvCount() != null
    && targetStats.size() > PartitionStatistics.DV_COUNT_POSITION) {
```

**数据流图**

```
PartitionStatisticsScan.project(newSchema)
        │
        ▼
  保存 projection schema
        │
        ▼
  读取分区统计文件
        │
        ▼
  TypeUtil.select(schema, projectedIds)  ◄── 仅选择用户指定的列
        │
        ▼
  InternalData.read(...).project(readSchema)
        │
        ▼
  仅返回投影列数据（节省 I/O）
```

**意义**: 完成分区统计扫描 API 的列投影支持，提升大型表的查询效率，用户无需读取全部 13 个统计字段。

---

### PR #16538 — Docs: 发布 Iceberg 安全威胁模型

| 字段 | 内容 |
|------|------|
| **标题** | Docs: Publish Iceberg security model |
| **作者** | sungwy |
| **合并时间** | 2026-05-28 (包含在此批次) |
| **标签** | `docs` |
| **新增文件** | `SECURITY-THREAT-MODEL.md`, `AGENTS.md`, `site/docs/security.md` |
| **变更规模** | +279 / -0 行 |

#### 核心内容

这是 Apache Iceberg 首次发布正式的**安全威胁模型文档**，面向人工和 AI Agent 消费。

**新增文件说明**

```
SECURITY-THREAT-MODEL.md  ← 260 行，详细描述安全威胁模型
AGENTS.md                 ← AI Agent 使用说明（4行）
site/docs/security.md     ← 安全文档索引页（15行）
```

**安全模型涵盖的关键领域** (基于文档结构):
- 信任边界与组件交互
- Catalog 服务的访问控制威胁
- REST Catalog OAuth2 认证威胁
- S3/FileIO 数据访问权限
- 元数据完整性保护
- 供应链安全

**意义**: 这是一个重要的里程碑，为 Iceberg 社区和安全研究者提供了官方的威胁模型参考，也是对 AGENTS.md 规范（供 AI 代理理解项目安全边界）的落地实践。

---

### PR #16597 — Flink: 回迁 Dynamic Sink 路由遵循 Schema Identifier Fields

| 字段 | 内容 |
|------|------|
| **标题** | Flink: Backport honor schema identifier fields in dynamic-sink record routing |
| **作者** | jordepic |
| **合并时间** | 2026-05-28 (包含在此批次) |
| **标签** | `flink` |
| **关联 PR** | 回迁自 #16243 |
| **变更文件** | 10 个文件（Flink 1.20 / 2.0 / 2.1 三个版本） |
| **变更规模** | +474 / -16 行 |

#### 问题背景

Flink Dynamic Sink 在做 record routing（路由到正确分区）时，没有遵循 Schema 的 `identifier-field` 语义（即标识某条记录唯一性的字段）。这导致在处理 UPSERT/CDC 场景时，routing key 计算不正确。

#### 核心改动

**`DynamicSinkUtil.java` — 提取 identifier field IDs**

```java
// 新增：从 Schema 中获取 identifier fields 用于路由
static Set<Integer> identifierFieldIds(Schema schema) {
    return schema.identifierFieldIds();
}
```

**`HashKeyGenerator.java` — 使用 identifier fields 计算 hash key**

```java
// 改动前：使用所有分区字段计算 hash
// 改动后：优先使用 schema identifier fields 参与 routing 决策
```

**`DynamicRecordProcessor.java` — 将 identifier 语义传入处理链**

**回迁覆盖版本**

```
Flink 1.20  ✅ backport
Flink 2.0   ✅ backport
Flink 2.1   ✅ backport（原始 PR #16243 首先落地于此）
```

**测试新增**

| 测试类 | 场景 |
|--------|------|
| `TestDynamicRecordProcessor` | 验证 identifier fields 在 processor 中正确生效 |
| `TestHashKeyGenerator` | 验证 hash key 在有/无 identifier fields 时的计算结果 |

**意义**: 确保 Flink CDC (Change Data Capture) 场景下，Dynamic Sink 按照 Iceberg schema identifier 语义正确路由记录，避免 UPSERT 数据写入错误分区。

---

### PR #16208 — Core: 缓存 PartitionData 模板避免每次重建 Avro Schema

| 字段 | 内容 |
|------|------|
| **标题** | Core: Cache PartitionData template in PartitionsTable to avoid rebuilding Avro schema per partition |
| **作者** | SevenJ (Wenjun7J) |
| **合并时间** | 2026-05-28 (包含在此批次) |
| **标签** | `core` |
| **变更文件** | `PartitionsTable.java`, `TestMetadataTableScans.java` |
| **变更规模** | 净增 ~20 行 |

#### 问题背景

在 `PartitionsTable.partitions()` 中，每次为一个 partition 条目创建 `Partition` 对象时，都会 `new PartitionData(keyType)`，这个操作会触发 Avro Schema 的构建。对于有大量分区的表，这会造成大量重复的 Schema 对象分配。

#### 核心改动

```java
// 改动前：每个 partition 都 new 一个 PartitionData，触发 Avro schema 构建
partitions.computeIfAbsent(key, () -> new Partition(key, partitionType))

// 改动后：复用同一个 template，只 copy 数据
PartitionData partitionDataTemplate = new PartitionData(partitionType); // 只创建一次

partitions.computeIfAbsent(key, () -> new Partition(key, partitionDataTemplate))
```

**内存分配对比图**

```
改动前 (N 个分区):
  partition_1 → new PartitionData(keyType) → new Avro Schema
  partition_2 → new PartitionData(keyType) → new Avro Schema  (重复!)
  partition_3 → new PartitionData(keyType) → new Avro Schema  (重复!)
  ...N 次

改动后 (N 个分区):
  template   → new PartitionData(keyType) → new Avro Schema  (只创建一次!)
  partition_1 → template.copyFor(key)  (轻量级 copy)
  partition_2 → template.copyFor(key)  (轻量级 copy)
  ...N 次（无 Schema 重建）
```

**测试验证**

```java
// 新增测试验证所有 partition 共享同一个 Avro Schema 引用
for (PartitionsTable.Partition partition : PartitionsTable.partitions(table, scan)) {
    partitionSchemas.add(partition.partitionData().getSchema());
}
// 断言所有 schema 是同一个实例
assertThat(partitionSchemas).allMatch(s -> s == partitionSchemas.get(0));
```

**意义**: 对于有数千/数万分区的大型表，元数据扫描的内存分配和 GC 压力大幅降低，`PartitionsTable` 查询性能显著提升。

---

### PR #16243 — Flink: Dynamic Sink 路由遵循 Schema Identifier Fields (原始版本)

| 字段 | 内容 |
|------|------|
| **标题** | Flink: Honor schema identifier fields in dynamic-sink record routing |
| **作者** | jordepic |
| **合并时间** | 2026-05-28 (包含在此批次) |
| **标签** | `flink` |
| **变更文件** | 6 个文件 |
| **变更规模** | +239 / -10 行 |

> 这是 #16597 回迁 PR 的源头版本，落地于 Flink 最新版本。详见 #16597 分析，逻辑相同。

**文档更新**: `docs/docs/flink-writes.md` 同步记录了此行为变更。

---

### PR #16539 — Core: 修复不稳定测试（generateContentLength 返回正值）

| 字段 | 内容 |
|------|------|
| **标题** | Core: Fix flaky test by ensuring generateContentLength returns positive value |
| **作者** | sanshi (lilei1128) |
| **合并时间** | 2026-05-28 (包含在此批次) |
| **标签** | `core` |
| **变更文件** | `FileGenerationUtil.java` |
| **变更规模** | +2 / -2 行 |

#### 核心改动

```java
// 改动前：Math.random() * MAX_VALUE 可能返回 0，导致测试偶发失败
private static long generateContentLength() {
    return (long) (Math.random() * Long.MAX_VALUE);
}

// 改动后：保证返回正数
private static long generateContentLength() {
    return (long) (Math.random() * Long.MAX_VALUE) + 1;
}
```

**意义**: 修复 CI 中偶发的 `flaky test`，提升测试稳定性。

---

## 新增 Issue 分析

> 2026-05-29 新增 **4 个 Issue**，均为 Bug 报告

---

### Issue #16600 — Parquet 向量化 I/O 硬编码 ON + 堆内分配器导致 S3FileIO OOM (1.11.0 回归)

| 字段 | 内容 |
|------|------|
| **编号** | [#16600](https://github.com/apache/iceberg/issues/16600) |
| **标题** | Parquet vectored I/O hardcoded ON + on-heap allocator causes executor OOM on S3FileIO reads |
| **报告者** | andyguwc |
| **严重程度** | 🔴 高（生产级 OOM，无外部配置规避手段） |
| **受影响版本** | Iceberg 1.11.0（1.10.x 不受影响） |

#### 问题描述

这是 1.11.0 引入的严重回归 Bug：

**两个变化叠加产生问题：**

```
变化 1: ParquetIO.java 新增 ParquetRangeReadableInputStreamAdapter
         → S3InputStream 现在实现了 RangeReadable 接口
         → parquet-hadoop 会触发 readVectored 代码路径

变化 2: Parquet.ReadBuilder 硬编码:
         optionsBuilder.withUseHadoopVectoredIo(true);
         → 无论用户如何配置，向量化 I/O 始终开启

结果: S3FileIO 读取走向量化路径 + HeapByteBufferAllocator（堆内分配）
      对于 14× 压缩比的表：128 MB 压缩 → ~1.8 GB 堆内分配 per row group
      多任务并发 → OOM
```

**复现条件**

```
- S3FileIO
- write.parquet.compression-codec=zstd（或其他高压缩比编解码器）
- 默认 128 MB row group
- 1.11.0 版本
```

**关键堆栈**

```
java.lang.OutOfMemoryError: Java heap space
  at HeapByteBufferAllocator.allocate(HeapByteBufferAllocator.java:34)
  at ParquetRangeReadableInputStreamAdapter.readVectored(ParquetIO.java:180)
  at ParquetFileReader.readVectored(ParquetFileReader.java:1357)
```

**用户临时规避方案（均有代价）**

| 规避方式 | 代价 |
|----------|------|
| `spark.executor.cores=2` | 减少并发，降低吞吐 |
| 升级 Worker 规格 | 增加成本 |
| 减小 row-group-size 并重写文件 | 操作复杂，I/O 开销大 |
| `spark.hadoop.parquet.hadoop.vectored.io.enabled=false` | **无效**（被硬编码覆盖） |

**建议修复**：
- Fix A: 移除硬编码 `true`，读取 `parquet.hadoop.vectored.io.enabled` 配置
- Fix B: 默认使用 `DirectByteBufferAllocator`（堆外）

> 已有 PR #16614 提交修复

---

### Issue #16605 — Kafka Connect: inferIcebergType 对某些 BigDecimal 生成非法 DecimalType

| 字段 | 内容 |
|------|------|
| **编号** | [#16605](https://github.com/apache/iceberg/issues/16605) |
| **标题** | Kafka Connect: inferIcebergType produces an invalid DecimalType for some BigDecimal values |
| **报告者** | wombatu-kun |
| **严重程度** | 🟠 中（触发条件特定：schema evolution + 无 schema 的 schemaless record） |

#### 问题描述

```java
// 当前代码：直接使用 BigDecimal 的 precision/scale
return DecimalType.of(bigDecimal.precision(), bigDecimal.scale());
```

**两种非法场景：**

```
场景 1: new BigDecimal("0.001")
  precision=1, scale=3 → decimal(1, 3)
  Iceberg 要求: scale <= precision → ❌ 非法

场景 2: new BigDecimal("1E+2")
  precision=1, scale=-2 → decimal(1, -2)
  Iceberg 要求: scale >= 0 → ❌ 非法
```

**错误信息**: `Invalid DECIMAL scale: 3 cannot be greater than precision: 1`

**修复方案**: 规范化 precision 和 scale，使其满足 Iceberg 约束

> 已有 PR #16606 提交修复

---

### Issue #16603 — Kafka Connect: MongoDataConverter 数组 TIMESTAMP/DATE_TIME 转换异常

| 字段 | 内容 |
|------|------|
| **编号** | [#16603](https://github.com/apache/iceberg/issues/16603) |
| **标题** | Kafka Connect: MongoDataConverter throws on arrays of TIMESTAMP/DATE_TIME values |
| **报告者** | wombatu-kun |
| **严重程度** | 🟠 中（影响含时间戳数组的 MongoDB 文档处理） |

#### 问题描述

```java
// 错误代码：用错了类型转换器
// DATE_TIME 元素：
Date temp = new Date(arrValue.asInt64().getValue());  // ❌ arrValue 是 BsonDateTime，不是 BsonInt64

// TIMESTAMP 元素：
Date temp = new Date(1000L * arrValue.asInt32().getValue());  // ❌ arrValue 是 BsonTimestamp，不是 BsonInt32
```

**正确写法（标量路径已正确）：**

```java
// DATE_TIME 正确写法：
new Date(arrValue.asDateTime().getValue())

// TIMESTAMP 正确写法：
new Date(1000L * arrValue.asTimestamp().getTime())
```

> 已有 PR #16604 提交修复

---

### Issue #16601 — Kafka Connect: KafkaMetadataTransform static 字段导致实例间配置泄漏

| 字段 | 内容 |
|------|------|
| **编号** | [#16601](https://github.com/apache/iceberg/issues/16601) |
| **标题** | Kafka Connect: KafkaMetadataTransform leaks configuration across instances via a static field |
| **报告者** | wombatu-kun |
| **严重程度** | 🟠 中（多个 connector 使用同一 SMT 时静默产生错误输出） |

#### 问题描述

```java
// 问题代码：recordAppender 是 static 字段
private static RecordAppender recordAppender;

// 每次 configure() 都会覆盖同一个 static 字段
// 最后一个 configure() 的配置会被所有实例使用
```

**影响场景**: 两个 Connector 都使用 `KafkaMetadataTransform` 但配置了不同的 `field_name`，最后一个的配置会覆盖前者，导致第一个 Connector 输出错误的字段名。

**修复**: 将 `static` 改为实例字段

> 已有 PR #16602 提交修复

---

## 新提交 PR 分析

> 2026-05-29 新提交 **10 个 PR**（其中 9 个 Open，1 个已关闭但未合并）

---

### PR #16616 — Spark: 废弃 SparkFilters（计划 1.12.0 移除）

| 字段 | 内容 |
|------|------|
| **状态** | 🟢 Open |
| **作者** | huaxingao |
| **标签** | `spark` |
| **目标版本** | 1.12.0 |

**改动概述**: `SparkFilters`（DSv1 `Filter[]` → Iceberg `Expression`）已无生产代码引用，所有调用方已迁移到 `SparkV2Filters`（DSv2 `Predicate[]`）。本 PR 添加 `@Deprecated` 注解，为 1.12.0 正式移除做准备。

```
影响范围: spark/v3.5, spark/v4.0, spark/v4.1
变更类型: 仅注解，零行为变更
```

---

### PR #16615 — Build: .gitignore 新增 warehouse/ 目录

| 字段 | 内容 |
|------|------|
| **状态** | 🟢 Open |
| **作者** | venkateshwaracholan |
| **标签** | `INFRA` |

**改动**: 将本地 Spark/Catalog 实验产生的 `warehouse/` 目录加入 `.gitignore`，防止 Parquet、Avro、元数据 JSON 等文件被意外提交。

---

### PR #16614 — Spark/Parquet: 遵循 Hadoop vectored IO read 配置（修复 #16600）

| 字段 | 内容 |
|------|------|
| **状态** | 🟢 Open |
| **作者** | venkateshwaracholan |
| **标签** | `spark`, `parquet` |
| **关联 Issue** | #16600 |

**改动概述**:
- 移除硬编码的 `withUseHadoopVectoredIo(true)`
- 保留 Parquet 默认向量化 I/O 行为
- 通过 Spark 读取配置传播 `parquet.hadoop.vectored.io.enabled`
- 使 executor 端 Parquet 读取器能够响应配置

---

### PR #16612 — REST: 允许 schema 字段中的 struct 默认值（修复 #16596）

| 字段 | 内容 |
|------|------|
| **状态** | 🟢 Open |
| **作者** | akashmalbari |
| **标签** | `API`, `core`, `OPENAPI` |
| **关联 Issue** | #16596 |

**改动概述**: 更新 REST/OpenAPI 对 schema 字段默认值的处理，使 `initial-default` 和 `write-default` 支持 struct 类型默认值（如 `{}`），符合 Iceberg v3 spec 要求。

---

### PR #16611 — Common/Core: 处理 Dyn* 类加载时的 ExceptionInInitializerError 和 NoClassDefFoundError

| 字段 | 内容 |
|------|------|
| **状态** | 🟢 Open |
| **作者** | nastra |
| **标签** | `core`, `common` |

**改动概述**: `Class.forName` 在类找到但传递依赖缺失或静态初始化失败时，会抛出 `NoClassDefFoundError` 或 `ExceptionInInitializerError`，应与 `ClassNotFoundException` 同等处理（降级为可选实现缺失）。

---

### PR #16609 — ORC: 支持 timestamp_ns 和 timestamptz_ns 谓词下推

| 字段 | 内容 |
|------|------|
| **状态** | 🟢 Open |
| **作者** | wombatu-kun |
| **标签** | `ORC` |

**改动概述**: 

```
问题: ExpressionToSearchArgument 对 TIMESTAMP_NANO 类型无处理
      → 读取带纳秒时间戳谓词的列时抛出 UnsupportedOperationException

修复: 将 TIMESTAMP_NANO 映射到 PredicateLeaf.Type.TIMESTAMP
      并将纳秒数值转换为 java.sql.Timestamp（支持纳秒精度）
```

新增测试验证纳秒级精度的谓词下推正确性。

---

### PR #16608 — Snowflake: 避免在 catalog 初始化时修改调用方传入的 properties map

| 字段 | 内容 |
|------|------|
| **状态** | 🟢 Open |
| **作者** | wombatu-kun |
| **标签** | `SNOWFLAKE` |

**改动概述**:

```java
// 问题: 直接 put 到调用方的 map，若是 immutable map 则抛异常
properties.put("application", ...);

// 修复: 先 copy 再 put
Map<String, Object> props = Maps.newHashMap(properties);
props.put("application", ...);
```

与 `JdbcCatalog`、`GlueCatalog` 等其他 Catalog 实现保持一致。

---

### PR #16607 — API: geometry 和 geography 类型的单值二进制序列化

| 字段 | 内容 |
|------|------|
| **状态** | 🟢 Open |
| **作者** | huan233usc |
| **标签** | `API` |

**改动概述**: 将 `GeospatialBound` 接入 `Conversions.toByteBuffer` / `fromByteBuffer`，实现 v3 geo 类型的二进制序列化。编码格式：x:y:z:m 各 8 字节 little-endian IEEE 754 double。

```
toByteBuffer(GeometryType, ...)   之前: 抛 UnsupportedOperationException
                                  之后: 正确序列化为二进制
fromByteBuffer(GeometryType, ...) 之前: 抛 UnsupportedOperationException
                                  之后: 正确反序列化
```

---

### PR #16606 — Kafka Connect: 修复 BigDecimal 推断 DecimalType 的非法值（关联 #16605）

| 字段 | 内容 |
|------|------|
| **状态** | 🟢 Open |
| **作者** | wombatu-kun |
| **标签** | `KAFKACONNECT` |

直接修复 Issue #16605，见 Issue 分析部分。

---

### PR #16604 — Kafka Connect: 修复 MongoDataConverter 数组转换（关联 #16603）

| 字段 | 内容 |
|------|------|
| **状态** | 🟢 Open |
| **作者** | wombatu-kun |
| **标签** | `KAFKACONNECT` |

直接修复 Issue #16603，见 Issue 分析部分。

---

### PR #16602 — Kafka Connect: 修复 KafkaMetadataTransform 配置泄漏（关联 #16601）

| 字段 | 内容 |
|------|------|
| **状态** | 🟢 Open |
| **作者** | wombatu-kun |
| **标签** | `KAFKACONNECT` |

直接修复 Issue #16601，见 Issue 分析部分。

---

### PR #16610 — Core: 实现 cache 过期双策略（已关闭，未合并）

| 字段 | 内容 |
|------|------|
| **状态** | 🔴 Closed (未合并) |
| **作者** | blcksrx |
| **标签** | `core`, `spark`, `flink`, `docs` |

**改动概述**: 为 `CachingCatalog` 引入 `expire-after-write` + `expire-after-access` 双策略并行支持。关联 Issue #14417（长期运行的 streaming job 导致 cache 永不过期）。

**关闭原因**: 未标注，可能与已有的 #14440 跟进 Issue 有设计冲突，需进一步讨论。

---

## 总结与趋势

### 今日亮点

| 优先级 | 内容 |
|--------|------|
| 🔴 重要 | Issue #16600: Parquet 向量化 I/O OOM 回归，生产用户已受影响，PR #16614 修复中 |
| 🟠 重要 | Flink Dynamic Sink identifier fields 路由修复同时落地 3 个 Flink 版本 |
| 🟢 重要 | 分区统计扫描 API 完成 `project()` 支持，提升大型表元数据查询效率 |
| 📖 重要 | 首次发布官方安全威胁模型文档 (SECURITY-THREAT-MODEL.md) |

### 模块热度分析

```
Kafka Connect  ████████████ 3 Issues + 3 PR  (最活跃)
ORC            ████████░░░░ 1 合并PR + 1 新PR
Flink          ████████░░░░ 2 合并PR
Core           ████████░░░░ 2 合并PR
Parquet/Spark  ████░░░░░░░░ 1 新Issue + 1 新PR (高优先级修复)
API            ████░░░░░░░░ 1 合并PR + 1 新PR
Snowflake      ████░░░░░░░░ 1 新PR
```

### 贡献者活跃度

| 贡献者 | 贡献内容 |
|--------|----------|
| wombatu-kun | 3 Issues + 4 PRs（Kafka Connect, ORC, Snowflake 全面修复） |
| jordepic | 2 PRs（Flink Dynamic Sink 原始版 + 回迁版） |
| gaborkaszab | 1 PR（分区统计 project() 实现） |
| SevenJ | 1 PR（PartitionsTable 性能优化） |
| sungwy | 1 PR（安全模型文档） |
| huaxingao | 1 PR（SparkFilters 废弃） |

### 下一步关注点

1. **PR #16614** — Parquet vectored I/O 修复是否被快速合并（影响生产用户）
2. **Issue #16600** — 是否会触发 1.11.x patch release
3. **PR #16607** — Geospatial 支持进展（geometry/geography 序列化）
4. **PR #16611** — Dyn* 类加载健壮性修复审查进度

---

*本报告由自动化脚本生成，数据来源于 GitHub API 和本地 git log 分析。*
