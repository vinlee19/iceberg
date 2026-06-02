# Apache Iceberg 每日动态报告

> **报告日期：** 2026-06-01（周一）
> **数据范围：** 2026-06-01 00:00 UTC — 2026-06-01 23:59 UTC
> **数据来源：** [apache/iceberg](https://github.com/apache/iceberg)
> **Fork 同步：** ✅ 已同步至 `vinlee19/iceberg`（合并 6 个新 commit）

---

## 概览

```
┌─────────────────────────────────────────────┐
│          2026-06-01 活动汇总                  │
├──────────────────┬──────────────────────────┤
│  合并的 PR        │  7 个                    │
│  新建的 PR        │  10 个（含 3 个草稿）     │
│  新建的 Issue     │  1 个                    │
└──────────────────┴──────────────────────────┘
```

### 模块分布

| 模块    | 合并 PR 数 | 新建 PR 数 |
|---------|-----------|-----------|
| Core    | 2         | 4         |
| Flink   | 3         | 2         |
| Arrow   | 1         | 0         |
| Spark   | 1         | 2         |
| Parquet | 0         | 1         |
| REST    | 0         | 1         |
| Build   | 1         | 0         |
| Site    | 0         | 1         |

---

## 一、已合并 PR 详细分析

### 1. [#16011] Flink：修复 DynamicCommitter 在 JobId 变更时重复提交问题

- **链接：** https://github.com/apache/iceberg/pull/16011
- **作者：** @lrsb
- **标签：** `flink`
- **合并时间：** 2026-06-01 12:03 UTC
- **修复 Issue：** #16008

#### 问题描述

`DynamicCommitter` 通过三元组 `(flink.job-id, flink.operator-id, max-committed-checkpoint-id)` 对提交进行去重。但 `DynamicRecordAggregator` 在每次 `open()` 时从运行时环境重新采样 Job ID，导致 Flink 作业重启或扩缩容后生成新的 Job ID。

```
旧行为（Bug）：
  restart → new JobId → committables 打上新 JobId
          → committer 找不到旧 JobId 的 snapshot
          → 静默重复提交 → 数据重复！

新行为（Fix）：
  restart → 从 operator state 恢复旧 JobId
          → committables 保持原有 JobId → 去重正常
```

#### 修复方案

- 将 aggregator 的 job id **持久化为 operator state**，重启时恢复，确保 committables 始终携带原始 job id。
- 修复了以下场景下的数据重复问题：
  - 作业重启
  - Autoscaler 的 savepoint + resubmit 周期
  - Session cluster 的重新提交

#### 核心修改文件

| 文件 | 改动类型 |
|------|---------|
| `flink/v2.1/flink/src/main/java/org/apache/iceberg/flink/sink/dynamic/DynamicCommitter.java` | 修复：持久化 jobId |
| `flink/v2.1/flink/src/main/java/org/apache/iceberg/flink/sink/dynamic/JobOperatorKey.java` | 新建：封装 (jobId, operatorId) 键 |
| `flink/v2.1/flink/src/main/java/org/apache/iceberg/flink/sink/dynamic/TableKey.java` | 删除冗余字段 |
| `TestDynamicCommitter.java` | 新增测试覆盖 |

---

### 2. [#16627] Arrow：修复精度大于 18 的 Decimal 截断问题

- **链接：** https://github.com/apache/iceberg/pull/16627
- **作者：** @wombatu-kun
- **标签：** `arrow`
- **合并时间：** 2026-06-01 15:25 UTC

#### 问题描述

通过向量化 Arrow reader 读取精度 > 18 的 decimal 列（如 `decimal(38, 0)`）时，**静默返回错误值且无任何报错**。

```
示例：
  写入值：12345678901234567890
  读取值：-6101065172474983726  ← 错误！
```

#### 根本原因

```
存储路径：
  decimal(38,0) → FIXED_LEN_BYTE_ARRAY → FixedSizeBinaryVector
  → 正确解码为 BigDecimal(unscaled=12345678901234567890, scale=0)

错误路径：
  JavaDecimalFactory.ofBigDecimal(value, scale)
  → BigDecimal.valueOf(value.unscaledValue().longValue(), scale)
                                         ↑
                             longValue() 只保留低 64 位 → 截断！

修复路径：
  直接 return value（已是正确的 BigDecimal，无需重建）
```

#### 受影响范围

- 仅影响精度 > 18 的 decimal（使用 INT32/INT64 的精度 ≤ 18 不受影响）
- 已有测试使用 `decimal(9, 2)`，未能覆盖此缺陷

#### 修复后验证

新增测试 `TestArrowReader.testHighPrecisionDecimalIsReadCorrectly`：写入 `decimal(38, 0)` 的 Parquet 文件，断言大于 `Long.MAX_VALUE` 的值可正确往返读取。

---

### 3. [#16648] Flink：将 DynamicCommitter JobId 修复回移植至 v1.20 和 v2.0

- **链接：** https://github.com/apache/iceberg/pull/16648
- **作者：** @lrsb
- **标签：** `flink`
- **合并时间：** 2026-06-01 16:26 UTC

#### 说明

这是 #16011（DynamicCommitter jobId 修复）的 **backport**，将修复同步至：
- `flink/v1.20/`
- `flink/v2.0/`

确保旧版本 Flink 用户也能获得此关键 Bug 修复。

---

### 4. [#16365] Spark 4.1：升级至 Spark 4.1.2

- **链接：** https://github.com/apache/iceberg/pull/16365
- **作者：** @manuzhang
- **标签：** `build`
- **合并时间：** 2026-06-01 18:49 UTC

#### 修改内容

| 项目 | 旧版本 | 新版本 |
|------|-------|-------|
| Apache Spark (4.1 catalog) | 4.1.1 | 4.1.2 |

- 新增 Apache Spark staging Maven 仓库以解析 4.1.2 artifacts：
  `https://repository.apache.org/content/repositories/orgapachespark-1519/`

---

### 5. [#16572] Core：v4 表元数据 location 字段改为可选

- **链接：** https://github.com/apache/iceberg/pull/16572
- **作者：** @rambleraptor
- **标签：** `API` `core` `spark` `flink` `hive` `NESSIE`
- **合并时间：** 2026-06-01 20:27 UTC

#### 背景

作为 **Iceberg 相对路径规范（Relative Paths Spec）** 的一部分，v4 表元数据的 `location` 字段被设计为可选字段，从而支持无绝对路径的表存储。

```
版本要求矩阵：
  v1 metadata → location 必填
  v2 metadata → location 必填
  v3 metadata → location 必填
  v4 metadata → location 可选 ✅ (新规范)
```

#### 修改内容

- 确保 `location` 在 v1-v3 中为必填，在 v4 中为可选
- 修正了部分测试中错误地允许非 v4 表省略 location 的情况

#### 受影响文件

| 文件 | 说明 |
|------|------|
| `TableMetadata.java` | 版本控制 location 校验逻辑 |
| `TableMetadataParser.java` | 解析时区分 v4 与旧版本 |
| `LocationUtil.java` | 相关工具方法调整 |
| 多个 Spark/Flink/Hive/Nessie 测试 | 修正 location 测试断言 |

---

### 6. [#16108] Core：为 SnapshotUpdate 实现 writeManifestsWith executor

- **链接：** https://github.com/apache/iceberg/pull/16108
- **作者：** @dramaticlly
- **标签：** `API` `core`
- **合并时间：** 2026-06-01 20:54 UTC

#### 背景

`SnapshotUpdate` 接口已有 `scanManifestsWith(ExecutorService)` 可以控制 manifest 读取的线程池，但 **manifest 写入**仍硬编码使用 `ThreadPools.getWorkerPool()`，调用方无法控制。

#### 新增能力

```java
// 新增 API
SnapshotUpdate<T> writeManifestsWith(ExecutorService executorService);
```

#### 解决的问题

| 痛点 | 修复前 | 修复后 |
|------|-------|-------|
| Manifest 写入线程池 | 硬编码 `ThreadPools.getWorkerPool()` | 调用方可注入自定义 ExecutorService |
| 关闭协调（#15031） | 困难 | 可控 |
| 调试信息（tagging/logging/metrics） | 无法附加 | 可通过 executor 传递 |

#### 实现变更

- `SnapshotProducer` 新增 `writeManifestsWith` 逻辑
- `TestSnapshotProducer`、`TestMergeAppend` 新增完整测试

---

### 7. [#16637] Build：升级 azure-sdk-bom 至 1.3.7

- **链接：** https://github.com/apache/iceberg/pull/16637
- **作者：** @ebyhr
- **标签：** `AZURE` `KAFKACONNECT`
- **合并时间：** 2026-06-01 01:03 UTC

#### 修改内容

| 依赖 | 旧版本 | 新版本 |
|------|-------|-------|
| `com.azure:azure-sdk-bom` | 1.3.6 | 1.3.7 |

取代了 #16633 的版本。

---

## 二、新建 Issue 分析

### [#16646] Flink：支持 ALTER TABLE ... DROP PARTITION 语法

- **链接：** https://github.com/apache/iceberg/issues/16646
- **作者：** @SteveStevenpoor
- **标签：** `improvement`
- **创建时间：** 2026-06-01 10:41 UTC
- **状态：** Open

#### 问题背景

Flink 已经支持 `ALTER TABLE ... DROP PARTITION` SQL 语法，并会调用 catalog 的 `dropPartition(...)` 实现，但 **Iceberg Flink catalog 尚未实现该操作**。

#### 现有替代方案（繁琐）

```sql
-- 目前不支持：
DELETE FROM events WHERE region = 'eu';  -- Flink batch/streaming 均不支持

-- 用户必须使用 Java API：
table.newDelete()
     .deleteFromRowFilter(Expressions.equal("region", "eu"))
     .commit();
```

#### 提议的功能

```sql
-- 期望支持的 SQL 语法：
ALTER TABLE events DROP PARTITION (id = 0);
ALTER TABLE events DROP PARTITION (region = 'eu', dt = '2024-01-01');
ALTER TABLE events DROP IF EXISTS PARTITION (data = 'z');
```

#### 实现思路

- `FlinkCatalog.dropPartition(...)` 将 partition spec 转为 Iceberg row filter，调用 `deleteFromRowFilter(...)`
- Flink SQL parser 已支持 `ALTER TABLE … DROP [IF EXISTS] PARTITION (...)` 语法

> **安全性保障：** `deleteFromRowFilter` 在 commit 时会执行严格的 projection check，拒绝部分文件删除，不会静默删除目标分区以外的行。

---

## 三、新建 PR 分析

### [#16641] Parquet：Hadoop FS 缓存禁用时保持 FileSystem 可达 _(Open)_

- **链接：** https://github.com/apache/iceberg/pull/16641
- **作者：** @wombatu-kun
- **标签：** `parquet`

#### 问题

当 Hadoop FileSystem 缓存被禁用（如 Azure ADLS Gen2 的 `fs.abfs.impl.disable.cache=true`），Parquet 写入中途会触发 GC，导致 `AzureBlobFileSystem.finalize()` 关闭线程池，写入失败：

```
Could not submit task to executor ... ThreadPoolExecutor [Terminated]
```

#### 根本原因

```
ParquetWriter 构建流程：
  new ParquetFileWriter(ParquetIO.file(output, conf), ...)
  ↑ 不保留 OutputFile 引用 → FileSystem 可被 GC

FS 被回收 → finalize() → 线程池关闭 → 写入失败
```

#### 修复方案

在 `ParquetWriter` 中持久持有 `OutputFile` 引用，确保 `FileSystem` 在写入期间保持可达。

---

### [#16642] Core：Avro 写入时保持 FileSystem 可达 _(Open)_

- **链接：** https://github.com/apache/iceberg/pull/16642
- **作者：** @wombatu-kun
- **标签：** `core`

与 #16641 相同类型的问题，但针对 **Avro 写入路径**：

```
受影响组件：
  AvroFileAppender → 仅保存输出流，不持有 OutputFile
  DataWriter / PositionDeleteWriter / EqualityDeleteWriter → 同样不持有

不受影响：
  ManifestWriter / ManifestListWriter → 自行持有 OutputFile ✅
  OrcFileAppender → 已持有 OutputFile ✅
```

#### 修复方案

在 `AvroFileAppender` 中持有 `OutputFile` 引用，写入期间 FileSystem 不被 GC。

---

### [#16643] API, Spark 4.1：为 migrate procedure 增加 ignore_missing_files 参数 _(Open)_

- **链接：** https://github.com/apache/iceberg/pull/16643
- **作者：** @drexler-sky
- **标签：** `API` `spark`

为 Spark 4.1 的 `migrate` procedure 新增 `ignore_missing_files` 参数，允许在迁移时忽略缺失文件，提升迁移的容错性。

---

### [#16644] REST：将 HTTP 400 commit 验证失败映射为 CommitFailedException _(Draft)_

- **链接：** https://github.com/apache/iceberg/pull/16644
- **作者：** @martinskeem
- **标签：** `core`

#### 问题

部分 REST catalog 实现（如 Databricks Unity Catalog）在并发写入冲突时返回 HTTP 400 + `"commit validation failed"` 消息，而非规范要求的 HTTP 409。

当前 `CommitErrorHandler` 将所有 400 响应映射为 `BadRequestException`，导致冲突异常逃出 `SnapshotProducer` 的重试逻辑，以致命错误传播：

```
Kafka Connect 日志示例：
BadRequestException: Malformed request: Commit validation failed.
  at ErrorHandlers$CommitErrorHandler.accept(ErrorHandlers.java:137)
  at SnapshotProducer.commit(SnapshotProducer.java:473)
  → 应触发 CommitFailedException + 自动重试，但实际直接失败
```

#### 修复方案

在 `CommitErrorHandler` 中为 400 状态码新增识别冲突 pattern 的逻辑，命中时抛出 `CommitFailedException`，恢复乐观并发的重试行为。

---

### [#16645] Core：为 RewriteManifests 操作增加分支支持 _(Open)_

- **链接：** https://github.com/apache/iceberg/pull/16645
- **作者：** @wombatu-kun
- **标签：** `core`

#### 问题

`RewriteManifests` 是唯一不能提交到命名分支的 `SnapshotUpdate`：
- 调用 `toBranch()` 会抛出 `UnsupportedOperationException`
- 即使 `toBranch()` 被启用，`apply()` 也会从 `base.currentSnapshot()`（始终是 main）读取 manifests，而非目标分支

#### 修复方案

```java
// 新增：
@Override
public RewriteManifests toBranch(String branchName) {
    return (RewriteManifests) super.targetBranch(branchName);
}

// 修复 apply()：
List<ManifestFile> manifests = snapshot != null
    ? snapshot.allManifests(io)   // 从目标分支读取
    : Collections.emptyList();
```

#### 新增测试

| 测试 | 验证内容 |
|------|---------|
| `testRewriteManifestsOnBranch` | 分支 rewrite 不影响 main |
| `testRewriteManifestsCreatesBranchIfNeeded` | 不存在的分支会从 main fork |
| `testRewriteManifestsToBranchRejectsNullBranch` | null 分支名校验 |
| `testRewriteManifestsToBranchRejectsTag` | tag ref 被拒绝 |

---

### [#16647] Flink：支持 ALTER TABLE ... DROP PARTITION 语法 _(Open)_

- **链接：** https://github.com/apache/iceberg/pull/16647
- **作者：** @SteveStevenpoor
- **标签：** `flink`

配合 Issue #16646 的实现 PR，在 `FlinkCatalog` 中实现 `dropPartition(...)` 并添加相关测试。

---

### [#16649] Site：新增 Iceberg-Go 0.6.0 发布博文 _(Open)_

- **链接：** https://github.com/apache/iceberg/pull/16649
- **作者：** @nssalian
- **标签：** `docs`

为 [iceberg-go 0.6.0](https://github.com/apache/iceberg-go/releases/tag/v0.6.0) 添加发布博客文章。

---

### [#16650] Spark 4.1：Geometry/Geography 端到端支持 _(Draft)_

- **链接：** https://github.com/apache/iceberg/pull/16650
- **作者：** @huan233usc
- **标签：** `API` `spark` `parquet`

#### 概述

在 Spark 4.1 上对 Iceberg 的 `GEOMETRY` / `GEOGRAPHY` 类型提供端到端支持，使 `CREATE TABLE / INSERT / SELECT / DELETE` 在 Parquet 格式下正常工作。

#### 三层架构实现

```
1. Parquet schema mapping
   TypeToMessageType / MessageTypeToType
   → GEOMETRY/GEOGRAPHY ↔ LogicalTypeAnnotation.geometryType/geographyType

2. Generic Parquet RW
   BaseParquetReaders / BaseParquetWriter
   → WKB 作为 ByteBuffer

3. Spark 4.1 集成
   TypeToSparkType / SparkTypeToType
   → Iceberg ↔ Spark GeometryType/GeographyType (保留 CRS)
   SparkParquetReaders / SparkParquetWriters
   → Spark 内部 GeometryVal/GeographyVal (4字节 LE SRID + WKB)
      ↔ 纯 WKB Parquet 表示
```

#### 测试覆盖（11 个 case）

| 测试 | 内容 |
|------|------|
| `testGeometryRoundTrip` | GEOMETRY 往返读取 |
| `testGeographyRoundTrip` | GEOGRAPHY 往返读取 |
| `testSridFilterRoundtrip` | `ST_Srid(geom) = N` 断言 CRS→SRID 重附 |
| `testDeleteWithDeletionVector` | v3 + MoR DELETE 产生 Puffin DV |
| `testNullGeometryValue` | NULL 与非 NULL 混合行 |
| `testMultipleGeoColumnsInOneTable` | GEOMETRY + GEOGRAPHY 并列 |
| `testStructWithGeometry` | `STRUCT<eid, loc: GEOMETRY>` |
| `testArrayOfGeometry` | `ARRAY<GEOMETRY>` |
| `testMapOfGeometry` | `MAP<STRING, GEOMETRY>` |
| `testStructOfArrayOfGeometry` | 嵌套结构 |
| `testDeleteWithDeletionVectorOnNestedGeometry` | DV + 嵌套 geo |

#### 当前限制（待后续 PR）

- 空间边界框统计（`GeospatialBound` X:Y:Z:M）尚未通过 `FieldMetrics` 传递
- 向量化读取暂时关闭
- 不支持拓扑谓词下推（`ST_Intersects`、`ST_Within`）
- 仅支持 Parquet（ORC / Avro 待后续）

---

### [#16651] Core：manifest list 的相对路径支持 _(Draft)_

- **链接：** https://github.com/apache/iceberg/pull/16651
- **作者：** @rambleraptor
- **标签：** `core`

作为 **Iceberg 相对路径规范** 系列的一部分，新增对 manifest-list 的相对路径支持，是 #16572（v4 location 可选）的延续工作。

---

## 四、趋势与洞察

### 关键主题

```
1. 相对路径规范（Relative Paths Spec）
   ├── #16572 ✅ v4 metadata location 可选
   └── #16651 🔄 manifest list 相对路径（Draft）

2. Flink DynamicCommitter 稳定性
   ├── #16011 ✅ 修复重复提交（主分支）
   └── #16648 ✅ 回移植至 v1.20 + v2.0

3. Hadoop FileSystem GC 问题（Azure ADLS Gen2）
   ├── #16641 🔄 Parquet 写入路径修复
   └── #16642 🔄 Avro 写入路径修复

4. 地理空间类型支持
   └── #16650 🔄 Spark 4.1 Geometry/Geography 端到端（Draft）

5. Core 并发与线程池
   └── #16108 ✅ writeManifestsWith executor 可插拔
```

### 值得关注的 PR

| PR | 理由 |
|----|------|
| #16011 / #16648 | 修复 Flink 数据重复关键 Bug，已回移植 |
| #16627 | 静默数据损坏 Bug（Arrow decimal），影响精度 > 18 的所有用户 |
| #16641 / #16642 | Azure ADLS Gen2 用户写入失败的根因修复 |
| #16650 | Iceberg 地理空间能力的重要里程碑 |

---

*报告由自动化脚本生成，如有疑问请查阅原始 PR 链接。*
