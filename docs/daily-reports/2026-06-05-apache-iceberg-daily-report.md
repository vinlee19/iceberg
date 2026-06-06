# Apache Iceberg 每日动态报告
## 📅 日期：2026-06-05

> 数据来源：[apache/iceberg](https://github.com/apache/iceberg) | 统计周期：2026-06-05 00:00:00 UTC — 23:59:59 UTC  
> Fork 同步状态：✅ **已同步** (`vinlee19/iceberg` main 已与 `apache/iceberg` main 保持一致)

---

## 📊 当日活动概览

| 类型 | 数量 |
|------|------|
| ✅ 合并的 PR | 6 |
| 🆕 新建的 PR（open） | 6 |
| 🐛 新建的 Issue | 3 |

---

## ✅ 已合并 PR 详细分析

### PR #16643 — `API, Spark 4.1: Add ignore_missing_files to migrate procedure`

- **合并时间**：2026-06-05 02:52:22 UTC  
- **作者**：[drexler-sky](https://github.com/drexler-sky)  
- **标签**：`API` · `spark` · `docs`  
- **链接**：https://github.com/apache/iceberg/pull/16643

#### 背景与问题
在执行 Spark `migrate` procedure（将 Hive/Spark 表迁移为 Iceberg 表）时，如果源数据文件在迁移过程中被并发删除或已不存在，迁移任务会直接抛出 `FileNotFoundException` 导致失败，没有优雅处理方式。

#### 修改内容

```
api/src/main/java/org/apache/iceberg/actions/MigrateTable.java       (+12)
docs/docs/spark-procedures.md                                          (+1)
spark/v4.1/spark/src/main/java/.../MigrateTableSparkAction.java       (+17)
spark/v4.1/spark/src/main/java/.../MigrateTableProcedure.java         (+14)
spark/v4.1/.../TestMigrateTableProcedure.java                         (+41)
```

#### 核心变更

**1. API 接口新增默认方法（`MigrateTable.java`）**
```java
// 新增：遇到缺失文件时跳过而非失败
default MigrateTable ignoreMissingFiles() {
  throw new UnsupportedOperationException(...);
}
```

**2. Action 实现层（`MigrateTableSparkAction.java`）**
- 新增 `boolean ignoreMissingFiles` 字段
- 实现 `ignoreMissingFiles()` 方法（支持链式调用）
- 将该标志传递给 `SparkTableUtil.importSparkTable()`

**3. Procedure 层（`MigrateTableProcedure.java`）**
- 新增可选参数 `ignore_missing_files`（默认值：`false`）
- 从参数中解析后调用 action 的对应方法

**4. 文档更新**
`spark-procedures.md` 中为 migrate procedure 新增参数说明。

#### 测试覆盖
| 测试方法 | 验证内容 |
|----------|----------|
| `testMigrateMissingFilesFailByDefault()` | 默认情况下遇到缺失文件会失败 |
| `testMigrateIgnoreMissingFiles()` | 启用参数后可成功跳过缺失分区 |

---

### PR #16684 — `Spark 3.5, 4.0: Add ignore_missing_files to migrate procedure`

- **合并时间**：2026-06-05 16:42:28 UTC  
- **作者**：[drexler-sky](https://github.com/drexler-sky)  
- **标签**：`spark`  
- **链接**：https://github.com/apache/iceberg/pull/16684

#### 说明
此 PR 是 #16643 的**反向移植（backport）**，将 `ignore_missing_files` 参数支持从 Spark 4.1 扩展到 Spark **3.5** 和 **4.0**。

#### 修改文件（每个版本各 3 个文件）

```
spark/v3.5/spark/src/main/java/.../MigrateTableSparkAction.java
spark/v3.5/spark/src/main/java/.../MigrateTableProcedure.java
spark/v3.5/.../TestMigrateTableProcedure.java

spark/v4.0/spark/src/main/java/.../MigrateTableSparkAction.java
spark/v4.0/spark/src/main/java/.../MigrateTableProcedure.java
spark/v4.0/.../TestMigrateTableProcedure.java
```

#### 影响范围
迁移功能的跨版本一致性：Spark 3.5 / 4.0 / 4.1 现在均支持 `ignore_missing_files` 参数。

---

### PR #16575 — `Data: Add TCK for Writer builder in FileFormat API`

- **合并时间**：2026-06-05 10:41:53 UTC  
- **作者**：[Guosmilesmile](https://github.com/Guosmilesmile)  
- **标签**：`data`  
- **链接**：https://github.com/apache/iceberg/pull/16575

#### 背景
FileFormat API 的 Writer builder 缺乏系统性的 TCK（技术兼容性套件）测试覆盖，此 PR 补全了对以下方法的测试：
- `set(String key, String value)`
- `setAll(Map<String, String> properties)`
- `meta(String key, ByteBuffer value)`
- `meta(Map<String, ByteBuffer> metadata)`
- `overwrite()`

#### 修改文件

```
data/src/test/java/org/apache/iceberg/data/BaseFormatModelTests.java
data/src/test/java/org/apache/iceberg/data/FileFormatTestSupport.java   (新增)
data/src/test/java/org/apache/iceberg/data/avro/AvroFormat.java         (新增)
data/src/test/java/org/apache/iceberg/data/orc/OrcFormat.java           (新增)
data/src/test/java/org/apache/iceberg/data/parquet/ParquetFormat.java   (新增)
```

共新增 **586 行**，删除 **204 行**。

#### 关键测试
| 测试方法 | 格式覆盖 | 测试内容 |
|----------|----------|----------|
| `testDataWriterNoOverwriteFailsIfFileExists` | Avro/ORC/Parquet | 不设 overwrite 时文件已存在应失败 |
| `testSchemaHandling` | 全部 | 写入数据的 schema 正确性 |
| `testSetAndSetAll` | 全部 | 配置项传入验证 |
| `testMetaKey` | 全部 | 元数据写入验证 |

> **注**：`withFileEncryptionKey()` 和 `withAADPrefix()` 是 Parquet 专属加密接口，将在后续 PR 中补充。

---

### PR #16055 — `GCS, S3, ADLS: Handle EOF in inputStreams`

- **合并时间**：2026-06-05 23:41:32 UTC  
- **作者**：[vladislav-sidorovich](https://github.com/vladislav-sidorovich)  
- **标签**：`AWS` · `GCP` · `AZURE`  
- **链接**：https://github.com/apache/iceberg/pull/16055

#### 问题根因
云存储 InputStreams（GCS/S3/ADLS）的 `read()` 和 `read(byte[], int, int)` 方法在遇到 EOF（流末尾）时没有正确返回 `-1`，而是：
- `read()` 方法：返回 `singleByteBuffer` 中已过期的旧数据
- `read(byte[], int, int)` 方法：错误地递增了 `pos` 位置计数器和 metrics

这违反了 Java `InputStream` 规范，可能导致读取数据异常。

#### 修改文件与关键变更

**GCS（谷歌云存储）**
```java
// GCSInputStream.java - 修复前
int b = stream.read();
singleByteBuffer[0] = (byte) b;  // 若b=-1，写入了错误数据
return singleByteBuffer[0] & 0xFF;

// 修复后
int b = stream.read();
if (b == -1) return -1;  // 正确返回 EOF
singleByteBuffer[0] = (byte) b;
return singleByteBuffer[0] & 0xFF;
```

**S3（AWS S3）**
- `S3InputStream.java`：同样在 `read()` 和 `read(byte[], int, int)` 中增加 EOF 检查

**ADLS（Azure Data Lake Storage）**
- `ADLSInputStream.java`：修复了 `read()` 方法中 `stream.read()` 被调用两次的 bug，正确存储返回值后检查 EOF

#### 测试覆盖

| 文件 | 新增测试 |
|------|----------|
| `TestGCSInputStream.java` | `testReadSingle()`, `testReadBufferedEOF()` |
| `TestS3InputStream.java` | `testReadSingle()`, `testReadBufferedEOF()` |
| `TestADLSInputStream.java` | 更新 mock 验证 EOF 返回及位置追踪 |

> ⚠️ **重要性**：此 Bug 存在较长时间（PR 于 2026-04-19 创建，历时 47 天合并），影响三大主流云存储平台。

---

### PR #16626 — `Spark 4.1: Bind parameters in IcebergSparkSqlExtensionsParser`

- **合并时间**：2026-06-05 23:48:16 UTC  
- **作者**：[j1wonpark](https://github.com/j1wonpark)  
- **标签**：`spark`  
- **链接**：https://github.com/apache/iceberg/pull/16626

#### 问题背景
Spark 4.1 新增了参数化查询路由接口 `ParserInterface.parsePlanWithParameters(sqlText, parameterContext)`（来自 [SPARK-53573](https://issues.apache.org/jira/browse/SPARK-53573)）。`IcebergSparkSqlExtensionsParser` 只覆盖了 `parsePlan` 而未覆盖 `parsePlanWithParameters`，导致：

- 所有参数绑定上下文（`ParameterContext`）被静默丢弃
- `?` 位置参数和 `:name` 命名参数均无法正常绑定
- **仅 Spark 4.1 受影响**（3.5 和 4.0 在解析后绑定参数，无此问题）

#### 修改文件

```
spark/v4.1/spark-extensions/src/main/scala/.../IcebergSparkSqlExtensionsParser.scala
spark/v4.1/spark-extensions/src/test/java/.../TestExtendedParser.java
```

#### 核心代码变更

```scala
// 新增 parsePlanWithParameters 方法
override def parsePlanWithParameters(
    sqlText: String,
    parameterContext: ParameterContext): LogicalPlan = {
  // Iceberg DDL 语法不接受参数标记，委托给包装的 Spark 解析器
  parsePlanWithDelegate(sqlText, Some(parameterContext))
}

// 提取共享辅助方法消除重复代码
private def parsePlanWithDelegate(
    sqlText: String,
    parameterContext: Option[ParameterContext] = None): LogicalPlan = {
  // Iceberg 命令走原有路径，其他 SQL 委托给 Spark 解析器（含参数上下文）
  ...
}
```

#### 测试覆盖

| 测试方法 | 验证内容 |
|----------|----------|
| `testParsePlanWithParametersDelegatesForNonIcebergSql` | 非 Iceberg SQL 正确携带参数上下文委托给 Spark |
| `testParsePlanWithParametersBindsPositionalParameter` | `SELECT ? AS id` 端到端绑定成功 |
| `testParsePlanWithParametersBindsNamedParameter` | `SELECT :id AS id` 端到端绑定成功 |

---

### PR #16523 — `Spark: Fix time-travel filter on renamed columns in distributed planning mode`

- **合并时间**：2026-06-05 23:56:13 UTC  
- **作者**：[lilei1128](https://github.com/lilei1128)  
- **标签**：`spark` · `core`  
- **链接**：https://github.com/apache/iceberg/pull/16523

#### 问题详情
在启用分布式规划模式（`read.data-planning-mode=distributed`）时，执行时间旅行（time-travel）查询并在已被**重命名的列**上应用过滤条件，会抛出 `ValidationException`：

```
Cannot find field 'col' in struct: struct<..., 2: value: ...>
```

#### 根本原因（双重问题）

**问题一：Driver 侧**
```java
// 修复前（错误）：返回绑定到当前 schema 的 partition specs
return table().specs();

// 修复后（正确）：返回绑定到 snapshot schema 的 partition specs
return specs();
```
`BaseDistributedDataScan.specCache()` 调用了 `table().specs()`，返回的是**当前表 schema** 绑定的分区规格。当 `Projections.inclusive()` 用于过滤表达式投影时，尝试用**当前 schema** 解析列名，导致找不到列。

**问题二：Executor 侧**
`ReadDataManifest` 和 `ReadDeleteManifest` 在 executor 上也调用了 `table.value().specs()`，同样返回当前 schema 的分区规格，导致 `filterRows(filter)` 无法绑定已重命名列的过滤条件。

#### 修改文件

```
spark/v3.5/spark/src/main/scala/.../SparkDistributedDataScan.scala
spark/v3.5/spark-extensions/src/test/java/.../TestSelect.java

spark/v4.0/spark/src/main/scala/.../SparkDistributedDataScan.scala
spark/v4.0/spark-extensions/src/test/java/.../TestSelect.java

spark/v4.1/spark/src/main/scala/.../SparkDistributedDataScan.scala
spark/v4.1/spark-extensions/src/test/java/.../TestSelect.java

core/src/main/java/org/apache/iceberg/BaseDistributedDataScan.java
```

#### 修复示意图

```
时间旅行查询: SELECT * FROM t TIMESTAMP AS OF '...' WHERE old_name = 'x'
                                                          ↑ 列被重命名了

修复前流程:
  Driver: specCache → table().specs() [当前schema] → Projections.inclusive() → ❌ 找不到 old_name
  
修复后流程:
  Driver: specCache → specs() [snapshot schema]   → Projections.inclusive() → ✅ 正确解析
  Executor: 使用 Driver 传入的 snapshot specs                                → ✅ 正确过滤
```

#### 测试覆盖（每个 Spark 版本）

| 测试方法 | 场景 |
|----------|------|
| `testTimeTravelFilterOnRenamedColumn()` | 基础时间旅行过滤 + 列重命名 |
| `testTimeTravelFilterOnRenamedColumnWithDeleteFiles()` | 同上，附加 delete files 场景 |

---

## 🆕 新建 PR（Open，待审查）

| # | 标题 | 标签 | 作者 | 创建时间 |
|---|------|------|------|----------|
| [#16692](https://github.com/apache/iceberg/pull/16692) | Parquet: Fix initial-default rows dropped when filtering on the defaulted column | `spark` `parquet` `data` | cbb330 | 09:29 |
| [#16691](https://github.com/apache/iceberg/pull/16691) | Core: Skip unnecessary manifest scans during expire snapshots metadata cleanup | `core` | zhongyujiang | 08:58 |
| [#16689](https://github.com/apache/iceberg/pull/16689) | Core: Add EntryStatus.MODIFIED and TrackingBuilder status derivation | `core` | anoopj | 08:16 |
| [#16688](https://github.com/apache/iceberg/pull/16688) | Core: Add writer_format_version field to TrackedFile | `core` | gaborkaszab | 08:00 |
| [#16687](https://github.com/apache/iceberg/pull/16687) | AWS: Support assumed role credentials for REST SigV4 signing | `AWS` `docs` | Martozar | 07:50 |
| [#16686](https://github.com/apache/iceberg/pull/16686) | Core: Fix thread conflict when deleting duplicate files in manifest | `core` | HuaHuaY | 06:31 |

### 重点新 PR 简述

#### PR #16692 — Parquet: Fix initial-default rows dropped when filtering
**关键问题**：`ParquetMetricsRowGroupFilter` 对文件中**不存在的列**按全 null 处理，导致带 `initial-default` 的列在 schema 演化后，旧文件行被过滤器错误地跳过（静默丢数据）。

**修复思路**：当谓词引用的列在文件中不存在但有 `initialDefault` 时，对 default 值评估谓词，而非退回到 null 假设路径。

**影响**：`ParquetReader` 和 `VectorizedParquetReader` 均共享此过滤器，两条读取路径同时被修复。

#### PR #16691 — Core: Skip unnecessary manifest scans during expire snapshots
**优化点**：`cleanExpiredMetadata` 启用时需扫描所有 retained snapshot 的 manifest list，对保留大量 snapshot 的表开销极大。

**两项优化**：
1. 若表只有单一 spec 且为当前默认 spec，直接跳过整个扫描
2. 扫描过程中一旦所有已知 spec 都已确认可达，后续 snapshot 不再扫描

#### PR #16686 — Core: Fix thread conflict when deleting duplicate files in manifest
**并发 Bug**：`deleteFiles` 和 `duplicateDeleteCount` 在 `filterManifests` 中被并发修改，且 commit 失败重试时 `duplicateDeleteCount` 会被重复计数。

---

## 🐛 新增 Issue

### Issue #16690 — `v3 initial-default rows are silently dropped when a query filters on the defaulted column`

- **状态**：Open（已有对应修复 PR #16692）
- **严重程度**：高（静默错误结果，不抛异常）
- **标签**：`bug`
- **报告者**：cbb330

**现象对比**：

| 查询 | 预期 | 实际 |
|------|------|------|
| `SELECT id, c` | `(1,US),(2,US)` | `(1,US),(2,US)` ✅ |
| `WHERE c = 'US'` | `[1, 2]` | `[2]` ❌ |
| `WHERE upper(c) = 'US'` | `[1, 2]` | `[2]` ❌ |
| `WHERE c IS NOT NULL` | `[1, 2]` | `[2]` ❌ |
| `WHERE c IS NULL` | `[]` | `[]` ✅ |

**根因**：默认值在 per-format reader 中 record-level filtering **之后**注入，filter 读取缺列为 null，导致该行在 default 被应用之前就被过滤掉。

---

### Issue #16685 — `Unexpected duplicate ID after ingest: input parquet has single ID, but Iceberg table keeps one duplicated key`

- **状态**：Open
- **标签**：`bug`
- **报告者**：uthanuja
- **环境**：Spark 3.5.x + Iceberg 1.9.2

**现象**：MERGE/UPSERT 操作后，某一特定 ID 出现持久化重复，且删除后重新 ingest 仍会复现。源 Parquet 文件已确认无重复。

---

### Issue #16683 — `Spark 4.1: Fail fast when a parameter context reaches an Iceberg DDL command`

- **状态**：Open
- **来源**：PR #16626 review 中的非阻塞建议独立拆出
- **报告者**：j1wonpark（即 #16626 作者）

**提案**：当非空 `parameterContext` 被路由到 Iceberg DDL 命令时，应主动抛出清晰异常而非静默忽略，避免调用方产生误解。

---

## 📈 趋势分析

### 本日活动热点

```
模块分布（合并 PR）:
████████████ Spark       4 个 PR（67%）
████         Core/Data   2 个 PR（33%）

云存储:
███          AWS/GCP/Azure  1 个 PR（修复 EOF Bug）
```

### 关键趋势
1. **Spark 4.1 适配** 是本日最活跃主题（参数绑定修复 + 迁移功能扩展）
2. **云存储修复**：三大平台（GCS/S3/ADLS）同时修复 EOF 处理 Bug，提升 IO 可靠性
3. **Schema 演化正确性**：initial-default 相关 bug（issue #16690 + PR #16692）反映了 v3 格式下 schema 演化读取路径仍有待完善的角落

---

## 🔗 参考链接

- Apache Iceberg GitHub: https://github.com/apache/iceberg
- Fork 仓库: https://github.com/vinlee19/iceberg
- 本报告对应时间范围: 2026-06-05

---

*报告生成时间：2026-06-06 | 由自动化脚本基于 GitHub API 数据生成*
