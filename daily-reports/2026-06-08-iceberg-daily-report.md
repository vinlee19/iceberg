# Apache Iceberg 每日动态报告

**日期：** 2026-06-08  
**数据来源：** [apache/iceberg](https://github.com/apache/iceberg)  
**Fork 同步状态：** ✅ 已同步（`vinlee19/iceberg` main 分支已 fast-forward 至 `d2de290`）

---

## 目录

1. [Fork 同步摘要](#1-fork-同步摘要)
2. [总览数据面板](#2-总览数据面板)
3. [已合并 PR 详细分析（6 个）](#3-已合并-pr-详细分析)
4. [新增 Issue 分析（2 个）](#4-新增-issue-分析)
5. [新增 PR 列表（15 个）](#5-新增-pr-列表)
6. [技术趋势总结](#6-技术趋势总结)

---

## 1. Fork 同步摘要

```
操作：git fetch upstream main && git merge upstream/main --ff-only && git push origin main
结果：457854f → d2de290（新增 6 个 commit，67 个文件变更）
      3445 行新增 / 611 行删除
```

本次同步涵盖了 2026-06-08 当天合并的全部 6 个 PR 的代码变更。

---

## 2. 总览数据面板

```
┌─────────────────────────────────────────────────────┐
│           2026-06-08  Apache Iceberg 活动            │
├──────────────┬──────────────────────────────────────┤
│  已合并 PR   │  6 个                                │
│  新增 Issue  │  2 个（1 bug + 1 improvement）        │
│  新增 PR     │  15 个（含当天已合并的）              │
│  代码变更    │  +3445 / -611（67 个文件）            │
└──────────────┴──────────────────────────────────────┘
```

### 模块分布

| 模块     | 合并 PR | 新增 PR |
|----------|---------|---------|
| Core     | 2       | 3       |
| Parquet  | 1       | 3       |
| Data     | 1       | 1       |
| Docs     | 2       | 1       |
| Spark    | 0       | 4       |
| Arrow    | 0       | 2       |
| Flink    | 0       | 1       |
| Spec     | 0       | 1       |

---

## 3. 已合并 PR 详细分析

---

### 🔵 PR #16439 — Core: Align content stats fields with latest Spec changes

| 属性 | 内容 |
|------|------|
| **作者** | nastra |
| **标签** | `core` |
| **合并时间** | 2026-06-08T21:12:54Z |
| **链接** | https://github.com/apache/iceberg/pull/16439 |
| **变更规模** | 9 个文件，+606 行 / -268 行 |

#### 背景

该 PR 配合 [#14234](https://github.com/apache/iceberg/pull/14234)（Spec 变更）中引入的最新 Content Statistics 字段规范，对现有的统计字段做了全面对齐。另外为 Geo 和 Variant 类型更新了 content stats schema 的生成逻辑。两个保留元数据字段的计算更新（reserved metadata fields）留作后续单独 PR 处理。

#### 核心变更：`FieldStatistic` 枚举重排序

**变更前：**
```java
enum FieldStatistic {
  VALUE_COUNT(1, "value_count"),
  NULL_VALUE_COUNT(2, "null_value_count"),
  NAN_VALUE_COUNT(3, "nan_value_count"),
  AVG_VALUE_SIZE(4, "avg_value_size_in_bytes"),
  MAX_VALUE_SIZE(5, "max_value_size_in_bytes"),
  LOWER_BOUND(6, "lower_bound"),
  UPPER_BOUND(7, "upper_bound"),
  EXACT_BOUNDS(8, "exact_bounds");
}
```

**变更后：**
```java
enum FieldStatistic {
  LOWER_BOUND(1, "lower_bound"),
  UPPER_BOUND(2, "upper_bound"),
  TIGHT_BOUNDS(3, "tight_bounds"),
  VALUE_COUNT(4, "value_count"),
  NULL_VALUE_COUNT(5, "null_value_count"),
  NAN_VALUE_COUNT(6, "nan_value_count"),
  AVG_VALUE_SIZE_IN_BYTES(7, "avg_value_size_in_bytes");

  // Geo 类型 lower/upper 结构内的偏移量
  private static final int GEO_LOWER_X_OFFSET = 10;
  private static final int GEO_LOWER_Y_OFFSET = 11;
  private static final int GEO_LOWER_Z_OFFSET = 12;
  private static final int GEO_LOWER_M_OFFSET = 13;
  private static final int GEO_UPPER_X_OFFSET = 14;
  private static final int GEO_UPPER_Y_OFFSET = 15;
  private static final int GEO_UPPER_Z_OFFSET = 16;
  private static final int GEO_UPPER_M_OFFSET = 17;
}
```

#### 关键变化点

1. **字段顺序重排**：将 `LOWER_BOUND`/`UPPER_BOUND` 提前到首位，与最新规范保持一致。
2. **新增字段 `TIGHT_BOUNDS`**：替代旧的 `EXACT_BOUNDS`，用于表示边界是否精确。
3. **移除 `MAX_VALUE_SIZE`**：规范中已移除该字段。
4. **Geo 类型 4D 坐标支持**：新增 X/Y/Z/M 四个维度的 lower/upper 偏移常量，支持三维空间（Z 轴）和测量值（M 轴）。
5. **`fromPosition` 映射同步更新**：枚举位置与新顺序保持一致。

#### 影响文件

```
core/src/main/java/org/apache/iceberg/FieldStatistic.java   (+66/-28)
core/src/main/java/org/apache/iceberg/BaseFieldStats.java    (+61/-61)
core/src/main/java/org/apache/iceberg/BaseContentStats.java  (+24/-18)
core/src/main/java/org/apache/iceberg/FieldStats.java        (+20/-19)
core/src/main/java/org/apache/iceberg/StatsUtil.java         (+8/-4)
core/src/test/java/...TestStatsUtil.java                     (+204/-71)
core/src/test/java/...TestContentStats.java                  (+112/-45)
core/src/test/java/...TestFieldStats.java                    (+24/-33)
core/src/test/java/...TestTrackedFileStruct.java             (+12/-8)
```

> 📝 **注：** 该 PR 由作者借助 Claude Opus 4.7 辅助生成，并经手工调整。

---

### 🟢 PR #16723 — Parquet: Pre-size row-group filter maps to the column count

| 属性 | 内容 |
|------|------|
| **作者** | wombatu-kun |
| **标签** | `parquet` |
| **合并时间** | 2026-06-08T11:57:37Z |
| **链接** | https://github.com/apache/iceberg/pull/16723 |
| **变更规模** | 3 个文件，+14 行 / -10 行（极简改动，高价值）|

#### 问题分析

Parquet 行组过滤器（`ParquetMetricsRowGroupFilter`、`ParquetDictionaryRowGroupFilter`、`ParquetBloomRowGroupFilter`）在每次处理一个行组时，都会用 `Maps.newHashMap()` 初始化内部的每列 Map（`stats`、`valueCounts`、`conversions`）。当表有宽 Schema（列数多）时，HashMap 内部 `Node[]` 数组会随着插入量增加而**多次扩容**，每次扩容都会触发 rehash 和内存重分配。

#### 解决方案：预设初始容量

```java
// 变更前：每次都从空 HashMap 开始
this.stats = Maps.newHashMap();
this.valueCounts = Maps.newHashMap();
this.conversions = Maps.newHashMap();

// 变更后：按行组列数预设容量
int columnCount = rowGroup.getColumns().size();
this.stats = Maps.newHashMapWithExpectedSize(columnCount);
this.valueCounts = Maps.newHashMapWithExpectedSize(columnCount);
this.conversions = Maps.newHashMapWithExpectedSize(columnCount);
```

#### JMH 基准测试结果（64 列 Schema）

| 指标 | 变更前 | 变更后 | 提升 |
|------|--------|--------|------|
| **内存分配** | 17,280 B/op | 14,448 B/op | **-16%** |
| **执行时间** | 8.1 μs/op | 6.3 μs/op | 更快（CI 重叠，趋势一致）|

> 💡 **为什么有效：** `shouldRead` 是每个 Parquet 文件、每个行组都必经的扫描规划路径。在大型查询中调用频率极高，细微的分配优化累积效果显著。懒加载缓存（`dictCache`、`bloomCache`）和 `fieldsWithBloomFilter` 集合保持默认大小不变（它们不会填满到列数）。

---

### 🔵 PR #16582 — API, Core: Implement filter() for partition statistics scan API

| 属性 | 内容 |
|------|------|
| **作者** | gaborkaszab |
| **标签** | `API`, `core` |
| **合并时间** | 2026-06-08T11:31:16Z |
| **链接** | https://github.com/apache/iceberg/pull/16582 |
| **变更规模** | 3 个文件，+370 行 / -7 行 |

#### 背景

`PartitionStatisticsScan` API 此前的 `filter()` 方法直接抛出 `UnsupportedOperationException`，分区统计扫描无法按条件过滤，功能残缺。

#### 新增能力

**1. `caseSensitive()` 方法（接口层）**

```java
// PartitionStatisticsScan.java
default PartitionStatisticsScan caseSensitive(boolean caseSensitive) {
    throw new UnsupportedOperationException("caseSensitive is not supported");
}
```

**2. 完整的 `filter()` 实现（实现层）**

```java
// BasePartitionStatisticsScan.java
private Expression filter = Expressions.alwaysTrue();
private boolean caseSensitive = true;

@Override
public PartitionStatisticsScan filter(Expression newFilter) {
    Preconditions.checkArgument(newFilter != null, "Invalid filter: null");
    this.filter = newFilter;
    return this;
}

@Override
public PartitionStatisticsScan caseSensitive(boolean newCaseSensitive) {
    this.caseSensitive = newCaseSensitive;
    return this;
}
```

**3. 读取时应用过滤器**

```java
// 在 planWith() / planWithout() 返回结果前应用过滤
if (filter != Expressions.alwaysTrue()) {
    Evaluator evaluator = new Evaluator(readSchema.asStruct(), filter, caseSensitive);
    result = CloseableIterable.filter(result, evaluator::eval);
}
```

#### 设计要点

- 使用 `Expressions.alwaysTrue()` 作为默认 filter（不过滤），兼容无 filter 场景
- 使用 `Evaluator` 绑定 Schema 进行表达式求值，支持列名大小写敏感控制
- 测试用例新增 309 行，覆盖各类过滤场景

---

### 🟡 PR #15795 — Data: Add comprehensive data type tests to Format Model TCK

| 属性 | 内容 |
|------|------|
| **作者** | rambleraptor |
| **标签** | `data`, `spark`, `flink` |
| **合并时间** | 2026-06-08T11:50:19Z |
| **链接** | https://github.com/apache/iceberg/pull/15795 |
| **变更规模** | 8 个文件（+4 个新文件），+351 行 / -56 行 |

#### 目标

Format Model TCK（Technology Compatibility Kit）此前对基础数据类型的覆盖不完整。本 PR 确保 TCK 覆盖所有主要的原始类型（primitive types）。

#### 新增测试基础设施

新增 4 个测试辅助类：

```
data/src/test/java/org/apache/iceberg/data/avro/AvroFormat.java     (新建)
data/src/test/java/org/apache/iceberg/data/orc/OrcFormat.java       (新建)
data/src/test/java/org/apache/iceberg/data/parquet/ParquetFormat.java (新建)
data/src/test/java/org/apache/iceberg/data/FileFormatTestSupport.java (新建)
```

#### 覆盖范围扩展

各引擎新增测试方法数（概况）：

| 引擎 | 新增测试 |
|------|---------|
| Spark 3.5 / 4.0 / 4.1 | 各 +14 行 |
| Flink 1.20 / 2.0 / 2.1 | 各 +10~11 行 |

`DataGenerators.java` 新增 169 行，`BaseFormatModelTests.java` 重构为更模块化的结构（+164/-56 行）。

---

### 📄 PR #16443 — Docs: Add Dataddo to the vendor list

| 属性 | 内容 |
|------|------|
| **作者** | cenotee |
| **标签** | `docs` |
| **合并时间** | 2026-06-08T11:34:54Z |
| **链接** | https://github.com/apache/iceberg/pull/16443 |
| **变更规模** | 1 个文件（`site/docs/vendors.md`），+8 行 |

将 **Dataddo**（数据集成平台）添加到 Apache Iceberg 官方支持的供应商/集成列表中。

---

### 📄 PR #16712 — Docs: Add Apache Iceberg Summit 2026 Playlist

| 属性 | 内容 |
|------|------|
| **作者** | ebyhr |
| **标签** | `docs` |
| **合并时间** | 2026-06-08T17:03:36Z |
| **链接** | https://github.com/apache/iceberg/pull/16712 |
| **变更规模** | 1 个文件（`site/docs/talks.md`），+3 行 |

在官方演讲页面新增 **Apache Iceberg Summit 2026 播放列表**链接。

---

## 4. 新增 Issue 分析

### 🐛 Issue #16720 — FormatModelRegistry init fails when iceberg-orc is absent

| 属性 | 内容 |
|------|------|
| **作者** | shihadaf |
| **类型** | Bug |
| **状态** | Open |
| **链接** | https://github.com/apache/iceberg/issues/16720 |

#### 问题描述

`GenericFormatModels.register()` 中直接引用了 `org.apache.iceberg.orc.OrcRowWriter` 作为类字面量。当 `iceberg-orc` 模块不在 classpath 时，JVM 会抛出 `NoClassDefFoundError`（这是 `Error` 而非 `Exception`）。

`DynMethods.invoke()` 会直接重新抛出 `Error`，绕过了 catch 块。这导致 `FormatModelRegistry` 的静态初始化块（`<clinit>`）失败，并**永久毒化**该类加载器生命周期内的 `FormatModelRegistry`。

#### 错误堆栈

```
Caused by: java.lang.NoClassDefFoundError: org/apache/iceberg/orc/OrcRowWriter
    at org.apache.iceberg.data.GenericFormatModels.register(GenericFormatModels.java:55)
    at org.apache.iceberg.formats.FormatModelRegistry.registerSupportedFormats(FormatModelRegistry.java:207)
    at org.apache.iceberg.formats.FormatModelRegistry.<clinit>(FormatModelRegistry.java:69)
Caused by: java.lang.ClassNotFoundException: org.apache.iceberg.orc.OrcRowWriter
```

#### 建议修复

扩大 `registerSupportedFormats()` 中的 catch 范围，捕获 `Error` 或 `NoClassDefFoundError`，以优雅地跳过不可用的格式模块。

---

### 💡 Issue #16726 — Spark: Support read select paths of shredded variant (parquet readers)

| 属性 | 内容 |
|------|------|
| **作者** | qlong |
| **类型** | Improvement |
| **状态** | Open |
| **链接** | https://github.com/apache/iceberg/issues/16726 |

#### 功能需求

这是 [#16448](https://github.com/apache/iceberg/issues/16448)（Spark variant 抽取下推）的配套 issue。

Spark 将 `variant_get` 路径从 Filter/Project 节点下推到 Iceberg 扫描后，需要 Parquet reader 能**选择性地只读取 shredded variant 中的特定路径**，而非返回整个 variant blob。

#### 初版实现范围

- 支持常用标量类型的读取/提取
- 不支持数组、struct、嵌套 struct 等复杂类型（若下推类型不支持，则拒绝下推并返回完整 variant）
- 仅对批量行扫描（batch row scan）生效

> 📌 **关联 PR：** [#16714](https://github.com/apache/iceberg/pull/16714) 和 [#16715](https://github.com/apache/iceberg/pull/16715) 是配套实现 PR，当天已同步提交。

---

## 5. 新增 PR 列表

2026-06-08 共新增 **15 个 PR**（含当天已合并的 6 个），以下列出当天仍处于 Open 状态的新 PR：

### 性能优化类

| PR | 标题 | 作者 | 标签 |
|----|------|------|------|
| [#16729](https://github.com/apache/iceberg/pull/16729) | Core: Combine 3 GET requests for parquet reads | varun-lakhyani | `core`, `spark` |
| [#16722](https://github.com/apache/iceberg/pull/16722) | Parquet: Avoid intermediate BigInteger in int and long decimal readers | wombatu-kun | `parquet` |
| [#16719](https://github.com/apache/iceberg/pull/16719) | Arrow: avoid per-value byte[] in dictionary-encoded fixed-binary decode | wombatu-kun | `arrow` |
| [#16718](https://github.com/apache/iceberg/pull/16718) | Arrow: bulk-copy fixed-size-binary vectorized reads | wombatu-kun | `arrow` |

#### 亮点：PR #16729 — Combine 3 GET requests for parquet reads

针对小文件工作负载（compaction、manifests、小数据文件），每次 Parquet 读取需要 3 次独立 GET 请求（footer size → footer body → row group data），延迟随文件数线性增长成为扩展瓶颈。

**解决方案：** 引入 `SingleFetchInputFile` 装饰器。当文件大小不超过 `read.single-fetch-threshold-bytes` 时，`newStream()` 一次性下载整个文件到内存，后续所有读取（footer、row group）均从内存提供，无需额外远程调用。默认值为 0（关闭），大文件走原有路径，完全向后兼容。

#### 亮点：PR #16718 — Arrow: bulk-copy fixed-size-binary vectorized reads

`FixedSizeBinaryReader`（用于 UUID、fixed[N]、高精度 decimal 等 `FIXED_LEN_BYTE_ARRAY` 类型）此前逐个值读取并拷贝。由于 `FIXED_LEN_BYTE_ARRAY` 在文件中连续存储，与 `FixedSizeBinaryVector` 内存布局相同，逐值循环是纯开销。

**解决方案：** 新增 `readFixedLengthBytes` 批量读取接口，在 `FixedSizeBinaryReader` 中引入 `nextRleBatch` 快速路径，一次 memcpy 完成整个非空值段的复制。

**JMH 基准测试（1.5M 行，uuid + decimal(38,0) + fixed[16]）：**

| 压缩格式 | 改前 | 改后 | 提速 |
|----------|------|------|------|
| gzip | 275.98 ms/op | 170.96 ms/op | **1.6x** |
| 无压缩 | 171.30 ms/op | 65.91 ms/op | **2.6x** |

---

### 新功能类

| PR | 标题 | 作者 | 标签 |
|----|------|------|------|
| [#16730](https://github.com/apache/iceberg/pull/16730) | Core: extract parallel manifest write logic out of SnapshotProducer | dramaticlly | `core` |
| [#16727](https://github.com/apache/iceberg/pull/16727) | Spec: Add optional specific-name to UDF definition model | szehon-ho | `Specification` |
| [#16715](https://github.com/apache/iceberg/pull/16715) | Spark: Implement variant extraction pushdown for shredded VARIANT columns | qlong | `API`, `spark` |
| [#16714](https://github.com/apache/iceberg/pull/16714) | Spark: Add selective shredded variant extraction Parquet readers | qlong | `API`, `spark`, `parquet` |
| [#16724](https://github.com/apache/iceberg/pull/16724) | Data: Add TCK for Encrypt in FileFormat API | Guosmilesmile | `data` |

#### 亮点：PR #16714/#16715 — Spark Variant 抽取下推（端到端）

这是一组配套 PR，实现了 Spark 对 shredded VARIANT 列的路径提取下推优化：

- **#16714**：实现选择性 Parquet 读取器（`ParquetVariantExtractionReaders`），只读取请求路径对应的 shredded typed_value 列，而非整个 variant blob
- **#16715**：实现 Spark 侧 DSv2 pushdown（`SparkVariantExtractionScanBuilder`），将 `variant_get` 路径下推到 Iceberg 扫描

**实测性能（GitHub Activities 数据，299 个 shredded 列，12 个查询）：**

| 对比 | 总计时间 | vs. 基线 |
|------|----------|---------|
| **Variant + 下推（基线）** | **49.74 s** | — |
| string_json 存储 | 63.66 s | +28.0%（Variant 更快）|
| Variant 不下推 | 735.39 s | **+1379%**（慢 14 倍！）|

#### 亮点：PR #16727 — Spec: Add optional specific-name to UDF definition model

在 UDF 函数定义模型中新增可选的 `specific-name` 字段，类比 SQL 标准中的 routine specific name。该字段让引擎可以用稳定的用户自定义名称（而非基于签名推导的 `definition-id`）来标识一个函数重载，支持 `DROP SPECIFIC FUNCTION add_one_int` 这样的语法。

该字段为可选，向后兼容，不影响重载解析（重载解析仍基于 `parameters`）。

---

### Bug 修复 / 文档类

| PR | 标题 | 作者 | 标签 |
|----|------|------|------|
| [#16716](https://github.com/apache/iceberg/pull/16716) | Spark 4.1: Fail fast when a parameter context reaches an Iceberg DDL command | j1wonpark | `spark` |
| [#16728](https://github.com/apache/iceberg/pull/16728) | Flink: SQL: Pass only white-listed Catalog properties for Table LIKE | swapna267 | `flink` |
| [#16721](https://github.com/apache/iceberg/pull/16721) | Docs: Clarify Spark `spark.sql.adaptive.advisoryPartitionSizeInBytes` | pan3793 | `docs` |
| [#16725](https://github.com/apache/iceberg/pull/16725) | Docker: Add configuration to toggle info logs flooding in iceberg-rest-fixture | sejal-gupta-ksolves | — |

**PR #16716** 修复了 Spark 4.1 SQL 解析器的一个静默错误：带参数上下文（`?` / `:name`）的查询到达 Iceberg DDL 命令时，参数上下文会被静默丢弃而不是报错。现在改为快速失败并抛出 `IcebergParseException`，提示更清晰。

---

### 构建 / 验证类

| PR | 标题 | 作者 | 标签 |
|----|------|------|------|
| [#16717](https://github.com/apache/iceberg/pull/16717) | Spark: Verify Spark 4.0.3 [Draft] | manuzhang | `build` |

---

## 6. 技术趋势总结

### 🚀 本日核心趋势

#### 1. Variant / Shredded Variant 生态加速完善

PR #16714、#16715（新增）以及 Issue #16726 共同构成了 Spark Variant 抽取下推的完整链路。实测在工业级数据集上，与不下推相比性能提升高达 **14 倍**，是近期最重要的性能工程突破之一。

#### 2. Parquet / Arrow 内存分配持续精细化

同一作者（wombatu-kun）一天内提交了 3 个独立的性能 PR（#16723 已合并，#16722、#16718、#16719 新增），围绕 Parquet 和 Arrow 的 scan 路径进行系统性的内存分配优化：

```
已合并：行组 filter Map 预分配      → -16% 分配
待合并：BigInteger 消除              → -35% 分配（1M 行 decimal 读取）
待合并：Arrow fixed-binary 批量复制 → 2.6x 提速（无压缩）
待合并：Arrow 字典编码 byte[] 消除  → 每值 0B 分配
```

#### 3. Spec 对齐与 API 完善

- PR #16439 将内容统计字段与最新规范对齐，`TIGHT_BOUNDS` 替代 `EXACT_BOUNDS`，移除 `MAX_VALUE_SIZE`，新增 Geo 4D 坐标支持
- PR #16582 真正实现了 `PartitionStatisticsScan.filter()`，不再是 `UnsupportedOperationException`

#### 4. 小文件读取优化

PR #16729 提出的 `SingleFetchInputFile` 方案，将小文件场景的 3 次 GET 合并为 1 次，对 compaction 和 manifest 读取有直接收益。

---

**报告生成时间：** 2026-06-09  
**下次同步时间：** 2026-06-09（自动）
