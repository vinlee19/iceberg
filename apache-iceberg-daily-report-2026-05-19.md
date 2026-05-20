# Apache Iceberg 每日动态分析报告

> **报告日期**: 2026-05-20（分析覆盖 2026-05-19 全天）
> **数据来源**: [apache/iceberg](https://github.com/apache/iceberg)
> **Fork 同步状态**: ✅ 已同步至 `vinlee19/iceberg`（71 个文件变更，+1670 / -868 行）

---

## 📊 活动概览

```
┌─────────────────────────────────────────────────┐
│           2026-05-19  活动统计                   │
├─────────────────┬───────────────────────────────┤
│ 已合并 PR       │  11 个                         │
│ 新增 PR         │  23 个（含已合并）              │
│ 新增 Issue      │  6 个                           │
│ 主要领域        │  Flink · Spark · Docs · Build   │
│ 重大事件        │  🎉 Apache Iceberg 1.11.0 发布   │
└─────────────────┴───────────────────────────────┘
```

> **亮点**: 2026-05-19 是 Apache Iceberg **1.11.0 正式发布日**！
> 大量 PR 围绕版本发布后的文档整理、构建配置更新展开，
> 同时还有一个重要的 Flink 功能增强被合并。

---

## 🔀 Fork 同步记录

| 项目 | 详情 |
|------|------|
| 上游仓库 | `apache/iceberg` main 分支 |
| 目标仓库 | `vinlee19/iceberg` main 分支 |
| 同步时间 | 2026-05-20 |
| 文件变更 | 71 个文件 |
| 新增代码行 | +1,670 行 |
| 删除代码行 | -868 行 |
| 新增文件 | `.github/trivyignores/spark-runtime-3.4_2.12.trivyignore`<br>`spark/v4.1/spark/src/jmh/java/.../IcebergCompactionBenchmark.java`<br>`spark/v4.1/spark/src/jmh/java/.../IcebergDataCompactionBenchmark.java` |

---

## ✅ 已合并 PR 详细分析（2026-05-19）

### 🔥 重点 PR

---

### PR #16065 — Flink: DynamicSink 支持细粒度资源管理的 Slot Sharing Group 配置

| 字段 | 内容 |
|------|------|
| **编号** | [#16065](https://github.com/apache/iceberg/pull/16065) |
| **标题** | Flink: Allow setting slot sharing group for fine-grained resource management in DynamicSink |
| **作者** | [@sqd](https://github.com/sqd) |
| **标签** | `flink`, `docs` |
| **合并时间** | 2026-05-19 13:02 UTC |
| **审阅评论** | 13 条 |

#### 背景与问题

Flink DynamicSink 中所有 Operator 默认加入同一个 Slot Sharing Group（`default`），
导致 TaskManager 中资源被平均分配。然而 **Shuffle Writer** 和 **Generator + Forward Writer** 实际上比其他 Operator 消耗更多资源（CPU、内存、I/O），平均分配造成资源浪费。

```
旧行为（所有算子共享 default slot sharing group）：
┌─────────────────────────────────────────────────────────┐
│                   default slot group                     │
│  [Source] → [Operator] → [Shuffle Writer] → [Generator] │
│             均等资源分配，资源效率低下                    │
└─────────────────────────────────────────────────────────┘

新行为（可独立配置 slot sharing group）：
┌────────────────────┐  ┌─────────────────────────────────┐
│   default group    │  │   iceberg-sink-heavy group       │
│  [Source][Op...]   │  │  [Shuffle Writer]                │
└────────────────────┘  │  [Generator + Forward Writer]   │
                        │   ← 链式融合，共享同一 Group     │
                        └─────────────────────────────────┘
```

#### 变更内容

Flink 的 [Fine-Grained Resource Management](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/finegrained_resource/) 允许用户为不同算子指定不同的 Slot Sharing Group，以实现差异化资源分配。

此 PR 为 DynamicSink 暴露两个新配置项：

| 配置项 | 说明 |
|--------|------|
| `shuffle-writer-slot-sharing-group` | Shuffle Writer 算子的 Slot Sharing Group 名称 |
| `generator-slot-sharing-group` | Generator + Forward Writer 算子的 Slot Sharing Group 名称 |

> **注意**: Generator 和 Forward Writer 必须共享同一个 Slot Sharing Group 才能实现算子链（Operator Chaining）。

#### 使用示例

```java
// 通过 FlinkSink.Builder 设置
FlinkSink.forRowData(dataStream)
    .table(table)
    .set("shuffle-writer-slot-sharing-group", "iceberg-sink-shuffle")
    .set("generator-slot-sharing-group", "iceberg-sink-write")
    .append();
```

#### 影响范围

- `flink/v1.20/` 和 `flink/v2.0/` 两个版本均需适配（#16429 已开启 Backport PR）
- 涉及 `DynamicSink`, `IcebergSink`, 相关配置类和文档

---

### PR #16371 — Docs: 修复文档站点搜索结果重复问题

| 字段 | 内容 |
|------|------|
| **编号** | [#16371](https://github.com/apache/iceberg/pull/16371) |
| **标题** | Docs: Fix duplicate search results by moving versioned docs out of docs_dir |
| **作者** | [@kevinjqliu](https://github.com/kevinjqliu) |
| **标签** | `INFRA`, `docs` |
| **合并时间** | 2026-05-19 02:27 UTC |
| **评论数** | 5 条 |

#### 问题根因

```
旧目录结构（有问题）：
site/
└── docs/           ← MkDocs docs_dir
    └── docs/
        └── <version>/  ← 版本化文档在 docs_dir 内部
            └── docs/

MkDocs 直接扫描 + mkdocs-monorepo-plugin 注入 → 同一页面被渲染两次 → 搜索重复
```

```
新目录结构（已修复）：
site/
├── docs/              ← MkDocs docs_dir（不含版本文档）
└── versioned-docs/    ← 版本化文档移出 docs_dir
    └── <version>/
        └── docs/

只有 monorepo-plugin 渲染版本化文档 → 搜索结果唯一
```

#### 修改文件

| 文件 | 变更说明 |
|------|----------|
| `site/dev/common.sh` | worktree 路径由 `docs/docs/` 改为 `versioned-docs/` |
| `site/nav.yml` | `!include` 路径更新 |
| `site/mkdocs-dev.yml` | 路径更新 |
| `site/README.md` | 文档说明更新 |
| `.gitignore` | 忽略规则更新 |

---

### PR #16409 — Core: 删除冗余字符串拼接

| 字段 | 内容 |
|------|------|
| **编号** | [#16409](https://github.com/apache/iceberg/pull/16409) |
| **标题** | Core: Remove redundant string concatenation |
| **作者** | [@ebyhr](https://github.com/ebyhr) |
| **标签** | `core` |
| **合并时间** | 2026-05-19 06:53 UTC |

#### 变更说明

清理代码中无意义的字符串拼接，例如：

```java
// 旧代码（冗余）
throw new RuntimeException("" + someVariable);
String msg = "" + anotherString;

// 新代码（精简）
throw new RuntimeException(String.valueOf(someVariable));
String msg = anotherString;
```

这是一个纯代码质量优化 PR，不影响功能逻辑，提升代码可读性和编译效率。

---

### PR #16382 — Build: 升级 GCP libraries-bom 依赖

| 字段 | 内容 |
|------|------|
| **编号** | [#16382](https://github.com/apache/iceberg/pull/16382) |
| **标题** | Build: Bump com.google.cloud:libraries-bom from 26.80.0 to 26.81.0 |
| **作者** | [@huaxingao](https://github.com/huaxingao) |
| **标签** | `GCP`, `KAFKACONNECT` |
| **合并时间** | 2026-05-19 02:29 UTC |

将 Google Cloud Java BOM 从 `26.80.0` 升级至 `26.81.0`，同步最新 GCS 客户端依赖，
修复潜在的 CVE 安全漏洞。此 PR 取代了先前的 #16377。

---

### 🏷️ 1.11.0 版本发布后配套 PR

以下 PR 为 **Apache Iceberg 1.11.0 发布**后的标准化后处理步骤：

| PR | 标题 | 作者 | 合并时间 | 说明 |
|----|------|------|----------|------|
| [#16428](https://github.com/apache/iceberg/pull/16428) | Docs: Update Javadocs for 1.11.0 | @aihuaxu | 17:20 | 发布版本化 Javadoc |
| [#16427](https://github.com/apache/iceberg/pull/16427) | Docs: add versioned docs for 1.11.0 | @aihuaxu | 17:31 | 添加 1.11.0 版本文档站点 |
| [#16415](https://github.com/apache/iceberg/pull/16415) | Doap: Update DOAP to reference 1.11.0 | @aihuaxu | 07:18 | 更新 Apache DOAP 项目描述文件 |
| [#16413](https://github.com/apache/iceberg/pull/16413) | infra: add 1.11.0 to issue template | @aihuaxu | 06:56 | Issue 模板中加入 1.11.0 版本选项 |
| [#16412](https://github.com/apache/iceberg/pull/16412) | Build: Let revapi compare against 1.11.0 | @aihuaxu | 06:55 | API 兼容性检查基线升至 1.11.0 |

---

### 📝 1.10.2 版本发布说明 PR

| PR | 标题 | 作者 | 合并时间 | 说明 |
|----|------|------|----------|------|
| [#16406](https://github.com/apache/iceberg/pull/16406) | Docs: Add release notes for 1.10.2 | @amogh-jahagirdar | 00:18 | 添加 1.10.2 发布说明 |
| [#16410](https://github.com/apache/iceberg/pull/16410) | Docs: add more to 1.10.2 release notes | @kevinjqliu | 05:48 | 补充 1.10.2 发布说明中的 OpenAPI Jar 变更 |

---

## 🐛 新增 Issue 分析（2026-05-19）

### Issue #16430 — Spark: 实现 SupportsReportOrdering

| 字段 | 内容 |
|------|------|
| **编号** | [#16430](https://github.com/apache/iceberg/issues/16430) |
| **类型** | Feature Request / Improvement |
| **标签** | `improvement` |
| **作者** | @Shekharrajak |

#### 问题描述

`SparkBatchQueryScan` 在 manifest 中存储了每个文件的 `sort_order_id`，
但从未实现 `SupportsReportOrdering` 接口，导致 `BatchScanExec.outputOrdering` 始终返回 `Nil`。

```
当前行为：
Sort[event_time] → BatchScanExec (outputOrdering=Nil)
                   ↑ 不必要的额外排序，浪费资源

期望行为：
BatchScanExec (outputOrdering=[event_time ASC]) → 排序消除，直接利用文件有序性
```

**收益**：消除 Sort-Merge Join、有序聚合、MOR Compaction 读取中的冗余预排序步骤。

---

### Issue #16426 — Flink: Source Fetcher 线程泄漏（严重 Bug）

| 字段 | 内容 |
|------|------|
| **编号** | [#16426](https://github.com/apache/iceberg/issues/16426) |
| **类型** | Bug |
| **标签** | `bug` |
| **严重性** | 🔴 高（可导致 TaskManager OOM） |
| **作者** | @shanzi |
| **影响版本** | 1.10.2 |

#### 问题根因

```
任务取消流程：
Flink 调用 SplitFetcherManager.close()
    └─ SplitFetcher.shutdown()
         └─ 等待线程退出（最多 30s）
              └─ 但 IcebergSourceSplitReader.wakeUp() 是空实现！

线程卡死位置：
"Source Data Fetcher ..." WAITING (parking)
  at ArrayBlockingQueue.take           ← 无限阻塞等待 Pool entry
  at Pool.pollEntry(Pool.java:82)
  at ArrayPoolDataIteratorBatcher$ArrayPoolBatchIterator.getCachedEntry()
  at IcebergSourceSplitReader.fetch()
  at SplitFetcher.run()
```

#### 后果

多次 Failover 后，泄漏的线程持有 Parquet 缓冲区、S3 输入流、Iceberg 读取器状态等大量内存，
最终导致 TaskManager GC 压力过大并死亡。

#### 建议修复方案

```java
// 当前（有问题）
@Override
public void wakeUp() {}  // 空实现，无法唤醒阻塞线程

// 建议修复
@Override
public void wakeUp() {
    this.wakeUpFlag.set(true);
    // 唤醒等待 pool entry 的线程
}

// Pool 等待改为有界轮询
while (!wakeUp) {
    T[] entry = pool.pollEntry(Duration.ofSeconds(10));
    if (entry != null) return entry;
}
throw new RuntimeException("Woken up while waiting for pool entry");
```

参考：Apache Paimon 在 [paimon#4098](https://github.com/apache/paimon/pull/4098) 修复了相同问题。

---

### Issue #16422 — Flink: 表注释丢失 Bug

| 字段 | 内容 |
|------|------|
| **编号** | [#16422](https://github.com/apache/iceberg/issues/16422) |
| **类型** | Bug |
| **标签** | `bug` |
| **作者** | @SteveStevenpoor |
| **影响版本** | 1.11.0 |
| **修复 PR** | [#16423](https://github.com/apache/iceberg/pull/16423)（已提交） |

通过 Flink 引擎设置的表注释在 `FlinkCatalog` 中被完全忽略。
Spark 引擎行为正常，属于 Flink 集成的实现遗漏。
**修复方法**：将 table comment 写入 properties map。

---

### Issue #16420 — Flink: 添加列到指定位置不生效

| 字段 | 内容 |
|------|------|
| **编号** | [#16420](https://github.com/apache/iceberg/issues/16420) |
| **类型** | Bug |
| **标签** | `bug` |
| **作者** | @SteveStevenpoor |
| **影响版本** | 1.11.0 |
| **修复 PR** | [#16419](https://github.com/apache/iceberg/pull/16419)（已提交） |

通过 Flink 执行 `ALTER TABLE ADD COLUMN ... FIRST/AFTER` 时，列总是被添加到末尾。
Spark 引擎行为正常。**修复方法**：在 `FlinkAlterTableUtil` 中添加 `applyModifyColumnPosition` 调用。

---

### Issue #16418 — Spark: rewrite_table_path 二次执行报 FileAlreadyExistsException

| 字段 | 内容 |
|------|------|
| **编号** | [#16418](https://github.com/apache/iceberg/issues/16418) |
| **类型** | Bug |
| **标签** | `bug` |
| **作者** | @MarigWeizhi |
| **影响版本** | 1.10.1 |
| **修复 PR** | [#16421](https://github.com/apache/iceberg/pull/16421)（已提交） |

#### 问题分析

`rewrite_table_path` Procedure 设计上应支持幂等执行（重复运行），但 Position Delete 文件
的写入使用 `CREATE` 语义而非 `createOrOverwrite`，导致第二次执行时报错：

```
org.apache.hadoop.fs.FileAlreadyExistsException: .../00059-1-...-deletes.parquet already exists
```

| 文件类型 | 写入方式 | 是否支持重试 |
|---------|---------|------------|
| vN-*.metadata.json | `TableMetadataParser.overwrite()` | ✅ 覆盖 |
| snap-*.avro | ManifestLists.write（Avro 默认）| ✅ 覆盖 |
| *.avro | ManifestFiles.write（Avro 默认）| ✅ 覆盖 |
| file-list | `outputFile.createOrOverwrite()` | ✅ 覆盖 |
| **position delete *.parquet** | `Parquet.writeDeletes()` **CREATE** | ❌ 失败 |

---

### Issue #16414 — Spec: Avro 中 `day` 分区变换字段的类型歧义

| 字段 | 内容 |
|------|------|
| **编号** | [#16414](https://github.com/apache/iceberg/issues/16414) |
| **类型** | 规范歧义 |
| **标签** | 无 |
| **作者** | @kevinjqliu |

#### 跨实现兼容性矩阵

| 实现 | 写入格式 | 读 plain `int`? | 读 logical `date`? |
|------|---------|:---------------:|:-----------------:|
| Java (`apache/iceberg`) | logical `date` | ✅ | ✅ |
| PyIceberg | logical `date` | ✅ | ✅ |
| Rust (`apache/iceberg-rust`) | logical `date` | ✅（PR #496 修复后）| ✅ |
| Go (`apache/iceberg-go`) | plain `int` | ✅ | ⚠️（PR #915 开放中）|

**Spec 矛盾点**：分区变换表说 `day` 结果类型是 `int`，但 Avro 类型映射表中 `date` 类型才对应 logical `date`。
**建议**：保留 `int` 文档类型，但在 Avro manifest 编码中明确 SHOULD 使用 logical `date`，readers MUST 同时接受两种格式。

---

## 🔧 新增 PR 分析（2026-05-19，仅 Open 状态）

### PR #16434 — Kafka Connect: 为瞬时提交异常添加有界重试

| 字段 | 内容 |
|------|------|
| **编号** | [#16434](https://github.com/apache/iceberg/pull/16434) |
| **状态** | Open |
| **作者** | @yadavay-amzn |
| **标签** | `KAFKACONNECT` |
| **修复** | #16393 |

**变更**: 添加可配置的连续失败阈值（`iceberg.connect.commit.max-consecutive-failures`，默认 3）。
Coordinator 仅在 N 次连续完整提交失败后才终止，避免因瞬时错误（网络抖动等）触发不必要的运维干预。
成功提交后计数器归零，保留安全性保障的同时提升容错能力。

---

### PR #16433 — Kafka Connect: 为部分提交失败添加监控指标

| 字段 | 内容 |
|------|------|
| **编号** | [#16433](https://github.com/apache/iceberg/pull/16433) |
| **状态** | Open |
| **作者** | @yadavay-amzn |
| **标签** | `KAFKACONNECT` |
| **修复** | #16392 |

在 Coordinator 中添加部分提交失败计数器。当 `commit(partialCommit=true)` 捕获到 RuntimeException 时计数递增，
帮助运维人员了解集群繁忙时的部分提交失败频率。

---

### PR #16432 — Kafka Connect: 添加提交失败传播端到端测试

| 字段 | 内容 |
|------|------|
| **编号** | [#16432](https://github.com/apache/iceberg/pull/16432) |
| **状态** | Open |
| **作者** | @yadavay-amzn |
| **标签** | `KAFKACONNECT` |
| **修复** | #16380 |

```
测试验证的完整生产路径：
Coordinator.process() 抛出 RuntimeException
    └─ CoordinatorThread.run() 捕获，设置 terminated=true
         └─ CommitterImpl.save() 调用 processControlEvents()
              └─ 检测到 termination，抛出 NotRunningException
                   └─ 任务转为 FAILED 状态
```

---

### PR #16431 — Docs: 添加 1.11.0 发布说明

| 字段 | 内容 |
|------|------|
| **编号** | [#16431](https://github.com/apache/iceberg/pull/16431) |
| **状态** | Open |
| **作者** | @aihuaxu |
| **标签** | `docs` |

为 1.11.0 版本添加完整发布说明文档（配合已合并的 #16427/#16428）。

---

### PR #16429 — Flink: 将 #16065 Backport 到 v2.0 和 v1.20

| 字段 | 内容 |
|------|------|
| **编号** | [#16429](https://github.com/apache/iceberg/pull/16429) |
| **状态** | Open |
| **作者** | @sqd |
| **标签** | `flink` |

将已合并的 Slot Sharing Group 功能（#16065）向下移植到 `flink/v2.0` 和 `flink/v1.20` 分支。

---

### PR #16425 — Spec: 添加 V4 列更新规范

| 字段 | 内容 |
|------|------|
| **编号** | [#16425](https://github.com/apache/iceberg/pull/16425) |
| **状态** | Open |
| **作者** | @anuragmantri |
| **标签** | `Specification` |

Iceberg 格式 V4 列更新功能的规范文档，与以下 PR 联动：
- [#16025](https://github.com/apache/iceberg/pull/16025)
- [#14234](https://github.com/apache/iceberg/pull/14234)
- [#15630](https://github.com/apache/iceberg/pull/15630)

这是 Iceberg Spec V4 系列重要改进之一。

---

### PR #16424 — Spark: 修复 SPJ 中 Bucket 分区键字符串列类型不匹配

| 字段 | 内容 |
|------|------|
| **编号** | [#16424](https://github.com/apache/iceberg/pull/16424) |
| **状态** | Open |
| **作者** | @ammarchalifah |
| **标签** | `spark` |

#### 问题

```
表按 bucket(N, string_column) 分区时：
- bucket transform 产生 Integer 类型分区值
- Spark SPJ 通过 StructInternalRow 读分区值时
  调用 struct.get(ordinal, CharSequence.class)   ← 假设是 CharSequence
  实际是 Integer → ClassCastException！
```

修复类型转换逻辑，使 SPJ（Storage Partitioned Join）在字符串列的 Bucket 分区场景下正确工作。

---

### PR #16417 — Core: 修复转义列名的 Metrics Mode 查找问题

| 字段 | 内容 |
|------|------|
| **编号** | [#16417](https://github.com/apache/iceberg/pull/16417) |
| **状态** | Open |
| **作者** | @wombatu-kun |
| **标签** | `parquet`, `core` |
| **修复** | #11950 |

#### 问题

`MetricsConfig` 使用**原始 Iceberg 列名**作为 key 存储 per-column metrics mode；
而 `MetricsUtil.metricsMode()` 通过 Parquet footer 的 schema 解析字段名时，
对于含有特殊字符（需转义）的列名，解析结果与存储 key 不一致，导致 metrics mode 查找失败。

影响场景：`add_files` / `migrate` / `snapshot` 通过 `ParquetUtil.fileMetrics` 路径（`TableMigrationUtil`）处理含转义列名的表。

---

### PR #16411 — Flink: 添加委托给 Flink FileSystem 的 FlinkFileSystemFileIO

| 字段 | 内容 |
|------|------|
| **编号** | [#16411](https://github.com/apache/iceberg/pull/16411) |
| **状态** | Open |
| **作者** | @wombatu-kun |
| **标签** | `flink` |
| **修复** | #15352 |

#### 解决痛点

```
当前（繁琐的双重配置）：
用户已在 Flink 中配置 S3/HDFS/GCS（用于 Checkpoint/Savepoint）
还需要 单独为 Iceberg FileIO 再次配置相同的存储连接！

新增 FlinkFileSystemFileIO（统一配置）：
┌─────────────────────────────────────────────┐
│         Flink Job                           │
│  Checkpoint/Savepoint → Flink FileSystem    │
│  Iceberg FileIO       → FlinkFileSystemFileIO│
│                           └─ 委托给 ↗       │
│                    同一套认证和插件配置      │
└─────────────────────────────────────────────┘
```

FlinkFileSystemFileIO 发现并委托给 Flink 已注册的 FileSystem 实现（包括 S3、HDFS、GCS 等），
统一利用 Flink 的 delegation-token 和 plugin-based 认证机制，无需重复配置。

---

### PR #16408 — Core: 重构 V4 Struct Builders 以改善验证逻辑

| 字段 | 内容 |
|------|------|
| **编号** | [#16408](https://github.com/apache/iceberg/pull/16408) |
| **状态** | Open |
| **作者** | @anoopj |
| **标签** | `core` |

#16092 代码审查后的跟进 PR，重构 Iceberg Spec V4 的 struct builder 实现，
改善构建器的验证逻辑，提升代码质量与正确性。

---

### PR #16407 — Build: 禁止 Preconditions.checkState 使用 %d 占位符

| 字段 | 内容 |
|------|------|
| **编号** | [#16407](https://github.com/apache/iceberg/pull/16407) |
| **状态** | Open |
| **作者** | @ebyhr |
| **标签** | `API`, `core`, `flink`, `INFRA` |

#### 问题

Guava 的 `Preconditions.checkState` 只支持 `%s` 占位符，使用 `%d` 不会报错但格式化无效：

```java
// 错误用法（%d 不被 Guava 格式化）
Preconditions.checkState(condition, "Expected value: %d", count);
// 输出："Expected value: %d" ← 占位符未被替换！

// 正确用法
Preconditions.checkState(condition, "Expected value: %s", count);
```

此 PR 修复所有不正确的 `%d` 用法，并添加 Checkstyle 规则防止未来引入同类问题
（类似已有的 `checkArgument` 规则）。

---

## 📈 总结与趋势分析

### 活动分布

```
2026-05-19 PR/Issue 类别分布：
┌──────────────────────────────────────────────┐
│  Flink   ████████████  5 个（功能+Bug修复）   │
│  Docs    ██████████    8 个（1.11.0 发布）    │
│  Spark   ██████        4 个                  │
│  Build   ████          3 个                  │
│  Core    ████          3 个                  │
│  Kafka   ████          3 个                  │
│  Spec    ██            1 个                  │
└──────────────────────────────────────────────┘
```

### 关键趋势

1. **🎉 1.11.0 发布后繁忙期**：大量 PR 围绕版本发布后的文档整理、基线更新、DOAP 文件更新。

2. **🔧 Flink 集成持续完善**：
   - 新增 Slot Sharing Group 支持（细粒度资源管理）
   - 修复表注释和列位置 Bug
   - Source Fetcher 线程泄漏问题值得关注（严重 Bug）
   - `FlinkFileSystemFileIO` 大幅简化存储配置

3. **☁️ Kafka Connect 可靠性改进**：三个 Kafka Connect PR 均来自同一作者，系统性地改善了 Commit 失败场景下的容错性和可观测性。

4. **📐 Spec V4 进行中**：多个 V4 相关 PR 同时推进（列更新、struct builder），Iceberg 下一代格式正在成型。

5. **🔍 Spark SPJ 修复**：Bucket 分区字符串列类型转换 Bug 已有对应修复 PR，影响使用 Storage Partitioned Join 的场景。

---

## 🔗 参考链接

| 资源 | 链接 |
|------|------|
| Apache Iceberg 仓库 | https://github.com/apache/iceberg |
| Iceberg 1.11.0 发布 | https://github.com/apache/iceberg/releases/tag/apache-iceberg-1.11.0 |
| Fork 仓库 | https://github.com/vinlee19/iceberg |
| Flink Fine-Grained Resource Management | https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/finegrained_resource/ |

---

*报告由 Claude Code 自动生成 · 数据截止 2026-05-19 23:59 UTC*
