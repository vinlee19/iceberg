# Apache Iceberg 每日动态报告

> **报告日期：** 2026-06-03（分析时间范围：2026-06-03 00:00 – 23:59 UTC）
> **生成时间：** 2026-06-04
> **数据来源：** https://github.com/apache/iceberg

---

## 📋 执行摘要

| 统计项 | 数量 |
|--------|------|
| ✅ Fork 同步状态 | **已同步至最新** `4b4c5b5` |
| 🔀 当日合并 PR | **3** |
| 🆕 当日新增 Issue | **2** |
| 📬 当日新增 PR | **9** |

---

## 🔄 Fork 同步状态

本次同步将 `vinlee19/iceberg:main` 更新至 `apache/iceberg:main` 最新提交：

```
最新提交: 4b4c5b554c829d946d7152dbf70dcd987ff6a9ec
提交信息: Infra: Update collaborators list (#16678)
同步时间: 2026-06-04
```

**同步结果：**
```
To http://.../vinlee19/iceberg
   c958fcf..4b4c5b5  upstream/main -> main
```

---

## 🔀 当日合并 PR 详细分析

> 共 3 个 PR 在 2026-06-03 合并到 `apache/iceberg:main`

---

### PR #16408 · Core: Refactor v4 struct builders to improve validation

| 属性 | 信息 |
|------|------|
| **链接** | https://github.com/apache/iceberg/pull/16408 |
| **作者** | [@anoopj](https://github.com/anoopj)（Anoop Johnson）|
| **标签** | `core` |
| **合并时间** | 2026-06-03 18:29:42 UTC |
| **Review 评论** | 3 条 |
| **变更规模** | +934 / -261 行，8 个文件 |

#### 背景

此 PR 是 #16092 的后续跟进，针对 Iceberg Format v4 中的 Struct Builder 进行重构，以改进验证逻辑、实现更早的错误提示并更清晰地指导状态转换。

#### 核心变更

**新增文件：`TrackingBuilder.java`**

将原本混在 `TrackingStruct` 内的 Builder 模式抽取为独立的 `TrackingBuilder` 类，提供类型安全的静态工厂方法：

```java
// 新增文件的关键API设计
class TrackingBuilder {
  // 创建新增文件的 Builder
  static TrackingBuilder added(long newSnapshotId)

  // 从已有 source 创建派生 Builder
  static TrackingBuilder from(Tracking source, long newSnapshotId)

  // 直接返回 DELETED 状态的 Tracking
  static Tracking deleted(Tracking source, long newSnapshotId)

  // 直接返回 REPLACED 状态的 Tracking
  static Tracking replaced(Tracking source, long newSnapshotId)
}
```

**重构 `TrackingStruct.java`（-97 行）**

移除原有的 `Builder` 内部类，由 `TrackingBuilder` 接管。精简内部实现，改进 `toString()` 输出（对 `null` 值直接输出而非字符串 `"null"`）：

```diff
- static Builder builder() {
-     return new Builder();
- }
- .add("snapshot_id", snapshotId == null ? "null" : snapshotId)
+ .add("snapshot_id", snapshotId)
```

**重构 `ManifestInfoStruct.java`（验证强化）**

Builder 字段由 `int`/`long` 原始类型改为 `Integer`/`Long` 包装类，支持 `null` 表示"未设置"；同时为每个 setter 添加前置条件检查：

```diff
- private int addedFilesCount = -1;
- private long addedRowsCount = -1L;
+ private Integer addedFilesCount = null;
+ private Long addedRowsCount = null;

+ Preconditions.checkArgument(
+     count >= 0, "Invalid added files count: %s (must be >= 0)", count);
+ Preconditions.checkArgument(buffer != null, "Invalid DV: null");
```

**`DeletionVectorStruct.java` 同步更新**

与 `ManifestInfoStruct` 同步，对 null 值校验逻辑统一。

#### 测试变更

| 测试文件 | 变化 |
|---------|------|
| `TestTrackingStruct.java` | +464 / -~100 行，大量新增验证场景 |
| `TestManifestInfoStruct.java` | +257 / -少量，覆盖新增校验逻辑 |
| `TestDeletionVectorStruct.java` | +47 行 |
| `TestTrackedFileStruct.java` | +20 行 |

#### 影响分析

```
┌─────────────────────────────────────────────┐
│         Format v4 Builder 重构影响范围         │
├───────────────────┬─────────────────────────┤
│ TrackingBuilder   │ 新文件，接管 Builder 职责  │
│ TrackingStruct    │ 精简，移除内部 Builder     │
│ ManifestInfoStruct│ 验证强化，-1 → null        │
│ DeletionVectorStruct│ 同步 null 校验           │
└───────────────────┴─────────────────────────┘
```

> **总结：** 这是 v4 格式构建器的质量提升工作，通过将 Builder 模式与 Struct 分离、将哨兵值（`-1`）替换为 `null`、增加前置条件校验，使代码更安全、错误信息更清晰。

---

### PR #16676 · ORC: Remove ORC tests for partition statistics

| 属性 | 信息 |
|------|------|
| **链接** | https://github.com/apache/iceberg/pull/16676 |
| **作者** | [@gaborkaszab](https://github.com/gaborkaszab)（Gabor Kaszab）|
| **标签** | `ORC` |
| **合并时间** | 2026-06-03 20:48:25 UTC |
| **Review 评论** | 1 条 |
| **变更规模** | -146 行，删除 2 个测试文件 |

#### 背景

Partition statistics 的 Reader/Writer 当前不支持 ORC 文件格式，但代码库中仍存在两个 ORC 格式的分区统计测试套件。这些测试并不增加实质性的测试覆盖率——它们只是重写父类测试方法，并期望因"不支持格式"抛出 `UnsupportedOperationException`。

#### 核心变更

**删除文件：`TestOrcPartitionStatisticsScan.java`**（73 行）

该测试类继承自 `PartitionStatisticsScanTestBase`，但每个测试方法都断言会抛出异常：

```java
// 删除前的测试内容（所有方法均为此模式）
@Override
public void testScanPartitionStatsForCurrentSnapshot() throws Exception {
  assertThatThrownBy(super::testScanPartitionStatsForCurrentSnapshot)
      .isInstanceOf(UnsupportedOperationException.class)
      .hasMessage("Cannot write using unregistered internal data format: ORC");
}
```

**删除文件：`TestOrcPartitionStatsHandler.java`**（73 行）

同样模式，测试 `PartitionStatsHandler` 的 ORC 格式行为，每个方法期望异常。

#### 变更理由

```
问题：ORC 不支持 Partition Statistics → 测试只测"不支持"行为
结果：这些测试等价于 "功能预期不存在的测试"，无实际价值
方案：删除冗余测试，等 ORC 支持实现后再添加真正的测试
```

> **总结：** 纯粹的测试代码清理工作。移除已知无覆盖价值、仅断言功能不支持的 ORC 分区统计测试，使测试套件更精简，降低维护负担。

---

### PR #16678 · Infra: Update collaborators list

| 属性 | 信息 |
|------|------|
| **链接** | https://github.com/apache/iceberg/pull/16678 |
| **作者** | [@stevenzwu](https://github.com/stevenzwu)（Steven Zhen Wu）|
| **标签** | `INFRA` |
| **合并时间** | 2026-06-03 22:27:13 UTC |
| **Review 评论** | 0 条（快速合并）|
| **变更规模** | +2 / -2 行，1 个文件（`.asf.yaml`）|

#### 背景

Apache 项目在 GitHub 上的协作者人数限制为 10 人。此 PR 在保持人数上限的前提下，为两个即将上任的发版经理添加协作者权限，同时移除不活跃的成员。

#### 核心变更

修改 `.asf.yaml` 中 `github.collaborators` 列表：

```diff
 collaborators:  # Note: the number of collaborators is limited to 10
-  - chenjunjiedada    # 移除（不活跃）
-  - jun-he            # 移除（不活跃）
   - marton-bod
   - samarthjain
   - SreeramGarlapati
   - ajantha-bhat
   - jbonofre
   - manuzhang
+  - anuragmantri      # 新增（1.12.0 发版经理）
+  - nssalian          # 新增（1.13.0 发版经理）
```

**版本发布规划影响：**

| 发版经理 | 负责版本 |
|---------|---------|
| `anuragmantri` | Iceberg 1.12.0 |
| `nssalian` | Iceberg 1.13.0 |

> **总结：** 基础设施维护工作，为两名新发版经理开放 GitHub 协作者权限，以支持 Iceberg 1.12.0 和 1.13.0 的发布工作。

---

## 🆕 当日新增 Issue 分析

> 共 2 个 Issue 在 2026-06-03 创建

---

### Issue #16675 · [Spark] Capture and emit aggregated data-file metrics at commit time

| 属性 | 信息 |
|------|------|
| **链接** | https://github.com/apache/iceberg/issues/16675 |
| **作者** | [@gtrettenero](https://github.com/gtrettenero) |
| **标签** | `proposal` |
| **状态** | Open |
| **创建时间** | 2026-06-03 15:58:35 UTC |

#### 提案核心

这是一个关于 Iceberg 可观测性增强的重要提案，提议在 **Spark 写入路径** 中增加一个可选机制，在提交时从 Parquet footer 中提取聚合的数据文件指标，并通过 Iceberg 的事件框架发射出去，**而不持久化到表元数据中**。

#### 问题痛点

```
当前状态：
┌─────────────────────────────────────────────────────────┐
│  Iceberg 列级指标 (value_counts, bounds 等)               │
│  ┌──────────────────────────────────────┐               │
│  │ 仅限前 N 列 (默认 100 列)              │               │
│  │ write.metadata.metrics.max-inferred  │               │
│  │ -column-defaults = 100               │               │
│  └──────────────────────────────────────┘               │
│                                                         │
│  宽表 (>100 列) → 超限列无任何指标                          │
│  物理存储统计 (压缩大小、编码、bloom filter) → 完全缺失       │
│  提高 cap → manifest 膨胀 + 扫描规划变慢                    │
└─────────────────────────────────────────────────────────┘
```

#### 提案方案

```
┌──────────────────────────────────────────────────────────┐
│          Executor 侧（Spark Write 任务）                   │
│  DataWriter.commit() → 读 Parquet footer → 按 field-id   │
│  聚合统计 → 写入 AccumulatorV2                             │
└───────────────────────┬──────────────────────────────────┘
                        │ Driver 侧合并
                        ▼
┌──────────────────────────────────────────────────────────┐
│          提交前（commitOperation 之前）                    │
│  聚合结果（按 partition + column field-id）                 │
│  → 通过 thread-local bridge 传递                          │
│  → 发布到 org.apache.iceberg.events.Listeners            │
└──────────────────────────────────────────────────────────┘
```

**采集的指标（聚合至 partition + column 粒度）：**

| 指标类别 | 具体字段 |
|---------|---------|
| 文件级 | 文件数量、总大小、大小分布（p50/p75/p90/p95/p99）、小文件计数 |
| 列级（by field-id）| 压缩/未压缩大小、值数量、null 数量、编解码器、字典页/bloom filter 是否存在 |

**新增表属性：**
- `write.data-file-metrics.enabled`（默认 `false`）
- `write.data-file-metrics.small-file-threshold-bytes`（默认 128 KB）

#### 讨论要点

1. 事件框架 vs 专用 MetricsReporter 接口？
2. 聚合粒度是否需要可配置？
3. 是否扩展到 ORC/Avro 和 Flink？

---

### Issue #16670 · Spark: Support ALTER TABLE ... DROP PARTITION syntax

| 属性 | 信息 |
|------|------|
| **链接** | https://github.com/apache/iceberg/issues/16670 |
| **作者** | [@SteveStevenpoor](https://github.com/SteveStevenpoor) |
| **标签** | `improvement` |
| **状态** | Open |
| **创建时间** | 2026-06-03 03:23:47 UTC |

#### 问题描述

Iceberg 目前缺乏 SQL 级别的 `ALTER TABLE ... DROP PARTITION` 支持，用户不得不通过 `DELETE FROM ... WHERE` 配合源列谓词变通处理。

#### 痛点对比

| 场景 | 现有方式 | 提案方式 |
|------|---------|---------|
| 删除 2024-01 的数据 | `DELETE FROM events WHERE ts >= '2024-01-01' AND ts < '2024-02-01'` | `ALTER TABLE events DROP PARTITION (ts_month = '2024-01')` |
| Bucket 分区 | 无法用 SQL 清晰表达 | `ALTER TABLE events DROP PARTITION (id_bucket = 0)` |
| 迁移兼容性 | 需要重写 Hive/Spark 管道 | 与标准 DDL 兼容 |

#### 实现草图

- `SparkTable` 实现 `SupportsAtomicPartitionManagement`
- `dropPartition(InternalRow)` 构建 transform-aware 谓词并通过 `DeleteFiles.deleteFromRowFilter(...)` 提交
- 新增 `Transforms.parseHumanYear/Month/Day/Hour(String)` 静态方法

> 作者同时提交了对应的 PR #16671 来实现此功能。

---

## 📬 当日新增 PR 汇总

> 共 9 个 PR 在 2026-06-03 创建（含已合并的 3 个）

### 状态总览

| PR | 标题 | 状态 | 标签 |
|----|------|------|------|
| [#16679](https://github.com/apache/iceberg/pull/16679) | Spark: Support Initial Snapshot Load for Streaming Reads | 🟡 Draft | spark, core |
| [#16678](https://github.com/apache/iceberg/pull/16678) | Infra: Update collaborators list | ✅ Merged | INFRA |
| [#16677](https://github.com/apache/iceberg/pull/16677) | API: Decouple default test version range from MAX_FORMAT_VERSION | 🟢 Open | API |
| [#16676](https://github.com/apache/iceberg/pull/16676) | ORC: Remove ORC tests for partition statistics | ✅ Merged | ORC |
| [#16674](https://github.com/apache/iceberg/pull/16674) | CI: Scope CVE scan PR triggers | 🔴 Closed | INFRA |
| [#16673](https://github.com/apache/iceberg/pull/16673) | Core: Validate required unknown schema fields | 🟢 Open | core |
| [#16672](https://github.com/apache/iceberg/pull/16672) | Core: Wire REST catalog table encryption | 🔴 Closed | core |
| [#16671](https://github.com/apache/iceberg/pull/16671) | Spark: Support ALTER TABLE ... DROP PARTITION syntax | 🟢 Open | API, spark |
| [#16669](https://github.com/apache/iceberg/pull/16669) | Core: skip deleted position delete files in rewrite table path | 🟢 Open | core |

---

### PR #16679 · Spark: Support Initial Snapshot Load for Streaming Reads *(Draft)*

| 属性 | 信息 |
|------|------|
| **链接** | https://github.com/apache/iceberg/pull/16679 |
| **作者** | [@alexprosak](https://github.com/alexprosak) |
| **关联 Issue** | #13188 |

**功能说明：** 为 Iceberg Spark 流式读取添加初始快照加载支持，解决新建 checkpoint 的流任务需要重放完整历史的问题。

**新增 `stream-from-snapshot` 选项：**

| 选项值 | 行为 |
|-------|------|
| *(未设置)* | **新默认值**：先全量读取当前快照，再增量追踪新快照 |
| `latest` | 从流启动时的最新快照之后开始 |
| `earliest` | 从最老的祖先快照开始（原默认行为）|
| `<snapshot-id>` | 从指定快照 ID 之后开始（独占）|

**实现机制：** 复用 `StreamingOffset` 中已存在但从未启用的 `scanAllFiles` 标志，新默认返回 `StreamingOffset(currentSnapshotId, 0, scanAllFiles=true)`。

**已知限制：** 当前快照包含行级删除文件（V2/V3 delete files）时，初始快照加载会抛异常，以避免静默输出被删除的行。

---

### PR #16677 · API: Decouple default test version range from MAX_FORMAT_VERSION

| 属性 | 信息 |
|------|------|
| **链接** | https://github.com/apache/iceberg/pull/16677 |
| **作者** | [@stevenzwu](https://github.com/stevenzwu) |

**功能说明：** 在 v4 格式仍处于孵化阶段时，将参数化测试的默认版本范围与 `MAX_FORMAT_VERSION` 解耦，避免 v4 不完整的读写路径导致虚假测试失败。

**关键变更：**
- 新增 `MAX_PRODUCTION_VERSION = 3`
- `ALL_VERSIONS`、`V2_AND_ABOVE`、`V3_AND_ABOVE` 基于 `MAX_PRODUCTION_VERSION`
- `MAX_FORMAT_VERSION` 保持为 4，升级测试继续覆盖 v4
- `TestRowLineageMetadata` 从 22 个测试用例减至 11 个（v4 不再参数化）

---

### PR #16673 · Core: Validate required unknown schema fields

| 属性 | 信息 |
|------|------|
| **链接** | https://github.com/apache/iceberg/pull/16673 |
| **作者** | [@manuzhang](https://github.com/manuzhang) |

**修复说明：** 修复 `SchemaParser` 中 `required unknown` 字段的验证顺序问题。

**根因：** `SchemaParser` 先解析容器子类型（list element、map key/value），再检查是否为 `required unknown`。当验证失败时，错误信息仅显示通用名称（`element`、`value`），而非实际的 Schema 路径。

**修复方案：**
- 在 `SchemaParser` 中解析嵌套容器类型时携带字段路径
- 在构建 `NestedField` 之前就拒绝 `required unknown` 字段
- 覆盖 struct、list element、map key/value 的回归测试

---

### PR #16671 · Spark: Support ALTER TABLE ... DROP PARTITION syntax

| 属性 | 信息 |
|------|------|
| **链接** | https://github.com/apache/iceberg/pull/16671 |
| **作者** | [@SteveStevenpoor](https://github.com/SteveStevenpoor) |
| **关联 Issue** | #16670 |

实现 Issue #16670 中提案的功能，为 Spark 引擎添加 `ALTER TABLE ... DROP PARTITION` 语法支持（详见上方 Issue 分析）。

---

### PR #16669 · Core: skip deleted position delete files in rewrite table path

| 属性 | 信息 |
|------|------|
| **链接** | https://github.com/apache/iceberg/pull/16669 |
| **作者** | [@venkateshwaracholan](https://github.com/venkateshwaracholan) |
| **修复 Issue** | #16662 |

**Bug 修复说明：** `rewrite_table_path` 操作可能将已被删除的 position delete 文件纳入物理重写队列。若这些文件在 `rewrite_position_delete_files` + `expire_snapshots` 后已被删除，重写时会抛出 `FileNotFoundException`。

**修复方案：** 仅对 manifest entry 状态为 `LIVE` 且属于目标快照集合的 position delete 文件添加到 `toRewrite()` 列表，已删除的 entry 仍保留在重写后的 manifest 元数据中，但不再安排物理重写。

---

## 🔴 当日关闭（未合并）PR

### PR #16674 · CI: Scope CVE scan PR triggers

| 属性 | 信息 |
|------|------|
| **作者** | [@manuzhang](https://github.com/manuzhang) |
| **创建** | 2026-06-03 14:06 → **关闭** 15:23 UTC（~1.5 小时）|

为 CVE Scan 工作流添加 PR 路径过滤，只在构建配置/依赖/Gradle 文件变更时触发，避免对纯源码 PR 运行完整的 CVE 扫描矩阵。（已关闭，未合并到 main）

### PR #16672 · Core: Wire REST catalog table encryption

| 属性 | 信息 |
|------|------|
| **作者** | [@hkwi](https://github.com/hkwi) |
| **创建** | 2026-06-03 10:39 → **关闭** 11:30 UTC（~1 小时）|

将表加密功能接入 REST Catalog 的表操作中，包括从 `encryption.kms-type`/`encryption.kms-impl` 创建 `KeyManagementClient`，并在提交请求中附加 `AddEncryptionKey` 元数据更新。（已关闭，未合并到 main）

---

## 📊 每日活动趋势分析

```
2026-06-03  Apache Iceberg 活动时间线（UTC）
─────────────────────────────────────────────────────
01:22  PR #16669 创建  [Core: rewrite table path bug fix]
03:23  Issue #16670 创建  [Spark: DROP PARTITION]
04:46  PR #16671 创建  [Spark: DROP PARTITION impl]
10:39  PR #16672 创建  [Core: REST encryption]
11:29  PR #16408 ✅ 合并  [Core: v4 struct builders]
11:30  PR #16672 🔴 关闭
13:44  PR #16673 创建  [Core: schema validation]
14:06  PR #16674 创建  [CI: CVE scan scope]
15:23  PR #16674 🔴 关闭
15:58  Issue #16675 创建  [Spark: Parquet footer metrics]
16:27  PR #16676 创建  [ORC: remove partition stats tests]
20:48  PR #16676 ✅ 合并  [ORC: remove partition stats tests]
21:12  PR #16677 创建  [API: test version range]
22:23  PR #16678 创建  [Infra: collaborators]
22:27  PR #16678 ✅ 合并  [Infra: collaborators]
23:29  PR #16679 创建  [Spark: streaming initial snapshot]
─────────────────────────────────────────────────────
```

## 🏷️ 标签分布

| 标签 | 数量 | PR/Issue |
|------|------|---------|
| `core` | 4 | #16408(merged), #16669, #16672, #16673 |
| `spark` | 3 | #16671, #16679, #16670(issue) |
| `INFRA` | 2 | #16678(merged), #16674 |
| `ORC` | 1 | #16676(merged) |
| `API` | 2 | #16671, #16677 |
| `proposal` | 1 | #16675(issue) |

## 🔑 关键主题

1. **Format v4 持续完善** — `#16408`（struct builders 重构）、`#16677`（测试版本范围解耦）
2. **Spark 功能增强** — `#16671`（DROP PARTITION）、`#16679`（流式初始快照加载）
3. **可观测性提案** — `#16675`（Parquet footer 指标在提交时发射）
4. **Bug 修复** — `#16669`（rewrite 路径跳过已删除的 delete files）、`#16673`（schema 字段验证）
5. **基础设施维护** — `#16678`（协作者名单更新，为 1.12.0/1.13.0 发版做准备）

---

*报告自动生成 | Apache Iceberg Daily Report | 2026-06-04*
