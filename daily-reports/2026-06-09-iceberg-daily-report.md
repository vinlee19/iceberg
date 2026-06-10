# Apache Iceberg 每日动态报告

> **日期**: 2026-06-09（UTC）
> **数据来源**: [apache/iceberg](https://github.com/apache/iceberg)
> **生成时间**: 2026-06-10 自动生成

---

## 目录

- [今日概览](#今日概览)
- [已合并 PR（7 个）](#已合并-pr7-个)
  - [PR #16518 — 规范：Mumbling Bitmap 草案规格](#pr-16518--规范mumbling-bitmap-草案规格)
  - [PR #16710 — API/Spark：snapshot 过程新增 ignore_missing_files](#pr-16710--apisparksnapshot-过程新增-ignore_missing_files)
  - [PR #16722 — Parquet：消除 Decimal 读取中的中间 BigInteger](#pr-16722--parquet消除-decimal-读取中的中间-biginteger)
  - [PR #16545 — Flink：实现 wakeup 方法修复线程/内存泄漏](#pr-16545--flink实现-wakeup-方法修复线程内存泄漏)
  - [PR #16441 — Core：调整保留字段 ID 的计算逻辑](#pr-16441--core调整保留字段-id-的计算逻辑)
  - [PR #15911 — Core：在 DV 合并中保留加密元数据](#pr-15911--core在-dv-合并中保留加密元数据)
  - [PR #16699 — Spark：修复 manifest rewrite 的 first row ID 丢失问题](#pr-16699--spark修复-manifest-rewrite-的-first-row-id-丢失问题)
- [新增 Issue（3 个）](#新增-issue3-个)
- [新增 PR（16 个）](#新增-pr16-个)
- [趋势与亮点](#趋势与亮点)

---

## 今日概览

| 指标 | 数量 |
|------|------|
| 合并 PR | **7** |
| 新增 Issue | **3** |
| 新增 PR | **16** |

```
合并 PR 分类分布
┌─────────────────────────────────────────────────────┐
│ Specification  ██░░░░░░░░░░░░░░░░░░░░  1 (14%)      │
│ Parquet        ██░░░░░░░░░░░░░░░░░░░░  1 (14%)      │
│ Flink          ██░░░░░░░░░░░░░░░░░░░░  1 (14%)      │
│ Core           ████░░░░░░░░░░░░░░░░░░  2 (29%)      │
│ Spark          ████░░░░░░░░░░░░░░░░░░  2 (29%)      │
└─────────────────────────────────────────────────────┘
```

---

## 已合并 PR（7 个）

### PR #16518 — 规范：Mumbling Bitmap 草案规格

| 字段 | 内容 |
|------|------|
| **PR 链接** | [#16518](https://github.com/apache/iceberg/pull/16518) |
| **作者** | rdblue |
| **合并时间** | 2026-06-09 21:43 UTC |
| **标签** | `Specification` |
| **修改文件** | `format/mumbling-spec.md` |

#### 背景与问题

Iceberg v4 规范中引入了**嵌入式删除向量（Embedded Deletion Vectors）**，存储在 v4 根 manifest 中。为高效压缩这些位图数据，社区提出了 Mumbling Bitmap 格式。此 PR 将该格式的草案规范加入代码库，但**不代表它已被采纳为正式格式**——仅为草案，待社区达成共识、参考实现稳定后才会正式启用。

#### 技术要点

Mumbling Bitmap 是一种 **混合稀疏/稠密位图压缩格式**，核心思路：

```
整体结构
├── 描述符数组（PFOR 编码）
│   └── 记录每个 256-bit 逻辑区域的容器长度
└── 容器段
    ├── 稀疏容器（≤31 个值）→ 直接存储值列表
    └── 稠密容器（≥32 个值）→ 32 字节紧凑位图
```

**PFOR（Patched Frame-of-Reference）编码策略**：
- 使用基准位宽 `b1` 对描述符数组进行位压缩
- 超出 `b1` 范围的异常值单独以额外 bits 存储
- 避免为每个容器单独分配一字节描述符，节省空间

**设计亮点**：
- 两级位打包（base bits + exception bits）适应数据密度的动态变化
- 256-bit 逻辑区域划分使稀疏与稠密切换的阈值（31 个值）在实现中清晰可辨
- 已通过 Apache 社区投票

---

### PR #16710 — API/Spark：snapshot 过程新增 `ignore_missing_files`

| 字段 | 内容 |
|------|------|
| **PR 链接** | [#16710](https://github.com/apache/iceberg/pull/16710) |
| **作者** | drexler-sky |
| **合并时间** | 2026-06-09 16:29 UTC |
| **标签** | `API`, `spark`, `docs` |
| **Spark 版本** | 4.1 |
| **评审人** | ebyhr, huaxingao |

#### 背景与问题

`snapshot` 存储过程用于将 Hive/外部表迁移为 Iceberg 表。在高并发环境中，若源数据文件在 snapshot 过程中被并发清理任务删除，整个操作会直接失败，导致迁移中断。

此前 `migrate` 过程（PR #16643）已实现了 `ignore_missing_files` 参数，但 `snapshot` 过程缺乏一致的处理能力。

#### 修复内容

```sql
-- 旧行为：文件消失 → 报错失败
CALL catalog.system.snapshot('source_db.source_table', 'iceberg_db.iceberg_table');

-- 新行为：文件消失 → 跳过并打印 WARNING
CALL catalog.system.snapshot(
  source_table => 'source_db.source_table',
  table => 'iceberg_db.iceberg_table',
  ignore_missing_files => true   -- 新增参数
);
```

**行为变更对比**：

| 场景 | 旧行为 | 新行为（启用参数） |
|------|--------|--------------------|
| 源文件正常 | 成功 | 成功 |
| 源文件被并发删除 | 抛出异常，迁移失败 | 跳过丢失文件，打印警告，迁移继续 |
| 默认行为 | — | 不变（向后兼容） |

---

### PR #16722 — Parquet：消除 Decimal 读取中的中间 BigInteger

| 字段 | 内容 |
|------|------|
| **PR 链接** | [#16722](https://github.com/apache/iceberg/pull/16722) |
| **作者** | wombatu-kun |
| **合并时间** | 2026-06-09 16:29 UTC |
| **标签** | `parquet` |
| **评审人** | nastra |

#### 背景与问题

`IntegerAsDecimalReader` 和 `LongAsDecimalReader` 使用以下写法构造 Decimal 值：

```java
// 旧写法 —— 产生中间 BigInteger 对象（堆分配）
new BigDecimal(BigInteger.valueOf(unscaled), scale)
// BigDecimal 内部的 intVal 字段持有该 BigInteger 引用 → 无法 GC
```

该 `BigInteger` 作为每值堆分配逃逸出去，在 INT32/INT64 backed Decimal 列的读取路径上造成大量 GC 压力。

#### 修复方案

```java
// 新写法 —— 直接从 long 构造，无 BigInteger
BigDecimal.valueOf(long unscaled, int scale)
// 与 ParquetConversions.convertValue / converterFromParquet 已有的模式一致
```

#### 性能基准（JMH + GC Profiler）

**单值构造基准（1024 值/次）**：

| 读取器 | 内存/值（优化前） | 内存/值（优化后） | 耗时（优化前） | 耗时（优化后） |
|--------|------------------|------------------|----------------|----------------|
| INT32 Decimal | 112 B | **48 B** (-57%) | 10.9 µs | **4.6 µs** (-58%) |
| INT64 Decimal | 112 B | **48 B** (-57%) | 11.5 µs | **5.1 µs** (-56%) |

**端到端读取基准（100 万行，INT32 + INT64 各一列）**：

```
内存分配: 361 MB → 233 MB  (-35%，精确 = 64 B × 2M 值)
GC 次数:  10 次  →   5 次  (-50%)
挂钟时间: 201 ms → 239 ms  (在置信区间内，无显著差异)
```

> 挂钟时间无明显变化是因为单值解码只占总读取时间的小部分；内存与 GC 的收益是实实在在的。

#### 影响范围

此修改覆盖所有 engine-agnostic 的 INT32/INT64 Decimal 读取路径：
- `GenericParquetReaders`
- `InternalReader`（通过 `BaseParquetReaders`）
- `ParquetAvroValueReaders`

---

### PR #16545 — Flink：实现 wakeup 方法修复线程/内存泄漏

| 字段 | 内容 |
|------|------|
| **PR 链接** | [#16545](https://github.com/apache/iceberg/pull/16545) |
| **作者** | shanzi |
| **合并时间** | 2026-06-09 10:29 UTC |
| **标签** | `flink` |
| **关联 Issue** | Closes #16426 |

#### 背景与问题

**严重级别：高**（资源泄漏）

当 Flink 任务被取消时，使用 Iceberg Source 的 **Source Data Fetcher 线程会泄漏**，且每个泄漏线程持有：
- Parquet 读取缓冲区
- S3 Stream 连接
- Reader 状态

在多次 failover/restart 场景下，泄漏线程不断累积，最终导致 OOM 或资源耗尽。

#### 根因分析

```
问题调用链：
ArrayPoolDataIteratorBatcher.getCachedEntry()
    └── Pool#pollEntry()          ← 阻塞调用
IcebergSourceSplitReader#wakeUp() ← 未实现！（空方法）

Flink 协作式关闭流程：
  调用 wakeUp() → 期望线程从阻塞中退出 → 但因 wakeUp() 为空操作
  → 线程永久阻塞在 pollEntry() 队列 → 泄漏
```

#### 解决架构

引入三个核心组件：

```
┌─────────────────────────────────────────────────────────┐
│  PoolWithWakeup                                         │
│  ├── 包装阻塞队列，增加 lock/condition 同步              │
│  └── wakeup 后 pollEntry() 返回 null（而非永久阻塞）     │
├─────────────────────────────────────────────────────────┤
│  WakeableIterator                                       │
│  ├── 可中断的可关闭迭代器                                │
│  └── 收到 null poll 结果 → 返回 emptyBatch()            │
├─────────────────────────────────────────────────────────┤
│  IcebergSourceSplitReader#wakeUp()（更新）              │
│  ├── volatile 字段确保跨线程可见性                       │
│  └── 将 wakeup 信号转发给当前 reader                    │
└─────────────────────────────────────────────────────────┘
```

**关键设计决策**：
- 空 batch（`emptyBatch()`）既用于水印对齐暂停，也用于关闭场景 → 行为一致，无需区分
- `pauseOrResumeSplits()` 保持 no-op（Iceberg 顺序读取 splits，暂停不依赖 wakeup 信号）

**新增测试**：`TestPoolWithWakeup`、`TestArrayPoolDataIteratorBatcherWakeup`

> 相关 backport PR #16745 已创建，将同步到 Flink v1.20 和 v2.0 分支。

---

### PR #16441 — Core：调整保留字段 ID 的计算逻辑

| 字段 | 内容 |
|------|------|
| **PR 链接** | [#16441](https://github.com/apache/iceberg/pull/16441) |
| **作者** | nastra |
| **合并时间** | 2026-06-09 09:01 UTC |
| **标签** | `core` |
| **修改文件** | `core/src/main/java/org/apache/iceberg/StatsUtil.java`, `TestStatsUtil.java` |
| **评审人** | anoopj, danielcweeks |

#### 背景与问题

Iceberg 规范中，`content_stats` 使用的列统计结构的字段 ID 保留在范围 `[10_000, 200_000_000)` 内。此前实现存在以下问题：
1. stats 字段 ID 的计算存在边界错误
2. 对 data 列与 metadata 列的 stats 字段 ID 区间计算不一致
3. 部分溢出检查逻辑冗余（应由调用方保证）

#### 修复内容

```java
// 关键常量调整（StatsUtil.java）
STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS = 9_000
STATS_SPACE_FIELD_ID_END                       = 200_000_000  // 不含
NUM_SUPPORTED_STATS_PER_COLUMN                 = 200
MAX_DATA_STATS_FIELD_ID                        = 200_010_000
```

- 移除了多余的溢出检查（字段 ID 合法性由上游保证）
- 简化了不必要的 `Math.max()` 调用
- 确保 metadata 字段的计算严格遵守 9K–10K 边界约束

---

### PR #15911 — Core：在 DV 合并中保留加密元数据

| 字段 | 内容 |
|------|------|
| **PR 链接** | [#15911](https://github.com/apache/iceberg/pull/15911) |
| **作者** | manuzhang |
| **合并时间** | 2026-06-09 04:46 UTC |
| **标签** | `core`, `data` |
| **里程碑** | 1.10.3 |
| **评审人** | amogh-jahagirdar, ggershinsky |

#### 背景与问题

当**删除向量（DV）被合并**时，加密输出元数据（encrypted output metadata）未被正确传播。这导致：
- 读取方无法使用最终文件长度重建 `NativeEncryptionKeyMetadata`
- 可能引发解密失败或数据访问异常

#### 修复内容

```
修复前：
BaseDVFileWriter 写入合并后 DV
    └── 加密输出元数据 ✗ 未传播 → 读取方无法解密

修复后：
BaseDVFileWriter.close()
    ├── 提取加密 key metadata（本地化至 close 操作）
    └── 直接传入 DV metadata 创建方法 → 读取方可重建 key
```

**修改文件**：
- `BaseDVFileWriter.java` — 增强加密输出元数据传播
- `DVUtil.java` — DV 元数据操作工具方法修改
- `MergingSnapshotProducer.java` — 集成加密元数据处理
- `TestBaseDVFileWriter.java`、`TestDVWriters.java` — 新增加密场景测试

> 此修复已纳入 1.10.3 milestone，同时 PR #16737 已提交，将 backport 到 1.10.x 分支。

---

### PR #16699 — Spark：修复 manifest rewrite 的 first row ID 丢失问题

| 字段 | 内容 |
|------|------|
| **PR 链接** | [#16699](https://github.com/apache/iceberg/pull/16699) |
| **作者** | amogh-jahagirdar |
| **合并时间** | 2026-06-09 04:42 UTC |
| **标签** | `spark` |
| **里程碑** | 1.10.3 |
| **Spark 版本** | 3.5, 4.0, 4.1 |
| **评审人** | stevenzwu, huaxingao, kevinjqliu, nastra, singhpk234, dramaticlly（5 人通过）|

#### 背景与问题

在 Spark 执行 **manifest rewrite**（manifest 文件重写优化）时，`firstRowId` 字段未被正确继承：

```
问题调用链：
Spark 从 entries 元数据表读取记录
    └── 适配为 SparkContentFile 结构
        └── SparkContentFile 未覆写 firstRowId() 方法
            └── 返回 null（父类默认）
                └── 新行 ID 通过继承机制重新分配 ← BUG
```

这会导致 manifest rewrite 后 row ID 发生变化，破坏依赖 row ID 稳定性的下游逻辑（如 DV 关联、行级 CDC 等）。

#### 修复方案

在 Spark 3.5、4.0、4.1 三个版本的 `SparkContentFile.java` 中**补充 `firstRowId()` 方法覆写**，与已有的 `referencedDataFile()`、`contentOffset()`、`contentSizeInBytes()` 模式一致：

```java
// 新增字段（schema 位置 142）
private final int firstRowIdPosition;

// 新增覆写
@Override
public Long firstRowId() {
    return row.isNullAt(firstRowIdPosition) ? null : row.getLong(firstRowIdPosition);
}
```

**修改文件**（3 个 Spark 版本 × 2 类型 = 6 个文件）：
- `spark/v{3.5,4.0,4.1}/spark/src/main/java/.../SparkContentFile.java`
- `spark/v{3.5,4.0,4.1}/spark/src/test/java/.../TestRewriteManifestsAction.java`

**测试**：新增 3 个测试用例（v3 format 验证），使用 `.doesNotContainNull()` 断言防止回归。

---

## 新增 Issue（3 个）

| # | 标题 | 类型 | 作者 | 状态 |
|---|------|------|------|------|
| [#16744](https://github.com/apache/iceberg/issues/16744) | Core: Expose commit retry exhaustion reason in failure messages | improvement | jhrotko | open |
| [#16741](https://github.com/apache/iceberg/issues/16741) | REST Catalog: expose a staged create-or-replace transaction primitive for non-Java clients | improvement | malon64 | open |
| [#16734](https://github.com/apache/iceberg/issues/16734) | [Spec] table properties needs a spec | bug | Tishj | open |

---

### Issue #16744 — Core：在提交失败信息中暴露重试耗尽原因

**提交者**：jhrotko

**问题描述**：

当 commit 重试因以下原因耗尽时，当前实现直接重新抛出 `CommitFailedException`，**不告知用户是哪个限制被触发**：
- `commit.retry.num-retries` 达到上限
- `commit.retry.total-timeout-ms` 超时
- 两者同时发生

这使得运维人员无法快速定位应该调整哪个表属性。

**提案方向**：
1. 在 `Tasks` 中对重试耗尽原因进行分类
2. 将原始异常作为 cause 保留
3. 在 commit 调用点将重试耗尽原因转换为具体的提交调优建议

**期望错误提示示例**：
```
CommitFailedException: Commit failed after 10 retries. 
Retry attempt limit reached. Consider increasing 'commit.retry.num-retries' (current: 10).
```

**贡献意愿**：作者愿意独立贡献此功能。

---

### Issue #16741 — REST Catalog：为非 Java 客户端暴露 staged create-or-replace 事务原语

**提交者**：malon64（DuckDB Iceberg 集成相关开发者）

**问题描述**：

Java 客户端可通过以下 API 安全实现 `CREATE OR REPLACE TABLE`：

```java
catalog.buildTable(identifier, schema).createOrReplaceTransaction();
```

但非 Java 客户端（如 C++/DuckDB）只能通过 REST Catalog API 操作，而 REST API 目前**缺乏等效的原语**。现有替代方案均有严重缺陷：

| 方案 | 问题 |
|------|------|
| `DROP TABLE` + `CREATE TABLE` | 非原子，中间窗口表会消失 |
| 手动构建 CommitTableRequest | 需重新实现 Java 端复杂的替换规划逻辑 |

**为何简单的最终提交端点不够**：`createOrReplaceTransaction()` 需要在写入数据文件前就决定是 create 还是 replace（因为两者的字段 ID 分配可能不同）。

**提案方向**：增加 REST 级别的 staged create-or-replace 端点：

```http
POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/stage-create-or-replace
```

响应应包含：操作类型（create/replace）、计划的表元数据、字段 ID、分区规范、并发要求、写入凭证等。

**关联 Issue**：#16232（REPLACE TABLE 并发安全问题）

---

### Issue #16734 — [Spec] 表属性缺乏规范定义

**提交者**：Tishj

**问题描述**：

表属性（table properties）在 Iceberg 中承载大量运行时配置（如 `commit.retry.*`、`write.format.default` 等），但**官方规范文档中没有专门章节**系统定义它们。目前 [iceberg runtime docs](https://iceberg.apache.org/docs/1.10.0/configuration/#table-properties) 已成为事实上的参考，但这属于运行时文档而非格式规范文档。

**期望**：在格式规范（`format/` 目录下）中增加表属性专属章节，或至少在规范中明确链接至 runtime docs 并声明其规范地位。

---

## 新增 PR（16 个）

| # | 标题 | 标签 | 作者 | 状态 |
|---|------|------|------|------|
| [#16749](https://github.com/apache/iceberg/pull/16749) | Build: Bump Netty pin to 4.2.15.Final for Trivy fixes | build | nssalian | open |
| [#16748](https://github.com/apache/iceberg/pull/16748) | API, Core: Reuse VariantUtil methods through ByteBuffers | api, core | rdblue | open |
| [#16747](https://github.com/apache/iceberg/pull/16747) | Core: Add a read-only Mumbling bitmap implementation | core | rdblue | open |
| [#16746](https://github.com/apache/iceberg/pull/16746) | AWS: Truncate column comments to 255 Glue limit | aws | jakelong95 | open |
| [#16745](https://github.com/apache/iceberg/pull/16745) | Flink: Backport source fetcher thread leak fix to v1.20 and v2.0 | flink | shanzi | open |
| [#16743](https://github.com/apache/iceberg/pull/16743) | perf(schema-update): use Set\<Integer\> for deletes to get O(1) contains() | core | ingaleniranjan365 | open |
| [#16742](https://github.com/apache/iceberg/pull/16742) | Data: Skip equality-delete filter when there are no equality deletes | data | wombatu-kun | open |
| [#16740](https://github.com/apache/iceberg/pull/16740) | Spark: Spark tests cache rewrite input | spark | Baunsgaard | open |
| [#16739](https://github.com/apache/iceberg/pull/16739) | AWS: Honor init-creation-stacktrace in S3 input and output streams | aws | wombatu-kun | open |
| [#16738](https://github.com/apache/iceberg/pull/16738) | AWS: Optimize Iceberg-to-Glue schema type conversion | aws | wombatu-kun | open |
| [#16737](https://github.com/apache/iceberg/pull/16737) | Core: Backport DV encryption metadata preservation to 1.10.x | core | manuzhang | open |
| [#16736](https://github.com/apache/iceberg/pull/16736) | Core: Extend org.apache.iceberg.hadoop.Configurable in HadoopConfigurable | core | ebyhr | open |
| [#16735](https://github.com/apache/iceberg/pull/16735) | Hive: Fix conversion failure for nested VARIANT and reduce type-string allocations | hive | wombatu-kun | open |
| [#16733](https://github.com/apache/iceberg/pull/16733) | Flink: Avoid redundant java.time allocations in ORC timestamp writers | flink | wombatu-kun | open |
| [#16732](https://github.com/apache/iceberg/pull/16732) | Core: Avoid implementing HadoopConfigurable in ResolvingFileIO | core | ebyhr | closed |
| [#16731](https://github.com/apache/iceberg/pull/16731) | ORC, Flink: Avoid redundant java.time allocations when reading ORC timestamps | orc, flink | wombatu-kun | open |

### 值得关注的新 PR

#### #16747 — Core: Add a read-only Mumbling bitmap implementation
与当天合并的 #16518（Mumbling Bitmap 规范）紧密配套，这是**参考实现 PR**。由 rdblue 提交，代表 Iceberg v4 删除向量嵌入功能向前推进了重要一步。

#### #16742 — Data: Skip equality-delete filter when there are no equality deletes
`wombatu-kun` 持续优化读取路径性能。当无等值删除时，完全跳过 equality-delete filter，可减少不必要的判断开销。

#### #16743 — perf(schema-update): use Set\<Integer\> for deletes to get O(1) contains()
将 `SchemaUpdate` 中的删除字段查找从 `O(n)` 降为 `O(1)`，在大 schema 场景下有明显收益。

#### #16738 — AWS: Optimize Iceberg-to-Glue schema type conversion
AWS Glue 集成的 schema 类型转换优化，减少对象分配。与 #16735（Hive VARIANT 修复）、#16739（S3 stacktrace）共同体现 `wombatu-kun` 对性能与正确性的全面关注。

#### #16749 — Build: Bump Netty pin to 4.2.15.Final for Trivy fixes
安全修复，将 Netty 固定版本升级到 4.2.15.Final 以解决 Trivy 漏洞扫描问题。

---

## 趋势与亮点

### 1. Iceberg v4 删除向量功能稳步推进

今日合并了 Mumbling Bitmap 规范（#16518），并当天就跟进了参考实现 PR（#16747）。同时 DV 加密元数据保留（#15911）也获得合并，且已安排 1.10.x backport（#16737）。**v4 DV 相关功能正处于密集开发阶段**。

```
v4 DV 功能链路：
规范草案(#16518) → 参考实现(#16747) → 加密支持(#15911) → 行 ID 一致性(#16699)
```

### 2. 性能优化浪潮：wombatu-kun 的多线并行

单日提交了 5 个性能/正确性 PR（#16742、#16738、#16739、#16735、#16733/#16731），覆盖 Parquet Decimal 读取、ORC 时间戳、AWS Glue 转换、Hive VARIANT 处理，展现了系统性的性能调优工作。

### 3. 1.10.3 Patch Release 正在准备

多个 PR 挂载了 `1.10.3` milestone（#15911、#16699），表明**patch 版本修复工作正在进行**，主要修复内容包括：
- DV 加密元数据丢失
- Spark manifest rewrite 中 first row ID 丢失

### 4. REST Catalog 的跨语言互操作性需求凸显

Issue #16741 来自 DuckDB 社区，清晰描述了非 Java 客户端实现 `CREATE OR REPLACE TABLE` 的困境。这折射出 Iceberg REST Catalog 在**跨语言原子性操作**方面的设计缺口，值得持续关注。

### 5. Flink 资源泄漏修复完整闭环

#16545 的合并（线程/内存泄漏修复）+ 当天提交的 backport PR #16745（v1.20/v2.0）展示了社区在 bug 修复上的完整交付流程。

---

*本报告由自动化脚本生成，分析内容基于 GitHub API 数据与 PR 页面内容。*
