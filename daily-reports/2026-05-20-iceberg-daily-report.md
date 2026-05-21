# Apache Iceberg 每日动态报告 — 2026-05-20

> **Fork 同步状态：** ✅ `vinlee19/iceberg` 已与 `apache/iceberg@main` 完成同步（2026-05-21 生成）  
> **数据范围：** 2026-05-20 00:00 UTC — 2026-05-21 00:00 UTC

---

## 目录

1. [当日合并 PR 概览](#1-当日合并-pr-概览)
2. [合并 PR 深度分析](#2-合并-pr-深度分析)
   - [#16343 Arrow：修复向量化读取器 int-to-long 提升的 ClassCastException](#pr-16343)
   - [#14234 Spec：添加 v4 内容统计表示（Content Stats）](#pr-14234)
   - [#14122 Spark：移除 Spark 3.4 支持](#pr-14122)
   - [#16419 Flink：修复 ALTER TABLE 添加列到指定位置的 Bug](#pr-16419)
   - [#16429 Flink：将 #16065 回移植到 v2.0 和 v1.20](#pr-16429)
   - [#16431 Docs：新增 1.11.0 版本发布说明](#pr-16431)
   - [#16287 CI：PR 上 CVE 扫描设为阻塞，main 上设为信息提示](#pr-16287)
   - [#16233 Build：runtime-deps 基线只保留 major.minor 版本号](#pr-16233)
3. [新增 Issues（2026-05-20）](#3-新增-issues)
4. [新开 PR（2026-05-20）](#4-新开-pr)
5. [趋势洞察](#5-趋势洞察)

---

## 1. 当日合并 PR 概览

| PR | 标题 | 模块 | 作者 | 类型 |
|----|------|------|------|------|
| [#16343](https://github.com/apache/iceberg/pull/16343) | Arrow: Fix ClassCastException in vectorized reader on int-to-long promotion | Arrow / Parquet | xndai | 🐛 Bug Fix |
| [#14234](https://github.com/apache/iceberg/pull/14234) | Spec: Add v4 content stats representation | 规格说明 | nastra | ✨ Feature |
| [#14122](https://github.com/apache/iceberg/pull/14122) | Spark: Remove Spark 3.4 support | Spark | kevinjqliu | 🗑️ Deprecation |
| [#16419](https://github.com/apache/iceberg/pull/16419) | Flink: Fix ALTER TABLE to add column to specific position | Flink | SteveStevenpoor | 🐛 Bug Fix |
| [#16429](https://github.com/apache/iceberg/pull/16429) | Flink: Backport #16065 to v2.0 and v1.20 | Flink | sqd | 🔙 Backport |
| [#16431](https://github.com/apache/iceberg/pull/16431) | Docs: Add release notes for 1.11.0 | 文档 | aihuaxu | 📝 Docs |
| [#16287](https://github.com/apache/iceberg/pull/16287) | CI: Make CVE scan blocking on PRs | CI/Infra | kevinjqliu | 🔒 Security |
| [#16233](https://github.com/apache/iceberg/pull/16233) | Build: Use major.minor versions in runtime-deps baselines | 构建 | kevinjqliu | 🔧 Build |

**合计：8 个 PR 合并**

---

## 2. 合并 PR 深度分析

---

### PR #16343

## 🐛 [Arrow] 修复向量化读取器 int-to-long 提升的 ClassCastException

**PR 链接：** https://github.com/apache/iceberg/pull/16343  
**作者：** xndai（Xiening Dai）  
**合并时间：** 2026-05-20 15:54 UTC-7  
**关联 Issue：** [#16341](https://github.com/apache/iceberg/issues/16341)  

---

#### 问题描述

当向量化 Arrow 读取器处理 **列从 `int` 提升到 `long`**、且 Parquet 文件中带有 `INT(32)` logical type 注解的列时，触发以下崩溃：

```
java.lang.ClassCastException: BigIntVector cannot be cast to IntVector
```

**崩溃根因：**

```
VectorizedArrowReader.allocateFieldVector()
          │
          ▼
  根据 Iceberg schema（提升后为 LongType）
  → 分配 BigIntVector
          │
          ▼
  LogicalTypeVisitor
  → 读取 Parquet INT(32) logical type
  → 强制转换 BigIntVector → IntVector  ❌ 崩溃！
```

#### 修复方案

修改 `VectorizedArrowReader` 中 `LogicalTypeVisitor` 的向量分配逻辑，**以 Parquet 物理类型为准** 分配向量，而非依赖可能已经提升的 Iceberg schema 类型。

这使向量化读取器与非向量化读取器（`BaseParquetReaders`）的行为一致——后者已通过 `IntAsLongReader` 正确处理此场景。

```
修复前：
  Iceberg LongType → BigIntVector → 被 Parquet INT(32) 强转 → 崩溃

修复后：
  Parquet INT32 物理类型 → 分配 IntVector → IntAsLongReader 包装 → 正确读取 long
```

#### 测试覆盖

新增以下三个测试：

| 测试方法 | 验证场景 |
|---------|---------|
| `testIntToLongPromotionWithLogicalType` | 带 `INT(32, true)` 注解的 int→long 提升 |
| `testIntToLongPromotionWithoutLogicalType` | 裸 INT32 类型的 int→long 提升 |
| `testIntToLongPromotionWithLargeValuesAndReuseContainers` | 大值场景 + 容器复用 |

#### 影响范围

- **受影响场景：** 使用 Arrow 向量化读取器读取经历过 `int` → `long` schema 演化的 Parquet 文件
- **修复文件：** `arrow/` 模块中的 `VectorizedArrowReader.java`

---

### PR #14234

## ✨ [Spec] 添加 v4 内容统计表示（Content Stats）

**PR 链接：** https://github.com/apache/iceberg/pull/14234  
**作者：** nastra（Eduard Tudenhoefner）  
**合并时间：** 2026-05-20 08:30 UTC+2  
**讨论历程：** 长期 RFC，历经多轮审阅，最终获 rdblue、stevenzwu 等核心 committer 批准  

---

#### 背景与动机

Iceberg 规格目前缺乏在 manifest 级别捕获**列级统计信息**的标准方式。现有的 Puffin 文件方案（NDV、直方图）需要独立文件；此 PR 在 **manifest 中直接内联轻量列统计**，查询引擎可利用其做剪枝优化。

#### 规格变更

在 manifest 中新增根字段 `content_stats`（字段 ID 146），结构如下：

```
manifest
└── content_stats (field id: 146)
    └── per-column statistics
        ├── avg_value_size_in_bytes  (float)
        ├── column_sizes            (long, bytes)
        └── null_count / nan_count  (long)
```

**设计决策亮点：**

| 决策项 | 结论 |
|-------|------|
| 是否包含 `max_value_size_in_bytes` | ❌ 延迟，需等查询引擎有实际使用需求 |
| 统计级别 | 文件级（file-level），非 partition 级 |
| 与 Puffin 的关系 | 互补，NDV/直方图仍在 Puffin；轻量统计内联 manifest |
| 命名规范 | 统一使用 `_in_bytes` 后缀，类型保持一致 |

#### 变更文件

- `format/spec.md` — 正式写入 v4 规格文档

#### 影响

这是 Iceberg **格式规格（spec）层面**的变更，对所有引擎（Spark、Flink、Trino 等）均有意义，未来读取优化可直接消费这些统计信息。

---

### PR #14122

## 🗑️ [Spark] 移除 Spark 3.4 支持

**PR 链接：** https://github.com/apache/iceberg/pull/14122  
**作者：** kevinjqliu  
**合并时间：** 2026-05-20 04:55 UTC-4  
**关联 Issue：** [#14121](https://github.com/apache/iceberg/issues/14121) — "Remove Spark 3.4 in Iceberg 1.12 release"  
**审阅者：** nastra、manuzhang、singhpk234  

---

#### 背景

Apache Iceberg 遵循随主要 Iceberg 版本清理旧引擎支持的策略。Spark 3.4 已进入 EOL 阶段，此 PR 为 **Iceberg 1.12.0 发布前**完成该清理工作。

#### 删除内容

```
删除 / 修改的内容：

spark/v3.4/                    ← 完整目录删除（数百个文件）
│
├── spark/src/main/java/...    ← Spark 3.4 实现代码
├── spark/src/test/java/...    ← 大量测试文件（见下方列表）
└── build.gradle               ← 构建配置

其他受影响文件：
├── docs/aws.md                ← 移除 Spark 3.4 兼容说明
├── docs/contribute.md         ← 更新贡献指南
├── docs/multi-engine-support.md ← 更新引擎兼容矩阵
├── docs/releases.md           ← 更新版本说明
└── .trivyignore (Spark 3.4)   ← 安全扫描忽略规则一并删除
```

**删除的典型测试类（共计数十个）：**

```
TestSparkBucketFunction.java
TestSparkDaysFunction.java
TestSparkHoursFunction.java
TestSparkMonthsFunction.java
TestSparkYearsFunction.java
TestSparkTruncateFunction.java
TestStoragePartitionedJoins.java
TestTimestampWithoutZone.java
TestUnpartitionedWrites.java
TestUnpartitionedWritesToBranch.java
... (更多)
```

#### 影响

- **用户影响：** 使用 Spark 3.4 的用户需升级到 Spark 3.5 或 Spark 4.0+
- **构建优化：** 删除冗余代码显著减小代码库，CI 矩阵缩减
- **安全改善：** `.trivyignore` 中针对 Spark 3.4 CVE 的豁免规则同步删除（见 PR #16287）

---

### PR #16419

## 🐛 [Flink] 修复 ALTER TABLE 添加列到指定位置的 Bug

**PR 链接：** https://github.com/apache/iceberg/pull/16419  
**作者：** SteveStevenpoor（Stepan Stepanishchev）  
**合并时间：** 2026-05-20 18:20 UTC+7  
**关联 Issue：** [#16420](https://github.com/apache/iceberg/issues/16420)  
**合并者：** pvary  

---

#### 问题描述

在 Flink SQL 中执行以下语句时，新列不会被添加到预期的位置：

```sql
-- 期望将 new_col 添加到 existing_col 之后
ALTER TABLE my_table ADD COLUMN new_col INT AFTER existing_col;
```

`FlinkAlterTableUtil` 中的位置处理逻辑存在缺陷，导致列总是被追加到末尾，忽略了 `AFTER` / `FIRST` 等位置指令。

#### 修复内容

- **修改：** `FlinkAlterTableUtil` — 修正列位置解析和应用逻辑
- **新增测试：** `TestFlinkCatalogTable#testAlterTableAddColumnPosition()` — 验证不同位置场景（`FIRST`、`AFTER col`）

```
修复前：
  ALTER TABLE ... ADD col AFTER other_col → col 被追加到末尾

修复后：
  ALTER TABLE ... ADD col AFTER other_col → col 正确插入 other_col 之后
```

#### 后续工作

此修复已触发回移植 PR：
- [#16447](https://github.com/apache/iceberg/pull/16447) — Backport 到 Flink v2.0 和 v1.20（当日已合并）

---

### PR #16429

## 🔙 [Flink] 将 #16065 回移植到 Flink v2.0 和 v1.20

**PR 链接：** https://github.com/apache/iceberg/pull/16429  
**作者：** sqd（Han You）  
**合并时间：** 2026-05-20 11:13 UTC-5  
**原始 PR：** [#16065](https://github.com/apache/iceberg/pull/16065)  

---

#### 原始功能（#16065）

**Flink DynamicSink 支持精细化资源管理——配置 Slot Sharing Group**

在 Flink 流处理中，同一 Slot Sharing Group 的算子共享一个 slot。Iceberg DynamicSink 的所有算子此前都默认使用同一 group，导致资源分配不均（写入算子和生成器算子本应消耗更多资源）。

修复后，用户可为以下两类算子**分别指定** slot sharing group：

```java
// Java Builder API
IcebergSink.builder()
    .shuffleWriterSlotSharingGroup("heavy-group")
    .generatorAndForwardWriterSlotSharingGroup("medium-group")
    ...
```

```sql
-- SQL 配置
'write.shuffle-writer-slot-sharing-group' = 'heavy-group'
'write.generator-and-forward-writer-slot-sharing-group' = 'medium-group'
```

#### 此回移植

将上述精细化资源管理能力同步到维护中的 Flink 版本：

| 目标版本 | 状态 |
|---------|------|
| Flink v2.0 | ✅ 已回移植 |
| Flink v1.20 | ✅ 已回移植 |

---

### PR #16431

## 📝 [Docs] 新增 1.11.0 版本发布说明

**PR 链接：** https://github.com/apache/iceberg/pull/16431  
**作者：** aihuaxu（Aihua Xu）  
**合并时间：** 2026-05-20 11:53 UTC-7  
**审阅者：** stevenzwu、kevinjqliu、nssalian、dramaticlly、manuzhang  

---

#### 内容概述

为 Apache Iceberg **1.11.0 正式发布**补充完整的发布说明文档，覆盖：

```
更新文件：
├── site/docs/releases.md        ← 主版本说明，含完整 changelog
├── site/mkdocs.yml              ← 导航配置更新
├── site/nav.yml                 ← 导航结构修改
└── site/docs/multi-engine-support.md ← 引擎兼容性矩阵更新
```

#### 发版说明涵盖的类别

| 类别 | 说明 |
|-----|------|
| Spark 改进 | 新特性与 Bug Fix 列表 |
| Flink 改进 | 新特性与 Bug Fix 列表 |
| Trino/其他引擎 | 兼容性说明 |
| REST Catalog | API 和 OpenAPI 运行时变更 |
| Bug Fixes | 跨模块修复汇总 |
| 性能改进 | 性能相关 PR 汇总 |

> PR 经历多轮迭代，修正了发布说明中的链接错误和不准确描述，最终获多位 committer 批准合并。

---

### PR #16287

## 🔒 [CI] PR 上 CVE 扫描设为阻塞，main 上设为信息提示

**PR 链接：** https://github.com/apache/iceberg/pull/16287  
**作者：** kevinjqliu  
**合并时间：** 2026-05-20（UTC-4）  
**关联：** #15430, #16291, #16290, #14122  

---

#### 背景与动机

此前 CVE 安全扫描对 PR 和 main 分支采用相同策略，存在以下问题：

- 在 main 上严格扫描会导致已知无法修复的 CVE 持续 block 构建
- 在 PR 上宽松扫描会让新引入的漏洞悄然进入代码库

#### 新策略

```
PR（拉取请求）：
  ┌─────────────────────────────────────┐
  │  检测到新 CVE？                      │
  │  → ❌ CI 失败，阻止合并              │
  └─────────────────────────────────────┘

main 分支：
  ┌─────────────────────────────────────┐
  │  检测到 CVE？                        │
  │  → ⚠️ 上报到 GitHub Security Tab    │
  │  → ✅ CI 不阻塞构建                  │
  └─────────────────────────────────────┘
```

#### 特殊处理

由于 Spark 3.4 存在一个无法修复的 CVE，临时加入 `.trivyignore`。此例外在 PR #14122（移除 Spark 3.4 支持）合并后随之删除。

**修改文件：**
- `.github/workflows/kafka-connect-cve-scan.yml`
- `.trivyignore`（临时，已随 #14122 一起清理）

---

### PR #16233

## 🔧 [Build] runtime-deps 基线只保留 major.minor 版本号

**PR 链接：** https://github.com/apache/iceberg/pull/16233  
**作者：** kevinjqliu  
**合并时间：** 2026-05-20（UTC-4）  
**审阅者：** RussellSpitzer  

---

#### 问题根因

`checkRuntimeDeps` 任务将依赖项的完整版本（`major.minor.patch`）记录到 `runtime-deps.txt`。但验证时只比较 `major.minor`，导致：

```
Dependabot 更新 1.2.3 → 1.2.4（patch 更新）
  ├── 实际依赖版本：1.2.4
  └── 基线文件记录：1.2.3
      → 不一致！但 CI 不报错（只验证 1.2）
      → 基线文件悄然漂移
```

**在 PR #16204 中已观察到此问题。**

#### 修复方案

基线文件记录格式从 `major.minor.patch` 改为 `major.minor`，使生成与验证使用完全相同的信息：

```
修复前 runtime-deps.txt:
  com.example:foo:1.2.3
  com.example:bar:4.5.6

修复后 runtime-deps.txt:
  com.example:foo:1.2
  com.example:bar:4.5
```

**效果：**
- Dependabot patch 更新 → 基线无变化 → CI 自动通过 ✅
- major/minor 升级 → 基线不一致 → CI 报错，需人工审查 ⚠️
- 新增/删除依赖 → 基线不一致 → CI 报错 ⚠️

**受影响模块：** AWS、Azure、Build、Flink、GCP、KafkaConnect、Spark

---

## 3. 新增 Issues

> **今日亮点：rdblue 集中提交了大批安全相关 Issues（12 个中有 10 个涉及安全），呈现出系统性安全审查的特征。**

### 🔒 安全类 Issues

| Issue | 标题 | 状态 | 优先级 |
|-------|------|------|--------|
| [#16494](https://github.com/apache/iceberg/issues/16494) | Spark 直接路径访问绕过 catalog 中介 | Not planned | 🔴 高 |
| [#16492](https://github.com/apache/iceberg/issues/16492) | `remove_orphan_files` 可被用作目标表外的删除原语 | Completed | 🔴 高 |
| [#16491](https://github.com/apache/iceberg/issues/16491) | 服务器提供的 REST config 实质上是无安全允许列表的远程客户端重配置 | Open | 🔴 高 |
| [#16490](https://github.com/apache/iceberg/issues/16490) | 计划句柄和幂等键未规范地绑定到主体/路由/请求体 | Completed | 🟡 中 |
| [#16489](https://github.com/apache/iceberg/issues/16489) | 注册端点允许不受限的 `metadata-location` URI（含 `file://`）| Completed | 🔴 高 |
| [#16488](https://github.com/apache/iceberg/issues/16488) | 已废弃的 S3 签名规格完全未声明认证 | Open | 🟡 中 |
| [#16487](https://github.com/apache/iceberg/issues/16487) | 废弃的 OAuth 端点仍文档化了不安全的 token-exchange 行为 | Open | 🟡 中 |
| [#16486](https://github.com/apache/iceberg/issues/16486) | 远程签名端点可通过绝对 URI 逃逸 catalog base URI | Open | 🔴 高 |
| [#16485](https://github.com/apache/iceberg/issues/16485) | 含密钥的 GET 响应仍可被缓存和 ETag 重验证 | Open | 🟡 中 |
| [#16484](https://github.com/apache/iceberg/issues/16484) | `OutputFile.create()` 非原子操作，存在覆盖竞争 | Open | 🔴 高 |
| [#16483](https://github.com/apache/iceberg/issues/16483) | `GoogleAuthManager` 使用 Google 废弃的通用凭据加载器 | Completed | 🟡 中 |

### 🐛 Bug 类 Issues

| Issue | 标题 | 状态 |
|-------|------|------|
| [#16493](https://github.com/apache/iceberg/issues/16493) | `remove_orphan_files` 使用原始字符串前缀匹配来界定 `file_list_view` 范围 | Open（good first issue）|

### 安全审查背景分析

rdblue（Ryan Blue，Iceberg PMC 成员）在当日集中提报多个安全问题，覆盖：

```
REST Catalog 安全面：
  ├── URI 注入（#16489, #16486）
  ├── 配置劫持（#16491）
  ├── 认证缺失（#16488, #16487）
  └── 缓存泄漏（#16485）

存储操作安全面：
  ├── 原子性缺失（#16484）
  └── 越界删除（#16492, #16493）

凭据管理：
  └── 废弃 API 使用（#16483）
```

---

## 4. 新开 PR

| PR | 标题 | 模块 | 作者 | 状态 |
|----|------|------|------|------|
| [#16496](https://github.com/apache/iceberg/pull/16496) | Site: Add version URL alias hook for docs | 文档 | kevinjqliu | Open |
| [#16495](https://github.com/apache/iceberg/pull/16495) | Docs: Drop manual deploy step from release instructions | 文档 | stevenzwu | Open |
| [#16453](https://github.com/apache/iceberg/pull/16453) | Kafka Connect: Make CommitState.isCommitReady() O(1) | KafkaConnect | HenryCaiHaiying | Open |
| [#16452](https://github.com/apache/iceberg/pull/16452) | Core: Remove deprecated APIs for 1.12.0 | Core | raushanprabhakar1 | Open |
| [#16450](https://github.com/apache/iceberg/pull/16450) | Flink: SQL: Add variant avro dynamic record generator | Flink | swapna267 | Open |
| [#16449](https://github.com/apache/iceberg/pull/16449) | All: Remove deprecated methods for 1.12.0 | 全模块 | dramaticlly | Draft |
| [#16447](https://github.com/apache/iceberg/pull/16447) | Flink: Backport #16419 to Flink v2.0 and v1.20 | Flink | SteveStevenpoor | **Merged** |
| [#16446](https://github.com/apache/iceberg/pull/16446) | Spec: clarify Avro encoding for day partition transform | 规格 | laskoviymishka | Draft |
| [#16444](https://github.com/apache/iceberg/pull/16444) | Core/AWS/Azure/GCP: Add per-file failure handler for bulk deletes | Core+Cloud | sarthaksin1857 | Draft |
| [#16443](https://github.com/apache/iceberg/pull/16443) | Docs: adding Dataddo to the vendor list | 文档 | cenotee | Open |

### 值得关注的新 PR

**#16452 / #16449 — 移除 1.12.0 废弃 API**

两个 PR 均以 1.12.0 发布为目标，清理跨模块（Core、Flink、Spark、Parquet、ORC、AWS/GCP/Azure、KafkaConnect）的废弃 API。这是重大版本间的常规清理工作，需关注对下游的兼容性影响。

**#16453 — Kafka Connect CommitState.isCommitReady() O(1)**

将提交就绪状态检查从线性复杂度优化到常量复杂度，对大批量写入场景有性能收益。

**#16444 — 云存储批量删除的逐文件错误处理**

为 AWS S3、Azure ADLS、GCP GCS 的批量删除操作添加逐文件失败处理器（per-file failure handler），提高大规模删除的可靠性和可观测性。

---

## 5. 趋势洞察

### 🚀 1.11.0 已发布，1.12.0 准备工作开始

当日合并的 `#16431`（1.11.0 release notes）确认 **Iceberg 1.11.0 正式发布**。同时，`#16452` 和 `#16449` 已开始针对 **1.12.0 的废弃 API 清理**工作，版本节奏明显加快。

### 🔒 系统性安全审查正在进行

rdblue 单日提报 10+ 安全 issues，涵盖 REST Catalog、存储操作、OAuth/S3 签名等多个面向，表明项目正在进行**主动的系统性安全审查**。重点关注：
- REST Catalog URI 安全（#16489、#16486、#16491）
- 存储操作原子性（#16484）

### 📐 Iceberg Spec v4 进入落地阶段

`#14234`（Content Stats）长期 RFC 终于合并，这是 **Iceberg v4 格式规格**的重要组成部分，未来查询引擎可利用 manifest 内联统计做更精细的剪枝。

### 🔧 持续的构建/CI 健康度投入

`#16287`（CVE 阻塞）和 `#16233`（基线版本格式）体现了项目对 **CI 健康度和安全门禁**的持续投入。

---

*报告由 Claude Code 自动生成 | 数据来源：apache/iceberg@main | 生成时间：2026-05-21*
