# Apache Iceberg 每日动态报告

**日期：** 2026-05-16（北京时间 00:00 — 23:59）  
**生成时间：** 2026-05-17  
**数据来源：** [apache/iceberg](https://github.com/apache/iceberg)  
**Fork 同步：** ✅ vinlee19/iceberg 已同步至 upstream/main 最新状态

---

## 目录

- [概览统计](#概览统计)
- [Fork 同步状态](#fork-同步状态)
- [已合并 PR 深度分析](#已合并-pr-深度分析)
  - [PR #16356 — 构建基础设施优化](#pr-16356)
  - [PR #16167 — Core 层 Bug 修复](#pr-16167)
- [新增 Issue 分析](#新增-issue-分析)
  - [Issue #16361 — Kafka Connect 性能 Bug](#issue-16361)
- [新增 PR 概览](#新增-pr-概览)
  - [PR #16370 — Kafka Connect Parquet Variant Shredding](#pr-16370)
  - [PR #16367 — Gradle 依赖图提交工作流](#pr-16367)
  - [PR #16366 — Kafka Connect Rebalance 异常容忍](#pr-16366)
  - [PR #16363 — Parquet 自适应 Bloom Filter](#pr-16363)
  - [PR #16362 — API 字符串截断谓词改写](#pr-16362)
  - [PR #16364 — UUID 基础测试扩展](#pr-16364)
  - [PR #16368 — 官网搜索去重](#pr-16368)
  - [PR #16365 — Spark 4.1.2 兼容验证（Draft）](#pr-16365)
- [技术趋势分析](#技术趋势分析)

---

## 概览统计

```
┌─────────────────────────────────────────────────────────┐
│              2026-05-16  Apache Iceberg 日报             │
├──────────────────┬──────────────────────────────────────┤
│  合并 PR         │  2 个                                  │
│  新增 PR         │  9 个（含 1 个 Draft、1 个已关闭）     │
│  新增 Issue      │  1 个                                  │
│  涉及模块        │  Core / Kafka Connect / Parquet /      │
│                  │  Build / API / Spark / Flink / Website │
│  Fork 同步       │  ✅ 已同步（15 文件，81 行变更）        │
└──────────────────┴──────────────────────────────────────┘
```

### 活动分布

```
模块活跃度（PR 数量）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Kafka Connect  ████████████ 3 个 PR + 1 个 Issue
Parquet        ████████     2 个 PR
Build/Infra    ████████     2 个 PR（含已合并）
Core           ████         1 个 PR + 已合并修复
API            ████         1 个 PR
Spark          ██           1 个 PR（Draft）
Website        ██           1 个 PR
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

---

## Fork 同步状态

```
上游 apache/iceberg (main)
        │
        │  git fetch upstream main
        ▼
upstream/main ────────────────────────────────────────────►
                                                            │
                                                     git merge
                                                            │
                                                            ▼
        vinlee19/iceberg (claude/happy-ride-IZfNT) ───────►
```

**同步结果：**
- 同步分支：`claude/happy-ride-IZfNT`
- 变更文件：**15 个文件**，新增 **81 行**，删除 **7 行**
- 主要变更来自：PR #16356（CI 工作流优化）和 PR #16167（ByteBufferInputStream 修复）

---

## 已合并 PR 深度分析

### PR #16356

> **Build: Designate a single Gradle cache writer across CI workflows**  
> 作者：`kevinjqliu` | 标签：`INFRA` | 合并时间：2026-05-16

#### 问题背景

Apache Iceberg 的 CI/CD 管道存在严重的**缓存竞争（Cache Thrashing）**问题：

```
┌────────────────────────────────────────────────────────────────┐
│                   原有问题示意图                                  │
│                                                                  │
│  Commit Push                                                     │
│      │                                                           │
│      ├──► Job 1 ──write──► Cache Entry A (3-4 GB)               │
│      ├──► Job 2 ──write──► Cache Entry B (3-4 GB)  ← 互相覆盖   │
│      ├──► Job 3 ──write──► Cache Entry C (3-4 GB)               │
│      │      ... × 10 个 Gradle 调用 × 12 个工作流               │
│      │                                                           │
│  GitHub Cache 容量上限：10 GB（LRU 驱逐）                        │
│                                                                  │
│  结果：热缓存被持续驱逐 → 重新下载依赖 → 浪费构建时间           │
└────────────────────────────────────────────────────────────────┘
```

#### 解决方案：单写多读策略

```
┌────────────────────────────────────────────────────────────────┐
│                   修复后架构                                      │
│                                                                  │
│  Push to main                                                    │
│      │                                                           │
│      └──► build-checks(java-ci.yml) ──WRITE──► Cache ◄──────┐  │
│                                                               │  │
│  其他所有 Job（11 个）────────────────────READ ONLY──────────┘  │
│                                                                  │
│  jmh-benchmarks.yml ──────────────────── 完全禁用缓存            │
│  (workflow_dispatch 触发，避免污染)                               │
└────────────────────────────────────────────────────────────────┘
```

#### 涉及文件（12 个 CI 工作流）

| 文件 | 变更内容 |
|------|---------|
| `.github/workflows/java-ci.yml` | `build-checks` 设为写入者；其余 3 个 job 设为只读 |
| `.github/workflows/spark-ci.yml` | `cache-read-only: true` |
| `.github/workflows/flink-ci.yml` | `cache-read-only: true` |
| `.github/workflows/hive-ci.yml` | `cache-read-only: true` |
| `.github/workflows/kafka-connect-ci.yml` | `cache-read-only: true` |
| `.github/workflows/delta-conversion-ci.yml` | 两个 job 均设只读 |
| `.github/workflows/cve-scan.yml` | `cache-read-only: true` |
| `.github/workflows/api-binary-compatibility.yml` | `cache-read-only: true` |
| `.github/workflows/publish-snapshot.yml` | `cache-read-only: true` |
| `.github/workflows/publish-iceberg-rest-fixture-docker.yml` | `cache-read-only: true` |
| `.github/workflows/recurring-jmh-benchmarks.yml` | `cache-read-only: true` |
| `.github/workflows/jmh-benchmarks.yml` | 完全禁用缓存 |

#### 验证效果（4 轮测试）

```
Round 1: 写入者创建新缓存条目 ✅
Round 2: PR 运行，0 个新缓存条目产生 ✅
Round 3: 后续 main push 只更新写入者条目 ✅
Round 4: 只读 job 日志显示 "Cache is read-only" ✅
```

**核心收益：** 消除竞争 → 确定性缓存状态 → 减少冗余构建时间 → 节省 CI 成本

---

### PR #16167

> **Core: Fix ByteBufferInputStream.read() to return -1 at EOF**  
> 作者：`sachinnn99` | 标签：`core` | 合并时间：2026-05-16 | 关联 Issue：#16127

#### 问题背景

Java `InputStream` 的标准契约规定：`read()` 方法在遇到 EOF 时**必须返回 `-1`**。  
然而 Iceberg 的两个 InputStream 实现违反了这一约定：

```java
// ❌ 修复前：违反 Java InputStream 契约
public int read() {
    if (position >= end) {
        throw new EOFException();  // 不应该在这里抛异常！
    }
    return buffer.get(position++) & 0xFF;
}
```

```java
// 用户无法使用标准的 while 循环模式
while ((b = in.read()) != -1) {  // ← 这段代码会因为 EOFException 而崩溃
    process(b);
}
```

#### 不一致性

```
┌──────────────────────────────────────────────────────────────┐
│               修复前的行为不一致                               │
│                                                               │
│  方法                           EOF 时行为       是否正确     │
│  ──────────────────────────────────────────────────────────  │
│  read()                         抛 EOFException   ❌ 错误     │
│  read(byte[], int, int)         返回 -1            ✅ 正确    │
│                                                               │
│  单字节读取和批量读取行为不一致！                              │
└──────────────────────────────────────────────────────────────┘
```

#### 修复内容

**修改文件：**

```
core/src/main/java/org/apache/iceberg/io/
├── SingleBufferInputStream.java    ← read() 改为返回 -1
└── MultiBufferInputStream.java     ← read() 两处 EOF 点均改为返回 -1

core/src/test/java/org/apache/iceberg/io/
└── TestByteBufferInputStreams.java  ← 新增 34 行测试
```

**SingleBufferInputStream 修复：**

```java
// ✅ 修复后
public int read() {
    if (position >= end) {
        return -1;  // 符合 Java InputStream 契约
    }
    return buffer.get(position++) & 0xFF;
}
```

**新增测试覆盖：**

| 测试方法 | 验证内容 |
|---------|---------|
| `testReadByte()` | 断言 EOF 时返回 -1，验证幂等性 |
| `assertAtEOF()` | 跨位置/可用性/重复读取验证 EOF 行为 |
| `testEmptyStream()` | 单/多空 Buffer 及空 Buffer 列表 |
| `testDrainedMultiBufferStream()` | 触发 `nextBuffer()` 代码路径 |

**未修改的方法：**  
`slice()`、`sliceBuffers()`、`skipFully()` 等"精确读取 N 字节"语义方法仍保留 `EOFException`，因为这是其正确行为。

**36 个检查全部通过 ✅**

---

## 新增 Issue 分析

### Issue #16361

> **Kafka Connect: Coordinator's check on commitState.isCommitReady() is inefficient**  
> 报告者：`HenryCaiHaiying` | 状态：Open | 标签：Bug | 版本：1.10.1

#### 性能问题描述

Kafka Connect Coordinator 的 `commitState.isCommitReady()` 方法存在算法复杂度问题：

```
┌──────────────────────────────────────────────────────────────┐
│                  O(N²) 问题示意                                │
│                                                               │
│  当前实现：遍历所有历史 DATA_COMPLETE 消息来验证 Topic 分区    │
│                                                               │
│  消息数 n   → 处理复杂度                                      │
│  ─────────────────────────────────────────────────────────── │
│  100 条     → 10,000 次比较                                   │
│  200 条     → 40,000 次比较（积压时翻倍）                     │
│  1000 条    → 1,000,000 次比较  ← 严重性能降级                │
│                                                               │
│  触发场景：网络问题或 HiveMetaStore 不可用 → 积压堆积         │
│           → Worker 持续生成 DATA_COMPLETE                     │
│           → 下次循环要处理 2n 条消息                          │
└──────────────────────────────────────────────────────────────┘
```

#### 提议的修复方案

```
当前实现（O(N²)）：
  for each message in all_messages:
    for each partition in expected_partitions:
      if message matches partition: mark seen

建议实现（O(N)）：
  seen_partitions = HashMap()
  for each message in all_messages:
    seen_partitions.put(message.partition, true)
  return seen_partitions.size() == expected_count
```

**状态：** 报告者表示愿意提交 PR 修复，等待社区指引。

---

## 新增 PR 概览

### PR #16370

> **Kafka Connect: Enable Parquet variant shredding for generic Record writes**  
> 作者：`soumilshah1995` | 状态：Open

#### 什么是 Variant Shredding？

```
┌──────────────────────────────────────────────────────────────┐
│              Parquet Variant Shredding 原理                   │
│                                                               │
│  未启用 Shredding：                                           │
│  ┌─────────────────────────────────┐                         │
│  │  VARIANT 列（不透明 blob）       │                         │
│  │  {"name":"Alice","age":30,...}  │                         │
│  │  {"name":"Bob","score":95,...}  │                         │
│  └─────────────────────────────────┘                         │
│  查询 name 字段 → 必须解析整个 blob                           │
│                                                               │
│  启用 Shredding：                                             │
│  ┌──────────┬───────┬───────────┐                            │
│  │ name(str)│age(int)│score(int)│                            │
│  │  Alice   │  30   │   null   │                            │
│  │  Bob     │ null  │   95     │                            │
│  └──────────┴───────┴──────────┘                             │
│  查询 name 字段 → 直接读取列，性能大幅提升                    │
└──────────────────────────────────────────────────────────────┘
```

#### 技术实现

- **问题根因：** Kafka Connect 使用 Iceberg generic `Record` 模型和 `Void` 引擎 schema，缺少 Analyzer 和 Row Copier 组件
- **修复：** 在 `GenericFormatModels` 中注册 `ParquetFormatModel` 与 `RecordVariantShreddingAnalyzer`

**配置方式：**
```properties
write.parquet.shred-variants=true
write.parquet.variant-inference-buffer-size=<rows>
```

---

### PR #16367

> **Build: Add Gradle dependency-submission workflow**  
> 作者：`kevinjqliu` | 状态：Open

#### 功能概述

新增 GitHub Actions 工作流，自动向 GitHub 的 Dependency Submission API 提交完整的 Gradle 依赖图，实现：

```
┌──────────────────────────────────────────────────────────────┐
│           依赖安全监控架构                                     │
│                                                               │
│  触发条件：                                                    │
│  ├── Push to main                                            │
│  ├── 每日 06:17 UTC（定时）                                   │
│  └── 手动 workflow_dispatch                                   │
│                                                               │
│  执行内容：                                                    │
│  gradle/actions/dependency-submission                        │
│      └── -DallModules（包含所有 Spark/Flink/Kafka 子项目）   │
│      └── 排除 :buildSrc 和测试依赖                           │
│                                                               │
│  输出：                                                        │
│  GitHub Dependency Graph ──► Dependabot CVE 告警             │
│                                                               │
│  现有工具对比：                                                │
│  cve-scan.yml       → 扫描已发布 jar 文件（Trivy）           │
│  dependency-submission → 扫描 Gradle 声明依赖（新增）        │
└──────────────────────────────────────────────────────────────┘
```

**安全措施：**
- 30 分钟超时
- `persist-credentials: false`
- 所有 Action 固定到特定 SHA
- Fork 仓库跳过执行

---

### PR #16366

> **Kafka Connect: Tolerate CommitFailedException and InvalidProducerEpochException during rebalance**  
> 作者：`kumarpritam863` | 状态：Open

#### 问题场景

```
Consumer Group Rebalance 期间的事务失败：

时间线：
  T1: producer.beginTransaction()
  T2: 写入数据到 Iceberg
  T3: [Rebalance 发生！Generation ID 变化]
  T4: producer.commitTransaction()
       ↓
    CommitFailedException / InvalidProducerEpochException
       ↓
  ❌ 当前行为：任务崩溃（Fatal Error）
  ✅ 期望行为：转换为 RetriableException，等待重新分配后重试
```

#### 修复逻辑

```java
// Channel.send() 中的异常处理
try {
    producer.commitTransaction();
} catch (CommitFailedException | InvalidProducerEpochException e) {
    producer.abortTransaction();
    // ✅ 转换为可重试异常，而非致命错误
    throw new RetriableException("Rebalance occurred, will retry", e);
}
```

**数据安全保障：**
- 事务原子性：中止事务不推进 offset → 无数据丢失
- 无双重提交：通过协调器级别的 offset 属性保证

---

### PR #16363

> **Parquet: add adaptive bloom filter sizing (PARQUET-2326)**  
> 作者：`raghav-reglobe` | 状态：Open

#### 当前问题

```
固定大小 Bloom Filter 的浪费：

写入 5 行数据，max-bloom-filter-bytes = 4MB：
┌─────────────────────────────────┐
│ 实际数据：  ~268 KB             │
│ Bloom Filter：~4 MB（预分配）   │
│ 总文件大小：~4.2 MB             │
│ 浪费比例：  16倍！              │
└─────────────────────────────────┘
```

#### 解决方案：自适应 Bloom Filter

```
新增 Table Property：
write.parquet.bloom-filter-adaptive-enabled=true（默认 false）

启用后的工作原理：
┌─────────────────────────────────────────────────────────────┐
│  parquet-mr 的 AdaptiveBlockSplitBloomFilter                 │
│                                                               │
│  评估多个候选尺寸                                             │
│      │                                                        │
│      ├── 候选大小 1 (1 MB)                                   │
│      ├── 候选大小 2 (512 KB)   ← 选择满足实际去重值数量的    │
│      ├── 候选大小 3 (128 KB)      最小尺寸（按配置 FPP）     │
│      └── 候选大小 4 (32 KB) ✅                               │
│                                                               │
│  结果：文件至少缩小 2 倍                                      │
└─────────────────────────────────────────────────────────────┘
```

**修改文件：**
- `TableProperties.java` — 新增常量和默认值
- `Parquet.java` — 通过 Context 和 WriteBuilder 集成属性
- `TestParquetAdaptiveBloomFilter.java` — 新增单元测试

---

### PR #16362

> **API: Rewrite string truncate equality predicates onto the source column**  
> 作者：`wombatu-kun` | 状态：Open

#### 背景

长期存在的 TODO：将 `truncate(col) == value` 转换为更高效的 `col STARTS_WITH value` 操作。

#### 等价规则

```
对于 truncate 宽度 W 和字面量 v：

len(v) > W  →  EQ: alwaysFalse()      NOT_EQ: alwaysTrue()
              （v 截断后不可能等于 v）
              
len(v) == W →  EQ: col STARTS_WITH v  NOT_EQ: col NOT_STARTS_WITH v
              （截断后恰好等于 v，意味着 col 以 v 开头）

len(v) < W  →  EQ: col == v           NOT_EQ: col != v
              （截断宽度大于 v 长度，不影响 v）
```

#### 优化效果

```
查询：WHERE truncate(name, 10) = 'Alice'

优化前：
  读取所有行 → 对每行应用 truncate → 与 'Alice' 比较

优化后：
  利用分区修剪（partition pruning）
  利用指标修剪（metrics pruning）
  利用字典修剪（dictionary pruning）
  → 直接过滤，大幅减少读取数据量
```

**新增公共 API：**
- `Transforms.StringTruncateRewrite` 枚举
- `Transforms.stringTruncateRewrite()` 方法

---

### PR #16364

> **Core, Flink: Add UUID to DataTestBase SUPPORTED_PRIMITIVES**  
> 作者：`joyhaldar` | 状态：Open

#### 变更内容

扩展 `DataTestBase.SUPPORTED_PRIMITIVES` 集合，加入 UUID 类型，使所有继承该基类的测试自动覆盖 UUID 读写。

**影响范围：**
- `core/` — DataTestBase 更新
- `flink/` — TestFlinkParquetReader 中的 Parquet schema（支持 Flink 1.20、2.0、2.1）

---

### PR #16368

> **Website: remove duplicated entries from search**  
> 作者：`MaxNevermind` | 状态：Open

#### 问题

```
搜索 "rewrite_table_path" 返回：
  ├── [1.4 文档] rewrite_table_path
  ├── [1.5 文档] rewrite_table_path
  ├── [1.6 文档] rewrite_table_path
  ├── ...
  ├── [1.10 文档] rewrite_table_path  ← 8 个重复结果！
  └── [nightly 文档] rewrite_table_path
```

**修复：** 在 `site/mkdocs.yml` 中配置 `mkdocs-exclude-search` 插件，只保留 nightly 文档在搜索索引中。

---

### PR #16365

> **Spark: Verify Spark 4.1.2**  
> 作者：`manuzhang` | 状态：Draft  
> 验证 Iceberg 与 Spark 4.1.2 的兼容性。

---

## 技术趋势分析

```
┌──────────────────────────────────────────────────────────────┐
│                  2026-05-16 技术主题分析                      │
│                                                               │
│  1. 🔧 构建基础设施稳定性（2 个 PR）                          │
│     CI 缓存优化 + Gradle 依赖图 → 关注 CI 效率和安全          │
│                                                               │
│  2. 📡 Kafka Connect 可靠性（3 个 PR + 1 Issue）             │
│     Variant Shredding / Rebalance 容忍 / 性能优化             │
│     → Kafka Connect 模块正在快速成熟                          │
│                                                               │
│  3. 🚀 查询性能优化（2 个 PR）                               │
│     自适应 Bloom Filter + 谓词改写                            │
│     → 持续关注读取性能，特别是 Parquet 优化                   │
│                                                               │
│  4. 🐛 合规性修复（1 个合并 PR）                             │
│     ByteBufferInputStream 契约修复                            │
│     → 重视 Java 标准接口兼容性                                │
│                                                               │
│  5. 🔒 安全监控（1 个 PR）                                   │
│     自动化依赖 CVE 扫描 → 供应链安全意识提升                  │
└──────────────────────────────────────────────────────────────┘
```

### 重点关注建议

| 优先级 | PR/Issue | 原因 |
|--------|---------|------|
| ⭐⭐⭐ | PR #16366 Kafka Rebalance 容忍 | 生产环境 Kafka Rebalance 是常见场景，当前 Fatal 行为影响稳定性 |
| ⭐⭐⭐ | Issue #16361 O(N²) 性能问题 | 在高并发/积压场景下会显著降低 Coordinator 性能 |
| ⭐⭐ | PR #16363 自适应 Bloom Filter | 对写小批量数据的场景有显著存储节省 |
| ⭐⭐ | PR #16362 谓词改写 | 改善基于 truncate 分区表的查询性能 |
| ⭐ | PR #16367 依赖图提交 | 改善安全态势，补充现有 CVE 扫描 |

---

*报告由 Claude Code 自动生成 | 数据截止：2026-05-16 23:59 UTC*
