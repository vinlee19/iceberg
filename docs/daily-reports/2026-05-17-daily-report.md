# Apache Iceberg 每日动态报告 — 2026-05-17

> **报告范围：** 2026-05-17 00:00 UTC — 2026-05-17 23:59 UTC  
> **上游仓库：** [apache/iceberg](https://github.com/apache/iceberg)  
> **Fork 仓库：** [vinlee19/iceberg](https://github.com/vinlee19/iceberg)  
> **同步状态：** ✅ 已同步（main 分支快进合并至 `6a3f782`）

---

## 📊 当日活动总览

| 类别 | 数量 |
|------|------|
| ✅ 合并的 PR (Merged PRs) | **9** |
| 🆕 新开 PR (New PRs) | **8** |
| 🐛 新增 Issue (New Issues) | **2** |
| 👤 贡献者 (Contributors) | **4** (+ dependabot) |

```
活动热力图 (2026-05-17 UTC)
00:00  ▓▓▓▓░░░░░░░░░░░░░░░░░░░░  06:00  ▓▓▓▓▓░░░░░░░░░░░░░░░░░░░  12:00
       #16368 #16167                    #16372~75 #16379                 #16356
                                                                                
12:00  ░░░░░░░░░░░░░░░░░░░░░░░░  18:00  ▓▓▓▓░░░░░░░░░░░░░░░░░░░░  24:00
                                       #16378
```

---

## 🔄 Fork 同步状态

```
vinlee19/iceberg (main)
  旧 HEAD: 6976e020  →  新 HEAD: 6a3f7827
  快进合并: 21 个文件变更 | +95 行 / -15 行
  
  上游分支:    upstream/main ──────────────────●  6a3f782 (最新)
  Fork 分支:   origin/main   ──────────────────●  6a3f782 (已同步 ✅)
```

**合并的文件变更概览：**

| 文件类别 | 文件数 | 说明 |
|----------|--------|------|
| `.github/workflows/*.yml` | 18 | CI 流程优化 + Action 版本更新 |
| `core/src/main/java/...` | 2 | ByteBufferInputStream EOF 修复 |
| `core/src/test/java/...` | 1 | 相关测试增强 |
| `gradle/libs.versions.toml` | 1 | 依赖版本升级 |
| `site/mkdocs.yml` | 1 | 文档搜索去重 |
| `site/requirements.txt` | 2 | 文档依赖更新 |
| `open-api/requirements.txt` | 1 | OpenAPI 依赖更新 |

---

## ✅ 已合并 PR 详细分析

### 🔴 重要修复

---

#### PR #16167 — `Core: Fix ByteBufferInputStream.read() to return -1 at EOF`

| 字段 | 内容 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16167 |
| **作者** | Sachin Ranjalkar (`sachinnn99`) |
| **合并时间** | 2026-05-17 01:25 +0530 |
| **标签** | `core` |
| **关联 Issue** | #16127 |

**问题背景**

`ByteBufferInputStream` 是 Iceberg 核心 I/O 层的基础读取类，有两个实现：
- `SingleBufferInputStream` — 单缓冲区实现
- `MultiBufferInputStream` — 多缓冲区实现

这两个类的 `read()` 方法在流数据读取完毕（EOF）时，**违反了 `java.io.InputStream` 的契约**：标准规定 EOF 时应返回 `-1`，但这两个类会**抛出 `EOFException`**，导致所有依赖 `read()` 返回值来检测 EOF 的调用者出现意外异常。

**代码变更**

`SingleBufferInputStream.java`（修复前 vs 修复后）：

```java
// 修复前 ❌ — 不符合 InputStream 契约
@Override
public int read() throws IOException {
    if (!buffer.hasRemaining()) {
        throw new EOFException();          // 违反 InputStream 规范
    }
    return buffer.get() & 0xFF;
}

// 修复后 ✅ — 符合标准规范
@Override
public int read() throws IOException {
    if (!buffer.hasRemaining()) {
        return -1;                         // 正确：EOF 返回 -1
    }
    return buffer.get() & 0xFF;
}
```

`MultiBufferInputStream.java`（两处修复）：

```java
// 修复前 ❌
public int read() throws IOException {
    if (current == null) {
        throw new EOFException();          // 缓冲区为空时抛出异常
    }
    while (true) {
        if (current.hasRemaining()) {
            return current.get() & 0xFF;
        } else if (!nextBuffer()) {
            throw new EOFException();      // 没有更多缓冲区时抛出异常
        }
    }
}

// 修复后 ✅
public int read() throws IOException {
    if (current == null) {
        return -1;                         // 正确返回 -1
    }
    while (true) {
        if (current.hasRemaining()) {
            return current.get() & 0xFF;
        } else if (!nextBuffer()) {
            return -1;                     // 正确返回 -1
        }
    }
}
```

**测试增强** (`TestByteBufferInputStreams.java`, +31 行)

新增 `testDrainedMultiBufferStream` 测试用例，专门验证 `MultiBufferInputStream` 在所有缓冲区耗尽后触发 `nextBuffer() → return -1` 代码路径；同时增强 `assertAtEOF` 辅助方法，同时断言 `read()` 和 `read(byte[])` 两个重载。

**影响分析**

```
修复影响范围:
  InputStream.read()  ──→  EOF 时返回 -1 (标准行为)
                     ✅ 兼容所有依赖 while((b = read()) != -1) 模式的代码
                     ✅ 兼容 Java 标准库 I/O 流接口
                     ✅ 修复与 Parquet/ORC 等格式读取器的集成问题
```

**变更统计：** 3 个文件 | +33 行 / -7 行

---

### 🟡 构建基础设施改进

---

#### PR #16356 — `Build: Designate a single Gradle cache writer across CI workflows`

| 字段 | 内容 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16356 |
| **作者** | Kevin Liu (`kevinjqliu`) |
| **合并时间** | 2026-05-16 15:37 UTC (本次同步纳入) |
| **标签** | `build` |

**问题背景**

Iceberg 项目有 12 个 CI workflow，每个都会尝试写入 Gradle 构建缓存，造成：
- 缓存写入竞争（多个 job 并发写入相同键）
- 不必要的存储和网络开销
- 缓存污染风险

**解决方案**

引入 **唯一缓存写入者** 策略：只有 `java-ci.yml` 中运行在 `main` 分支且使用 JVM 17 的 `build-checks` job 有权写缓存，其他所有 job 设置为只读（`cache-read-only: true`）。

**关键配置变更（java-ci.yml 示例）：**

```yaml
# build-checks job (唯一的缓存写入者)
- uses: gradle/actions/setup-gradle@...
  with:
    # Writes cache on main; read-only otherwise.
    cache-read-only: ${{ !(github.ref == 'refs/heads/main' && matrix.jvm == 17) }}

# build-javadoc job (只读)
- uses: gradle/actions/setup-gradle@...
  with:
    # Read-only: java-ci's build-checks (17) is the global canonical writer.
    cache-read-only: true
```

**涉及的 CI 工作流（12个）：**

```
api-binary-compatibility.yml  ─→  cache-read-only: true
cve-scan.yml                  ─→  cache-read-only: true
delta-conversion-ci.yml       ─→  cache-read-only: true
flink-ci.yml                  ─→  cache-read-only: true
hive-ci.yml                   ─→  cache-read-only: true
java-ci.yml (build-checks)    ─→  cache-read-only: !(main && jvm=17)  ← 唯一写入者
java-ci.yml (其他job)         ─→  cache-read-only: true
jmh-benchmarks.yml            ─→  cache-read-only: true
kafka-connect-ci.yml          ─→  cache-read-only: true
publish-*.yml                 ─→  cache-read-only: true
recurring-jmh-benchmarks.yml  ─→  cache-read-only: true
spark-ci.yml                  ─→  cache-read-only: true
```

**变更统计：** 12 个文件 | +48 行 / 0 行

---

### 🟢 文档改进

---

#### PR #16368 — `Website: remove duplicated entries from search`

| 字段 | 内容 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16368 |
| **作者** | Maksim Konstantinov |
| **合并时间** | 2026-05-17 01:54 UTC |
| **标签** | `docs` |

**问题背景**

Iceberg 文档网站（基于 MkDocs Material）维护多个版本目录（`docs/nightly/`、`docs/latest/`、`docs/x.y.z/`），全局搜索索引将所有版本页面都加入索引，导致搜索结果出现大量**重复条目**，用户体验较差。

**解决方案**

引入 `mkdocs-exclude-search` 插件，只索引 `docs/nightly/` 目录，从搜索中排除历史版本：

```yaml
# site/mkdocs.yml
plugins:
  - exclude-search:
      # Index only docs/nightly/* to avoid duplicate hits across versions.
      exclude:
        - 'docs/latest*'   # 排除 latest 及其子页面
        - 'docs/[0-9]*'    # 排除 docs/<x.y.z> 及其子页面
```

```
# site/requirements.txt 新增依赖
mkdocs-exclude-search==0.6.6
```

**效果示意：**

```
修复前：搜索 "partition spec"
  结果: [nightly] Partition Spec      ← 重复 ×3
        [latest]  Partition Spec
        [1.9.1]   Partition Spec
        ...

修复后：搜索 "partition spec"
  结果: [nightly] Partition Spec      ← 唯一结果 ✅
```

**变更统计：** 2 个文件 | +6 行 / 0 行

---

### ⚪ 依赖版本升级（dependabot）

以下 6 个 PR 均由 dependabot 自动生成，属于常规依赖维护：

| PR | 依赖名称 | 旧版本 | 新版本 | 升级类型 | 影响范围 |
|----|----------|--------|--------|----------|----------|
| [#16378](https://github.com/apache/iceberg/pull/16378) | `actions/labeler` | 6.0.1 | **6.1.0** | minor | GitHub Actions CI |
| [#16375](https://github.com/apache/iceberg/pull/16375) | `github/codeql-action` | 4.35.1 | **4.35.4** | patch | 安全扫描 CI |
| [#16379](https://github.com/apache/iceberg/pull/16379) | `org.xerial:sqlite-jdbc` | 3.53.0.0 | **3.53.1.0** | patch | SQLite JDBC 驱动 |
| [#16374](https://github.com/apache/iceberg/pull/16374) | `jetty` | 12.1.8 | **12.1.9** | patch | HTTP 服务器（测试） |
| [#16373](https://github.com/apache/iceberg/pull/16373) | `datamodel-code-generator` | 0.56.1 | **0.57.0** | minor | OpenAPI 代码生成 |
| [#16372](https://github.com/apache/iceberg/pull/16372) | `pymarkdownlnt` | 0.9.36 | **0.9.37** | patch | Markdown lint |

**`actions/labeler` 6.1.0 主要新功能：**
- 新增 `label-count-limit` 选项，可限制单次添加的标签数量上限
- 改进文档和配置示例
- 内部依赖更新

**`github/codeql-action` 4.35.4 修复：**
- 安全漏洞修复（patch 版本）
- 分析引擎稳定性改进

---

## 🆕 当日新开 PR 分析

### 功能性 PR（需关注）

---

#### PR #16384 — `Spark: Push down aggregates for partition-column GROUP BY`

| 字段 | 内容 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16384 |
| **作者** | wombatu-kun |
| **状态** | Open（进行中） |
| **标签** | `spark` |

**功能说明**

扩展 Spark 聚合下推（Aggregate Pushdown）能力，支持 `GROUP BY` 分区列时的聚合下推优化。这是对现有聚合下推功能的重要增强——允许 Spark 在读取 Iceberg 表时，将 `COUNT`、`MIN`、`MAX`、`SUM` 等聚合操作直接从 Parquet/ORC 文件统计信息中读取，而无需扫描实际数据行。

**预期效果：**
```sql
-- 对于按分区列 GROUP BY 的查询，可利用文件级统计信息
SELECT date, COUNT(*), MIN(value), MAX(value)
FROM iceberg_table
GROUP BY date   -- date 是分区列
-- 优化前: 全表扫描
-- 优化后: 从 manifest 文件统计信息直接计算 ✅
```

---

#### PR #16383 — `API: Rewrite string truncate equality predicates`

| 字段 | 内容 |
|------|------|
| **PR 链接** | https://github.com/apache/iceberg/pull/16383 |
| **作者** | zhjwpku |
| **状态** | Open（进行中） |
| **标签** | `API`, `data`, `parquet` |

**功能说明**

改进 Iceberg 谓词下推机制中字符串截断（truncate）转换相等谓词的逻辑。当列使用 `truncate(N)` 变换时，`equal(value)` 谓词可以被改写为更精确的范围谓词，从而减少不必要的文件扫描。

---

### 依赖更新 PR（进行中）

| PR | 依赖 | 升级 | 状态 | 备注 |
|----|------|------|------|------|
| [#16382](https://github.com/apache/iceberg/pull/16382) | `com.google.cloud:libraries-bom` | 26.80.0 → 26.81.0 | Open | 手动提交（可能与 #16377 重复） |
| [#16381](https://github.com/apache/iceberg/pull/16381) | `gradle-wrapper` | 8.14.4 → 8.14.5 | Open | 手动提交（可能与 #16376 重复） |
| [#16377](https://github.com/apache/iceberg/pull/16377) | `com.google.cloud:libraries-bom` | 26.80.0 → 26.81.0 | Open | dependabot 自动 |
| [#16376](https://github.com/apache/iceberg/pull/16376) | `gradle-wrapper` | 8.14.4 → 8.14.5 | Open | dependabot 自动 |

### 已关闭/撤回 PR

| PR | 标题 | 备注 |
|----|------|------|
| [#16386](https://github.com/apache/iceberg/pull/16386) | Core: Enforce that v4 manifests do not contain POSITION_DELETES | 当日开当日关，可能已重新提交 |
| [#16385](https://github.com/apache/iceberg/pull/16385) | Aliyun, Dell: Classify missing object reads as NotFoundException | 当日开当日关，可能已重新提交 |

---

## 🐛 当日新增 Issue 分析

---

### Issue #16387 — `Kafka Connect: Enable Parquet variant shredding for generic Record writes`

| 字段 | 内容 |
|------|------|
| **Issue 链接** | https://github.com/apache/iceberg/issues/16387 |
| **作者** | soumilshah199500 |
| **状态** | Open |
| **标签** | `bug` |
| **组件** | Kafka Connect |

**问题描述**

在 Kafka Connect sink connector 将数据写入 Iceberg 表时，针对 **Parquet Variant Shredding**（变体列分拆）功能的支持存在缺失。具体而言，当使用 generic Record（Avro 通用记录格式）进行写入时，variant shredding 优化路径未被激活，导致变体数据以未优化的方式存储，影响查询性能。

**Parquet Variant Shredding 背景：**
```
Variant 类型（Apache Parquet 标准）
  ├── 传统存储: 所有变体数据存为二进制 blob
  └── Shredding 优化: 将已知字段提取为独立列
       └── 效果: 大幅提升变体数据的过滤和聚合性能
```

---

### Issue #16380 — `Kafka Connect: Add end-to-end test for commit failure propagation through CoordinatorThread`

| 字段 | 内容 |
|------|------|
| **Issue 链接** | https://github.com/apache/iceberg/issues/16380 |
| **作者** | yadavay-amzn (Amazon 贡献者) |
| **状态** | Open |
| **标签** | — |
| **组件** | Kafka Connect |

**问题描述**

Kafka Connect 的 `CoordinatorThread` 负责协调事务提交。当提交失败时，故障应当正确传播并触发重试/告警机制。目前缺少针对此故障传播路径的**端到端测试**，存在回归风险。

**需要覆盖的测试场景：**
```
Kafka Connect Sink
  └── CoordinatorThread.commit()
       ├── 正常路径: commit → success → offset commit
       └── 故障路径: commit → failure
                        ├── 当前: 行为未经测试验证 ⚠️
                        └── 期望: failure → propagate → retry/error reporter
```

> **注意：** 两个新 issue 都集中在 **Kafka Connect** 组件，表明该组件正处于活跃的开发/测试阶段（与近期多个 Kafka Connect 相关 PR 合并一致）。

---

## 📈 趋势分析

### 近期组件活跃度

```
组件活跃度 (基于近 7 天合并 PR 统计)

Spark          ██████████████████████  ★★★★★  (高)
Flink          █████████████████       ★★★★☆  (较高)
Build/Infra    █████████████████████   ★★★★★  (高)
Core           ████████                ★★★☆☆  (中)
Kafka Connect  ███                     ★★☆☆☆  (活跃上升)
Website/Docs   ██                      ★★☆☆☆  (中)
API            ██                      ★★☆☆☆  (中)
```

### 本周重要合并（近 7 天上下文）

| PR | 标题 | 重要性 |
|----|------|--------|
| #16295 | Spark 4.1: Migrate SparkCopyOnWriteScan to SupportsRuntimeV2Filtering | 🔴 高 |
| #16088 | Spark 4.1: Add session configs for adaptive split sizing | 🔴 高 |
| #16263 | Core: Fix row ID assignment for EXISTING entry during manifest merge | 🔴 高 |
| #16097 | Flink: Support UUID type in Avro and Parquet readers and writers | 🟡 中 |
| #16268 | Flink: Nanosecond gaps in SortKeySerializer | 🟡 中 |
| #16257 | Build: Bump to Parquet 1.17.1 | 🟡 中 |
| #16167 | Core: Fix ByteBufferInputStream EOF behavior | 🟡 中 |

---

## 🔧 操作日志

```bash
# Fork 同步操作记录
时间: 2026-05-18 (每日自动同步)
操作: git fetch upstream main
      git checkout main
      git merge upstream/main --ff-only  (fast-forward: 6976e02 → 6a3f782)
      git push -u origin main

结果:
  ✅ upstream/main 拉取成功 (21 文件, +95/-15 行)
  ✅ origin/main 推送成功
  ✅ 工作分支 claude/happy-ride-SSLCq 未受影响
```

---

*报告生成时间：2026-05-18 | 工具：Claude Code + git log + GitHub Web*
