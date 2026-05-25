# Apache Iceberg 每日动态分析报告

**日期：** 2026-05-24  
**报告范围：** 2026-05-24 00:00:00 UTC — 2026-05-24 23:59:59 UTC  
**上游仓库：** [apache/iceberg](https://github.com/apache/iceberg)  
**Fork 仓库：** [vinlee19/iceberg](https://github.com/vinlee19/iceberg)  
**报告生成时间：** 2026-05-25

---

## 目录

1. [Fork 同步状态](#1-fork-同步状态)
2. [数据总览](#2-数据总览)
3. [合并的 PR 深度分析](#3-合并的-pr-深度分析)
4. [新增 Issue 分析](#4-新增-issue-分析)
5. [新增 PR 分析](#5-新增-pr-分析)
6. [今日重点关注](#6-今日重点关注)

---

## 1. Fork 同步状态

| 项目 | 状态 |
|------|------|
| 上游分支 | `apache/iceberg:main` |
| Fork 分支 | `vinlee19/iceberg:main` |
| 同步状态 | ✅ 已同步（Fast-forward merge） |
| 同步文件数 | 12 个文件 |
| 新增行数 | +430 行 |
| 删除行数 | -12 行 |

**本次同步涵盖的主要文件变更：**

```
.github/workflows/codeql.yml                            (CI 配置更新)
.github/workflows/cve-scan.yml                          (安全扫描更新)
.github/workflows/zizmor.yml                            (工作流更新)
arrow/.../VectorizedParquetDefinitionLevelReader.java   (BUG FIX: INT96 时间戳)
arrow/.../TestVectorizedParquetDefinitionLevelReader.java (新增测试)
build.gradle                                            (构建配置)
core/.../SerializableTable.java                         (BUG FIX: 历史排序顺序)
core/.../SortOrderParser.java                           (排序解析器修复)
core/.../util/LocationUtil.java                         (路径工具增强)
gradle/libs.versions.toml                               (依赖版本更新)
```

---

## 2. 数据总览

```
┌─────────────────────────────────────────────────────────────────────┐
│              2026-05-24 Apache Iceberg 社区活动总览                   │
├──────────────────┬──────┬────────────────────────────────────────────┤
│ 类型             │ 数量 │ 说明                                        │
├──────────────────┼──────┼────────────────────────────────────────────┤
│ 合并 PR (Merged) │  8   │ 2 个核心修复 + 6 个依赖更新                │
│ 新增 Issue       │  1   │ 文档缺失问题                               │
│ 新增 PR (Open)   │  5   │ 2 个测试优化 + 1 个新特性 + 2 个文档/修复  │
└──────────────────┴──────┴────────────────────────────────────────────┘
```

---

## 3. 合并的 PR 深度分析

### 🔴 高优先级修复

---

#### PR #16435 — Arrow: 修复 INT96 时间戳字典解码的字节偏移量错误

| 属性 | 详情 |
|------|------|
| **PR 链接** | [#16435](https://github.com/apache/iceberg/pull/16435) |
| **作者** | @sungwy (Sung Yun) |
| **合并时间** | 2026-05-24T21:55:07Z |
| **标签** | `arrow` |
| **关联 Issue** | Fixes #13485 |
| **影响组件** | Arrow 向量化 Parquet 读取器 |

**问题背景**

当使用 Arrow 向量化读取器（`VectorizedParquetDefinitionLevelReader`）读取 **Parquet 字典编码的 INT96 时间戳**数据时，存在一个字节偏移量计算错误，导致时间戳数据被写入错误的缓冲区位置，引发**数据静默损坏**（silent data corruption）。

**根因分析**

INT96 时间戳在 Arrow 的 `FixedSizeBinary` 类型向量中，每个值占用 `typeWidth`（= 12 bytes）。在 PACKED 字典解码模式下，写入目标缓冲区时需要使用**字节偏移量**，而不是值的行索引。

```
正确的字节偏移量计算：
  字节偏移 = 行索引 × 每个值的字节宽度
  byte_offset = idx × typeWidth（12 bytes per INT96）
```

**代码修改（单行关键修复）**

```java
// 文件：arrow/src/main/java/org/apache/iceberg/arrow/vectorized/parquet/
//        VectorizedParquetDefinitionLevelReader.java
// 位置：Line ~539，TimestampInt96Reader.nextDictEncodedVal，PACKED case

// ❌ 修复前 — 使用行索引，导致所有时间戳都覆写前 8 字节
vector.getDataBuffer().setLong(idx, timestampInt96);

// ✅ 修复后 — 使用正确的字节偏移量
vector.getDataBuffer().setLong((long) idx * typeWidth, timestampInt96);
```

**影响范围**

```
受影响场景：
  - Parquet 文件包含 INT96 格式时间戳列
  - 该列使用字典编码（dictionary encoding）
  - 使用 Arrow 向量化读取器（默认开启）
  - 读取多行数据时第 2 行起数据全部错误

错误表现：
  - 行 0：正确（偶然正确，idx=0 时两种计算相同）
  - 行 1+：时间戳值错误（硬覆写 buffer 前 8 字节）
```

**新增测试**

新增 `TestVectorizedParquetDefinitionLevelReader.java`（115 行），验证多行字典编码 INT96 时间戳的正确解码：

```java
// 测试覆盖：多行 INT96 字典编码时间戳精确性验证
// 文件：arrow/src/test/java/.../TestVectorizedParquetDefinitionLevelReader.java
@Test
public void testTimestampInt96DictEncodedMultipleRows() {
    // 构造包含多个不同时间戳值的字典编码 Parquet 数据
    // 验证每一行解码值均正确，不存在覆写问题
}
```

**重要性评级：** ⭐⭐⭐⭐⭐  
此 Bug 存在时间较长（关联 Issue #13485），属于**数据正确性问题**，影响所有使用 INT96 时间戳 + 字典编码的 Parquet 文件读取场景。

---

#### PR #16521 — Core: 修复历史排序顺序中引用已删除字段导致的异常

| 属性 | 详情 |
|------|------|
| **PR 链接** | [#16521](https://github.com/apache/iceberg/pull/16521) |
| **作者** | @MonkeyCanCode (Yong Zheng) |
| **合并时间** | 2026-05-24T13:56:48Z |
| **标签** | `core` |
| **关联 Issue** | 回归问题（PR #15150 引入） |
| **里程碑** | Iceberg 1.11.1 (Backport) |

**问题背景**

从 Iceberg **1.10.1 升级到 1.11.0** 后，当表的 Schema 经历过列删除（Schema Evolution），且历史上存在引用已删除列的排序顺序（Sort Order）时，任何 Spark 写操作都会抛出异常并失败。

**根因分析**

PR #15150 修改了 `SerializableTable.sortOrders()` 的行为，使其对**所有**排序顺序（包括历史的、已废弃的）都进行严格的 Schema 绑定验证。这导致历史排序顺序中引用的已删除字段无法通过验证，抛出异常。

```
正确的语义应为：
  - 当前默认排序顺序（default sort order）: 需要严格绑定当前 Schema
  - 历史排序顺序（historical sort orders）: 只需能加载，无需验证字段是否存在
```

**代码修改**

**(1) `SerializableTable.java` — 记录默认排序顺序 ID**

```java
// 新增字段追踪默认排序顺序 ID
private final int defaultSortOrderId;

// 构造时记录
this.defaultSortOrderId = table.sortOrder().orderId();

// sortOrders() 方法中传递默认 ID，让解析器区分对待
sortOrderAsJsonMap.forEach(
    (id, json) ->
        sortOrders.put(id, SortOrderParser.fromJson(schema(), json, defaultSortOrderId)));
//  修复前：
//  sortOrders.put(id, SortOrderParser.fromJson(schema(), json)));
```

**(2) `SortOrderParser.java` — 新增支持默认 ID 的重载方法**

```java
// 新增重载：接受 defaultSortOrderId，历史排序顺序使用宽松绑定
public static SortOrder fromJson(Schema schema, String json, int defaultSortOrderId) {
    return JsonUtil.parse(json, node -> fromJson(schema, node, defaultSortOrderId));
}
```

**修复逻辑示意**

```
┌─────────────────────────────────────────────────────────┐
│  sortOrders() 遍历所有历史排序顺序                         │
│                                                         │
│  for each (id, json) in sortOrderAsJsonMap:             │
│    if id == defaultSortOrderId:                         │
│      → 严格绑定（strict bind）→ 校验字段必须存在          │
│    else:                                                │
│      → 宽松绑定（lenient bind）→ 字段不存在时不抛出异常   │
└─────────────────────────────────────────────────────────┘
```

**影响范围**

- 所有从 1.10.x 升级到 1.11.0 的用户
- 表曾执行过 `ALTER TABLE DROP COLUMN` 操作
- 表历史上存在引用被删除列的排序顺序

**重要性评级：** ⭐⭐⭐⭐⭐  
升级必现回归 Bug，会导致 Spark 写操作完全失败，已被纳入 **1.11.1 Backport 里程碑**。

---

### 🟡 依赖更新（Dependabot 自动化）

以下 6 个 PR 均由 Dependabot 机器人自动生成并在同一时间窗口内批量合并（2026-05-24T17:29～17:31 UTC）：

| PR # | 依赖包 | 版本变更 | 类型 | 重要说明 |
|------|--------|---------|------|---------|
| [#16551](https://github.com/apache/iceberg/pull/16551) | `com.google.cloud:libraries-bom` | 26.81.0 → 26.83.0 | Java 依赖 | 包含 google-cloud-datastore 升级到 3.0.0 |
| [#16550](https://github.com/apache/iceberg/pull/16550) | `zizmorcore/zizmor-action` | 0.5.3 → 0.5.6 | GitHub Actions | 安全分析工具更新 |
| [#16553](https://github.com/apache/iceberg/pull/16553) | `slf4j` | 2.0.17 → 2.0.18 | Java 依赖 | 日志门面更新 |
| [#16552](https://github.com/apache/iceberg/pull/16552) | `com.diffplug.spotless:spotless-plugin-gradle` | 8.4.0 → 8.5.1 | 构建工具 | ⚠️ 修复 LicenseHeaderStep 的 shell-injection 漏洞 |
| [#16554](https://github.com/apache/iceberg/pull/16554) | `github/codeql-action` | 4.35.4 → 4.35.5 | GitHub Actions | CI 安全扫描工具 |
| [#16555](https://github.com/apache/iceberg/pull/16555) | `software.amazon.awssdk:bom` | 2.44.4 → 2.44.7 | Java 依赖 | AWS SDK 版本更新 |

> **注意：** PR #16552（Spotless 插件升级）包含一个 **安全修复**，修复了 `LicenseHeaderStep` 中的 shell-injection 漏洞，建议关注。

---

## 4. 新增 Issue 分析

### Issue #16556 — 文档缺失：adaptive split sizing 配置项未被记录

| 属性 | 详情 |
|------|------|
| **Issue 链接** | [#16556](https://github.com/apache/iceberg/issues/16556) |
| **提交者** | @pratham76 |
| **创建时间** | 2026-05-24T09:19:23Z |
| **状态** | 已关闭（被 PR #16557 解决） |
| **标签** | `docs` |

**问题描述**

PR #16088 中添加了两个新的 Spark session 级别配置项（用于**自适应分片大小**功能），但这些配置项**未被写入官方文档**的 Spark Configuration 页面，导致用户无法发现和使用该功能。

**缺失的配置项（推测）：**

```properties
# 自适应分片大小相关配置（在 Spark Configuration 文档中缺失）
spark.sql.iceberg.split-size.adaptive.enabled = ...
spark.sql.iceberg.split-size.adaptive.target-size-bytes = ...
```

**后续处理：** 该 Issue 在同日被 PR #16557 快速响应并解决。

---

## 5. 新增 PR 分析

### PR #16548 — Flink: 修复 TestMonitorSource.testStateRestore 偶发性失败

| 属性 | 详情 |
|------|------|
| **PR 链接** | [#16548](https://github.com/apache/iceberg/pull/16548) |
| **作者** | @wombatu-kun |
| **创建时间** | 2026-05-24T04:21:31Z |
| **状态** | Open |
| **标签** | `flink` |
| **关联 Issue** | Closes #16546 |

**问题描述**

`TestMonitorSource.testStateRestore` 在 Flink v2.0/v2.1 中**间歇性失败**，抛出 `TimeoutException`（来自 `CollectingSink.poll`，5 秒超时）。表面看是超时，实质是 Savepoint 文件系统的竞态条件。

**根因**

```
时序问题：
  1. stopWithSavepoint(...) 返回 savepoint 目录路径
  2. 目录在文件系统上可见
  3. 但 state 文件尚未完全写入 ← 竞态条件在此
  4. 恢复的 Job 读取到不完整的 state，静默失败
  5. CollectingSink 队列始终为空 → 5s 超时
```

**修复方案**

```java
// 修复 1：正确 await stopWithSavepoint() 的 future，确保 savepoint 完全写入后再返回路径
// 文件：OperatorTestBase.closeJobClient()
// 修复前：不等待 future 完成
// 修复后：await future → 返回已完整写入的 savepoint 路径

// 修复 2：将 Phase 2 的 poll 超时从 5 秒提升到 30 秒
// 为繁忙 CI 环境提供足够的启动容忍时间
```

**影响范围：** Flink v2.0、v2.1（v1.20 使用不同执行模型，不受影响）

---

### PR #16549 — Spark: 精简行级操作测试参数矩阵（6→3 行）

| 属性 | 详情 |
|------|------|
| **PR 链接** | [#16549](https://github.com/apache/iceberg/pull/16549) |
| **作者** | @stevenzwu |
| **创建时间** | 2026-05-24T04:31:32Z |
| **状态** | Open |
| **标签** | `spark` |

**背景**

Spark 行级操作测试（MERGE、UPDATE、DELETE）的参数矩阵过于庞大，导致 CI 时间过长，每个测试 cell 耗时约 6-7 分钟。

**调整策略**

```
调整前（v4.0/v4.1）：6 个 catalog × 多种格式组合
调整后（v4.0/v4.1）：3 个精选行

保留的测试行：
  ✅ testhive (ORC)   — HiveMetastore 基线
  ✅ testhive (PARQUET) — 覆盖 HASH/DISTRIBUTED 轴
  ✅ testrest (REST)  — 覆盖 REST commit 路径

移除的测试行：
  ❌ testhadoop       — HadoopCatalog 非生产推荐
  ❌ spark_catalog    — 差异体现在 DDL/表解析，非行级操作
```

**额外修复：** `RESTCatalogServer` 测试 fixture 路径问题

```java
// 修复 RESTCatalogServer 返回不带 URI scheme 的路径问题
// 修复前：getAbsolutePath() → "/tmp/iceberg_warehouse"（无 file:// 前缀）
// 修复后：toURI().toString() → "file:///tmp/iceberg_warehouse"（与其他 catalog 一致）
```

**CI 影响：** 每个测试类调用次数减少约 **50%**（Spark 3.5 减少约 57%）

---

### PR #16557 — Docs: 补充 adaptive split sizing 配置文档

| 属性 | 详情 |
|------|------|
| **PR 链接** | [#16557](https://github.com/apache/iceberg/pull/16557) |
| **作者** | @pratham76 |
| **创建时间** | 2026-05-24T09:29:11Z |
| **状态** | Open（等待 Review） |
| **标签** | `docs` |
| **关联 Issue** | Closes #16556 |

**内容：** 为 PR #16088 中引入的自适应分片大小配置项补充官方 Spark Configuration 文档，包含配置项说明、默认值和使用示例。

---

### PR #16558 — Kafka Connect: 新增 Topic 到 Table 的静态映射配置

| 属性 | 详情 |
|------|------|
| **PR 链接** | [#16558](https://github.com/apache/iceberg/pull/16558) |
| **作者** | @igorvoltaic |
| **创建时间** | 2026-05-24T10:07:42Z |
| **状态** | Open |
| **标签** | `docs`, `KAFKACONNECT` |

**功能描述**

引入新配置项 `iceberg.tables.topic-to-table-mapping`，允许用户显式配置 Kafka Topic 到 Iceberg Table 的静态路由映射。

**配置示例**

```properties
# 新增配置：静态 Topic → Table 映射
iceberg.tables.topic-to-table-mapping=\
  some_topic0:catalog.db.table_name0,\
  some_topic1:catalog.db.table_name1,\
  sensor_data:warehouse.iot.sensors
```

**设计参考**

此方案遵循以下 Kafka Sink 连接器的既有标准：
- Snowflake Sink Connector
- ClickHouse Sink Connector  
- Aiven JDBC Sink Connector

**超越 PR #10422：** 此 PR 替代了之前因 GitHub 分支保护策略而无法重开的旧 PR #10422。

---

### PR #16559 — Spark: 精简流式读取测试参数矩阵（8→2 行）

| 属性 | 详情 |
|------|------|
| **PR 链接** | [#16559](https://github.com/apache/iceberg/pull/16559) |
| **作者** | @stevenzwu |
| **创建时间** | 2026-05-24T23:33:23Z |
| **状态** | Open |
| **标签** | `spark` |

**背景**

`TestStructuredStreamingRead3` 是 Spark core CI Job 中 **CPU 消耗最高**的测试类，占据总 CPU 时间的 **20.3%**（约 931 CPU-秒/4595 总计）。

**调整方案**

```
调整前：4 catalogs × 2 async modes = 8 行
调整后：2 行（战略性覆盖）

保留：
  ✅ testhive (async=true)  — HiveMetastore 生产基线
  ✅ testrest (async=false) — REST 开源标准路径

移除：
  ❌ testhadoop             — 非生产推荐
  ❌ spark_catalog          — DDL 差异，不影响流式读取语义

理由：流式读取语义在 Iceberg 表加载后与 Catalog 无关
```

**CI 影响：** 总调用次数从 **264 降至 66**（减少 **75%**），预计节省大量 CI 时间。

---

## 6. 今日重点关注

### 🚨 需要立即关注

| 优先级 | 内容 | 行动建议 |
|--------|------|---------|
| P0 | **PR #16521**: `SerializableTable.sortOrders()` 回归修复 | 若使用 Iceberg 1.11.0 且有列删除历史，立即关注 1.11.1 发布 |
| P0 | **PR #16435**: Arrow INT96 时间戳数据静默损坏修复 | 检查是否使用 Arrow 向量化读取器 + INT96 时间戳 + 字典编码 Parquet |

### ⚠️ 安全相关

| PR | 内容 |
|----|------|
| PR #16552 | Spotless 8.5.1 修复 `LicenseHeaderStep` 的 shell-injection 漏洞 |

### 📊 CI 优化趋势

今日有 **3 个 PR** 专注于 Spark 测试矩阵优化（#16549、#16559）和 Flink 测试稳定性（#16548），体现出社区正在主动控制 CI 成本并提升测试可靠性。

### 📋 待合并 PR 关注列表

| PR | 描述 | 推荐关注原因 |
|----|------|------------|
| [#16558](https://github.com/apache/iceberg/pull/16558) | Kafka Connect Topic 静态映射 | 重要新特性，影响 Kafka Connect 用户 |
| [#16548](https://github.com/apache/iceberg/pull/16548) | Flink 测试稳定性修复 | 修复 flaky test，提升 CI 可靠性 |
| [#16557](https://github.com/apache/iceberg/pull/16557) | 自适应分片文档补充 | 文档完整性，影响用户体验 |

---

## 附录：今日组件活动热图

```
组件活动分布（2026-05-24）：

Arrow        ████████████ 1 个修复（数据正确性）
Core         ████████████ 1 个修复（序列化回归）
Build/Infra  ████████████████████████ 6 个依赖更新
Spark        ████████████████████ 2 个 PR（测试优化）
Flink        ████████████ 1 个 PR（测试稳定性）
Kafka Conn.  ████████████ 1 个 PR（新特性）
Docs         ████████ 1 个 Issue + 1 个 PR（文档补充）
```

---

*报告由 Claude Code 自动生成 | 数据来源：apache/iceberg GitHub 仓库*  
*Fork 同步分支：`vinlee19/iceberg:main` ← `apache/iceberg:main`*
