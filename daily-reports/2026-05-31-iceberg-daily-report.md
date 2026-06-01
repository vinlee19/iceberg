# Apache Iceberg 每日动态报告

**报告日期：** 2026-06-01（分析范围：2026-05-31）  
**数据来源：** [apache/iceberg](https://github.com/apache/iceberg)  
**Fork 同步状态：** ✅ 已同步至最新 commit `67cbc1a`

---

## 📋 目录

1. [Fork 同步状态](#-fork-同步状态)
2. [2026-05-31 合并的 PR（8 个）](#-2026-05-31-合并的-pr)
3. [近期重要功能 PR 深度分析（2026-05-28~05-30）](#-近期重要功能-pr-深度分析)
4. [2026-05-31 新增 Issue（1 个）](#-2026-05-31-新增-issue)
5. [2026-05-31 新提交 PR（3 个）](#-2026-05-31-新提交-pr)
6. [趋势总结](#-趋势总结)

---

## 🔄 Fork 同步状态

```
同步前 HEAD：f12e0cfd07c50ef42f5785c7e8f938009128c02c
同步后 HEAD：67cbc1aeabbce52d618f8e3c6fc34747f6695a8d

变更统计：65 个文件, +1506 行, -487 行
```

本次同步新增内容：
- `SECURITY-THREAT-MODEL.md` — Iceberg 安全威胁模型新文档（260 行）
- `AGENTS.md` — Agent 使用文档更新
- Flink 动态 Sink 路由修复（v1.20 / v2.0 / v2.1 三版本）
- ORC Partition Statistics 扫描测试
- Spark 行级操作测试精简
- OpenAPI 规范扩展（新增 unregister table 端点等）
- AWS SDK、Netty、Docker Action 等依赖更新

---

## ✅ 2026-05-31 合并的 PR

> 当日共合并 **8 个 PR**，全部为依赖升级（Dependabot 自动提交）

### 📊 依赖升级汇总表

| PR # | 依赖包 | 升级版本 | 合并时间 (UTC) | 类型 |
|------|--------|---------|--------------|------|
| [#16628](https://github.com/apache/iceberg/pull/16628) | `io.netty:netty-buffer` | `4.2.13.Final` → `4.2.14.Final` | 05:52 | Java |
| [#16636](https://github.com/apache/iceberg/pull/16636) | `org.immutables:value` | `2.12.1` → `2.12.2` | 05:53 | Java |
| [#16635](https://github.com/apache/iceberg/pull/16635) | `github/codeql-action` | `4.35.5` → `4.36.0` | 05:54 | GitHub Actions |
| [#16629](https://github.com/apache/iceberg/pull/16629) | `openapi-spec-validator` | `0.8.5` → `0.9.0` | 15:41 | Python / OpenAPI |
| [#16630](https://github.com/apache/iceberg/pull/16630) | `actions/stale` | `10.2.0` → `10.3.0` | 15:41 | GitHub Actions |
| [#16631](https://github.com/apache/iceberg/pull/16631) | `docker/build-push-action` | `7.1.0` → `7.2.0` | 15:42 | Docker CI |
| [#16632](https://github.com/apache/iceberg/pull/16632) | `docker/setup-buildx-action` | `4.0.0` → `4.1.0` | 15:42 | Docker CI |
| [#16634](https://github.com/apache/iceberg/pull/16634) | `software.amazon.awssdk:bom` | `2.44.7` → `2.44.12` | 15:42 | AWS SDK |

#### 🔍 重要依赖升级说明

**`software.amazon.awssdk:bom` 2.44.7 → 2.44.12（#16634）**  
AWS SDK for Java 的 BOM 跨越了 5 个小版本，包含多处安全补丁和性能改进。对于所有使用 S3、Glue、DynamoDB 等 AWS 服务作为 Iceberg catalog 的用户有直接影响。

**`io.netty:netty-buffer` 4.2.13 → 4.2.14（#16628）**  
Netty 的 buffer 模块升级，Iceberg 使用 Netty 进行 REST catalog 的 HTTP 通信，该升级可带来潜在的内存管理优化。

**`github/codeql-action` 4.35.5 → 4.36.0（#16635）**  
CodeQL 静态安全分析 Action 升级，提升代码安全扫描能力。

**`docker/build-push-action` + `docker/setup-buildx-action`（#16631, #16632）**  
Docker 构建工具链升级，影响 iceberg-rest-fixture 的 Docker 镜像发布流水线。

---

## 🌟 近期重要功能 PR 深度分析

> 覆盖范围：2026-05-28 ~ 2026-05-30，共 **8 个功能 PR**，按重要性排序分析

---

### 🐛 [#16023] Core: 修复 OAuth token 刷新时 optionalOAuthParams 丢失问题

**合并时间：** 2026-05-30 19:30 UTC  
**作者：** [@bharos](https://github.com/bharos)  
**标签：** `core`  
**PR 链接：** https://github.com/apache/iceberg/pull/16023

#### 问题背景

使用 REST Catalog 且启用 OAuth 认证的用户遭遇了一个严重 Bug：**首次 token 获取成功，但 token 过期后刷新失败（收到 401/403 错误）**。

#### 根本原因

```
OAuth 认证参数（audience, resource, scope）
    │
    ├── 首次 fetchToken() ──> 正确传入 optionalOAuthParams ✅
    │
    └── token 过期刷新路径：
        ├── refreshExpiredToken()  ──> 传入 ImmutableMap.of() ❌
        └── static refreshToken() ──> 传入 Map.of()          ❌
```

问题根源在于 PR #14059 删除了带 5 个参数的废弃 `fetchToken()` 重载时，将 `optionalOAuthParams` 替换成了空 Map，而不是转发实际参数。

#### 修复方案

```java
// 修复前（错误）
fetchToken(client, headers, ImmutableMap.of(), credential, scope, tokenEndpoint);

// 修复后（正确）
fetchToken(client, headers, optionalOAuthParams, credential, scope, tokenEndpoint);
```

**修复涉及两处非 exchange 分支：**
1. `refreshExpiredToken()` — token 过期时的刷新
2. 静态 `refreshToken()` — 主动预刷新路径

#### 测试覆盖

新增两个测试在 `TestOAuth2Util`：
- 验证 token 过期刷新时 `audience` 出现在表单数据中
- 验证主动刷新路径同样包含 `audience`

#### 影响范围

> ⚠️ **高影响**：所有使用 REST Catalog + OAuth 认证（非 token exchange 模式）且依赖 `audience`、`resource`、`scope` 参数的用户，升级前均受此 Bug 影响。

---

### ⚡ [#16208] Core: 缓存 PartitionData 模板大幅提升元数据扫描性能

**合并时间：** 2026-05-28 14:27 UTC  
**作者：** [@Wenjun7J](https://github.com/Wenjun7J)  
**标签：** `core`  
**PR 链接：** https://github.com/apache/iceberg/pull/16208

#### 性能数据对比

| 指标 | 修复前 | 修复后 | 提升幅度 |
|------|--------|--------|---------|
| 平均执行时间 | 12.71 秒 | 5.24 秒 | **↓ 58.8%** |
| 最大内存占用 | 5.66 GiB | 1.41 GiB | **↓ 75.0%** |

*测试场景：20,000 个分区值，4 个分区列，重复全量扫描*

#### 问题根因

`PartitionsTable` 扫描分区元数据表时，**每行都会重新创建一个 `PartitionData` 对象**，触发完整的 Avro Schema 转换：

```
每个分区行循环执行：
  new PartitionData(partitionType)
    └── 调用 AvroSchemaUtil.convert()
          └── 调用 TypeToSchema$WithTypeToName.struct()
                └── 产生大量对象分配 + GC 压力
```

对于拥有数万分区的大型表，这造成严重的内存分配压力。

#### 修复方案

```java
// 修复前：每个分区都重新创建
PartitionData data = new PartitionData(partitionType);
data.set(...);

// 修复后：复用模板，通过 copyFor(key) 填充数据
PartitionData template = new PartitionData(partitionType); // 创建一次
// 扫描循环中：
PartitionData data = template.copyFor(partitionKey);      // 仅复制数据
```

#### 影响范围

> ⚡ **高价值优化**：对元数据表扫描（如 `SELECT * FROM catalog.db.table.partitions`）有显著加速效果，分区数越多收益越大。

---

### 🔧 [#16243 + #16597] Flink: 修复 Dynamic Sink 中 Identifier 字段路由错误

**合并时间：**
- 主 PR #16243：2026-05-28 05:07 UTC（Flink 2.1）
- 反向移植 #16597：2026-05-28 15:56 UTC（Flink 1.20 + 2.0）

**作者：** [@jordepic](https://github.com/jordepic)  
**标签：** `flink`, `docs`  
**PR 链接：** https://github.com/apache/iceberg/pull/16243

#### 问题描述

Flink Dynamic Sink 在使用 **Equality Delete** 时存在数据一致性 Bug，可能导致**重复数据无法被正确删除**：

```
Bug 1 - HashKeyGenerator 路由错误：
  表只设置了 identifier fields（无显式 equality fields）
        ↓
  HashKeyGenerator 未识别 identifier 字段 → round-robin 分发
        ↓
  相同 key 的两行被分发到不同 writer subtask
        ↓
  equality delete 无法正确删除重复行 ❌

Bug 2 - DynamicRecordProcessor 转发错误：
  distributionMode 为 null 的记录 → 直接转发给 writer
        ↓
  未检查是否有 equality fields → 相同 key 可能分散到多个 writer
        ↓
  留下重复数据 ❌
```

#### 修复方案

在 `DynamicSinkUtil` 中**集中处理** equality field 解析逻辑：

```java
// 新增集中解析方法
DynamicSinkUtil.resolveEqualityFieldNames(schema, userProvidedFields)
  // 如果 userProvidedFields 为空，自动回退到 schema.identifierFieldIds()

// HashKeyGenerator 和 DynamicRecordProcessor 都使用此方法
// 保证 distribution 决策与 write-side equality field 推断一致
```

同时更新了 `flink-writes.md` 文档，说明 identifier field 的处理逻辑。

#### 影响范围

> ⚠️ **高影响**：使用 Flink Dynamic Sink + Equality Delete 且依赖 `schema identifier fields` 的用户，在 Flink 1.20、2.0、2.1 上均受此 Bug 影响。

---

### 🔒 [#16586] Core: 修复 JsonUtil.getStringArray 非字符串元素无声转换

**合并时间：** 2026-05-30 22:35 UTC  
**作者：** [@stevenzwu](https://github.com/stevenzwu)  
**标签：** `core`  
**PR 链接：** https://github.com/apache/iceberg/pull/16586

#### 问题描述

`JsonUtil.getStringArray(JsonNode)` 对每个元素调用 `asText()` 而不验证其是否为 JSON 字符串。这意味着：

```json
// 非法输入（包含数字和布尔值）
["valid", 45, true, "another"]

// 修复前：静默转换为
["valid", "45", "true", "another"]  // 掩盖了错误！

// 修复后：立即抛出异常
// "Cannot parse string from non-text value: 45"
```

#### 涉及的三个调用点

| 调用位置 | 解析的字段 | 为何非字符串是错误 |
|---------|-----------|-----------------|
| `ViewVersionParser.fromJson` | `default-namespace` | View spec 定义为字符串列表 |
| `RESTSerializers.NamespaceDeserializer` | Namespace 数组 | REST OpenAPI 定义为 `array<string>` |
| `RemoteSignRequestParser.headersFromJson` | HTTP header 值 | Header 值必须是字符串 |

#### 修复内容

```java
// 修复前
for (JsonNode element : arrayNode) {
    result.add(element.asText()); // 危险：静默转换
}

// 修复后
for (JsonNode element : arrayNode) {
    Preconditions.checkArgument(
        element.isTextual(),
        "Cannot parse string from non-text value: %s", element);
    result.add(element.asText());
}
```

---

### 🔁 [#16616] Spark: 废弃 SparkFilters，推荐使用 SparkV2Filters

**合并时间：** 2026-05-30 22:37 UTC  
**作者：** [@huaxingao](https://github.com/huaxingao)  
**标签：** `spark`  
**PR 链接：** https://github.com/apache/iceberg/pull/16616

#### 背景

`SparkFilters` 用于将 Spark **DSv1** `Filter[]` API 转换为 Iceberg `Expression`。

在所有受支持的 Spark 版本（3.5、4.0、4.1）中，**生产代码已全部迁移到** `SparkV2Filters`（处理 DSv2 `Predicate[]`），仅有 `TestSparkFilters` 仍引用旧类。

#### 变更内容

```java
// spark/v3.5, spark/v4.0, spark/v4.1 均添加废弃注解
@Deprecated
// Deprecated since 1.11.0, to be removed in 1.12.0.
// Use {@link SparkV2Filters} instead.
public class SparkFilters {
    ...
}

// 测试类添加抑制警告，使现有测试继续运行
@SuppressWarnings("deprecation")
public class TestSparkFilters {
    ...
}
```

#### 迁移指南

| 废弃 | 替代 |
|------|------|
| `SparkFilters.convert(Filter[])` | `SparkV2Filters.convert(Predicate[])` |

> 计划在 Iceberg **1.12.0** 版本彻底移除 `SparkFilters`。

---

### 📊 [#16569] API/Core/ORC: 为 Partition Statistics Scan API 实现 project() 方法

**合并时间：** 2026-05-29 12:22 UTC  
**作者：** [@gaborkaszab](https://github.com/gaborkaszab)  
**标签：** `core`, `ORC`  
**PR 链接：** https://github.com/apache/iceberg/pull/16569

#### 功能概述

为分区统计信息扫描 API 实现了**列投影（Column Projection）**支持，允许调用方只读取所需的列，而不必加载所有分区统计字段。

#### 影响的模块

- `API`：`PartitionStatisticsScan` 接口新增 `project()` 方法
- `Core`：`BasePartitionStatisticsScan` 实现投影逻辑
- `ORC`：`TestOrcPartitionStatisticsScan` 新增投影测试
- 新增通用测试基类 `PartitionStatisticsScanTestBase`

---

### 🔀 [#16593] Spark: IcebergSortCompactionBenchmark 重构为继承基类

**合并时间：** 2026-05-29 14:36 UTC  
**作者：** [@varun-lakhyani](https://github.com/varun-lakhyani)  
**标签：** `spark`  
**PR 链接：** https://github.com/apache/iceberg/pull/16593

**简述：** 将 `IcebergSortCompactionBenchmark` 重构为继承 `IcebergCompactionBenchmark` 基类（在 #16219 中引入），减少重复代码，功能不变。

---

### 🏗️ [#16538] Docs: 发布 Iceberg 安全威胁模型文档

**合并时间：** 2026-05-28 UTC  
**标签：** `docs`  
**PR 链接：** https://github.com/apache/iceberg/pull/16538

新增 `SECURITY-THREAT-MODEL.md`（260 行），正式发布 Iceberg 的安全威胁模型，并在 `site/docs/security.md` 中添加链接。

---

## 🐞 2026-05-31 新增 Issue

### [#16640] [Spark] Azure 禁用 Hadoop FS 缓存时 Parquet 写入卡死

**提交时间：** 2026-05-31 19:18 UTC  
**状态：** Open  
**标签：** `bug`  
**Issue 链接：** https://github.com/apache/iceberg/issues/16640

#### 问题描述

在 **Azure Blob Storage + Spark + Iceberg** 场景下，当设置 `fs.abfs.impl.disable.cache=true`（禁用 Hadoop FS 缓存）时，写入任务**随机卡死**，错误信息如下：

```
ERROR o.a.h.util.BlockingThreadPoolExecutorService - Could not submit task to executor
java.util.concurrent.ThreadPoolExecutor@24ad434d
[Terminated, pool size = 0, active threads = 0, queued tasks = 0, completed tasks = 0]
```

#### 根本原因分析

```
时间线：
  [Executor 线程] 开始写 Parquet 数据
        ↓
  [Finalizer 线程] AzureBlobFileSystem 对象被 GC 回收！
        DEBUG: finalize() called
        DEBUG: 正在关闭 BlockingThreadPoolExecutorService（等待最长 30s）
        ↓
  [Executor 线程] Parquet 尝试写入 footer
        ERROR: 线程池已终止，无法提交任务！
```

**关键原因：** 禁用 FS 缓存后，`AzureBlobFileSystem` 实例没有被强引用持有，被 GC 回收，其内部的 `BlockingThreadPoolExecutorService` 也被关闭。而 Parquet 仍持有该 FS 的弱引用并尝试继续写入。

#### 复现环境

| 组件 | 版本 |
|------|------|
| Apache Spark | 3.5.8 |
| Apache Iceberg | 1.11.0 |
| hadoop-azure | 3.3.6 |
| Java | 17.0.18 |

**复现 JVM 参数：** `-Xmx480m -XX:G1ReservePercent=50`（限制内存以触发 GC）

#### 复现代码

```java
// 配置禁用 FS 缓存
.config("fs.abfs.impl.disable.cache", "true")

// 循环插入触发多次 GC
for (int i = 0; i < 20; i++) {
    spark.sql("INSERT INTO test_table_x VALUES (1, 'a'), ...");
}
```

#### 建议关注

该问题影响 Azure 存储用户的生产稳定性，特别是在内存受限环境下。潜在修复方向可能需要 Iceberg 在 Parquet Writer 中持有 FS 对象的强引用，或在写入完成前阻止 GC 回收。

---

## 📝 2026-05-31 新提交 PR

### [#16627] Arrow: 修复高精度 Decimal 读取截断问题

**提交时间：** 2026-05-31 04:30 UTC  
**状态：** Open（待 Review）  
**作者：** [@wombatu-kun](https://github.com/wombatu-kun)  
**标签：** `arrow`  
**PR 链接：** https://github.com/apache/iceberg/pull/16627

#### 🔴 严重程度：高（数据静默损坏）

#### 问题描述

通过向量化 Arrow 读取器读取**精度大于 18 的 Decimal 类型**时，会**静默地损坏数据**，且不抛出任何异常。

```
decimal(38, 0) 的大值读取示例：

期望结果: 12345678901234567890
实际结果: -6101065172474983726  ← 完全错误！
```

#### 根本原因

```java
// 问题代码路径
FixedSizeBinaryVector → binary accessor → JavaDecimalFactory.ofBigDecimal(value)

// ofBigDecimal 内部：
BigDecimal.valueOf(
    value.unscaledValue().longValue(),  // ← 罪魁祸首！
    scale
)
// BigInteger.longValue() 仅保留低 64 位
// 对于超过 Long.MAX_VALUE 的值，高位被截断！
```

数据流图：
```
Parquet FIXED_LEN_BYTE_ARRAY
        ↓ 正确解码
  BigDecimal(正确的 unscaled value)
        ↓ 不必要的往返转换
  bigDecimal.unscaledValue().longValue()  ← 截断！
        ↓
  BigDecimal.valueOf(截断的 long, scale)  ← 错误结果
```

#### 修复方案

```java
// 修复前（多余的往返转换，导致截断）
return BigDecimal.valueOf(value.unscaledValue().longValue(), scale);

// 修复后（直接返回已正确的 BigDecimal）
return value;  // value 已经是正确的 BigDecimal，直接返回
```

#### 影响范围

| Decimal 精度 | 是否受影响 |
|-------------|----------|
| ≤ 18 位（INT32/INT64 存储）| ❌ 不受影响 |
| > 18 位（FIXED_LEN_BYTE_ARRAY）| ✅ 受影响 |

> ⚠️ **重要**：此 Bug 导致读取 `decimal(19,x)` 到 `decimal(38,x)` 类型大值时静默损坏，现有测试（使用 `decimal(9,2)`）无法覆盖此场景。

---

### [#16639] Parquet: 为多列禁用统计信息添加测试覆盖

**提交时间：** 2026-05-31 14:14 UTC  
**状态：** Open（待 Review）  
**作者：** [@algojogacor](https://github.com/algojogacor)  
**标签：** `parquet`  
**PR 链接：** https://github.com/apache/iceberg/pull/16639

#### 背景

Issue #15347 报告了一个 Bug：当对多个列同时禁用统计信息时（例如同时设置 `write.parquet.stats-enabled.column.foo=false` 和 `write.parquet.stats-enabled.column.bar=false`），第二列可能仍会写入统计信息。

#### 新增测试

```java
// 测试 1：部分列禁用统计
testColumnStatisticsDisabledMultipleColumns()
// Schema: 3列（int, string, double）
// col1: 启用统计, col2 & col3: 禁用统计
// 验证: col2 和 col3 的 ColumnChunkMetaData.statistics 为空

// 测试 2：全部列禁用统计
testColumnStatisticsDisabledAllColumns()
// Schema: 2列
// 全部禁用统计
// 验证: 所有列的统计信息为空
```

---

### [#16638] Data: 将 reader default value 测试移入 TCK（Draft）

**提交时间：** 2026-05-31 13:02 UTC  
**状态：** Draft（草稿）  
**作者：** [@joyhaldar](https://github.com/joyhaldar)  
**标签：** `data`  
**PR 链接：** https://github.com/apache/iceberg/pull/16638

#### 变更内容

将 `DataTestBase` 中的 reader default value 测试移入 `BaseFormatModelTests`（Base Format model TCK），提升测试覆盖的通用性：

- `testDefaultValues`
- `testNullDefaultValue`
- `testNestedDefaultValue`
- `testMapNestedDefaultValue`
- `testListNestedDefaultValue`
- `testMissingRequiredWithoutDefault`

---

## 📈 趋势总结

### 本周重点方向

```
┌─────────────────────────────────────────────────────────┐
│              Apache Iceberg 本周活动趋势                  │
├─────────────────┬──────────────────────────────────────┤
│ 方向            │ 代表 PR                               │
├─────────────────┼──────────────────────────────────────┤
│ 🔒 安全 & 认证  │ #16023 OAuth token 刷新 Bug 修复      │
│ ⚡ 性能优化     │ #16208 PartitionsTable 扫描性能 ↑75%  │
│ 🔧 数据正确性   │ #16243 Flink Dynamic Sink 路由修复    │
│                 │ #16627 Arrow Decimal 截断修复（待合）  │
│ 📝 文档 & 安全  │ #16538 安全威胁模型文档               │
│ 🔁 API 演进     │ #16616 SparkFilters 废弃              │
│                 │ #16569 Partition Stats 投影 API       │
│ 🏗️ 构建依赖     │ 8 个 Dependabot 依赖升级              │
└─────────────────┴──────────────────────────────────────┘
```

### 关键指标

| 指标 | 数值 |
|------|------|
| 2026-05-31 合并 PR 数 | 8（全为依赖升级） |
| 2026-05-31 新增 Issue 数 | 1 |
| 2026-05-31 新增 PR 数 | 3 |
| 近 3 天合并功能 PR 数 | 8 |
| Fork 同步状态 | ✅ 最新 |

### 需要重点关注

1. **[#16627] Arrow Decimal 数据损坏** — 精度 > 18 的 Decimal 静默损坏，建议在合并前详细测试
2. **[#16640] Azure FS 缓存禁用卡死** — 生产稳定性问题，Azure 用户需关注
3. **[#16616] SparkFilters 废弃** — 如有使用 `SparkFilters` 的代码，需在 1.12.0 前迁移至 `SparkV2Filters`

---

*报告生成时间：2026-06-01 | 下次同步计划：2026-06-02 00:00 UTC*
