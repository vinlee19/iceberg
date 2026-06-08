# Apache Iceberg 每日动态分析报告
## 📅 日期：2026-06-07

> **Fork 同步状态：** ✅ 已同步 — `vinlee19/iceberg:main` 已追平 `apache/iceberg:main`，共合并 14 个新提交。

---

## 目录

1. [概览统计](#概览统计)
2. [已合并 PR 深度分析](#已合并-pr-深度分析)
   - [PR #16327 — Parquet 行组大小追踪（功能增强）](#pr-16327)
   - [PR #16691 — 过期快照元数据清理优化（性能优化）](#pr-16691)
   - [PR #15180 — REST Catalog 函数端点 OpenAPI 规范（新功能）](#pr-15180)
   - [依赖升级类 PR（#16702~#16706）](#依赖升级)
3. [新增 PR 汇总](#新增-pr-汇总)
4. [新增 Issue 分析](#新增-issue-分析)
5. [趋势与关注点](#趋势与关注点)

---

## 概览统计

| 指标 | 数量 |
|------|------|
| 已合并 PR | **8** |
| 新建 PR | **13** |
| 新增 Issue | **1** |
| 功能性合并 PR | **3** |
| 依赖升级 PR | **5** |

```
2026-06-07 合并活动时间线
──────────────────────────────────────────────────────────
00:00                  12:00                      24:00
  │                      │                          │
  ├─ 01:01 #15180 REST spec 函数端点          (huaxingao)
  ├─ 01:14 #16691 快照元数据清理优化      (zhongyujiang)
  ├─ 04:58 #16327 Parquet 行组大小追踪         (nssalian)
  ├─ 16:40 #16702 jackson-bom 升级          (dependabot)
  ├─ 16:41 #16703 nessie 升级               (dependabot)
  ├─ 16:41 #16704 docker/setup-qemu 升级    (dependabot)
  ├─ 16:41 #16705 shadow-gradle 升级        (dependabot)
  └─ 16:42 #16706 spotless 升级             (dependabot)
──────────────────────────────────────────────────────────
```

---

## 已合并 PR 深度分析

### PR #16327

## 🔧 Parquet：新增可选的未压缩行组大小追踪

| 字段 | 内容 |
|------|------|
| **PR 编号** | [#16327](https://github.com/apache/iceberg/pull/16327) |
| **作者** | `nssalian` |
| **合并时间** | 2026-06-07 04:58 UTC |
| **标签** | `parquet` `core` `docs` |
| **关联 Issue** | #16325 |

### 问题背景

```
┌─────────────────────────────────────────────────────────────┐
│                      问题根因                                │
│                                                              │
│  ParquetWriter.checkSize() 使用【压缩后字节数】              │
│  与【未压缩大小限制】进行比较                                │
│                                                              │
│  write.parquet.row-group-size = 128MB (未压缩限制)          │
│                                                              │
│  GZIP/ZSTD 压缩后：实际 40MB → 触发继续写入                │
│  → 行组无限增长，超出预期内存占用和读取性能边界              │
└─────────────────────────────────────────────────────────────┘
```

使用 GZIP、ZSTD 等压缩编解码器时，`ParquetWriter.checkSize()` 方法将**压缩后**字节数与**未压缩**大小限制进行比较，导致行组持续增长，远超 `write.parquet.row-group-size` 所设置的上限，影响查询性能和内存稳定性。

### 解决方案

新增一个可选配置项，通过追踪未压缩大小来准确触发行组刷新：

```
新增配置项：
  write.parquet.row-group-size-check-uncompressed = false (默认关闭)

工作原理：
  每次 model.write() 调用前后测量 buffer 大小差值
  → 数据在未压缩列缓冲区中累积（无页面刷新）
  → delta 即为精确的未压缩记录大小
  → 累积值达到阈值时触发行组 flush
```

### 核心实现逻辑

```
写入每条记录的流程（启用后）：
┌──────────────────────────────────────────────────────────┐
│  before = getBufferedSize()   ← 迭代所有列写入器        │
│  model.write(record)                                     │
│  after  = getBufferedSize()                              │
│  uncompressedSize += (after - before)                    │
│                                                          │
│  if uncompressedSize >= rowGroupSizeLimit:               │
│      flushRowGroup()           ← 精确控制行组大小        │
└──────────────────────────────────────────────────────────┘
```

**性能说明：** `getBufferedSize()` 每条记录调用一次，会遍历列写入器，与 `parquet-mr` 的 `ColumnWriteStoreBase.sizeCheck()` 模式相同。默认关闭保持向后兼容。

### 修改文件

| 文件 | 修改内容 |
|------|----------|
| `TableProperties.java` | 新增 `PARQUET_ROW_GROUP_SIZE_CHECK_UNCOMPRESSED` 常量 |
| `parquet/ParquetWriter.java` | 主要实现，记录级别大小追踪逻辑 |
| `parquet/TestParquetDataWriter.java` | 参数化测试，覆盖 GZIP/Snappy/ZSTD/无压缩 |
| `docs/docs/configuration.md` | 文档更新 |

### 测试覆盖

```
参数化测试矩阵：
  ┌─────────────┬──────────────────────┐
  │  压缩格式   │  验证行组大小准确性  │
  ├─────────────┼──────────────────────┤
  │ GZIP        │  ✅ 通过             │
  │ Snappy      │  ✅ 通过             │
  │ ZSTD        │  ✅ 通过             │
  │ Uncompressed│  ✅ 通过             │
  └─────────────┴──────────────────────┘
```

---

### PR #16691

## ⚡ Core：过期快照时跳过不必要的 manifest 扫描

| 字段 | 内容 |
|------|------|
| **PR 编号** | [#16691](https://github.com/apache/iceberg/pull/16691) |
| **作者** | `zhongyujiang` |
| **合并时间** | 2026-06-07 01:14 UTC |
| **标签** | `core` |

### 问题背景

```
cleanExpiredMetadata 流程（优化前）：

  对每个保留的 Snapshot
      ↓
  读取 manifest list 文件         ← I/O 开销
      ↓
  扫描所有 manifest 条目
      ↓
  确认 partition spec 可达性
      ↓
  ……继续扫描下一个 Snapshot（即使所有 spec 已确认）
```

当 `cleanExpiredMetadata` 启用时，`expire snapshots` 操作需要扫描所有**保留快照**的 manifest list，以确认哪些 partition spec 仍然可达。对维护大量快照的表，这会产生大量不必要的 I/O 读取。

### 优化策略

本 PR 引入两项优化：

```
优化 1：单 Spec 快速路径
┌───────────────────────────────────────────────────┐
│  if (table.specs().size() == 1)                   │
│      → 跳过所有扫描（当前 spec 必然可达）         │
│  适用大多数生产表（未做 partition evolution）     │
└───────────────────────────────────────────────────┘

优化 2：所有 Spec 已确认后提前退出
┌───────────────────────────────────────────────────┐
│  reachableSpecs = Set()                           │
│  for snapshot in retainedSnapshots:               │
│      scan manifest list                           │
│      reachableSpecs.addAll(found specs)           │
│      if reachableSpecs == allKnownSpecs:          │
│          break  ← 提前退出，跳过剩余快照          │
└───────────────────────────────────────────────────┘
```

**额外简化：** `mayHaveExpiredSpecs` 条件判断得到精简，因为 default spec 始终存在于 specs 列表中，无需单独判断。

### 性能收益

```
场景对比：

  单 Spec 表（最常见）：
    优化前：扫描 N 个 snapshot × manifest list 文件
    优化后：0 次 I/O ← 完全跳过

  多 Spec 表（经过 partition evolution）：
    优化前：扫描全部 N 个 snapshot
    优化后：一旦所有 spec 被发现即停止
            平均减少约 (1 - k/N) × 100% 的 I/O
            其中 k = 确认所有 spec 所需扫描数
```

### 修改文件

| 文件 | 修改内容 |
|------|----------|
| `core/RemoveSnapshots.java` | 主逻辑：单 spec 快速路径 + 提前退出 |
| 测试文件 | 在 `TestRemoveSnapshots` 中验证新优化路径 |

---

### PR #15180

## 🌐 REST Spec：新增 Function 列表/加载端点 OpenAPI 规范

| 字段 | 内容 |
|------|------|
| **PR 编号** | [#15180](https://github.com/apache/iceberg/pull/15180) |
| **作者** | `huaxingao` |
| **合并时间** | 2026-06-07 01:01 UTC |
| **标签** | `OPENAPI` |

### 功能概述

这是 Iceberg REST Catalog 函数（UDF）支持的**第一阶段**，提供只读访问能力：

```
REST Catalog 函数端点（阶段一 · 只读）

  GET /v1/{prefix}/namespaces/{namespace}/functions
      └─ 返回：ListFunctionsResponse
         {
           "functions": [
             { "namespace": [...], "name": "my_udf" },
             ...
           ]
         }

  GET /v1/{prefix}/namespaces/{namespace}/functions/{function}
      └─ 返回：LoadFunctionResponse
         {
           "metadata-location": "...",
           "function": {
             "function-id": "uuid",
             "name": "my_udf",
             "namespace": [...],
             "definitions": [
               {
                 "definition-id": "uuid",
                 "version": 1,
                 "input-schema": { ... },
                 "return-type": { ... }
               }
             ]
           }
         }
```

**明确排除（留待后续 PR）：**
- `POST` — 创建函数
- `PUT` — 替换函数
- `DELETE` — 删除函数

### 新增 OpenAPI 组件

| 组件类型 | 名称 | 说明 |
|----------|------|------|
| Schema | `ListFunctionsResponse` | 函数列表响应体 |
| Schema | `LoadFunctionResponse` | 单函数加载响应体 |
| Schema | `FunctionDefinition` | 函数定义（含版本、重载） |
| Schema | `FunctionDataType` | 数据类型（支持 primitive/map/list/struct） |
| Parameter | `function` | 路径参数定义 |
| Example | `NoSuchFunctionError` | 标准错误响应示例 |

### 修改文件

| 文件 | 修改内容 |
|------|----------|
| `open-api/rest-catalog-open-api.yaml` | 核心 OpenAPI 规范，新增 400+ 行 |
| `open-api/rest-catalog-open-api.py` | Python 生成脚本同步更新 |

### 关联背景

此 PR 合并后立即触发了新 Issue [#16700](#issue-16700) 的讨论：是否需要在函数定义模型中增加 SQL 标准的 `specific-name` 字段（详见下方 Issue 分析）。

---

### 依赖升级

## 📦 依赖升级批次（#16702 ~ #16706）

> 均由 `dependabot[bot]` 自动提交，由维护者集中批量合并于 2026-06-07 16:40~16:42。

| PR | 依赖名称 | 旧版本 | 新版本 | 标签 |
|----|----------|--------|--------|------|
| [#16702](https://github.com/apache/iceberg/pull/16702) | `jackson-bom` | 2.21.3 | 2.21.4 | java |
| [#16703](https://github.com/apache/iceberg/pull/16703) | `nessie` | 0.107.5 | 0.107.6 | java |
| [#16704](https://github.com/apache/iceberg/pull/16704) | `docker/setup-qemu-action` | 4.0.0 | 4.1.0 | github_actions |
| [#16705](https://github.com/apache/iceberg/pull/16705) | `shadow-gradle-plugin` | 8.3.10 | 8.3.11 | java |
| [#16706](https://github.com/apache/iceberg/pull/16706) | `spotless-plugin-gradle` | 8.5.1 | 8.6.0 | java/build |

---

## 新增 PR 汇总

> 统计 2026-06-07 新建（含当日已合并）的 PR，共 **13 个**。

### 功能性新 PR（重点关注）

#### PR #16713 — Core/Parquet：每列独立控制字典编码

| 字段 | 内容 |
|------|------|
| **PR 编号** | [#16713](https://github.com/apache/iceberg/pull/16713) |
| **作者** | `Gerrrr` |
| **状态** | 🟡 Open |
| **标签** | `parquet` `core` `docs` |
| **关联 Issue** | #16599 |

**问题：** 现有的 `write.parquet.dict-enabled` 仅支持**全局**开关，无法对特定列单独控制字典编码。

**方案：**
```
新属性前缀：write.parquet.dict-enabled.column.<col-name> = true/false

优先级：列级别覆盖 > 全局默认值

API 新增：
  Parquet.WriteBuilder.withDictionaryEncoding(String colName, boolean enabled)

底层：复用 parquet-java 的 ParquetProperties.Builder.withDictionaryEncoding()
```

高基数列（如 UUID、日志ID）通常字典编码无效甚至有害，此功能允许精细化调优。

---

#### PR #16711 — Spark 4.1：SQL DDL 支持列默认值

| 字段 | 内容 |
|------|------|
| **PR 编号** | [#16711](https://github.com/apache/iceberg/pull/16711) |
| **作者** | `AntiO2` |
| **状态** | 🟡 Open |
| **标签** | `spark` |
| **关联 Issue** | #16698 |

**支持的 DDL 语法（仅 Spark 4.1）：**

```sql
-- 建表时设置默认值
CREATE TABLE t (id INT DEFAULT 0, name STRING DEFAULT 'unknown') ...;

-- 新增列时设置默认值
ALTER TABLE t ADD COLUMN status INT DEFAULT 1;

-- 修改列默认值
ALTER TABLE t ALTER COLUMN status SET DEFAULT 2;

-- 移除列默认值
ALTER TABLE t ALTER COLUMN status DROP DEFAULT;
```

**支持的数据类型：** boolean、所有数字类型、decimal、string、date、timestamp、timestamp_ntz、binary

**明确不支持：** Array、Map、Struct 等复杂类型（带明确错误提示）

---

#### PR #16710 — API/Spark 4.1：snapshot 存储过程支持 `ignore_missing_files`

| 字段 | 内容 |
|------|------|
| **PR 编号** | [#16710](https://github.com/apache/iceberg/pull/16710) |
| **作者** | `drexler-sky` |
| **状态** | 🟡 Open（已获 1 个 Approve） |
| **标签** | `API` `spark` `docs` |

**背景：** `migrate` 存储过程已在 PR #16643/16684 中增加 `ignore_missing_files` 参数，本 PR 将相同能力同步到 `snapshot` 存储过程。

```sql
CALL iceberg.system.snapshot(
  'source_table',
  ignore_missing_files => true   -- 新增参数
)
```

允许在存在文件缺失的情况下继续快照操作，而非直接报错失败。

---

#### PR #16712 — Docs：新增 Apache Iceberg Summit 2026 播放列表

| 字段 | 内容 |
|------|------|
| **PR 编号** | [#16712](https://github.com/apache/iceberg/pull/16712) |
| **作者** | `ebyhr` |
| **状态** | 🟡 Open |
| **标签** | `docs` |

文档更新，将 2026 年 Iceberg Summit 的视频播放列表添加至官方文档。

---

### 依赖升级新 PR

| PR | 依赖 | 变更 | 状态 |
|----|------|------|------|
| [#16701](https://github.com/apache/iceberg/pull/16701) | `datamodel-code-generator` | 0.57.0 → 0.59.0 | 🟡 Open (dependabot) |
| [#16707](https://github.com/apache/iceberg/pull/16707) | `software.amazon.awssdk:bom` | 2.44.12 → 2.45.1 | 🟡 Open (dependabot) |
| [#16708](https://github.com/apache/iceberg/pull/16708) | `datamodel-code-generator` | 0.57.0 → 0.59.0 | 🟡 Open (huaxingao) |
| [#16709](https://github.com/apache/iceberg/pull/16709) | `software.amazon.awssdk:bom` | 2.44.12 → 2.45.1 | 🟡 Open (huaxingao) |

---

## 新增 Issue 分析

### Issue #16700

## 💡 UDF Spec：考虑为函数定义模型增加 SQL 标准 `specific-name`

| 字段 | 内容 |
|------|------|
| **Issue 编号** | [#16700](https://github.com/apache/iceberg/issues/16700) |
| **提出者** | `huaxingao` |
| **时间** | 2026-06-07 01:06 UTC（PR #15180 合并后 5 分钟） |
| **状态** | 🟡 Open |

### 背景与动机

```
SQL 标准中的函数命名体系：

  my_add(INT, INT) ← routine name（用户可见，可跨重载共享）
      │
      ├─ overload 1: my_add(INT, INT)
      │   └─ specific name: "my_add_int_int"    ← 人类可读的唯一标识
      │   └─ definition-id: "550e8400-..."       ← 当前 Iceberg 的机器标识
      │
      └─ overload 2: my_add(BIGINT, BIGINT)
          └─ specific name: "my_add_bigint_bigint"
          └─ definition-id: "f47ac10b-..."
```

### 现状

Iceberg 函数模型中已有 `definition-id`（UUID），在技术上实现了"特定名"的唯一标识功能，但缺乏**人类可读性**，跨引擎协作时难以调试和对齐。

### 提案

在 `FunctionDefinition` 模型中新增**可选**的 `specific-name` 字段：

```yaml
FunctionDefinition:
  required:
    - definition-id
    - version
    - input-schema
    - return-type
  optional:
    - specific-name  # 新增：SQL 标准的人类可读唯一标识符
```

**优势：** 不同引擎可通过 `specific-name` 协商并引用特定重载版本，提升互操作性与可调试性。

---

## 趋势与关注点

```
📊 2026-06-07 技术方向分布

  UDF / 函数支持     ████████░░  40%  (#15180合并, #16700议题, 后续关注)
  Parquet 优化       ██████░░░░  30%  (#16327合并, #16713在审)
  Spark 4.1 新特性   ████░░░░░░  20%  (#16711, #16710在审)
  运维优化           ██░░░░░░░░  10%  (#16691合并)
```

### 🔍 重点关注

1. **UDF/函数生态快速推进**
   PR #15180 刚合并，Issue #16700 随即产生，PR #16713（字典编码）也在同日提交，Iceberg 正在快速构建函数计算能力的标准化接口。

2. **Parquet 写入精细化控制**
   连续两个 Parquet 相关 PR（#16327 行组大小、#16713 字典编码），表明社区正在从全局配置向**列级、编解码感知**的精细配置演进。

3. **Spark 4.1 特性同步**
   `column default values`、`ignore_missing_files`、参数绑定等多项 Spark 4.1 特性正在密集落地，建议跟踪 Spark 4.1 集成的完整性。

4. **核心操作性能优化**
   PR #16691 的 manifest 扫描优化是运维友好型改进，对生产环境中大量快照的表有显著帮助，值得在 Iceberg 升级时重点测试验证。

---

## Fork 同步记录

| 项目 | 详情 |
|------|------|
| **同步时间** | 2026-06-08（本报告生成时） |
| **同步方向** | `apache/iceberg:main` → `vinlee19/iceberg:main` |
| **合并提交数** | 14 个 commits |
| **涉及文件** | 43 个文件，+2093 / -270 行 |
| **方式** | `git fetch upstream main && git merge --ff-only upstream/main && git push origin main` |

---

*报告生成时间：2026-06-08 | 数据来源：apache/iceberg GitHub 仓库 2026-06-07 00:00 ~ 23:59 UTC*
