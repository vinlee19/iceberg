# Apache Iceberg 每日动态分析报告

> **分析日期**: 2026-05-22（覆盖 2026-05-21 00:00 ~ 23:59 UTC）  
> **数据来源**: [apache/iceberg](https://github.com/apache/iceberg)  
> **Fork 同步状态**: ✅ 已同步至最新 upstream/main

---

## 📊 今日数据概览

| 类别 | 数量 |
|------|------|
| 🔀 合并的 PR | 10 |
| 🆕 新增 Issue | 4 |
| 📝 新增 PR | 20 |

---

## 🔀 合并的 Pull Requests（10个）

### 1. 【文档】#16495 — Docs: Drop manual deploy step from release instructions

| 字段 | 内容 |
|------|------|
| **作者** | stevenzwu |
| **合并时间** | 2026-05-21 22:10 UTC |
| **标签** | `docs` |
| **链接** | https://github.com/apache/iceberg/pull/16495 |

**问题背景**

发布流程文档（`how-to-release.md`）中存在一个过时的步骤，要求发布经理手动执行 `make deploy` 将文档推送到 `asf-site` 分支。但实际上，GitHub Actions 工作流 `site-ci` 已经在每次 `main` 分支合并涉及 `docs/`、`site/` 或 `format/` 路径时，自动执行 `make deploy`。

**修复内容**

```
修改文件：
  - site/docs/how-to-release.md   ← 删除过时的手动部署步骤
  - site/README.md                ← 添加自动触发说明
```

**影响分析**

- ✅ 防止发布经理误操作手动推送 `asf-site` 分支
- ✅ 明确说明 CI/CD 自动化机制，降低人为错误风险
- 📁 纯文档变更，无代码逻辑影响

---

### 2. 【构建】#16511 — Revapi: Fix order in the revapi file

| 字段 | 内容 |
|------|------|
| **作者** | gaborkaszab |
| **合并时间** | 2026-05-21 14:16 UTC |
| **标签** | 无 |
| **链接** | https://github.com/apache/iceberg/pull/16511 |

**问题背景**

运行 `revapi`（API 兼容性检查工具）时，版本顺序被重新排列，导致其他 PR 产生无意义的噪音变更。

**修复内容**

```
修改文件：
  - revapi.yml   ← 重新整理版本顺序，保持规范化排列
```

**影响分析**

- ✅ 减少代码审查噪音，提升 PR 可读性
- ✅ 保持 API 兼容性检查文件的整洁状态
- 📁 纯构建/配置变更

---

### 3. 【构建/CI】#16505 — Build: Skip spotlessCheck in core-tests

| 字段 | 内容 |
|------|------|
| **作者** | ebyhr |
| **合并时间** | 2026-05-21 11:57 UTC |
| **标签** | `INFRA` |
| **链接** | https://github.com/apache/iceberg/pull/16505 |

**问题背景**

`core-tests` 构建任务中包含了 `spotlessCheck`（代码格式化检查），但该检查已在独立的 `build-checks` 任务中覆盖，造成 CI 重复执行，浪费时间。

**修复内容**

```
CI 配置变更：
  core-tests 任务中跳过 spotlessCheck
  ↓
  统一由 build-checks 任务负责格式化检查
```

**影响分析**

- ✅ 减少 CI 执行时间
- ✅ 避免重复检查，优化构建管线
- 📁 无功能代码变更

---

### 4. 【Flink】#16503 — Flink: Backport table comments fix to flink v1.20, v2.0

| 字段 | 内容 |
|------|------|
| **作者** | SteveStevenpoor |
| **合并时间** | 2026-05-21 08:15 UTC |
| **标签** | `flink` |
| **链接** | https://github.com/apache/iceberg/pull/16503 |

**问题背景**

这是 #16423（Flink 表注释处理修复）的向后移植版本，将修复应用到 Flink v1.20 和 v2.0 分支。

**修复内容**

```
反向移植目标：
  Flink v1.20  ← 应用 #16423 的表注释修复
  Flink v2.0   ← 应用 #16423 的表注释修复

核心变更（同 #16423）：
  FlinkCatalog.java 中将表注释添加到 properties map
```

**影响分析**

- ✅ 所有维护中的 Flink 版本均获得一致的表注释支持
- ✅ 使用 Flink v1.20 / v2.0 的用户可以正确使用 `COMMENT` 语法

---

### 5. 【构建/代码质量】#16407 — Build: Ban Preconditions.checkState with %d placeholder

| 字段 | 内容 |
|------|------|
| **作者** | ebyhr |
| **合并时间** | 2026-05-21 07:34 UTC |
| **标签** | `API`, `core`, `flink`, `INFRA` |
| **链接** | https://github.com/apache/iceberg/pull/16407 |

**问题背景**

Guava 的 `Preconditions.checkState` 方法仅支持 `%s` 占位符，不支持 `%d`（整数格式化）。代码中误用 `%d` 会导致格式化字符串原样输出而非替换数值，产生误导性的错误信息。

**修复内容**

```java
// 错误用法（修复前）
Preconditions.checkState(condition, "Expected %d items", count);
// 输出: "Expected %d items"（%d 不被替换）

// 正确用法（修复后）
Preconditions.checkState(condition, "Expected %s items", count);
// 输出: "Expected 42 items"
```

```
修改内容：
  1. 替换所有使用 %d 的 checkState 调用
  2. 在 checkstyle.xml 中新增规则，禁止 checkState 使用 %d
     （类似已有的 checkArgument 规则）
  
涉及模块：API, core, flink
```

**影响分析**

- ✅ 修复潜在的误导性错误信息
- ✅ 通过静态检查预防未来类似错误
- ⚠️ 涉及多个模块的代码修改，影响面较广但均为安全修复

---

### 6. 【文档】#16391 — Docs: Update information about metrics mode

| 字段 | 内容 |
|------|------|
| **作者** | psvri |
| **合并时间** | 2026-05-21 05:44 UTC |
| **标签** | `docs` |
| **链接** | https://github.com/apache/iceberg/pull/16391 |

**问题背景**

文档中缺少对表属性 `write.metadata.metrics.default` 和 `write.metadata.metrics.column.col1` 所支持的各种 **Metrics Mode**（指标模式）的说明。随着 [File Format API](https://iceberg.apache.org/blog/apache-iceberg-file-format-api/) 的引入，整合者需要清晰的文档指导。

**新增内容**

```
文档新增：
  - none    : 不收集任何统计信息
  - counts  : 仅收集 null/non-null 计数
  - truncate(N): 截断字符串到 N 个字符后收集边界统计
  - full    : 收集完整列统计（默认）
```

**影响分析**

- ✅ 改善 Iceberg 集成开发者体验
- ✅ 补充 File Format API 配套文档
- 📁 纯文档变更

---

### 7. 【Flink】#16423 — Flink: Handle table comments in FlinkSQL

| 字段 | 内容 |
|------|------|
| **作者** | SteveStevenpoor |
| **合并时间** | 2026-05-21 05:29 UTC |
| **标签** | `flink` |
| **链接** | https://github.com/apache/iceberg/pull/16423 |

**问题背景**

Flink SQL 中使用 `COMMENT` 关键字创建表时，注释信息没有被正确保存到 Iceberg 表的属性中。这导致表注释丢失，不符合 Flink SQL 规范。

**修复内容**

```java
// FlinkCatalog.java 修复前
// table comment 被忽略

// FlinkCatalog.java 修复后
if (table.getComment() != null && !table.getComment().isEmpty()) {
    properties.put(TABLE_COMMENT_PROP, table.getComment());
}
```

```
变更文件：
  flink/v1.19/flink/src/.../FlinkCatalog.java   ← 将注释加入 properties
  flink/v1.19/flink/src/.../TestFlinkCatalogTable.java  ← 新增测试
    - testCreateTableComment()  验证注释正确保存
```

**SQL 示例**

```sql
-- 修复后可正确保存表注释
CREATE TABLE my_table (
  id BIGINT,
  name STRING
) COMMENT '这是我的 Iceberg 表'
WITH ('connector' = 'iceberg', ...);
```

**影响分析**

- ✅ 解决 Flink SQL `COMMENT` 语法与 Iceberg 的兼容性问题
- ✅ 新增回归测试保障
- 🔗 关联 Issue: #16422

---

### 8. 【核心/存储】#14876 — Encrypting IO as a `DelegateFileIO`

| 字段 | 内容 |
|------|------|
| **作者** | smaheshwar-pltr |
| **合并时间** | 2026-05-21 03:43 UTC |
| **标签** | `API` |
| **链接** | https://github.com/apache/iceberg/pull/14876 |

**问题背景**

这是一个历时较长（从 2025-12-17 到 2026-05-21）的重要 PR。原有的 `EncryptingFileIO` 实现存在一个设计缺陷：

```java
// 旧实现问题
// EncryptingFileIO.combine() 从不返回支持 SupportsBulkOperations 的 FileIO
// 即使底层 IO 支持批量操作（如删除孤立文件），加密层也会屏蔽这个能力

// 导致问题的场景：
// DeleteOrphanFilesSparkAction 检查 SupportsBulkOperations
// → EncryptingFileIO 返回 false
// → 回退到逐文件删除（极慢）
```

**修复内容**

将 `EncryptingFileIO` 重构为 `DelegateFileIO` 实现：

```
架构变更：
  EncryptingFileIO
    ↓ 重构为
  EncryptingFileIO implements DelegateFileIO
    ├── 代理底层 IO 的所有接口能力
    ├── SupportsBulkOperations  ← 正确透传
    ├── SupportsRecovery        ← 正确透传  
    └── 其他扩展接口            ← 正确透传
```

**影响分析**

- ✅ 加密表现在可以充分利用批量删除等高性能操作
- ✅ 修复了加密 IO 层对底层存储能力的不正确屏蔽
- 🔧 对加密表的 `DeleteOrphanFilesAction`、`RewriteDataFilesAction` 等操作有显著性能提升
- ⚠️ 潜在 API 变更，经过 11 次评审迭代充分验证

---

### 9. 【站点】#16496 — Site: Add version URL alias hook for docs

| 字段 | 内容 |
|------|------|
| **作者** | kevinjqliu |
| **合并时间** | 2026-05-21 01:08 UTC |
| **标签** | `docs` |
| **链接** | https://github.com/apache/iceberg/pull/16496 |

**问题背景**

在本地运行文档站点时，`/docs/latest/` 可以正常访问，但 `/docs/1.11.0/` 无法解析，影响文档的版本化 URL 体验。

**修复内容**

```python
# 新增 site/hooks/version_alias.py
# MkDocs on_post_build 钩子
# 在构建输出中创建版本目录的符号链接

def on_post_build(config):
    site_dir = config['site_dir']
    docs_dir = os.path.join(site_dir, 'docs')
    # 创建: docs/1.11.0/ → docs/latest/
    version = get_current_version()
    os.symlink('latest', os.path.join(docs_dir, version))
```

```
变更文件：
  site/hooks/version_alias.py   ← 新建版本别名钩子（51行）
  site/mkdocs.yml               ← 注册新钩子
  site/mkdocs-dev.yml           ← 开发模式也启用
```

**影响分析**

- ✅ `/docs/1.11.0/` 和 `/docs/latest/` 现在解析到同一内容
- ✅ 不重复导航条目，保持站点整洁
- 🔗 是 #16497（静态托管修复）的前置 PR

---

### 10. 【Flink】#16447 — Flink: Backport #16419 to Flink v2.0 and v1.20

| 字段 | 内容 |
|------|------|
| **作者** | SteveStevenpoor |
| **合并时间** | 2026-05-21 00:25 UTC |
| **标签** | `flink` |
| **链接** | https://github.com/apache/iceberg/pull/16447 |

**问题背景**

这是 #16419（Flink ALTER TABLE ADD COLUMN 到指定位置的修复）向 Flink v2.0 和 v1.20 的反向移植。

**修复内容**

```sql
-- 修复后可以正确执行：
ALTER TABLE my_table ADD COLUMN new_col STRING AFTER existing_col;
-- 修复前：列被添加到末尾，忽略 AFTER 位置参数
```

```
反向移植目标：
  Flink v2.0   ← 应用 #16419 修复
  Flink v1.20  ← 应用 #16419 修复
```

**影响分析**

- ✅ 维护版本的 Flink 用户获得 `ADD COLUMN` 位置正确性修复
- 🔗 原始修复 PR: #16419，本次 backport: #16447

---

## 🆕 新增 Issues（4个）

### Issue #16519 — 🐛 SerializableTable.sortOrders() 与已删除列的历史排序顺序冲突

| 字段 | 内容 |
|------|------|
| **作者** | aihuaxu |
| **严重级别** | Bug |
| **受影响版本** | 1.11.0 |
| **链接** | https://github.com/apache/iceberg/issues/16519 |

**问题描述**

当一张 Iceberg 表存在**历史排序顺序**（已不再使用的旧排序定义），且该历史排序引用了已被删除的列时，调用 `SerializableTable.sortOrders()` 会抛出 `ValidationException`。

**根本原因**

PR #15150 在序列化 `SerializableTable` 时新增了对所有历史排序顺序的序列化。但反序列化时，使用了严格验证模式绑定每个排序顺序，而历史排序合理引用已不存在的字段：

```java
// 问题代码路径
SerializableTable.sortOrders()
  → 对每个历史 SortOrder 调用 SortOrder.bind(schema, caseSensitive=true)
  → 找不到已删除列 "ts"
  → 抛出 ValidationException
```

**复现步骤**

```java
// 1. 添加列 ts，建立包含 id+ts 的排序顺序 1
table.updateSchema().addColumn("ts", Types.LongType.get()).commit();
table.replaceSortOrder().asc("id").asc("ts").commit();

// 2. 切换到仅含 id 的排序顺序 2，然后删除列 ts
table.replaceSortOrder().asc("id").commit();
table.updateSchema().deleteColumn("ts").commit();

// 3. 触发 Bug
Table serialized = SerializableTable.copyOf(table);
serialized.sortOrders(); // ← 抛出 ValidationException！
```

**期望行为**: 历史排序顺序应使用非严格（unchecked）绑定，与 `TableMetadataParser` 处理 `PartitionSpec` 的方式一致。

---

### Issue #16514 — 💡 优化可满足分区演化的重新分区以减少小文件

| 字段 | 内容 |
|------|------|
| **作者** | mukund-thakur |
| **类型** | improvement |
| **链接** | https://github.com/apache/iceberg/issues/16514 |

**问题描述**

在对大型 Iceberg 表进行分区演化重写时（例如从按月分区改为按月+日分区），当前算法会将所有旧规格数据文件归为一个大组。启用 partial-progress 后，文件被随机分配到多个 Spark 任务，每个任务可能写入所有新分区，导致大量小文件。

**场景示例**

```
问题场景：
  旧分区规格：按月
  新分区规格：按月+日
  数据量：15TB

当前行为：
  15TB 数据 → 150个 Spark shuffle 任务（每个 100GB）
  每个任务随机处理文件 → 每个输出分区最多产生 150 个小文件
  → 需要额外的 Compaction 任务

期望行为：
  识别"可满足"关系（新分区规格满足旧分区规格时）
  → 按旧分区分组文件
  → 每个输出分区文件数量大幅减少
```

**已有相关 PR**: #16515

---

### Issue #16510 — 🐛 Spark 时间旅行中过滤器列名解析使用当前 Schema 而非快照 Schema

| 字段 | 内容 |
|------|------|
| **作者** | amenck |
| **严重级别** | Bug |
| **受影响版本** | 1.11.0 |
| **受影响引擎** | Spark 3.5.5 |
| **链接** | https://github.com/apache/iceberg/issues/16510 |

**问题描述**

在 Spark 时间旅行（time-travel）读取时，`SELECT` 操作正确使用快照 Schema，但 `filter()` 操作错误地使用**当前** Schema 解析列名，导致列重命名后历史快照的过滤查询失败。

**复现代码**

```python
# 创建表，写入数据
spark.sql("CREATE TABLE {TABLE} (id BIGINT, col DOUBLE) USING iceberg")
spark.sql("INSERT INTO {TABLE} VALUES (1, 100.0)")
snapshot_v1 = spark.sql(f"SELECT snapshot_id FROM {TABLE}.snapshots").collect()[-1].snapshot_id

# 重命名列
spark.sql(f"ALTER TABLE {TABLE} RENAME COLUMN col TO value")
spark.sql(f"INSERT INTO {TABLE} VALUES (4, 400.0)")

# 时间旅行读取
df = spark.read.format(ICEBERG_FORMAT).option("snapshot-id", snapshot_v1).load(TABLE)

df.columns       # ✅ ['id', 'col']  — 正确使用快照 Schema
df.select("col") # ✅ 正常
df.count()       # ✅ 返回 3

# BUG：使用当前 Schema（含 value 列）解析 col
df.filter(F.col("col") > 0).count()
# ❌ ValidationException: Cannot find field 'col' in struct:
#      struct<1: id: optional long, 2: value: optional double>
```

**期望行为**: `filter()` 应与 `select()` 一致，使用快照的 Schema 解析列名。

---

### Issue #16502 — 🐛 Arrow 向量化读取 decimal 列默认值时抛出 IllegalArgumentException

| 字段 | 内容 |
|------|------|
| **作者** | harperjiang |
| **严重级别** | Bug |
| **受影响版本** | main（开发中）|
| **受影响引擎** | Spark（向量化模式）|
| **关联 PR** | #16501 |
| **链接** | https://github.com/apache/iceberg/issues/16502 |

**问题描述**

对含有 `initialDefault` 或 `writeDefault` 的 `decimal` 列，Arrow 向量化读取器在分配 Arrow vector 时抛出异常：

```
java.lang.IllegalArgumentException: Cannot cast default value to FIXED[9]: 12345.6789
  at VectorizedArrowReader.getPhysicalType(VectorizedArrowReader.java:255)
  at VectorizedArrowReader.allocateFieldVector(VectorizedArrowReader.java:228)
```

**根本原因分析**

```
VectorizedArrowReader.getPhysicalType()
  ↓
  将逻辑类型（decimal）转换为物理类型（int/long/fixed[N]）以分配 Arrow vector
  ↓
  使用 Types.NestedField.from(field) 复制字段时，连同 initialDefault/writeDefault 一起复制
  ↓
  调用 castDefault(decimalLiteral, physicalType)
  → DecimalLiteral 无法转换为 FixedType
  → 抛出 IllegalArgumentException
```

**影响范围**

- 非向量化读取路径（`spark.sql.iceberg.vectorization.enabled=false`）不受影响
- 字典编码的小数据集可能偶然不触发（走 `allocateDictEncodedVector` 路径）
- 已有修复 PR: #16501

---

## 📝 新增 Pull Requests（20个）

### 正在开发中的 PRs

| # | 标题 | 作者 | 标签 | 状态 |
|---|------|------|------|------|
| [#16520](https://github.com/apache/iceberg/pull/16520) | Spark: Fix DELETE_GRANULARITY_DEFAULT and use it in SparkWriteConf | turboFei | `spark`, `core` | 🟡 Open |
| [#16518](https://github.com/apache/iceberg/pull/16518) | Mumbling: Add draft Mumbling Bitmap spec | rdblue | `Specification` | 🟡 Open |
| [#16517](https://github.com/apache/iceberg/pull/16517) | Flink: Deprecate 1.20 Flink support | talatuyarer | `flink`, `INFRA`, `docs`, `build` | 🟡 Open |
| [#16516](https://github.com/apache/iceberg/pull/16516) | Site: Add 1.11.0 release blog post | aihuaxu | `docs` | 🟡 Open |
| [#16515](https://github.com/apache/iceberg/pull/16515) | Core: Implement old partition aware rewrite | mukund-thakur | `API`, `core` | 🟡 Open |
| [#16513](https://github.com/apache/iceberg/pull/16513) | CI: Limit CVE scan runs to relevant changes | kevinjqliu | `INFRA` | 🟡 Open |
| [#16512](https://github.com/apache/iceberg/pull/16512) | Docs: Replace Hadoop catalog examples with JDBC and REST catalog | KodaiD | `docs` | 🟡 Open |
| [#16508](https://github.com/apache/iceberg/pull/16508) | Arrow: Cap heap allocations in vectorized Parquet decoders | wombatu-kun | `arrow` | 🟡 Open |
| [#16507](https://github.com/apache/iceberg/pull/16507) | API, Core: Add exceptions for OAuth2 token endpoint errors | oguzhanunlu | `API`, `core` | 🟡 Open |
| [#16506](https://github.com/apache/iceberg/pull/16506) | Core: Reduce multiplier to 2 in testAddManyFilesWithConsistentOrdering | ebyhr | `core`, `build` | 🟡 Open |
| [#16504](https://github.com/apache/iceberg/pull/16504) | Kafka Connect: Capture integration test logs in CI | wombatu-kun | `INFRA`, `build`, `KAFKACONNECT` | 🟡 Open |
| [#16501](https://github.com/apache/iceberg/pull/16501) | Arrow: Fix vectorized reads of decimal columns with default values | harperjiang | `spark`, `arrow` | 🟡 Open |
| [#16500](https://github.com/apache/iceberg/pull/16500) | Core, Data: Validate deletion-vector offset and length | wombatu-kun | `core`, `data` | 🟡 Open |
| [#16499](https://github.com/apache/iceberg/pull/16499) | GCP: Route GCS batch deletes per credential prefix | wombatu-kun | `GCP` | 🟡 Open |
| [#16498](https://github.com/apache/iceberg/pull/16498) | Spark: Fix DeleteOrphanFilesSparkAction sibling-prefix scope | wombatu-kun | `spark` | 🟡 Open |
| [#16497](https://github.com/apache/iceberg/pull/16497) | Site: Fix version URL alias for static hosting | kevinjqliu | `docs` | 🟡 Open |

### 重点新增 PR 详细分析

---

#### 【Spark】#16520 — Fix DELETE_GRANULARITY_DEFAULT and use it in SparkWriteConf

**背景**: PR #11478 将 Spark 写入的有效默认删除粒度更改为 `FILE`，但留下了两个不一致性：

```java
// 问题 1：常量指向错误的值
// TableProperties.java
public static final String DELETE_GRANULARITY_DEFAULT = "partition";  // ← 应该是 "file"

// 问题 2：三个 Spark 版本各自硬编码
// SparkWriteConf.java (v3.5, v4.0, v4.1)
DeleteGranularity.FILE  // ← 没有使用常量，造成分散维护
```

**修复方案**:

```java
// 修复后
public static final String DELETE_GRANULARITY_DEFAULT = "file";  // ✅ 正确

// SparkWriteConf 统一引用
TableProperties.DELETE_GRANULARITY_DEFAULT  // ✅ 单一真相来源
```

**影响**: 纯代码一致性修复，无行为变更。

---

#### 【规范】#16518 — Mumbling: Add draft Mumbling Bitmap spec

**背景**: 这是一个重要的前瞻性 PR，为 **Iceberg v4 格式**添加了 `Mumbling Bitmap` 草案规范。

**Mumbling Bitmap** 是一种嵌入式删除向量（Deletion Vector）格式，设计用于 v4 根清单（root manifests）中，目标是高效地标记已删除行。

```
Iceberg v4 架构愿景：
  Root Manifest
    ├── Data files
    └── Deletion vectors (Mumbling Bitmap format)
          ↓
          高效压缩的行级删除标记
          无需单独的 delete files
```

> ⚠️ 这是草案规范，尚未正式采纳为 Iceberg 格式标准。

**重要性**: 体现了 Iceberg 社区对下一代格式（v4）删除机制的重要探索。

---

#### 【Flink】#16517 — Deprecate 1.20 Flink support

**背景**: Iceberg 1.11 已发布，按照 Iceberg 支持策略，将废弃 Flink 1.20 支持。

```
变更内容：
  - 移除所有 Flink 1.20 相关代码
  - 保留 Flink v2.0+ 支持
  - 更新文档和构建配置

标签: flink, INFRA, docs, build
```

**影响**: Flink 1.20 用户需要升级到 Flink v2.0 或更高版本。

---

#### 【安全】#16500 — Core, Data: Validate deletion-vector offset and length

**背景**: 恶意或损坏的 manifest 元数据可能包含负数或接近 2GiB 的 `content_offset` / `content_size_in_bytes`，绕过现有的不完整前置条件检查。

```java
// 现有问题
// BaseDeleteLoader.validateDV() 只检查：
//   1. non-null
//   2. size <= Integer.MAX_VALUE
// 未检查负数情况！

// 攻击场景
content_offset = -1         // 负偏移量
content_size_in_bytes = -1  // 负大小
// → 可能导致越界读取或整数溢出

// 修复：增加 >= 0 验证
Preconditions.checkArgument(offset >= 0, "Invalid DV offset: %s", offset);
Preconditions.checkArgument(size >= 0, "Invalid DV size: %s", size);
```

**影响**: 提高系统健壮性和安全性，防范损坏数据的异常行为。

---

#### 【GCP】#16499 — Route GCS batch deletes per credential prefix

**背景**: `GCSFileIO.internalDeleteFiles` 在批量删除时，从批次的第一个对象路径选择 GCS Storage 客户端。当 GCSFileIO 配置了多个按前缀凭证时，跨凭证边界的批次会用错误的凭证删除文件。

```
问题场景：
  Bucket A（凭证 1）: file1, file2
  Bucket B（凭证 2）: file3, file4

  批次 = [file1, file3]  ← 跨凭证边界
  当前行为：用凭证1（file1的凭证）尝试删除 file3 → 权限错误

修复方案：
  按凭证前缀分组 BlobId，每组使用对应凭证
```

**影响**: 修复多凭证 GCS 配置下的批量删除失败问题。

---

## 🔄 Fork 同步状态

```
同步时间: 2026-05-22
上游仓库: https://github.com/apache/iceberg
同步分支: main → claude/happy-ride-Bj031
同步结果: ✅ 成功（27 files changed, 398 insertions(+), 145 deletions(-)）

最新同步的 upstream 提交：
  7bb0fa2 Docs: Drop manual deploy step from release instructions (#16495)
  2cce97a Build: Fix order in revapi.yml (#16511)
  c4f835e CI: Skip spotlessCheck in core-tests (#16505)
  56013b7 Flink: Backport handle table comments in FlinkSQL (#16503)
  2f05390 Build: Ban Preconditions.checkState with %d placeholder (#16407)
  46ae3ad Docs: Update information about metrics mode (#16391)
  693e8a7 Flink: Handle table comments in FlinkSQL (#16423)
  8a90699 Encrypting IO as a `DelegateFileIO` (#14876)
  d0a4954 Site: Add version URL alias hook for docs (#16496)
  90c2c4c Flink: Backport add column fix to flink v1.20, v2.0 (#16447)
```

---

## 📈 趋势分析

### 今日活跃领域

```
Flink        ████████████ 4 PRs（backport 工作密集）
文档/站点     ████████████ 4 PRs
构建/CI      ████████░░░░ 3 PRs
Arrow        ████████░░░░ 2 new PRs（性能与稳定性）
核心         ████░░░░░░░░ 2 new PRs
Spark        ████░░░░░░░░ 2 new PRs
存储/安全    ████░░░░░░░░ 2 new PRs
```

### 重点关注

| 优先级 | 事项 | 说明 |
|--------|------|------|
| 🔴 高 | #16510 时间旅行过滤器 Bug | Spark 用户普遍受影响，需尽快修复 |
| 🔴 高 | #16519 SerializableTable Bug | 1.11.0 回归 Bug，影响生产 |
| 🟡 中 | #14876 EncryptingIO 合并 | 加密表性能显著提升 |
| 🟡 中 | #16518 Mumbling Bitmap 规范 | v4 格式重要方向，值得关注 |
| 🟢 低 | #16517 Flink 1.20 废弃 | 需提前通知 Flink 1.20 用户 |

---

*报告生成时间: 2026-05-22 | 下次同步: 2026-05-23 00:00 UTC*
