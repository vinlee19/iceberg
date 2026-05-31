# Apache Iceberg 每日动态报告

> **报告日期：** 2026-05-30（UTC）  
> **数据范围：** 2026-05-30 00:00 UTC — 2026-05-30 23:59 UTC  
> **上游仓库：** [apache/iceberg](https://github.com/apache/iceberg)  
> **Fork 同步状态：** ✅ 已同步至最新 commit `6a73700`

---

## 📊 今日数据概览

| 类别 | 数量 |
|------|------|
| ✅ 合并的 PR | 3 |
| 🆕 新提交的 PR | 7 |
| 🐛 新增 Issue | 3 |
| 🔒 Fork 同步 | 已完成，同步 57 个文件 |

---

## ✅ 合并的 PR（Merged Pull Requests）

### PR #16616 · Spark: 废弃 SparkFilters，推荐使用 SparkV2Filters

| 字段 | 内容 |
|------|------|
| **PR 链接** | [#16616](https://github.com/apache/iceberg/pull/16616) |
| **作者** | [@huaxingao](https://github.com/huaxingao) |
| **标签** | `spark` |
| **合并时间** | 2026-05-30 22:37 UTC |
| **影响版本** | Spark 3.5 / 4.0 / 4.1 |

#### 问题背景

`SparkFilters` 是 Spark DSv1 `Filter[]` API 的旧式适配器，将旧版过滤器转换为 Iceberg `Expression`。在所有当前支持的 Spark 版本（3.5、4.0、4.1）中，生产代码已全部迁移到 `SparkV2Filters`（处理 DSv2 `Predicate[]` API），`SparkFilters` 仅剩测试代码 `TestSparkFilters` 仍在引用。

#### 变更内容

```
spark/v3.5/src/main/java/.../SparkFilters.java   -- 添加 @Deprecated + Javadoc
spark/v4.0/src/main/java/.../SparkFilters.java   -- 添加 @Deprecated + Javadoc
spark/v4.1/src/main/java/.../SparkFilters.java   -- 添加 @Deprecated + Javadoc
spark/v3.5/src/test/java/.../TestSparkFilters.java  -- 添加 @SuppressWarnings("deprecation")
spark/v4.0/src/test/java/.../TestSparkFilters.java  -- 添加 @SuppressWarnings("deprecation")
spark/v4.1/src/test/java/.../TestSparkFilters.java  -- 添加 @SuppressWarnings("deprecation")
```

#### 技术细节

```
旧路径（已废弃）：  SparkFilters
                    ↓  转换 Filter[] (DSv1)
                    Iceberg Expression

新路径（推荐）：    SparkV2Filters
                    ↓  转换 Predicate[] (DSv2)
                    Iceberg Expression
```

**废弃说明：** 目标在 **1.12.0** 版本中彻底删除 `SparkFilters`。  
**影响评估：** 纯注解变更，无生产行为变化。

---

### PR #16586 · Core: 修复 JsonUtil.getStringArray 对非字符串元素的静默强制转换

| 字段 | 内容 |
|------|------|
| **PR 链接** | [#16586](https://github.com/apache/iceberg/pull/16586) |
| **作者** | [@stevenzwu](https://github.com/stevenzwu) |
| **标签** | `core` `docs` `build` |
| **合并时间** | 2026-05-30 22:35 UTC |

#### 问题背景

`JsonUtil.getStringArray(JsonNode)` 原先对每个数组元素直接调用 `asText()`，**不验证**元素是否为 JSON 字符串类型。非字符串类型会被静默强制转换（例如整数 `45` → 字符串 `"45"`，布尔 `true` → `"true"`），掩盖了格式错误的输入。

与之对比，`JsonStringArrayIterator`（被 `getStringList`、`getStringSet`、`getStringListOrNull` 使用）已经有 `isTextual()` 检查。

#### 影响的调用方分析

| 调用方 | 文件位置 | 影响 |
|--------|----------|------|
| `ViewVersionParser.fromJson` | `core/.../view/ViewVersionParser.java:100` | 解析视图版本的 `default-namespace`（规范要求字符串列表） |
| `RESTSerializers.NamespaceDeserializer` | `core/.../rest/RESTSerializers.java:262` | 从 REST 载荷反序列化 `Namespace`（OpenAPI 规范定义为 `array<string>`） |
| `RemoteSignRequestParser.headersFromJson` | `core/.../rest/requests/RemoteSignRequestParser.java:136` | 解析 HTTP 请求头（定义即为字符串） |

#### 变更内容

```java
// 修复前：静默强制转换
for (JsonNode element : arrayNode) {
    result[i++] = element.asText();  // 数字/布尔被静默转换
}

// 修复后：严格类型检查
for (JsonNode element : arrayNode) {
    Preconditions.checkArgument(
        element.isTextual(),
        "Cannot parse string from non-text value: %s", element);
    result[i++] = element.asText();
}
```

#### 新增测试覆盖

- `null` 节点处理
- 非数组节点处理
- 含非字符串元素的数组（触发异常）
- 有效字符串数组
- 空数组

**安全意义：** 提升了 REST API 数据验证的健壮性，符合快速失败（fail-fast）原则。

---

### PR #16023 · Core: 修复 Token 刷新时 optionalOAuthParams 被丢弃的问题

| 字段 | 内容 |
|------|------|
| **PR 链接** | [#16023](https://github.com/apache/iceberg/pull/16023) |
| **作者** | [@bharos](https://github.com/bharos) |
| **标签** | `core` |
| **合并时间** | 2026-05-30 19:30 UTC |
| **修复 Issue** | [#16022](https://github.com/apache/iceberg/issues/16022) |

#### 问题背景

当 `exchangeEnabled` 为 `false` 时，`refreshExpiredToken()` 和静态 `refreshToken()` 方法在调用 `fetchToken()` 时使用了**空 Map** `ImmutableMap.of()`，而不是传入 `optionalOAuthParams`（包含 `audience`、`resource`、`scope` 等关键参数）。

这导致：
- **首次 Token 获取**正常（参数传递正确）
- **Token 过期后刷新**失败（丢失 audience/scope 等参数，返回 401/403）

#### 根因溯源

```
PR #14059：删除已废弃的 5 参数 fetchToken() 重载
           ↓
将 6 参数调用内联时误用 ImmutableMap.of() 替代了 optionalOAuthParams
           ↓
非 exchange 分支的 Token 刷新丢失关键 OAuth 参数
```

#### 变更对比

```java
// 修复前（exchangeEnabled=false 分支）
fetchToken(client, headers, grantType, credential,
           scope, ImmutableMap.of());  // ← 错误：空 Map

// 修复后
fetchToken(client, headers, grantType, credential,
           scope, optionalOAuthParams);  // ← 正确：透传参数
```

**涉及位置：**
- `refreshExpiredToken()` 中的非 exchange 分支
- 静态 `refreshToken()` 中的非 exchange 分支

#### 新增测试

- `TestOAuth2Util`：验证过期 Token 场景下 `audience` 出现在 Token 请求表单中
- `TestOAuth2Util`：验证主动刷新场景下 `audience` 出现在 Token 请求表单中

**影响范围：** 使用 OAuth2 直接 Token 刷新（非 Token Exchange）且配置了 `audience`/`resource`/`scope` 的用户，此前会在 Token 过期后遭遇认证失败。

---

## 🆕 新提交的 PR（New Pull Requests）

### PR #16626 · Spark 4.1: 修复 IcebergSparkSqlExtensionsParser 中参数绑定丢失

| 字段 | 内容 |
|------|------|
| **PR 链接** | [#16626](https://github.com/apache/iceberg/pull/16626) |
| **作者** | [@j1wonpark](https://github.com/j1wonpark) |
| **状态** | 🟢 Open |
| **标签** | `spark` |
| **提交时间** | 2026-05-30 23:38 UTC |
| **关联 Issue** | [#16625](https://github.com/apache/iceberg/issues/16625) |

**问题：** Spark 4.1 通过 `ParserInterface.parsePlanWithParameters()` 路由参数化查询（SPARK-53573），而 `IcebergSparkSqlExtensionsParser` 只重写了 `parsePlan`，未重写 `parsePlanWithParameters`，导致参数上下文被丢弃。

**修复：** 重写 `parsePlanWithParameters`，Iceberg 命令走原有路径，非 Iceberg SQL 完整透传 `ParameterContext` 给底层 Spark 解析器。

---

### PR #16621 · Parquet: 修复 decimal 和 UUID 过滤器下推转换

| 字段 | 内容 |
|------|------|
| **PR 链接** | [#16621](https://github.com/apache/iceberg/pull/16621) |
| **作者** | [@alexandrefimov](https://github.com/alexandrefimov) |
| **状态** | 🟢 Open |
| **标签** | `parquet` |
| **提交时间** | 2026-05-30 20:02 UTC |
| **关联 Issue** | [#16035](https://github.com/apache/iceberg/issues/16035) |

**主要修复点：**
- 使用 Parquet `MessageType` 构建下推过滤器，确保 decimal 谓词使用正确的物理原始类型
- 适配 `INT32`、`INT64`、`BINARY`、`FIXED_LEN_BYTE_ARRAY` 编码的 decimal 字面量转换
- 使用实际 fixed-length 字节数组长度处理 fixed decimal
- decimal 字面量无法在文件 schema 精度下安全表示时回退到 `NOOP`
- 修复 Parquet UUID 逻辑类型到 Iceberg `UUIDType` 的回转映射，使 UUID 谓词正确绑定

---

### PR #16620 · Hive: 修复 HMS createTime/lastAccessTime 的整数溢出

| 字段 | 内容 |
|------|------|
| **PR 链接** | [#16620](https://github.com/apache/iceberg/pull/16620) |
| **作者** | [@wombatu-kun](https://github.com/wombatu-kun) |
| **状态** | 🟢 Open |
| **标签** | `hive` |
| **提交时间** | 2026-05-30 15:37 UTC |

**问题根因（Java 运算符优先级 Bug）：**

```java
// 错误写法：先将 long 截断为 int，再除以 1000
(int) currentTimeMillis / 1000
// 等价于：((int) currentTimeMillis) / 1000  ← 64位先被截断为32位

// 正确写法：先整除，再类型转换
(int) (currentTimeMillis / 1000)
```

**后果：** HMS `lastAccessTime` 字段存储的值落在 1970 年附近甚至为负数（约每 49 天一次 wrap 循环），会误导依赖 `TBLS.LAST_ACCESS_TIME` 的数据保留/过期工具。

---

### PR #16619 · Parquet: 修复 timestamp_ns / timestamptz_ns 谓词下推

| 字段 | 内容 |
|------|------|
| **PR 链接** | [#16619](https://github.com/apache/iceberg/pull/16619) |
| **作者** | [@wombatu-kun](https://github.com/wombatu-kun) |
| **状态** | 🟢 Open |
| **标签** | `parquet` |
| **提交时间** | 2026-05-30 13:41 UTC |

**问题：** 通过 `ReadSupport` 读取路径过滤 `timestamp_ns`/`timestamptz_ns` 列时，纳秒精度谓词会匹配所有行（过滤失效），无任何异常抛出。

**双重根因：**

```
问题1：MessageTypeToType 忽略 timestamp 单位
  Parquet INT64 TIMESTAMP(NANOS) → 错误映射为 Iceberg micros TimestampType
  过滤器用微秒字面量（~1.7e15）与纳秒数据（~1.7e18）比较
  → 所有行均满足谓词

问题2：ParquetFilters 缺少 TIMESTAMP_NANO case
  修复问题1后，转换会抛出 UnsupportedOperationException
```

**修复：**
- `MessageTypeToType`：根据 `TimestampLogicalTypeAnnotation.getUnit()` 区分 NANOS（映射到 `TimestampNanoType`）和其他单位
- `ParquetFilters`：新增 `case TIMESTAMP_NANO`，直接以 INT64 long 比较原始纳秒值

**参考：** ORC 侧同类修复见 [#16609](https://github.com/apache/iceberg/pull/16609)

---

### PR #16617 · Arrow: 向量化 Reader 支持将 Parquet UINT32 读取为 long

| 字段 | 内容 |
|------|------|
| **PR 链接** | [#16617](https://github.com/apache/iceberg/pull/16617) |
| **作者** | [@drexler-sky](https://github.com/drexler-sky) |
| **状态** | 🟢 Open |
| **标签** | `arrow` |
| **提交时间** | 2026-05-30 03:08 UTC |
| **Follow-up to** | [#16006](https://github.com/apache/iceberg/pull/16006) |

**背景：** `#16006` 在非向量化路径支持了 Parquet `UINT32` → Iceberg `LongType` 的读取，本 PR 补齐向量化路径。

**实现方式：** Parquet `UINT32` 物理类型是 `INT32`，`IntAccessor.getLong` 会以符号扩展方式将 int 拓宽为 long——对无符号 32 位值（均能放入 Java long）来说行为正确。因此**无需新的 Reader/Accessor/VectorType**，只需在 `VectorizedArrowReader` 中放宽 `UINT32` 对 `LongType` 字段的前置条件即可。

---

### PR #16618 · CI: 新增 ActionScope GitHub Actions 安全扫描（已关闭）

| 字段 | 内容 |
|------|------|
| **PR 链接** | [#16618](https://github.com/apache/iceberg/pull/16618) |
| **作者** | [@r12habh](https://github.com/r12habh) |
| **状态** | 🔴 Closed（未合并） |
| **标签** | `INFRA` |
| **提交/关闭时间** | 09:32 → 19:40 UTC |

提议添加 ActionScope 工具对 GitHub Actions workflow 文件进行 CI/CD 安全扫描。本地扫描结果显示高风险 2 项、中风险 1 项、低风险 26 项，无严重（Critical）项。已被关闭，未合并到主干。

---

### PR #16623 · CI: Fork Runner 负载分发 E2E POC（草稿，已关闭）

| 字段 | 内容 |
|------|------|
| **PR 链接** | [#16623](https://github.com/apache/iceberg/pull/16623) |
| **作者** | [@singhpk234](https://github.com/singhpk234) |
| **状态** | 🔴 Closed Draft |
| **标签** | `API` `INFRA` `docs` `OPENAPI` |

CI 基础设施探索性 PR，测试将 CI 工作负载分发到 Fork Runner 的可行性。作为 POC 草稿提交后关闭。

---

## 🐛 新增 Issue

### Issue #16625 · Spark: 启用 Iceberg SQL 扩展时参数化查询报 UNBOUND_SQL_PARAMETER

| 字段 | 内容 |
|------|------|
| **Issue 链接** | [#16625](https://github.com/apache/iceberg/issues/16625) |
| **报告人** | [@j1wonpark](https://github.com/j1wonpark) |
| **类型** | 🐛 Bug |
| **创建时间** | 2026-05-30 23:25 UTC |
| **状态** | Open（已有修复 PR #16626） |
| **影响版本** | Spark 4.1 + Iceberg SQL Extensions |

**复现代码：**

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

# 位置参数——抛出 UNBOUND_SQL_PARAMETER
spark.sql("SELECT ? AS id", args=[42]).show()

# 命名参数——抛出 UNBOUND_SQL_PARAMETER
spark.sql("SELECT :id AS id", args={"id": 42}).show()
```

**错误信息：**

```
[UNBOUND_SQL_PARAMETER] Found the unbound parameter: _7.
'Project [posparameter(7) AS id#N]
+- OneRowRelation
```

**根因：** `IcebergSparkSqlExtensionsParser` 未重写 Spark 4.1 新增的 `parsePlanWithParameters()`，导致参数上下文被默认实现忽略。

---

### Issue #16624 · Spec: Avro 元数据中 "iceberg.schema" 键名在规范文档中缺失

| 字段 | 内容 |
|------|------|
| **Issue 链接** | [#16624](https://github.com/apache/iceberg/issues/16624) |
| **报告人** | [@Tishj](https://github.com/Tishj) |
| **类型** | 📄 文档/规范问题 |
| **创建时间** | 2026-05-30 21:50 UTC |
| **状态** | Open，已有 2 条评论 |

**问题：** Iceberg 规范文档的 Manifests 章节提到 JSON 表示使用 `"schema"` 键：

```
> JSON representation of the table schema at the time the manifest was written
```

但实际代码（`Avro.java:203`，已有 8 年历史）使用的是 `"iceberg.schema"` 键：

```java
// core/src/main/java/org/apache/iceberg/avro/Avro.java:203
metadata.put("iceberg.schema", schemaJson);
```

提议修正规范文档，将键名更正为 `"iceberg.schema"`。

---

### Issue #16622 · 对简单聚合查询更好地利用分区统计

| 字段 | 内容 |
|------|------|
| **Issue 链接** | [#16622](https://github.com/apache/iceberg/issues/16622) |
| **报告人** | [@iandw](https://github.com/iandw) |
| **类型** | 💡 Improvement（功能改进） |
| **创建时间** | 2026-05-30 21:30 UTC |
| **状态** | Open |
| **查询引擎** | Trino |

**期望行为：**

对于按分区列的简单聚合查询，例如：

```sql
SELECT min(utc_date) FROM foo
-- 表按 utc_date 分区
```

应直接从分区统计信息中获取结果，**无需扫描实际数据文件**，类似于 Parquet/ORC 文件级统计的下推优化。

**当前行为：** 全量扫描所有数据文件。

---

## 🔄 Fork 同步记录

| 操作 | 详情 |
|------|------|
| **同步时间** | 2026-05-31 (UTC) |
| **同步前 commit** | `8f28a86` |
| **同步后 commit** | `6a73700` |
| **变更文件数** | 57 个文件 |
| **代码变化** | +1485 行 / -466 行 |
| **新增文件** | `SECURITY-THREAT-MODEL.md`（安全威胁模型文档）、Flink DynamicRecordProcessor 测试文件（v1.20/v2.0/v2.1） |

---

## 📈 趋势洞察

### 本日重点关注

1. **Parquet 过滤器下推质量提升**：本日有 2 个 Parquet 相关 PR（#16619 timestamp_ns 精度、#16621 decimal/UUID），持续修复 Parquet 谓词下推的边界情况，体现社区对读取性能正确性的持续投入。

2. **OAuth2 Token 刷新可靠性**：PR #16023 修复了一个潜伏已久的 OAuth 参数丢失 Bug，影响所有使用非 Exchange Token 刷新且配置了 audience/scope 的 REST Catalog 用户，**建议升级**。

3. **Spark 4.1 生态适配**：Issue #16625 + PR #16626 快速响应 Spark 4.1 的新 API，显示社区对 Spark 新版本的快速跟进能力。

4. **Java 类型系统陷阱**：PR #16620（`(int) long / 1000` 运算符优先级 Bug）是一个经典的 Java 隐式类型转换陷阱，值得在代码审查 checklist 中特别关注。

### 标签分布

```
parquet  ████████░░  2 个新 PR
spark    ████░░░░░░  1 个合并 PR + 1 个新 PR
core     ████░░░░░░  2 个合并 PR
hive     ██░░░░░░░░  1 个新 PR
arrow    ██░░░░░░░░  1 个新 PR
infra    ██░░░░░░░░  2 个新 PR（均已关闭）
```

---

*报告由 Claude Code 自动生成 | 数据来源：GitHub API | 生成时间：2026-05-31*
