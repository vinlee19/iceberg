# Apache Iceberg 每日动态报告

> **报告日期：** 2026-05-25
> **生成时间：** 2026-05-26 00:00 UTC
> **数据来源：** [apache/iceberg](https://github.com/apache/iceberg)
> **Fork 同步状态：** ✅ 已同步（vinlee19/iceberg ← apache/iceberg main）

---

## 📊 每日概览

| 类别 | 数量 |
|------|------|
| ✅ 合并的 PR | 2 |
| 🆕 新增 Issue | 2 |
| 🔀 新增 PR | 3 |

---

## ✅ 合并的 Pull Requests

### PR #16529 — Data: Remove Flag `FEATURE_META_ROW_LINEAGE` in BaseFormatModelTests

| 字段 | 内容 |
|------|------|
| **作者** | GuoYu (Guosmilesmile) |
| **合并者** | pvary |
| **合并时间** | 2026-05-25 08:37 CEST |
| **标签** | `data`, `spark` |
| **影响模块** | `data/src/test/` |
| **变更规模** | 1 file changed, 1 insertion(+), 7 deletions(-) |

#### 背景与动机

在 PR #15776 完成之后，ORC 格式正式获得了 Row Lineage（行溯源）元数据列的支持。原先 `BaseFormatModelTests` 中存在一个特性开关标志 `FEATURE_META_ROW_LINEAGE`，用于在 ORC 格式支持该功能前临时跳过相关单元测试。随着 ORC 支持的落地，该特性标志已无存在必要。

#### 变更内容

**删除 `FEATURE_META_ROW_LINEAGE` 常量定义：**

```java
// 删除前
static final String FEATURE_META_ROW_LINEAGE = "metaRowLineage";

// 删除后（该行已移除）
```

**简化 ORC 特性数组声明：**

```java
// 删除前
FileFormat.ORC,
new String[] {
    FEATURE_REUSE_CONTAINERS,
    FEATURE_COLUMN_METRICS_TRUNCATE_BINARY,
    FEATURE_META_ROW_LINEAGE,    // ← 已删除
    FEATURE_READER_DEFAULT
}

// 删除后
FileFormat.ORC,
new String[] {
    FEATURE_REUSE_CONTAINERS, FEATURE_COLUMN_METRICS_TRUNCATE_BINARY, FEATURE_READER_DEFAULT
}
```

**移除两处 `assumeSupports` 测试跳过逻辑：**

```java
// testReadMetadataColumnRowLinage 方法
// 删除前
void testReadMetadataColumnRowLinage(FileFormat fileFormat) throws IOException {
    assumeSupports(fileFormat, FEATURE_META_ROW_LINEAGE);  // ← 已删除，ORC 现在正常运行
    ...
}

// testReadMetadataColumnRowLinageExistValue 方法
// 删除前
void testReadMetadataColumnRowLinageExistValue(FileFormat fileFormat) throws IOException {
    assumeSupports(fileFormat, FEATURE_META_ROW_LINEAGE);  // ← 已删除
    ...
}
```

#### 影响分析

```
测试覆盖度变化：
  ORC 格式 Row Lineage 测试
    ├── testReadMetadataColumnRowLinage       原来：ORC 跳过 → 现在：ORC 正常执行 ✅
    └── testReadMetadataColumnRowLinageExistValue  原来：ORC 跳过 → 现在：ORC 正常执行 ✅
```

#### 开发时间线

```
2026-05-22  提交初始 PR，标记为 Draft
              ↓  等待 Spark 3.5/4.0 反向移植 PR #16534 完成
2026-05-23  PR #16534 完成，PR 状态改为 Ready for Review
              ↓  请求 pvary 评审
2026-05-25  pvary 批准并合并 ✅
```

---

### PR #16149 — Core: Replace Deprecated `CloseableHttpClient.execute`

| 字段 | 内容 |
|------|------|
| **作者** | Yuya Ebihara (@ebyhr) |
| **合并者** | pvary |
| **合并时间** | 2026-05-25 07:45 CEST |
| **标签** | `core` |
| **影响模块** | `core/src/main/java/org/apache/iceberg/rest/HTTPClient.java` |
| **变更规模** | 1 file changed, 58 insertions(+), 43 deletions(-) |

#### 背景与动机

Apache HttpClient 5.x 将 `CloseableHttpClient.execute(ClassicHttpRequest, HttpContext)` 标记为 **已弃用（deprecated）**，推荐使用响应处理器（ResponseHandler）模式：`execute(ClassicHttpRequest, HttpClientResponseHandler)`。该 PR 消除了技术债务，将 HTTPClient 迁移到现代 API。

#### 核心架构变化

**API 迁移对比：**

```
旧方式（已弃用）：                    新方式（推荐）：
┌──────────────────────────────┐    ┌─────────────────────────────────────┐
│ CloseableHttpResponse resp = │    │ return httpClient.execute(           │
│   httpClient.execute(        │    │   request,                          │
│     request, context);       │    │   context,                          │
│ try (resp) {                 │    │   response -> handleResponse(...)   │
│   ... 处理响应 ...            │    │ );                                  │
│ } catch (IOException e) {    │    │                                     │
│   throw new RESTException(); │    │ // 响应处理逻辑提取到独立方法        │
│ }                            │    │ private <T> T handleResponse(...)   │
└──────────────────────────────┘    └─────────────────────────────────────┘
```

**类型替换：**

```java
// 删除的 import
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;

// 新增的 import
import org.apache.hc.core5.http.ClassicHttpResponse;

// 所有参数类型从 CloseableHttpResponse → ClassicHttpResponse
private static String extractResponseBodyAsString(ClassicHttpResponse response) { ... }
private static boolean isSuccessful(ClassicHttpResponse response) { ... }
private static ErrorResponse buildDefaultErrorResponse(ClassicHttpResponse response) { ... }
private static void throwFailure(ClassicHttpResponse response, ...) { ... }
private <T> boolean emptyBody(ClassicHttpResponse response, ...) { ... }
```

#### 关键重构：提取 `handleResponse` 方法

原来所有响应处理逻辑都内嵌于 `execute()` 方法的 try-with-resources 块中，重构后提取为独立的 `handleResponse()` 方法：

```java
// 新增独立方法
private <T extends RESTResponse> T handleResponse(
    HTTPRequest request,
    ClassicHttpResponse response,       // ← 使用新 API 类型
    Class<T> responseType,
    Consumer<ErrorResponse> errorHandler,
    Consumer<Map<String, String>> responseHeaders,
    ParserContext parserContext) throws IOException {

    // 1. 提取响应头
    Map<String, String> respHeaders = Maps.newHashMap();
    for (Header header : response.getHeaders()) {
        respHeaders.put(header.getName(), header.getValue());
    }
    responseHeaders.accept(respHeaders);

    // 2. 处理空响应体（204 No Content / 304 Not Modified）
    if (emptyBody(response, responseType)) {
        if (response.getCode() == HttpStatus.SC_NOT_MODIFIED
            && !request.headers().contains(HttpHeaders.IF_NONE_MATCH)) {
            throw new RESTException("Invalid (NOT_MODIFIED) response...");
        }
        return null;
    }

    // 3. 处理错误响应
    if (!isSuccessful(response)) {
        String responseBody = extractResponseBodyAsString(response);
        throwFailure(response, responseBody, errorHandler);
    }

    // 4. 反序列化响应体
    ObjectReader reader = objectReaderCache.computeIfAbsent(responseType, mapper::readerFor);
    if (parserContext != null && !parserContext.isEmpty()) {
        reader = reader.with(parserContext.toInjectableValues());
    }
    return reader.readValue(response.getEntity().getContent());
}
```

#### 废弃注解移除

```java
// 删除前（标记为需要抑制弃用警告）
@SuppressWarnings("deprecation")
@Override
protected <T extends RESTResponse> T execute(...) { ... }

// 删除后（不再需要该注解）
@Override
protected <T extends RESTResponse> T execute(...) { ... }
```

#### 评审过程

```
2026-05-06  gaborkaszab 首次评审，提出代码改进建议
              ↓  作者 force-push 修改
            gaborkaszab 确认修改"合理"
2026-05-18  pvary 批准
              ↓  36 个 CI 检查全部通过
2026-05-25  pvary 合并 ✅
```

---

## 🆕 新增 Issues

### Issue #16564 — `dropTable` 的 `purgeRequested` 行为是否应由 REST Catalog 实现自行决定？

| 字段 | 内容 |
|------|------|
| **作者** | @szymonorz |
| **状态** | Open 🟢 |
| **标签** | `question` |
| **查询引擎** | Trino 481 |

#### 问题描述

该 issue 揭示了不同 REST Catalog 实现之间关于 `purgeRequested` 标志行为的不一致性：

```
REST Catalog 规范中 dropTable(purgeRequested=false) 的预期行为：
┌─────────────────────────────────────────────────────────┐
│  purgeRequested=false → 只删除元数据，保留数据文件      │
│  purgeRequested=true  → 同时删除元数据和数据文件        │
└─────────────────────────────────────────────────────────┘

实际问题场景：
  Trino unregister_table
      │
      ▼ 调用
  dropTable(purgeRequested=false)
      │
      ▼ Lakekeeper 实现
  默认将 purgeRequested 强制设为 true
      │
      ▼ 结果
  ⚠️  数据文件被意外删除！
```

#### 核心矛盾

- Trino 的 `unregister_table` 文档明确说明：「只影响元数据，不删除数据」
- 但某些 REST Catalog 实现（如 Lakekeeper）会忽略 `purgeRequested=false`，默认清除数据
- 这在跨 Catalog 实现迁移时存在**数据丢失风险**

#### 讨论方向

此 issue 的核心问题是：REST Catalog 规范是否应该强制约束 `purgeRequested` 的行为，还是将其留给各实现自行决定？这对数据安全性有重大影响。

---

### Issue #16563 — SparkRuntimeFilterableScan 中过滤表达式规范化（Canonicalization）存在缺陷

| 字段 | 内容 |
|------|------|
| **作者** | @ahshahid |
| **状态** | Open 🟢 |
| **标签** | 无 |
| **涉及版本** | Spark 4.1+ |

#### 问题描述

`SparkBatchQueryScan` 的 `equals()` 和 `hashCode()` 实现在生成过滤表达式字符串时**未强制排序**，导致：

```
问题根因：
  执行计划 A 的过滤器字段顺序：[i_item_sk, i_brand,    i_category]
  执行计划 B 的过滤器字段顺序：[i_item_sk, i_category, i_brand   ]

  虽然逻辑等价，但 equals() 返回 false！

  后果：
  Spark 优化器认为两个扫描节点不同
       ↓
  无法复用 Exchange 算子
       ↓
  TPC-DS 等基准测试中出现性能退化
```

#### 影响范围

- 影响所有使用 `SupportsRuntimeFiltering` 接口的 Spark 集成场景
- 特别是 TPC-DS 基准查询中涉及 Dynamic Partition Pruning (DPP) 的场景
- 与 SPARK-45866（DPP + AQE 交互问题）相关

#### 已提供修复

作者已附上针对 Spark 4.1 的补丁，但需要同步修复所有相关 Spark 版本。

---

## 🔀 新增 Pull Requests

### PR #16562 — Spark: 为 Serializable-Isolation 和 Concurrent-Refresh 测试增加超时限制

| 字段 | 内容 |
|------|------|
| **作者** | @wombatu-kun |
| **状态** | Open 🟢 |
| **标签** | `spark` |
| **关联 Issue** | #16359 |

#### 问题背景

在 PR #16303 的 CI 运行中，发现某些并发测试可能无限期运行：
- Worker 线程在以 `Integer.MAX_VALUE` 为上限的循环中提交操作
- 主线程等待冲突异常但没有任何超时机制
- 当冲突异常始终未出现时，测试进程持续运行直到 CI 耗尽资源/磁盘空间

#### 修复方案

```java
// 变更前（无界循环，无超时）
for (int i = 0; i < Integer.MAX_VALUE; i++) {
    // 提交操作...
}
// 主线程无限等待...

// 变更后（有界循环 + 超时）
private static final int MAX_OPERATIONS = 20;       // 统一上限常量
private static final int OPERATION_TIMEOUT_MINUTES = 5; // 超时时间

for (int i = 0; i < MAX_OPERATIONS; i++) {          // 循环有界
    Future<?> future = executor.submit(...);
    try {
        future.get(OPERATION_TIMEOUT_MINUTES, TimeUnit.MINUTES); // 超时控制
    } finally {
        future.cancel(true);                         // 确保清理
    }
}
```

#### 影响范围

涉及 Spark 3.5 / 4.0 / 4.1 三个版本中的 6 个测试方法：
- `testMergeWithSerializableIsolation`
- `testDeleteWithSerializableIsolation`
- `testUpdateWithSerializableIsolation`
- 以及上述三个方法对应的 `concurrent-refresh` 变体

常量迁移至 `SparkRowLevelOperationsTestBase` 以确保所有并发测试使用统一边界。

---

### PR #16560 — Core: 暴露分区统计（Partition Statistics）的辅助方法

| 字段 | 内容 |
|------|------|
| **作者** | @ebyhr (Yuya Ebihara) |
| **状态** | Open 🟢 |
| **标签** | `core` |
| **关联 Issue** | #14284, #14998 |
| **评审人** | @pvary, @gaborkaszab |

#### 问题背景

PR #14998 引入的变更导致 **Trino** 和 **Starburst** 的构建失败：

```
问题链路：
  Trino 限制使用 Hadoop 相关 Parquet 库（HDFS 依赖）
      │
      ▼
  无法直接使用 Iceberg 内部的 Parquet reader/writer 类
      │
      ▼
  原来可用的辅助方法现在是 package-private 或 internal
      │
      ▼
  ⚠️  Trino / Starburst 构建失败
```

#### 修复方案

将分区统计相关的辅助方法访问修饰符改为 `public`，允许下游项目在不依赖 Hadoop 的情况下访问这些功能。这是维护 Iceberg 生态系统兼容性的必要举措。

---

### PR #16561 — chore(ci): 对 Draft PR 跳过 GitHub Actions

| 字段 | 内容 |
|------|------|
| **作者** | @zhjwpku |
| **状态** | Draft ⬜ |
| **标签** | `INFRA` |

#### 变更内容

在 CI workflow 文件中添加条件逻辑，当 PR 处于 Draft 状态时跳过大多数 GitHub Actions 执行：

```yaml
# 添加的条件判断
if: ${{ github.event_name != 'pull_request' || github.event.pull_request.draft == false }}
```

注意：PR 标题格式检查 workflow 故意保留，不受此规则影响。

#### 当前讨论

- @manuzhang 提问：Draft PR 是否应该保留标题格式检查
- @ebyhr 要求在描述中补充相关 Apache 邮件列表的讨论背景

---

## 📈 整体趋势分析

### 今日活动热图

```
模块活动分布（2026-05-25）：
┌─────────────────────────────────────────────────────────┐
│ Core    ████████████████████  3 项（#16149✅ #16560🔀 #16561🔀）│
│ Data    ████████              1 项（#16529✅）                   │
│ Spark   ████████████          2 项（#16562🔀 #16563🆕）          │
│ REST    ████                  1 项（#16564🆕）                   │
└─────────────────────────────────────────────────────────┘
```

### 关键主题

1. **技术债务清理** — PR #16149 和 PR #16529 都属于"完成后收尾"型工作，分别清理了废弃 API 和已完成功能的临时标志
2. **测试稳定性** — PR #16562 解决了 CI 中的测试不稳定问题（runaway tests），这类问题持续困扰大型并发测试套件
3. **下游兼容性** — PR #16560 体现了 Iceberg 作为基础库对生态系统（Trino/Starburst）兼容性的重视
4. **规范一致性争议** — Issue #16564 触及 REST Catalog 规范层面的分歧，可能引发社区讨论和规范修订
5. **性能优化** — Issue #16563 中的过滤表达式规范化问题影响 Spark 查询优化器的 Exchange 复用，是性能敏感的修复

---

## 🔗 参考链接

| 资源 | 链接 |
|------|------|
| PR #16529 | https://github.com/apache/iceberg/pull/16529 |
| PR #16149 | https://github.com/apache/iceberg/pull/16149 |
| Issue #16564 | https://github.com/apache/iceberg/issues/16564 |
| Issue #16563 | https://github.com/apache/iceberg/issues/16563 |
| PR #16562 | https://github.com/apache/iceberg/pull/16562 |
| PR #16560 | https://github.com/apache/iceberg/pull/16560 |
| PR #16561 | https://github.com/apache/iceberg/pull/16561 |
| apache/iceberg main | https://github.com/apache/iceberg/tree/main |

---

*本报告由自动化脚本生成，覆盖时间范围：2026-05-25 00:00 UTC — 2026-05-25 23:59 UTC*
