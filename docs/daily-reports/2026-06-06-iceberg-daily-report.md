# Apache Iceberg 每日动态报告

**日期：** 2026-06-06（周六）  
**数据来源：** [apache/iceberg](https://github.com/apache/iceberg)  
**Fork 同步状态：** ✅ 已同步（vinlee19/iceberg `main` → upstream `95540ca`）

---

## 目录

1. [当日概览](#当日概览)
2. [已合并 PR 深度分析](#已合并-pr-深度分析)
   - [PR #16055 — GCS/S3/ADLS: 修复 InputStream EOF 处理](#pr-16055--gcss3adls-修复-inputstream-eof-处理)
   - [PR #16523 — Spark: 修复分布式规划模式下列重命名时的时间旅行过滤问题](#pr-16523--spark-修复分布式规划模式下列重命名时的时间旅行过滤问题)
   - [PR #16626 — Spark 4.1: 修复 IcebergSparkSqlExtensionsParser 参数绑定问题](#pr-16626--spark-41-修复-icebergsparksqlextensionsparser-参数绑定问题)
   - [PR #15180 — REST Catalog 规范: 新增函数端点](#pr-15180--rest-catalog-规范-新增函数端点)
3. [当日新增 PR](#当日新增-pr)
4. [当日新增 Issue](#当日新增-issue)
5. [总结与趋势](#总结与趋势)

---

## 当日概览

| 类型 | 数量 |
|------|------|
| 合并 PR | 4 |
| 新增 PR | 5 |
| 新增 Issue | 2 |

```
🟢 合并: PR #16055  GCS/S3/ADLS EOF 修复
🟢 合并: PR #16523  Spark 分布式扫描时间旅行 Bug 修复
🟢 合并: PR #16626  Spark 4.1 参数绑定修复
🟢 合并: PR #15180  REST Catalog 函数 API 规范扩展
🔵 新增: PR #16699  Spark 行ID Manifest 重写修复
🔵 新增: PR #16697  IN 谓词 null 值处理修复
🔵 新增: PR #16696  随机数据集合大小上限优化（测试加速）
🔵 新增: PR #16695  RESTMetricsReporter 阻塞线程修复
🟡 Issue: #16698   Spark DDL 支持 Iceberg V3 列默认值
🟡 Issue: #16694   v4: 新增 Manifest 写入支持
```

---

## 已合并 PR 深度分析

---

### PR #16055 — GCS/S3/ADLS: 修复 InputStream EOF 处理

> **链接：** https://github.com/apache/iceberg/pull/16055  
> **作者：** Vladislav Sidorovich（Google）  
> **标签：** `AWS` `GCP` `AZURE`  
> **合并时间：** 2026-06-06 01:41 +0200  
> **变更规模：** 7 个文件，+169 / -9 行

#### 问题背景

在 Iceberg 的三个云存储 InputStream 实现（GCS、S3、ADLS）中，`read()` 单字节方法存在相同的 EOF 处理缺陷：

- 返回值 `-1`（表示 EOF）被**忽略**，没有传播给调用方
- `singleByteBuffer` 中残留的旧字节被错误地返回
- 即使到达文件末尾，`pos`（位置指针）依然递增，导致位置统计错误

#### 根本原因

以 `GCSInputStream` 为例，原始代码：

```java
// 修复前 — 有 Bug 的代码
pos += 1;                          // ❌ 先递增，不检查是否 EOF
try {
    channel.read(singleByteBuffer); // ❌ 返回值被丢弃
} catch (IOException e) { ... }
readBytes.increment();
readOperations.increment();
return singleByteBuffer.array()[0] & 0xFF; // ❌ 可能返回旧缓冲区数据
```

#### 修复方案

```java
// 修复后 — GCSInputStream.java
try {
    int bytesRead = channel.read(singleByteBuffer);
    if (bytesRead == -1) {
        return -1;           // ✅ 正确传播 EOF
    }
    pos += 1;               // ✅ 只有成功读取才递增位置
    readBytes.increment();
    readOperations.increment();
    return singleByteBuffer.array()[0] & 0xFF;
} catch (IOException e) {
    GCSExceptionUtil.throwNotFoundIfNotPresent(e, blobId);
    throw e;
}
```

同样的修复被应用到 `read(byte[] b, int off, int len)` 方法，以及 S3 和 ADLS 的对应实现：

```java
// S3InputStream.java - read(byte[] b, int off, int len) 修复
int bytesRead = Failsafe.with(retryPolicy).get(() -> stream.read(b, off, len));
if (bytesRead == -1) {
    return -1;  // ✅ 正确传播 EOF，避免 pos 负数递增
}
pos += bytesRead;
```

#### 影响的文件

| 文件 | 类型 | 说明 |
|------|------|------|
| `gcp/src/main/java/.../GCSInputStream.java` | 修复 | GCS 单字节和多字节 read |
| `aws/src/main/java/.../S3InputStream.java` | 修复 | S3 单字节和多字节 read |
| `azure/.../ADLSInputStream.java` | 修复 | ADLS 单字节和多字节 read |
| `gcp/.../TestGCSInputStream.java` | 测试 | 新增 EOF 测试用例 |
| `aws/.../TestS3InputStream.java` | 测试 | 新增 EOF 测试用例 |
| `azure/.../TestADLSInputStream.java` | 测试 | 新增 EOF 测试用例（×2 版本） |

#### 影响评估

> **严重性：中等**  
> 在正常读取完整文件时此 Bug 不影响结果，但在流式/增量读取场景下可能导致调用方无法感知 EOF，陷入无限等待或读取到错误数据。所有三大云存储后端（GCS、S3、ADLS）均受影响。

---

### PR #16523 — Spark: 修复分布式规划模式下列重命名时的时间旅行过滤问题

> **链接：** https://github.com/apache/iceberg/pull/16523  
> **作者：** sanshi (lilei1128)  
> **标签：** `spark` `core`  
> **合并时间：** 2026-06-06 07:56 +0800  
> **变更规模：** 7 个文件，+268 / -22 行（覆盖 Spark 3.5/4.0/4.1）

#### 问题背景

当满足以下所有条件时，查询会抛出 `ValidationException`：

1. 使用**分布式规划模式**（`read.data-planning-mode=distributed`）
2. 执行**时间旅行**查询（`AS OF` / `VERSION AS OF`）
3. 过滤条件中引用的列**已被重命名**

报错信息：
```
ValidationException: Cannot find field 'col' in struct: struct<..., 2: value: ...>
```

#### 根本原因（双重 Bug）

问题出现在两个位置：

**① Driver 端 — `specCache()` 使用了当前 Schema 而非快照 Schema**

```java
// BaseDistributedDataScan.java（原始代码）
// table().specs() 返回绑定到"当前"表结构的分区规范
// 时间旅行时，当前结构可能与目标快照结构不同
```

**② Executor 端 — ReadDataManifest / ReadDeleteManifest 使用了当前 Schema**

```java
// 修复前
public Iterator<DataFile> call(ManifestFileBean manifest) {
    FileIO io = table.value().io();
    Map<Integer, PartitionSpec> specs = table.value().specs(); // ❌ 使用当前表结构
    return ManifestFiles.read(manifest, io, specs)
        .filterRows(filter) // ❌ filter 基于历史列名，specs 基于当前列名 → 报错
        ...
}
```

#### 修复方案

在 Driver 端计算出**快照绑定**的 `specs()`，然后通过构造函数传入 Executor：

```java
// 修复后 — SparkDistributedDataScan.java (Spark 3.5/4.0/4.1 三版本同步)

// Driver 端：使用 specs()（快照绑定）代替 table().specs()（当前绑定）
.flatMap(new ReadDataManifest(tableBroadcast(), specs(), context(), withColumnStats));

// Executor 端：通过构造函数接收 specs，不再从 table 重新获取
private static class ReadDataManifest {
    private final Map<Integer, PartitionSpec> specs; // ✅ 快照绑定的规范
    
    ReadDataManifest(Broadcast<Table> table,
                     Map<Integer, PartitionSpec> specs, // ✅ 从 Driver 传入
                     TableScanContext context,
                     boolean withStats) {
        this.specs = specs;
    }
    
    public Iterator<DataFile> call(ManifestFileBean manifest) {
        FileIO io = table.value().io();
        // ✅ 不再调用 table.value().specs()
        return ManifestFiles.read(manifest, io, specs)...
    }
}
```

同样修复了 `ReadDeleteManifest` 和 `DeleteFileIndex.builderFor().specsById()` 调用。

#### 覆盖范围

修复同步到三个 Spark 版本：
- `spark/v3.5/spark/src/main/java/.../SparkDistributedDataScan.java`
- `spark/v4.0/spark/src/main/java/.../SparkDistributedDataScan.java`
- `spark/v4.1/spark/src/main/java/.../SparkDistributedDataScan.java`

每个版本都新增了对应测试用例（`TestSelect.java` 中约 75 行）。

#### 影响评估

> **严重性：高**  
> 对使用分布式扫描并进行时间旅行 + 列重命名的场景，这是一个硬性崩溃 Bug。大量数据湖场景会同时满足这三个条件（Schema Evolution + 历史查询 + 分布式规划）。

---

### PR #16626 — Spark 4.1: 修复 IcebergSparkSqlExtensionsParser 参数绑定问题

> **链接：** https://github.com/apache/iceberg/pull/16626  
> **作者：** Jiwon Park (j1wonpark)  
> **标签：** `spark`  
> **合并时间：** 2026-06-06 08:48 +0900  
> **变更规模：** 2 个文件，+72 / -2 行

#### 问题背景

Spark 4.1 在 SPARK-53573 中引入了新的参数化查询接口：`parsePlanWithParameters(sqlText, parameterContext)`。

当安装了 Iceberg SQL 扩展时，所有包含参数标记（`?` 位置参数或 `:name` 命名参数）的查询会**静默失败**——参数上下文被丢弃，参数标记未被绑定。

**只有 Spark 4.1 受影响**，3.5 和 4.0 在解析后才进行参数绑定，没有这个接口。

#### 根本原因

```scala
// 修复前 — IcebergSparkSqlExtensionsParser.scala
// trait 的默认实现会忽略 parameterContext，退回到 parsePlan
// Iceberg 只覆盖了 parsePlan，没有覆盖 parsePlanWithParameters
// → 4.1 路由到 parsePlanWithParameters 时，parameterContext 被丢弃
```

#### 修复方案

重构 `parsePlan` 为共享的私有方法，然后新增 `parsePlanWithParameters` 覆盖：

```scala
// 修复后 — IcebergSparkSqlExtensionsParser.scala

// ① 原 parsePlan 委托给新的私有方法
override def parsePlan(sqlText: String): LogicalPlan =
  parsePlanWithDelegate(sqlText)(delegate.parsePlan)

// ② 新增覆盖，正确转发 parameterContext
override def parsePlanWithParameters(
    sqlText: String,
    parameterContext: ParameterContext): LogicalPlan =
  parsePlanWithDelegate(sqlText) { sql =>
    delegate.parsePlanWithParameters(sql, parameterContext) // ✅ 保留上下文
  }

// ③ 共享分发逻辑
private def parsePlanWithDelegate(sqlText: String)(
    delegateParse: String => LogicalPlan): LogicalPlan = {
  val sqlTextAfterSubstitution = substitutor.substitute(sqlText)
  if (isIcebergCommand(sqlTextAfterSubstitution)) {
    // Iceberg 命令走扩展解析器
    parse(sqlTextAfterSubstitution) { parser => astBuilder.visit(parser.singleStatement()) }
      .asInstanceOf[LogicalPlan]
  } else {
    // 非 Iceberg SQL 走 delegate，并保留 parameterContext
    RewriteViewCommands(SparkSession.active).apply(delegateParse(sqlText))
  }
}
```

#### 测试用例

`TestExtendedParser.java` 中新增 3 个回归测试：

| 测试方法 | 验证内容 |
|---------|---------|
| `testParsePlanWithParametersDelegatesForNonIcebergSql` | 非 Iceberg SQL 正确委托并保留上下文 |
| `testParsePlanWithParametersBindsPositionalParameter` | `SELECT ? AS id` 位置参数端到端绑定 |
| `testParsePlanWithParametersBindsNamedParameter` | `SELECT :id AS id` 命名参数端到端绑定 |

#### 影响评估

> **严重性：中等（仅 Spark 4.1）**  
> 使用 Spark 4.1 + Iceberg 扩展的参数化查询场景受影响。常规 SQL 不受影响。

---

### PR #15180 — REST Catalog 规范: 新增函数端点

> **链接：** https://github.com/apache/iceberg/pull/15180  
> **作者：** Huaxin Gao (huaxingao)  
> **标签：** `OPENAPI`  
> **合并时间：** 2026-06-07 01:01 UTC（本地 2026-06-06 18:01 -0700）  
> **变更规模：** 2 个文件，+627 行（纯新增）

#### 变更概述

本 PR 是 Iceberg REST Catalog 对**函数（Functions）**支持的**第一阶段规范**（Stage 1），仅包含只读端点。

#### 新增 API 端点

```yaml
# 1. 列出命名空间下的所有函数
GET /v1/{prefix}/namespaces/{namespace}/functions

# 2. 加载指定函数的详情（包含所有重载定义）
GET /v1/{prefix}/namespaces/{namespace}/functions/{function}
```

#### OpenAPI 新增组件

| 组件类型 | 名称 | 说明 |
|---------|------|------|
| Response | `ListFunctionsResponse` | 函数列表响应 |
| Response | `LoadFunctionResponse` | 函数详情响应（含所有重载） |
| Parameter | `function` | 路径参数（函数名）|
| Example | `NoSuchFunctionError` | 404 错误示例 |

#### 端点详情

**`GET .../functions` — 列出函数**

```yaml
responses:
  200: ListFunctionsResponse   # 函数标识符列表
  400: BadRequestErrorResponse
  401: UnauthorizedResponse
  403: ForbiddenResponse
  404: NamespaceNotFound
  419: AuthenticationTimeoutResponse
  503: ServiceUnavailableResponse
  5XX: ServerErrorResponse
```

**`GET .../functions/{function}` — 加载函数**

```yaml
description: |
  Load a function from the catalog.
  All overloaded definitions are included in a single response.
responses:
  200: LoadFunctionResponse
  404: NamespaceNotFound | FunctionNotFound  # 两种 404 场景
```

#### 设计决策

- **范围**：仅读（list + load），不含 CRUD（create/replace/drop）
- **函数重载**：单次 `loadFunction` 请求返回该函数的**所有重载定义**
- **后续计划**：函数写入端点将在后续 PR 独立提案和投票；Spark 4.1 集成跟踪于 [#14954](https://github.com/apache/iceberg/pull/14954)

#### 影响评估

> **严重性：功能扩展（无破坏性）**  
> 这是 REST Catalog 对 SQL 函数支持的基础规范，是 Iceberg 功能完善度的重要里程碑。为后续 UDF / 存储过程的 catalog 管理提供了规范基础。

---

## 当日新增 PR

### PR #16699 — Spark: 修复 Manifest 重写时 first row ID 未正确保留

> **链接：** https://github.com/apache/iceberg/pull/16699  
> **作者：** amogh-jahagirdar  
> **状态：** Open  
> **标签：** `spark`

**问题描述：** 在通过 Spark Actions 执行 manifest 重写时，被移动的 entries 的 `firstRowId` 未被正确保留。原因是 `entries` 元数据表记录被适配为 `SparkContentFile` 结构时，该结构未覆盖 `firstRowId` 方法，导致适配后 `firstRowId` 变为 null，触发通过继承机制分配新行 ID 的逻辑，行 ID 被错误地重新分配。

---

### PR #16697 — Data: 修复 IN 谓词中 null 值导致的 NPE

> **链接：** https://github.com/apache/iceberg/pull/16697  
> **作者：** hantangwangd  
> **状态：** Open  
> **标签：** `API` `data`

**问题描述：** 通过 `IcebergGenerics.read(table).where(filter)` 扫描时，若过滤条件包含 `IN` 谓词且目标列含 null 值，会抛出 `NullPointerException`：
```
java.lang.NullPointerException: Invalid object: null
```
根本原因：`EvalVisitor` 的 `in(...)` 方法未检查目标列值是否为 null，在数据文件同时包含有效值和 null 值时崩溃。本 PR 新增 null 值防护逻辑。

---

### PR #16696 — Spark: 限制 RandomData 集合大小以加速嵌套类型测试

> **链接：** https://github.com/apache/iceberg/pull/16696  
> **作者：** Baunsgaard  
> **状态：** Open  
> **标签：** `spark`

**问题描述：** `RandomData`（Spark 测试工具）为每个嵌套 list/map 生成最多 20 个元素，深度嵌套时元素数量按层数相乘，最坏情况下单次测试生成超百万叶节点。`testMixedTypes` 是所有格式读写测试类中最耗时的测试方法。

**改进方案：** 将上限从 20 改为命名常量 `MAX_COLLECTION_SIZE = 10`，应用于 Spark 3.5/4.0/4.1。

**实测效果（JDK 17, Spark 3.5 单线程）：**

| 测试类 | 优化前 | 优化后 | 加速比 |
|--------|-------|--------|--------|
| TestAvroDataFrameWrite | 24.1s | 7.8s | 3.1× |
| TestParquetDataFrameWrite | 20.0s | 3.6s | 5.6× |
| TestORCDataFrameWrite | 19.9s | 3.4s | 5.9× |
| TestParquetScan | 17.7s | 2.6s | 6.8× |
| TestParquetVectorizedScan | 17.5s | 2.3s | 7.6× |
| TestAvroScan | 17.5s | 2.4s | 7.3× |

全套 5,084 个测试，0 个失败。

---

### PR #16695 — Core: 修复 RESTMetricsReporter.report() 阻塞调用线程

> **链接：** https://github.com/apache/iceberg/pull/16695  
> **作者：** turboFei  
> **状态：** Open（替换已关闭的 #16693）  
> **标签：** `core`

**问题描述：** `RESTMetricsReporter.report()` 虽然使用 `METRICS_EXECUTOR` 后台线程池，但调用后立即进入 `Tasks.waitFor()` 的 `sleep(10ms)` 轮询循环，**实际上是同步阻塞**的。

在生产环境中，每次 Iceberg 扫描/提交都触发 `report()`，当调用速度超过 `METRICS_EXECUTOR` 处理速度时，调用线程全部阻塞在 `waitFor()` 中。实际观测到的现象：**9,351 个 `Iceberg-CompactionReport-Thread` 线程**在数小时内以约 2.7 个/秒的速度积累，占用数 GB 线程栈内存。

**修复方案：**
```java
// 修复前 — 实际同步阻塞
Tasks.range(1).executeWith(METRICS_EXECUTOR).run(...); // 内部有 waitFor()

// 修复后 — 真正异步
METRICS_EXECUTOR.execute(() -> {
    try {
        client.post(...);
    } catch (Exception e) {
        LOG.warn("Failed to report metrics...", e);
    }
});
// report() 立即返回
```

线程池改为 daemon 线程，JVM 退出时自动终止，无需 shutdown hook。

---

## 当日新增 Issue

### Issue #16698 — Spark: 支持在 DDL 中为 Iceberg V3 表设置列默认值

> **链接：** https://github.com/apache/iceberg/issues/16698  
> **作者：** AntiO2  
> **标签：** `improvement`  
> **状态：** Open

**功能请求：** Iceberg V3 格式已支持列默认值元数据，Spark 也可以在写入时使用 `DEFAULT` 关键字。但 Spark SQL DDL 目前无法**创建或修改** Iceberg 列默认值。

请求支持的语法：

```sql
-- 1. 建表时设置列默认值
CREATE TABLE local.db.t (
  id INT,
  data STRING DEFAULT 'default-value'
) USING iceberg TBLPROPERTIES ('format-version' = '3');

-- 2. ADD COLUMN 时设置默认值
ALTER TABLE local.db.t ADD COLUMN data STRING DEFAULT 'default-value';

-- 3. 修改已有列默认值
ALTER TABLE local.db.t ALTER COLUMN data SET DEFAULT 'new-value';
```

**当前行为（均失败）：**
- `CREATE TABLE ... DEFAULT ...` → `UNSUPPORTED_FEATURE.TABLE_OPERATION`
- `ALTER TABLE ... ADD COLUMN ... DEFAULT ...` → `Cannot add column since setting default values is currently unsupported`
- `ALTER TABLE ... ALTER COLUMN ... SET DEFAULT ...` → `Cannot apply unknown table change: UpdateColumnDefaultValue`

**关联 Issue：** [#10761](https://github.com/apache/iceberg/issues/10761)（通用列默认值请求）

---

### Issue #16694 — v4: 新增 Manifest 写入支持

> **链接：** https://github.com/apache/iceberg/issues/16694  
> **作者：** stevenzwu  
> **标签：** `improvement`  
> **状态：** Open（已分配 @stevenzwu）

**功能请求：** 将 Iceberg v4 表的写入路径切换到新的 manifest 文件 schema（root/leaf 结构，co-located Deletion Vectors）。

这是 Iceberg v4 格式支持工作的关键写入路径组件，目标是让写入操作能够生成 v4 格式的 manifest 文件。

---

## 总结与趋势

### 本日重点

| 领域 | 活动 |
|------|------|
| **Bug 修复** | 4 个合并 PR 均为 Bug 修复（EOF 处理、时间旅行、参数绑定、函数端点）|
| **生产稳定性** | RESTMetricsReporter 线程泄漏（PR #16695）为严重生产问题，正在 review |
| **云存储** | GCS/S3/ADLS 三大云后端 EOF 处理统一修复 |
| **Spark 4.1** | 两个 Spark 4.1 相关修复（参数绑定 + 行ID保留），4.1 支持日趋成熟 |
| **REST Catalog** | 函数端点规范落地，Iceberg catalog 能力持续扩展 |
| **Iceberg V4** | manifest 写入支持 Issue 开启，v4 格式研发推进 |

### 值得关注

1. **PR #16695（RESTMetricsReporter 线程泄漏）** — 生产环境已观察到数千线程积累，修复方案清晰，预计很快合并
2. **PR #16699（行ID保留）** — 与 Iceberg V3 Row ID 功能高度相关，涉及数据正确性
3. **Issue #16698（Spark V3 列默认值 DDL）** — 功能完整性需求，社区关注度高
4. **Issue #16694（V4 Manifest 写入）** — V4 格式的核心写入路径，代表下一代格式研发方向

---

*报告生成时间：2026-06-07 | 基于 upstream commit `95540ca`*
