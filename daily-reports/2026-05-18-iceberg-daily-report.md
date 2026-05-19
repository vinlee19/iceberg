# Apache Iceberg 每日动态报告

**报告日期：** 2026-05-19（覆盖 2026-05-18 全天）
**数据来源：** [apache/iceberg](https://github.com/apache/iceberg)
**Fork 同步时间：** 2026-05-19

---

## 概览统计

```
┌─────────────────────────────────────────────┐
│          Apache Iceberg 每日活动概览          │
│              2026-05-18                      │
├──────────────────────┬──────────────────────┤
│  合并 PR 数量         │         15           │
│  新增 PR 数量         │         12           │
│  新增 Issue 数量      │          6           │
└──────────────────────┴──────────────────────┘
```

> **亮点：** 2026-05-18 是 **Apache Iceberg 1.10.2** 版本正式发布日，大量 PR 围绕版本发布进行文档更新与基础设施调整；同时有多个重要 Bug 修复和功能增强落地。

---

## 一、合并 PR 详细分析（15 个）

### 🏷️ 版本发布类（1.10.2 Release）

---

#### PR #16405 — Doap: Update Doap to reference 1.10.2

| 属性 | 详情 |
|------|------|
| **作者** | amogh-jahagirdar |
| **合并时间** | 2026-05-18 23:52 UTC |
| **类型** | 基础设施 / 版本发布 |
| **链接** | [#16405](https://github.com/apache/iceberg/pull/16405) |

**修改内容：**
更新 DOAP（Description of a Project）文件，将项目描述中的版本引用更新为 1.10.2。DOAP 是 Apache 基金会项目的标准元数据文件，包含项目版本历史和发布信息，供 ASF 基础设施使用。

```
修改文件: doap/iceberg.rdf
变更内容: 版本号引用 → 1.10.2
```

---

#### PR #16404 — infra: add 1.10.2 to issue template

| 属性 | 详情 |
|------|------|
| **作者** | amogh-jahagirdar |
| **合并时间** | 2026-05-18 23:50 UTC |
| **标签** | `INFRA` |
| **链接** | [#16404](https://github.com/apache/iceberg/pull/16404) |

**修改内容：**
在 GitHub Issue 模板中新增 `1.10.2` 版本选项，使用户提交 Bug 报告时能准确标注所使用的 Iceberg 版本。

```
修改文件: .github/ISSUE_TEMPLATE/bug-report.yaml
变更内容: versions 列表新增 "1.10.2"
```

---

#### PR #16403 — Docs: Update Javadocs for 1.10.2

| 属性 | 详情 |
|------|------|
| **作者** | amogh-jahagirdar |
| **合并时间** | 2026-05-18 22:55 UTC |
| **类型** | 文档 |
| **链接** | [#16403](https://github.com/apache/iceberg/pull/16403) |

**修改内容：**
为 1.10.2 版本添加版本化 Javadoc，将 API 文档发布到对应版本目录，便于用户查阅历史版本的 API 参考。

```
操作: 添加 1.10.2 版本 Javadoc 到文档站点
影响: 开发者可通过 iceberg.apache.org 访问 1.10.2 的完整 API 文档
```

---

#### PR #16402 — Docs: add versioned docs for 1.10.2

| 属性 | 详情 |
|------|------|
| **作者** | amogh-jahagirdar |
| **合并时间** | 2026-05-18 22:53 UTC |
| **类型** | 文档 |
| **链接** | [#16402](https://github.com/apache/iceberg/pull/16402) |

**修改内容：**
为 1.10.2 版本添加完整的版本化文档，同时更新 `mkdocs.yaml` 使文档站点默认引用 1.10.2。

```
修改文件:
  - mkdocs.yml            # 更新默认版本引用
  - docs/versioned/1.10.2/ # 新增版本化文档目录
```

---

### 🐛 Bug 修复类

---

#### PR #16237 — Kafka Connect: Surface commit failures instead of silently swallowing them

| 属性 | 详情 |
|------|------|
| **作者** | yadavay-amzn |
| **合并时间** | 2026-05-18 20:44 UTC |
| **标签** | `KAFKACONNECT` |
| **关联 Issue** | #15878 |
| **链接** | [#16237](https://github.com/apache/iceberg/pull/16237) |

**问题描述：**

Kafka Connect 的 `Coordinator` 组件在 `doCommit()` 调用周围捕获了 `Exception` 并仅记录 WARNING 日志。当提交失败时（例如 Glue 检测到并发表更新导致的 `CommitFailedException`），连接器保持 `RUNNING` 状态，**静默丢弃正在传输的数据**，操作员完全无感知。

```
问题场景：
Worker → [写入数据] → S3
                     ↓
Coordinator → [提交元数据] → Glue Catalog
                              ↓ CommitFailedException
                    ⚠️ 仅打印 WARNING，数据丢失！
```

**修复方案：**

```java
// 修复前（问题代码）：
try {
    doCommit();
} catch (Exception e) {
    LOG.warn("Commit failed", e);  // 静默吞掉异常！
}

// 修复后：
try {
    doCommit();
} catch (Exception e) {
    if (commitState.isCommitTimedOut()) {
        LOG.warn("Partial commit failed", e);  // 部分失败：仅记录
    } else {
        throw e;  // 完整提交失败：重新抛出，终止 Coordinator 线程
    }
} finally {
    commitState.endCurrentCommit();  // 清理状态
}
```

**影响范围：**
- 完整提交失败 → 重新抛出异常 → Coordinator 线程终止 → Connect 任务状态变为 `FAILED` → 操作员可感知
- 部分提交失败（超时触发）→ 仅打印 WARN，由下一周期重试

**新增测试：**
- `testCommitFailedExceptionPropagates` — 验证 `CommitFailedException` 能正常传播
- `testCommitError` — 验证 `IllegalArgumentException` 传播
- `testCoordinatorCommittedOffsetValidation` — 验证 `ValidationException` 传播

---

#### PR #16351 — Spark: Fix NPE in RemoveOrphanFiles with prefix_listing for root table location

| 属性 | 详情 |
|------|------|
| **作者** | liuliquan-marshal |
| **合并时间** | 2026-05-18 10:12 UTC |
| **标签** | `core` |
| **关联 Issue** | #16350 |
| **链接** | [#16351](https://github.com/apache/iceberg/pull/16351) |

**问题描述：**

当 Iceberg 表的 location 为存储根路径（如 `s3://bucket/`）时，使用 `prefix_listing=true` 配合 `S3FileIO` 执行孤立文件清理（`RemoveOrphanFiles`）会触发 **NullPointerException**，导致清理操作完全失败。

```
复现场景：
- metadata.json 中 location = "s3://bucket/"（根路径）
- 执行 RemoveOrphanFiles 时启用 prefix_listing=true
- S3FileIO 遍历到存储根节点时返回 null → NPE
```

**修复方案：**

在遍历到存储根节点时增加 null 检查，防止 NPE：

```java
// 修复前
parent = getParent(path);  // 根路径时返回 null
processNode(parent);       // NPE!

// 修复后
parent = getParent(path);
if (parent != null) {      // 增加 null 检查
    processNode(parent);
}
```

**影响：** 所有使用 S3 根路径作为表 location 的用户，执行孤立文件清理时不再崩溃。

---

### 📖 文档与规范类

---

#### PR #16398 — Docs: switch default docs version from nightly to latest

| 属性 | 详情 |
|------|------|
| **作者** | MaxNevermind |
| **合并时间** | 2026-05-18 19:44 UTC |
| **标签** | `docs` |
| **链接** | [#16398](https://github.com/apache/iceberg/pull/16398) |

**背景：**

iceberg.apache.org 文档站点的默认版本和搜索索引此前指向 `nightly`（基于 main 分支的最新文档），这会导致用户看到尚未发布的变更，造成困惑。

**修复方案：**

```
变更前: 默认文档版本 → nightly (未发布内容)
变更后: 默认文档版本 → latest (最新稳定版)

搜索索引调整:
  - 从索引中排除 nightly
  - 仅索引 latest 版本
```

```
修改文件:
  - mkdocs-base.yml    # 调整 nightly/latest 顺序
  - docs/*.md          # 相关引用更新
```

**效果：** 官网文档 Tab 和搜索结果默认展示最新发布版本内容，避免误导用户。

---

#### PR #15834 — Spec: Clarify non-default CRS conventions

| 属性 | 详情 |
|------|------|
| **作者** | milastdbx |
| **合并时间** | 2026-05-18 12:13 UTC |
| **标签** | `Specification` |
| **链接** | [#15834](https://github.com/apache/iceberg/pull/15834) |

**背景：**

Iceberg 地理空间规范（Geospatial spec）中关于非默认坐标参考系（CRS，Coordinate Reference System）的表述存在歧义，不同实现方对相同内容有不同解读。此次修改与 Apache Parquet Format 规范的最新调整保持一致。

**修改内容：**
更新地理空间类型规范文档，明确区分"允许的行为"和"建议的行为"，消除歧义，确保不同实现的互操作性。

```
影响范围: 地理空间数据类型处理（Geometry、Geography 等）
参考: apache/parquet-format#560
```

---

### 🔧 核心功能增强类

---

#### PR #16144 — OpenAPI: Add CatalogObjectIdentifier schema

| 属性 | 详情 |
|------|------|
| **作者** | stevenzwu |
| **合并时间** | 2026-05-18 18:01 UTC |
| **标签** | `OPENAPI` |
| **链接** | [#16144](https://github.com/apache/iceberg/pull/16144) |

**背景：**

多个并行进行的开发工作（Events 端点、Resolve 端点、Functions 端点）都需要一个通用的目录对象标识符，若各自定义会造成标识符类型泛滥。

**解决方案：**

在 REST Catalog OpenAPI 规范中新增 `CatalogObjectIdentifier` schema：

```yaml
# 新增的 schema 结构
CatalogObjectIdentifier:
  type: array
  items:
    type: string
  description: >
    有序的层级列表，例如 ["accounting", "tax", "paid"]
    适用于表、视图、物化视图和命名空间
```

**设计原则：**
- 结构与现有 `Namespace` 完全相同（字符串数组）
- 仅通过名称区分语义（"任意目录对象" vs "命名空间路径"）
- **不修改任何现有端点**，零破坏性变更
- 不含 discriminator enum，各端点按需定义各自的类型枚举

```
验证通过:
  ✅ make -C open-api lint  (openapi-spec-validator + yamllint --strict)
  ✅ make -C open-api generate
  ✅ python3 -m py_compile open-api/rest-catalog-open-api.py
```

---

#### PR #15640 — Core: Use ArrayList for manifest list materialization

| 属性 | 详情 |
|------|------|
| **作者** | manuzhang |
| **合并时间** | 2026-05-18 10:55 UTC |
| **标签** | `core` |
| **链接** | [#15640](https://github.com/apache/iceberg/pull/15640) |

**优化内容：**

将 `BaseSnapshot.allManifests` 的 manifest 列表物化从默认集合类型改为 `ArrayList`，获得更低的内存开销和更好的内存局部性（cache locality）。

```java
// 优化前：使用默认集合（可能为 LinkedList 或 ImmutableList）
List<ManifestFile> allManifests = materializeManifests();

// 优化后：明确使用 ArrayList
ArrayList<ManifestFile> allManifests = new ArrayList<>(manifestCount);
// ArrayList 优势：
//   - 连续内存存储 → CPU 缓存友好
//   - 无额外节点开销（对比 LinkedList）
//   - 随机访问 O(1)
```

**影响：** 在大型 Iceberg 表（manifest 数量多）场景下，读取快照 manifest 列表时内存占用降低，GC 压力减小。

---

### 🏗️ 构建与 CI 优化类

---

#### PR #16357 — Build: Speed up Spark CI with parallel test execution

| 属性 | 详情 |
|------|------|
| **作者** | kevinjqliu |
| **合并时间** | 2026-05-18 15:59 UTC |
| **标签** | `INFRA` |
| **链接** | [#16357](https://github.com/apache/iceberg/pull/16357) |

**问题背景：**

`spark-ci` 是 Iceberg CI 中耗时最长的任务，平均 84 分钟，是第二慢任务（`flink-ci`，21分钟）的 4 倍，严重拖慢 PR 合并速度。

**优化措施：**

| 优化项 | 变更内容 | 效果 |
|--------|----------|------|
| 矩阵拆分 | `tests: [core, extensions]` 分成 2 个并发 job | 关键路径缩短 33% |
| 并行测试 | 启用 `-DtestParallelism=auto`（4 vCPU → 2 forks/JVM） | 最显著的单 job 加速 |
| 并发上限 | `max-parallel: 15` → `20`（Apache 基础设施上限） | 全矩阵单波次完成 |
| 超时保护 | 新增 `timeout-minutes: 90` | 防止 hang 占用资源 |
| 磁盘清理 | 移除 `jlumbroso/free-disk-space` | 节省 1-2 分钟/job |
| Artifact | 名称含 `matrix.tests`，保留 7 天 | 避免命名冲突 |

**实测效果：**

```
                    优化前(main)    优化后(PR)      变化
Wall time:          84 min      →   58 min      -26 min (-31%)
关键路径 job:        86 min      →   58 min      -28 min (-33%)
中位 job 耗时:       78 min      →   42 min      -36 min (-46%)
Matrix jobs 数量:    10          →   20          +10
最大并发:            15          →   20          +5
```

---

#### PR #16381 — Build: Bump gradle-wrapper from 8.14.4 to 8.14.5

| 属性 | 详情 |
|------|------|
| **作者** | huaxingao |
| **合并时间** | 2026-05-18 13:16 UTC |
| **标签** | `build` |
| **链接** | [#16381](https://github.com/apache/iceberg/pull/16381) |

**修改内容：**
将 Gradle Wrapper 从 8.14.4 升级至 8.14.5（bug fix 版本）。此 PR 超代了 #16376（该 PR 因 `./gradlew wrapper` 再生成时移除了自定义 bootstrap 导致 CI 失败）。

```
修改文件:
  - gradle/wrapper/gradle-wrapper.properties  # 版本号更新
  - gradlew                                   # Wrapper 脚本更新
```

---

### 🧪 测试增强类

---

#### PR #16219 — Spark: Add compaction only benchmark - rewrite data files

| 属性 | 详情 |
|------|------|
| **作者** | varun-lakhyani |
| **合并时间** | 2026-05-18 15:03 UTC |
| **标签** | `spark` |
| **链接** | [#16219](https://github.com/apache/iceberg/pull/16219) |

**背景：**

现有的压缩（compaction）基准测试都混入了其他操作的噪音，无法单独衡量"重写数据文件"的纯粹性能。

**新增内容：**

```java
// 新增基类（提供可复用的基础设施）
action/IcebergCompactionBenchmark.java

// 新增纯压缩基准测试
action/IcebergDataCompactionBenchmark.java
```

**可扩展性设计：**

通过重写基类方法，可轻松切换到真实 S3 存储进行更接近生产环境的测试：

```java
// 示例：切换到 S3FileIO
@Override
protected Map<String, String> extraCatalogProperties() {
    return Map.of(
        "catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog",
        "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
        "client.region", "ap-south-1");
}

@Override
protected String getCatalogWarehouse() {
    return "s3a://location-to-bucket/path-to-destination/";
}
```

---

#### PR #16346 — Flink: Add decimal write/read roundtrip test for FlinkParquetReaders

| 属性 | 详情 |
|------|------|
| **作者** | wombatu-kun |
| **合并时间** | 2026-05-18 12:16 UTC |
| **标签** | `data`, `flink` |
| **链接** | [#16346](https://github.com/apache/iceberg/pull/16346) |

**背景：**
`FlinkParquetReaders.BinaryDecimalReader` 中长期存在一个 TODO 注释，缺乏 Flink 写入 → Flink 读取的 decimal 类型往返测试。

**新增测试覆盖：**

| 精度/小数位 | Parquet 物理编码 |
|------------|----------------|
| 9/2        | INT32           |
| 15/3       | INT64           |
| 38/10      | FIXED_LEN_BYTE_ARRAY |

```
影响的 Flink 版本: v1.20, v2.0, v2.1
测试位置: TestFlinkParquetReader（各版本）
TODO 注释: 已移除
```

---

#### PR #16364 — Core, Flink: Add UUID to DataTestBase SUPPORTED_PRIMITIVES

| 属性 | 详情 |
|------|------|
| **作者** | joyhaldar |
| **合并时间** | 2026-05-18 08:13 UTC |
| **标签** | `core`, `flink` |
| **链接** | [#16364](https://github.com/apache/iceberg/pull/16364) |

**修改内容：**

在核心测试基类 `DataTestBase.SUPPORTED_PRIMITIVES` 中添加 UUID 字段，使所有继承 `DataTestBase` 的测试都能自动覆盖 UUID 类型的读写：

```java
// 添加前：UUID 仅在 Flink 专属的 TestFlinkUuidType 中测试
// 添加后：所有继承 DataTestBase 的测试都包含 UUID 读写覆盖
static final Schema SUPPORTED_PRIMITIVES = new Schema(
    // ...其他类型...
    required(25, "uuid_col", Types.UUIDType.get())  // 新增
);
```

同时更新了 `TestFlinkParquetReader` 中手写的 Parquet schema，使其与 `SUPPORTED_PRIMITIVES` 保持同步（影响 Flink 2.1、2.0、1.20）。

---

## 二、新增 Issue 分析（6 个）

### 📋 Issue #16399 — Expose server-assigned resource identifiers (tableId) in LoadTableResponse

| 属性 | 详情 |
|------|------|
| **创建时间** | 2026-05-18 19:56 UTC |
| **标签** | `proposal` |
| **状态** | Open |
| **链接** | [#16399](https://github.com/apache/iceberg/issues/16399) |

**背景：**
REST Catalog 后端（如 S3 Tables）会为表分配服务端 `tableId`，下游系统在联邦访问时需要此 ID 来构建 IAM 策略（`arn:aws:s3tables:::table/{tableId}`）。目前 `tableId` 虽然存在于 HTTP 响应体中，但未通过 `RESTCatalog` 客户端 API 暴露，只能通过包装 HTTP 客户端并拦截原始响应来获取——这种方式极其脆弱。

**提案：** 在 `LoadTableResponse` 的 `config` 映射中包含服务端 resource identifier，或引入专用字段。

---

### 📋 Issue #16397 — spark-ci walltime improvements

| 属性 | 详情 |
|------|------|
| **创建时间** | 2026-05-18 19:13 UTC |
| **状态** | Open |
| **链接** | [#16397](https://github.com/apache/iceberg/issues/16397) |

**背景：**
`spark-ci` 平均耗时 65 分钟，是 `flink-ci`（21 分钟）的 3 倍以上。该 Issue 追踪已完成和待进行的优化项：
- ✅ Gradle 缓存修复（#16356）
- ✅ 并行化执行 + job 拆分（#16357，优化 31%）
- 🔲 **待优化**：减少测试本身的运行时间（分析 Top 25 慢测试，其中很多被参数化矩阵放大）

---

### 📋 Issue #16396 — follow up LICENSE/NOTICE fixes from the 1.11.0 RC4 thread

| 属性 | 详情 |
|------|------|
| **创建时间** | 2026-05-18 19:03 UTC |
| **状态** | Open |
| **链接** | [#16396](https://github.com/apache/iceberg/issues/16396) |

**背景：**
1.11.0 RC4 审查过程中发现多个许可证文件问题，作为后续工作跟踪：

| 组件 | 问题 |
|------|------|
| AWS Bundle | `LICENSE`/`NOTICE` 在 jar 根目录和 `META-INF/` 下各存一份，重复打包 |
| Azure Bundle | `reactor-core`/`reactor-netty` 的 NOTICE 未包含 |
| GCP Bundle | shade 插件中 `META-INF/LICENSE*` 过滤建议 |
| Flink Runtime | `LICENSE`/`NOTICE` 打包两次 |

---

### 📋 Issue #16393 — Kafka Connect: Add bounded retry for transient commit exceptions

| 属性 | 详情 |
|------|------|
| **创建时间** | 2026-05-18 17:02 UTC |
| **标签** | 无 |
| **状态** | Open（贡献者自愿修复）|
| **链接** | [#16393](https://github.com/apache/iceberg/issues/16393) |

**背景：** 是 #16237（今日合并的 Kafka Connect Bug 修复）的后续工作。当前实现中任何完整提交失败都会立即终止 Coordinator，对于瞬时错误（如目录竞争）过于激进。

**提案：** 添加可配置的连续失败阈值（如 3 次），只有连续 N 次失败才终止 Coordinator。

---

### 📋 Issue #16392 — Kafka Connect: Add metric for partial commit failures

| 属性 | 详情 |
|------|------|
| **创建时间** | 2026-05-18 17:02 UTC |
| **状态** | Open（贡献者自愿修复）|
| **链接** | [#16392](https://github.com/apache/iceberg/issues/16392) |

**背景：** 同样是 #16237 的后续工作。在高负载 Connect 集群中，部分提交失败（由 `commitState.isCommitTimedOut()` 触发）可能频繁发生，操作员需要可观测性。

**提案：** 暴露部分提交失败的计数器指标，供告警使用。

---

### 📋 Issue #16389 — Proposal: KAFKA CONNECT: Worker needs to detect the Coordinator's progress

| 属性 | 详情 |
|------|------|
| **创建时间** | 2026-05-18 07:01 UTC |
| **标签** | `proposal` |
| **状态** | Open |
| **链接** | [#16389](https://github.com/apache/iceberg/issues/16389) |

**背景：**

当 Coordinator 遇到 Iceberg Catalog Server 不可用或网络问题时，无法成功提交。但 Worker 仍持续从 Kafka 消费并写入 S3，导致控制 topic 消息积压呈指数级增长。在实际案例中，15 分钟的 HMS 稳定性问题导致 iceberg-kafka-connect 后续数小时内持续性能下降，需要人工干预。

**架构设计：**

```
当前架构（无背压控制）：
Kafka Source → Worker → S3
                ↑
              持续消费
                              Coordinator → [失败] → 重试
                                   ↑
                         积压的控制消息越来越多 ❌

提案架构（有背压控制）：
Kafka Source → Worker → S3
     ↑ 暂停！        ↓
     └──────── 检测 Coordinator 失败次数
                         Coordinator → [失败] → 减压后重试 ✅
```

**提案细节：**

Worker 检测 START_COMMIT 后是否有匹配的 COMMIT_COMPLETE，连续 N 次失败后抛出 `RetriableException`，KC 框架捕获后暂停该 task 的消费。

新增配置参数：
```
iceberg.coordinator.progress.detection.enabled  # 是否启用（默认 false）
iceberg.coordinator.progress.stalled.cycles     # 失败次数阈值（默认 3）
```

**无破坏性变更**（默认关闭，完全向后兼容）。

---

## 三、新增 PR 汇总（12 个）

| PR # | 标题 | 作者 | 状态 | 标签 |
|------|------|------|------|------|
| [#16405](https://github.com/apache/iceberg/pull/16405) | Doap: Update Doap to reference 1.10.2 | amogh-jahagirdar | ✅ 已合并 | — |
| [#16404](https://github.com/apache/iceberg/pull/16404) | infra: add 1.10.2 to issue template | amogh-jahagirdar | ✅ 已合并 | INFRA |
| [#16403](https://github.com/apache/iceberg/pull/16403) | Docs: Update Javadocs for 1.10.2 | amogh-jahagirdar | ✅ 已合并 | docs |
| [#16402](https://github.com/apache/iceberg/pull/16402) | Docs: add versioned docs for 1.10.2 | amogh-jahagirdar | ✅ 已合并 | docs |
| [#16401](https://github.com/apache/iceberg/pull/16401) | Core: Improving comment documentation for SnapshotUtil | jennywang67 | 🔄 Open | core |
| [#16400](https://github.com/apache/iceberg/pull/16400) | REST Spec: Add unregister table endpoint | rdblue | 🔄 Open | OPENAPI |
| [#16398](https://github.com/apache/iceberg/pull/16398) | Docs: switch default docs version from nightly to latest | MaxNevermind | ✅ 已合并 | docs |
| [#16395](https://github.com/apache/iceberg/pull/16395) | Core: cleanExpiredMetadata cleans unreferenced encryption keys | Hugo-WB | 🔄 Draft | core |
| [#16394](https://github.com/apache/iceberg/pull/16394) | Core, OpenApi: Add X-Iceberg-Client-Capabilities header | singhpk234 | 🔄 Open | core, OPENAPI |
| [#16391](https://github.com/apache/iceberg/pull/16391) | Docs: Update information about metrics mode | psvri | 🔄 Open | docs |
| [#16390](https://github.com/apache/iceberg/pull/16390) | Core: Add streaming CloseableIterable accessors to SnapshotChanges | wombatu-kun | 🔄 Open | core |
| [#16388](https://github.com/apache/iceberg/pull/16388) | Core, Spark: Clean up uncommitted files when a staged table is aborted | wombatu-kun | 🔄 Open | API, spark, core |

### 重点关注中的 Open PR

#### PR #16400 — REST Spec: Add unregister table endpoint

已有 `register` 端点（从 Hive 等目录迁移表到 REST catalog），但缺乏 `unregister` 端点（REST → REST 迁移时安全移除表注册而不删除数据）。此 PR 新增：

```
POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/unregister
```

#### PR #16394 — Core, OpenApi: Add X-Iceberg-Client-Capabilities header

引入 `X-Iceberg-Client-Capabilities` HTTP header，允许 REST 客户端向服务端声明自己支持的能力集合，为细粒度访问控制（#13879）等功能铺垫。

#### PR #16388 — Core, Spark: Clean up uncommitted files when a staged table is aborted

修复 `StagedSparkTable.abortStagedChanges()` 中的空实现（`// TODO: clean up`）——当 staged CREATE 操作中止时，已写入的 manifest 和 manifest list 文件永久泄漏，且无法被 `removeOrphanFiles` 清理。

---

## 四、版本发布亮点：Apache Iceberg 1.10.2

> 2026-05-18 是 **Apache Iceberg 1.10.2** 正式发布日

当天合并的 4 个 1.10.2 相关 PR（#16402 ~ #16405）完成了版本发布的最后基础设施步骤：

```
1.10.2 发布检查清单:
  ✅ 版本化文档 (mkdocs)         PR #16402
  ✅ 版本化 Javadoc              PR #16403
  ✅ Issue 模板更新               PR #16404
  ✅ DOAP 文件更新                PR #16405
```

---

## 五、每日活动热力图

```
模块活动分布（按合并 PR 数）

版本发布 (1.10.2)  ████████████████  4 PRs  (26.7%)
Bug 修复           ████████          2 PRs  (13.3%)
文档               ████████          2 PRs  (13.3%)
构建/CI            ████████          2 PRs  (13.3%)
规范/OpenAPI       ████              1 PR   ( 6.7%)
核心优化           ████              1 PR   ( 6.7%)
测试增强           ████████████      3 PRs  (20.0%)
```

```
新增 Issue 分类

Kafka Connect    ████████████████  3 Issues (50%)
CI/构建          ████              1 Issue  (16.7%)
REST API         ████              1 Issue  (16.7%)
许可证/合规      ████              1 Issue  (16.7%)
```

---

## 六、关键趋势观察

1. **1.10.2 发布收尾**：今日大量工作围绕版本发布的善后工作，说明 Iceberg 版本发布流程逐渐成熟规范化。

2. **Kafka Connect 连续关注**：一个 Bug 修复（#16237）触发了 3 个新 Issue（#16392、#16393、#16389），说明社区正在系统性地完善 Kafka Connect 连接器的可靠性和可观测性。

3. **CI 持续优化**：Spark CI 从 84 分钟降至 58 分钟，反映出项目规模增长带来的 CI 负担问题已引起核心维护者的高度重视。

4. **REST API 扩展活跃**：`CatalogObjectIdentifier`（#16144）、`unregister` 端点（#16400）、客户端能力声明（#16394）等多个 OpenAPI 变更并行推进，REST Catalog 规范正在快速演进。

5. **数据加密完善**：`cleanExpiredMetadata` 清理未引用加密密钥（#16395）说明 Iceberg 的加密功能正在向生产就绪迈进。

---

## 七、Fork 同步记录

```bash
# 执行的同步操作
git fetch upstream main
# upstream/main 最新提交: 1802dbf
# Docs: Add release notes for 1.10.2 (#16406)
```

**同步状态：** ✅ 本地 fork 已与 upstream 对齐

---

*报告生成时间：2026-05-19 | 数据截止：2026-05-18 23:59 UTC*
*报告维护分支：claude/happy-ride-1Aj9e*
