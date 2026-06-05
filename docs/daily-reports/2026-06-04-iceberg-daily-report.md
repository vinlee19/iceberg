# Apache Iceberg 社区日报 — 2026-06-04

> **同步状态** ✅ Fork `vinlee19/iceberg` 已与 `apache/iceberg` main 分支成功同步（快进合并 2 个新提交）

---

## 📊 当日活动总览

| 类别 | 数量 |
|------|------|
| 🔀 合并的 PR | 2 |
| 🆕 新增 PR | 2 |
| 🐛 新增 Issue | 0 |
| 📁 新增文件 | 6 |
| ➕ 新增代码行 | 582 |

---

## 🔀 合并的 PR 详细分析

### PR #16516 — Site: Add 1.11.0 release blog post

- **合并时间**: 2026-06-04 16:20:48 UTC
- **作者**: [@aihuaxu](https://github.com/aihuaxu)
- **协作者**: [@gaborkaszab](https://github.com/gaborkaszab)、Claude Sonnet 4.6
- **标签**: `docs`
- **链接**: https://github.com/apache/iceberg/pull/16516

#### 📝 改动概述

新增一篇完整的 Apache Iceberg **1.11.0 发布博客文章**，对外正式发布版本说明。

**改动文件**:

```
site/docs/blog/posts/2026-05-19-iceberg-1.11.0-release.md  (+198 行)
```

#### 🌟 博客文章核心内容

该博客文章全面介绍了 1.11.0 版本的重要特性，主要分为以下几个维度：

---

##### 1. REST Catalog 协议重大升级

```
┌─────────────────────────────────────────────────────────┐
│              REST Catalog 1.11.0 新特性矩阵               │
├─────────────────────┬───────────────────────────────────┤
│ 特性                │ 说明                               │
├─────────────────────┼───────────────────────────────────┤
│ 远程 Scan Planning  │ 服务端直接返回 FileScanTask，减少  │
│                     │ Driver 内存压力                    │
├─────────────────────┼───────────────────────────────────┤
│ 增量 Scan 支持      │ 扩展至 Structured Streaming        │
├─────────────────────┼───────────────────────────────────┤
│ Metadata Table Scan │ 支持 history/snapshots 等元数据表  │
├─────────────────────┼───────────────────────────────────┤
│ Freshness-aware加载 │ ETag-based 缓存，返回 304 减少RTT  │
├─────────────────────┼───────────────────────────────────┤
│ 幂等键支持          │ Idempotency-Key 防止重复写入       │
├─────────────────────┼───────────────────────────────────┤
│ Register View       │ 视图可通过 REST API 注册           │
├─────────────────────┼───────────────────────────────────┤
│ 自定义 Operations   │ 可注入自定义 TableOperations       │
└─────────────────────┴───────────────────────────────────┘
```

远程 Scan Planning 架构变化：

```
旧架构（客户端 scan planning）:
Client ──────────► Catalog Server (获取 manifest list)
Client ──────────► Object Storage (读取 manifests)
Client (本地计算 FileScanTasks) ──────────► Executors

新架构（服务端 scan planning）:
Client ──────────► Catalog Server
                   Catalog Server (服务端计算 FileScanTasks)
Client ◄──────────── FileScanTask 流
Client ──────────► Executors (直接读取数据)
```

##### 2. OpenAPI 规范更新

新增/变更的 REST 端点：

```
POST /v1/{prefix}/views/{view}/register  ← 新增：Register View
GET  /v1/config                          ← 404 when warehouse missing
PUT  /v1/{prefix}/tables/{table}         ← 增加 ETag on CommitTableResponse
POST /v1/{prefix}/tables/{table}/plan    ← 返回 storage credentials
```

##### 3. SQL UDF 规范 & 地理空间类型

```
新增类型系统:
├── SQL UDF 规范
│   ├── 版本化函数存储
│   ├── 支持多 SQL 方言
│   └── 跨引擎可移植
│
└── 地理空间支持
    ├── 原生 bounding box 类型
    ├── INTERSECTS 谓词
    └── V3 geometry 限制说明
```

##### 4. 性能与可靠性

| 优化项 | 效果 |
|--------|------|
| LIMIT 下推 | 找到足够行后停止扫描，减少 I/O 数量级 |
| 向量化读取新编码 | BYTE_STREAM_SPLIT / DELTA_LENGTH_BYTE_ARRAY / DELTA_BYTE_ARRAY 不再回退行读 |
| 唯一表路径 | UUID 后缀防止 DeleteOrphanFiles 误删重命名表文件 |
| 凭证定时刷新 | AWS S3FileIO / GCS FileIO 主动轮换，防止长任务凭证过期 |

##### 5. Table Format V4 基础建设

```
V4 核心接口层级:
TrackedFile
  └── TrackingInfo
        └── ContentInfo
              └── ManifestStats

FormatModel (可插拔格式抽象)
  ├── ParquetFormatModel
  ├── OrcFormatModel
  ├── AvroFormatModel
  └── ArrowFormatModel
```

##### 6. 引擎支持矩阵

```
Spark:
  ✅ Spark 4.1 正式支持
  ✅ MERGE INTO schema evolution
  ✅ Shredded Variant 写入
  ✅ 异步 micro-batch planner
  ✅ 自适应 split sizing
  ⚠️  Spark 3.4 标记废弃

Flink:
  ✅ Flink 2.1 正式支持
  ✅ row_id / _last_updated_sequence_number lineage
  ✅ Deletion Vector 支持
  ✅ Variant 类型支持
  ✅ 列删除 Schema Evolution
  ❌ Flink 1.19 已移除
```

##### 7. 破坏性变更

```
⚠️  BREAKING CHANGES:
├── Apache DataFusion Comet 集成已移除
├── Java 11 不再支持 → 最低 Java 17
└── Flink 1.19 不再支持 → 升级到 Flink 1.20+
```

##### 8. 依赖版本更新

| 依赖 | 旧版本 | 新版本 |
|------|--------|--------|
| AWS SDK | 2.33.0 | 2.44.4 |
| Parquet | 1.16.0 | 1.17.1 |
| ORC | 1.9.7 | 1.9.8 |
| Avro | 1.12.0 | 1.12.1 |
| Nessie | 0.104.5 | 0.107.5 |
| Guava | 33.4.8-jre | 33.6.0-jre |
| Hadoop | 3.4.1 | 3.4.3 |
| Jetty | 11.0.26 | 12.1.8 |

---

### PR #16160 — API, Core: Add CatalogObjectIdentifier

- **合并时间**: 2026-06-04 22:04:10 UTC
- **作者**: [@stevenzwu](https://github.com/stevenzwu)
- **标签**: `API`、`core`
- **关联规范 PR**: #16144
- **链接**: https://github.com/apache/iceberg/pull/16160

#### 📝 改动概述

在 Java 实现层面为 REST OpenAPI 规范中定义的 `CatalogObjectIdentifier` 添加完整的参考实现，包含 API 层 POJO、Core 层 JSON 序列化/反序列化，以及 REST 层的注册。

**改动文件统计**:

```
文件                                                    改动
─────────────────────────────────────────────────────────────────
api/.../catalog/CatalogObjectIdentifier.java       +96  行（新增）
api/.../catalog/TestCatalogObjectIdentifier.java  +101  行（新增）
core/.../catalog/CatalogObjectIdentifierParser.java +62  行（新增）
core/.../rest/RESTSerializers.java                  +24  行（修改）
core/.../TestCatalogObjectIdentifierParser.java    +101  行（新增）
─────────────────────────────────────────────────────────────────
合计                                               +384  行
```

#### 🏗️ 架构设计分析

##### 类层次结构

```
CatalogObjectIdentifier           (api/catalog 层)
├── 对标 Namespace 设计，结构完全镜像
├── 静态工厂方法: of(String... levels)
├── 空指针验证 + null-byte 字符验证
└── 访问器: levels() / level(i) / length()

CatalogObjectIdentifierParser     (core/catalog 层)
├── JSON serde，对标 TableIdentifierParser
└── 序列化为裸 JSON 字符串数组: ["a","b","c"]

RESTSerializers                   (core/rest 层)
└── 注册 serializer/deserializer 对
```

##### 核心实现解析

```java
// CatalogObjectIdentifier 核心逻辑
public class CatalogObjectIdentifier {
  // null-byte 检测模式 (安全防护)
  private static final Predicate<String> CONTAINS_NULL_CHARACTER =
      Pattern.compile(" ", Pattern.UNICODE_CHARACTER_CLASS).asPredicate();

  // 工厂方法 (对标 Namespace.of())
  public static CatalogObjectIdentifier of(String... levels) {
    return new CatalogObjectIdentifier(levels);
  }

  // 双重验证: 非空数组 + 每个 level 非 null + 非 null-byte
  private CatalogObjectIdentifier(String[] levels) {
    Preconditions.checkArgument(null != levels, ...);
    for (String level : levels) {
      Preconditions.checkNotNull(level, ...);
      Preconditions.checkArgument(!CONTAINS_NULL_CHARACTER.test(level), ...);
    }
    this.levels = levels;
  }

  // toString 使用 . 分隔符 (如 "db.table")
  public String toString() { return DOT.join(levels); }
}
```

```java
// CatalogObjectIdentifierParser — JSON 序列化为字符串数组
// 输入:  CatalogObjectIdentifier.of("db", "table")
// 输出:  ["db", "table"]
```

##### 设计对比：CatalogObjectIdentifier vs Namespace vs TableIdentifier

```
┌──────────────────────────┬────────────────┬──────────────────────────┐
│ 类型                     │ 用途           │ JSON 序列化              │
├──────────────────────────┼────────────────┼──────────────────────────┤
│ Namespace                │ 命名空间       │ ["a", "b"]               │
│ TableIdentifier          │ 表标识         │ {"namespace":["a"],...}  │
│ CatalogObjectIdentifier  │ 任意 Catalog   │ ["a", "b", "c"]          │
│                          │ 对象（通用）   │ (裸数组，无类型歧义)     │
└──────────────────────────┴────────────────┴──────────────────────────┘
```

##### 数据流图

```
REST 请求 (JSON)
      │
      ▼
RESTSerializers.deserialize()
      │
      ▼
CatalogObjectIdentifierParser.fromJson()
      │  解析 JSON 数组 → String[]
      ▼
CatalogObjectIdentifier.of(levels)
      │  验证: 非 null + 非 null-byte
      ▼
业务逻辑处理
      │
      ▼
CatalogObjectIdentifierParser.toJson()
      │  String[] → JSON 数组
      ▼
RESTSerializers.serialize()
      │
      ▼
REST 响应 (JSON)
```

#### 🧪 测试覆盖

```
TestCatalogObjectIdentifier (api 层)        — 101 行
├── 正常创建: of() 各种 level 组合
├── toString() 点分隔验证
├── equals()/hashCode() 正确性
├── 边界条件: 空数组、单 level
└── 异常场景:
    ├── null 数组 → IllegalArgumentException
    ├── null level → NullPointerException
    └── null-byte 字符 → IllegalArgumentException

TestCatalogObjectIdentifierParser (core 层) — 101 行
├── JSON 序列化往返测试
├── 空标识符序列化
├── 多级标识符序列化
└── 格式验证
```

---

## 🆕 新增 PR（待审核）

### PR #16681 — Core, Kafka Connect: Support additional Avro temporal logical types

- **创建时间**: 2026-06-04 11:18:43 UTC
- **作者**: [@JustMaris](https://github.com/JustMaris)
- **标签**: `core`、`KAFKACONNECT`
- **状态**: Open（待审核）
- **链接**: https://github.com/apache/iceberg/pull/16681

#### 📋 改动摘要

为 Iceberg 添加对 Avro 时态逻辑类型（temporal logical types）的完整支持，覆盖 Core 读取路径和 Kafka Connect Sink 两个独立代码路径。

##### 问题背景

Kafka/Avro Java 服务（通过 Confluent AvroConverter）常见地生成以下类型，而 Iceberg 当前不支持：

```
❌ 当前不支持的 Avro 逻辑类型:
├── time-millis         → 抛 IllegalArgumentException: Unknown logical type
├── local-timestamp-millis  → 同上
├── local-timestamp-micros  → 同上
└── local-timestamp-nanos   → 同上
```

##### Core 层修复（Avro 读取路径）

```
Avro 逻辑类型          →   Iceberg 类型
─────────────────────────────────────────────────────
time-millis (int)      →   time (ms → µs 缩放)
local-timestamp-millis →   timestamp (without zone)
local-timestamp-micros →   timestamp (without zone)
local-timestamp-nanos  →   timestamp_ns (without zone)
```

> 说明: `local-timestamp-*` 按定义不含时区，始终映射到 without-zone 类型。**写路径不变**，Iceberg 仍输出 `timestamp-micros`/`timestamp-nanos`（带 adjust-to-utc）。

涉及修改组件：
- `SchemaToType`
- `GenericAvroReader`、`InternalReader`、`DataReader`、`PlannedDataReader`

##### Kafka Connect 层修复（Sink）

```
Connect Schema Name             →   Iceberg 类型
───────────────────────────────────────────────────────────
timestamp-micros               →   timestamptz
timestamp-nanos                →   timestamptz_ns
local-timestamp-millis/micros  →   timestamp
local-timestamp-nanos          →   timestamp_ns
time-micros                    →   time
```

**注意**: `*-nanos` 类型需要表格式 **v3+**。

##### 向后兼容性

```
旧行为 (无 schema name / schemaless):
  数值类型 → 按毫秒解释 ✅ (不变)

新行为 (有 schema name):
  按实际单位缩放，不再丢失时态语义 ✅
```

---

### PR #16680 — Build: Bump caffeine from 2.9.3 to 3.2.0

- **创建时间**: 2026-06-04 10:21:08 UTC
- **作者**: [@moomindani](https://github.com/moomindani)
- **标签**: `spark`、`core`、`flink`、`KAFKACONNECT`
- **状态**: Open（待审核）
- **链接**: https://github.com/apache/iceberg/pull/16680

#### 📋 改动摘要

将 Caffeine 缓存库从 **2.9.3**（2021-12-03）升级到 **3.2.0**（2025-01）。

##### 升级原因

```
1. 前向兼容性:
   Caffeine 3.x 用 VarHandle 替代 sun.misc.Unsafe
   → Unsafe 内存访问在新 JDK 上已废弃并警告
   → 对 Iceberg + Spark/Flink 在新版 Java 运行至关重要

2. expireAfterWrite 优化:
   3.2 版本扩展了该路径的过期优化
   Iceberg 使用场景: StandardEncryptionManager, RESTTableCache, S3 REST signer cache
   PR #14440 拟在 catalog cache 中进一步使用

3. 版本时效性:
   旧版本落后约 4.5 年 (major 版本)
   Dependabot 默认忽略 gradle 生态的 major 升级
```

##### 依赖变更（包含传递依赖）

```
Caffeine 3.x 注解库变更:
  旧: org.checkerframework:checker-qual  (Checker Framework)
  新: org.jspecify:jspecify:1.0          (JSpecify)
      + com.google.errorprone:error_prone_annotations (新版)
```

影响模块的 runtime-deps.txt 需要重新生成：
- spark 3.5 / 4.0 / 4.1
- flink 1.20 / 2.0 / 2.1
- kafka-connect

> **注**: `aws-bundle` 保持不变，因为其通过 AWS SDK 传递依赖的是 Caffeine 2.9。

##### 测试修改

```java
// 旧断言 (Caffeine 2.x 行为确定性):
assertThat(evictionCount).isEqualTo(2);

// 新断言 (Caffeine 3.x 权重驱逐策略可能产生 1 或 2):
assertThat(evictionCount).isGreaterThan(0);
```

只有断言变更，`CacheMetricsReport` 本身逻辑无改动。

---

## 🐛 新增 Issue

当日无新增 Issue。

---

## 📈 趋势分析

### 技术方向观察

```
2026-06-04 活动反映的开发重点:

1. REST Catalog 成熟化
   → PR #16160 (CatalogObjectIdentifier) 是 REST 规范 (#16144) 的 Java 实现
   → 持续完善 REST 协议的类型系统

2. 广泛的格式与类型支持
   → PR #16681 修复 Avro 时态类型覆盖盲区
   → 影响 Kafka Connect 与 Iceberg 的互操作性

3. 基础设施现代化
   → PR #16680 推动依赖库跟进主流版本
   → 为 Java 17+ JDK 的 Unsafe 移除做前置准备

4. 社区里程碑
   → PR #16516 (1.11.0 博客) 标志着该版本正式对外宣布
   → 1.11.0: 1000+ commits, 200+ contributors
```

---

## 🔄 Fork 同步记录

| 操作 | 详情 |
|------|------|
| 同步时间 | 2026-06-05 |
| 来源分支 | `apache/iceberg:main` |
| 目标分支 | `vinlee19/iceberg:main` |
| 同步方式 | `git merge upstream/main --ff-only` |
| 新增提交 | 2 个 |
| 新增文件 | 6 个 |
| 新增代码行 | 582 行 |
| 推送状态 | ✅ 成功 |

---

*本报告由 Claude Code 自动生成 | Apache Iceberg 社区日报 | 覆盖日期: 2026-06-04*
