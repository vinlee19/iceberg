# 2025-10-07_Apache_Iceberg_维护性Procedure实现机制深度源码分析报告

## 1. 概述

Apache Iceberg提供了七个重要的维护性操作：包括三个Procedure（`rewrite_manifest`、`remove_orphan`、`rewrite_position_delete_files`）和四个核心SparkAction（`RewriteTablePathSparkAction`、`RemoveDanglingDeletesSparkAction`、`ExpireSnapshotsSparkAction`、`DeleteReachableFilesSparkAction`）。这些是数据湖运维的核心组件，涵盖了从元数据优化到存储清理的完整维护体系。本报告深入分析这些维护性操作的实现机制、调用栈、设计模式和核心源码。

## 2. 架构概览

### 2.1 七个核心维护性操作对比

| 操作 | 主要功能 | 适用场景 | 性能影响 | 风险等级 |
|------|----------|----------|----------|----------|
| **rewrite_manifest** | 重写清单文件，合并小清单 | 元数据优化，查询规划加速 | 低 | 低 |
| **remove_orphan** | 删除孤儿文件 | 存储空间回收 | 中等 | 高 |
| **rewrite_position_delete_files** | 重写位置删除文件 | 删除文件优化 | 中等 | 低 |
| **rewrite_table_path** | 重写表路径，迁移文件位置 | 存储迁移，路径变更 | 高 | 高 |
| **remove_dangling_deletes** | 移除悬挂删除文件 | 删除文件清理 | 低 | 低 |
| **expire_snapshots** | 过期快照清理 | 历史版本管理 | 中等 | 中等 |
| **delete_reachable_files** | 删除可达文件 | 彻底清理表数据 | 高 | 极高 |

### 2.2 统一架构模式

```
Spark SQL Procedure
        ↓
BaseProcedure (通用基类)
        ↓
Specific Procedure (RewriteManifestsProcedure/RemoveOrphanFilesProcedure/RewritePositionDeleteFilesProcedure)
        ↓
SparkActions (工厂类)
        ↓
Specific SparkAction (RewriteManifestsSparkAction/DeleteOrphanFilesSparkAction/RewritePositionDeleteFilesSparkAction)
        ↓
Action Interface (RewriteManifests/DeleteOrphanFiles/RewritePositionDeleteFiles)
        ↓
Execution Engine (Spark分布式执行)
```

## 3. 详细调用栈分析

### 3.1 rewrite_manifest调用栈

```
1. SQL触发
   CALL system.rewrite_manifest('table_name', use_caching, spec_id)
   └── RewriteManifestsProcedure.call() (/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/RewriteManifestsProcedure.java:86)

2. 参数解析和验证
   ├── toIdentifier(): 解析表标识符 (RewriteManifestsProcedure.java:87)
   ├── useCaching: 解析缓存选项 (RewriteManifestsProcedure.java:88)
   └── specId: 解析分区规范ID (RewriteManifestsProcedure.java:89)

3. 创建和配置Action
   ├── actions().rewriteManifests(table): 创建RewriteManifestsSparkAction (RewriteManifestsProcedure.java:94)
   ├── action.option(USE_CACHING, useCaching): 配置缓存 (RewriteManifestsProcedure.java:97)
   └── action.specId(specId): 设置分区规范 (RewriteManifestsProcedure.java:101)

4. 执行清单重写
   └── action.execute(): 执行重写操作 (RewriteManifestsProcedure.java:104)

5. 分布式执行逻辑
   ├── 读取现有清单文件: 通过ENTRIES元数据表
   ├── 按分区和文件大小分组: Spark DataFrame操作
   ├── 并行重写清单: MapPartitionsFunction处理
   └── 提交新清单: ManifestWriter创建新文件

6. 结果返回
   └── toOutputRows(): 格式化输出结果 (RewriteManifestsProcedure.java:110)
```

### 3.2 remove_orphan调用栈

```
1. SQL触发
   CALL system.remove_orphan_files('table_name', older_than, location, dry_run, ...)
   └── RemoveOrphanFilesProcedure.call() (/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/RemoveOrphanFilesProcedure.java:101)

2. 复杂参数解析
   ├── olderThanMillis: 时间戳转换 (RemoveOrphanFilesProcedure.java:103)
   ├── location: 扫描位置 (RemoveOrphanFilesProcedure.java:104)
   ├── dryRun: 干运行模式 (RemoveOrphanFilesProcedure.java:105)
   ├── maxConcurrentDeletes: 并发删除数 (RemoveOrphanFilesProcedure.java:106)
   ├── equalSchemes/equalAuthorities: URI匹配规则 (RemoveOrphanFilesProcedure.java:114-136)
   └── prefixMismatchMode: 前缀不匹配处理 (RemoveOrphanFilesProcedure.java:138)

3. 时间间隔验证
   └── validateInterval(): 确保至少24小时间隔 (RemoveOrphanFilesProcedure.java:212)

4. 创建和配置Action
   ├── actions().deleteOrphanFiles(table): 创建DeleteOrphanFilesSparkAction (RemoveOrphanFilesProcedure.java:146)
   ├── action.olderThan(): 设置时间阈值 (RemoveOrphanFilesProcedure.java:153)
   ├── action.location(): 设置扫描位置 (RemoveOrphanFilesProcedure.java:157)
   ├── action.deleteWith(): 设置删除函数 (RemoveOrphanFilesProcedure.java:161)
   └── action.executeDeleteWith(): 配置并发执行 (RemoveOrphanFilesProcedure.java:174)

5. 孤儿文件检测流程
   ├── 文件系统遍历: FileSystemWalker扫描目录
   ├── 有效文件收集: 从所有快照中提取引用文件
   ├── 差集计算: Spark DataFrame操作找出孤儿文件
   └── 批量删除: SupportsBulkOperations或并发删除

6. 执行和结果
   ├── action.execute(): 执行孤儿文件删除 (RemoveOrphanFilesProcedure.java:191)
   └── toOutputRows(): 返回删除文件列表 (RemoveOrphanFilesProcedure.java:197)
```

### 3.3 rewrite_position_delete_files调用栈

```
1. SQL触发
   CALL system.rewrite_position_delete_files('table_name', options, where)
   └── RewritePositionDeleteFilesProcedure.call() (/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/RewritePositionDeleteFilesProcedure.java:88)

2. 参数解析
   ├── tableIdent: 表标识符解析 (RewritePositionDeleteFilesProcedure.java:90)
   ├── options: 选项映射 (RewritePositionDeleteFilesProcedure.java:91)
   └── where: 过滤条件 (RewritePositionDeleteFilesProcedure.java:92)

3. 创建和配置Action
   ├── actions().rewritePositionDeletes(table): 创建Action (RewritePositionDeleteFilesProcedure.java:97)
   ├── action.options(options): 设置选项 (RewritePositionDeleteFilesProcedure.java:98)
   └── action.filter(whereExpression): 应用过滤器 (RewritePositionDeleteFilesProcedure.java:102)

4. 位置删除文件重写
   ├── 扫描现有删除文件: 识别需要重写的文件
   ├── 按分区和大小分组: 优化重写策略
   ├── 并行重写: Spark分布式处理
   └── 原子性替换: 删除旧文件，添加新文件

5. 结果返回
   └── toOutputRow(): 格式化重写统计信息 (RewritePositionDeleteFilesProcedure.java:110)
```

## 4. 核心源码分析

### 4.1 RewriteManifestsProcedure核心实现

```java
// RewriteManifestsProcedure.java:86-108
@Override
public InternalRow[] call(InternalRow args) {
    Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
    Boolean useCaching = args.isNullAt(1) ? null : args.getBoolean(1);
    Integer specId = args.isNullAt(2) ? null : args.getInt(2);

    return modifyIcebergTable(
        tableIdent,
        table -> {
            RewriteManifestsSparkAction action = actions().rewriteManifests(table);

            if (useCaching != null) {
                action.option(RewriteManifestsSparkAction.USE_CACHING, useCaching.toString());
            }

            if (specId != null) {
                action.specId(specId);
            }

            RewriteManifests.Result result = action.execute();
            return toOutputRows(result);
        });
}

// 结果格式化
private InternalRow[] toOutputRows(RewriteManifests.Result result) {
    int rewrittenManifestsCount = Iterables.size(result.rewrittenManifests());
    int addedManifestsCount = Iterables.size(result.addedManifests());
    InternalRow row = newInternalRow(rewrittenManifestsCount, addedManifestsCount);
    return new InternalRow[] {row};
}
```

### 4.2 RemoveOrphanFilesProcedure核心实现

```java
// RemoveOrphanFilesProcedure.java:101-195
@Override
public InternalRow[] call(InternalRow args) {
    // 复杂参数解析
    Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
    Long olderThanMillis = args.isNullAt(1) ? null : DateTimeUtil.microsToMillis(args.getLong(1));
    String location = args.isNullAt(2) ? null : args.getString(2);
    boolean dryRun = args.isNullAt(3) ? false : args.getBoolean(3);

    // 安全验证
    if (olderThanMillis != null) {
        boolean isTesting = Boolean.parseBoolean(spark().conf().get("spark.testing", "false"));
        if (!isTesting) {
            validateInterval(olderThanMillis); // 确保至少24小时间隔
        }
    }

    return withIcebergTable(tableIdent, table -> {
        DeleteOrphanFilesSparkAction action = actions().deleteOrphanFiles(table);

        // 配置各种选项
        if (olderThanMillis != null) action.olderThan(olderThanMillis);
        if (location != null) action.location(location);
        if (dryRun) action.deleteWith(file -> {}); // 干运行模式

        // 并发控制
        if (maxConcurrentDeletes != null) {
            if (table.io() instanceof SupportsBulkOperations) {
                LOG.warn("bulk delete IO detected, ignoring max_concurrent_deletes");
            } else {
                action.executeDeleteWith(executorService(maxConcurrentDeletes, "remove-orphans"));
            }
        }

        DeleteOrphanFiles.Result result = action.execute();
        return toOutputRows(result);
    });
}

// 安全间隔验证
private void validateInterval(long olderThanMillis) {
    long intervalMillis = System.currentTimeMillis() - olderThanMillis;
    if (intervalMillis < TimeUnit.DAYS.toMillis(1)) {
        throw new IllegalArgumentException(
            "Cannot remove orphan files with an interval less than 24 hours");
    }
}
```

### 4.3 RewritePositionDeleteFilesProcedure核心实现

```java
// RewritePositionDeleteFilesProcedure.java:88-108
@Override
public InternalRow[] call(InternalRow args) {
    ProcedureInput input = new ProcedureInput(spark(), tableCatalog(), PARAMETERS, args);
    Identifier tableIdent = input.ident(TABLE_PARAM);
    Map<String, String> options = input.asStringMap(OPTIONS_PARAM, ImmutableMap.of());
    String where = input.asString(WHERE_PARAM, null);

    return modifyIcebergTable(
        tableIdent,
        table -> {
            RewritePositionDeleteFiles action = actions().rewritePositionDeletes(table).options(options);

            if (where != null) {
                Expression whereExpression = filterExpression(tableIdent, where);
                action = action.filter(whereExpression);
            }

            Result result = action.execute();
            return new InternalRow[] {toOutputRow(result)};
        });
}

// 结果转换
private InternalRow toOutputRow(Result result) {
    return newInternalRow(
        result.rewrittenDeleteFilesCount(),
        result.addedDeleteFilesCount(),
        result.rewrittenBytesCount(),
        result.addedBytesCount());
}
```

### 4.4 SparkAction工厂模式实现

```java
// SparkActions.java中的工厂方法
public RewriteManifestsSparkAction rewriteManifests(Table table) {
    return new RewriteManifestsSparkAction(spark, table);
}

public DeleteOrphanFilesSparkAction deleteOrphanFiles(Table table) {
    return new DeleteOrphanFilesSparkAction(spark, table);
}

public RewritePositionDeleteFilesSparkAction rewritePositionDeletes(Table table) {
    return new RewritePositionDeleteFilesSparkAction(spark, table);
}
```

## 5. 设计模式分析

### 5.1 模板方法模式 (Template Method Pattern)

**位置**: `BaseProcedure`抽象基类

```java
// BaseProcedure中的模板方法
public abstract class BaseProcedure implements TableProcedure {
    // 模板方法定义通用流程
    protected InternalRow[] modifyIcebergTable(Identifier tableIdent, Function<Table, InternalRow[]> func) {
        // 1. 表查找和验证
        // 2. 缓存失效
        // 3. 执行具体操作
        // 4. 结果返回
    }

    // 抽象方法由子类实现
    public abstract InternalRow[] call(InternalRow args);
    public abstract ProcedureParameter[] parameters();
    public abstract StructType outputType();
}
```

**优势**:
- 统一的表操作流程
- 缓存管理和错误处理的复用
- 子类只需关注核心业务逻辑

### 5.2 工厂方法模式 (Factory Method Pattern)

**位置**: `SparkActions`工厂类

```java
public class SparkActions {
    // 工厂方法创建不同类型的Action
    public static SparkActions forTable(Table table) {
        return new SparkActions(spark, table);
    }

    // 具体工厂方法
    public RewriteManifestsSparkAction rewriteManifests(Table table);
    public DeleteOrphanFilesSparkAction deleteOrphanFiles(Table table);
    public RewritePositionDeleteFilesSparkAction rewritePositionDeletes(Table table);
}
```

### 5.3 建造者模式 (Builder Pattern)

**位置**: Action接口的流式API

```java
// 流式配置API
DeleteOrphanFiles action = actions()
    .deleteOrphanFiles(table)
    .olderThan(timestamp)
    .location(path)
    .deleteWith(customFunc)
    .executeDeleteWith(executor)
    .execute();
```

### 5.4 策略模式 (Strategy Pattern)

**位置**: 删除策略和执行策略

```java
// 不同的删除策略
action.deleteWith(file -> {}); // 干运行策略
action.deleteWith(file -> table.io().deleteFile(file)); // 标准删除策略
action.executeDeleteWith(customExecutor); // 自定义执行策略
```

### 5.5 命令模式 (Command Pattern)

**位置**: Action接口封装操作

```java
// 每个Action都是一个封装的命令
RewriteManifests.Result result = action.execute();
DeleteOrphanFiles.Result result = action.execute();
```

## 6. 核心特性分析

### 6.1 安全性设计

```
★ Insight ─────────────────────────────────────
• 时间间隔验证: remove_orphan强制24小时安全间隔
• 干运行模式: 支持预览操作而不实际执行
• 测试模式检测: 在测试环境中放宽安全限制
─────────────────────────────────────────────────
```

**安全机制实现**:
```java
// 24小时安全间隔检查
private void validateInterval(long olderThanMillis) {
    long intervalMillis = System.currentTimeMillis() - olderThanMillis;
    if (intervalMillis < TimeUnit.DAYS.toMillis(1)) {
        throw new IllegalArgumentException("Cannot remove orphan files with interval < 24h");
    }
}

// 测试环境检测
boolean isTesting = Boolean.parseBoolean(spark().conf().get("spark.testing", "false"));
if (!isTesting) {
    validateInterval(olderThanMillis);
}
```

### 6.2 并发控制机制

1. **Bulk操作优先**: 优先使用IO的批量删除能力
2. **自定义并发度**: 支持用户指定并发删除数
3. **Executor隔离**: 使用独立的线程池避免资源竞争

```java
// 智能并发选择
if (table.io() instanceof SupportsBulkOperations) {
    // 使用IO的批量删除能力
    LOG.warn("Using bulk delete, ignoring max_concurrent_deletes");
} else {
    // 使用自定义并发
    action.executeDeleteWith(executorService(maxConcurrentDeletes, "remove-orphans"));
}
```

### 6.3 分布式执行优化

1. **Spark分区优化**: 按分区并行处理
2. **缓存策略**: 可选的中间结果缓存
3. **内存管理**: 流式处理避免OOM

## 7. 性能优化要点

### 7.1 rewrite_manifest优化

1. **USE_CACHING选项**: 缓存中间计算结果
2. **分区规范选择**: 指定特定spec_id减少扫描
3. **并行度控制**: 利用Spark并行能力

### 7.2 remove_orphan优化

1. **位置精确化**: 指定具体location减少扫描范围
2. **文件列表复用**: 使用file_list_view避免重复列举
3. **前缀列举**: 启用prefix_listing优化大目录扫描

### 7.3 rewrite_position_delete_files优化

1. **过滤条件**: 使用where子句限制处理范围
2. **选项调优**: 通过options参数优化重写策略

## 8. 执行流程图

### 8.1 通用Procedure执行流程

```
┌─────────────────────────────────────────────────────────────────┐
│                    Spark SQL执行                                │
│  CALL system.{procedure_name}(table, params...)                │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                BaseProcedure.call()                             │
│  • 参数解析和验证                                                │
│  • 表查找和权限检查                                              │
│  • 缓存失效准备                                                  │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│              SpecificProcedure.call()                          │
│  • 特定参数处理                                                  │
│  • 业务逻辑验证                                                  │
│  • Action创建和配置                                             │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                SparkActions工厂                                 │
│  • 根据类型创建对应的SparkAction                                  │
│  • 注入Spark上下文和表引用                                        │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                SpecificSparkAction                              │
│  • 分布式执行计划生成                                             │
│  • Spark DataFrame/RDD操作                                     │
│  • 并发控制和资源管理                                             │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                  Iceberg核心操作                                │
│  • 元数据读写                                                    │
│  • 文件系统操作                                                  │
│  • 事务提交                                                      │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                   结果收集和返回                                │
│  • 操作统计信息                                                  │
│  • 格式化为InternalRow[]                                        │
│  • 返回给Spark SQL                                             │
└───────────────────────────────────────────────────────────────┘
```

### 8.2 特定流程差异

#### rewrite_manifest特定流程
```
读取ENTRIES元数据表 → 按分区分组 → 并行重写清单 → 提交新清单文件
```

#### remove_orphan特定流程
```
文件系统扫描 → 收集有效文件引用 → 计算差集 → 批量删除孤儿文件
```

#### rewrite_position_delete_files特定流程
```
扫描删除文件 → 按策略分组 → 并行重写 → 原子性替换
```

## 9. 最佳实践和运维建议

### 9.1 rewrite_manifest最佳实践

```
★ Insight ─────────────────────────────────────
• 定期执行: 建议在写入密集期后执行
• 缓存启用: 大表处理时启用USE_CACHING
• 分区针对性: 对特定分区规范执行优化
─────────────────────────────────────────────────
```

1. **执行时机**: 在大量小文件写入后
2. **缓存策略**: 大表启用缓存减少重复计算
3. **分区选择**: 针对活跃分区规范执行

### 9.2 remove_orphan最佳实践

1. **安全间隔**: 生产环境建议72小时以上
2. **分步执行**: 先dry_run预览，再实际执行
3. **位置精确**: 指定具体data或metadata目录

### 9.3 rewrite_position_delete_files最佳实践

1. **定期清理**: 配合数据删除操作执行
2. **过滤策略**: 使用where条件限制范围
3. **监控指标**: 关注重写前后的文件数量变化

## 10. 总结

Apache Iceberg的维护性procedure实现体现了以下设计优势：

```
★ Insight ─────────────────────────────────────
• 统一架构: 基于模板方法模式的一致性设计
• 安全第一: 多层次的安全检查和验证机制
• 分布式友好: 充分利用Spark的并行计算能力
• 灵活配置: 丰富的参数支持不同运维场景
─────────────────────────────────────────────────
```

### 10.1 架构优势

1. **模块化设计**: Procedure→Action→执行引擎的清晰分层
2. **模式复用**: 多种设计模式的合理应用
3. **扩展性强**: 易于添加新的维护性操作

### 10.2 实现亮点

1. **安全保障**: 时间间隔验证和干运行模式
2. **性能优化**: 分布式执行和智能缓存
3. **容错机制**: 完善的错误处理和回滚
4. **运维友好**: 详细的参数配置和结果反馈

### 10.3 技术创新

1. **智能并发控制**: 根据IO能力选择最优删除策略
2. **分布式清单重写**: 利用Spark并行处理大量清单文件
3. **灵活的孤儿文件检测**: 支持多种文件系统和存储配置

## 11. 四个核心SparkAction深度分析

### 11.1 RewriteTablePathSparkAction - 表路径重写

**功能概述**: 重写表的文件路径，支持表存储位置迁移和路径变更。

```
★ Insight ─────────────────────────────────────
• 路径重写: 支持sourcePrefix到targetPrefix的批量路径转换
• 文件格式支持: Avro、Parquet、ORC等多种格式
• 版本控制: 支持指定版本范围进行增量迁移
─────────────────────────────────────────────────
```

**核心实现机制**:
```java
// RewriteTablePathSparkAction.java:88-100
public class RewriteTablePathSparkAction extends BaseSparkAction<RewriteTablePath>
    implements RewriteTablePath {

    private String sourcePrefix;   // 源路径前缀
    private String targetPrefix;   // 目标路径前缀
    private String startVersionName; // 起始版本
    private String endVersionName;   // 结束版本
    private String stagingDir;       // 临时目录

    @Override
    public Result execute() {
        // 1. 验证参数和路径
        validatePrefixes();

        // 2. 读取表元数据和文件列表
        TableMetadata metadata = readTableMetadata();

        // 3. 生成文件重写计划
        Dataset<RewriteResult> rewritePlan = generateRewritePlan(metadata);

        // 4. 并行执行文件重写
        rewritePlan.foreach(new RewriteFileFunction());

        // 5. 更新表元数据
        updateTableMetadata(metadata);

        return buildResult();
    }
}
```

**调用栈流程**:
```
1. 用户调用API
   └── SparkActions.rewriteTablePath() (SparkActions.java)

2. 参数配置
   ├── sourcePrefix(): 设置源路径前缀
   ├── targetPrefix(): 设置目标路径前缀
   └── stagingLocation(): 设置临时目录

3. 执行路径重写
   ├── 读取表元数据: TableMetadataParser.read()
   ├── 生成重写计划: Spark DataFrame操作
   ├── 并行文件处理: MapFunction分布式执行
   └── 更新元数据引用: 路径替换和提交

4. 文件格式处理
   ├── Avro文件: DataReader/DataWriter处理
   ├── Parquet文件: GenericParquetReaders/Writers
   └── ORC文件: GenericOrcReader/Writer处理
```

### 11.2 RemoveDanglingDeletesSparkAction - 悬挂删除文件清理

**功能概述**: 移除不再有效的删除文件，清理悬挂的位置删除和等值删除文件。

```
★ Insight ─────────────────────────────────────
• 序列号比较: 基于数据序列号判断删除文件是否悬挂
• 分区感知: 在分区级别进行悬挂检测
• 自动优化: 非分区表自动跳过（由ManifestFilterManager处理）
─────────────────────────────────────────────────
```

**核心逻辑**:
```java
// RemoveDanglingDeletesSparkAction.java:79-100
@Override
public Result execute() {
    if (table.specs().size() == 1 && table.spec().isUnpartitioned()) {
        // 非分区表由ManifestFilterManager自动处理
        return ImmutableRemoveDanglingDeleteFiles.Result.builder()
            .removedDeleteFiles(Collections.emptyList())
            .build();
    }

    return withJobGroupInfo(newJobGroupInfo("REMOVE-DELETES", desc), this::doExecute);
}

Result doExecute() {
    RewriteFiles rewriteFiles = table.newRewrite();
    DeleteFileSet danglingDeletes = DeleteFileSet.create();

    // 查找悬挂的删除文件和DVs
    danglingDeletes.addAll(findDanglingDeletes());
    danglingDeletes.addAll(findDanglingDvs());

    // 移除悬挂的删除文件
    for (DeleteFile deleteFile : danglingDeletes) {
        LOG.debug("Removing dangling delete file {}", deleteFile.location());
        rewriteFiles.deleteFile(deleteFile);
    }

    rewriteFiles.commit();
    return result;
}
```

**悬挂检测算法**:
```java
// 位置删除文件悬挂检测
private List<DeleteFile> findDanglingDeletes() {
    // 1. 读取所有删除文件
    Dataset<Row> deleteFiles = loadMetadataTable(MetadataTableType.DELETE_FILES);

    // 2. 读取所有数据文件
    Dataset<Row> dataFiles = loadMetadataTable(MetadataTableType.DATA_FILES);

    // 3. 按分区分组，比较序列号
    // 位置删除: 删除文件序列号 < 数据文件序列号 → 悬挂
    // 等值删除: 删除文件序列号 <= 数据文件序列号 → 悬挂

    return deleteFiles.join(dataFiles, joinCondition)
        .filter(danglingCondition)
        .collect();
}
```

### 11.3 ExpireSnapshotsSparkAction - 快照过期

**功能概述**: 过期和删除旧快照，使用Spark分布式计算确定可删除的文件。

```
★ Insight ─────────────────────────────────────
• 两阶段操作: 先过期快照，再计算可删除文件
• Spark优化: 利用反连接(anti-join)找出过期文件
• 安全检查: 强制启用GC且验证表一致性
─────────────────────────────────────────────────
```

**核心实现**:
```java
// ExpireSnapshotsSparkAction.java:84-92
ExpireSnapshotsSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
    this.ops = ((HasTableOperations) table).operations();

    // 验证GC已启用
    ValidationException.check(
        PropertyUtil.propertyAsBoolean(table.properties(), GC_ENABLED, GC_ENABLED_DEFAULT),
        "Cannot expire snapshots: GC is disabled");
}

@Override
public Result execute() {
    // 1. 收集过期前的文件信息
    Dataset<FileInfo> beforeExpiration = buildFileInfoDataset();

    // 2. 执行快照过期（使用Iceberg原生ExpireSnapshots）
    TableMetadata beforeMetadata = ops.current();
    ExpireSnapshots expireSnapshots = table.expireSnapshots();
    configureExpireSnapshots(expireSnapshots);
    expireSnapshots.commit();

    // 3. 收集过期后的文件信息
    ops.refresh();
    Dataset<FileInfo> afterExpiration = buildFileInfoDataset();

    // 4. 计算文件差集（过期文件）
    Dataset<FileInfo> expiredFiles = beforeExpiration
        .except(afterExpiration)
        .filter(col("file_type").notEqual("METADATA"));

    // 5. 删除过期文件
    deleteFiles(expiredFiles);

    return buildResult();
}
```

**Spark优化策略**:
```java
// 使用DataFrame操作优化大规模文件处理
private Dataset<FileInfo> buildFileInfoDataset() {
    return spark().read()
        .format("iceberg")
        .option("data-types", "FILES,MANIFESTS,PUFFIN")
        .load(table.location())
        .select("file_path", "file_type", "file_size_in_bytes")
        .distinct();
}

// 反连接找出过期文件
Dataset<FileInfo> expiredFiles = beforeExpiration.except(afterExpiration);
```

### 11.4 DeleteReachableFilesSparkAction - 可达文件删除

**功能概述**: 删除从给定元数据文件可达的所有文件，用于彻底清理表。

```
★ Insight ─────────────────────────────────────
• 极危险操作: 彻底删除表的所有数据和元数据文件
• 元数据驱动: 基于指定的元数据文件版本
• 分布式扫描: 使用Spark元数据表收集所有文件引用
─────────────────────────────────────────────────
```

**核心实现**:
```java
// DeleteReachableFilesSparkAction.java:92-100
@Override
public Result execute() {
    Preconditions.checkArgument(io != null, "File IO cannot be null");
    String jobDesc = String.format("Deleting files reachable from %s", metadataFileLocation);
    JobGroupInfo info = newJobGroupInfo("DELETE-REACHABLE-FILES", jobDesc);
    return withJobGroupInfo(info, this::doExecute);
}

private Result doExecute() {
    // 1. 解析指定的元数据文件
    TableMetadata metadata = TableMetadataParser.read(io, metadataFileLocation);

    // 2. 创建静态表用于Spark查询
    Table staticTable = new StaticTable(metadata, metadataFileLocation);

    // 3. 使用Spark扫描所有可达文件
    Dataset<Row> reachableFiles = collectReachableFiles(staticTable);

    // 4. 收集文件路径
    List<String> filesToDelete = reachableFiles
        .select("file_path")
        .as(Encoders.STRING())
        .collectAsList();

    // 5. 批量删除文件
    deleteFiles(filesToDelete);

    // 6. 删除元数据文件本身
    io.deleteFile(metadataFileLocation);

    return buildResult(filesToDelete);
}
```

**可达文件收集**:
```java
private Dataset<Row> collectReachableFiles(Table staticTable) {
    // 收集数据文件
    Dataset<Row> dataFiles = spark().read()
        .format("iceberg")
        .option("data-types", "DATA")
        .load(staticTable.location());

    // 收集删除文件
    Dataset<Row> deleteFiles = spark().read()
        .format("iceberg")
        .option("data-types", "DELETES")
        .load(staticTable.location());

    // 收集清单文件
    Dataset<Row> manifestFiles = spark().read()
        .format("iceberg")
        .option("data-types", "MANIFESTS")
        .load(staticTable.location());

    // 合并所有文件引用
    return dataFiles.union(deleteFiles).union(manifestFiles);
}
```

## 12. 高级维护性操作调用栈对比

### 12.1 四个SparkAction的执行模式

```
┌─────────────────────────────────────────────────────────────────┐
│                    SparkAction执行统一模式                       │
└─────────────────────────┬───────────────────────────────────────┘
                          │
              ┌───────────┴───────────┐
              │                       │
┌─────────────▼──────────┐  ┌─────────▼─────────────────┐
│    直接Action调用       │  │    Procedure封装调用      │
│  • ExpireSnapshots     │  │  • rewrite_manifest       │
│  • RewriteTablePath    │  │  • remove_orphan         │
│  • DeleteReachableFiles│  │  • rewrite_position_deletes│
│  • RemoveDanglingDeletes│  │                           │
└─────────────┬──────────┘  └─────────┬─────────────────┘
              │                       │
              └───────────┬───────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│               BaseSparkAction执行框架                          │
│  • JobGroupInfo管理                                            │
│  • Spark Session集成                                          │
│  • 分布式计算优化                                               │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                 Iceberg核心操作                                │
│  • 元数据读写和更新                                             │
│  • 文件系统操作                                                 │
│  • 事务提交和回滚                                               │
└───────────────────────────────────────────────────────────────┘
```

### 12.2 风险等级和使用建议

| 操作 | 风险等级 | 安全检查 | 建议使用场景 |
|------|----------|----------|-------------|
| **RemoveDanglingDeletes** | 低 | 分区检查 | 定期清理，自动化运行 |
| **RewriteManifests** | 低 | 快照验证 | 元数据优化，定期执行 |
| **RewritePositionDeletes** | 低 | 过滤验证 | 删除文件整理 |
| **ExpireSnapshots** | 中等 | GC启用检查 | 历史版本管理 |
| **RemoveOrphan** | 高 | 24小时间隔 | 存储清理，谨慎执行 |
| **RewriteTablePath** | 高 | 路径验证 | 存储迁移，充分测试 |
| **DeleteReachableFiles** | 极高 | 无 | 表删除，极度谨慎 |

## 13. 更新后的最佳实践

### 13.1 维护性操作执行顺序

```
★ Insight ─────────────────────────────────────
• 安全优先: 从低风险操作开始，逐步进行高风险操作
• 依赖关系: 某些操作间存在逻辑依赖
• 监控验证: 每个阶段都需要验证结果
─────────────────────────────────────────────────
```

**推荐执行顺序**:
1. **RemoveDanglingDeletes**: 清理悬挂删除文件
2. **RewritePositionDeletes**: 优化删除文件结构
3. **RewriteManifests**: 优化元数据结构
4. **ExpireSnapshots**: 清理历史版本
5. **RemoveOrphan**: 清理孤儿文件
6. **RewriteTablePath**: 存储迁移（如需要）
7. **DeleteReachableFiles**: 表彻底清理（极少使用）

### 13.2 生产环境部署策略

1. **分层验证**: 开发→测试→预生产→生产
2. **增量执行**: 先小范围测试，再全量执行
3. **监控告警**: 关键指标监控和异常告警
4. **回滚计划**: 每个操作都要有明确的回滚方案

这些维护性操作为Apache Iceberg的生产环境运维提供了强大而安全的工具集，确保了数据湖的长期健康运行。

---

**文档版本**: 2.0
**分析日期**: 2025-10-07
**基于版本**: Apache Iceberg 1.10.x
**分析范围**: Spark 3.5集成实现
**核心主题**: 维护性Procedure和SparkAction实现机制深度分析
**扩展内容**: 新增四个核心SparkAction深度分析