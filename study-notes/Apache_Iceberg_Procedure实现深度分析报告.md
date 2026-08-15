# 2025-09-27_Apache_Iceberg_Procedure实现深度源码分析报告

## 概述

本文档详细分析了 Apache Iceberg 中三个核心 Procedure 的实现机制：`rewrite_manifests`、`expire_snapshots` 和 `remove_orphan_files`。需要注意的是，在当前的 Iceberg 源码中，并不存在 `optimize_manifests` 和 `drop_extended_stats` 这两个 procedure。

**重要发现**：通过源码分析发现，这些 procedure 实际上是 Spark 引擎的实现，而非 Trino 的实现。Iceberg 的 procedure 机制是通过 Spark 的 Catalog API 来实现的。

## 1. Procedure 架构设计

### 1.1 整体架构

```
SparkProcedures (注册器)
├── ProcedureBuilder (构建器接口)
├── BaseProcedure (基础抽象类)
└── 具体 Procedure 实现
    ├── RewriteManifestsProcedure
    ├── ExpireSnapshotsProcedure
    └── RemoveOrphanFilesProcedure
```

### 1.2 核心组件

**SparkProcedures 类** (`org.apache.iceberg.spark.procedures.SparkProcedures`)：
- 作为 procedure 注册中心
- 管理所有可用的 procedure 及其构建器
- 提供大小写不敏感的 procedure 名称解析

```java
private static Map<String, Supplier<ProcedureBuilder>> initProcedureBuilders() {
    ImmutableMap.Builder<String, Supplier<ProcedureBuilder>> mapBuilder = ImmutableMap.builder();
    mapBuilder.put("rewrite_manifests", RewriteManifestsProcedure::builder);
    mapBuilder.put("remove_orphan_files", RemoveOrphanFilesProcedure::builder);
    mapBuilder.put("expire_snapshots", ExpireSnapshotsProcedure::builder);
    // ... 其他 procedures
    return mapBuilder.build();
}
```

## 2. rewrite_manifests Procedure 深度分析

### 2.1 功能概述

`rewrite_manifests` procedure 用于重写表的 manifest 文件，优化 manifest 文件的大小和数量，提高查询性能。

### 2.2 实现架构

```
RewriteManifestsProcedure
├── 参数验证和解析
├── RewriteManifestsSparkAction (执行引擎)
└── 结果返回
```

### 2.3 源码分析

**Procedure 定义** (`RewriteManifestsProcedure.java:44`)：

```java
class RewriteManifestsProcedure extends BaseProcedure {
    private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[] {
        ProcedureParameter.required("table", DataTypes.StringType),
        ProcedureParameter.optional("use_caching", DataTypes.BooleanType),
        ProcedureParameter.optional("spec_id", DataTypes.IntegerType)
    };
}
```

**核心执行逻辑** (`RewriteManifestsProcedure.java:86`)：

```java
public InternalRow[] call(InternalRow args) {
    Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
    Boolean useCaching = args.isNullAt(1) ? null : args.getBoolean(1);
    Integer specId = args.isNullAt(2) ? null : args.getInt(2);

    return modifyIcebergTable(tableIdent, table -> {
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
```

### 2.4 RewriteManifestsSparkAction 核心实现

**类职责** (`RewriteManifestsSparkAction.java:78-87`)：
- 分布式重写 manifest 文件
- 根据分区共存元数据
- 支持自定义 predicate 和分区规格

**执行流程** (`RewriteManifestsSparkAction.java:171`)：

```java
private RewriteManifests.Result doExecute() {
    List<ManifestFile> rewrittenManifests = Lists.newArrayList();
    List<ManifestFile> addedManifests = Lists.newArrayList();

    // 分别处理数据文件和删除文件的 manifest
    RewriteManifests.Result dataResult = rewriteManifests(ManifestContent.DATA);
    Iterables.addAll(rewrittenManifests, dataResult.rewrittenManifests());
    Iterables.addAll(addedManifests, dataResult.addedManifests());

    RewriteManifests.Result deletesResult = rewriteManifests(ManifestContent.DELETES);
    Iterables.addAll(rewrittenManifests, deletesResult.rewrittenManifests());
    Iterables.addAll(addedManifests, deletesResult.addedManifests());

    if (rewrittenManifests.isEmpty()) {
        return EMPTY_RESULT;
    }

    replaceManifests(rewrittenManifests, addedManifests);

    return ImmutableRewriteManifests.Result.builder()
        .rewrittenManifests(rewrittenManifests)
        .addedManifests(addedManifests)
        .build();
}
```

**关键优化策略**：

1. **目标文件数计算** (`RewriteManifestsSparkAction.java:321`)：
   ```java
   private int targetNumManifests(long totalSizeBytes) {
       return (int) ((totalSizeBytes + targetManifestSizeBytes - 1) / targetManifestSizeBytes);
   }
   ```

2. **分区感知重写** (`RewriteManifestsSparkAction.java:251`)：
   ```java
   private List<ManifestFile> writePartitionedManifests(
       ManifestContent content, Dataset<Row> manifestEntryDF, int numManifests) {

       return withReusableDS(manifestEntryDF, df -> {
           WriteManifests<?> writeFunc = newWriteManifestsFunc(content, df.schema());
           Column partitionColumn = df.col("data_file.partition");
           Dataset<Row> transformedDF = repartitionAndSort(df, partitionColumn, numManifests);
           return writeFunc.apply(transformedDF).collectAsList();
       });
   }
   ```

## 3. expire_snapshots Procedure 深度分析

### 3.1 功能概述

`expire_snapshots` procedure 用于删除过期的快照及其相关文件，释放存储空间，是 Iceberg 表维护的关键操作。

### 3.2 实现架构

```
ExpireSnapshotsProcedure
├── 参数验证（时间戳、保留数量等）
├── ExpireSnapshotsSparkAction (执行引擎)
├── 文件差异计算 (Spark Dataset 操作)
└── 批量文件删除
```

### 3.3 源码分析

**Procedure 定义** (`ExpireSnapshotsProcedure.java:49`)：

```java
private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[] {
    ProcedureParameter.required("table", DataTypes.StringType),
    ProcedureParameter.optional("older_than", DataTypes.TimestampType),
    ProcedureParameter.optional("retain_last", DataTypes.IntegerType),
    ProcedureParameter.optional("max_concurrent_deletes", DataTypes.IntegerType),
    ProcedureParameter.optional("stream_results", DataTypes.BooleanType),
    ProcedureParameter.optional("snapshot_ids", DataTypes.createArrayType(DataTypes.LongType))
};
```

**执行逻辑** (`ExpireSnapshotsProcedure.java:100`)：

```java
public InternalRow[] call(InternalRow args) {
    Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
    Long olderThanMillis = args.isNullAt(1) ? null : DateTimeUtil.microsToMillis(args.getLong(1));
    Integer retainLastNum = args.isNullAt(2) ? null : args.getInt(2);
    Integer maxConcurrentDeletes = args.isNullAt(3) ? null : args.getInt(3);
    Boolean streamResult = args.isNullAt(4) ? null : args.getBoolean(4);
    long[] snapshotIds = args.isNullAt(5) ? null : args.getArray(5).toLongArray();

    return modifyIcebergTable(tableIdent, table -> {
        ExpireSnapshots action = actions().expireSnapshots(table);

        if (olderThanMillis != null) {
            action.expireOlderThan(olderThanMillis);
        }

        if (retainLastNum != null) {
            action.retainLast(retainLastNum);
        }

        // ... 其他参数配置

        ExpireSnapshots.Result result = action.execute();
        return toOutputRows(result);
    });
}
```

### 3.4 ExpireSnapshotsSparkAction 核心实现

**关键特性** (`ExpireSnapshotsSparkAction.java:49-63`)：
- 使用 Spark 计算过期前后的文件差异
- 支持流式结果处理
- 在删除文件前完全提交快照过期操作
- 通过 shuffle 操作控制并行度

**文件差异计算** (`ExpireSnapshotsSparkAction.java:141`)：

```java
public Dataset<FileInfo> expireFiles() {
    if (expiredFileDS == null) {
        // 获取过期前的元数据
        TableMetadata originalMetadata = ops.current();

        // 执行快照过期
        org.apache.iceberg.ExpireSnapshots expireSnapshots = table.expireSnapshots();
        // ... 配置过期参数
        expireSnapshots.cleanExpiredFiles(false).commit();

        // 获取过期后的有效文件
        TableMetadata updatedMetadata = ops.refresh();
        Dataset<FileInfo> validFileDS = fileDS(updatedMetadata);

        // 获取已删除快照引用的文件
        Set<Long> deletedSnapshotIds = findExpiredSnapshotIds(originalMetadata, updatedMetadata);
        Dataset<FileInfo> deleteCandidateFileDS = fileDS(originalMetadata, deletedSnapshotIds);

        // 计算过期文件（差集）
        this.expiredFileDS = deleteCandidateFileDS.except(validFileDS);
    }

    return expiredFileDS;
}
```

**文件删除执行** (`ExpireSnapshotsSparkAction.java:208`)：

```java
private ExpireSnapshots.Result doExecute() {
    if (streamResults()) {
        return deleteFiles(expireFiles().toLocalIterator());
    } else {
        return deleteFiles(expireFiles().collectAsList().iterator());
    }
}
```

## 4. remove_orphan_files Procedure 深度分析

### 4.1 功能概述

`remove_orphan_files` procedure 用于清理表位置中的孤儿文件（即不被任何快照引用的文件），释放存储空间。

### 4.2 实现架构

```
RemoveOrphanFilesProcedure
├── 安全性检查 (24小时间隔验证)
├── DeleteOrphanFilesSparkAction (执行引擎)
├── 文件系统列举 (Hadoop FileSystem)
├── 有效文件集合计算
└── 孤儿文件识别和删除
```

### 4.3 源码分析

**Procedure 定义** (`RemoveOrphanFilesProcedure.java:55`)：

```java
private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[] {
    ProcedureParameter.required("table", DataTypes.StringType),
    ProcedureParameter.optional("older_than", DataTypes.TimestampType),
    ProcedureParameter.optional("location", DataTypes.StringType),
    ProcedureParameter.optional("dry_run", DataTypes.BooleanType),
    ProcedureParameter.optional("max_concurrent_deletes", DataTypes.IntegerType),
    ProcedureParameter.optional("file_list_view", DataTypes.StringType),
    ProcedureParameter.optional("equal_schemes", STRING_MAP),
    ProcedureParameter.optional("equal_authorities", STRING_MAP),
    ProcedureParameter.optional("prefix_mismatch_mode", DataTypes.StringType),
};
```

**安全性检查** (`RemoveOrphanFilesProcedure.java:206`)：

```java
private void validateInterval(long olderThanMillis) {
    long intervalMillis = System.currentTimeMillis() - olderThanMillis;
    if (intervalMillis < TimeUnit.DAYS.toMillis(1)) {
        throw new IllegalArgumentException(
            "Cannot remove orphan files with an interval less than 24 hours. " +
            "Executing this procedure with a short interval may corrupt the table " +
            "if other operations are happening at the same time.");
    }
}
```

### 4.4 DeleteOrphanFilesSparkAction 核心实现

**列举策略** (`DeleteOrphanFilesSparkAction.java:302`)：

```java
private Dataset<String> listedFileDS() {
    List<String> subDirs = Lists.newArrayList();
    List<String> matchingFiles = Lists.newArrayList();

    Predicate<FileStatus> predicate = file -> file.getModificationTime() < olderThanTimestamp;
    PathFilter pathFilter = PartitionAwareHiddenPathFilter.forSpecs(table.specs());

    // 在驱动程序上最多列举 MAX_DRIVER_LISTING_DEPTH 层级
    listDirRecursively(
        location, predicate, hadoopConf.value(),
        MAX_DRIVER_LISTING_DEPTH, MAX_DRIVER_LISTING_DIRECT_SUB_DIRS,
        subDirs, pathFilter, matchingFiles);

    JavaRDD<String> matchingFileRDD = sparkContext().parallelize(matchingFiles, 1);

    if (subDirs.isEmpty()) {
        return spark().createDataset(matchingFileRDD.rdd(), Encoders.STRING());
    }

    // 对于大目录，在执行器上并行列举
    int parallelism = Math.min(subDirs.size(), listingParallelism);
    JavaRDD<String> subDirRDD = sparkContext().parallelize(subDirs, parallelism);

    Broadcast<SerializableConfiguration> conf = sparkContext().broadcast(hadoopConf);
    ListDirsRecursively listDirs = new ListDirsRecursively(conf, olderThanTimestamp, pathFilter);
    JavaRDD<String> matchingLeafFileRDD = subDirRDD.mapPartitions(listDirs);

    JavaRDD<String> completeMatchingFileRDD = matchingFileRDD.union(matchingLeafFileRDD);
    return spark().createDataset(completeMatchingFileRDD.rdd(), Encoders.STRING());
}
```

**孤儿文件识别** (`DeleteOrphanFilesSparkAction.java:391`)：

```java
static List<String> findOrphanFiles(
    SparkSession spark,
    Dataset<FileURI> actualFileIdentDS,    // 实际文件列表
    Dataset<FileURI> validFileIdentDS,     // 有效文件列表
    PrefixMismatchMode prefixMismatchMode) {

    SetAccumulator<Pair<String, String>> conflicts = new SetAccumulator<>();
    spark.sparkContext().register(conflicts);

    Column joinCond = actualFileIdentDS.col("path").equalTo(validFileIdentDS.col("path"));

    // 左外连接找出孤儿文件
    List<String> orphanFiles = actualFileIdentDS
        .joinWith(validFileIdentDS, joinCond, "leftouter")
        .mapPartitions(new FindOrphanFiles(prefixMismatchMode, conflicts), Encoders.STRING())
        .collectAsList();

    // 处理前缀不匹配的情况
    if (prefixMismatchMode == PrefixMismatchMode.ERROR && !conflicts.value().isEmpty()) {
        throw new ValidationException(
            "Unable to determine whether certain files are orphan. " +
            "Metadata references files that match listed/provided files except for authority/scheme.");
    }

    return orphanFiles;
}
```

## 5. 性能优化技术

### 5.1 Manifest 重写优化

1. **缓存策略**：
   - 支持可配置的 Dataset 缓存
   - 避免重复计算相同的 manifest 条目

2. **分区感知**：
   - 按分区列重新分布和排序数据
   - 提高查询时的剪枝效率

3. **滚动写入**：
   - 使用 `RollingManifestWriter` 控制 manifest 文件大小
   - 避免产生过小或过大的文件

### 5.2 快照过期优化

1. **流式处理**：
   - 支持流式结果处理，减少内存消耗
   - 可配置的批量删除 vs 流式删除

2. **批量操作**：
   - 利用 `SupportsBulkOperations` 接口
   - 对支持批量删除的 FileIO 进行优化

3. **并行控制**：
   - 通过 `spark.sql.shuffle.partitions` 控制并行度
   - 支持自定义删除执行器

### 5.3 孤儿文件清理优化

1. **智能列举**：
   - 驱动程序列举浅层目录
   - 执行器并行列举深层目录
   - 避免驱动程序内存压力

2. **路径过滤**：
   - 使用 `PartitionAwareHiddenPathFilter` 跳过隐藏文件
   - 避免处理不相关的文件

3. **URI 标准化**：
   - 处理不同的 scheme 和 authority
   - 支持 S3 多协议兼容 (s3, s3a, s3n)

## 6. 安全性和一致性保障

### 6.1 GC 检查

所有 procedure 都会检查表的 GC 设置：

```java
ValidationException.check(
    PropertyUtil.propertyAsBoolean(table.properties(), GC_ENABLED, GC_ENABLED_DEFAULT),
    "Cannot delete files: GC is disabled (deleting files may corrupt other tables)");
```

### 6.2 事务性保障

1. **快照过期**：先提交元数据更改，再删除文件
2. **Manifest 重写**：使用分阶段提交，失败时清理临时文件
3. **孤儿文件清理**：24小时安全间隔检查

### 6.3 错误处理

1. **CommitStateUnknownException** 处理
2. **CleanableFailure** 自动清理
3. **前缀不匹配模式** 配置

## 7. 使用建议和最佳实践

### 7.1 rewrite_manifests

```sql
-- 基本用法
CALL catalog.system.rewrite_manifests('namespace.table');

-- 使用缓存提高性能
CALL catalog.system.rewrite_manifests('namespace.table', true);

-- 针对特定分区规格
CALL catalog.system.rewrite_manifests('namespace.table', false, 1);
```

### 7.2 expire_snapshots

```sql
-- 保留最近7天的快照
CALL catalog.system.expire_snapshots('namespace.table', TIMESTAMP '2023-01-01 00:00:00');

-- 保留最近10个快照
CALL catalog.system.expire_snapshots('namespace.table', null, 10);

-- 流式处理大量文件
CALL catalog.system.expire_snapshots('namespace.table', TIMESTAMP '2023-01-01 00:00:00', null, null, true);
```

### 7.3 remove_orphan_files

```sql
-- 基本清理（必须大于24小时）
CALL catalog.system.remove_orphan_files('namespace.table', TIMESTAMP '2023-01-01 00:00:00');

-- 干跑模式检查
CALL catalog.system.remove_orphan_files('namespace.table', TIMESTAMP '2023-01-01 00:00:00', null, true);

-- 指定清理位置
CALL catalog.system.remove_orphan_files('namespace.table', TIMESTAMP '2023-01-01 00:00:00', 's3://bucket/data/');
```

## 8. 结论

Apache Iceberg 的 procedure 实现展现了以下特点：

1. **Spark 驱动**：所有 procedure 都基于 Spark 引擎实现，充分利用了 Spark 的分布式计算能力

2. **安全优先**：内置多层安全检查，包括 GC 启用检查、时间间隔验证等

3. **性能优化**：针对大规模数据场景进行了深度优化，包括智能列举、批量操作、流式处理等

4. **灵活配置**：提供丰富的参数选项，支持不同的使用场景和性能需求

5. **一致性保障**：通过事务性操作和错误处理机制确保数据一致性

这些 procedure 是 Iceberg 表维护的核心工具，对于保持表的性能和存储效率至关重要。在生产环境中使用时，应根据表的大小、访问模式和业务需求选择合适的参数配置。

**注意**：本分析基于 Apache Iceberg Spark 实现，不同的计算引擎（如 Flink、Trino）可能有不同的实现方式，但核心的 Actions API 是共通的。