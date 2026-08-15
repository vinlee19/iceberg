# 2025-11-15 Apache Iceberg ExpireSnapshots 在 Flink 与 Spark 实现差异深度源码分析报告

## 目录

1. [概述](#概述)
2. [Core 层核心实现原理](#core-层核心实现原理)
3. [Spark 实现机制深度分析](#spark-实现机制深度分析)
4. [Flink 实现机制深度分析](#flink-实现机制深度分析)
5. [Flink vs Spark 实现差异对比](#flink-vs-spark-实现差异对比)
6. [性能与适用场景分析](#性能与适用场景分析)
7. [最佳实践建议](#最佳实践建议)

---

## 概述

### 功能定位

ExpireSnapshots 是 Apache Iceberg 表维护的核心功能之一，用于删除过期的快照（Snapshots）及其关联的文件，包括：
- 快照元数据（Snapshot metadata）
- Manifest 文件
- Manifest List 文件
- 数据文件（Data files）
- 删除文件（Delete files）
- 统计文件（Statistics files）

### 架构分层

Iceberg 的 ExpireSnapshots 实现采用三层架构：

```
┌─────────────────────────────────────┐
│   引擎层 (Flink/Spark)              │  ← 分布式执行层
├─────────────────────────────────────┤
│   Actions API (可选)                │  ← 分布式优化层
├─────────────────────────────────────┤
│   Core API (RemoveSnapshots)       │  ← 核心逻辑层
└─────────────────────────────────────┘
```

---

## Core 层核心实现原理

### 1. 核心接口定义

#### API 接口 (`org.apache.iceberg.ExpireSnapshots`)

位置: `api/src/main/java/org/apache/iceberg/ExpireSnapshots.java`

```java
public interface ExpireSnapshots extends PendingUpdate<List<Snapshot>> {
  // 按快照 ID 过期
  ExpireSnapshots expireSnapshotId(long snapshotId);

  // 按时间戳过期
  ExpireSnapshots expireOlderThan(long timestampMillis);

  // 保留最近 N 个快照
  ExpireSnapshots retainLast(int numSnapshots);

  // 自定义删除函数
  ExpireSnapshots deleteWith(Consumer<String> deleteFunc);

  // 自定义删除执行器
  ExpireSnapshots executeDeleteWith(ExecutorService executorService);

  // 自定义规划执行器
  ExpireSnapshots planWith(ExecutorService executorService);

  // 是否清理过期文件
  ExpireSnapshots cleanExpiredFiles(boolean clean);

  // 是否清理过期元数据（Schema、PartitionSpec）
  ExpireSnapshots cleanExpiredMetadata(boolean clean);
}
```

#### Actions API 接口 (`org.apache.iceberg.actions.ExpireSnapshots`)

位置: `api/src/main/java/org/apache/iceberg/actions/ExpireSnapshots.java`

```java
public interface ExpireSnapshots extends Action<ExpireSnapshots, ExpireSnapshots.Result> {
  // 与 Core API 类似的方法
  ExpireSnapshots expireSnapshotId(long snapshotId);
  ExpireSnapshots expireOlderThan(long timestampMillis);
  ExpireSnapshots retainLast(int numSnapshots);

  // 返回详细的执行结果
  interface Result {
    long deletedDataFilesCount();
    long deletedEqualityDeleteFilesCount();
    long deletedPositionDeleteFilesCount();
    long deletedManifestsCount();
    long deletedManifestListsCount();
    long deletedStatisticsFilesCount();
  }
}
```

### 2. Core 实现类 (`RemoveSnapshots`)

位置: `core/src/main/java/org/apache/iceberg/RemoveSnapshots.java`

#### 核心数据结构

```java
class RemoveSnapshots implements ExpireSnapshots {
  private final TableOperations ops;              // 表操作接口
  private final Set<Long> idsToRemove;            // 待删除的快照 ID
  private final long now;                         // 当前时间戳
  private final long defaultMaxRefAgeMs;          // 默认引用最大年龄

  private boolean cleanExpiredFiles = true;       // 是否清理过期文件
  private TableMetadata base;                     // 基础表元数据
  private long defaultExpireOlderThan;            // 默认过期时间
  private int defaultMinNumSnapshots;             // 默认保留快照数
  private Consumer<String> deleteFunc;            // 自定义删除函数
  private ExecutorService deleteExecutorService;  // 删除执行器
  private ExecutorService planExecutorService;    // 规划执行器
  private Boolean incrementalCleanup;             // 是否增量清理
  private boolean cleanExpiredMetadata;           // 是否清理过期元数据
}
```

#### 快照过期核心逻辑

**第一阶段：确定要保留的快照**

```java
private TableMetadata internalApply() {
  this.base = ops.refresh();

  // 1. 计算保留的引用（Refs）
  Map<String, SnapshotRef> retainedRefs = computeRetainedRefs(base.refs());

  Set<Long> idsToRetain = Sets.newHashSet();

  // 2. 添加所有分支快照到保留集合
  idsToRetain.addAll(computeAllBranchSnapshotsToRetain(retainedRefs.values()));

  // 3. 添加未引用但未到期的快照
  idsToRetain.addAll(unreferencedSnapshotsToRetain(retainedRefs.values()));

  // 4. 确定要移除的快照
  base.snapshots().stream()
      .map(Snapshot::snapshotId)
      .filter(snapshot -> !idsToRetain.contains(snapshot))
      .forEach(idsToRemove::add);

  // 5. 构建更新后的元数据
  TableMetadata.Builder updatedMetaBuilder = TableMetadata.buildFrom(base);
  updatedMetaBuilder.removeSnapshots(idsToRemove);

  return updatedMetaBuilder.build();
}
```

**计算分支快照保留逻辑**

```java
private Set<Long> computeBranchSnapshotsToRetain(
    long snapshot, long expireSnapshotsOlderThan, int minSnapshotsToKeep) {
  Set<Long> idsToRetain = Sets.newHashSet();

  for (Snapshot ancestor : SnapshotUtil.ancestorsOf(snapshot, base::snapshot)) {
    // 保留条件：
    // 1. 未达到最小保留数量
    // 2. 或快照时间戳晚于过期时间
    if (idsToRetain.size() < minSnapshotsToKeep ||
        ancestor.timestampMillis() >= expireSnapshotsOlderThan) {
      idsToRetain.add(ancestor.snapshotId());
    } else {
      return idsToRetain;
    }
  }

  return idsToRetain;
}
```

**第二阶段：清理过期文件**

Core 提供了两种清理策略：

##### 1. **增量清理策略** (`IncrementalFileCleanup`)

位置: `core/src/main/java/org/apache/iceberg/IncrementalFileCleanup.java`

**适用条件：**
- 未指定特定快照 ID
- 未删除 main 分支外的祖先快照
- 不存在 main 分支外的快照

**核心逻辑：**

```java
public void cleanFiles(TableMetadata beforeExpiration, TableMetadata afterExpiration) {
  // 1. 确定过期的快照 ID
  Set<Long> validIds = afterExpiration.snapshots().stream()
      .map(Snapshot::snapshotId)
      .collect(Collectors.toSet());

  Set<Long> expiredIds = beforeExpiration.snapshots().stream()
      .map(Snapshot::snapshotId)
      .filter(id -> !validIds.contains(id))
      .collect(Collectors.toSet());

  // 2. 获取当前表状态的祖先快照
  Set<Long> ancestorIds = Sets.newHashSet(
      SnapshotUtil.ancestorIds(latest, beforeExpiration::snapshot));

  // 3. 扫描有效快照，识别需要清理的 manifests
  Set<String> validManifests = ConcurrentHashMap.newKeySet();
  Set<ManifestFile> manifestsToScan = ConcurrentHashMap.newKeySet();

  Tasks.foreach(afterExpiration.snapshots())
      .executeWith(planExecutorService)
      .run(snapshot -> {
        for (ManifestFile manifest : readManifests(snapshot)) {
          validManifests.add(manifest.path());

          // 如果 manifest 由过期快照创建，但仍被有效快照引用
          if (!validIds.contains(manifest.snapshotId()) &&
              ancestorIds.contains(manifest.snapshotId()) &&
              manifest.hasDeletedFiles()) {
            manifestsToScan.add(manifest);
          }
        }
      });

  // 4. 扫描过期快照，识别要删除的 manifests 和数据文件
  Set<String> manifestsToDelete = ConcurrentHashMap.newKeySet();
  Set<ManifestFile> manifestsToRevert = ConcurrentHashMap.newKeySet();

  Tasks.foreach(beforeExpiration.snapshots())
      .executeWith(planExecutorService)
      .run(snapshot -> {
        if (!validIds.contains(snapshot.snapshotId())) {
          for (ManifestFile manifest : readManifests(snapshot)) {
            if (!validManifests.contains(manifest.path())) {
              manifestsToDelete.add(manifest.path());

              // 删除由祖先快照删除的数据文件
              if (ancestorIds.contains(manifest.snapshotId()) &&
                  manifest.hasDeletedFiles()) {
                manifestsToScan.add(manifest);
              }

              // 删除由过期快照（非祖先）添加的数据文件
              if (!ancestorIds.contains(manifest.snapshotId()) &&
                  expiredIds.contains(manifest.snapshotId()) &&
                  manifest.hasAddedFiles()) {
                manifestsToRevert.add(manifest);
              }
            }
          }
        }
      });

  // 5. 扫描 manifests 获取待删除的数据文件
  Set<String> filesToDelete = findFilesToDelete(
      manifestsToScan, manifestsToRevert, validIds, specsById);

  // 6. 执行删除
  deleteFiles(filesToDelete, "data");
  deleteFiles(manifestsToDelete, "manifest");
  deleteFiles(manifestListsToDelete, "manifest list");
  deleteFiles(expiredStatisticsFiles, "statistics files");
}
```

**数据文件删除逻辑：**

```java
private Set<String> findFilesToDelete(
    Set<ManifestFile> manifestsToScan,
    Set<ManifestFile> manifestsToRevert,
    Set<Long> validIds,
    Map<Integer, PartitionSpec> specsById) {

  Set<String> filesToDelete = ConcurrentHashMap.newKeySet();

  // 扫描包含删除记录的 manifests
  Tasks.foreach(manifestsToScan)
      .executeWith(planExecutorService)
      .run(manifest -> {
        try (ManifestReader<?> reader = ManifestFiles.open(manifest, fileIO, specsById)) {
          for (ManifestEntry<?> entry : reader.entries()) {
            // 删除状态为 DELETED 且快照 ID 已过期的文件
            if (entry.status() == ManifestEntry.Status.DELETED &&
                !validIds.contains(entry.snapshotId())) {
              filesToDelete.add(entry.file().location());
            }
          }
        }
      });

  // 扫描需要回滚的 manifests
  Tasks.foreach(manifestsToRevert)
      .executeWith(planExecutorService)
      .run(manifest -> {
        try (ManifestReader<?> reader = ManifestFiles.open(manifest, fileIO, specsById)) {
          for (ManifestEntry<?> entry : reader.entries()) {
            // 删除状态为 ADDED 的文件（回滚操作）
            if (entry.status() == ManifestEntry.Status.ADDED) {
              filesToDelete.add(entry.file().location());
            }
          }
        }
      });

  return filesToDelete;
}
```

##### 2. **可达性清理策略** (`ReachableFileCleanup`)

位置: `core/src/main/java/org/apache/iceberg/ReachableFileCleanup.java`

**适用条件：**
- 指定了特定快照 ID
- 删除了 main 分支外的祖先快照
- 存在 main 分支外的快照

**核心逻辑：**
扫描所有有效快照，收集所有可达文件，然后删除不可达的文件。

**文件删除实现：**

位置: `core/src/main/java/org/apache/iceberg/FileCleanupStrategy.java`

```java
protected void deleteFiles(Set<String> pathsToDelete, String fileType) {
  // 优先使用批量删除
  if (deleteFunc == null && fileIO instanceof SupportsBulkOperations) {
    try {
      ((SupportsBulkOperations) fileIO).deleteFiles(pathsToDelete);
    } catch (BulkDeletionFailureException e) {
      LOG.warn("Bulk deletion failed for {} of {} {} file(s)",
               e.numberFailedObjects(), pathsToDelete.size(), fileType, e);
    }
  } else {
    // 使用自定义删除函数或单文件删除
    Consumer<String> deleteFuncToUse = deleteFunc == null ? defaultDeleteFunc : deleteFunc;

    Tasks.foreach(pathsToDelete)
        .executeWith(deleteExecutorService)
        .retry(3)
        .stopRetryOn(NotFoundException.class)
        .suppressFailureWhenFinished()
        .onFailure((file, thrown) ->
            LOG.warn("Delete failed for {} file: {}", fileType, file, thrown))
        .run(deleteFuncToUse::accept);
  }
}
```

### 3. 元数据清理

当 `cleanExpiredMetadata = true` 时，额外清理：

```java
if (cleanExpiredMetadata) {
  Set<Integer> reachableSpecs = Sets.newConcurrentHashSet();
  Set<Integer> reachableSchemas = Sets.newConcurrentHashSet();

  // 添加默认和当前的 Spec/Schema
  reachableSpecs.add(base.defaultSpecId());
  reachableSchemas.add(base.currentSchemaId());

  // 扫描所有保留的快照
  Tasks.foreach(idsToRetain)
      .executeWith(planExecutorService())
      .run(snapshotId -> {
        Snapshot snapshot = base.snapshot(snapshotId);
        snapshot.allManifests(ops.io()).stream()
            .map(ManifestFile::partitionSpecId)
            .forEach(reachableSpecs::add);
        reachableSchemas.add(snapshot.schemaId());
      });

  // 删除不可达的 Specs 和 Schemas
  Set<Integer> specsToRemove = base.specs().stream()
      .map(PartitionSpec::specId)
      .filter(specId -> !reachableSpecs.contains(specId))
      .collect(Collectors.toSet());
  updatedMetaBuilder.removeSpecs(specsToRemove);

  Set<Integer> schemasToRemove = base.schemas().stream()
      .map(Schema::schemaId)
      .filter(schemaId -> !reachableSchemas.contains(schemaId))
      .collect(Collectors.toSet());
  updatedMetaBuilder.removeSchemas(schemasToRemove);
}
```

---

## Spark 实现机制深度分析

### 1. 架构设计

Spark 的 ExpireSnapshots 实现分为两部分：

```
┌──────────────────────────────────────────┐
│  ExpireSnapshotsProcedure                │  ← SQL Procedure 接口
│  (spark/procedures/ExpireSnapshotsProcedure.java)
└─────────────────┬────────────────────────┘
                  │ 调用
┌─────────────────▼────────────────────────┐
│  ExpireSnapshotsSparkAction              │  ← 分布式实现
│  (spark/actions/ExpireSnapshotsSparkAction.java)
└─────────────────┬────────────────────────┘
                  │ 使用
┌─────────────────▼────────────────────────┐
│  RemoveSnapshots (Core)                  │  ← 核心逻辑
└──────────────────────────────────────────┘
```

### 2. Procedure 实现

位置: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/ExpireSnapshotsProcedure.java`

**参数定义：**

```java
private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[] {
  ProcedureParameter.required("table", DataTypes.StringType),
  ProcedureParameter.optional("older_than", DataTypes.TimestampType),
  ProcedureParameter.optional("retain_last", DataTypes.IntegerType),
  ProcedureParameter.optional("max_concurrent_deletes", DataTypes.IntegerType),
  ProcedureParameter.optional("stream_results", DataTypes.BooleanType),
  ProcedureParameter.optional("snapshot_ids", DataTypes.createArrayType(DataTypes.LongType)),
  ProcedureParameter.optional("clean_expired_metadata", DataTypes.BooleanType)
};
```

**SQL 调用示例：**

```sql
-- 基本用法
CALL catalog.system.expire_snapshots('db.table');

-- 指定过期时间
CALL catalog.system.expire_snapshots(
  table => 'db.table',
  older_than => TIMESTAMP '2023-01-01 00:00:00'
);

-- 保留最近 N 个快照
CALL catalog.system.expire_snapshots(
  table => 'db.table',
  retain_last => 10
);

-- 指定并发删除线程数
CALL catalog.system.expire_snapshots(
  table => 'db.table',
  max_concurrent_deletes => 8
);

-- 清理过期元数据
CALL catalog.system.expire_snapshots(
  table => 'db.table',
  clean_expired_metadata => true
);
```

**Procedure 执行逻辑：**

```java
public InternalRow[] call(InternalRow args) {
  // 解析参数
  Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
  Long olderThanMillis = args.isNullAt(1) ? null : DateTimeUtil.microsToMillis(args.getLong(1));
  Integer retainLastNum = args.isNullAt(2) ? null : args.getInt(2);
  Integer maxConcurrentDeletes = args.isNullAt(3) ? null : args.getInt(3);
  Boolean streamResult = args.isNullAt(4) ? null : args.getBoolean(4);
  long[] snapshotIds = args.isNullAt(5) ? null : args.getArray(5).toLongArray();
  Boolean cleanExpiredMetadata = args.isNullAt(6) ? null : args.getBoolean(6);

  // 执行 Action
  return modifyIcebergTable(tableIdent, table -> {
    ExpireSnapshots action = actions().expireSnapshots(table);

    if (olderThanMillis != null) {
      action.expireOlderThan(olderThanMillis);
    }

    if (retainLastNum != null) {
      action.retainLast(retainLastNum);
    }

    if (maxConcurrentDeletes != null) {
      if (table.io() instanceof SupportsBulkOperations) {
        LOG.warn("max_concurrent_deletes only works with FileIOs that do not support bulk deletes");
      } else {
        action.executeDeleteWith(executorService(maxConcurrentDeletes, "expire-snapshots"));
      }
    }

    if (snapshotIds != null) {
      for (long snapshotId : snapshotIds) {
        action.expireSnapshotId(snapshotId);
      }
    }

    if (streamResult != null) {
      action.option(ExpireSnapshotsSparkAction.STREAM_RESULTS, Boolean.toString(streamResult));
    }

    if (cleanExpiredMetadata != null) {
      action.cleanExpiredMetadata(cleanExpiredMetadata);
    }

    ExpireSnapshots.Result result = action.execute();
    return toOutputRows(result);
  });
}
```

### 3. SparkAction 核心实现

位置: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/ExpireSnapshotsSparkAction.java`

#### 关键特性

1. **使用 Spark 分布式计算识别过期文件**
2. **基于 Dataset 的 anti-join 操作**
3. **支持流式结果处理**
4. **提供详细的执行结果统计**

#### 核心执行流程

```java
public Dataset<FileInfo> expireFiles() {
  if (expiredFileDS == null) {
    // 1. 获取过期前的元数据
    TableMetadata originalMetadata = ops.current();

    // 2. 执行 Core 的 ExpireSnapshots（不清理文件）
    org.apache.iceberg.ExpireSnapshots expireSnapshots = table.expireSnapshots();

    for (long id : expiredSnapshotIds) {
      expireSnapshots = expireSnapshots.expireSnapshotId(id);
    }

    if (expireOlderThanValue != null) {
      expireSnapshots = expireSnapshots.expireOlderThan(expireOlderThanValue);
    }

    if (retainLastValue != null) {
      expireSnapshots = expireSnapshots.retainLast(retainLastValue);
    }

    if (cleanExpiredMetadata != null) {
      expireSnapshots.cleanExpiredMetadata(cleanExpiredMetadata);
    }

    // 关键：不清理文件，仅提交快照过期
    expireSnapshots.cleanExpiredFiles(false).commit();

    // 3. 获取过期后的元数据
    TableMetadata updatedMetadata = ops.refresh();

    // 4. 使用 Spark 计算有效文件集合
    Dataset<FileInfo> validFileDS = fileDS(updatedMetadata);

    // 5. 使用 Spark 计算过期快照引用的文件
    Set<Long> deletedSnapshotIds = findExpiredSnapshotIds(originalMetadata, updatedMetadata);
    Dataset<FileInfo> deleteCandidateFileDS = fileDS(originalMetadata, deletedSnapshotIds);

    // 6. 通过 except 操作（anti-join）确定要删除的文件
    this.expiredFileDS = deleteCandidateFileDS.except(validFileDS);
  }

  return expiredFileDS;
}
```

#### 文件 Dataset 构建

```java
private Dataset<FileInfo> fileDS(TableMetadata metadata, Set<Long> snapshotIds) {
  Table staticTable = newStaticTable(metadata, table.io());

  return contentFileDS(staticTable, snapshotIds)        // 数据文件
      .union(manifestDS(staticTable, snapshotIds))      // Manifest 文件
      .union(manifestListDS(staticTable, snapshotIds))  // Manifest List 文件
      .union(statisticsFileDS(staticTable, snapshotIds)); // 统计文件
}
```

**contentFileDS 实现：**

```java
protected Dataset<FileInfo> contentFileDS(Table table, Set<Long> snapshotIds) {
  Dataset<Row> snapshotDS;

  if (snapshotIds == null) {
    // 读取所有快照的所有内容文件
    snapshotDS = spark().read().format("iceberg")
        .load(table.name() + ".all_data_files");
  } else {
    // 仅读取指定快照的内容文件
    snapshotDS = spark().read().format("iceberg")
        .option("snapshot-id", StringUtils.join(snapshotIds, ","))
        .load(table.name() + ".all_data_files");
  }

  return snapshotDS.selectExpr("file_path", "file_size_in_bytes as file_size")
      .as(Encoders.bean(FileInfo.class));
}
```

#### 文件删除执行

```java
private ExpireSnapshots.Result deleteFiles(Iterator<FileInfo> files) {
  DeleteSummary summary;

  // 优先使用批量删除
  if (deleteFunc == null && table.io() instanceof SupportsBulkOperations) {
    summary = deleteFiles((SupportsBulkOperations) table.io(), files);
  } else {
    // 使用自定义删除函数或并发删除
    if (deleteFunc == null) {
      LOG.info("Table IO {} does not support bulk operations. Using non-bulk deletes.",
               table.io().getClass().getName());
      summary = deleteFiles(deleteExecutorService, table.io()::deleteFile, files);
    } else {
      LOG.info("Custom delete function provided. Using non-bulk deletes");
      summary = deleteFiles(deleteExecutorService, deleteFunc, files);
    }
  }

  LOG.info("Deleted {} total files", summary.totalFilesCount());

  return ImmutableExpireSnapshots.Result.builder()
      .deletedDataFilesCount(summary.dataFilesCount())
      .deletedPositionDeleteFilesCount(summary.positionDeleteFilesCount())
      .deletedEqualityDeleteFilesCount(summary.equalityDeleteFilesCount())
      .deletedManifestsCount(summary.manifestsCount())
      .deletedManifestListsCount(summary.manifestListsCount())
      .deletedStatisticsFilesCount(summary.statisticsFilesCount())
      .build();
}
```

#### 流式 vs 批量处理

```java
private ExpireSnapshots.Result doExecute() {
  if (streamResults()) {
    // 流式处理：逐条删除，不收集所有结果到 Driver
    return deleteFiles(expireFiles().toLocalIterator());
  } else {
    // 批量处理：先收集所有结果到 Driver，再删除
    return deleteFiles(expireFiles().collectAsList().iterator());
  }
}

private boolean streamResults() {
  return PropertyUtil.propertyAsBoolean(options(), STREAM_RESULTS, STREAM_RESULTS_DEFAULT);
}
```

### 4. 执行示例

**编程式调用：**

```java
// 使用 Spark Actions API
SparkActions actions = SparkActions.get();

ExpireSnapshots.Result result = actions.expireSnapshots(table)
    .expireOlderThan(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7))
    .retainLast(10)
    .option(ExpireSnapshotsSparkAction.STREAM_RESULTS, "true")
    .cleanExpiredMetadata(true)
    .execute();

System.out.println("Deleted data files: " + result.deletedDataFilesCount());
System.out.println("Deleted manifests: " + result.deletedManifestsCount());
```

---

## Flink 实现机制深度分析

### 1. 架构设计

Flink 的实现基于流式处理架构：

```
┌──────────────────────────────────────────┐
│  ExpireSnapshots (Builder API)           │  ← 流构建器
│  (flink/maintenance/api/ExpireSnapshots.java)
└─────────────────┬────────────────────────┘
                  │ 构建
┌─────────────────▼────────────────────────┐
│  ExpireSnapshotsProcessor                │  ← 处理器 (Single Parallelism)
│  (flink/maintenance/operator/ExpireSnapshotsProcessor.java)
└─────────────────┬────────────────────────┘
                  │ Side Output
┌─────────────────▼────────────────────────┐
│  DeleteFilesProcessor                    │  ← 删除处理器 (Parallel)
│  (flink/maintenance/operator/DeleteFilesProcessor.java)
└──────────────────────────────────────────┘
```

### 2. Builder API 设计

位置: `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/maintenance/api/ExpireSnapshots.java`

**流式构建器：**

```java
public class ExpireSnapshots {
  private static final int DELETE_BATCH_SIZE_DEFAULT = 1000;

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends MaintenanceTaskBuilder<ExpireSnapshots.Builder> {
    private Duration maxSnapshotAge = null;        // 最大快照年龄
    private Integer numSnapshots = null;           // 保留快照数
    private Integer planningWorkerPoolSize;        // 规划线程池大小
    private int deleteBatchSize = 1000;            // 删除批次大小
    private Boolean cleanExpiredMetadata = null;   // 是否清理元数据

    public Builder maxSnapshotAge(Duration newMaxSnapshotAge) {
      this.maxSnapshotAge = newMaxSnapshotAge;
      return this;
    }

    public Builder retainLast(int newNumSnapshots) {
      this.numSnapshots = newNumSnapshots;
      return this;
    }

    public Builder planningWorkerPoolSize(int newPlanningWorkerPoolSize) {
      this.planningWorkerPoolSize = newPlanningWorkerPoolSize;
      return this;
    }

    public Builder deleteBatchSize(int newDeleteBatchSize) {
      this.deleteBatchSize = newDeleteBatchSize;
      return this;
    }

    public Builder cleanExpiredMetadata(boolean newCleanExpiredMetadata) {
      this.cleanExpiredMetadata = newCleanExpiredMetadata;
      return this;
    }
  }
}
```

**构建流拓扑：**

```java
@Override
DataStream<TaskResult> append(DataStream<Trigger> trigger) {
  Preconditions.checkNotNull(tableLoader(), "TableLoader should not be null");

  // 1. 创建过期快照处理器（单并行度）
  SingleOutputStreamOperator<TaskResult> result = trigger
      .process(new ExpireSnapshotsProcessor(
          tableLoader(),
          maxSnapshotAge == null ? null : maxSnapshotAge.toMillis(),
          numSnapshots,
          planningWorkerPoolSize,
          cleanExpiredMetadata))
      .name(operatorName(EXECUTOR_OPERATOR_NAME))
      .uid(EXECUTOR_OPERATOR_NAME + uidSuffix())
      .slotSharingGroup(slotSharingGroup())
      .forceNonParallel();  // 强制单并行度

  // 2. 创建文件删除处理器（可并行）
  result.getSideOutput(ExpireSnapshotsProcessor.DELETE_STREAM)
      .rebalance()  // 重新平衡分区
      .transform(
          operatorName(DELETE_FILES_OPERATOR_NAME),
          TypeInformation.of(Void.class),
          new DeleteFilesProcessor(
              tableLoader().loadTable(),
              taskName(),
              index(),
              deleteBatchSize))
      .uid(DELETE_FILES_OPERATOR_NAME + uidSuffix())
      .slotSharingGroup(slotSharingGroup())
      .setParallelism(parallelism());  // 设置并行度

  return result;
}
```

### 3. ExpireSnapshotsProcessor 实现

位置: `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/maintenance/operator/ExpireSnapshotsProcessor.java`

**核心数据结构：**

```java
public class ExpireSnapshotsProcessor extends ProcessFunction<Trigger, TaskResult> {
  public static final OutputTag<String> DELETE_STREAM =
      new OutputTag<>("expire-snapshots-file-deletes-stream", Types.STRING);

  private final TableLoader tableLoader;
  private final Long maxSnapshotAgeMs;
  private final Integer numSnapshots;
  private final Integer plannerPoolSize;
  private final Boolean cleanExpiredMetadata;

  private transient ExecutorService plannerPool;
  private transient Table table;
}
```

**初始化：**

```java
@Override
public void open(Configuration parameters) throws Exception {
  tableLoader.open();
  this.table = tableLoader.loadTable();

  // 创建或使用共享的规划线程池
  this.plannerPool = plannerPoolSize != null
      ? ThreadPools.newFixedThreadPool(table.name() + "-table-planner", plannerPoolSize)
      : ThreadPools.getWorkerPool();
}
```

**核心处理逻辑：**

```java
@Override
public void processElement(Trigger trigger, Context ctx, Collector<TaskResult> out) {
  try {
    table.refresh();

    // 1. 创建 Core 的 ExpireSnapshots
    ExpireSnapshots expireSnapshots = table.expireSnapshots();

    // 2. 配置过期策略
    if (maxSnapshotAgeMs != null) {
      expireSnapshots = expireSnapshots.expireOlderThan(
          ctx.timestamp() - maxSnapshotAgeMs);
    }

    if (numSnapshots != null) {
      expireSnapshots = expireSnapshots.retainLast(numSnapshots);
    }

    if (cleanExpiredMetadata != null) {
      expireSnapshots.cleanExpiredMetadata(cleanExpiredMetadata);
    }

    // 3. 配置自定义删除函数（输出到 Side Output）
    AtomicLong deleteFileCounter = new AtomicLong(0L);
    expireSnapshots
        .planWith(plannerPool)
        .deleteWith(file -> {
          ctx.output(DELETE_STREAM, file);  // 发送到侧输出流
          deleteFileCounter.incrementAndGet();
        })
        .cleanExpiredFiles(true)
        .commit();

    LOG.info("Successfully finished expiring snapshots for {} at {}. Scheduled {} files for delete.",
             table, ctx.timestamp(), deleteFileCounter.get());

    // 4. 输出成功结果
    out.collect(new TaskResult(trigger.taskId(), trigger.timestamp(), true, Collections.emptyList()));

  } catch (Exception e) {
    LOG.error("Failed to expiring snapshots for {} at {}", table, ctx.timestamp(), e);
    out.collect(new TaskResult(trigger.taskId(), trigger.timestamp(), false, Lists.newArrayList(e)));
  }
}
```

**关键设计点：**

1. **单并行度处理**：确保同一时间只有一个任务修改表元数据
2. **侧输出流**：将文件路径输出到 Side Output，由下游并行处理
3. **异常处理**：捕获异常并返回失败结果，不中断整个流
4. **时间语义**：使用 `ctx.timestamp()` 获取事件时间

### 4. DeleteFilesProcessor 实现

位置: `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/maintenance/operator/DeleteFilesProcessor.java`

**核心数据结构：**

```java
public class DeleteFilesProcessor extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<String, Void> {

  private final String tableName;
  private final String taskName;
  private final int taskIndex;
  private final SupportsBulkOperations io;
  private final Set<String> filesToDelete = Sets.newHashSet();
  private final int batchSize;

  private transient Counter failedCounter;
  private transient Counter succeededCounter;
}
```

**批量删除逻辑：**

```java
@Override
public void processElement(StreamRecord<String> element) throws Exception {
  if (element.isRecord()) {
    filesToDelete.add(element.getValue());
  }

  // 当积累到批次大小时执行删除
  if (filesToDelete.size() >= batchSize) {
    deleteFiles();
  }
}

@Override
public void processWatermark(Watermark mark) {
  // Watermark 到达时删除剩余文件
  deleteFiles();
}

@Override
public void prepareSnapshotPreBarrier(long checkpointId) {
  // Checkpoint 前删除剩余文件
  deleteFiles();
}

private void deleteFiles() {
  try {
    io.deleteFiles(filesToDelete);
    LOG.info("Deleted {} files from table {} using bulk deletes",
             filesToDelete.size(), tableName);
    succeededCounter.inc(filesToDelete.size());
    filesToDelete.clear();
  } catch (BulkDeletionFailureException e) {
    int deletedFilesCount = filesToDelete.size() - e.numberFailedObjects();
    LOG.warn("Deleted only {} of {} files from table {} using bulk deletes",
             deletedFilesCount, filesToDelete.size(), tableName, e);
    succeededCounter.inc(deletedFilesCount);
    failedCounter.inc(e.numberFailedObjects());
  }
}
```

**关键设计点：**

1. **批量删除**：积累到 batchSize 才执行删除，提高效率
2. **Watermark 触发**：确保所有文件都被处理
3. **Checkpoint 保证**：在 Checkpoint 前完成删除，保证一致性
4. **Metrics 监控**：记录成功和失败的文件数量
5. **并行处理**：可以设置多个并行度，分布式删除文件

### 5. 使用示例

**Flink DataStream API：**

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// 创建触发器（例如：周期性触发）
DataStream<Trigger> trigger = env.fromSource(
    new PeriodicTriggerSource(Duration.ofHours(1)),
    WatermarkStrategy.noWatermarks(),
    "trigger");

// 构建 ExpireSnapshots 流
TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://path/to/table");

DataStream<TaskResult> result = ExpireSnapshots.builder()
    .tableLoader(tableLoader)
    .maxSnapshotAge(Duration.ofDays(7))
    .retainLast(10)
    .planningWorkerPoolSize(4)
    .deleteBatchSize(2000)
    .cleanExpiredMetadata(true)
    .parallelism(8)  // 删除文件的并行度
    .append(trigger);

result.print();

env.execute("Iceberg ExpireSnapshots Maintenance");
```

**流拓扑图：**

```
Trigger Source (P=1)
    │
    ▼
ExpireSnapshotsProcessor (P=1, forceNonParallel)
    │
    ├─── Main Output ──► TaskResult Sink
    │
    └─── Side Output (DELETE_STREAM) ──► Rebalance
                                           │
                                           ▼
                                    DeleteFilesProcessor (P=8)
                                           │
                                           ▼
                                         Void
```

---

## Flink vs Spark 实现差异对比

### 1. 架构模式对比

| 维度 | Spark 实现 | Flink 实现 |
|------|-----------|-----------|
| **执行模式** | 批处理（Batch） | 流处理（Streaming） |
| **分布式计算** | 使用 Dataset anti-join 计算过期文件 | 单节点过期 + 分布式删除 |
| **触发方式** | 主动调用（Procedure/Action） | 流式触发器驱动 |
| **并行度控制** | Spark 自动管理 | 明确的单/多并行度设置 |
| **状态管理** | 无状态（每次完整计算） | 支持 Checkpoint 状态 |

### 2. 文件识别策略对比

#### Spark 策略

**优势：**
- 利用 Spark 的分布式计算能力
- 通过 Dataset anti-join 高效识别过期文件
- 可以处理大规模表（数百万文件）
- Shuffle 操作可以通过 `spark.sql.shuffle.partitions` 调优

**实现细节：**

```java
// 1. 获取过期后的有效文件（分布式读取元数据表）
Dataset<FileInfo> validFileDS = fileDS(updatedMetadata);

// 2. 获取过期快照的候选文件
Dataset<FileInfo> deleteCandidateFileDS = fileDS(originalMetadata, deletedSnapshotIds);

// 3. 通过 except (anti-join) 计算差集
Dataset<FileInfo> expiredFileDS = deleteCandidateFileDS.except(validFileDS);
```

**性能特点：**
- 需要一次 Shuffle 操作
- 内存占用取决于文件数量
- 可以利用 Spark 的执行优化器

#### Flink 策略

**优势：**
- 轻量级：直接使用 Core 的文件识别逻辑
- 无需额外的分布式计算
- 内存占用可控（只在单节点）
- 适合流式周期性维护

**实现细节：**

```java
// 使用 Core 的 IncrementalFileCleanup 或 ReachableFileCleanup
expireSnapshots
    .deleteWith(file -> {
      ctx.output(DELETE_STREAM, file);  // 直接输出到侧输出流
    })
    .commit();
```

**性能特点：**
- 无 Shuffle 操作
- 单节点扫描 Manifest
- 适合中小规模表

### 3. 文件删除策略对比

| 维度 | Spark 实现 | Flink 实现 |
|------|-----------|-----------|
| **删除位置** | Driver 节点或自定义 ExecutorService | Flink Task 并行删除 |
| **批量处理** | 支持（通过 SupportsBulkOperations） | 支持（批次大小可配置） |
| **并行删除** | 可选（通过 ExecutorService） | 原生支持（并行度可配置） |
| **流式删除** | 支持（toLocalIterator） | 默认流式处理 |
| **容错机制** | 重试 + suppressFailure | Checkpoint + Metrics |

#### Spark 删除实现

```java
// 方式 1: 批量收集后删除
deleteFiles(expireFiles().collectAsList().iterator());

// 方式 2: 流式删除（节省 Driver 内存）
deleteFiles(expireFiles().toLocalIterator());

// 方式 3: 使用批量删除 API
if (table.io() instanceof SupportsBulkOperations) {
  ((SupportsBulkOperations) table.io()).deleteFiles(pathsToDelete);
}
```

**特点：**
- 删除在 Driver 节点执行
- 可以通过 `max_concurrent_deletes` 控制并发
- 流式模式避免 Driver OOM

#### Flink 删除实现

```java
// 单并行度过期处理器
expireSnapshots.deleteWith(file -> ctx.output(DELETE_STREAM, file))

// 多并行度删除处理器
result.getSideOutput(DELETE_STREAM)
    .rebalance()
    .transform(..., new DeleteFilesProcessor(...))
    .setParallelism(8);  // 8 个并行任务删除文件
```

**特点：**
- 删除在 Flink Task 执行
- 原生分布式并行删除
- Checkpoint 保证一致性
- 批量删除减少 IO 操作

### 4. 调用接口对比

#### Spark 调用方式

**1. SQL Procedure（推荐）：**

```sql
CALL catalog.system.expire_snapshots(
  table => 'db.table',
  older_than => TIMESTAMP '2023-01-01 00:00:00',
  retain_last => 10,
  max_concurrent_deletes => 8,
  clean_expired_metadata => true
);
```

**2. Actions API：**

```java
SparkActions.get()
    .expireSnapshots(table)
    .expireOlderThan(timestampMillis)
    .retainLast(10)
    .execute();
```

**3. Core API：**

```java
table.expireSnapshots()
    .expireOlderThan(timestampMillis)
    .retainLast(10)
    .commit();
```

#### Flink 调用方式

**1. DataStream API（流式维护）：**

```java
DataStream<TaskResult> result = ExpireSnapshots.builder()
    .tableLoader(tableLoader)
    .maxSnapshotAge(Duration.ofDays(7))
    .retainLast(10)
    .parallelism(8)
    .append(trigger);
```

**2. Core API（单次执行）：**

```java
table.expireSnapshots()
    .expireOlderThan(timestampMillis)
    .retainLast(10)
    .commit();
```

### 5. 配置参数对比

| 参数 | Spark | Flink |
|------|-------|-------|
| **过期时间** | `older_than` (TIMESTAMP) | `maxSnapshotAge` (Duration) |
| **保留数量** | `retain_last` (Integer) | `retainLast` (Integer) |
| **删除并发** | `max_concurrent_deletes` | `parallelism` |
| **批次大小** | 固定 (100000) | `deleteBatchSize` (可配置) |
| **流式结果** | `stream_results` | 默认流式 |
| **清理元数据** | `clean_expired_metadata` | `cleanExpiredMetadata` |
| **规划线程** | - | `planningWorkerPoolSize` |

### 6. 适用场景对比

#### Spark 适用场景

✅ **适合：**
1. **大规模表**：数百万到数十亿文件
2. **按需维护**：不定期手动执行
3. **SQL 集成**：通过 SQL Procedure 调用
4. **复杂查询**：需要 Spark 的优化器
5. **已有 Spark 集群**：复用现有资源

❌ **不适合：**
1. **周期性维护**：需要额外的调度系统
2. **流式任务**：Spark Structured Streaming 集成度低
3. **小规模表**：Spark 启动开销大

#### Flink 适用场景

✅ **适合：**
1. **流式维护**：定时自动执行
2. **实时系统**：与其他 Flink 任务集成
3. **中小规模表**：文件数量在百万级以内
4. **Checkpoint 一致性**：需要精确一次语义
5. **已有 Flink 任务**：作为维护任务嵌入

❌ **不适合：**
1. **超大规模表**：单节点扫描 Manifest 可能成为瓶颈
2. **按需执行**：需要保持 Flink 任务常驻
3. **SQL 集成**：目前无 SQL 接口

### 7. 性能对比

#### 测试场景

假设表有以下特征：
- 1000 个快照
- 每个快照 1000 个 Manifest 文件
- 每个 Manifest 包含 100 个数据文件
- 总计：100 万数据文件

| 阶段 | Spark | Flink |
|------|-------|-------|
| **元数据扫描** | 分布式并行扫描 | 单节点顺序扫描 |
| **文件识别** | Dataset anti-join (Shuffle) | Core 逻辑（内存） |
| **文件删除** | Driver 串行或并发 | 分布式并行批量删除 |
| **总耗时（估算）** | 5-10 分钟 | 10-20 分钟 |
| **内存占用** | Executor 内存 | 单节点内存 |
| **网络开销** | Shuffle | 无 Shuffle |

**结论：**
- **大规模表（百万级文件）**：Spark 更快
- **中小规模表（十万级文件）**：性能相近
- **删除阶段**：Flink 并行删除更快

### 8. 容错机制对比

#### Spark 容错

```java
// 重试机制
Tasks.foreach(pathsToDelete)
    .executeWith(deleteExecutorService)
    .retry(3)
    .stopRetryOn(NotFoundException.class)
    .suppressFailureWhenFinished()
    .run(deleteFuncToUse::accept);
```

**特点：**
- 基于重试的容错
- 失败时记录警告日志
- 不会中断整个任务
- 无状态恢复

#### Flink 容错

```java
@Override
public void prepareSnapshotPreBarrier(long checkpointId) {
  deleteFiles();  // Checkpoint 前完成删除
}
```

**特点：**
- 基于 Checkpoint 的容错
- 失败时可以从 Checkpoint 恢复
- Metrics 记录失败数量
- 有状态恢复

### 9. 监控与可观测性对比

#### Spark 监控

```java
ExpireSnapshots.Result result = action.execute();

LOG.info("Deleted data files: {}", result.deletedDataFilesCount());
LOG.info("Deleted manifests: {}", result.deletedManifestsCount());
LOG.info("Deleted manifest lists: {}", result.deletedManifestListsCount());
```

**特点：**
- 执行结果统计
- 日志输出
- Spark UI 任务跟踪

#### Flink 监控

```java
private transient Counter failedCounter;
private transient Counter succeededCounter;

succeededCounter.inc(filesToDelete.size());
failedCounter.inc(e.numberFailedObjects());
```

**特点：**
- Flink Metrics 系统集成
- 实时监控指标
- Checkpoint 统计
- 任务状态追踪

---

## 性能与适用场景分析

### 1. 大规模表维护（1000万+ 文件）

**推荐：Spark**

理由：
1. 分布式扫描 Manifest 文件
2. Dataset 操作可以利用内存缓存
3. Shuffle 可以处理超大数据集
4. 可以调整 `spark.sql.shuffle.partitions` 优化性能

**配置建议：**

```sql
-- 设置 Shuffle 分区数
SET spark.sql.shuffle.partitions = 2000;

-- 使用流式结果避免 Driver OOM
CALL catalog.system.expire_snapshots(
  table => 'db.large_table',
  older_than => TIMESTAMP '2023-01-01 00:00:00',
  stream_results => true,
  max_concurrent_deletes => 16
);
```

### 2. 周期性维护（每小时/每天）

**推荐：Flink**

理由：
1. 原生流式触发器支持
2. 不需要额外的调度系统
3. 与其他 Flink 任务集成
4. Checkpoint 保证一致性

**实现示例：**

```java
// 每小时触发一次
DataStream<Trigger> trigger = env.addSource(
    new PeriodicTriggerSource(Duration.ofHours(1)));

DataStream<TaskResult> result = ExpireSnapshots.builder()
    .tableLoader(tableLoader)
    .maxSnapshotAge(Duration.ofDays(7))
    .retainLast(10)
    .parallelism(8)
    .append(trigger);
```

### 3. 按需维护（手动触发）

**推荐：Spark Procedure**

理由：
1. SQL 接口简单易用
2. 无需保持常驻任务
3. 集成到现有 Spark SQL 工作流
4. 结果立即可见

**使用示例：**

```sql
-- 简单调用
CALL catalog.system.expire_snapshots('db.table');

-- 高级配置
CALL catalog.system.expire_snapshots(
  table => 'db.table',
  older_than => TIMESTAMP '2023-01-01 00:00:00',
  retain_last => 10,
  clean_expired_metadata => true
);
```

### 4. 实时数据湖（持续写入）

**推荐：Flink**

理由：
1. 与实时写入任务集成
2. 流式维护避免积累过多快照
3. 低延迟删除过期文件
4. 统一的流式架构

**架构示例：**

```java
// 实时写入 + 维护任务
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// 写入流
DataStream<RowData> input = ...;
FlinkSink.forRowData(input)
    .table(table)
    .append();

// 维护流
DataStream<Trigger> trigger = env.addSource(new PeriodicTriggerSource(Duration.ofHours(1)));
ExpireSnapshots.builder()
    .tableLoader(tableLoader)
    .maxSnapshotAge(Duration.ofDays(1))
    .append(trigger);
```

### 5. 多表维护

#### Spark 实现

```sql
-- 批量维护多表
WITH tables AS (
  SELECT 'db.table1' AS table_name
  UNION ALL SELECT 'db.table2'
  UNION ALL SELECT 'db.table3'
)
SELECT
  table_name,
  CALL catalog.system.expire_snapshots(table_name) AS result
FROM tables;
```

#### Flink 实现

```java
// 为每个表创建独立的维护流
List<String> tables = Arrays.asList("db.table1", "db.table2", "db.table3");

for (String tableName : tables) {
  TableLoader loader = TableLoader.fromCatalog(catalog, TableIdentifier.parse(tableName));

  ExpireSnapshots.builder()
      .tableLoader(loader)
      .maxSnapshotAge(Duration.ofDays(7))
      .taskName("expire_" + tableName)
      .append(trigger);
}
```

---

## 最佳实践建议

### 1. Spark 最佳实践

#### 配置优化

```sql
-- 1. 调整 Shuffle 分区数（根据数据量）
SET spark.sql.shuffle.partitions = 2000;

-- 2. 使用流式结果避免 Driver OOM
SET spark.sql.execution.arrow.enabled = true;

-- 3. 调整并发删除线程数
SET spark.sql.iceberg.expire-snapshots.max-concurrent-deletes = 16;
```

#### 调用模式

```java
// 1. 对于大表使用流式模式
SparkActions.get()
    .expireSnapshots(table)
    .option(ExpireSnapshotsSparkAction.STREAM_RESULTS, "true")
    .execute();

// 2. 定期执行（通过 Airflow/Oozie 等调度）
// Schedule: 0 2 * * * (每天凌晨 2 点)
CALL catalog.system.expire_snapshots(
  table => 'db.table',
  older_than => CURRENT_TIMESTAMP - INTERVAL 7 DAYS,
  retain_last => 10
);

// 3. 分阶段执行（先提交快照，再删除文件）
// 阶段 1: 仅过期快照
table.expireSnapshots()
    .expireOlderThan(timestampMillis)
    .cleanExpiredFiles(false)
    .commit();

// 阶段 2: 使用 Spark 删除文件
SparkActions.get()
    .expireSnapshots(table)
    .execute();
```

#### 监控建议

```java
// 记录执行指标
ExpireSnapshots.Result result = action.execute();

metrics.recordGauge("expire_snapshots.deleted_data_files", result.deletedDataFilesCount());
metrics.recordGauge("expire_snapshots.deleted_manifests", result.deletedManifestsCount());
metrics.recordGauge("expire_snapshots.deleted_manifest_lists", result.deletedManifestListsCount());
```

### 2. Flink 最佳实践

#### 配置优化

```java
// 1. 根据表大小调整规划线程池
ExpireSnapshots.builder()
    .planningWorkerPoolSize(8)  // 大表使用更多线程
    .build();

// 2. 根据文件数量调整删除批次大小
ExpireSnapshots.builder()
    .deleteBatchSize(5000)  // 大表使用更大批次
    .build();

// 3. 调整删除并行度
ExpireSnapshots.builder()
    .parallelism(16)  // 增加删除并行度
    .build();
```

#### 触发器设计

```java
// 1. 周期性触发
DataStream<Trigger> periodicTrigger = env.addSource(
    new PeriodicTriggerSource(Duration.ofHours(1)));

// 2. 条件触发（基于快照数量）
DataStream<Trigger> conditionalTrigger = env.addSource(
    new ConditionalTriggerSource(
        tableLoader,
        snapshotCount -> snapshotCount > 100));

// 3. 事件触发（基于写入事件）
DataStream<Trigger> eventTrigger = writeStream
    .map(event -> new Trigger(...));
```

#### 容错配置

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// 1. 启用 Checkpoint
env.enableCheckpointing(Duration.ofMinutes(10).toMillis());

// 2. 配置 Checkpoint 模式
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

// 3. 配置 Checkpoint 超时
env.getCheckpointConfig().setCheckpointTimeout(Duration.ofMinutes(5).toMillis());

// 4. 配置最小间隔
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(Duration.ofMinutes(1).toMillis());
```

#### 监控建议

```java
// 1. 使用 Flink Metrics
MetricGroup taskMetricGroup = TableMaintenanceMetrics.groupFor(
    getRuntimeContext(), tableName, taskName, taskIndex);

Counter succeededCounter = taskMetricGroup.counter("delete_file_succeeded_counter");
Counter failedCounter = taskMetricGroup.counter("delete_file_failed_counter");

// 2. 输出 TaskResult
result.addSink(new RichSinkFunction<TaskResult>() {
  @Override
  public void invoke(TaskResult value, Context context) {
    if (!value.success()) {
      LOG.error("ExpireSnapshots failed: {}", value.exceptions());
      alertSystem.sendAlert("ExpireSnapshots Failed", value);
    }
  }
});
```

### 3. 通用最佳实践

#### 过期策略

```java
// 1. 基于时间 + 数量双重保护
table.expireSnapshots()
    .expireOlderThan(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7))  // 7 天前
    .retainLast(10)  // 至少保留 10 个快照
    .commit();

// 2. 为不同分支设置不同策略
table.manageSnapshots()
    .createBranch("dev", currentSnapshotId)
    .setMaxSnapshotAgeMs("dev", TimeUnit.DAYS.toMillis(1))  // dev 分支 1 天
    .setMinSnapshotsToKeep("dev", 5)
    .commit();

table.manageSnapshots()
    .createBranch("prod", currentSnapshotId)
    .setMaxSnapshotAgeMs("prod", TimeUnit.DAYS.toMillis(30))  // prod 分支 30 天
    .setMinSnapshotsToKeep("prod", 50)
    .commit();
```

#### 清理元数据

```java
// 定期清理未使用的 Schema 和 PartitionSpec
table.expireSnapshots()
    .expireOlderThan(timestampMillis)
    .cleanExpiredMetadata(true)  // 启用元数据清理
    .commit();
```

#### 分阶段维护

```java
// 1. 第一阶段：过期快照（快速）
table.expireSnapshots()
    .expireOlderThan(timestampMillis)
    .cleanExpiredFiles(false)  // 不删除文件
    .commit();

// 2. 第二阶段：删除文件（慢速，可分布式）
// Spark: 使用 Actions API
SparkActions.get().expireSnapshots(table).execute();

// Flink: 使用流式处理
ExpireSnapshots.builder()
    .tableLoader(tableLoader)
    .append(trigger);
```

#### 监控告警

```java
// 1. 监控快照数量
long snapshotCount = StreamSupport.stream(table.snapshots().spliterator(), false).count();
if (snapshotCount > 1000) {
  LOG.warn("Table {} has {} snapshots, consider running expire_snapshots",
           table.name(), snapshotCount);
}

// 2. 监控表大小
TableMetadata metadata = ((HasTableOperations) table).operations().current();
long totalFiles = metadata.currentSnapshot().allManifests(table.io()).stream()
    .mapToLong(ManifestFile::existingFilesCount)
    .sum();
if (totalFiles > 10_000_000) {
  LOG.warn("Table {} has {} files, consider partitioning or compaction",
           table.name(), totalFiles);
}

// 3. 监控过期执行时长
long startTime = System.currentTimeMillis();
table.expireSnapshots().expireOlderThan(timestampMillis).commit();
long duration = System.currentTimeMillis() - startTime;
metrics.recordTimer("expire_snapshots.duration", duration);
```

---

## 总结

### 核心差异总结

| 维度 | Spark | Flink |
|------|-------|-------|
| **设计哲学** | 批处理 + 分布式计算 | 流处理 + 轻量化 |
| **文件识别** | Dataset anti-join（分布式） | Core 逻辑（单节点） |
| **文件删除** | Driver 串行/并发 | Task 并行批量 |
| **调用接口** | SQL Procedure + Actions API | DataStream Builder API |
| **适用规模** | 大规模（千万级文件） | 中小规模（百万级文件） |
| **维护模式** | 按需手动触发 | 周期性自动触发 |
| **集成方式** | Spark SQL 生态 | Flink 流式生态 |

### 选型建议

**选择 Spark 如果：**
- 表规模超大（千万级文件）
- 需要按需手动维护
- 已有 Spark 集群和 SQL 工作流
- 不需要实时/流式维护

**选择 Flink 如果：**
- 需要周期性自动维护
- 已有 Flink 流式任务
- 表规模适中（百万级以内）
- 需要与实时写入集成
- 需要精确一次语义保证

**两者结合：**
- 日常维护使用 Flink（周期性）
- 大规模清理使用 Spark（手动触发）
- 元数据清理使用 Core API（轻量化）

---

## 附录：完整调用示例

### Spark 完整示例

```scala
// Scala Spark 示例
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.iceberg.catalog.TableIdentifier
import java.util.concurrent.TimeUnit

object ExpireSnapshotsExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Iceberg ExpireSnapshots")
      .config("spark.sql.catalog.catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.catalog.type", "hadoop")
      .config("spark.sql.catalog.catalog.warehouse", "hdfs://warehouse")
      .config("spark.sql.shuffle.partitions", "2000")
      .getOrCreate()

    // 方式 1: SQL Procedure
    spark.sql("""
      CALL catalog.system.expire_snapshots(
        table => 'db.table',
        older_than => TIMESTAMP '2023-01-01 00:00:00',
        retain_last => 10,
        max_concurrent_deletes => 16,
        stream_results => true,
        clean_expired_metadata => true
      )
    """).show()

    // 方式 2: Actions API
    val catalog = spark.sessionState.catalogManager.catalog("catalog")
    val table = catalog.loadTable(TableIdentifier.of("db", "table"))

    val result = SparkActions.get()
      .expireSnapshots(table)
      .expireOlderThan(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7))
      .retainLast(10)
      .option(ExpireSnapshotsSparkAction.STREAM_RESULTS, "true")
      .cleanExpiredMetadata(true)
      .execute()

    println(s"Deleted data files: ${result.deletedDataFilesCount()}")
    println(s"Deleted manifests: ${result.deletedManifestsCount()}")
    println(s"Deleted manifest lists: ${result.deletedManifestListsCount()}")

    spark.stop()
  }
}
```

### Flink 完整示例

```java
// Java Flink 示例
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.*;
import java.time.Duration;

public class ExpireSnapshotsExample {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // 配置 Checkpoint
    env.enableCheckpointing(Duration.ofMinutes(10).toMillis());
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

    // 创建触发器（每小时触发一次）
    DataStream<Trigger> trigger = env.addSource(
        new PeriodicTriggerSource(Duration.ofHours(1)));

    // 创建 TableLoader
    TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://warehouse/db/table");

    // 构建 ExpireSnapshots 流
    DataStream<TaskResult> result = ExpireSnapshots.builder()
        .tableLoader(tableLoader)
        .maxSnapshotAge(Duration.ofDays(7))
        .retainLast(10)
        .planningWorkerPoolSize(8)
        .deleteBatchSize(2000)
        .cleanExpiredMetadata(true)
        .parallelism(16)
        .slotSharingGroup("maintenance")
        .append(trigger);

    // 监控结果
    result.addSink(new RichSinkFunction<TaskResult>() {
      @Override
      public void invoke(TaskResult value, Context context) {
        if (value.success()) {
          LOG.info("ExpireSnapshots succeeded at {}", value.timestamp());
        } else {
          LOG.error("ExpireSnapshots failed: {}", value.exceptions());
        }
      }
    });

    env.execute("Iceberg ExpireSnapshots Maintenance");
  }
}
```

---

**文档版本：** 1.0
**生成日期：** 2025-11-15
**Iceberg 版本：** 1.10.x (基于 1.10.x 分支源码分析)
**分析深度：** 源码级深度分析

---

## 参考资源

### 源码位置

**Core 模块：**
- `api/src/main/java/org/apache/iceberg/ExpireSnapshots.java`
- `core/src/main/java/org/apache/iceberg/RemoveSnapshots.java`
- `core/src/main/java/org/apache/iceberg/FileCleanupStrategy.java`
- `core/src/main/java/org/apache/iceberg/IncrementalFileCleanup.java`
- `core/src/main/java/org/apache/iceberg/ReachableFileCleanup.java`

**Spark 模块：**
- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/ExpireSnapshotsSparkAction.java`
- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/ExpireSnapshotsProcedure.java`

**Flink 模块：**
- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/maintenance/api/ExpireSnapshots.java`
- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/maintenance/operator/ExpireSnapshotsProcessor.java`
- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/maintenance/operator/DeleteFilesProcessor.java`

### 关键配置项

**表属性：**
- `write.metadata.delete-after-commit.enabled`: 是否在提交后删除旧元数据
- `write.metadata.previous-versions-max`: 保留的元数据版本数
- `history.expire.max-snapshot-age-ms`: 快照最大保留时间
- `history.expire.min-snapshots-to-keep`: 最小保留快照数
- `gc.enabled`: 是否启用垃圾回收

**Spark 配置：**
- `spark.sql.shuffle.partitions`: Shuffle 分区数
- `spark.sql.iceberg.expire-snapshots.max-concurrent-deletes`: 最大并发删除数

**Flink 配置：**
- `table-loader`: 表加载器
- `max-snapshot-age`: 最大快照年龄
- `retain-last`: 保留快照数
- `planning-worker-pool-size`: 规划线程池大小
- `delete-batch-size`: 删除批次大小
- `parallelism`: 删除并行度
