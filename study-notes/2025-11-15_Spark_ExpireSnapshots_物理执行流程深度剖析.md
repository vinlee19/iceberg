# 2025-11-15 Spark ExpireSnapshots 物理执行流程深度剖析

## 问题解析

### Q1: Procedure 和 SparkAction 是一样的功能还是组合在一起的？

**答案：组合关系**

`ExpireSnapshotsProcedure` 和 `ExpireSnapshotsSparkAction` 是**组合关系**，而非重复实现：

```
ExpireSnapshotsProcedure (SQL 接口层)
    │
    │ 调用
    ▼
ExpireSnapshotsSparkAction (分布式实现层)
    │
    │ 调用
    ▼
RemoveSnapshots (Core 核心逻辑层)
```

#### 详细说明

**ExpireSnapshotsProcedure 的职责：**
- 提供 SQL 接口（CALL 语法）
- 参数解析和验证
- 调用 `ExpireSnapshotsSparkAction`
- 将结果转换为 SQL 返回格式

**ExpireSnapshotsSparkAction 的职责：**
- 实现分布式文件识别（使用 Spark Dataset）
- 文件删除逻辑
- 执行结果统计
- 提供编程式 API

### 源码验证

#### 1. Procedure 调用 SparkAction

位置: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/ExpireSnapshotsProcedure.java:115-160`

```java
public InternalRow[] call(InternalRow args) {
  // ... 参数解析 ...

  return modifyIcebergTable(tableIdent, table -> {
    // 创建 ExpireSnapshots Action（实际是 ExpireSnapshotsSparkAction）
    ExpireSnapshots action = actions().expireSnapshots(table);

    // 配置参数
    if (olderThanMillis != null) {
      action.expireOlderThan(olderThanMillis);
    }
    if (retainLastNum != null) {
      action.retainLast(retainLastNum);
    }
    // ... 更多配置 ...

    // 执行并返回结果
    ExpireSnapshots.Result result = action.execute();
    return toOutputRows(result);
  });
}
```

#### 2. SparkActions 工厂方法

位置: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkActions.java:89-91`

```java
@Override
public ExpireSnapshotsSparkAction expireSnapshots(Table table) {
  return new ExpireSnapshotsSparkAction(spark, table);
}
```

**关系图：**

```
┌────────────────────────────────────────────────────────┐
│  SQL 层                                                 │
│                                                         │
│  CALL catalog.system.expire_snapshots(...)             │
│                                                         │
└───────────────────────┬────────────────────────────────┘
                        │
                        ▼
┌────────────────────────────────────────────────────────┐
│  Procedure 层                                           │
│                                                         │
│  ExpireSnapshotsProcedure                              │
│  - 参数解析 (TimestampType → milliseconds)             │
│  - 参数验证 (max_concurrent_deletes > 0)               │
│  - 调用 actions().expireSnapshots(table)               │
│  - 结果转换 (Result → InternalRow[])                   │
│                                                         │
└───────────────────────┬────────────────────────────────┘
                        │
                        ▼
┌────────────────────────────────────────────────────────┐
│  SparkAction 层                                         │
│                                                         │
│  ExpireSnapshotsSparkAction                            │
│  - 分布式文件识别 (Dataset anti-join)                  │
│  - 批量/流式删除                                        │
│  - 执行统计                                             │
│                                                         │
└───────────────────────┬────────────────────────────────┘
                        │
                        ▼
┌────────────────────────────────────────────────────────┐
│  Core 层                                                │
│                                                         │
│  RemoveSnapshots                                        │
│  - 快照过期逻辑                                         │
│  - 元数据更新                                           │
│  - 文件清理策略                                         │
│                                                         │
└────────────────────────────────────────────────────────┘
```

---

## 完整物理执行流程

### 流程概览

```
┌─────────────────────────────────────────────────────────────┐
│ 阶段 0: 初始化                                               │
│ - 加载表元数据                                               │
│ - 验证 GC 启用                                               │
│ - 获取配置参数                                               │
└────────────┬────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────┐
│ 阶段 1: 快照过期（Core RemoveSnapshots）                     │
│ - 确定要保留的快照                                           │
│ - 更新表元数据（移除快照）                                   │
│ - 提交元数据变更                                             │
│ - cleanExpiredFiles(false) ← 不删除文件！                   │
└────────────┬────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────┐
│ 阶段 2: 文件识别（Spark Dataset）                            │
│ - 获取过期前后的元数据                                       │
│ - 使用 Spark 读取元数据表                                    │
│ - 通过 Dataset.except() 计算过期文件                        │
└────────────┬────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────┐
│ 阶段 3: 文件删除（Driver 或 ExecutorService）                │
│ - 批量收集或流式处理                                         │
│ - 使用 BulkDelete 或并发删除                                 │
│ - 统计删除结果                                               │
└────────────┬────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────┐
│ 阶段 4: 返回结果                                             │
│ - 构建 Result 对象                                           │
│ - 记录日志                                                   │
└─────────────────────────────────────────────────────────────┘
```

---

## 详细执行流程

### 阶段 0: 初始化 (Driver 节点)

#### 时序图

```
User/SQL Client                  Spark Driver                    Iceberg Table
      │                                │                                │
      │  CALL expire_snapshots(...)   │                                │
      │──────────────────────────────>│                                │
      │                                │                                │
      │                                │  ops.current()                 │
      │                                │───────────────────────────────>│
      │                                │                                │
      │                                │  TableMetadata (Before)        │
      │                                │<───────────────────────────────│
      │                                │                                │
      │                                │  验证 GC 启用                  │
      │                                │  (GC_ENABLED = true)           │
      │                                │                                │
```

#### 核心代码

```java
// ExpireSnapshotsSparkAction 构造函数
ExpireSnapshotsSparkAction(SparkSession spark, Table table) {
  super(spark);
  this.table = table;
  this.ops = ((HasTableOperations) table).operations();

  // 验证 GC 启用
  ValidationException.check(
      PropertyUtil.propertyAsBoolean(table.properties(), GC_ENABLED, GC_ENABLED_DEFAULT),
      "Cannot expire snapshots: GC is disabled (deleting files may corrupt other tables)");
}
```

**关键点：**
- 在 Driver 节点执行
- 获取表的当前元数据
- 验证表属性 `gc.enabled = true`

---

### 阶段 1: 快照过期 (Driver 节点 → Core 逻辑)

#### 时序图

```
ExpireSnapshotsSparkAction         RemoveSnapshots                  TableOperations
        │                                │                                │
        │  expireFiles()                 │                                │
        │────────────────────────────────>│                                │
        │                                │                                │
        │                                │  ops.current()                 │
        │                                │───────────────────────────────>│
        │                                │                                │
        │                                │  TableMetadata (Before)        │
        │                                │<───────────────────────────────│
        │                                │                                │
        │                                │  计算保留快照                  │
        │                                │  - computeRetainedRefs()       │
        │                                │  - computeBranchSnapshots()    │
        │                                │  - unreferencedSnapshots()     │
        │                                │                                │
        │                                │  构建新元数据                  │
        │                                │  - removeSnapshots(idsToRemove)│
        │                                │  - cleanExpiredMetadata()      │
        │                                │                                │
        │                                │  cleanExpiredFiles(false)      │
        │                                │  ← 重点：不删除文件！          │
        │                                │                                │
        │                                │  commit()                      │
        │                                │───────────────────────────────>│
        │                                │                                │
        │                                │  原子更新元数据文件            │
        │                                │  (v1.metadata.json → v2)       │
        │                                │                                │
        │                                │  Committed                     │
        │                                │<───────────────────────────────│
        │                                │                                │
        │  Snapshots expired             │                                │
        │<────────────────────────────────│                                │
        │                                │                                │
```

#### 核心代码

```java
// ExpireSnapshotsSparkAction.expireFiles()
public Dataset<FileInfo> expireFiles() {
  if (expiredFileDS == null) {
    // 1. 获取过期前的元数据
    TableMetadata originalMetadata = ops.current();

    // 2. 创建 Core 的 ExpireSnapshots
    org.apache.iceberg.ExpireSnapshots expireSnapshots = table.expireSnapshots();

    // 3. 配置过期策略
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

    // 4. 关键：仅提交快照过期，不删除文件
    expireSnapshots.cleanExpiredFiles(false).commit();

    // 5. 获取过期后的元数据
    TableMetadata updatedMetadata = ops.refresh();

    // ... 文件识别逻辑（阶段 2）...
  }
  return expiredFileDS;
}
```

**关键点：**
- `cleanExpiredFiles(false)` - 禁用 Core 的文件删除
- 仅更新元数据，移除快照引用
- 使用乐观锁提交元数据变更
- 如果并发冲突，会重试

**元数据变更示例：**

```json
// Before (v1.metadata.json)
{
  "snapshots": [
    {"snapshot-id": 1, "timestamp-ms": 1609459200000},  ← 要删除
    {"snapshot-id": 2, "timestamp-ms": 1609545600000},  ← 要删除
    {"snapshot-id": 3, "timestamp-ms": 1609632000000},  ← 保留
    {"snapshot-id": 4, "timestamp-ms": 1609718400000}   ← 保留 (current)
  ],
  "current-snapshot-id": 4,
  "refs": {
    "main": {"snapshot-id": 4, "type": "branch"}
  }
}

// After (v2.metadata.json)
{
  "snapshots": [
    {"snapshot-id": 3, "timestamp-ms": 1609632000000},
    {"snapshot-id": 4, "timestamp-ms": 1609718400000}
  ],
  "current-snapshot-id": 4,
  "refs": {
    "main": {"snapshot-id": 4, "type": "branch"}
  }
}
```

---

### 阶段 2: 文件识别 (Driver + Spark Executors)

这是 **Spark 实现的核心差异点**！

#### 时序图

```
Driver                            Spark Executors                   Metadata Tables
  │                                      │                                │
  │  fileDS(updatedMetadata)            │                                │
  │──────────────────────────────────────────────────────────────────────>│
  │                                      │                                │
  │                                      │  读取 all_data_files           │
  │                                      │  读取 all_manifests             │
  │                                      │  读取 manifest_lists           │
  │                                      │<───────────────────────────────│
  │                                      │                                │
  │  validFileDS (Dataset<FileInfo>)    │                                │
  │<──────────────────────────────────────                                │
  │                                      │                                │
  │  fileDS(originalMetadata, expiredIds)│                                │
  │──────────────────────────────────────────────────────────────────────>│
  │                                      │                                │
  │                                      │  读取过期快照的文件            │
  │                                      │<───────────────────────────────│
  │                                      │                                │
  │  deleteCandidateFileDS              │                                │
  │<──────────────────────────────────────                                │
  │                                      │                                │
  │  deleteCandidateFileDS.except(validFileDS)                            │
  │  ────────────────────────────────────>                                │
  │                                      │                                │
  │                                      │  执行 Shuffle (Anti-Join)      │
  │                                      │  - Hash 分区                    │
  │                                      │  - 过滤重复                     │
  │                                      │                                 │
  │                                      │                                │
  │  expiredFileDS (Dataset<FileInfo>)  │                                │
  │<──────────────────────────────────────                                │
  │                                      │                                │
```

#### 核心代码

```java
// ExpireSnapshotsSparkAction.expireFiles() - 文件识别部分
public Dataset<FileInfo> expireFiles() {
  // ... 阶段 1 代码 ...

  // 1. 获取过期后的有效文件（所有快照引用的文件）
  TableMetadata updatedMetadata = ops.refresh();
  Dataset<FileInfo> validFileDS = fileDS(updatedMetadata);

  // 2. 获取过期快照引用的文件
  Set<Long> deletedSnapshotIds = findExpiredSnapshotIds(originalMetadata, updatedMetadata);
  Dataset<FileInfo> deleteCandidateFileDS = fileDS(originalMetadata, deletedSnapshotIds);

  // 3. 计算差集：deleteCandidateFileDS - validFileDS = expiredFileDS
  this.expiredFileDS = deleteCandidateFileDS.except(validFileDS);

  return expiredFileDS;
}
```

#### fileDS() 详细实现

```java
private Dataset<FileInfo> fileDS(TableMetadata metadata, Set<Long> snapshotIds) {
  Table staticTable = newStaticTable(metadata, table.io());

  return contentFileDS(staticTable, snapshotIds)        // 数据文件
      .union(manifestDS(staticTable, snapshotIds))      // Manifest 文件
      .union(manifestListDS(staticTable, snapshotIds))  // Manifest List 文件
      .union(statisticsFileDS(staticTable, snapshotIds)); // 统计文件
}
```

**1. contentFileDS() - 数据文件识别**

```java
protected Dataset<FileInfo> contentFileDS(Table table, Set<Long> snapshotIds) {
  Table serializableTable = SerializableTableWithSize.copyOf(table);
  Broadcast<Table> tableBroadcast = sparkContext.broadcast(serializableTable);
  int numShufflePartitions = spark.sessionState().conf().numShufflePartitions();

  // 读取 all_manifests 元数据表
  Dataset<ManifestFileBean> manifestBeanDS =
      manifestDF(table, snapshotIds)
          .selectExpr(
              "content",
              "path",
              "length",
              "0 as sequenceNumber",
              "partition_spec_id as partitionSpecId",
              "added_snapshot_id as addedSnapshotId")
          .dropDuplicates("path")
          .repartition(numShufflePartitions) // 避免自适应执行合并任务
          .as(ManifestFileBean.ENCODER);

  // flatMap 读取每个 Manifest 文件的内容
  return manifestBeanDS.flatMap(new ReadManifest(tableBroadcast), FileInfo.ENCODER);
}
```

**ReadManifest FlatMap 函数：**

```java
static class ReadManifest implements FlatMapFunction<ManifestFileBean, FileInfo> {
  private final Broadcast<Table> tableBroadcast;

  ReadManifest(Broadcast<Table> tableBroadcast) {
    this.tableBroadcast = tableBroadcast;
  }

  @Override
  public Iterator<FileInfo> call(ManifestFileBean manifest) throws Exception {
    Table table = tableBroadcast.value();

    // 读取 Manifest 文件
    try (CloseableIterator<ContentFile<?>> iterator =
           ManifestFiles.read(manifest, table.io()).iterator()) {

      // 转换为 FileInfo
      return Iterators.transform(iterator, file -> {
        String type = file.content().name(); // DATA, POSITION_DELETES, EQUALITY_DELETES
        return new FileInfo(file.location(), type);
      });
    }
  }
}
```

**2. manifestDS() - Manifest 文件识别**

```java
protected Dataset<FileInfo> manifestDS(Table table, Set<Long> snapshotIds) {
  return manifestDF(table, snapshotIds)
      .select(col("path"), lit(MANIFEST).as("type"))
      .as(FileInfo.ENCODER);
}

private Dataset<Row> manifestDF(Table table, Set<Long> snapshotIds) {
  // 读取 all_manifests 元数据表
  Dataset<Row> manifestDF = loadMetadataTable(table, ALL_MANIFESTS);

  if (snapshotIds != null) {
    // 过滤指定快照的 Manifests
    Column filterCond = col(AllManifestsTable.REF_SNAPSHOT_ID.name()).isInCollection(snapshotIds);
    return manifestDF.filter(filterCond);
  } else {
    return manifestDF;
  }
}
```

**3. manifestListDS() - Manifest List 文件识别**

```java
protected Dataset<FileInfo> manifestListDS(Table table, Set<Long> snapshotIds) {
  // 直接从内存中获取 Manifest List 位置
  List<String> manifestLists = ReachableFileUtil.manifestListLocations(table, snapshotIds);
  return toFileInfoDS(manifestLists, MANIFEST_LIST);
}
```

**4. statisticsFileDS() - 统计文件识别**

```java
protected Dataset<FileInfo> statisticsFileDS(Table table, Set<Long> snapshotIds) {
  List<String> statisticsFiles =
      ReachableFileUtil.statisticsFilesLocationsForSnapshots(table, snapshotIds);
  return toFileInfoDS(statisticsFiles, STATISTICS_FILES);
}
```

#### Dataset.except() 实现

```java
// deleteCandidateFileDS.except(validFileDS)
this.expiredFileDS = deleteCandidateFileDS.except(validFileDS);
```

**Spark 执行计划：**

```
== Physical Plan ==
HashAggregate(keys=[path#10, type#11], functions=[])
+- Exchange hashpartitioning(path#10, type#11, 200)
   +- HashAggregate(keys=[path#10, type#11], functions=[])
      +- Union
         :- Project [file_path#1 AS path#10, content#2 AS type#11]
         :  +- Scan [file_path#1, content#2] (deleteCandidateFileDS)
         +- Project [file_path#3 AS path#10, content#4 AS type#11]
            +- LeftAnti Join BuildRight
               :- Scan [file_path#3, content#4] (deleteCandidateFileDS)
               +- Scan [file_path#5, content#6] (validFileDS)
```

**执行步骤：**

1. **Scan**: 读取 deleteCandidateFileDS 和 validFileDS
2. **LeftAnti Join**: 左反连接（保留左表中右表没有的记录）
3. **HashPartitioning**: Shuffle 重分区（默认 200 个分区）
4. **HashAggregate**: 去重（基于 path + type）

**内存占用：**
- Executor 内存：取决于文件数量和 Shuffle 分区数
- Driver 内存：如果使用 `collectAsList()`，需要容纳所有过期文件

---

### 阶段 3: 文件删除 (Driver 节点)

#### 流程选择

```
                    streamResults()?
                          │
          ┌───────────────┼───────────────┐
          │ true                          │ false
          ▼                               ▼
  toLocalIterator()                collectAsList()
  (流式处理)                        (批量处理)
          │                               │
          │                               │
          └───────────────┬───────────────┘
                          │
                          ▼
              table.io() instanceof SupportsBulkOperations?
                          │
          ┌───────────────┼───────────────┐
          │ true                          │ false
          ▼                               ▼
  deleteFiles(io, files)          deleteFiles(executor, func, files)
  (批量删除 API)                   (并发单文件删除)
```

#### 时序图（批量删除模式）

```
Driver                            Executors                         Object Storage
  │                                    │                                  │
  │  expiredFileDS.collectAsList()    │                                  │
  │───────────────────────────────────>│                                  │
  │                                    │                                  │
  │                                    │  执行 Spark Job                  │
  │                                    │  - Scan 元数据表                 │
  │                                    │  - Anti-Join                     │
  │                                    │  - 收集结果                      │
  │                                    │                                  │
  │  List<FileInfo> (所有过期文件)     │                                  │
  │<───────────────────────────────────│                                  │
  │                                    │                                  │
  │  partition(files, 100000)         │                                  │
  │  分批处理                          │                                  │
  │                                    │                                  │
  │  io.deleteFiles(batch1)           │                                  │
  │───────────────────────────────────────────────────────────────────────>│
  │                                    │                                  │
  │                                    │  批量删除 100k 文件              │
  │                                    │  (S3 DeleteObjects API)          │
  │                                    │                                  │
  │  Deleted (or BulkDeletionFailure)  │                                  │
  │<───────────────────────────────────────────────────────────────────────│
  │                                    │                                  │
  │  io.deleteFiles(batch2)           │                                  │
  │───────────────────────────────────────────────────────────────────────>│
  │                                    │                                  │
  │  ... 重复直到所有批次完成 ...      │                                  │
  │                                    │                                  │
```

#### 核心代码

**1. 批量收集模式**

```java
private ExpireSnapshots.Result doExecute() {
  if (streamResults()) {
    // 流式模式：逐条处理，不收集所有结果到 Driver
    return deleteFiles(expireFiles().toLocalIterator());
  } else {
    // 批量模式：先收集所有结果到 Driver
    return deleteFiles(expireFiles().collectAsList().iterator());
  }
}
```

**2. 批量删除实现**

```java
protected DeleteSummary deleteFiles(SupportsBulkOperations io, Iterator<FileInfo> files) {
  DeleteSummary summary = new DeleteSummary();

  // 分批：每批 100,000 个文件
  Iterator<List<FileInfo>> fileGroups = Iterators.partition(files, DELETE_GROUP_SIZE);

  Tasks.foreach(fileGroups)
      .suppressFailureWhenFinished()
      .run(fileGroup -> deleteFileGroup(fileGroup, io, summary));

  return summary;
}

private static void deleteFileGroup(
    List<FileInfo> fileGroup, SupportsBulkOperations io, DeleteSummary summary) {

  // 按类型分组（DATA, MANIFEST, MANIFEST_LIST, etc.）
  ListMultimap<String, FileInfo> filesByType = Multimaps.index(fileGroup, FileInfo::getType);
  ListMultimap<String, String> pathsByType =
      Multimaps.transformValues(filesByType, FileInfo::getPath);

  for (Map.Entry<String, Collection<String>> entry : pathsByType.asMap().entrySet()) {
    String type = entry.getKey();
    Collection<String> paths = entry.getValue();
    int failures = 0;

    try {
      // 调用批量删除 API（例如 S3 的 DeleteObjects）
      io.deleteFiles(paths);
    } catch (BulkDeletionFailureException e) {
      failures = e.numberFailedObjects();
      LOG.warn("Bulk deletion failed for {} of {} {} files", failures, paths.size(), type);
    }

    summary.deletedFiles(type, paths.size() - failures);
  }
}
```

**3. 并发单文件删除实现**

```java
protected DeleteSummary deleteFiles(
    ExecutorService executorService, Consumer<String> deleteFunc, Iterator<FileInfo> files) {

  DeleteSummary summary = new DeleteSummary();

  Tasks.foreach(files)
      .retry(DELETE_NUM_RETRIES)  // 重试 3 次
      .stopRetryOn(NotFoundException.class)
      .suppressFailureWhenFinished()
      .executeWith(executorService)  // 使用线程池并发执行
      .onFailure((fileInfo, exc) -> {
        LOG.warn("Delete failed for {}: {}", fileInfo.getType(), fileInfo.getPath(), exc);
      })
      .run(fileInfo -> {
        deleteFunc.accept(fileInfo.getPath());
        summary.deletedFile(fileInfo.getPath(), fileInfo.getType());
      });

  return summary;
}
```

#### S3 批量删除示例

```java
// S3FileIO 实现 SupportsBulkOperations
public class S3FileIO implements FileIO, SupportsBulkOperations {

  @Override
  public void deleteFiles(Iterable<String> paths) throws BulkDeletionFailureException {
    List<ObjectIdentifier> keys = Lists.newArrayList();
    for (String path : paths) {
      keys.add(ObjectIdentifier.builder().key(toS3Key(path)).build());
    }

    // S3 API 限制：每次最多删除 1000 个对象
    List<List<ObjectIdentifier>> batches = Lists.partition(keys, 1000);
    int totalFailures = 0;

    for (List<ObjectIdentifier> batch : batches) {
      DeleteObjectsRequest request = DeleteObjectsRequest.builder()
          .bucket(bucket)
          .delete(Delete.builder().objects(batch).build())
          .build();

      DeleteObjectsResponse response = s3Client.deleteObjects(request);

      // 检查错误
      if (response.hasErrors()) {
        totalFailures += response.errors().size();
        for (S3Error error : response.errors()) {
          LOG.warn("Failed to delete {}: {}", error.key(), error.message());
        }
      }
    }

    if (totalFailures > 0) {
      throw new BulkDeletionFailureException(totalFailures);
    }
  }
}
```

---

### 阶段 4: 返回结果 (Driver 节点)

#### 时序图

```
Driver                            Procedure                         SQL Client
  │                                    │                                  │
  │  DeleteSummary                     │                                  │
  │  - dataFilesCount                  │                                  │
  │  - manifestsCount                  │                                  │
  │  - manifestListsCount              │                                  │
  │  - ...                             │                                  │
  │                                    │                                  │
  │  构建 Result 对象                  │                                  │
  │────────────────────────────────────>│                                  │
  │                                    │                                  │
  │                                    │  转换为 InternalRow[]            │
  │                                    │                                  │
  │                                    │  InternalRow[]:                  │
  │                                    │  [100, 50, 20, 30, 10, 5]        │
  │                                    │──────────────────────────────────>│
  │                                    │                                  │
  │                                    │                                  │
  │                                    │  显示结果表格                    │
  │                                    │  +----------+----------+         │
  │                                    │  | deleted  | deleted  |         │
  │                                    │  | data     | manifests|         │
  │                                    │  +----------+----------+         │
  │                                    │  | 100      | 30       |         │
  │                                    │  +----------+----------+         │
  │                                    │                                  │
```

#### 核心代码

```java
// ExpireSnapshotsSparkAction.deleteFiles()
private ExpireSnapshots.Result deleteFiles(Iterator<FileInfo> files) {
  DeleteSummary summary;

  // ... 执行删除 ...

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

// ExpireSnapshotsProcedure.toOutputRows()
private InternalRow[] toOutputRows(ExpireSnapshots.Result result) {
  InternalRow row = newInternalRow(
      result.deletedDataFilesCount(),
      result.deletedPositionDeleteFilesCount(),
      result.deletedEqualityDeleteFilesCount(),
      result.deletedManifestsCount(),
      result.deletedManifestListsCount(),
      result.deletedStatisticsFilesCount());
  return new InternalRow[] {row};
}
```

---

## 完整物理流程示例

### 场景设定

- 表名: `db.orders`
- 快照数: 1000 个
- 每个快照: 100 个 Manifest 文件
- 每个 Manifest: 100 个数据文件
- 总数据文件: 10,000,000 个
- 过期策略: 保留最近 100 个快照

### 执行命令

```sql
CALL catalog.system.expire_snapshots(
  table => 'db.orders',
  retain_last => 100,
  stream_results => true,
  max_concurrent_deletes => 16
);
```

### 详细执行日志

```
[Driver] 00:00:00 - 初始化 ExpireSnapshotsSparkAction
[Driver] 00:00:00 - 加载表元数据: db.orders
[Driver] 00:00:00 - 验证 GC 启用: true
[Driver] 00:00:00 - 当前快照数: 1000

[Driver] 00:00:01 - 阶段 1: 快照过期
[Driver] 00:00:01 - 创建 RemoveSnapshots
[Driver] 00:00:01 - 配置: retainLast=100
[Driver] 00:00:01 - 计算保留快照: 100 个
[Driver] 00:00:01 - 计算过期快照: 900 个
[Driver] 00:00:02 - 更新元数据: 移除 900 个快照引用
[Driver] 00:00:02 - 提交元数据变更 (v1023.metadata.json → v1024.metadata.json)
[Driver] 00:00:02 - 快照过期完成，耗时: 1.2s

[Driver] 00:00:02 - 阶段 2: 文件识别
[Driver] 00:00:02 - 读取过期后元数据: v1024.metadata.json
[Driver] 00:00:02 - 创建 validFileDS (100 个快照的文件)
[Executor-1] 00:00:03 - 读取 all_manifests: 10,000 个 Manifest
[Executor-2] 00:00:03 - 读取 all_manifests: 10,000 个 Manifest
[Executor-3] 00:00:03 - 读取 all_manifests: 10,000 个 Manifest
...
[Driver] 00:00:15 - validFileDS 计算完成: 1,000,000 个文件

[Driver] 00:00:15 - 创建 deleteCandidateFileDS (900 个过期快照的文件)
[Executor-1] 00:00:16 - 读取过期快照 Manifests: 90,000 个 Manifest
[Executor-2] 00:00:16 - 读取过期快照 Manifests: 90,000 个 Manifest
...
[Driver] 00:02:30 - deleteCandidateFileDS 计算完成: 9,000,000 个文件

[Driver] 00:02:30 - 执行 Dataset.except() (Anti-Join)
[Executor-1] 00:02:31 - Shuffle Read: 处理分区 0-49
[Executor-2] 00:02:31 - Shuffle Read: 处理分区 50-99
[Executor-3] 00:02:31 - Shuffle Read: 处理分区 100-149
...
[Driver] 00:04:00 - expiredFileDS 计算完成: 8,000,000 个文件
[Driver] 00:04:00 - 文件识别完成，耗时: 1m 58s

[Driver] 00:04:00 - 阶段 3: 文件删除 (流式模式)
[Driver] 00:04:00 - 使用批量删除 API: S3FileIO
[Driver] 00:04:01 - 删除批次 1: 100,000 个文件
[Driver] 00:04:05 - 批次 1 完成: 成功 99,850, 失败 150
[Driver] 00:04:05 - 删除批次 2: 100,000 个文件
[Driver] 00:04:10 - 批次 2 完成: 成功 99,900, 失败 100
...
[Driver] 00:10:00 - 所有批次完成
[Driver] 00:10:00 - 文件删除完成，耗时: 6m 00s

[Driver] 00:10:00 - 阶段 4: 返回结果
[Driver] 00:10:00 - 统计结果:
  - 删除数据文件: 7,200,000
  - 删除 Position Delete 文件: 600,000
  - 删除 Equality Delete 文件: 200,000
  - 删除 Manifest 文件: 90,000
  - 删除 Manifest List 文件: 900
  - 删除统计文件: 1,800

[Driver] 00:10:00 - 总耗时: 10m 00s
```

---

## 性能优化建议

### 1. Shuffle 优化

```sql
-- 根据文件数量调整 Shuffle 分区数
SET spark.sql.shuffle.partitions = 2000;  -- 默认 200

-- 大表建议：文件数 / 100000
-- 例如：10,000,000 文件 → 100 个分区
-- 每个分区处理约 100,000 个文件
```

### 2. 内存优化

```sql
-- 流式模式避免 Driver OOM
CALL catalog.system.expire_snapshots(
  table => 'db.table',
  stream_results => true  -- 使用 toLocalIterator()
);

-- 调整 Driver 内存
SET spark.driver.memory = 8g;

-- 调整 Executor 内存
SET spark.executor.memory = 16g;
```

### 3. 并发删除优化

```sql
-- 对于不支持批量删除的 FileIO
CALL catalog.system.expire_snapshots(
  table => 'db.table',
  max_concurrent_deletes => 32  -- 增加并发删除线程
);

-- 对于支持批量删除的 FileIO（S3FileIO）
-- max_concurrent_deletes 参数会被忽略
-- 批量删除性能取决于对象存储的限流
```

### 4. 分阶段执行

```java
// 分两个阶段执行，避免长时间占用 Spark 资源

// 阶段 1: 仅过期快照（快速，秒级）
table.expireSnapshots()
    .expireOlderThan(timestampMillis)
    .cleanExpiredFiles(false)  // 不删除文件
    .commit();

// 阶段 2: 使用 Spark 删除文件（慢速，分钟到小时级）
SparkActions.get()
    .expireSnapshots(table)
    .option(ExpireSnapshotsSparkAction.STREAM_RESULTS, "true")
    .execute();
```

---

## 总结

### Procedure vs SparkAction

**关系：组合关系**

```
ExpireSnapshotsProcedure (SQL 包装层)
    ↓ 调用
ExpireSnapshotsSparkAction (分布式实现层)
    ↓ 调用
RemoveSnapshots (Core 核心逻辑层)
```

### 完整执行流程

1. **阶段 0: 初始化** (Driver)
   - 加载表元数据
   - 验证 GC 启用

2. **阶段 1: 快照过期** (Driver → Core)
   - 调用 `RemoveSnapshots`
   - 计算保留/过期快照
   - 提交元数据更新
   - **关键**: `cleanExpiredFiles(false)` - 不删除文件

3. **阶段 2: 文件识别** (Driver + Executors)
   - 使用 Spark 读取元数据表
   - 通过 `Dataset.except()` 计算过期文件
   - **分布式并行计算**

4. **阶段 3: 文件删除** (Driver)
   - 批量或流式收集文件
   - 使用批量删除或并发删除
   - **在 Driver 节点执行**

5. **阶段 4: 返回结果** (Driver)
   - 构建 Result 对象
   - 返回统计信息

### 核心特点

**Spark 实现的独特优势：**
- ✅ 利用 Spark 分布式计算识别过期文件
- ✅ Dataset anti-join 高效处理大规模文件
- ✅ 支持流式处理避免 Driver OOM
- ✅ 与 Spark SQL 生态无缝集成

**与 Core 的关系：**
- Spark 复用 Core 的快照过期逻辑
- Spark 替换 Core 的文件识别和删除逻辑
- 两者是**组合关系**，而非重复实现

---

**文档版本：** 1.0
**生成日期：** 2025-11-15
**Iceberg 版本：** 1.10.x
