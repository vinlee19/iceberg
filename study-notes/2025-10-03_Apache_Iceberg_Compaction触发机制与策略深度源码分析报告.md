# 2025-10-03_Apache_Iceberg_Compaction触发机制与策略深度源码分析报告

## 1. 概述

Apache Iceberg的Compaction（压缩/重写）机制是通过数据文件重写（RewriteDataFiles）实现的表优化操作。通过深入分析源码，发现Iceberg的Compaction操作完全是**手动触发**的，不存在自动的后台压缩服务。

## 2. Compaction触发机制分析

### 2.1 触发方式

Iceberg的Compaction操作通过以下几种方式手动触发：

#### 2.1.1 Spark Action API触发
```java
// 位置：org.apache.iceberg.spark.actions.RewriteDataFilesSparkAction
Table table = // 获取表实例
RewriteDataFiles action = SparkActions.get(spark).rewriteDataFiles(table);
RewriteDataFiles.Result result = action.execute();
```

#### 2.1.2 Spark Procedure SQL触发
```sql
-- 位置：org.apache.iceberg.spark.procedures.RewriteDataFilesProcedure
CALL system.rewrite_data_files(
    table => 'catalog.db.table',
    strategy => 'binpack',  -- 或 'sort'
    options => map('target-file-size-bytes', '134217728')
);
```

#### 2.1.3 Flink Action API触发
```java
// 位置：org.apache.iceberg.flink.actions.RewriteDataFilesAction
Table table = // 获取表实例
RewriteDataFiles action = Actions.forTable(table).rewriteDataFiles();
RewriteDataFiles.Result result = action.execute();
```

### 2.2 无自动触发机制

通过源码分析确认：
- **无后台调度服务**：Iceberg核心库中不存在任何后台调度或自动触发Compaction的服务
- **无配置驱动的自动触发**：表属性中没有关于自动Compaction的配置选项
- **无阈值触发机制**：不会基于文件数量、大小或其他指标自动触发

## 3. Compaction策略深度分析

### 3.1 策略分类

Iceberg提供三种主要的Compaction策略，每种策略都有不同的优化目标：

#### 3.1.1 BinPack策略（默认）
**源码位置**：`org.apache.iceberg.spark.actions.SparkBinPackDataRewriter`

**核心机制**：
```java
// SparkBinPackDataRewriter.java:43
protected void doRewrite(String groupId, List<FileScanTask> group) {
    // 读取文件并将其打包成所需大小的分片
    Dataset<Row> scanDF = spark()
        .read()
        .format("iceberg")
        .option(SparkReadOptions.SCAN_TASK_SET_ID, groupId)
        .option(SparkReadOptions.SPLIT_SIZE, splitSize(inputSize(group)))
        .option(SparkReadOptions.FILE_OPEN_COST, "0")
        .load(groupId);

    // 将打包的数据写入新文件，每个分片成为一个新文件
    scanDF.write()
        .format("iceberg")
        .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, writeMaxFileSize())
        .mode("append")
        .save(groupId);
}
```

**触发条件**：
- 基于文件大小的阈值判断（`SizeBasedFileRewritePlanner`）
- 小于`MIN_FILE_SIZE_BYTES`（默认为目标文件大小的75%）的文件
- 大于`MAX_FILE_SIZE_BYTES`（默认为目标文件大小的180%）的文件
- 删除文件数量超过`DELETE_FILE_THRESHOLD`的文件
- 删除比例超过`DELETE_RATIO_THRESHOLD`（默认30%）的文件

#### 3.1.2 Sort策略
**源码位置**：`org.apache.iceberg.spark.actions.SparkSortDataRewriter`

**核心机制**：
```java
// SparkSortDataRewriter.java:34
SparkSortDataRewriter(SparkSession spark, Table table) {
    super(spark, table);
    Preconditions.checkArgument(
        table.sortOrder().isSorted(),
        "Cannot sort data without a valid sort order");
    this.sortOrder = table.sortOrder();
}
```

**特点**：
- 基于表的排序顺序或用户指定的排序顺序重写数据
- 改善查询性能，特别是范围查询和排序查询
- 需要额外的shuffle操作

#### 3.1.3 Z-Order策略
**源码位置**：`org.apache.iceberg.spark.actions.SparkZOrderDataRewriter`

**核心机制**：
```java
// SparkZOrderDataRewriter.java:128
protected Dataset<Row> sortedDF(Dataset<Row> df, Function<Dataset<Row>, Dataset<Row>> sortFunc) {
    Dataset<Row> zValueDF = df.withColumn(Z_COLUMN, zValue(df));
    Dataset<Row> sortedDF = sortFunc.apply(zValueDF);
    return sortedDF.drop(Z_COLUMN);
}

private Column zValue(Dataset<Row> df) {
    SparkZOrderUDF zOrderUDF = new SparkZOrderUDF(zOrderColNames.size(), varLengthContribution, maxOutputSize);

    Column[] zOrderCols = zOrderColNames.stream()
        .map(df.schema()::apply)
        .map(col -> zOrderUDF.sortedLexicographically(df.col(col.name()), col.dataType()))
        .toArray(Column[]::new);

    return zOrderUDF.interleaveBytes(array(zOrderCols));
}
```

**特点**：
- 使用Z-Order曲线对多维数据进行空间填充排序
- 优化多列条件查询性能
- 适用于高维数据分析场景

### 3.2 Compaction类型分析

基于源码分析，Iceberg的"Compaction"类型划分并非传统数据库的minor/major/full概念，而是基于文件重写的粒度：

#### 3.2.1 文件级别重写（File-level Rewrite）
**对应传统概念**：Minor Compaction

**源码位置**：`BinPackRewriteFilePlanner.planFileGroups()`

**触发条件**：
```java
// BinPackRewriteFilePlanner.java:171
protected Iterable<FileScanTask> filterFiles(Iterable<FileScanTask> tasks) {
    return Iterables.filter(tasks, task ->
        outsideDesiredFileSizeRange(task) ||
        tooManyDeletes(task) ||
        tooHighDeleteRatio(task));
}

// BinPackRewriteFilePlanner.java:234
private boolean tooHighDeleteRatio(FileScanTask task) {
    if (task.deletes() == null || task.deletes().isEmpty()) {
        return false;
    }

    long knownDeletedRecordCount = task.deletes().stream()
        .filter(ContentFileUtil::isFileScoped)
        .mapToLong(ContentFile::recordCount)
        .sum();

    double deletedRecords = (double) Math.min(knownDeletedRecordCount, task.file().recordCount());
    double deleteRatio = deletedRecords / task.file().recordCount();
    return deleteRatio >= deleteRatioThreshold;  // 默认0.3
}
```

**特征**：
- 处理单个分区内的文件优化
- 主要解决小文件问题和删除文件碎片化
- 影响范围有限，提交冲突概率低

#### 3.2.2 分区级别重写（Partition-level Rewrite）
**对应传统概念**：Major Compaction

**源码位置**：`SizeBasedFileRewritePlanner.planFileGroups()`

**实现机制**：
```java
// SizeBasedFileRewritePlanner.java:169
protected Iterable<List<T>> planFileGroups(Iterable<T> tasks) {
    Iterable<T> filteredTasks = rewriteAll ? tasks : filterFiles(tasks);
    BinPacking.ListPacker<T> packer = new BinPacking.ListPacker<>(maxGroupSize, 1, false);
    List<List<T>> groups = packer.pack(filteredTasks, ContentScanTask::length);
    return rewriteAll ? groups : filterFileGroups(groups);
}

protected boolean enoughInputFiles(List<T> group) {
    return group.size() > 1 && group.size() >= minInputFiles;  // 默认5
}

protected boolean enoughContent(List<T> group) {
    return group.size() > 1 && inputSize(group) > targetFileSize;
}
```

**特征**：
- 处理整个分区的文件重组
- 基于BinPacking算法优化文件分布
- 可能涉及大量文件的重写

#### 3.2.3 表级别重写（Table-level Rewrite）
**对应传统概念**：Full Compaction

**实现机制**：
```java
// 通过设置 REWRITE_ALL = true 实现
public static final String REWRITE_ALL = "rewrite-all";
public static final boolean REWRITE_ALL_DEFAULT = false;

// SizeBasedFileRewritePlanner.java:156
if (rewriteAll) {
    LOG.info("Configured to rewrite all provided files in table {}", table.name());
}
```

**特征**：
- 重写表中的所有数据文件
- 通常用于表结构变更后的优化
- 资源消耗最大，但优化效果最显著

## 4. 核心执行流程分析

### 4.1 执行入口
**源码位置**：`org.apache.iceberg.spark.actions.RewriteDataFilesSparkAction.execute()`

```java
// RewriteDataFilesSparkAction.java:160
public RewriteDataFiles.Result execute() {
    if (table.currentSnapshot() == null) {
        return EMPTY_RESULT;
    }

    long startingSnapshotId = table.currentSnapshot().snapshotId();

    // 默认使用BinPack策略
    if (this.rewriter == null) {
        this.rewriter = new SparkBinPackDataRewriter(spark(), table);
    }

    // 验证和初始化选项
    validateAndInitOptions();

    // 规划文件组
    StructLikeMap<List<List<FileScanTask>>> fileGroupsByPartition = planFileGroups(startingSnapshotId);
    RewriteExecutionContext ctx = new RewriteExecutionContext(fileGroupsByPartition);

    if (ctx.totalGroupCount() == 0) {
        LOG.info("Nothing found to rewrite in {}", table.name());
        return EMPTY_RESULT;
    }

    // 执行重写
    Stream<RewriteFileGroup> groupStream = toGroupStream(ctx, fileGroupsByPartition);
    Builder resultBuilder = partialProgressEnabled
        ? doExecuteWithPartialProgress(ctx, groupStream, commitManager(startingSnapshotId))
        : doExecute(ctx, groupStream, commitManager(startingSnapshotId));

    return resultBuilder.build();
}
```

### 4.2 文件分组规划
**源码位置**：`BinPackRewriteFilePlanner.planFileGroups()`

```java
// BinPackRewriteFilePlanner.java:250
private StructLikeMap<List<List<FileScanTask>>> planFileGroups() {
    TableScan scan = table().newScan()
        .filter(filter)
        .caseSensitive(caseSensitive)
        .ignoreResiduals();

    if (snapshotId != null) {
        scan = scan.useSnapshot(snapshotId);
    }

    CloseableIterable<FileScanTask> fileScanTasks = scan.planFiles();

    try {
        Types.StructType partitionType = table().spec().partitionType();
        StructLikeMap<List<FileScanTask>> filesByPartition = groupByPartition(table(), partitionType, fileScanTasks);
        return filesByPartition.transformValues(tasks -> ImmutableList.copyOf(planFileGroups(tasks)));
    } finally {
        try {
            fileScanTasks.close();
        } catch (IOException io) {
            LOG.error("Cannot properly close file iterable while planning for rewrite", io);
        }
    }
}
```

### 4.3 并发执行控制
**源码位置**：`RewriteDataFilesSparkAction.doExecute()`

```java
// RewriteDataFilesSparkAction.java:282
private Builder doExecute(RewriteExecutionContext ctx, Stream<RewriteFileGroup> groupStream, RewriteDataFilesCommitManager commitManager) {
    ExecutorService rewriteService = rewriteService();
    ConcurrentLinkedQueue<RewriteFileGroup> rewrittenGroups = Queues.newConcurrentLinkedQueue();

    Tasks.Builder<RewriteFileGroup> rewriteTaskBuilder = Tasks.foreach(groupStream)
        .executeWith(rewriteService)
        .stopOnFailure()
        .noRetry()
        .onFailure((fileGroup, exception) -> {
            LOG.warn("Failure during rewrite process for group {}", fileGroup.info(), exception);
        });

    try {
        rewriteTaskBuilder.run(fileGroup -> {
            rewrittenGroups.add(rewriteFiles(ctx, fileGroup));
        });
    } catch (Exception e) {
        // 清理已完成的重写
        Tasks.foreach(rewrittenGroups)
            .suppressFailureWhenFinished()
            .run(commitManager::abortFileGroup);
        throw e;
    } finally {
        rewriteService.shutdown();
    }

    try {
        commitManager.commitOrClean(Sets.newHashSet(rewrittenGroups));
    } catch (ValidationException | CommitFailedException e) {
        String errorMessage = String.format("Cannot commit rewrite because of a ValidationException or CommitFailedException...");
        throw new RuntimeException(errorMessage, e);
    }

    List<FileGroupRewriteResult> rewriteResults = rewrittenGroups.stream()
        .map(RewriteFileGroup::asResult)
        .collect(Collectors.toList());
    return ImmutableRewriteDataFiles.Result.builder().rewriteResults(rewriteResults);
}
```

## 5. 关键配置参数分析

### 5.1 文件大小控制参数
```java
// RewriteDataFiles.java
// 目标文件大小
String TARGET_FILE_SIZE_BYTES = "target-file-size-bytes";

// 最大文件组大小（默认100GB）
String MAX_FILE_GROUP_SIZE_BYTES = "max-file-group-size-bytes";
long MAX_FILE_GROUP_SIZE_BYTES_DEFAULT = 1024L * 1024L * 1024L * 100L;

// SizeBasedFileRewritePlanner.java
// 最小文件大小阈值（默认为目标大小的75%）
String MIN_FILE_SIZE_BYTES = "min-file-size-bytes";
double MIN_FILE_SIZE_DEFAULT_RATIO = 0.75;

// 最大文件大小阈值（默认为目标大小的180%）
String MAX_FILE_SIZE_BYTES = "max-file-size-bytes";
double MAX_FILE_SIZE_DEFAULT_RATIO = 1.80;
```

### 5.2 并发控制参数
```java
// 最大并发文件组重写数量（默认5）
String MAX_CONCURRENT_FILE_GROUP_REWRITES = "max-concurrent-file-group-rewrites";
int MAX_CONCURRENT_FILE_GROUP_REWRITES_DEFAULT = 5;

// 最小输入文件数量阈值（默认5）
String MIN_INPUT_FILES = "min-input-files";
int MIN_INPUT_FILES_DEFAULT = 5;
```

### 5.3 删除文件优化参数
```java
// BinPackRewriteFilePlanner.java
// 删除文件数量阈值（默认Integer.MAX_VALUE，即禁用）
String DELETE_FILE_THRESHOLD = "delete-file-threshold";
int DELETE_FILE_THRESHOLD_DEFAULT = Integer.MAX_VALUE;

// 删除比例阈值（默认30%）
String DELETE_RATIO_THRESHOLD = "delete-ratio-threshold";
double DELETE_RATIO_THRESHOLD_DEFAULT = 0.3;
```

### 5.4 提交控制参数
```java
// 部分进度提交开关（默认false）
String PARTIAL_PROGRESS_ENABLED = "partial-progress.enabled";
boolean PARTIAL_PROGRESS_ENABLED_DEFAULT = false;

// 最大提交数量（默认10）
String PARTIAL_PROGRESS_MAX_COMMITS = "partial-progress.max-commits";
int PARTIAL_PROGRESS_MAX_COMMITS_DEFAULT = 10;
```

## 6. 使用示例与最佳实践

### 6.1 Spark API使用示例

#### 6.1.1 基础BinPack压缩
```java
Table table = catalog.loadTable(TableIdentifier.of("database", "table"));
RewriteDataFiles.Result result = SparkActions.get(spark)
    .rewriteDataFiles(table)
    .binPack()
    .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, "134217728") // 128MB
    .option(RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_REWRITES, "10")
    .execute();

System.out.println("重写文件数: " + result.rewrittenDataFilesCount());
System.out.println("新增文件数: " + result.addedDataFilesCount());
System.out.println("重写字节数: " + result.rewrittenBytesCount());
```

#### 6.1.2 按删除比例触发的压缩
```java
RewriteDataFiles.Result result = SparkActions.get(spark)
    .rewriteDataFiles(table)
    .binPack()
    .option("delete-ratio-threshold", "0.2")  // 删除比例超过20%时触发
    .option("delete-file-threshold", "5")     // 删除文件数超过5个时触发
    .execute();
```

#### 6.1.3 Sort策略压缩
```java
SortOrder sortOrder = SortOrder.builderFor(table.schema())
    .sortBy("date", SortDirection.ASC)
    .sortBy("category", SortDirection.ASC)
    .build();

RewriteDataFiles.Result result = SparkActions.get(spark)
    .rewriteDataFiles(table)
    .sort(sortOrder)
    .filter(Expressions.equal("date", "2024-01-01"))  // 只压缩特定分区
    .execute();
```

#### 6.1.4 Z-Order策略压缩
```java
RewriteDataFiles.Result result = SparkActions.get(spark)
    .rewriteDataFiles(table)
    .zOrder("user_id", "timestamp", "region")
    .option("max-output-size", "1048576")  // 1MB Z-order输出大小
    .execute();
```

### 6.2 Spark SQL Procedure使用示例

#### 6.2.1 基础压缩
```sql
CALL system.rewrite_data_files(
    table => 'catalog.database.table',
    strategy => 'binpack',
    options => map(
        'target-file-size-bytes', '134217728',
        'max-concurrent-file-group-rewrites', '8'
    )
);
```

#### 6.2.2 条件过滤压缩
```sql
CALL system.rewrite_data_files(
    table => 'catalog.database.table',
    strategy => 'sort',
    sort_order => 'date ASC, category ASC',
    where => 'date >= ''2024-01-01'' AND date < ''2024-02-01'''
);
```

#### 6.2.3 Z-Order压缩
```sql
CALL system.rewrite_data_files(
    table => 'catalog.database.table',
    strategy => 'sort',
    sort_order => 'zorder(user_id, timestamp, region)'
);
```

### 6.3 Flink API使用示例
```java
Table table = catalog.getTable(new ObjectPath("database", "table"));
RewriteDataFiles.Result result = Actions.forTable(table)
    .rewriteDataFiles()
    .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, "268435456") // 256MB
    .execute();
```

## 7. 性能优化与监控

### 7.1 性能调优参数

#### 7.1.1 并发控制优化
```java
// 根据集群资源调整并发度
.option(RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_REWRITES, "16")

// 调整文件组大小以平衡内存使用和并行度
.option(RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, "53687091200") // 50GB
```

#### 7.1.2 部分提交优化（用于大表）
```java
.option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, "true")
.option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS, "20")
.option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_FAILED_COMMITS, "5")
```

### 7.2 监控指标
- **重写文件数量**：`result.rewrittenDataFilesCount()`
- **新增文件数量**：`result.addedDataFilesCount()`
- **重写数据量**：`result.rewrittenBytesCount()`
- **失败文件数量**：`result.failedDataFilesCount()`

## 8. 结论

### 8.1 核心发现

1. **完全手动触发**：Iceberg的Compaction机制完全依赖手动触发，无自动后台服务
2. **策略多样化**：提供BinPack、Sort、Z-Order三种策略，分别适用于不同场景
3. **灵活的粒度控制**：支持文件级、分区级、表级的重写粒度
4. **丰富的配置选项**：通过多种参数精确控制压缩行为
5. **强一致性保证**：通过快照隔离和事务提交保证数据一致性

### 8.2 最佳实践建议

1. **定期执行**：由于无自动触发，建议建立定期执行计划（如每日/每周）
2. **监控驱动**：基于文件数量、大小分布、删除比例等指标决定压缩时机
3. **策略选择**：
   - 小文件问题：使用BinPack策略
   - 查询性能优化：使用Sort策略
   - 多维查询优化：使用Z-Order策略
4. **资源控制**：合理设置并发度和文件组大小，避免集群资源过载
5. **分批处理**：对于大表，启用部分提交机制，降低单次操作风险

### 8.3 与传统数据库的差异

传统数据库（如HBase、Cassandra）通常具有自动的Minor/Major Compaction机制，而Iceberg采用完全手动的方式，这体现了湖仓一体架构的设计理念：
- **用户可控**：用户完全控制压缩时机和策略
- **资源可预测**：避免意外的后台压缩影响查询性能
- **成本可控**：在云环境中精确控制计算成本
- **灵活性更高**：支持多种压缩策略和细粒度控制

这种设计使得Iceberg在大数据场景下具有更好的可控性和可预测性，特别适合云原生环境的成本和性能优化需求。