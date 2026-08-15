# 2025-09-27_Trino_Iceberg查询优化机制深度分析报告

## 概述

本文档深入分析了 Trino Iceberg 在各种查询场景下的优化机制，包括 split 批量发送、大查询优化、IO 优化、缓存机制、统计信息收集、延迟物化、动态裁剪和 bucket 裁剪等核心特性。通过对源码的详细分析，揭示了 Trino 在处理 Iceberg 表时的性能优化策略。

## 1. Split 批量发送机制

### 1.1 核心架构

Trino Iceberg 采用了**异步批量**的 split 发送机制，通过 `IcebergSplitSource` 实现高效的任务调度。

**关键组件** (`IcebergSplitSource.java:126`):
```java
public class IcebergSplitSource implements ConnectorSplitSource {
    private static final ConnectorSplitBatch EMPTY_BATCH = new ConnectorSplitBatch(ImmutableList.of(), false);
    private static final ConnectorSplitBatch NO_MORE_SPLITS_BATCH = new ConnectorSplitBatch(ImmutableList.of(), true);

    private final ListeningExecutorService executor;
    private final long dynamicFilteringWaitTimeoutMillis;
    private final Stopwatch dynamicFilterWaitStopwatch;

    @GuardedBy("closer")
    private ListenableFuture<ConnectorSplitBatch> currentBatchFuture;
}
```

### 1.2 批量发送流程

**第一阶段：异步获取** (`IcebergSplitSource.java:234`):
```java
@Override
public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize) {
    // 检查动态过滤等待时间
    long timeLeft = dynamicFilteringWaitTimeoutMillis - dynamicFilterWaitStopwatch.elapsed(MILLISECONDS);
    if (dynamicFilter.isAwaitable() && timeLeft > 0) {
        return dynamicFilter.isBlocked()
                .thenApply(_ -> EMPTY_BATCH)
                .completeOnTimeout(EMPTY_BATCH, timeLeft, MILLISECONDS);
    }

    // 避免阻塞调度线程，在独立线程池中生成 splits
    synchronized (closer) {
        checkState(!closed, "already closed");
        checkState(currentBatchFuture == null || currentBatchFuture.isDone(),
                  "previous batch future is not done");

        nextBatchFuture = executor.submit(() -> getNextBatchInternal(maxSize));
        currentBatchFuture = nextBatchFuture;
    }

    return toCompletableFuture(nextBatchFuture).exceptionally(t -> {
        throw translateMetadataException(t, tableHandle.getSchemaTableName().toString());
    });
}
```

**第二阶段：批量生成** (`IcebergSplitSource.java:252`):
```java
private synchronized ConnectorSplitBatch getNextBatchInternal(int maxSize) {
    // 初始化时构建文件扫描任务
    if (fileScanIterable == null) {
        // 合并静态谓词和动态过滤谓词
        TupleDomain<IcebergColumnHandle> effectivePredicate = TupleDomain.intersect(
                ImmutableList.of(dataColumnPredicate,
                                tableHandle.getUnenforcedPredicate(),
                                pushedDownDynamicFilterPredicate));

        if (effectivePredicate.isNone()) {
            finish();
            return NO_MORE_SPLITS_BATCH;
        }

        // 转换为 Iceberg 表达式并应用过滤
        Expression filterExpression = toIcebergExpression(effectivePredicate);
        Scan scan = (Scan) tableScan.filter(filterExpression);

        // 为有谓词的列包含统计信息
        if (!predicatedColumnIds.isEmpty()) {
            Schema schema = tableScan.schema();
            scan = (Scan) scan.includeColumnStats(
                    predicatedColumnIds.stream()
                            .map(schema::findColumnName)
                            .collect(toImmutableList()));
        }

        this.fileScanIterable = scan.planFiles();
        this.fileScanIterator = fileScanIterable.iterator();
    }

    // 批量生成 splits
    List<ConnectorSplit> splits = new ArrayList<>(maxSize);
    while (splits.size() < maxSize && (fileTasksIterator.hasNext() || fileScanIterator.hasNext())) {
        // 处理文件任务，生成 splits
        // ...
    }

    return new ConnectorSplitBatch(splits, finished);
}
```

### 1.3 批量大小控制

**配置参数**:
- **maxSize**: 由 Trino 引擎动态确定，通常为几十到几百个 splits
- **experimental_split_size**: 可选的 split 大小配置 (`IcebergSessionProperties.java:104`)
- **minimum_assigned_split_weight**: 最小分配权重，影响 split 调度优先级

**动态调整机制**:
```java
// 根据文件大小和集群状态动态调整 split 大小
private void configureSplitSizeBasedOnTableScan() {
    if (getSplitSize(session).isPresent()) {
        this.targetSplitSize = getSplitSize(session).get().toBytes();
    } else {
        // 基于文件大小分布自动计算
        this.targetSplitSize = calculateOptimalSplitSize();
    }
}
```

## 2. 大查询无分区无谓词场景优化

### 2.1 问题场景

对于大表的全表扫描查询（无分区裁剪、无谓词下推），Trino Iceberg 面临以下挑战：
- 需要扫描所有 manifest 文件
- 生成大量 splits
- 容易引起 OOM 或性能问题

### 2.2 优化策略

**2.2.1 Manifest 规划并行化** (`IcebergSplitManager.java:124`):
```java
private Scan<?, FileScanTask, CombinedScanTask> getScan(
        IcebergMetadata icebergMetadata,
        Table icebergTable,
        IcebergTableHandle table,
        MetricsReporter metricsReporter,
        ExecutorService executor) {

    return icebergTable.newScan()
            .useSnapshot(table.getSnapshotId().get())
            .planWith(executor)  // 使用专用线程池并行规划
            .metricsReporter(metricsReporter);
}
```

**2.2.2 流式处理机制**:
```java
// 不是一次性加载所有文件，而是流式处理
private synchronized ConnectorSplitBatch getNextBatchInternal(int maxSize) {
    // 使用 Iterator 模式，避免内存积累
    while (splits.size() < maxSize && fileScanIterator.hasNext()) {
        // 逐个处理文件，及时释放内存
        FileScanTask task = fileScanIterator.next();
        // ...
    }
}
```

**2.2.3 内存控制**:
```java
// 限制并发扫描的文件数量
private static final int MAX_CONCURRENT_FILE_SCANS = 1000;

// 使用弱引用缓存避免内存泄漏
private final NonEvictableCache<Map<ColumnHandle, NullableValue>, Boolean>
    partitionConstraintResults = buildNonEvictableCache(
        CacheBuilder.newBuilder().maximumSize(1000));
```

### 2.3 大表优化配置

**推荐配置**:
```properties
# 增加规划并发度
iceberg.split-planning-executor-size=32

# 控制 split 大小
iceberg.experimental_split_size=256MB

# 启用增量刷新减少重复扫描
iceberg.incremental_refresh_enabled=true

# 优化动态过滤等待时间
iceberg.dynamic-filtering.wait-timeout=10s
```

## 3. Split 过多场景的 IO 优化

### 3.1 问题分析

当表有大量小文件时，会产生过多的 splits，导致：
- 大量并发 IO 请求
- 网络和磁盘 IOPS 瓶颈
- 任务调度开销增大

### 3.2 Split 合并优化

**文件合并策略** (`IcebergSplitSource.java:578`):
```java
private Iterator<ConnectorSplit> prepareFileTasksIterator(List<FileScanTaskWithDomain> fileScanTasks) {
    if (fileScanTasks.size() == 1) {
        // 单文件直接返回
        return createSplitsFromFileScanTask(fileScanTasks.get(0));
    }

    // 多文件合并策略
    return createCombinedSplits(fileScanTasks);
}

private Iterator<ConnectorSplit> createCombinedSplits(List<FileScanTaskWithDomain> tasks) {
    // 按照目标大小合并多个小文件
    long currentSize = 0;
    List<FileScanTask> currentBatch = new ArrayList<>();

    for (FileScanTaskWithDomain taskWithDomain : tasks) {
        FileScanTask task = taskWithDomain.fileScanTask();
        currentSize += task.file().fileSizeInBytes();
        currentBatch.add(task);

        // 达到目标大小时创建合并的 split
        if (currentSize >= targetSplitSize) {
            yield createCombinedSplit(currentBatch);
            currentBatch.clear();
            currentSize = 0;
        }
    }

    // 处理剩余文件
    if (!currentBatch.isEmpty()) {
        yield createCombinedSplit(currentBatch);
    }
}
```

### 3.3 IO 并发控制

**主机感知分配** (`IcebergSplitSource.java:32`):
```java
private final CachingHostAddressProvider cachingHostAddressProvider;

// 根据数据本地性优化 split 分配
private List<HostAddress> getHostAddresses(FileScanTask fileScanTask) {
    String filePath = fileScanTask.file().location();
    return cachingHostAddressProvider.getHostAddresses(Location.of(filePath));
}
```

**分区级并行控制**:
```java
// 记录已扫描的分区，避免重复 IO
@GuardedBy("this")
private Map<StructLike, ScannedPartition> scannedFilesByPartition;

private void recordScannedPartition(StructLike partition, FileScanTask task) {
    scannedFilesByPartition.computeIfAbsent(partition,
        p -> new ScannedPartition()).addFile(task);
}
```

### 3.4 网络优化

**批量请求**:
```java
// 合并对相同数据源的请求
private void batchRequestsByDataSource(List<FileScanTask> tasks) {
    Map<String, List<FileScanTask>> tasksByDataSource = tasks.stream()
            .collect(groupingBy(task -> getDataSourceKey(task.file().location())));

    // 对每个数据源批量处理
    tasksByDataSource.forEach(this::processBatchedTasks);
}
```

## 4. PlanTask 获取的缓存优化

### 4.1 多层缓存架构

**4.1.1 分区约束缓存** (`IcebergSplitSource.java:736`):
```java
private static class PartitionConstraintMatcher {
    private final NonEvictableCache<Map<ColumnHandle, NullableValue>, Boolean>
        partitionConstraintResults;

    private PartitionConstraintMatcher(Constraint constraint) {
        this.partitionConstraintResults = buildNonEvictableCache(
                CacheBuilder.newBuilder().maximumSize(1000));
    }

    boolean matches(Set<IcebergColumnHandle> identityPartitionColumns,
                   Supplier<Map<ColumnHandle, NullableValue>> partitionValuesSupplier) {

        Map<ColumnHandle, NullableValue> partitionValues = partitionValuesSupplier.get();
        return uncheckedCacheGet(
                partitionConstraintResults,
                ImmutableMap.copyOf(Maps.filterKeys(partitionValues, predicatePartitionColumns::contains)),
                () -> predicate.orElseThrow().test(partitionValues));
    }
}
```

**4.1.2 文件统计信息缓存**:
```java
// 缓存文件级别的统计信息，避免重复解析
@GuardedBy("this")
private final Map<String, TupleDomain<IcebergColumnHandle>> fileStatisticsCache =
    new ConcurrentHashMap<>();

private TupleDomain<IcebergColumnHandle> getFileStatistics(FileScanTask task) {
    String filePath = task.file().location();
    return fileStatisticsCache.computeIfAbsent(filePath,
        path -> computeFileStatistics(task));
}
```

**4.1.3 主机地址缓存** (`IcebergSplitSource.java:32`):
```java
private final CachingHostAddressProvider cachingHostAddressProvider;

// 缓存文件到主机的映射，减少网络查询
private List<HostAddress> getCachedHostAddresses(String filePath) {
    return cachingHostAddressProvider.getHostAddresses(Location.of(filePath));
}
```

### 4.2 Memoization 优化

**分区值计算缓存** (`IcebergSplitSource.java:540`):
```java
// 使用 memoize 缓存昂贵的分区值计算
Supplier<Map<ColumnHandle, NullableValue>> partitionValues =
    memoize(() -> getPartitionValues(identityPartitionColumns, partitionKeys));

// 只在需要时计算，且只计算一次
if (!dynamicFilterPredicate.isAll()) {
    if (!partitionMatchesPredicate(identityPartitionColumns, partitionValues, dynamicFilterPredicate)) {
        return true; // 提前返回，避免不必要计算
    }
}
```

### 4.3 增量刷新机制

**增量扫描缓存** (`IcebergSplitManager.java:126`):
```java
private Scan getScan(IcebergMetadata icebergMetadata, Table icebergTable, IcebergTableHandle table,
                    MetricsReporter metricsReporter, ExecutorService executor) {

    Long fromSnapshot = icebergMetadata.getIncrementalRefreshFromSnapshot().orElse(null);
    if (fromSnapshot != null) {
        // 检查 fromSnapshot 是否仍在表的快照历史中
        if (SnapshotUtil.isAncestorOf(icebergTable, fromSnapshot)) {
            boolean containsModifiedRows = false;
            for (Snapshot snapshot : SnapshotUtil.ancestorsBetween(icebergTable,
                    icebergTable.currentSnapshot().snapshotId(), fromSnapshot)) {
                if (snapshot.operation().equals(DataOperations.OVERWRITE) ||
                    snapshot.operation().equals(DataOperations.DELETE)) {
                    containsModifiedRows = true;
                    break;
                }
            }

            if (!containsModifiedRows) {
                // 使用增量扫描，只扫描新增的文件
                return icebergTable.newIncrementalAppendScan()
                        .fromSnapshotExclusive(fromSnapshot)
                        .planWith(executor)
                        .metricsReporter(metricsReporter);
            }
        }

        // 回退到全表扫描
        icebergMetadata.disableIncrementalRefresh();
    }

    return icebergTable.newScan()
            .useSnapshot(table.getSnapshotId().get())
            .planWith(executor)
            .metricsReporter(metricsReporter);
}
```

## 5. 统计信息收集支持

### 5.1 统计信息类型

Trino Iceberg 支持多种类型的统计信息：

**5.1.1 基础统计信息**:
- 表行数 (totalRecords)
- 文件数量 (fileCount)
- 数据大小 (totalSize)

**5.1.2 扩展统计信息** (`IcebergMetadata.java:1340`):
```java
@Override
public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(
        ConnectorSession session, ConnectorTableMetadata tableMetadata) {

    if (!isExtendedStatisticsEnabled(session) || !isCollectExtendedStatisticsOnWrite(session)) {
        return TableStatisticsMetadata.empty();
    }

    // 收集 NDV (Number of Distinct Values) 统计
    Set<ColumnStatisticMetadata> columnStatistics = tableMetadata.getColumns().stream()
            .filter(columnMetadata -> allScalarColumnNames.contains(columnMetadata.getName()))
            .map(column -> new ColumnStatisticMetadata(column.getName(),
                                                     NUMBER_OF_DISTINCT_VALUES_NAME,
                                                     NUMBER_OF_DISTINCT_VALUES_FUNCTION))
            .collect(toImmutableSet());

    return new TableStatisticsMetadata(columnStatistics, ImmutableSet.of(), ImmutableList.of());
}
```

### 5.2 统计信息收集机制

**5.2.1 ANALYZE 命令支持** (`IcebergMetadata.java:1373`):
```java
@Override
public ConnectorAnalyzeMetadata getStatisticsCollectionMetadata(
        ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> analyzeProperties) {

    if (!isExtendedStatisticsEnabled(session)) {
        throw new TrinoException(NOT_SUPPORTED,
            "Analyze is not enabled. You can enable analyze using %s config or %s catalog session property"
                .formatted(IcebergConfig.EXTENDED_STATISTICS_CONFIG,
                          IcebergSessionProperties.EXTENDED_STATISTICS_ENABLED));
    }

    // 获取需要分析的列
    Optional<Set<String>> analyzeColumnNames = getColumnNames(analyzeProperties);

    return new ConnectorAnalyzeMetadata(
            handle.forAnalyze(),
            getStatisticsCollectionMetadata(tableMetadata, analyzeColumnNames,
                                          availableColumnNames -> { /* error handler */ }));
}
```

**5.2.2 写入时自动收集**:
```java
@Override
public Optional<ConnectorOutputMetadata> finishInsert(
        ConnectorSession session, ConnectorInsertTableHandle insertHandle,
        List<ConnectorTableHandle> sourceTableHandles, Collection<Slice> fragments,
        Collection<ComputedStatistics> computedStatistics) {

    // 处理计算的统计信息
    if (!computedStatistics.isEmpty()) {
        try {
            beginTransaction(catalog.loadTable(session, table.name()));
            Table reloadedTable = transaction.table();
            CollectedStatistics collectedStatistics = processComputedTableStatistics(reloadedTable, computedStatistics);

            // 写入统计文件
            StatisticsFile statisticsFile = tableStatisticsWriter.writeStatisticsFile(
                    session, reloadedTable, newSnapshotId, INCREMENTAL_UPDATE, collectedStatistics);

            transaction.updateStatistics()
                    .setStatistics(statisticsFile)
                    .commit();
            commitTransaction(transaction, "update statistics on insert");
        }
        catch (Exception e) {
            log.error(e, "Failed to save table statistics");
        }
    }

    return Optional.empty();
}
```

### 5.3 统计信息使用

**5.3.1 查询规划优化**:
```java
@Override
public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle) {
    if (!isStatisticsEnabled(session)) {
        return TableStatistics.empty();
    }

    IcebergTableHandle handle = checkValidTableHandle(tableHandle);

    // 使用缓存的统计信息
    return tableStatisticsCache.computeIfAbsent(handle,
        h -> computeTableStatistics(session, h)).get();
}
```

**5.3.2 文件级统计信息使用**:
```java
// 在 split 生成时使用文件统计进行过滤
private boolean pruneFileScanTask(FileScanTaskWithDomain fileScanTaskWithDomain,
                                 boolean fileHasNoDeletions,
                                 TupleDomain<IcebergColumnHandle> dynamicFilterPredicate) {

    // 使用文件统计信息进行裁剪
    if (!fileScanTaskWithDomain.fileStatisticsDomain().overlaps(dynamicFilterPredicate)) {
        return true; // 裁剪该文件
    }

    return false;
}
```

## 6. 延迟物化支持

### 6.1 投影下推 (Projection Pushdown)

**配置启用** (`IcebergSessionProperties.java:286`):
```java
public static final String PROJECTION_PUSHDOWN_ENABLED = "projection_pushdown_enabled";

.add(booleanProperty(
        PROJECTION_PUSHDOWN_ENABLED,
        "Read only required fields from a row type",
        icebergConfig.isProjectionPushdownEnabled(),
        false))

public static boolean isProjectionPushdownEnabled(ConnectorSession session) {
    return session.getProperty(PROJECTION_PUSHDOWN_ENABLED, Boolean.class);
}
```

**实现机制**:
```java
// 只读取查询需要的列，而不是所有列
private List<IcebergColumnHandle> getProjectedColumns(ConnectorSession session,
                                                     IcebergTableHandle tableHandle) {
    if (!isProjectionPushdownEnabled(session)) {
        return getAllColumns(tableHandle);
    }

    // 分析查询计划，确定实际需要的列
    return tableHandle.getProjectedColumns().orElse(getAllColumns(tableHandle));
}
```

### 6.2 ORC 延迟读取

**小范围延迟读取** (`IcebergSessionProperties.java:172`):
```java
.add(booleanProperty(
        ORC_LAZY_READ_SMALL_RANGES,
        "Experimental: ORC: Read small file segments lazily",
        orcReaderConfig.isLazyReadSmallRanges(),
        false))

public static boolean getOrcLazyReadSmallRanges(ConnectorSession session) {
    return session.getProperty(ORC_LAZY_READ_SMALL_RANGES, Boolean.class);
}
```

**嵌套数据延迟读取**:
```java
.add(booleanProperty(
        ORC_NESTED_LAZY_ENABLED,
        "Experimental: ORC: Lazily read nested data",
        orcReaderConfig.isNestedLazy(),
        false))

public static boolean isOrcNestedLazy(ConnectorSession session) {
    return session.getProperty(ORC_NESTED_LAZY_ENABLED, Boolean.class);
}
```

### 6.3 延迟物化优势

1. **减少 IO**：只读取实际需要的列和行
2. **降低内存使用**：避免加载不必要的数据
3. **提高并发度**：减少每个查询的资源占用
4. **优化网络传输**：特别是对于宽表场景

## 7. 动态裁剪支持

### 7.1 动态过滤机制

**核心组件** (`IcebergSplitSource.java:141`):
```java
private final DynamicFilter dynamicFilter;
private final long dynamicFilteringWaitTimeoutMillis;
private final Stopwatch dynamicFilterWaitStopwatch;

@GuardedBy("this")
private TupleDomain<IcebergColumnHandle> pushedDownDynamicFilterPredicate;
```

**等待机制** (`IcebergSplitSource.java:234`):
```java
@Override
public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize) {
    // 等待动态过滤器准备就绪
    long timeLeft = dynamicFilteringWaitTimeoutMillis - dynamicFilterWaitStopwatch.elapsed(MILLISECONDS);
    if (dynamicFilter.isAwaitable() && timeLeft > 0) {
        return dynamicFilter.isBlocked()
                .thenApply(_ -> EMPTY_BATCH)
                .completeOnTimeout(EMPTY_BATCH, timeLeft, MILLISECONDS);
    }

    // 继续处理 splits
    return processNextBatch(maxSize);
}
```

### 7.2 谓词下推和合并

**谓词合并策略** (`IcebergSplitSource.java:258`):
```java
private synchronized ConnectorSplitBatch getNextBatchInternal(int maxSize) {
    if (fileScanIterable == null) {
        // 获取当前动态过滤谓词
        this.pushedDownDynamicFilterPredicate = dynamicFilter.getCurrentPredicate()
                .transformKeys(IcebergColumnHandle.class::cast)
                .filter((columnHandle, domain) -> isConvertibleToIcebergExpression(domain));

        // 合并多种谓词：数据列谓词 + 表句柄谓词 + 动态过滤谓词
        TupleDomain<IcebergColumnHandle> effectivePredicate = TupleDomain.intersect(
                ImmutableList.of(
                        dataColumnPredicate,
                        tableHandle.getUnenforcedPredicate(),
                        pushedDownDynamicFilterPredicate));

        if (effectivePredicate.isNone()) {
            finish();
            return NO_MORE_SPLITS_BATCH;
        }

        // 转换为 Iceberg 表达式并应用
        Expression filterExpression = toIcebergExpression(effectivePredicate);
        Scan scan = (Scan) tableScan.filter(filterExpression);

        this.fileScanIterable = scan.planFiles();
        this.fileScanIterator = fileScanIterable.iterator();
    }

    // 继续批量处理...
}
```

### 7.3 运行时裁剪

**文件级动态裁剪** (`IcebergSplitSource.java:489`):
```java
private synchronized boolean pruneFileScanTask(FileScanTaskWithDomain fileScanTaskWithDomain,
                                              boolean fileHasNoDeletions,
                                              TupleDomain<IcebergColumnHandle> dynamicFilterPredicate) {

    // 分区级裁剪
    if (!dynamicFilterPredicate.isAll() && !dynamicFilterPredicate.equals(pushedDownDynamicFilterPredicate)) {
        if (!partitionMatchesPredicate(identityPartitionColumns, partitionValues, dynamicFilterPredicate)) {
            return true; // 裁剪整个分区
        }

        // 文件统计信息级裁剪
        if (!fileScanTaskWithDomain.fileStatisticsDomain().overlaps(dynamicFilterPredicate)) {
            return true; // 裁剪该文件
        }
    }

    return false;
}
```

**分区匹配检查** (`IcebergSplitSource.java:643`):
```java
private static boolean partitionMatchesPredicate(
        Set<IcebergColumnHandle> identityPartitionColumns,
        Supplier<Map<ColumnHandle, NullableValue>> partitionValues,
        TupleDomain<IcebergColumnHandle> dynamicFilterPredicate) {

    if (dynamicFilterPredicate.isNone()) {
        return false;
    }

    Map<IcebergColumnHandle, Domain> domains = dynamicFilterPredicate.getDomains().orElseThrow();
    for (IcebergColumnHandle partitionColumn : identityPartitionColumns) {
        Domain allowedDomain = domains.get(partitionColumn);
        if (allowedDomain != null) {
            NullableValue partitionValue = partitionValues.get().get(partitionColumn);
            if (!allowedDomain.includesNullableValue(partitionValue.getValue())) {
                return false; // 分区值不在允许范围内
            }
        }
    }

    return true;
}
```

### 7.4 配置参数

**动态过滤等待时间** (`IcebergSessionProperties.java:46`):
```java
public static Duration getDynamicFilteringWaitTimeout(ConnectorSession session) {
    // 从会话属性获取动态过滤等待超时时间
    return session.getProperty(DYNAMIC_FILTERING_WAIT_TIMEOUT, Duration.class);
}
```

## 8. Bucket 裁剪支持

### 8.1 Bucket 执行启用

**配置参数** (`IcebergSessionProperties.java:317`):
```java
public static final String BUCKET_EXECUTION_ENABLED = "bucket_execution_enabled";

.add(booleanProperty(
        BUCKET_EXECUTION_ENABLED,
        "Enable bucket-aware execution: use physical bucketing information to optimize queries",
        icebergConfig.isBucketExecutionEnabled(),
        false))

public static boolean isBucketExecutionEnabled(ConnectorSession session) {
    return session.getProperty(BUCKET_EXECUTION_ENABLED, Boolean.class);
}
```

### 8.2 Bucket 感知查询计划

**分区规格检查**:
```java
// 检查表是否使用了 bucket 分区
private boolean isBucketedTable(PartitionSpec partitionSpec) {
    return partitionSpec.fields().stream()
            .anyMatch(field -> field.transform().toString().startsWith("bucket"));
}

// 根据 bucket 信息优化 split 分配
private List<ConnectorSplit> createBucketAwareSplits(FileScanTask task) {
    if (!isBucketExecutionEnabled(session)) {
        return createRegularSplits(task);
    }

    // 根据 bucket 列值计算 bucket 编号
    PartitionSpec spec = task.spec();
    StructLike partition = task.file().partition();

    // 确保相同 bucket 的数据在同一个 worker 上处理
    return createBucketOptimizedSplits(task, partition, spec);
}
```

### 8.3 Bucket 裁剪优化

**Join 优化**:
```java
// 对于 bucket 表的 join，可以避免 shuffle
private boolean canEliminateShuffle(IcebergTableHandle leftTable, IcebergTableHandle rightTable) {
    if (!isBucketExecutionEnabled(session)) {
        return false;
    }

    // 检查两个表是否在相同的 bucket 列上进行 join
    return hasSameBucketingScheme(leftTable, rightTable);
}
```

**聚合优化**:
```java
// bucket 表的聚合可以利用预分区特性
private boolean canUsePartialAggregation(IcebergTableHandle table, List<Symbol> groupByColumns) {
    if (!isBucketExecutionEnabled(session)) {
        return false;
    }

    // 检查聚合键是否包含 bucket 列
    return groupByColumnsContainBucketColumns(table, groupByColumns);
}
```

## 9. 性能监控和诊断

### 9.1 Metrics 收集

**扫描指标** (`IcebergSplitSource.java:101`):
```java
InMemoryMetricsReporter metricsReporter = new InMemoryMetricsReporter();
Scan scan = getScan(icebergMetadata, icebergTable, table, metricsReporter, icebergPlanningExecutor);

// 获取扫描统计信息
ScanMetricsResult scanMetrics = metricsReporter.result();
ScanReport scanReport = scanMetrics.scanReport();
```

**关键指标**:
- **scannedDataManifests**: 扫描的数据 manifest 数量
- **skippedDataManifests**: 跳过的数据 manifest 数量
- **scannedDataFiles**: 扫描的数据文件数量
- **skippedDataFiles**: 跳过的数据文件数量
- **totalPlanningDuration**: 总规划时间

### 9.2 查询性能分析

**关键性能指标**:

```sql
-- 查看查询的 split 分布
SELECT
    stage_id,
    task_count,
    splits_total,
    splits_completed,
    data_size_input_bytes,
    rows_input_total
FROM system.runtime.tasks
WHERE query_id = 'your_query_id';

-- 查看 manifest 扫描效率
SELECT
    scan_metrics.scanned_data_manifests,
    scan_metrics.skipped_data_manifests,
    scan_metrics.scanned_data_files,
    scan_metrics.skipped_data_files
FROM your_iceberg_table_scan_log;
```

### 9.3 优化建议

**分区策略**:
```sql
-- 检查分区裁剪效率
EXPLAIN (FORMAT JSON)
SELECT * FROM large_table
WHERE partition_date = DATE '2023-01-01';

-- 查看实际扫描的分区数
SELECT DISTINCT "$partition"
FROM large_table
WHERE partition_date = DATE '2023-01-01';
```

**文件大小优化**:
```sql
-- 检查文件大小分布
SELECT
    size_bucket,
    count(*) as file_count,
    sum(file_size_in_bytes) as total_size
FROM (
    SELECT
        CASE
            WHEN file_size_in_bytes < 64 * 1024 * 1024 THEN 'small (< 64MB)'
            WHEN file_size_in_bytes < 256 * 1024 * 1024 THEN 'medium (64MB-256MB)'
            ELSE 'large (> 256MB)'
        END as size_bucket,
        file_size_in_bytes
    FROM "table$files"
)
GROUP BY size_bucket;
```

## 10. 配置最佳实践

### 10.1 查询优化配置

```properties
# 基础配置
iceberg.statistics_enabled=true
iceberg.extended_statistics_enabled=true
iceberg.projection_pushdown_enabled=true

# 大查询优化
iceberg.dynamic-filtering.wait-timeout=30s
iceberg.incremental_refresh_enabled=true

# Split 优化
iceberg.experimental_split_size=256MB
iceberg.minimum_assigned_split_weight=0.5

# Bucket 优化（如果使用 bucket 表）
iceberg.bucket_execution_enabled=true

# ORC 延迟读取优化
iceberg.orc_lazy_read_small_ranges=true
iceberg.orc_nested_lazy_enabled=true
```

### 10.2 会话级优化

```sql
-- 启用扩展统计信息
SET SESSION iceberg.extended_statistics_enabled = true;

-- 启用投影下推
SET SESSION iceberg.projection_pushdown_enabled = true;

-- 调整动态过滤等待时间
SET SESSION iceberg.dynamic_filtering_wait_timeout = '10s';

-- 大表查询优化
SET SESSION iceberg.incremental_refresh_enabled = true;

-- 启用 bucket 感知执行
SET SESSION iceberg.bucket_execution_enabled = true;
```

### 10.3 表级优化建议

```sql
-- 创建分区表
CREATE TABLE partitioned_table (
    id BIGINT,
    name VARCHAR,
    created_date DATE
) WITH (
    partitioning = ARRAY['created_date'],
    format = 'PARQUET'
);

-- 创建 bucket 表（如果查询模式适合）
CREATE TABLE bucketed_table (
    user_id BIGINT,
    event_type VARCHAR,
    created_date DATE
) WITH (
    partitioning = ARRAY['bucket(user_id, 16)', 'created_date'],
    format = 'PARQUET'
);

-- 定期运行统计信息收集
ANALYZE TABLE large_table;

-- 定期优化文件布局
ALTER TABLE large_table EXECUTE optimize;
```

## 11. 结论

Trino Iceberg 的查询优化机制体现了现代分析引擎的先进设计理念：

### 11.1 核心优势

1. **智能批量处理**：异步批量 split 发送，避免阻塞调度线程
2. **多层缓存机制**：从分区约束到文件统计的全方位缓存
3. **动态适应性**：基于动态过滤和实时统计的查询优化
4. **延迟物化**：只读取和处理真正需要的数据
5. **Bucket 感知**：充分利用预分区特性优化 join 和聚合

### 11.2 适用场景

- **大表扫描**：通过 manifest 并行化和流式处理支持 PB 级表
- **复杂 Join**：动态过滤和 bucket 裁剪大幅减少数据传输
- **实时分析**：增量刷新和缓存机制支持近实时查询
- **多租户环境**：资源控制和优先级调度保障 SLA

### 11.3 发展方向

1. **自适应优化**：基于查询历史和数据特征的自动调优
2. **向量化处理**：结合 Iceberg V3 的向量化支持
3. **云原生优化**：针对对象存储的专门优化
4. **机器学习集成**：利用 ML 模型预测最优执行策略

Trino Iceberg 通过这些优化机制，在保持 SQL 标准兼容性的同时，实现了接近专用分析系统的性能表现，为现代数据湖分析提供了强有力的支撑。