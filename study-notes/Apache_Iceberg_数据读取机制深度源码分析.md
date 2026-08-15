# Apache Iceberg 数据读取机制深度源码分析

## 概述

Apache Iceberg是一个开源的表格式，为大型分析数据集提供可靠、高性能的数据湖解决方案。本文档深入分析了Iceberg的数据读取机制，从源码层面详细剖析了从metadata、snapshot、manifest list、manifest到data file的完整读取流程，以及删除文件处理、增量读取、时间旅行等高级特性的实现原理。

## 1. 数据读取总体架构

### 1.1 读取流程架构图

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Iceberg Data Reading Architecture                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  User Query                                                         │
│       │                                                             │
│       ▼                                                             │
│  ┌─────────────────┐                                               │
│  │   TableScan     │  ◄─── Schema, Filter, Projection              │
│  │                 │                                               │
│  │ - DataTableScan │                                               │
│  │ - IncrementalScan│                                               │
│  │ - MetadataScan  │                                               │
│  └─────────────────┘                                               │
│           │                                                         │
│           ▼                                                         │
│  ┌─────────────────┐       ┌─────────────────┐                    │
│  │  Table Metadata │────── │   Snapshot      │                    │
│  │                 │       │                 │                    │
│  │ - Current Schema│       │ - SnapshotID    │                    │
│  │ - Partition Spec│       │ - Timestamp     │                    │
│  │ - Sort Order    │       │ - Manifest List │                    │
│  │ - Properties    │       │ - Summary       │                    │
│  └─────────────────┘       └─────────────────┘                    │
│                                       │                             │
│                                       ▼                             │
│                             ┌─────────────────┐                    │
│                             │  Manifest List  │                    │
│                             │                 │                    │
│                             │ - Data Manifests│                    │
│                             │ - Delete        │                    │
│                             │   Manifests     │                    │
│                             └─────────────────┘                    │
│                                       │                             │
│                                       ▼                             │
│                             ┌─────────────────┐                    │
│                             │ ManifestGroup   │                    │
│                             │                 │                    │
│                             │ - Filter        │                    │
│                             │ - Projection    │                    │
│                             │ - DeleteIndex   │                    │
│                             └─────────────────┘                    │
│                                       │                             │
│                                       ▼                             │
│  ┌─────────────────┐       ┌─────────────────┐                    │
│  │  Manifest Files │       │  Delete Index   │                    │
│  │                 │       │                 │                    │
│  │ - Data Entries  │       │ - Equality      │                    │
│  │ - Statistics    │       │ - Position      │                    │
│  │ - Partition Info│       │ - Deletion Vec  │                    │
│  └─────────────────┘       └─────────────────┘                    │
│           │                           │                             │
│           ▼                           │                             │
│  ┌─────────────────┐                 │                             │
│  │ FileScanTask    │ ◄───────────────┘                             │
│  │                 │                                               │
│  │ - DataFile      │                                               │
│  │ - DeleteFiles   │                                               │
│  │ - Residuals     │                                               │
│  │ - Schema        │                                               │
│  └─────────────────┘                                               │
│           │                                                         │
│           ▼                                                         │
│  ┌─────────────────┐                                               │
│  │  File Readers   │                                               │
│  │                 │                                               │
│  │ - ParquetReader │                                               │
│  │ - ORCReader     │                                               │
│  │ - AvroReader    │                                               │
│  └─────────────────┘                                               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.2 核心类继承关系

```java
// 扫描接口继承关系
TableScan (Interface)
    ├── Scan (Interface) 
    │   └── BaseScan (Abstract)
    │       └── SnapshotScan (Abstract)
    │           └── BaseTableScan (Abstract)
    │               ├── DataTableScan
    │               └── IncrementalDataTableScan
    │
    └── BatchScan (Interface)
        └── BaseAllMetadataTableScan (Abstract)
```

## 2. 从Metadata到Snapshot的读取流程

### 2.1 Table实例化和Metadata读取

```java
// BaseTable.java 关键实现
public abstract class BaseTable implements Table {
    private final TableOperations ops;
    private final String name;
    private volatile TableMetadata current = null;
    
    @Override
    public TableScan newScan() {
        return new DataTableScan(this, schema(), context());
    }
    
    @Override
    public Snapshot currentSnapshot() {
        return current().currentSnapshot();
    }
    
    private TableMetadata current() {
        if (current == null) {
            synchronized (this) {
                if (current == null) {
                    // 延迟加载表元数据
                    this.current = ops.current();
                }
            }
        }
        return current;
    }
}
```

### 2.2 Snapshot解析和Manifest List读取

```java
// BaseSnapshot.java 核心流程
class BaseSnapshot implements Snapshot {
    private final long snapshotId;
    private final String manifestListLocation;
    private transient List<ManifestFile> allManifests = null;
    
    private void cacheManifests(FileIO fileIO) {
        if (allManifests == null) {
            if (manifestListLocation != null) {
                // 读取 Manifest List 文件
                this.allManifests = ManifestLists.read(
                    fileIO.newInputFile(manifestListLocation)
                );
            }
        }
        
        if (dataManifests == null || deleteManifests == null) {
            // 分离数据和删除manifest
            this.dataManifests = ImmutableList.copyOf(
                Iterables.filter(allManifests, 
                    manifest -> manifest.content() == ManifestContent.DATA)
            );
            this.deleteManifests = ImmutableList.copyOf(
                Iterables.filter(allManifests, 
                    manifest -> manifest.content() == ManifestContent.DELETES)
            );
        }
    }
}
```

### 2.3 Manifest List文件解析

```java
// ManifestLists.java 读取实现
class ManifestLists {
    static List<ManifestFile> read(InputFile manifestList) {
        try (CloseableIterable<ManifestFile> files =
            InternalData.read(FileFormat.AVRO, manifestList)
                .setRootType(GenericManifestFile.class)
                .setCustomType(ManifestFile.PARTITION_SUMMARIES_ELEMENT_ID, 
                              GenericPartitionFieldSummary.class)
                .project(ManifestFile.schema())
                .build()) {
            
            return Lists.newLinkedList(files);
        }
    }
}
```

## 3. ManifestGroup和文件扫描规划

### 3.1 ManifestGroup初始化

```java
// DataTableScan.java 扫描规划
@Override
public CloseableIterable<FileScanTask> doPlanFiles() {
    Snapshot snapshot = snapshot();
    
    FileIO io = table().io();
    List<ManifestFile> dataManifests = snapshot.dataManifests(io);
    List<ManifestFile> deleteManifests = snapshot.deleteManifests(io);
    
    // 创建ManifestGroup来统一管理数据和删除manifest
    ManifestGroup manifestGroup = new ManifestGroup(io, dataManifests, deleteManifests)
        .caseSensitive(isCaseSensitive())
        .select(scanColumns())
        .filterData(filter())
        .specsById(table().specs())
        .scanMetrics(scanMetrics())
        .ignoreDeleted()
        .columnsToKeepStats(columnsToKeepStats());
    
    // 配置是否忽略残留条件
    if (shouldIgnoreResiduals()) {
        manifestGroup = manifestGroup.ignoreResiduals();
    }
    
    // 并行规划支持
    if (shouldPlanWithExecutor() && 
        (dataManifests.size() > 1 || deleteManifests.size() > 1)) {
        manifestGroup = manifestGroup.planWith(planExecutor());
    }
    
    return manifestGroup.planFiles();
}
```

### 3.2 ManifestGroup文件规划

```java
// ManifestGroup.java 核心规划逻辑
public CloseableIterable<FileScanTask> planFiles() {
    return plan(ManifestGroup::createFileScanTasks);
}

public <T extends ScanTask> CloseableIterable<T> plan(CreateTasksFunction<T> createTasksFunc) {
    // 创建残留条件评估器缓存
    LoadingCache<Integer, ResidualEvaluator> residualCache = 
        Caffeine.newBuilder().build(specId -> {
            PartitionSpec spec = specsById.get(specId);
            Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : dataFilter;
            return ResidualEvaluator.of(spec, filter, caseSensitive);
        });
    
    // 构建删除文件索引
    DeleteFileIndex deleteFiles = deleteIndexBuilder.scanMetrics(scanMetrics).build();
    
    // 检查是否需要统计信息列
    boolean dropStats = ManifestReader.dropStats(columns);
    if (deleteFiles.hasEqualityDeletes()) {
        select(ManifestReader.withStatsColumns(columns));
    }
    
    // 创建任务上下文缓存
    LoadingCache<Integer, TaskContext> taskContextCache = 
        Caffeine.newBuilder().build(specId -> {
            PartitionSpec spec = specsById.get(specId);
            ResidualEvaluator residuals = residualCache.get(specId);
            return new TaskContext(spec, deleteFiles, residuals, 
                                 dropStats, columnsToKeepStats, scanMetrics);
        });
    
    // 生成扫描任务
    Iterable<CloseableIterable<T>> tasks = entries(
        (manifest, entries) -> {
            int specId = manifest.partitionSpecId();
            TaskContext taskContext = taskContextCache.get(specId);
            return createTasksFunc.apply(entries, taskContext);
        }
    );
    
    // 并行或串行执行
    if (executorService != null) {
        return new ParallelIterable<>(tasks, executorService);
    } else {
        return CloseableIterable.concat(tasks);
    }
}
```

## 4. Manifest文件读取和过滤详解

### 4.1 ManifestReader核心流程

```java
// ManifestGroup.java 中的entries处理
private <T> Iterable<CloseableIterable<T>> entries(
    BiFunction<ManifestFile, CloseableIterable<ManifestEntry<DataFile>>, 
               CloseableIterable<T>> entryFn) {
    
    // 创建Manifest评估器缓存
    LoadingCache<Integer, ManifestEvaluator> evalCache = 
        specsById == null ? null : 
        Caffeine.newBuilder().build(specId -> {
            PartitionSpec spec = specsById.get(specId);
            return ManifestEvaluator.forPartitionFilter(
                Expressions.and(
                    partitionFilter,
                    Projections.inclusive(spec, caseSensitive).project(dataFilter)
                ),
                spec, caseSensitive
            );
        });
    
    // 文件级别过滤器
    Evaluator evaluator = null;
    if (fileFilter != null && fileFilter != Expressions.alwaysTrue()) {
        evaluator = new Evaluator(DataFile.getType(EMPTY_STRUCT), 
                                fileFilter, caseSensitive);
    }
    
    // Manifest文件过滤
    CloseableIterable<ManifestFile> matchingManifests = 
        evalCache == null ? closeableDataManifests :
        CloseableIterable.filter(
            scanMetrics.skippedDataManifests(),
            closeableDataManifests,
            manifest -> evalCache.get(manifest.partitionSpecId()).eval(manifest)
        );
    
    // 忽略已删除文件的Manifest过滤
    if (ignoreDeleted) {
        matchingManifests = CloseableIterable.filter(
            scanMetrics.skippedDataManifests(),
            matchingManifests,
            manifest -> manifest.hasAddedFiles() || manifest.hasExistingFiles()
        );
    }
    
    return Iterables.transform(matchingManifests, manifest -> 
        new CloseableIterable<T>() {
            @Override
            public CloseableIterator<T> iterator() {
                // 创建ManifestReader
                ManifestReader<DataFile> reader = 
                    ManifestFiles.read(manifest, io, specsById)
                        .filterRows(dataFilter)
                        .filterPartitions(partitionFilter)
                        .caseSensitive(caseSensitive)
                        .select(columns)
                        .scanMetrics(scanMetrics);
                
                // 获取条目
                CloseableIterable<ManifestEntry<DataFile>> entries = 
                    ignoreDeleted ? reader.liveEntries() : reader.entries();
                
                // 应用文件级别过滤
                if (evaluator != null) {
                    entries = CloseableIterable.filter(
                        scanMetrics.skippedDataFiles(),
                        entries,
                        entry -> evaluator.eval((GenericDataFile) entry.file())
                    );
                }
                
                // 应用自定义过滤条件
                entries = CloseableIterable.filter(
                    scanMetrics.skippedDataFiles(), 
                    entries, 
                    manifestEntryPredicate
                );
                
                CloseableIterable<T> iterable = entryFn.apply(manifest, entries);
                return iterable.iterator();
            }
        }
    );
}
```

### 4.2 ManifestReader内部实现

```java
// ManifestReader.java 条目读取
private CloseableIterable<ManifestEntry<F>> entries(boolean onlyLive) {
    if (hasRowFilter() || hasPartitionFilter() || partitionSet != null) {
        Evaluator evaluator = evaluator();
        InclusiveMetricsEvaluator metricsEvaluator = metricsEvaluator();
        
        // 确保统计列存在以进行度量评估
        boolean requireStatsProjection = requireStatsProjection(rowFilter, columns);
        Collection<String> projectColumns = 
            requireStatsProjection ? withStatsColumns(columns) : columns;
        
        CloseableIterable<ManifestEntry<F>> entries = 
            open(projection(fileSchema, fileProjection, projectColumns, caseSensitive));
        
        return CloseableIterable.filter(
            content == FileType.DATA_FILES ? 
                scanMetrics.skippedDataFiles() : scanMetrics.skippedDeleteFiles(),
            onlyLive ? filterLiveEntries(entries) : entries,
            entry -> entry != null && 
                    evaluator.eval(entry.file().partition()) &&
                    metricsEvaluator.eval(entry.file()) &&
                    inPartitionSet(entry.file())
        );
    } else {
        CloseableIterable<ManifestEntry<F>> entries = 
            open(projection(fileSchema, fileProjection, columns, caseSensitive));
        return onlyLive ? filterLiveEntries(entries) : entries;
    }
}

private CloseableIterable<ManifestEntry<F>> open(Schema projection) {
    FileFormat format = FileFormat.fromFileName(file.location());
    
    // 构建投影schema，确保包含必要字段
    List<Types.NestedField> fields = Lists.newArrayList();
    fields.addAll(projection.asStruct().fields());
    if (projection.findField(DataFile.RECORD_COUNT.fieldId()) == null) {
        fields.add(DataFile.RECORD_COUNT);
    }
    if (projection.findField(DataFile.FIRST_ROW_ID.fieldId()) == null) {
        fields.add(DataFile.FIRST_ROW_ID);
    }
    fields.add(MetadataColumns.ROW_POSITION);
    
    // 创建Avro读取器
    CloseableIterable<ManifestEntry<F>> reader = 
        InternalData.read(format, file)
            .project(ManifestEntry.wrapFileSchema(Types.StructType.of(fields)))
            .setRootType(GenericManifestEntry.class)
            .setCustomType(ManifestEntry.DATA_FILE_ID, content.fileClass())
            .setCustomType(DataFile.PARTITION_ID, PartitionData.class)
            .reuseContainers()
            .build();
    
    addCloseable(reader);
    
    // 应用可继承元数据和行ID分配
    CloseableIterable<ManifestEntry<F>> withMetadata = 
        CloseableIterable.transform(reader, inheritableMetadata::apply);
    return CloseableIterable.transform(withMetadata, idAssigner(firstRowId));
}
```

## 5. 删除文件处理和数据过滤

### 5.1 DeleteFileIndex构建

```java
// DeleteFileIndex.java 构建器
public static Builder builderFor(FileIO io, Iterable<ManifestFile> deleteManifests) {
    return new Builder(io, deleteManifests);
}

public static class Builder {
    private final FileIO io;
    private final List<ManifestFile> deleteManifests;
    private Map<Integer, PartitionSpec> specsById = ImmutableMap.of();
    private Expression dataFilter = Expressions.alwaysTrue();
    private Expression partitionFilter = Expressions.alwaysTrue();
    private boolean caseSensitive = true;
    private boolean ignoreResiduals = false;
    private ExecutorService executorService = null;
    private ScanMetrics scanMetrics = ScanMetrics.noop();
    
    public DeleteFileIndex build() {
        if (deleteManifests.isEmpty()) {
            return EMPTY;
        }
        
        // 按内容类型分组删除manifest
        List<ManifestFile> equalityDeleteManifests = Lists.newArrayList();
        List<ManifestFile> positionDeleteManifests = Lists.newArrayList();
        List<ManifestFile> dvManifests = Lists.newArrayList();
        
        for (ManifestFile manifest : deleteManifests) {
            switch (manifest.content()) {
                case EQUALITY_DELETES:
                    equalityDeleteManifests.add(manifest);
                    break;
                case POSITION_DELETES:
                    positionDeleteManifests.add(manifest);
                    break;
                case DELETION_VECTORS:
                    dvManifests.add(manifest);
                    break;
            }
        }
        
        // 构建不同类型的删除索引
        EqualityDeletes globalDeletes = loadGlobalDeletes(equalityDeleteManifests);
        PartitionMap<EqualityDeletes> eqDeletesByPartition = 
            loadEqualityDeletes(equalityDeleteManifests);
        PartitionMap<PositionDeletes> posDeletesByPartition = 
            loadPositionDeletesByPartition(positionDeleteManifests);
        Map<String, PositionDeletes> posDeletesByPath = 
            loadPositionDeletesByPath(positionDeleteManifests);
        Map<String, DeleteFile> dvByPath = loadDeletionVectors(dvManifests);
        
        return new DeleteFileIndex(
            globalDeletes, eqDeletesByPartition, 
            posDeletesByPartition, posDeletesByPath, dvByPath
        );
    }
}
```

### 5.2 删除文件应用流程

```java
// DeleteFileIndex.java 删除文件查找
DeleteFile[] forDataFile(long sequenceNumber, DataFile file) {
    if (isEmpty) {
        return EMPTY_DELETES;
    }
    
    DeleteFile[] global = findGlobalDeletes(sequenceNumber, file);
    DeleteFile[] eqPartition = findEqPartitionDeletes(sequenceNumber, file);
    DeleteFile dv = findDV(sequenceNumber, file);
    
    if (dv != null && global == null && eqPartition == null) {
        return new DeleteFile[] {dv};
    } else if (dv != null) {
        return concat(global, eqPartition, new DeleteFile[] {dv});
    } else {
        DeleteFile[] posPartition = findPosPartitionDeletes(sequenceNumber, file);
        DeleteFile[] posPath = findPathDeletes(sequenceNumber, file);
        return concat(global, eqPartition, posPartition, posPath);
    }
}

// 创建文件扫描任务时应用删除文件
private static CloseableIterable<FileScanTask> createFileScanTasks(
    CloseableIterable<ManifestEntry<DataFile>> entries, 
    TaskContext ctx) {
    
    return CloseableIterable.transform(entries, entry -> {
        DataFile dataFile = ContentFileUtil.copy(
            entry.file(), ctx.shouldKeepStats(), ctx.columnsToKeepStats()
        );
        
        // 查找适用的删除文件
        DeleteFile[] deleteFiles = ctx.deletes().forEntry(entry);
        ScanMetricsUtil.fileTask(ctx.scanMetrics(), dataFile, deleteFiles);
        
        return new BaseFileScanTask(
            dataFile, deleteFiles, ctx.schemaAsString(), 
            ctx.specAsString(), ctx.residuals()
        );
    });
}
```

### 5.3 等值删除过滤优化

```java
// DeleteFileIndex.java 等值删除范围检查优化
private static boolean canContainEqDeletesForFile(
    DataFile dataFile, EqualityDeleteFile deleteFile) {
    
    Map<Integer, ByteBuffer> dataLowers = dataFile.lowerBounds();
    Map<Integer, ByteBuffer> dataUppers = dataFile.upperBounds();
    
    boolean checkRanges = 
        dataLowers != null && dataUppers != null && 
        deleteFile.hasLowerAndUpperBounds();
    
    Map<Integer, Long> dataNullCounts = dataFile.nullValueCounts();
    Map<Integer, Long> dataValueCounts = dataFile.valueCounts();
    Map<Integer, Long> deleteNullCounts = deleteFile.nullValueCounts();
    Map<Integer, Long> deleteValueCounts = deleteFile.valueCounts();
    
    for (Types.NestedField field : deleteFile.equalityFields()) {
        if (!field.type().isPrimitiveType()) {
            continue; // 嵌套类型跳过统计检查
        }
        
        // 空值检查优化
        if (containsNull(dataNullCounts, field) && 
            containsNull(deleteNullCounts, field)) {
            continue; // 都有null值，需要应用删除
        }
        
        if (allNull(dataNullCounts, dataValueCounts, field) && 
            allNonNull(deleteNullCounts, field)) {
            return false; // 数据全是null，删除没有null
        }
        
        if (allNull(deleteNullCounts, deleteValueCounts, field) && 
            allNonNull(dataNullCounts, field)) {
            return false; // 删除全是null，数据没有null
        }
        
        if (!checkRanges) {
            continue; // 缺少边界信息，假设匹配
        }
        
        // 范围重叠检查
        int id = field.fieldId();
        ByteBuffer dataLower = dataLowers.get(id);
        ByteBuffer dataUpper = dataUppers.get(id);
        Object deleteLower = deleteFile.lowerBound(id);
        Object deleteUpper = deleteFile.upperBound(id);
        
        if (dataLower == null || dataUpper == null || 
            deleteLower == null || deleteUpper == null) {
            continue; // 边界未知，假设匹配
        }
        
        if (!rangesOverlap(field, dataLower, dataUpper, 
                          deleteLower, deleteUpper)) {
            return false; // 范围不重叠，可以跳过此删除文件
        }
    }
    
    return true;
}
```

## 6. 增量读取(Incremental Read)实现

### 6.1 增量扫描设计

```java
// IncrementalDataTableScan.java 核心实现
class IncrementalDataTableScan extends DataTableScan {
    @Override
    public CloseableIterable<FileScanTask> planFiles() {
        Long fromSnapshotId = context().fromSnapshotId();
        Long toSnapshotId = context().toSnapshotId();
        
        // 获取快照范围内的所有快照
        List<Snapshot> snapshots = snapshotsWithin(table(), fromSnapshotId, toSnapshotId);
        Set<Long> snapshotIds = Sets.newHashSet(
            Iterables.transform(snapshots, Snapshot::snapshotId)
        );
        
        // 收集相关的manifest文件
        Set<ManifestFile> manifests = FluentIterable.from(snapshots)
            .transformAndConcat(snapshot -> snapshot.dataManifests(table().io()))
            .filter(manifestFile -> snapshotIds.contains(manifestFile.snapshotId()))
            .toSet();
        
        // 创建ManifestGroup，过滤只包含ADDED条目
        ManifestGroup manifestGroup = new ManifestGroup(table().io(), manifests)
            .caseSensitive(isCaseSensitive())
            .select(scanColumns())
            .filterData(filter())
            .filterManifestEntries(manifestEntry ->
                snapshotIds.contains(manifestEntry.snapshotId()) &&
                manifestEntry.status() == ManifestEntry.Status.ADDED)
            .specsById(table().specs())
            .ignoreDeleted()
            .columnsToKeepStats(columnsToKeepStats());
        
        if (shouldIgnoreResiduals()) {
            manifestGroup = manifestGroup.ignoreResiduals();
        }
        
        // 发送增量扫描事件
        Listeners.notifyAll(new IncrementalScanEvent(
            table().name(), fromSnapshotId, toSnapshotId, 
            filter(), schema(), false
        ));
        
        if (manifests.size() > 1 && shouldPlanWithExecutor()) {
            manifestGroup = manifestGroup.planWith(planExecutor());
        }
        
        return manifestGroup.planFiles();
    }
    
    private static List<Snapshot> snapshotsWithin(
        Table table, long fromSnapshotId, long toSnapshotId) {
        
        List<Snapshot> snapshots = Lists.newArrayList();
        for (Snapshot snapshot : 
             SnapshotUtil.ancestorsBetween(toSnapshotId, fromSnapshotId, table::snapshot)) {
            
            if (snapshot.operation().equals(DataOperations.APPEND)) {
                snapshots.add(snapshot);
            } else if (snapshot.operation().equals(DataOperations.OVERWRITE)) {
                throw new UnsupportedOperationException(
                    String.format("Found %s operation, cannot support incremental data",
                                DataOperations.OVERWRITE));
            }
        }
        return snapshots;
    }
}
```

### 6.2 增量读取流程图

```
增量读取流程 (fromSnapshotId, toSnapshotId]:

┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Snapshot N  │◄───│ Snapshot N-1│◄───│ Snapshot N-2│◄───│   ...       │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
      │                    │                    │
      ▼                    ▼                    ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ManifestList │    │ManifestList │    │ManifestList │
└─────────────┘    └─────────────┘    └─────────────┘
      │                    │                    │
      ▼                    ▼                    ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Manifests   │    │ Manifests   │    │ Manifests   │
│ (Added only)│    │ (Added only)│    │ (Added only)│
└─────────────┘    └─────────────┘    └─────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │   File Scan     │
                    │     Tasks       │
                    └─────────────────┘
```

## 7. 时间旅行(Time Travel)实现

### 7.1 时间旅行扫描

```java
// SnapshotScan.java 时间旅行实现
public ThisT asOfTime(long timestampMillis) {
    Preconditions.checkArgument(
        snapshotId() == null, 
        "Cannot override snapshot, already set snapshot id=%s", 
        snapshotId()
    );
    
    // 根据时间戳查找最近的快照
    return useSnapshot(SnapshotUtil.snapshotIdAsOfTime(table(), timestampMillis));
}

public ThisT useSnapshot(long scanSnapshotId) {
    Preconditions.checkArgument(
        snapshotId() == null, 
        "Cannot override snapshot, already set snapshot id=%s", 
        snapshotId()
    );
    Preconditions.checkArgument(
        table().snapshot(scanSnapshotId) != null,
        "Cannot find snapshot with ID %s",
        scanSnapshotId
    );
    
    // 使用快照时的schema
    Schema newSchema = useSnapshotSchema() ? 
        SnapshotUtil.schemaFor(table(), scanSnapshotId) : tableSchema();
    TableScanContext newContext = context().useSnapshotId(scanSnapshotId);
    return newRefinedScan(table(), newSchema, newContext);
}

public ThisT useRef(String name) {
    if (SnapshotRef.MAIN_BRANCH.equals(name)) {
        return newRefinedScan(table(), tableSchema(), context());
    }
    
    Preconditions.checkArgument(
        snapshotId() == null, 
        "Cannot override ref, already set snapshot id=%s", 
        snapshotId()
    );
    
    Snapshot snapshot = table().snapshot(name);
    Preconditions.checkArgument(snapshot != null, "Cannot find ref %s", name);
    
    TableScanContext newContext = context().useSnapshotId(snapshot.snapshotId());
    return newRefinedScan(table(), SnapshotUtil.schemaFor(table(), name), newContext);
}
```

### 7.2 快照查找工具

```java
// SnapshotUtil.java 时间旅行工具方法
public static long snapshotIdAsOfTime(Table table, long timestampMillis) {
    Long snapshotId = null;
    for (HistoryEntry logEntry : table.history()) {
        if (logEntry.timestampMillis() <= timestampMillis) {
            snapshotId = logEntry.snapshotId();
        } else {
            break;
        }
    }
    
    Preconditions.checkArgument(snapshotId != null,
        "Cannot find a snapshot older than %s", timestampMillis);
    
    return snapshotId;
}

public static Schema schemaFor(Table table, long snapshotId) {
    Snapshot snapshot = table.snapshot(snapshotId);
    if (snapshot == null) {
        return table.schema();
    }
    
    Integer schemaId = snapshot.schemaId();
    if (schemaId != null) {
        return table.schemas().get(schemaId);
    } else {
        // V1表没有schema ID，使用当前schema
        return table.schema();
    }
}
```

## 8. 统计信息和索引过滤机制

### 8.1 统计信息过滤

```java
// ManifestReader.java 统计信息过滤
private CloseableIterable<ManifestEntry<F>> entries(boolean onlyLive) {
    if (hasRowFilter() || hasPartitionFilter() || partitionSet != null) {
        Evaluator evaluator = evaluator();
        InclusiveMetricsEvaluator metricsEvaluator = metricsEvaluator();
        
        // 确保统计列存在
        boolean requireStatsProjection = requireStatsProjection(rowFilter, columns);
        Collection<String> projectColumns = 
            requireStatsProjection ? withStatsColumns(columns) : columns;
        
        CloseableIterable<ManifestEntry<F>> entries = 
            open(projection(fileSchema, fileProjection, projectColumns, caseSensitive));
        
        return CloseableIterable.filter(
            scanMetrics.skippedDataFiles(),
            onlyLive ? filterLiveEntries(entries) : entries,
            entry -> entry != null &&
                    evaluator.eval(entry.file().partition()) &&
                    metricsEvaluator.eval(entry.file()) && // 统计信息过滤
                    inPartitionSet(entry.file())
        );
    }
    // ... 其他逻辑
}

// 创建统计信息评估器
private InclusiveMetricsEvaluator metricsEvaluator() {
    if (lazyMetricsEvaluator == null) {
        if (rowFilter != null && rowFilter != Expressions.alwaysTrue()) {
            lazyMetricsEvaluator = new InclusiveMetricsEvaluator(
                fileSchema, rowFilter, caseSensitive
            );
        } else {
            lazyMetricsEvaluator = new InclusiveMetricsEvaluator(
                fileSchema, Expressions.alwaysTrue(), caseSensitive
            );
        }
    }
    return lazyMetricsEvaluator;
}
```

### 8.2 Manifest级别过滤

```java
// ManifestEvaluator.java Manifest级别过滤优化
public class ManifestEvaluator {
    private final PartitionSpec spec;
    private final Expression expr;
    private final boolean caseSensitive;
    private transient ThreadLocal<Evaluator> evaluators = null;
    
    public static ManifestEvaluator forPartitionFilter(
        Expression expr, PartitionSpec spec, boolean caseSensitive) {
        return new ManifestEvaluator(spec, expr, caseSensitive);
    }
    
    public boolean eval(ManifestFile manifest) {
        if (expr == Expressions.alwaysTrue()) {
            return true;
        } else if (expr == Expressions.alwaysFalse()) {
            return false;
        } else if (manifest.partitions() == null || manifest.partitions().isEmpty()) {
            return true; // 无分区信息时保守处理
        }
        
        // 使用分区统计信息评估
        return evalCache().eval(manifest);
    }
    
    private Evaluator evalCache() {
        if (evaluators == null) {
            this.evaluators = ThreadLocal.withInitial(() -> 
                new Evaluator(spec.partitionType(), expr, caseSensitive)
            );
        }
        return evaluators.get();
    }
}
```

### 8.3 统计信息优化流程图

```
统计信息过滤层次:

1. Table Level
   ├── Snapshot Selection (Time Travel)
   └── Branch/Tag Selection

2. Manifest List Level
   ├── Manifest File Statistics
   ├── Partition Bounds Check
   └── Added/Existing Files Count

3. Manifest Level  
   ├── Partition Filter Evaluation
   ├── File Statistics (Min/Max/Null/NaN)
   ├── Record Count Check
   └── File Size Check

4. Row Group Level (Format Specific)
   ├── Parquet: Row Group Statistics
   ├── ORC: Stripe Statistics  
   └── Bloom Filter (if enabled)

5. Page Level (Format Specific)
   ├── Parquet: Page Index
   ├── Column Chunk Statistics
   └── Dictionary Filtering
```

## 9. MOR和COW表的读取差异

### 9.1 Copy-on-Write (COW) 读取

COW表的读取相对简单，因为删除操作会重写整个数据文件：

```java
// COW表特点：
// - 删除操作触发文件重写
// - 读取时无需处理删除文件
// - 查询性能高，但写入延迟高

public class COWTableScan {
    @Override
    public CloseableIterable<FileScanTask> doPlanFiles() {
        // COW表通常没有删除文件，或删除文件数量很少
        ManifestGroup manifestGroup = new ManifestGroup(io, dataManifests, deleteManifests)
            .ignoreDeleted() // 忽略已删除的文件条目
            .filterData(filter());
        
        return manifestGroup.planFiles();
    }
}
```

### 9.2 Merge-on-Read (MOR) 读取

MOR表需要在读取时处理删除文件：

```java
// MOR表特点：
// - 删除操作生成删除文件
// - 读取时需要应用删除文件过滤
// - 写入快，但读取需要额外处理

public class MORTableScan {
    @Override
    public CloseableIterable<FileScanTask> doPlanFiles() {
        // MOR表需要构建完整的删除索引
        DeleteFileIndex deleteFiles = deleteIndexBuilder
            .scanMetrics(scanMetrics())
            .build();
        
        // 每个FileScanTask都包含相应的删除文件
        ManifestGroup manifestGroup = new ManifestGroup(io, dataManifests, deleteManifests)
            .filterData(filter());
        
        return manifestGroup.planFiles(); // 包含删除文件处理
    }
}
```

### 9.3 读取性能对比

```java
// 性能特征对比
public class ReadPerformanceComparison {
    
    // COW表读取特征
    class COWReading {
        // 优势：
        // - 无删除文件处理开销
        // - 文件数量相对较少  
        // - 查询规划简单
        
        // 劣势：
        // - 删除操作代价高
        // - 小量删除也需要重写整个文件
    }
    
    // MOR表读取特征  
    class MORReading {
        // 优势：
        // - 删除操作快速
        // - 适合频繁更新场景
        
        // 劣势：
        // - 需要删除文件索引构建
        // - 查询规划复杂
        // - 可能产生大量小删除文件
    }
}
```

## 10. ORC和Parquet索引优化读取

### 10.1 Parquet读取优化

```java
// ParquetReader.java 优化特性
public class ParquetOptimizations {
    
    // 1. 行组级别过滤
    public static CloseableIterable<T> createReader(
        InputFile file, Schema projection, Expression filter) {
        
        ParquetReadOptions.Builder optionsBuilder = ParquetReadOptions.builder();
        
        // 启用统计信息过滤
        if (filter != Expressions.alwaysTrue()) {
            optionsBuilder.withRecordFilter(
                ParquetFilters.convert(projection, filter, caseSensitive)
            );
        }
        
        // 向量化读取支持
        if (enableVectorization) {
            return new VectorizedParquetReader<>(
                file, projection, filter, optionsBuilder.build()
            );
        }
        
        return new ParquetReader<>(file, projection, filter, optionsBuilder.build());
    }
    
    // 2. Page Index 利用
    private void configurePageIndex(ParquetReadOptions.Builder builder) {
        // 启用页面索引以支持更细粒度的跳过
        builder.usePageChecksumVerification(true);
        builder.useStatisticsFilter(true);
        builder.useDictionaryFilter(true);
        builder.useBloomFilter(true);
    }
    
    // 3. 字典过滤
    private void configureDictionaryFilter(ParquetReadOptions.Builder builder, 
                                          Expression filter) {
        if (supportsDictionaryFilter(filter)) {
            builder.useDictionaryFilter(true);
            // 设置字典过滤逻辑
        }
    }
    
    // 4. Bloom Filter 支持
    private void configureBloomFilter(Map<String, String> properties, 
                                    ParquetReadOptions.Builder builder) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX)) {
                String column = entry.getKey().substring(
                    PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX.length()
                );
                if (Boolean.parseBoolean(entry.getValue())) {
                    builder.enableBloomFilter(column);
                }
            }
        }
    }
}
```

### 10.2 ORC读取优化

```java
// ORC读取优化 (ORC模块实现类似逻辑)
public class ORCOptimizations {
    
    // 1. Stripe级别过滤
    private void configureStripeFilter(OrcFile.ReaderOptions options, 
                                     Expression filter) {
        if (filter != Expressions.alwaysTrue()) {
            // 转换Iceberg表达式为ORC SearchArgument
            SearchArgument searchArg = ConvertFilters.convert(filter);
            options.searchArgument(searchArg, null);
        }
    }
    
    // 2. 向量化读取
    private void enableVectorization(OrcFile.ReaderOptions options) {
        options.useZeroCopy(true);
        options.useColumnVectorBatch(true);
    }
    
    // 3. 压缩和编码优化
    private void configureCompression(OrcFile.ReaderOptions options) {
        options.orcTail(null); // 让ORC自动选择最优压缩
    }
    
    // 4. Predicate下推
    private CloseableIterable<T> createReaderWithPredicate(
        InputFile file, Schema projection, Expression filter) {
        
        try {
            OrcFile.ReaderOptions options = OrcFile.readerOptions(
                new Configuration()
            );
            
            configureStripeFilter(options, filter);
            enableVectorization(options);
            
            Reader orcReader = OrcFile.createReader(
                new Path(file.location()), options
            );
            
            return new OrcIterable<>(file, projection, orcReader, filter);
            
        } catch (IOException e) {
            throw new RuntimeIOException(e, "Failed to open ORC file: %s", 
                                       file.location());
        }
    }
}
```

### 10.3 格式特定优化对比

```java
// 格式优化特性对比
public class FormatOptimizationComparison {
    
    /*
     * Parquet优化特性:
     * 1. Row Group Statistics - 行组级别统计信息
     * 2. Page Index - 页面级别索引 (Parquet 1.12+)
     * 3. Dictionary Filter - 字典过滤
     * 4. Bloom Filter - 布隆过滤器
     * 5. Column Chunk Skipping - 列块跳过
     * 6. Vectorized Reading - 向量化读取
     */
    class ParquetFeatures {
        // 最适合：宽表、分析查询、列式访问模式
        // 优势：成熟的生态系统、广泛支持、优秀的压缩
    }
    
    /*
     * ORC优化特性:
     * 1. Stripe Statistics - 条带级别统计信息
     * 2. Row Index - 行级别索引
     * 3. Predicate Pushdown - 谓词下推
     * 4. ACID Support - ACID事务支持
     * 5. Vectorized Processing - 向量化处理
     * 6. Zero-Copy Reading - 零拷贝读取
     */
    class ORCFeatures {
        // 最适合：事务场景、更新密集、Hive生态系统
        // 优势：原生ACID支持、优秀的压缩率
    }
}
```

## 11. 完整读取流程示例

### 11.1 标准表扫描示例

```java
public class StandardTableScanExample {
    
    public void demonstrateFullReadFlow() {
        // 1. 获取表实例
        Table table = catalog.loadTable(TableIdentifier.of("db", "table"));
        
        // 2. 创建表扫描
        TableScan scan = table.newScan()
            .filter(Expressions.equal("status", "active"))
            .select("id", "name", "created_at")
            .caseSensitive(false);
        
        // 3. 执行扫描规划
        try (CloseableIterable<CombinedScanTask> tasks = scan.planTasks()) {
            for (CombinedScanTask task : tasks) {
                // 4. 处理每个扫描任务
                processTask(task);
            }
        }
    }
    
    private void processTask(CombinedScanTask combinedTask) {
        for (FileScanTask fileTask : combinedTask.files()) {
            // 5. 读取数据文件
            try (CloseableIterable<Record> records = readDataFile(fileTask)) {
                for (Record record : records) {
                    // 6. 处理记录（应用删除过滤等）
                    if (!isDeleted(record, fileTask.deletes())) {
                        processRecord(record);
                    }
                }
            }
        }
    }
    
    private CloseableIterable<Record> readDataFile(FileScanTask task) {
        InputFile inputFile = table.io().newInputFile(task.file().path());
        
        // 根据文件格式选择读取器
        switch (task.file().format()) {
            case PARQUET:
                return Parquet.read(inputFile)
                    .project(task.schema())
                    .filter(task.residual())
                    .createReaderFunc(ParquetReader::new)
                    .build();
                    
            case ORC:
                return ORC.read(inputFile)
                    .project(task.schema())
                    .filter(task.residual())
                    .build();
                    
            default:
                throw new UnsupportedOperationException("Unsupported format");
        }
    }
    
    private boolean isDeleted(Record record, DeleteFile[] deleteFiles) {
        // 应用删除文件过滤逻辑
        for (DeleteFile deleteFile : deleteFiles) {
            if (deleteFile.content() == FileContent.POSITION_DELETES) {
                // 检查位置删除
                if (isPositionDeleted(record, deleteFile)) {
                    return true;
                }
            } else if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
                // 检查等值删除
                if (isEqualityDeleted(record, deleteFile)) {
                    return true;
                }
            }
        }
        return false;
    }
}
```

### 11.2 增量读取示例

```java
public class IncrementalReadExample {
    
    public void demonstrateIncrementalRead() {
        Table table = catalog.loadTable(TableIdentifier.of("db", "events"));
        
        // 获取昨天到今天的增量数据
        long yesterdaySnapshot = getSnapshotAtTime(
            table, System.currentTimeMillis() - 24 * 3600 * 1000L
        );
        long currentSnapshot = table.currentSnapshot().snapshotId();
        
        // 创建增量扫描
        TableScan incrementalScan = table.newScan()
            .appendsBetween(yesterdaySnapshot, currentSnapshot)
            .filter(Expressions.equal("event_type", "purchase"));
        
        // 执行增量读取
        try (CloseableIterable<CombinedScanTask> tasks = incrementalScan.planTasks()) {
            for (CombinedScanTask task : tasks) {
                processIncrementalTask(task);
            }
        }
    }
    
    private long getSnapshotAtTime(Table table, long timestampMillis) {
        for (HistoryEntry entry : table.history()) {
            if (entry.timestampMillis() <= timestampMillis) {
                return entry.snapshotId();
            }
        }
        throw new IllegalArgumentException("No snapshot found at time: " + timestampMillis);
    }
    
    private void processIncrementalTask(CombinedScanTask task) {
        // 处理增量数据，所有文件都是新增的
        for (FileScanTask fileTask : task.files()) {
            // 增量读取通常不需要处理删除文件
            try (CloseableIterable<Record> records = readDataFile(fileTask)) {
                for (Record record : records) {
                    processIncrementalRecord(record);
                }
            }
        }
    }
}
```

### 11.3 时间旅行示例

```java
public class TimeTravelExample {
    
    public void demonstrateTimeTravelRead() {
        Table table = catalog.loadTable(TableIdentifier.of("db", "orders"));
        
        // 读取一周前的数据
        long oneWeekAgo = System.currentTimeMillis() - 7 * 24 * 3600 * 1000L;
        
        TableScan timeTravelScan = table.newScan()
            .asOfTime(oneWeekAgo)
            .filter(Expressions.equal("region", "us-west"))
            .select("order_id", "amount", "customer_id");
        
        // 执行时间旅行查询
        try (CloseableIterable<CombinedScanTask> tasks = timeTravelScan.planTasks()) {
            for (CombinedScanTask task : tasks) {
                processTimeTravelTask(task);
            }
        }
    }
    
    public void demonstrateSnapshotRead() {
        Table table = catalog.loadTable(TableIdentifier.of("db", "products"));
        
        // 使用特定快照ID读取
        long specificSnapshotId = 1234567890L;
        
        TableScan snapshotScan = table.newScan()
            .useSnapshot(specificSnapshotId)
            .filter(Expressions.greaterThan("price", 100));
        
        try (CloseableIterable<CombinedScanTask> tasks = snapshotScan.planTasks()) {
            for (CombinedScanTask task : tasks) {
                processSnapshotTask(task);
            }
        }
    }
    
    public void demonstrateBranchRead() {
        Table table = catalog.loadTable(TableIdentifier.of("db", "features"));
        
        // 读取特定分支的数据
        TableScan branchScan = table.newScan()
            .useRef("feature-branch-v2")
            .filter(Expressions.equal("enabled", true));
        
        try (CloseableIterable<CombinedScanTask> tasks = branchScan.planTasks()) {
            for (CombinedScanTask task : tasks) {
                processBranchTask(task);
            }
        }
    }
}
```

## 12. 性能优化最佳实践

### 12.1 扫描优化配置

```java
public class ScanOptimizationConfig {
    
    public TableScan optimizeTableScan(Table table, Expression filter) {
        return table.newScan()
            // 1. 早期过滤
            .filter(filter)
            
            // 2. 列裁剪
            .select(getRequiredColumns())
            
            // 3. 大小写敏感性配置
            .caseSensitive(false)
            
            // 4. 忽略残留条件(如果可以接受)
            .option(TableProperties.SCAN_IGNORE_RESIDUALS, "true")
            
            // 5. 并行规划
            .option(TableProperties.SCAN_THREAD_POOL_SIZE, "8")
            
            // 6. 批次大小优化
            .option(TableProperties.SCAN_BATCH_SIZE, "16777216") // 16MB
            
            // 7. 分区过滤优化
            .option(TableProperties.SCAN_PARTITION_FILTER_MODE, "advanced");
    }
    
    private List<String> getRequiredColumns() {
        // 只选择必要的列
        return Arrays.asList("id", "name", "created_at", "status");
    }
}
```

### 12.2 读取性能监控

```java
public class ReadPerformanceMonitoring {
    
    public void monitorScanPerformance(TableScan scan) {
        try (CloseableIterable<CombinedScanTask> tasks = scan.planTasks()) {
            long startTime = System.currentTimeMillis();
            int filesProcessed = 0;
            long recordsRead = 0;
            long bytesRead = 0;
            
            for (CombinedScanTask task : tasks) {
                for (FileScanTask fileTask : task.files()) {
                    filesProcessed++;
                    bytesRead += fileTask.file().fileSizeInBytes();
                    
                    try (CloseableIterable<Record> records = readDataFile(fileTask)) {
                        for (Record record : records) {
                            recordsRead++;
                        }
                    }
                }
            }
            
            long endTime = System.currentTimeMillis();
            long durationMs = endTime - startTime;
            
            // 输出性能指标
            System.out.printf(
                "Scan Performance:\n" +
                "  Duration: %d ms\n" +
                "  Files: %d\n" +
                "  Records: %d\n" +
                "  Bytes: %d\n" +
                "  Records/sec: %.2f\n" +
                "  MB/sec: %.2f\n",
                durationMs,
                filesProcessed,
                recordsRead,
                bytesRead,
                recordsRead * 1000.0 / durationMs,
                bytesRead / 1024.0 / 1024.0 * 1000.0 / durationMs
            );
        }
    }
}
```

## 13. 详细流程图补充

### 13.1 Parquet文件核心数据结构详解

#### Parquet Row Group 详细结构

```
Parquet Row Group内部组织:
┌─────────────────────────────────────────────────────────────────┐
│                    Row Group (128MB默认大小)                    │
├─────────────────────────────────────────────────────────────────┤
│ Column Chunk 1 (id: INT32)                                     │
│ ┌───────────────────────────────────────────────────────────┐ │
│ │ Page Header:                                                │ │
│ │ - type: DATA_PAGE                                          │ │
│ │ - uncompressed_page_size: 8192                            │ │
│ │ - compressed_page_size: 4096                              │ │
│ │ - data_page_header:                                       │ │
│ │   * num_values: 1000                                      │ │
│ │   * encoding: PLAIN                                       │ │
│ │   * definition_level_encoding: RLE                        │ │
│ │   * repetition_level_encoding: BIT_PACKED                 │ │
│ ├───────────────────────────────────────────────────────────┤ │
│ │ Repetition Levels (0字节, 原始类型)                        │ │
│ ├───────────────────────────────────────────────────────────┤ │
│ │ Definition Levels (RLE编码, 可空字段)                       │ │
│ │ - 数据: [1,1,1,0,1,1,1,1,0,1...] (1=非空, 0=null)           │ │
│ │ - RLE压缩: (1,3),(0,1),(1,4),(0,1),(1,1)...                  │ │
│ ├───────────────────────────────────────────────────────────┤ │
│ │ Values Data (PLAIN编码)                                     │ │
│ │ - 数据: [1,2,3,5,6,7,8,10,11...]                             │ │
│ │ - 存储: 4字节小端序整数数组                                │ │
│ └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│ Column Chunk 2 (name: BYTE_ARRAY)                              │
│ ┌───────────────────────────────────────────────────────────┐ │
│ │ Dictionary Page:                                            │ │
│ │ - 字典项: ["Alice","Bob","Charlie","David"]                   │ │
│ │ - 编码: length(5)+"Alice", length(3)+"Bob"...              │ │
│ ├───────────────────────────────────────────────────────────┤ │
│ │ Data Page:                                                  │ │
│ │ - Definition Levels: [1,1,0,1,1,1...]                      │ │
│ │ - Dictionary Indices (RLE编码): [0,1,2,0,1,3...]            │ │
│ │   * 0→"Alice", 1→"Bob", 2→"Charlie", 3→"David"           │ │
│ └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│ Column Chunk 3 (timestamp: INT96)                              │
│ ┌───────────────────────────────────────────────────────────┐ │
│ │ Data Page (DELTA_BINARY_PACKED编码):                        │ │
│ │ - 基准值: 2024-01-01T00:00:00                              │ │
│ │ - 增量: [0, +3600000, +7200000, +10800000...]  (秒)       │ │
│ │ - 优势: 高效压缩时间序列                                   │ │
│ └───────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

#### Parquet Page类型与编码方式

```
Parquet Page类型及编码策略:

┌─────────────────────────────────────────────────────────────────┐
│                      Page类型分类                             │
├─────────────────────────────────────────────────────────────────┤
│ 1. DATA_PAGE (V1)                                               │
│    - 传统数据页格式                                             │
│    - 包含: Header + RepLevels + DefLevels + Values              │
│    - 编码: PLAIN/DICTIONARY/RLE_DICTIONARY                      │
│                                                                 │
│ 2. DATA_PAGE_V2                                                 │
│    - 优化后的数据页格式                                       │
│    - Rep/Def Levels分开压缩                                      │
│    - 更高的压缩率和性能                                        │
│                                                                 │
│ 3. DICTIONARY_PAGE                                              │
│    - 字典数据存储                                               │
│    - 用于字符串类型优化                                        │
│    - 在Column Chunk开头出现                                      │
│                                                                 │
│ 4. INDEX_PAGE                                                   │
│    - 列索引信息                                                 │
│    - Bloom Filter数据                                            │
└─────────────────────────────────────────────────────────────────┘

编码方式详解:

┌─────────────────────────────────────────────────────────────────┐
│                     数值编码方式                            │
├─────────────────────────────────────────────────────────────────┤
│ PLAIN:                                                          │
│ ┌───────────────────────────────────────────────────────────┐ │
│ │ - 原始数据直接存储，无压缩                                 │ │
│ │ - 适用: 随机数据、唯一ID                                   │ │
│ │ - 示例: [1,5,3,8,2] → [1,5,3,8,2]                          │ │
│ └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│ DICTIONARY:                                                     │
│ ┌───────────────────────────────────────────────────────────┐ │
│ │ - 高重复度数据优化                                       │ │
│ │ - 适用: 字符串、枚举类型                                   │ │
│ │ - 示例: ["A","B","A","C","B"]                               │ │
│ │   → Dict:["A","B","C"] + Indices:[0,1,0,2,1]               │ │
│ └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│ DELTA_BINARY_PACKED:                                            │
│ ┌───────────────────────────────────────────────────────────┐ │
│ │ - 对于有序数据的增量编码                               │ │
│ │ - 适用: 时间序列、ID序列                                │ │
│ │ - 示例: [100,101,102,105,106]                               │ │
│ │   → base=100, deltas=[1,1,3,1]                            │ │
│ └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│ BYTE_STREAM_SPLIT:                                              │
│ ┌───────────────────────────────────────────────────────────┐ │
│ │ - 浮点数的字节分离编码                                 │ │
│ │ - 适用: float/double类型高精度数据                        │ │
│ │ - 理念: 相似值的相同字节聚集，提高压缩效果             │ │
│ └───────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### 13.2 Time Travel 数据读取详细流程图

#### Time Travel 核心架构图

```
Time Travel 数据读取架构:

┌─────────────────────────────────────────────────────────────────────────┐
│                        Time Travel Query Flow                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Client Query                                                          │
│       │                                                                │
│       ▼                                                                │
│  ┌─────────────────┐                                                  │
│  │ Time Travel     │                                                  │
│  │ Request         │                                                  │
│  │                 │                                                  │
│  │ - asOfTime()    │                                                  │
│  │ - useSnapshot() │                                                  │
│  │ - useRef()      │                                                  │
│  └─────────────────┘                                                  │
│           │                                                            │
│           ▼                                                            │
│  ┌─────────────────┐       ┌─────────────────┐                       │
│  │ SnapshotUtil    │────── │ Table History   │                       │
│  │                 │       │                 │                       │
│  │ - asOfTime()    │       │ - HistoryEntry[]│                       │
│  │ - schemaFor()   │       │ - Timestamps    │                       │
│  │ - ancestorIds() │       │ - SnapshotIds   │                       │
│  └─────────────────┘       └─────────────────┘                       │
│           │                                                            │
│           ▼                                                            │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │              Snapshot Resolution Process                        │  │
│  │                                                                 │  │
│  │  Time Travel Type:                                              │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │  │
│  │  │asOfTime()   │  │useSnapshot()│  │useRef()     │             │  │
│  │  │             │  │             │  │             │             │  │
│  │  │Binary Search│  │Direct Lookup│  │Ref Lookup   │             │  │
│  │  │by Timestamp │  │by ID        │  │Branch/Tag   │             │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘             │  │
│  └─────────────────────────────────────────────────────────────────┘  │
│           │                                                            │
│           ▼                                                            │
│  ┌─────────────────┐                                                  │
│  │Target Snapshot  │                                                  │
│  │                 │                                                  │
│  │ - SnapshotId    │                                                  │
│  │ - Schema        │                                                  │
│  │ - ManifestList  │                                                  │
│  │ - Timestamp     │                                                  │
│  └─────────────────┘                                                  │
│           │                                                            │
│           ▼                                                            │
│  ┌─────────────────┐                                                  │
│  │Regular Read     │                                                  │
│  │Flow Continues   │                                                  │
│  │(See Main Flow)  │                                                  │
│  └─────────────────┘                                                  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Time Travel 三种模式对比

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Time Travel Modes Comparison                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Mode 1: asOfTime(timestampMillis)                                     │
│  ┌───────────────────────────────────────────────────────────────────┐ │
│  │ Timeline: S1───S2───S3───S4───S5                                  │ │
│  │           ↑    ↑    ↑    ↑    ↑                                   │ │
│  │           T1   T2   T3   T4   T5                                  │ │
│  │                                                                   │ │
│  │ Query: asOfTime(T2.5)  → Returns S2 (Latest before T2.5)         │ │
│  │ Query: asOfTime(T3)    → Returns S3 (Exact match)                │ │
│  │ Query: asOfTime(T0)    → Error (No snapshot before T0)           │ │
│  └───────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  Mode 2: useSnapshot(snapshotId)                                       │
│  ┌───────────────────────────────────────────────────────────────────┐ │
│  │ Direct Snapshot Access:                                           │ │
│  │                                                                   │ │
│  │ Input: snapshotId = 12345                                         │ │
│  │ Process: table.snapshot(12345)                                    │ │
│  │ Result: Direct access to specific snapshot                        │ │
│  │         OR Error if snapshot doesn't exist                        │ │
│  └───────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  Mode 3: useRef(branchOrTag)                                           │
│  ┌───────────────────────────────────────────────────────────────────┐ │
│  │ Branch/Tag Resolution:                                             │ │
│  │                                                                   │ │
│  │ Main Branch:     S1───S2───S3───S4───S5                           │ │
│  │                            │                                      │ │
│  │ Feature Branch:            └───F1───F2                            │ │
│  │                                                                   │ │
│  │ Tag "v1.0":               S3                                      │ │
│  │                                                                   │ │
│  │ Query: useRef("main")      → S5 (Latest on main)                  │ │
│  │ Query: useRef("feature")   → F2 (Latest on feature)               │ │
│  │ Query: useRef("v1.0")      → S3 (Tagged snapshot)                 │ │
│  └───────────────────────────────────────────────────────────────────┘ │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 13.2 Parquet 文件存储结构和读取流程

#### Parquet 文件存储结构图

```
Parquet File Structure:

┌─────────────────────────────────────────────────────────────────────────┐
│                           Parquet File Layout                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  File Header                                                            │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Magic Number: "PAR1" (4 bytes)                                  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Row Group 1                                                            │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Column Chunk 1 (e.g., "id" column)                              │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ Page Header                                                 │ │   │
│  │ │ ┌─────────────────────────────────────────────────────────┐ │ │   │
│  │ │ │ - Page Type (DATA_PAGE/INDEX_PAGE/DICTIONARY_PAGE)      │ │ │   │
│  │ │ │ - Uncompressed Size                                     │ │ │   │
│  │ │ │ - Compressed Size                                       │ │ │   │
│  │ │ │ - CRC Checksum                                          │ │ │   │
│  │ │ │ - Statistics (Min/Max/Null Count)                       │ │ │   │
│  │ │ └─────────────────────────────────────────────────────────┘ │ │   │
│  │ │ Data Page 1                                                 │ │   │
│  │ │ ┌─────────────────────────────────────────────────────────┐ │ │   │
│  │ │ │ Repetition Levels (if nested)                          │ │ │   │
│  │ │ │ Definition Levels (for nulls)                          │ │ │   │
│  │ │ │ Encoded Values (Dictionary/Plain/Delta)                │ │ │   │
│  │ │ └─────────────────────────────────────────────────────────┘ │ │   │
│  │ │ Data Page 2, Data Page 3, ... Data Page N                  │ │   │
│  │ │                                                             │ │   │
│  │ │ Dictionary Page (if dictionary encoding used)               │ │   │
│  │ │ ┌─────────────────────────────────────────────────────────┐ │ │   │
│  │ │ │ Dictionary Values                                       │ │ │   │
│  │ │ │ [Value1, Value2, Value3, ..., ValueN]                  │ │ │   │
│  │ │ └─────────────────────────────────────────────────────────┘ │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ Column Chunk 2 (e.g., "name" column)                           │   │
│  │ Column Chunk 3 (e.g., "created_at" column)                     │   │
│  │ ...                                                             │   │
│  │ Column Chunk N                                                  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Row Group 2, Row Group 3, ... Row Group M                             │
│                                                                         │
│  Column Index (Page Index) - Optional (Parquet 1.12+)                  │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ For each Column Chunk:                                          │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ Page Statistics Index                                       │ │   │
│  │ │ - Min/Max values for each page                              │ │   │
│  │ │ - Null counts for each page                                 │ │   │
│  │ │ - Page locations and sizes                                  │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ Offset Index                                                │ │   │
│  │ │ - Page locations within the row group                      │ │   │
│  │ │ - First row index of each page                              │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Bloom Filter (Optional)                                                │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ For selected columns:                                           │   │
│  │ - Bloom filter for each column chunk                           │   │
│  │ - Configurable false positive rate                             │   │
│  │ - Used for fast existence checks                               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  File Footer                                                            │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Row Group Metadata                                              │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ For each Row Group:                                         │ │   │
│  │ │ - Total byte size                                           │ │   │
│  │ │ - Number of rows                                            │ │   │
│  │ │ - Column metadata for each column                           │ │   │
│  │ │   * Data type                                               │ │   │
│  │ │   * Encodings used                                          │ │   │
│  │ │   * Path in schema                                          │ │   │
│  │ │   * Statistics (min, max, null_count, distinct_count)       │ │   │
│  │ │   * Compression codec                                       │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ Schema Definition                                               │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ message schema {                                            │ │   │
│  │ │   required int64 id;                                        │ │   │
│  │ │   optional binary name (UTF8);                              │ │   │
│  │ │   required int64 created_at;                                │ │   │
│  │ │   optional binary status (UTF8);                            │ │   │
│  │ │ }                                                           │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ Key-Value Metadata                                              │   │
│  │ Version, Created By, etc.                                       │   │
│  │                                                                 │   │
│  │ Footer Length (4 bytes)                                         │   │
│  │ Magic Number: "PAR1" (4 bytes)                                  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 13.3 MOR表合并读取数据流程

#### MOR表读取架构图

```
MOR (Merge-on-Read) Table Reading Architecture:

┌─────────────────────────────────────────────────────────────────────────┐
│                        MOR Table Reading Flow                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Query Request                                                          │
│       │                                                                 │
│       ▼                                                                 │
│  ┌─────────────────┐                                                   │
│  │ DataTableScan   │                                                   │
│  │ (MOR Mode)      │                                                   │
│  └─────────────────┘                                                   │
│           │                                                             │
│           ▼                                                             │
│  ┌─────────────────┐       ┌─────────────────┐                        │
│  │   Snapshot      │────── │  ManifestList   │                        │
│  │                 │       │                 │                        │
│  │ - Data Manifests│       │ ┌─────────────┐ │                        │
│  │ - Delete        │       │ │Data Manifest│ │                        │
│  │   Manifests     │       │ │   File 1    │ │                        │
│  │                 │       │ └─────────────┘ │                        │
│  └─────────────────┘       │ ┌─────────────┐ │                        │
│                             │ │Data Manifest│ │                        │
│                             │ │   File 2    │ │                        │
│                             │ └─────────────┘ │                        │
│                             │ ┌─────────────┐ │                        │
│                             │ │Delete       │ │                        │
│                             │ │Manifest 1   │ │                        │
│                             │ └─────────────┘ │                        │
│                             │ ┌─────────────┐ │                        │
│                             │ │Delete       │ │                        │
│                             │ │Manifest 2   │ │                        │
│                             │ └─────────────┘ │                        │
│                             └─────────────────┘                        │
│                                     │                                   │
│                                     ▼                                   │
│                           ┌─────────────────┐                          │
│                           │ ManifestGroup   │                          │
│                           │                 │                          │
│                           │ Processing:     │                          │
│                           │ 1. Data Files   │                          │
│                           │ 2. Delete Files │                          │
│                           │ 3. Build Index  │                          │
│                           └─────────────────┘                          │
│                                     │                                   │
│                                     ▼                                   │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                DeleteFileIndex Building                         │   │
│  │                                                                 │   │
│  │  Delete Manifest 1        Delete Manifest 2                    │   │
│  │  ┌─────────────────┐      ┌─────────────────┐                  │   │
│  │  │Equality Deletes │      │Position Deletes │                  │   │
│  │  │                 │      │                 │                  │   │
│  │  │File: data1.parq │      │File: data2.parq │                  │   │
│  │  │Cols: [id, name] │      │Positions:[1,5,9]│                  │   │
│  │  │Values:          │      │                 │                  │   │
│  │  │(123, "Alice")   │      │File: data3.parq │                  │   │
│  │  │(456, "Bob")     │      │Positions:[2,7]  │                  │   │
│  │  └─────────────────┘      └─────────────────┘                  │   │
│  │           │                         │                           │   │
│  │           ▼                         ▼                           │   │
│  │  ┌─────────────────┐      ┌─────────────────┐                  │   │
│  │  │EqualityDeletes  │      │PositionDeletes  │                  │   │
│  │  │Index            │      │Index            │                  │   │
│  │  │                 │      │                 │                  │   │
│  │  │ByPartition:     │      │ByDataFile:      │                  │   │
│  │  │ p1 → {deletes1} │      │ data2 → {1,5,9} │                  │   │
│  │  │ p2 → {deletes2} │      │ data3 → {2,7}   │                  │   │
│  │  │Global: {global} │      │                 │                  │   │
│  │  └─────────────────┘      └─────────────────┘                  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                     │                                   │
│                                     ▼                                   │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                  FileScanTask Creation                          │   │
│  │                                                                 │   │
│  │  For each Data File:                                            │   │
│  │  ┌─────────────────────────────────────────────────────────────┐ │   │
│  │  │ DataFile: data1.parquet                                     │ │   │
│  │  │ ┌─────────────────────────────────────────────────────────┐ │ │   │
│  │  │ │ 1. Find applicable delete files:                       │ │ │   │
│  │  │ │    - Global equality deletes                            │ │ │   │
│  │  │ │    - Partition-scoped equality deletes                  │ │ │   │
│  │  │ │    - Position deletes for this file                    │ │ │   │
│  │  │ │    - Deletion vectors for this file                    │ │ │   │
│  │  │ │                                                         │ │ │   │
│  │  │ │ 2. Check sequence numbers:                              │ │ │   │
│  │  │ │    DataFile.sequenceNumber = 100                       │ │ │   │
│  │  │ │    DeleteFile.sequenceNumber = 105                     │ │ │   │
│  │  │ │    → Apply delete (105 > 100)                          │ │ │   │
│  │  │ │                                                         │ │ │   │
│  │  │ │ 3. Create FileScanTask:                                 │ │ │   │
│  │  │ │    - DataFile: data1.parquet                           │ │ │   │
│  │  │ │    - DeleteFiles: [eq_del1, pos_del1]                  │ │ │   │
│  │  │ │    - Schema: projected schema                           │ │ │   │
│  │  │ │    - Residual: remaining predicates                    │ │ │   │
│  │  │ └─────────────────────────────────────────────────────────┘ │ │   │
│  │  └─────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 13.4 多层过滤数据读取架构

```
Multi-Level Data Filtering Architecture:

┌─────────────────────────────────────────────────────────────────────────┐
│                      Multi-Level Data Filtering                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Level 1: Query Planning (表级别)                                       │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Input: SELECT * FROM table                                      │   │
│  │        WHERE id > 1000 AND status = 'ACTIVE'                    │   │
│  │        AND created_at > '2024-01-01'                             │   │
│  │                                                                 │   │
│  │ Parse to Iceberg Expression:                                    │   │
│  │ Expressions.and(                                                │   │
│  │   Expressions.greaterThan("id", 1000),                          │   │
│  │   Expressions.equal("status", "ACTIVE"),                        │   │
│  │   Expressions.greaterThan("created_at", timestamp)              │   │
│  │ )                                                               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                      │                                  │
│                                      ▼                                  │
│  Level 2: Snapshot Filtering                                            │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Time Travel / Branch Selection:                                 │   │
│  │                                                                 │   │
│  │ if (timeTravel) {                                               │   │
│  │   snapshot = SnapshotUtil.snapshotIdAsOfTime(table, timestamp) │   │
│  │ } else {                                                        │   │
│  │   snapshot = table.currentSnapshot()                           │   │
│  │ }                                                               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                      │                                  │
│                                      ▼                                  │
│  Level 3: Manifest Filtering (文件组级别)                               │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ ManifestEvaluator.eval(manifestFile):                           │   │
│  │                                                                 │   │
│  │ For each Manifest File:                                         │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ Partition Bounds Check:                                     │ │   │
│  │ │                                                             │ │   │
│  │ │ Manifest covers partitions:                                 │ │   │
│  │ │ - date_partition IN ['2024-01-01', '2024-01-31']           │ │   │
│  │ │ - region_partition IN ['US', 'EU']                         │ │   │
│  │ │                                                             │ │   │
│  │ │ Filter: created_at > '2024-01-01'                           │ │   │
│  │ │ Result: Manifest covers needed partitions ✓                │ │   │
│  │ │                                                             │ │   │
│  │ │ Added/Existing/Deleted Files Count:                         │ │   │
│  │ │ - hasAddedFiles: true                                       │ │   │
│  │ │ - hasExistingFiles: true                                    │ │   │
│  │ │ - ignoreDeleted: true → skip deleted-only manifests        │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ Result: Include/Skip entire manifest                            │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                      │                                  │
│                                      ▼                                  │
│  Level 4: Data File Filtering (文件级别)                                │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ InclusiveMetricsEvaluator.eval(dataFile):                       │   │
│  │                                                                 │   │
│  │ For each Data File in included manifests:                       │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ File Statistics Check:                                      │ │   │
│  │ │                                                             │ │   │
│  │ │ DataFile metrics:                                           │ │   │
│  │ │ - recordCount: 50000                                        │ │   │
│  │ │ - nullValueCounts: {id: 0, status: 1000, created_at: 0}    │ │   │
│  │ │ - lowerBounds: {id: 500, status: "ACTIVE", ...}           │ │   │
│  │ │ - upperBounds: {id: 2000, status: "INACTIVE", ...}        │ │   │
│  │ │                                                             │ │   │
│  │ │ Filter Evaluation:                                          │ │   │
│  │ │ 1. id > 1000:                                              │ │   │
│  │ │    upperBound(2000) > 1000 ✓ (cannot skip)                │ │   │
│  │ │                                                             │ │   │
│  │ │ 2. status = 'ACTIVE':                                      │ │   │
│  │ │    'ACTIVE' in [lowerBound, upperBound] ✓                 │ │   │
│  │ │    nullCount(1000) < recordCount(50000) ✓ (has values)    │ │   │
│  │ │                                                             │ │   │
│  │ │ 3. created_at > '2024-01-01':                              │ │   │
│  │ │    upperBound > '2024-01-01' ✓                             │ │   │
│  │ │                                                             │ │   │
│  │ │ Result: Include file (cannot be skipped)                   │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                      │                                  │
│                                      ▼                                  │
│  Level 5: Row Group/Stripe Filtering (格式特定)                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Format-Specific Statistics Filtering:                           │   │
│  │                                                                 │   │
│  │ Parquet (Row Group Level):                                      │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ For each Row Group:                                         │ │   │
│  │ │   RowGroupMetadata metadata = readRowGroupMetadata()        │ │   │
│  │ │   if (metricsEvaluator.eval(metadata.getColumns())) {      │ │   │
│  │ │     includeRowGroup(rowGroup)                               │ │   │
│  │ │   }                                                         │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ ORC (Stripe Level):                                             │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ For each Stripe:                                            │ │   │
│  │ │   SearchArgument searchArg = convertFilter(filter)          │ │   │
│  │ │   if (searchArg.evaluate(stripeStatistics)) {               │ │   │
│  │ │     includeStripe(stripe)                                   │ │   │
│  │ │   }                                                         │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                      │                                  │
│                                      ▼                                  │
│  Level 6: Page/Row Group Detail Filtering                               │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Fine-Grained Filtering:                                         │   │
│  │                                                                 │   │
│  │ Parquet Page Index (1.12+):                                     │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ For each Data Page:                                         │ │   │
│  │ │   if (pageStatistics.canSkip(filter)) {                    │ │   │
│  │ │     skipPage(page)                                          │ │   │
│  │ │   }                                                         │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ Dictionary Filtering:                                           │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ if (dictionaryFilter.canSkip(dictionary, filter)) {         │ │   │
│  │ │   skipColumnChunk(columnChunk)                              │ │   │
│  │ │ }                                                           │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ Bloom Filter:                                                   │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ if (!bloomFilter.mightContain(filterValue)) {               │ │   │
│  │ │   skipRowGroup(rowGroup)  // Definitely not present         │ │   │
│  │ │ }                                                           │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                      │                                  │
│                                      ▼                                  │
│  Level 7: Row-Level Filtering                                           │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Final Row Processing:                                           │   │
│  │                                                                 │   │
│  │ for (Record record : readRecords()) {                           │   │
│  │   // Apply remaining predicates that couldn't be pushed down    │   │
│  │   if (residualFilter.eval(record)) {                           │   │
│  │     if (!isDeleted(record, deleteFiles)) {                     │   │
│  │       yield record                                              │   │
│  │     }                                                           │   │
│  │   }                                                             │   │
│  │ }                                                               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 13.5 完整读取流程实例

#### 实际读取场景示例

```java
// 完整的 MOR 表读取流程示例
public class CompleteReadFlowExample {
    
    public void demonstrateCompleteReadFlow() {
        // 1. 表扫描设置
        Table table = catalog.loadTable(TableIdentifier.of("warehouse", "orders"));
        
        TableScan scan = table.newScan()
            .filter(Expressions.and(
                Expressions.greaterThan("order_id", 10000),
                Expressions.equal("status", "COMPLETED"),
                Expressions.greaterThan("order_date", "2024-01-01")
            ))
            .select("order_id", "customer_id", "amount", "order_date");
        
        // 2. 执行完整的读取流程
        try (CloseableIterable<CombinedScanTask> tasks = scan.planTasks()) {
            
            for (CombinedScanTask combinedTask : tasks) {
                processCompleteTask(combinedTask);
            }
        }
    }
    
    private void processCompleteTask(CombinedScanTask combinedTask) {
        for (FileScanTask fileTask : combinedTask.files()) {
            
            System.out.printf("Processing Data File: %s\n", 
                            fileTask.file().path());
            System.out.printf("Delete Files Count: %d\n", 
                            fileTask.deletes().length);
            
            // 展示删除文件处理
            for (DeleteFile deleteFile : fileTask.deletes()) {
                System.out.printf("  Delete File: %s, Type: %s\n",
                                deleteFile.path(),
                                deleteFile.content());
            }
            
            // 读取和过滤数据
            try (CloseableIterable<Record> records = 
                 readFileWithDeletes(fileTask)) {
                
                int recordCount = 0;
                for (Record record : records) {
                    recordCount++;
                    // 处理记录...
                }
                
                System.out.printf("Records read: %d\n", recordCount);
            }
        }
    }
    
    private CloseableIterable<Record> readFileWithDeletes(FileScanTask task) {
        // 这里展示了 MOR 表的完整读取流程：
        // 1. 读取数据文件
        // 2. 应用删除文件过滤
        // 3. 应用残留谓词过滤
        // 4. 返回最终结果
        
        return new MORTableReader(
            task.file(), 
            task.deletes(), 
            task.residual(), 
            task.schema()
        ).read();
    }
}
```

## 14. 总结

Apache Iceberg的数据读取机制体现了现代数据湖架构的先进设计理念：

### 13.1 核心优势

1. **多层过滤优化**：从Manifest List到Row Group层层过滤，最大化跳过不相关数据
2. **智能删除处理**：支持多种删除文件类型，MOR和COW模式灵活选择
3. **时间旅行支持**：基于快照的版本管理，支持历史数据查询
4. **增量读取能力**：高效的增量数据处理，支持流式场景
5. **格式无关性**：统一的API支持Parquet、ORC、Avro等格式
6. **统计信息利用**：全面利用文件级和行组级统计信息优化查询

### 13.2 技术特色

1. **延迟计算**：Manifest和删除索引的延迟加载减少内存占用
2. **缓存策略**：多级缓存提升重复查询性能
3. **并行处理**：支持多线程并行规划和执行
4. **表达式优化**：智能的谓词下推和残留条件处理
5. **监控集成**：完整的扫描指标收集和报告

### 13.3 应用场景

1. **批处理分析**：大规模数据分析和报表查询
2. **实时流处理**：增量数据处理和CDC场景
3. **数据血缘追溯**：时间旅行功能支持数据审计
4. **多版本管理**：分支和标签支持复杂的数据管理需求

Apache Iceberg的读取机制为大数据生态系统提供了一个高性能、可扩展的解决方案，其精心设计的多层优化架构使其能够在各种工作负载下保持优异的查询性能。

---

*本文档基于Apache Iceberg 1.9.x版本源码深度分析，涵盖了从metadata到data file的完整数据读取链路。*