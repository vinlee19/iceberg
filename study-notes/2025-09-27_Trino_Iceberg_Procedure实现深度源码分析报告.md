# 2025-09-27_Trino_Iceberg_Procedure实现深度源码分析报告

## 概述

本文档深入分析了 Trino 中 Iceberg connector 的四个核心表维护 procedure 的实现机制：`optimize_manifests`、`expire_snapshots`、`remove_orphan_files` 和 `drop_extended_stats`。通过对源码的详细分析，展现了 Trino 如何通过 Table Execute API 实现分布式表维护操作。

## 1. Trino Iceberg Procedure 架构概览

### 1.1 架构设计

Trino 的 Iceberg procedure 实现采用了与 Spark 完全不同的架构模式：

```
SQL语法: ALTER TABLE table_name EXECUTE procedure_name(parameters)
    ↓
IcebergMetadata.getTableExecuteHandle()
    ↓
IcebergTableExecuteHandle (包装执行上下文)
    ↓
IcebergMetadata.executeTableExecute() (Coordinator执行)
    ↓
具体的 procedure 实现方法
```

### 1.2 核心组件

**IcebergTableProcedureId 枚举** (`IcebergTableProcedureId.java:16`)：
```java
public enum IcebergTableProcedureId {
    OPTIMIZE,
    OPTIMIZE_MANIFESTS,
    DROP_EXTENDED_STATS,
    ROLLBACK_TO_SNAPSHOT,
    EXPIRE_SNAPSHOTS,
    REMOVE_ORPHAN_FILES,
    ADD_FILES,
    ADD_FILES_FROM_TABLE,
}
```

**执行模式**：所有 procedure 都使用 `coordinatorOnly()` 模式执行，即在 Coordinator 节点上串行执行，而非分布式执行。

## 2. optimize_manifests Procedure 深度分析

### 2.1 功能概述

`optimize_manifests` procedure 用于重新组织和优化 manifest 文件，通过合并小的 manifest 文件和按分区聚类来提高查询性能。

### 2.2 实现分析

**Procedure定义** (`OptimizeManifestsTableProcedure.java:23`)：
```java
public class OptimizeManifestsTableProcedure implements Provider<TableProcedureMetadata> {
    @Override
    public TableProcedureMetadata get() {
        return new TableProcedureMetadata(
                OPTIMIZE_MANIFESTS.name(),
                coordinatorOnly(),
                ImmutableList.of()); // 无参数
    }
}
```

**Handle类** (`IcebergOptimizeManifestsHandle.java:16`)：
```java
public record IcebergOptimizeManifestsHandle() implements IcebergProcedureHandle {}
```

**核心执行逻辑** (`IcebergMetadata.java:2099`)：
```java
private void executeOptimizeManifests(ConnectorSession session, IcebergTableExecuteHandle executeHandle) {
    checkArgument(executeHandle.procedureHandle() instanceof IcebergOptimizeManifestsHandle,
                 "Unexpected procedure handle %s", executeHandle.procedureHandle());

    BaseTable icebergTable = catalog.loadTable(session, executeHandle.schemaTableName());
    List<ManifestFile> manifests = loadAllManifestsFromSnapshot(icebergTable, icebergTable.currentSnapshot());

    // 检查是否需要优化
    if (manifests.isEmpty()) {
        return;
    }

    // 如果只有一个manifest且大小小于目标大小，则无需优化
    if (manifests.size() == 1 &&
        manifests.getFirst().length() < icebergTable.operations().current()
            .propertyAsLong(MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT)) {
        return;
    }

    beginTransaction(icebergTable);
    RewriteManifests rewriteManifests = transaction.rewriteManifests();

    // 按第一个分区字段进行聚类
    rewriteManifests.clusterBy(file -> {
        StructLike partition = file.partition();
        return partition.size() > 1 ?
            Optional.ofNullable(partition.get(0, Object.class)) : partition;
    });

    rewriteManifests.commit();
    commitTransaction(transaction, "optimize manifests");
    transaction = null;
}
```

### 2.3 优化策略

1. **智能检查**：只有在需要时才执行优化（多个manifest或单个manifest过大）
2. **分区聚类**：按第一个分区字段对文件进行聚类，提高查询时的裁剪效率
3. **事务性**：使用事务确保操作的原子性

### 2.4 使用示例

```sql
-- 基本用法
ALTER TABLE my_schema.my_table EXECUTE optimize_manifests;

-- 不支持WHERE子句
ALTER TABLE my_schema.my_table EXECUTE optimize_manifests WHERE id = 1; -- ERROR
```

## 3. expire_snapshots Procedure 深度分析

### 3.1 功能概述

`expire_snapshots` procedure 用于删除过期的快照，释放存储空间，是 Iceberg 表维护的关键操作。

### 3.2 实现分析

**Procedure定义** (`ExpireSnapshotsTableProcedure.java:25`)：
```java
public class ExpireSnapshotsTableProcedure implements Provider<TableProcedureMetadata> {
    @Override
    public TableProcedureMetadata get() {
        return new TableProcedureMetadata(
                EXPIRE_SNAPSHOTS.name(),
                coordinatorOnly(),
                ImmutableList.of(
                        durationProperty(
                                "retention_threshold",
                                "Only snapshots older than threshold should be removed",
                                Duration.valueOf("7d"),  // 默认7天
                                false)));
    }
}
```

**Handle类** (`IcebergExpireSnapshotsHandle.java:20`)：
```java
public record IcebergExpireSnapshotsHandle(Duration retentionThreshold)
        implements IcebergProcedureHandle {
    public IcebergExpireSnapshotsHandle {
        requireNonNull(retentionThreshold, "retentionThreshold is null");
    }
}
```

**核心执行逻辑** (`IcebergMetadata.java:2178`)：
```java
private void executeExpireSnapshots(ConnectorSession session, IcebergTableExecuteHandle executeHandle) {
    IcebergExpireSnapshotsHandle expireSnapshotsHandle =
        (IcebergExpireSnapshotsHandle) executeHandle.procedureHandle();

    BaseTable table = catalog.loadTable(session, executeHandle.schemaTableName());
    Duration retention = requireNonNull(expireSnapshotsHandle.retentionThreshold(), "retention is null");

    // 安全性验证：确保retention不低于最小值
    validateTableExecuteParameters(
            table,
            executeHandle.schemaTableName(),
            EXPIRE_SNAPSHOTS.name(),
            retention,
            getExpireSnapshotMinRetention(session),
            IcebergConfig.EXPIRE_SNAPSHOTS_MIN_RETENTION,
            IcebergSessionProperties.EXPIRE_SNAPSHOTS_MIN_RETENTION);

    // ForwardingFileIo 处理批量操作，无需单独的函数实现
    table.expireSnapshots()
            .expireOlderThan(session.getStart().toEpochMilli() - retention.toMillis())
            .planWith(icebergScanExecutor)  // 使用专用的扫描执行器
            .commit();
}
```

### 3.3 安全性保障

1. **最小保留时间检查**：防止意外删除重要快照
2. **配置验证**：支持会话级和表级配置覆盖
3. **计划执行**：使用专用的扫描执行器进行规划

### 3.4 使用示例

```sql
-- 删除7天前的快照（默认）
ALTER TABLE my_schema.my_table EXECUTE expire_snapshots;

-- 删除1天前的快照
ALTER TABLE my_schema.my_table EXECUTE expire_snapshots(retention_threshold => '1d');

-- 删除所有快照（测试用，需要配置允许）
ALTER TABLE my_schema.my_table EXECUTE expire_snapshots(retention_threshold => '0d');
```

## 4. remove_orphan_files Procedure 深度分析

### 4.1 功能概述

`remove_orphan_files` procedure 用于清理表目录中不被任何快照引用的孤儿文件，释放存储空间。

### 4.2 实现分析

**Procedure定义** (`RemoveOrphanFilesTableProcedure.java:25`)：
```java
public class RemoveOrphanFilesTableProcedure implements Provider<TableProcedureMetadata> {
    @Override
    public TableProcedureMetadata get() {
        return new TableProcedureMetadata(
                REMOVE_ORPHAN_FILES.name(),
                coordinatorOnly(),
                ImmutableList.of(
                        durationProperty(
                                "retention_threshold",
                                "Files older than threshold should be removed",
                                Duration.valueOf("7d"),  // 默认7天
                                false)));
    }
}
```

**Handle类** (`IcebergRemoveOrphanFilesHandle.java:20`)：
```java
public record IcebergRemoveOrphanFilesHandle(Duration retentionThreshold)
        implements IcebergProcedureHandle {
    public IcebergRemoveOrphanFilesHandle {
        requireNonNull(retentionThreshold, "retentionThreshold is null");
    }
}
```

**核心执行逻辑** (`IcebergMetadata.java:2248`)：
```java
public void executeRemoveOrphanFiles(ConnectorSession session, IcebergTableExecuteHandle executeHandle) {
    IcebergRemoveOrphanFilesHandle removeOrphanFilesHandle =
        (IcebergRemoveOrphanFilesHandle) executeHandle.procedureHandle();

    BaseTable table = catalog.loadTable(session, executeHandle.schemaTableName());
    Duration retention = requireNonNull(removeOrphanFilesHandle.retentionThreshold(), "retention is null");

    // 参数验证
    validateTableExecuteParameters(
            table,
            executeHandle.schemaTableName(),
            REMOVE_ORPHAN_FILES.name(),
            retention,
            getRemoveOrphanFilesMinRetention(session),
            IcebergConfig.REMOVE_ORPHAN_FILES_MIN_RETENTION,
            IcebergSessionProperties.REMOVE_ORPHAN_FILES_MIN_RETENTION);

    // 空表检查
    if (table.currentSnapshot() == null) {
        log.debug("Skipping remove_orphan_files procedure for empty table %s", table);
        return;
    }

    Instant expiration = session.getStart().minusMillis(retention.toMillis());
    removeOrphanFiles(table, session, executeHandle.schemaTableName(),
                     expiration, executeHandle.fileIoProperties());
}
```

### 4.3 孤儿文件识别算法

**有效文件集合构建** (`IcebergMetadata.java:2272`)：
```java
private void removeOrphanFiles(Table table, ConnectorSession session,
                              SchemaTableName schemaTableName, Instant expiration,
                              Map<String, String> fileIoProperties) {
    Set<String> validFileNames = ConcurrentHashMap.newKeySet();
    List<Future<?>> manifestScanFutures = new ArrayList<>();

    // 1. 扫描所有manifest文件
    List<ManifestFile> allManifests = loadAllManifests(table);
    for (ManifestFile manifest : allManifests) {
        validFileNames.add(fileName(manifest.path()));

        // 并发扫描manifest内容
        manifestScanFutures.add(icebergScanExecutor.submit(() -> {
            try (ManifestReader<? extends ContentFile<?>> manifestReader =
                     readerForManifest(manifest, table)) {
                for (ContentFile<?> contentFile : manifestReader.select(ImmutableList.of("file_path"))) {
                    validFileNames.add(fileName(contentFile.location()));
                }
            }
            catch (IOException | UncheckedIOException e) {
                throw new TrinoException(ICEBERG_FILESYSTEM_ERROR,
                    "Unable to list manifest file content from " + manifest.path(), e);
            }
        }));
    }

    // 2. 添加元数据文件
    metadataFileLocations(table, false).stream()
            .map(IcebergUtil::fileName)
            .forEach(validFileNames::add);

    // 3. 添加统计文件
    statisticsFilesLocations(table).stream()
            .map(IcebergUtil::fileName)
            .forEach(validFileNames::add);

    // 4. 添加版本提示文件
    validFileNames.add("version-hint.text");

    // 等待所有manifest扫描完成
    try {
        manifestScanFutures.forEach(MoreFutures::getFutureValue);
        manifestScanFutures.clear();
    }
    finally {
        manifestScanFutures.forEach(future -> future.cancel(true));
    }

    // 5. 扫描并删除无效文件
    scanAndDeleteInvalidFiles(table, session, schemaTableName, expiration,
                             validFileNames, fileIoProperties);
}
```

**文件删除逻辑** (`IcebergMetadata.java:2372`)：
```java
private void scanAndDeleteInvalidFiles(Table table, ConnectorSession session,
                                      SchemaTableName schemaTableName, Instant expiration,
                                      Set<String> validFiles, Map<String, String> fileIoProperties) {
    List<Future<?>> deleteFutures = new ArrayList<>();
    try {
        List<Location> filesToDelete = new ArrayList<>(DELETE_BATCH_SIZE);
        TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity(), fileIoProperties);

        // 列举表目录下的所有文件
        FileIterator allFiles = fileSystem.listFiles(Location.of(table.location()));
        while (allFiles.hasNext()) {
            FileEntry entry = allFiles.next();

            // 检查文件是否应该删除
            if (entry.lastModified().isBefore(expiration) &&
                !validFiles.contains(entry.location().fileName())) {
                filesToDelete.add(entry.location());

                // 批量删除以提高性能
                if (filesToDelete.size() >= DELETE_BATCH_SIZE) {
                    List<Location> finalFilesToDelete = filesToDelete;
                    deleteFutures.add(icebergFileDeleteExecutor.submit(() ->
                        deleteFiles(finalFilesToDelete, schemaTableName, fileSystem)));
                    filesToDelete = new ArrayList<>(DELETE_BATCH_SIZE);
                }
            }
            else {
                log.debug("%s file retained while removing orphan files %s",
                         entry.location(), schemaTableName.getTableName());
            }
        }

        // 删除剩余文件
        if (!filesToDelete.isEmpty()) {
            log.debug("Deleting files while removing orphan files for table %s %s",
                     schemaTableName, filesToDelete);
            fileSystem.deleteFiles(filesToDelete);
        }

        // 等待所有删除操作完成
        deleteFutures.forEach(MoreFutures::getFutureValue);
    }
    catch (IOException e) {
        throw new TrinoException(ICEBERG_FILESYSTEM_ERROR,
            "Failed to list files during orphan file removal for table: " + schemaTableName, e);
    }
    finally {
        deleteFutures.forEach(future -> future.cancel(true));
    }
}
```

### 4.4 性能优化技术

1. **并发扫描**：使用专用的扫描执行器并发读取manifest内容
2. **批量删除**：按批次删除文件，减少系统调用开销
3. **专用删除执行器**：使用独立的线程池进行文件删除操作
4. **异常处理**：完善的异常处理和资源清理机制

### 4.5 使用示例

```sql
-- 删除7天前的孤儿文件（默认）
ALTER TABLE my_schema.my_table EXECUTE remove_orphan_files;

-- 删除1天前的孤儿文件
ALTER TABLE my_schema.my_table EXECUTE remove_orphan_files(retention_threshold => '1d');
```

## 5. drop_extended_stats Procedure 深度分析

### 5.1 功能概述

`drop_extended_stats` procedure 用于删除表的扩展统计信息（NDV统计、布隆过滤器等），通常在统计信息过期或损坏时使用。

### 5.2 实现分析

**Procedure定义** (`DropExtendedStatsTableProcedure.java:23`)：
```java
public class DropExtendedStatsTableProcedure implements Provider<TableProcedureMetadata> {
    @Override
    public TableProcedureMetadata get() {
        return new TableProcedureMetadata(
                DROP_EXTENDED_STATS.name(),
                coordinatorOnly(),
                ImmutableList.of()); // 无参数
    }
}
```

**Handle类** (`IcebergDropExtendedStatsHandle.java:16`)：
```java
public record IcebergDropExtendedStatsHandle() implements IcebergProcedureHandle {}
```

**核心执行逻辑** (`IcebergMetadata.java:2137`)：
```java
private void executeDropExtendedStats(ConnectorSession session, IcebergTableExecuteHandle executeHandle) {
    checkArgument(executeHandle.procedureHandle() instanceof IcebergDropExtendedStatsHandle,
                 "Unexpected procedure handle %s", executeHandle.procedureHandle());

    Table icebergTable = catalog.loadTable(session, executeHandle.schemaTableName());

    beginTransaction(icebergTable);
    UpdateStatistics updateStatistics = transaction.updateStatistics();

    // 删除所有统计文件
    for (StatisticsFile statisticsFile : icebergTable.statisticsFiles()) {
        updateStatistics.removeStatistics(statisticsFile.snapshotId());
    }

    updateStatistics.commit();
    commitTransaction(transaction, "drop extended stats");
    transaction = null;
}
```

### 5.3 特点

1. **简单直接**：直接删除所有扩展统计信息
2. **事务性**：使用事务确保操作原子性
3. **无参数**：不需要任何参数

### 5.4 使用示例

```sql
-- 删除所有扩展统计信息
ALTER TABLE my_schema.my_table EXECUTE drop_extended_stats;
```

## 5. optimize (rewrite_data_files) Procedure 深度分析

### 5.1 功能概述

`optimize` procedure（对应 Spark 中的 `rewrite_data_files`）用于重写和优化数据文件，合并小文件、消除删除文件，提高查询性能。这是唯一一个**分布式执行**的 procedure。

### 5.2 实现分析

**Procedure定义** (`OptimizeTableProcedure.java:25`)：
```java
public class OptimizeTableProcedure implements Provider<TableProcedureMetadata> {
    @Override
    public TableProcedureMetadata get() {
        return new TableProcedureMetadata(
                OPTIMIZE.name(),
                distributedWithFilteringAndRepartitioning(), // 分布式执行！
                ImmutableList.of(
                        dataSizeProperty(
                                "file_size_threshold",
                                "Only compact files smaller than given threshold in bytes",
                                DataSize.of(100, DataSize.Unit.MEGABYTE), // 默认100MB
                                false)));
    }
}
```

**Handle类** (`IcebergOptimizeHandle.java:29`)：
```java
public record IcebergOptimizeHandle(
        Optional<Long> snapshotId,
        String schemaAsJson,
        String partitionSpecAsJson,
        List<IcebergColumnHandle> tableColumns,
        List<TrinoSortField> sortOrder,
        IcebergFileFormat fileFormat,
        Map<String, String> tableStorageProperties,
        DataSize maxScannedFileSize,
        boolean retriesEnabled)
        implements IcebergProcedureHandle
```

### 5.3 执行流程

**第一阶段：准备和开始** (`IcebergMetadata.java:1957`)：
```java
private BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginOptimize(
        ConnectorSession session,
        IcebergTableExecuteHandle executeHandle,
        IcebergTableHandle table) {

    IcebergOptimizeHandle optimizeHandle = (IcebergOptimizeHandle) executeHandle.procedureHandle();
    BaseTable icebergTable = catalog.loadTable(session, table.getSchemaTableName());

    // 验证不是在修改旧快照
    validateNotModifyingOldSnapshot(table, icebergTable);

    // 检查表格式版本兼容性
    int tableFormatVersion = formatVersion(icebergTable);
    if (tableFormatVersion > OPTIMIZE_MAX_SUPPORTED_TABLE_VERSION) {
        throw new TrinoException(NOT_SUPPORTED, format(
                "%s is not supported for Iceberg table format version > %d. Table %s format version is %s.",
                OPTIMIZE.name(),
                OPTIMIZE_MAX_SUPPORTED_TABLE_VERSION,
                table.getSchemaTableName(),
                tableFormatVersion));
    }

    // 开始事务
    beginTransaction(icebergTable);

    // 返回用于优化的表句柄
    return new BeginTableExecuteResult<>(
            executeHandle,
            table.forOptimize(true, optimizeHandle.maxScannedFileSize()));
}
```

**第二阶段：分布式扫描和处理**

通过 `distributedWithFilteringAndRepartitioning()` 模式，Trino 会：

1. **扫描候选文件**：
   - 只扫描小于 `file_size_threshold` 的文件
   - 支持 WHERE 子句进行分区裁剪
   - 记录扫描的数据文件和删除文件

2. **分布式重分区**：
   ```java
   private Optional<ConnectorTableLayout> getLayoutForOptimize(ConnectorSession session, IcebergTableExecuteHandle executeHandle) {
       Table icebergTable = catalog.loadTable(session, executeHandle.schemaTableName());
       // 从性能角度看，少量大文件比大量小文件更好
       // 因此我们强制重分区以实现这一点
       return getWriteLayout(icebergTable.schema(), icebergTable.spec(), true);
   }
   ```

3. **工作节点处理**：
   - 读取分配的文件
   - 根据排序字段重新组织数据
   - 写入新的优化文件

**第三阶段：提交结果** (`IcebergMetadata.java:1981`)：
```java
private void finishOptimize(ConnectorSession session,
                           IcebergTableExecuteHandle executeHandle,
                           Collection<Slice> fragments,
                           List<Object> splitSourceInfo) {
    IcebergOptimizeHandle optimizeHandle = (IcebergOptimizeHandle) executeHandle.procedureHandle();
    Table icebergTable = transaction.table();
    Optional<Long> beforeWriteSnapshotId = getCurrentSnapshotId(icebergTable);

    // 收集要删除的文件
    ImmutableSet.Builder<DataFile> scannedDataFilesBuilder = ImmutableSet.builder();
    ImmutableSet.Builder<DeleteFile> scannedDeleteFilesBuilder = ImmutableSet.builder();

    splitSourceInfo.stream().map(DataFileWithDeleteFiles.class::cast).forEach(dataFileWithDeleteFiles -> {
        scannedDataFilesBuilder.add(dataFileWithDeleteFiles.dataFile());
        scannedDeleteFilesBuilder.addAll(dataFileWithDeleteFiles.deleteFiles());
    });

    Set<DataFile> scannedDataFiles = scannedDataFilesBuilder.build();
    Set<DeleteFile> fullyAppliedDeleteFiles = scannedDeleteFilesBuilder.build();

    // 解析新文件信息
    List<CommitTaskData> commitTasks = fragments.stream()
            .map(Slice::getInput)
            .map(commitTaskCodec::fromJson)
            .collect(toImmutableList());

    // 构建新文件对象
    Type[] partitionColumnTypes = icebergTable.spec().fields().stream()
            .map(field -> field.transform().getResultType(
                    icebergTable.schema().findType(field.sourceId())))
            .toArray(Type[]::new);

    Set<DataFile> newFiles = new HashSet<>();
    for (CommitTaskData task : commitTasks) {
        DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                .withPath(task.path())
                .withFileSizeInBytes(task.fileSizeInBytes())
                .withFormat(optimizeHandle.fileFormat().toIceberg())
                .withMetrics(task.metrics().metrics());

        task.fileSplitOffsets().ifPresent(builder::withSplitOffsets);

        // 处理分区信息
        if (!icebergTable.spec().fields().isEmpty()) {
            String partitionDataJson = task.partitionDataJson()
                    .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
            builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
        }

        newFiles.add(builder.build());
    }

    // 检查是否有内容需要提交
    if (optimizeHandle.snapshotId().isEmpty() ||
        scannedDataFiles.isEmpty() && fullyAppliedDeleteFiles.isEmpty() && newFiles.isEmpty()) {
        // 表为空或扫描结果为空，无需提交
        transaction = null;
        return;
    }

    // 清理额外的输出文件（重试时）
    if (optimizeHandle.retriesEnabled()) {
        cleanExtraOutputFiles(
                session,
                newFiles.stream()
                        .map(ContentFile::location)
                        .collect(toImmutableSet()));
    }

    // 执行文件重写
    RewriteFiles rewriteFiles = transaction.newRewrite();
    scannedDataFiles.forEach(rewriteFiles::deleteFile);
    fullyAppliedDeleteFiles.forEach(rewriteFiles::deleteFile);
    newFiles.forEach(rewriteFiles::addFile);

    // 设置序列号避免与并发写入冲突
    Snapshot snapshot = requireNonNull(icebergTable.snapshot(optimizeHandle.snapshotId().get()), "snapshot is null");
    rewriteFiles.dataSequenceNumber(snapshot.sequenceNumber());
    rewriteFiles.validateFromSnapshot(snapshot.snapshotId());
    rewriteFiles.scanManifestsWith(icebergScanExecutor);

    // 提交重写操作
    commitUpdateAndTransaction(rewriteFiles, session, transaction, "optimize");

    long newSnapshotId = transaction.table().currentSnapshot().snapshotId();
    transaction = null;

    // 更新统计信息
    beforeWriteSnapshotId.ifPresent(previous ->
            verify(previous != newSnapshotId, "Failed to get new snapshot ID"));

    try {
        beginTransaction(catalog.loadTable(session, executeHandle.schemaTableName()));
        Table reloadedTable = transaction.table();
        StatisticsFile newStatsFile = tableStatisticsWriter.rewriteStatisticsFile(session, reloadedTable, newSnapshotId);

        transaction.updateStatistics()
                .setStatistics(newStatsFile)
                .commit();
        commitTransaction(transaction, "update statistics after optimize");
    }
    catch (Exception e) {
        // 写入已提交，此时不能失败查询
        log.error(e, "Failed to save table statistics");
    }
    transaction = null;
}
```

### 5.4 关键技术特点

1. **分布式执行**：
   - 唯一使用 `distributedWithFilteringAndRepartitioning()` 的procedure
   - 利用 Trino 集群的并行处理能力
   - 支持分区裁剪和过滤下推

2. **智能文件选择**：
   - 只处理小于阈值的文件（默认100MB）
   - 避免重新处理已经优化的大文件
   - 支持 WHERE 子句进行精确控制

3. **事务安全性**：
   - 使用快照验证防止并发冲突
   - 设置正确的数据序列号
   - 原子性地删除旧文件和添加新文件

4. **统计信息维护**：
   - 自动重新计算统计信息
   - 即使统计更新失败也不影响数据重写
   - 确保查询优化器有准确信息

5. **错误恢复**：
   - 支持重试机制
   - 清理重试时产生的额外文件
   - 完善的异常处理

### 5.5 性能优化技术

1. **重分区策略**：
   ```java
   // 强制重分区以获得更好的文件大小分布
   return getWriteLayout(icebergTable.schema(), icebergTable.spec(), true);
   ```

2. **并发扫描**：
   ```java
   rewriteFiles.scanManifestsWith(icebergScanExecutor);
   ```

3. **批量操作**：
   - 批量删除旧文件
   - 批量添加新文件
   - 原子性提交

### 5.6 使用示例

```sql
-- 基本优化（处理所有小于100MB的文件）
ALTER TABLE my_schema.my_table EXECUTE optimize;

-- 使用自定义阈值
ALTER TABLE my_schema.my_table EXECUTE optimize(file_size_threshold => '50MB');

-- 只优化特定分区
ALTER TABLE my_schema.my_table EXECUTE optimize WHERE partition_col = 'value';

-- 组合条件
ALTER TABLE my_schema.my_table EXECUTE optimize(file_size_threshold => '200MB')
WHERE year = 2023 AND month >= 6;
```

### 5.7 监控和诊断

```sql
-- 查看优化前的文件分布
SELECT file_size_in_bytes, count(*) as file_count
FROM "my_table$files"
GROUP BY 1 ORDER BY 1;

-- 查看分区级文件统计
SELECT partition, count(*) as file_count,
       sum(file_size_in_bytes) as total_size,
       avg(file_size_in_bytes) as avg_size
FROM "my_table$files"
GROUP BY partition;
```

## 6. 与Spark实现的对比分析

### 6.1 架构差异

| 维度 | Trino实现 | Spark实现 |
|------|-----------|-----------|
| **执行模式** | Coordinator Only (除optimize外) | 分布式执行 |
| **语法** | `ALTER TABLE ... EXECUTE` | `CALL system.procedure()` |
| **并行性** | optimize分布式，其他串行 | 分布式并行 |
| **资源利用** | optimize用集群，其他单节点 | 集群 |
| **适用场景** | 灵活适配不同规模 | 大型表 |

### 6.2 实现方式差异

**Trino方式**：
- 使用 Table Execute API
- **optimize**: 分布式执行，支持过滤下推
- **其他**: 在 Coordinator 上执行
- 直接调用 Iceberg 原生 API
- 简单直接，资源消耗可控

**Spark方式**：
- 使用 Procedure API
- 统一分布式执行
- 使用 Spark Dataset 进行计算
- 复杂但可处理大规模数据

### 6.3 性能特点

**Trino优势**：
- 启动开销小
- 适合频繁的小型维护操作
- 资源消耗可预测

**Spark优势**：
- 处理大规模数据能力强
- 可以利用集群资源
- 支持复杂的数据变换

## 6.4 Procedure执行模式对比

| Procedure | Trino执行模式 | Spark执行模式 | 适用场景 |
|-----------|--------------|---------------|----------|
| **optimize** | 分布式+过滤下推 | 分布式 | 大规模数据文件优化 |
| **optimize_manifests** | Coordinator Only | 分布式 | Manifest文件优化 |
| **expire_snapshots** | Coordinator Only | 分布式 | 快照清理 |
| **remove_orphan_files** | Coordinator Only | 分布式 | 孤儿文件清理 |
| **drop_extended_stats** | Coordinator Only | 不支持 | 统计信息清理 |

**Trino的设计哲学**：
- 根据操作复杂度选择执行模式
- optimize 需要处理大量数据，采用分布式
- 其他维护操作数据量小，Coordinator执行即可
- 平衡了性能和复杂度

## 7. 配置和调优

### 7.1 重要配置参数

**过期快照最小保留时间**：
```properties
# iceberg.properties
iceberg.expire-snapshots.min-retention=1d
```

**删除孤儿文件最小保留时间**：
```properties
# iceberg.properties
iceberg.remove-orphan-files.min-retention=1d
```

**删除批次大小**：
```java
private static final int DELETE_BATCH_SIZE = 1000;
```

### 7.2 会话级配置

```sql
-- 设置会话级最小保留时间
SET SESSION iceberg.expire_snapshots_min_retention = '2d';
SET SESSION iceberg.remove_orphan_files_min_retention = '2d';
```

### 7.3 执行器配置

Trino使用专门的执行器进行不同操作：

- `icebergScanExecutor`：用于扫描操作
- `icebergFileDeleteExecutor`：用于文件删除操作

## 8. 最佳实践和建议

### 8.1 操作顺序建议

```sql
-- 1. 先优化数据文件（分布式执行，可并行处理大量数据）
ALTER TABLE my_table EXECUTE optimize(file_size_threshold => '100MB');

-- 2. 再优化manifest文件（Coordinator执行，快速完成）
ALTER TABLE my_table EXECUTE optimize_manifests;

-- 3. 删除过期快照（Coordinator执行，删除元数据和文件）
ALTER TABLE my_table EXECUTE expire_snapshots(retention_threshold => '7d');

-- 4. 清理孤儿文件（Coordinator执行，但可能耗时较长）
ALTER TABLE my_table EXECUTE remove_orphan_files(retention_threshold => '7d');

-- 5. 清理过期统计（可选，Coordinator执行，快速完成）
ALTER TABLE my_table EXECUTE drop_extended_stats;
```

**执行建议**：
- optimize 可以并发在不同分区上执行
- 其他操作建议顺序执行，避免冲突
- 在低峰期执行维护操作

### 8.2 安全注意事项

1. **保留时间设置**：确保保留时间足够长，避免删除正在使用的文件
2. **并发操作**：避免在表写入过程中执行维护操作
3. **备份重要数据**：在执行破坏性操作前确保有备份
4. **监控执行时间**：大表的维护操作可能需要很长时间

### 8.3 监控和调试

```sql
-- 查看表的快照历史
SELECT * FROM "my_table$snapshots";

-- 查看manifest文件信息
SELECT * FROM "my_table$manifests";

-- 查看文件列表
SELECT * FROM "my_table$files";
```

## 9. 错误处理和故障排除

### 9.1 常见错误

1. **保留时间过短**：
   ```
   Retention specified (PT1H) is shorter than the minimum retention configured (P1D)
   ```

2. **表不存在**：
   ```
   Table 'catalog.schema.table' not found
   ```

3. **权限不足**：
   ```
   Access Denied: Cannot execute table procedure on table
   ```

### 9.2 故障排除步骤

1. **检查表状态**：确认表存在且可访问
2. **验证权限**：确认有足够的执行权限
3. **检查配置**：验证保留时间配置
4. **查看日志**：检查 Coordinator 日志中的详细错误信息

## 10. 结论

Trino 的 Iceberg procedure 实现采用了简洁而有效的架构设计：

### 10.1 核心特点

1. **Coordinator执行模式**：所有操作在Coordinator节点执行，简化了分布式协调
2. **Table Execute API**：使用标准的SQL语法，与表操作语义一致
3. **直接调用Iceberg API**：没有额外的抽象层，性能开销小
4. **完善的安全机制**：内置参数验证和最小保留时间检查

### 10.2 适用场景

**适合使用Trino实现的场景**：
- 中小型表的日常维护
- 频繁的快速维护操作
- 资源受限的环境
- 需要简单直接的操作语义

**考虑使用Spark实现的场景**：
- 大型表（TB级别以上）
- 需要分布式处理能力
- 复杂的数据转换需求
- 有充足的集群资源

### 10.3 发展方向

1. **性能优化**：可以考虑在某些操作中引入并行处理
2. **功能增强**：支持更多的自定义参数和策略
3. **监控改进**：提供更详细的执行指标和进度信息
4. **资源管理**：更好的资源控制和限制机制

Trino的Iceberg procedure实现体现了"简单即美"的设计哲学，通过合理的架构设计在功能完整性和实现复杂度之间找到了良好的平衡点。