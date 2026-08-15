# 2025-10-22 Apache Iceberg数据读取机制完整源码深度分析报告

## 文档概述

本文档基于Apache Iceberg 1.10.x版本源码,深度剖析从Metadata到ScanTask生成的完整数据读取链路,涵盖ManifestGroup核心作用、多层过滤机制、统计信息优化、索引拦截以及扫描任务生成全流程。

---

## 一、Iceberg物理存储结构

### 1.1 元数据层级架构

```
┌─────────────────────────────────────────────────────────────┐
│                      Table Metadata                         │
│                   (metadata.json)                           │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ - current_snapshot_id                               │   │
│  │ - schemas[]                                         │   │
│  │ - partition_specs[]                                 │   │
│  │ - snapshots[]                                       │   │
│  │ - properties                                        │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                       Snapshot                              │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ - snapshot_id: 3051729675574597004                  │   │
│  │ - timestamp_ms: 1697520000000                       │   │
│  │ - manifest_list: s3://bucket/table/snap-xxx.avro    │   │
│  │ - summary: {total-data-files: 100, ...}             │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                    Manifest List                            │
│                 (snap-xxx.avro)                             │
│  ┌────────────────────────────────────────────────────┐    │
│  │ ManifestFile 1 (DATA)                              │    │
│  │  - manifest_path: s3://.../manifest-m0.avro        │    │
│  │  - partition_spec_id: 0                            │    │
│  │  - content: DATA                                   │    │
│  │  - added_files_count: 10                           │    │
│  │  - existing_files_count: 20                        │    │
│  │  - deleted_files_count: 5                          │    │
│  │  - partitions: [                                   │    │
│  │      {contains_null: false,                        │    │
│  │       contains_nan: false,                         │    │
│  │       lower_bound: "2023-01-01",                   │    │
│  │       upper_bound: "2023-12-31"}                   │    │
│  │    ]                                               │    │
│  ├────────────────────────────────────────────────────┤    │
│  │ ManifestFile 2 (DELETES)                           │    │
│  │  - manifest_path: s3://.../manifest-m1.avro        │    │
│  │  - content: DELETES                                │    │
│  │  ...                                               │    │
│  └────────────────────────────────────────────────────┘    │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                    Manifest File                            │
│                (manifest-m0.avro)                           │
│  ┌────────────────────────────────────────────────────┐    │
│  │ ManifestEntry 1                                    │    │
│  │  - status: ADDED                                   │    │
│  │  - snapshot_id: 3051729675574597004                │    │
│  │  - data_sequence_number: 1                         │    │
│  │  - file_sequence_number: 1                         │    │
│  │  - data_file: {                                    │    │
│  │      file_path: s3://.../data/file-1.parquet       │    │
│  │      file_format: PARQUET                          │    │
│  │      partition: {dt: "2023-01-01"}                 │    │
│  │      record_count: 1000000                         │    │
│  │      file_size_in_bytes: 134217728                 │    │
│  │      column_sizes: {1: 10000, 2: 20000, ...}       │    │
│  │      value_counts: {1: 1000000, 2: 1000000, ...}   │    │
│  │      null_value_counts: {1: 0, 2: 100, ...}        │    │
│  │      nan_value_counts: {3: 0, ...}                 │    │
│  │      lower_bounds: {1: "A", 2: 0x00...}            │    │
│  │      upper_bounds: {1: "Z", 2: 0xFF...}            │    │
│  │      split_offsets: [4194304, 8388608, ...]        │    │
│  │    }                                               │    │
│  ├────────────────────────────────────────────────────┤    │
│  │ ManifestEntry 2 (status: EXISTING)                 │    │
│  │ ManifestEntry 3 (status: DELETED)                  │    │
│  │ ...                                                │    │
│  └────────────────────────────────────────────────────┘    │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                     Data Files                              │
│  ┌────────────────────────────────────────────────────┐    │
│  │ file-1.parquet                                     │    │
│  │  - Parquet Footer with Column Statistics           │    │
│  │  - Row Group 1 (rows 0-100000)                     │    │
│  │  - Row Group 2 (rows 100001-200000)                │    │
│  │  ...                                               │    │
│  └────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### 1.2 统计信息金字塔

Iceberg的统计信息采用多层级设计,支持渐进式数据过滤:

```
Level 1: Manifest List (Partition-level Summary)
  └─ ManifestFile.partitions[]
      ├─ contains_null: boolean
      ├─ contains_nan: boolean
      ├─ lower_bound: ByteBuffer
      └─ upper_bound: ByteBuffer

Level 2: Manifest File (File-level Metrics)
  └─ DataFile metadata
      ├─ record_count: long
      ├─ value_counts: Map<Integer, Long>
      ├─ null_value_counts: Map<Integer, Long>
      ├─ nan_value_counts: Map<Integer, Long>
      ├─ lower_bounds: Map<Integer, ByteBuffer>
      └─ upper_bounds: Map<Integer, ByteBuffer>

Level 3: Data File (Parquet/ORC internal stats)
  └─ Row Group / Stripe statistics
      ├─ min/max values per column
      ├─ null counts
      └─ distinct counts (optional)
```

---

## 二、数据读取完整流程架构

### 2.1 读取计划生成流程图

```
┌──────────────────────────────────────────────────────────────────┐
│                    1. TableScan 初始化                           │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ DataTableScan scan = table.newScan()                       │  │
│  │   .useSnapshot(snapshotId)                                 │  │
│  │   .filter(Expressions.equal("region", "us-west"))          │  │
│  │   .select("id", "name", "timestamp")                       │  │
│  │   .option("split-size", "134217728")                       │  │
│  └────────────────────────────────────────────────────────────┘  │
│  Entry Point: DataTableScan.doPlanFiles()                       │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│            2. 加载Snapshot元数据 (DataTableScan:64-69)           │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ Snapshot snapshot = snapshot();                            │  │
│  │ FileIO io = table().io();                                  │  │
│  │ List<ManifestFile> dataManifests = snapshot.dataManifests(io);│
│  │ List<ManifestFile> deleteManifests = snapshot.deleteManifests(io);│
│  │ // 更新扫描指标                                             │  │
│  │ scanMetrics().totalDataManifests().increment(dataManifests.size());│
│  │ scanMetrics().totalDeleteManifests().increment(deleteManifests.size());│
│  └────────────────────────────────────────────────────────────┘  │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│          3. ManifestGroup 构建与配置 (DataTableScan:72-88)       │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ ManifestGroup manifestGroup =                              │  │
│  │   new ManifestGroup(io, dataManifests, deleteManifests)    │  │
│  │     .caseSensitive(isCaseSensitive())                      │  │
│  │     .select(scanColumns())           // 列裁剪            │  │
│  │     .filterData(filter())            // 数据过滤表达式     │  │
│  │     .specsById(table().specs())      // PartitionSpec映射  │  │
│  │     .scanMetrics(scanMetrics())                            │  │
│  │     .ignoreDeleted()                 // 忽略已删除条目     │  │
│  │     .columnsToKeepStats(columnsToKeepStats())              │  │
│  │     .ignoreResiduals()               // 可选,忽略剩余谓词  │  │
│  │     .planWith(planExecutor());       // 并行扫描          │  │
│  └────────────────────────────────────────────────────────────┘  │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│  4. ManifestGroup.planFiles() - 启动扫描任务生成 (ManifestGroup:171-214)│
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ Step 4.1: 构建 DeleteFileIndex                             │  │
│  │ ┌────────────────────────────────────────────────────────┐ │  │
│  │ │ DeleteFileIndex deleteFiles =                          │ │  │
│  │ │   deleteIndexBuilder                                   │ │  │
│  │ │     .scanMetrics(scanMetrics)                          │ │  │
│  │ │     .build();                                          │ │  │
│  │ │                                                        │ │  │
│  │ │ 内部流程:                                               │ │  │
│  │ │ 1. 过滤delete manifests (ManifestEvaluator)            │ │  │
│  │ │ 2. 读取delete files并构建索引:                         │ │  │
│  │ │    - globalDeletes: EqualityDeletes (无分区)           │ │  │
│  │ │    - eqDeletesByPartition: 分区级Equality Deletes      │ │  │
│  │ │    - posDeletesByPartition: 分区级Position Deletes     │ │  │
│  │ │    - posDeletesByPath: 基于文件路径的Position Deletes  │ │  │
│  │ │    - dvByPath: Deletion Vectors (Puffin格式)           │ │  │
│  │ └────────────────────────────────────────────────────────┘ │  │
│  │                                                            │  │
│  │ Step 4.2: 创建ResidualEvaluator缓存                        │  │
│  │ ┌────────────────────────────────────────────────────────┐ │  │
│  │ │ LoadingCache<Integer, ResidualEvaluator> residualCache=│ │  │
│  │ │   Caffeine.newBuilder().build(                         │ │  │
│  │ │     specId -> {                                        │ │  │
│  │ │       PartitionSpec spec = specsById.get(specId);      │ │  │
│  │ │       Expression filter = ignoreResiduals ?            │ │  │
│  │ │         Expressions.alwaysTrue() : dataFilter;         │ │  │
│  │ │       return ResidualEvaluator.of(spec, filter, caseSensitive);│ │  │
│  │ │     });                                                │ │  │
│  │ └────────────────────────────────────────────────────────┘ │  │
│  │                                                            │  │
│  │ Step 4.3: 创建TaskContext缓存                              │  │
│  │ ┌────────────────────────────────────────────────────────┐ │  │
│  │ │ LoadingCache<Integer, TaskContext> taskContextCache =  │ │  │
│  │ │   Caffeine.newBuilder().build(                         │ │  │
│  │ │     specId -> new TaskContext(                         │ │  │
│  │ │       specsById.get(specId),                           │ │  │
│  │ │       deleteFiles,                                     │ │  │
│  │ │       residualCache.get(specId),                       │ │  │
│  │ │       dropStats,                                       │ │  │
│  │ │       columnsToKeepStats,                              │ │  │
│  │ │       scanMetrics)                                     │ │  │
│  │ │   );                                                   │ │  │
│  │ └────────────────────────────────────────────────────────┘ │  │
│  │                                                            │  │
│  │ Step 4.4: 生成FileScanTask迭代器                           │  │
│  │ ┌────────────────────────────────────────────────────────┐ │  │
│  │ │ Iterable<CloseableIterable<FileScanTask>> tasks =      │ │  │
│  │ │   entries(                                             │ │  │
│  │ │     (manifest, entries) -> {                           │ │  │
│  │ │       int specId = manifest.partitionSpecId();         │ │  │
│  │ │       TaskContext ctx = taskContextCache.get(specId);  │ │  │
│  │ │       return createFileScanTasks(entries, ctx);        │ │  │
│  │ │     }                                                  │ │  │
│  │ │   );                                                   │ │  │
│  │ └────────────────────────────────────────────────────────┘ │  │
│  │                                                            │  │
│  │ Step 4.5: 并行 vs 串行执行                                 │  │
│  │ ┌────────────────────────────────────────────────────────┐ │  │
│  │ │ if (executorService != null) {                         │ │  │
│  │ │   return new ParallelIterable<>(tasks, executorService);│ │  │
│  │ │ } else {                                               │ │  │
│  │ │   return CloseableIterable.concat(tasks);              │ │  │
│  │ │ }                                                      │ │  │
│  │ └────────────────────────────────────────────────────────┘ │  │
│  └────────────────────────────────────────────────────────────┘  │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│  5. ManifestGroup.entries() - Manifest级别过滤 (ManifestGroup:242-356)│
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ Step 5.1: 创建ManifestEvaluator缓存                        │  │
│  │ ┌────────────────────────────────────────────────────────┐ │  │
│  │ │ LoadingCache<Integer, ManifestEvaluator> evalCache =   │ │  │
│  │ │   Caffeine.newBuilder().build(                         │ │  │
│  │ │     specId -> {                                        │ │  │
│  │ │       PartitionSpec spec = specsById.get(specId);      │ │  │
│  │ │       // 投影数据过滤器到分区字段                        │ │  │
│  │ │       Expression projected =                           │ │  │
│  │ │         Projections.inclusive(spec, caseSensitive)     │ │  │
│  │ │           .project(dataFilter);                        │ │  │
│  │ │       return ManifestEvaluator.forPartitionFilter(     │ │  │
│  │ │         Expressions.and(partitionFilter, projected),   │ │  │
│  │ │         spec, caseSensitive);                          │ │  │
│  │ │     });                                                │ │  │
│  │ └────────────────────────────────────────────────────────┘ │  │
│  │                                                            │  │
│  │ Step 5.2: 创建文件级Evaluator (如果有fileFilter)            │  │
│  │ ┌────────────────────────────────────────────────────────┐ │  │
│  │ │ Evaluator evaluator = null;                            │ │  │
│  │ │ if (fileFilter != null && fileFilter != alwaysTrue()) {│ │  │
│  │ │   evaluator = new Evaluator(                           │ │  │
│  │ │     DataFile.getType(EMPTY_STRUCT),                    │ │  │
│  │ │     fileFilter,                                        │ │  │
│  │ │     caseSensitive);                                    │ │  │
│  │ │ }                                                      │ │  │
│  │ └────────────────────────────────────────────────────────┘ │  │
│  │                                                            │  │
│  │ Step 5.3: 过滤Data Manifests                               │  │
│  │ ┌────────────────────────────────────────────────────────┐ │  │
│  │ │ CloseableIterable<ManifestFile> matchingManifests =    │ │  │
│  │ │   evalCache == null                                    │ │  │
│  │ │     ? closeableDataManifests                           │ │  │
│  │ │     : CloseableIterable.filter(                        │ │  │
│  │ │         scanMetrics.skippedDataManifests(),            │ │  │
│  │ │         closeableDataManifests,                        │ │  │
│  │ │         manifest -> evalCache.get(manifest.partitionSpecId())│ │  │
│  │ │                       .eval(manifest));                │ │  │
│  │ │                                                        │ │  │
│  │ │ // 示例: ManifestEvaluator.eval()                      │ │  │
│  │ │ // 检查 manifest.partitions[] 的 lower/upper bounds    │ │  │
│  │ │ // 如果所有分区都不满足过滤条件,跳过整个manifest         │ │  │
│  │ └────────────────────────────────────────────────────────┘ │  │
│  │                                                            │  │
│  │ Step 5.4: 应用ignoreDeleted和ignoreExisting过滤            │  │
│  │ ┌────────────────────────────────────────────────────────┐ │  │
│  │ │ if (ignoreDeleted) {                                   │ │  │
│  │ │   matchingManifests = CloseableIterable.filter(        │ │  │
│  │ │     scanMetrics.skippedDataManifests(),                │ │  │
│  │ │     matchingManifests,                                 │ │  │
│  │ │     m -> m.hasAddedFiles() || m.hasExistingFiles());   │ │  │
│  │ │ }                                                      │ │  │
│  │ │ if (ignoreExisting) {                                  │ │  │
│  │ │   matchingManifests = CloseableIterable.filter(        │ │  │
│  │ │     scanMetrics.skippedDataManifests(),                │ │  │
│  │ │     matchingManifests,                                 │ │  │
│  │ │     m -> m.hasAddedFiles() || m.hasDeletedFiles());    │ │  │
│  │ │ }                                                      │ │  │
│  │ └────────────────────────────────────────────────────────┘ │  │
│  │                                                            │  │
│  │ Step 5.5: 遍历匹配的Manifests,读取并过滤ManifestEntry       │  │
│  │ ┌────────────────────────────────────────────────────────┐ │  │
│  │ │ return Iterables.transform(matchingManifests,          │ │  │
│  │ │   manifest -> new CloseableIterable<T>() {             │ │  │
│  │ │     @Override                                          │ │  │
│  │ │     public CloseableIterator<T> iterator() {           │ │  │
│  │ │       // 创建ManifestReader                            │ │  │
│  │ │       ManifestReader<DataFile> reader =                │ │  │
│  │ │         ManifestFiles.read(manifest, io, specsById)    │ │  │
│  │ │           .filterRows(dataFilter)    // 行级过滤       │ │  │
│  │ │           .filterPartitions(partitionFilter)           │ │  │
│  │ │           .caseSensitive(caseSensitive)                │ │  │
│  │ │           .select(columns)           // 列选择         │ │  │
│  │ │           .scanMetrics(scanMetrics);                   │ │  │
│  │ │                                                        │ │  │
│  │ │       // 读取条目                                      │ │  │
│  │ │       CloseableIterable<ManifestEntry<DataFile>> entries;│ │  │
│  │ │       if (ignoreDeleted) {                             │ │  │
│  │ │         entries = reader.liveEntries();                │ │  │
│  │ │       } else {                                         │ │  │
│  │ │         entries = reader.entries();                    │ │  │
│  │ │       }                                                │ │  │
│  │ │                                                        │ │  │
│  │ │       // 应用Entry级别过滤                              │ │  │
│  │ │       if (ignoreExisting) {                            │ │  │
│  │ │         entries = CloseableIterable.filter(            │ │  │
│  │ │           scanMetrics.skippedDataFiles(),              │ │  │
│  │ │           entries,                                     │ │  │
│  │ │           entry -> entry.status() != EXISTING);        │ │  │
│  │ │       }                                                │ │  │
│  │ │       if (evaluator != null) {                         │ │  │
│  │ │         entries = CloseableIterable.filter(            │ │  │
│  │ │           scanMetrics.skippedDataFiles(),              │ │  │
│  │ │           entries,                                     │ │  │
│  │ │           entry -> evaluator.eval(entry.file()));      │ │  │
│  │ │       }                                                │ │  │
│  │ │       entries = CloseableIterable.filter(              │ │  │
│  │ │         scanMetrics.skippedDataFiles(),                │ │  │
│  │ │         entries,                                       │ │  │
│  │ │         manifestEntryPredicate);                       │ │  │
│  │ │                                                        │ │  │
│  │ │       // 转换为目标类型 (FileScanTask)                  │ │  │
│  │ │       iterable = entryFn.apply(manifest, entries);     │ │  │
│  │ │       return iterable.iterator();                      │ │  │
│  │ │     }                                                  │ │  │
│  │ │   }                                                    │ │  │
│  │ │ );                                                     │ │  │
│  │ └────────────────────────────────────────────────────────┘ │  │
│  └────────────────────────────────────────────────────────────┘  │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│  6. ManifestReader详细过滤流程 (ManifestReader:229-255)          │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ Step 6.1: 判断是否需要统计信息投影                           │  │
│  │ ┌────────────────────────────────────────────────────────┐ │  │
│  │ │ boolean requireStatsProjection =                       │ │  │
│  │ │   requireStatsProjection(rowFilter, columns);          │ │  │
│  │ │                                                        │ │  │
│  │ │ // 如果有row filter且columns不包含统计列,自动添加:      │ │  │
│  │ │ // - value_counts                                      │ │  │
│  │ │ - null_value_counts                                    │ │  │
│  │ │ - nan_value_counts                                     │ │  │
│  │ │ - lower_bounds                                         │ │  │
│  │ │ - upper_bounds                                         │ │  │
│  │ │ - record_count                                         │ │  │
│  │ │                                                        │ │  │
│  │ │ Collection<String> projectColumns =                    │ │  │
│  │ │   requireStatsProjection ?                             │ │  │
│  │ │     withStatsColumns(columns) : columns;               │ │  │
│  │ └────────────────────────────────────────────────────────┘ │  │
│  │                                                            │  │
│  │ Step 6.2: 打开Manifest文件并读取entries                     │  │
│  │ ┌────────────────────────────────────────────────────────┐ │  │
│  │ │ Schema projection = projection(fileSchema,             │ │  │
│  │ │   fileProjection, projectColumns, caseSensitive);      │ │  │
│  │ │ CloseableIterable<ManifestEntry<F>> entries =          │ │  │
│  │ │   open(projection);                                    │ │  │
│  │ │                                                        │ │  │
│  │ │ // open()内部使用Avro读取manifest文件                   │ │  │
│  │ │ CloseableIterable<ManifestEntry<F>> reader =           │ │  │
│  │ │   InternalData.read(FileFormat.AVRO, file)             │ │  │
│  │ │     .project(ManifestEntry.wrapFileSchema(...))        │ │  │
│  │ │     .setRootType(GenericManifestEntry.class)           │ │  │
│  │ │     .setCustomType(DATA_FILE_ID, GenericDataFile.class)│ │  │
│  │ │     .reuseContainers()                                 │ │  │
│  │ │     .build();                                          │ │  │
│  │ └────────────────────────────────────────────────────────┘ │  │
│  │                                                            │  │
│  │ Step 6.3: 应用Partition和Metrics过滤                        │  │
│  │ ┌────────────────────────────────────────────────────────┐ │  │
│  │ │ if (hasRowFilter() || hasPartitionFilter() ||          │ │  │
│  │ │     partitionSet != null) {                            │ │  │
│  │ │   Evaluator evaluator = evaluator();                   │ │  │
│  │ │   InclusiveMetricsEvaluator metricsEvaluator =         │ │  │
│  │ │     metricsEvaluator();                                │ │  │
│  │ │                                                        │ │  │
│  │ │   return CloseableIterable.filter(                     │ │  │
│  │ │     scanMetrics.skippedDataFiles(),                    │ │  │
│  │ │     onlyLive ? filterLiveEntries(entries) : entries,   │ │  │
│  │ │     entry ->                                           │ │  │
│  │ │       entry != null &&                                 │ │  │
│  │ │       evaluator.eval(entry.file().partition()) &&      │ │  │
│  │ │       metricsEvaluator.eval(entry.file()) &&           │ │  │
│  │ │       inPartitionSet(entry.file())                     │ │  │
│  │ │   );                                                   │ │  │
│  │ │ }                                                      │ │  │
│  │ └────────────────────────────────────────────────────────┘ │  │
│  └────────────────────────────────────────────────────────────┘  │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│     7. FileScanTask生成 (ManifestGroup:359-370)                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ private static CloseableIterable<FileScanTask>             │  │
│  │   createFileScanTasks(                                     │  │
│  │     CloseableIterable<ManifestEntry<DataFile>> entries,    │  │
│  │     TaskContext ctx) {                                     │  │
│  │                                                            │  │
│  │   return CloseableIterable.transform(entries, entry -> {   │  │
│  │     // Step 7.1: 复制DataFile (保留/丢弃统计信息)            │  │
│  │     DataFile dataFile = ContentFileUtil.copy(              │  │
│  │       entry.file(),                                        │  │
│  │       ctx.shouldKeepStats(),                               │  │
│  │       ctx.columnsToKeepStats()                             │  │
│  │     );                                                     │  │
│  │                                                            │  │
│  │     // Step 7.2: 从DeleteFileIndex查找关联的删除文件         │  │
│  │     DeleteFile[] deleteFiles = ctx.deletes().forEntry(entry);│  │
│  │     // 内部调用:                                            │  │
│  │     // 1. findGlobalDeletes() - 全局Equality Deletes        │  │
│  │     // 2. findEqPartitionDeletes() - 分区Equality Deletes   │  │
│  │     // 3. findDV() - Deletion Vectors                       │  │
│  │     // 4. findPosPartitionDeletes() - 分区Position Deletes  │  │
│  │     // 5. findPathDeletes() - 路径Position Deletes          │  │
│  │                                                            │  │
│  │     // Step 7.3: 更新扫描指标                               │  │
│  │     ScanMetricsUtil.fileTask(                              │  │
│  │       ctx.scanMetrics(),                                   │  │
│  │       dataFile,                                            │  │
│  │       deleteFiles                                          │  │
│  │     );                                                     │  │
│  │                                                            │  │
│  │     // Step 7.4: 创建BaseFileScanTask                      │  │
│  │     return new BaseFileScanTask(                           │  │
│  │       dataFile,                                            │  │
│  │       deleteFiles,                                         │  │
│  │       ctx.schemaAsString(),      // JSON序列化schema        │  │
│  │       ctx.specAsString(),        // JSON序列化partition spec│  │
│  │       ctx.residuals()            // 剩余谓词evaluator      │  │
│  │     );                                                     │  │
│  │   });                                                      │  │
│  │ }                                                          │  │
│  └────────────────────────────────────────────────────────────┘  │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│     8. 可选: 任务分割与组合 (BaseTableScan:43-48)                 │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ @Override                                                  │  │
│  │ public CloseableIterable<CombinedScanTask> planTasks() {   │  │
│  │   CloseableIterable<FileScanTask> fileScanTasks =          │  │
│  │     planFiles();                                           │  │
│  │                                                            │  │
│  │   // Step 8.1: 分割大文件                                   │  │
│  │   CloseableIterable<FileScanTask> splitFiles =             │  │
│  │     TableScanUtil.splitFiles(                              │  │
│  │       fileScanTasks,                                       │  │
│  │       targetSplitSize()  // 默认128MB                      │  │
│  │     );                                                     │  │
│  │   // 内部: 如果 file.length() > targetSplitSize,           │  │
│  │   // 使用 file.split(targetSplitSize) 切分为多个            │  │
│  │   // SplitScanTask                                         │  │
│  │                                                            │  │
│  │   // Step 8.2: 组合小文件为CombinedScanTask                 │  │
│  │   return TableScanUtil.planTasks(                          │  │
│  │     splitFiles,                                            │  │
│  │     targetSplitSize(),                                     │  │
│  │     splitLookback(),        // 默认10                      │  │
│  │     splitOpenFileCost()     // 默认4MB                     │  │
│  │   );                                                       │  │
│  │   // Bin-packing算法:                                       │  │
│  │   // - 尝试将多个小任务打包到 targetSplitSize               │  │
│  │   // - 考虑文件打开成本 (splitOpenFileCost)                 │  │
│  │   // - 限制回溯范围 (splitLookback)                         │  │
│  │ }                                                          │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

---

## 三、ManifestGroup核心作用深度剖析

### 3.1 ManifestGroup职责定位

`ManifestGroup` (core/src/main/java/org/apache/iceberg/ManifestGroup.java) 是Iceberg读取链路的核心协调器,承担以下关键职责:

1. **统一Data和Delete Manifests管理**
   - 聚合来自Snapshot的数据清单和删除清单
   - 构建DeleteFileIndex实现高效的删除文件查找

2. **多层过滤策略编排**
   - Manifest List级别: 通过`ManifestEvaluator`快速跳过不匹配的manifest
   - Manifest File级别: 通过`ManifestReader`过滤不匹配的DataFile entry
   - DataFile级别: 通过`InclusiveMetricsEvaluator`利用统计信息过滤

3. **扫描任务生成抽象**
   - 提供`planFiles()`生成`FileScanTask`
   - 支持自定义`CreateTasksFunction`生成不同类型ScanTask
   - 管理并行扫描的ExecutorService

### 3.2 ManifestGroup关键字段解析

```java
class ManifestGroup {
  // ===== I/O和元数据 =====
  private final FileIO io;                          // 文件系统抽象
  private final Set<ManifestFile> dataManifests;    // 数据清单集合
  private final DeleteFileIndex.Builder deleteIndexBuilder; // 删除索引构建器
  private Map<Integer, PartitionSpec> specsById;    // PartitionSpec映射表

  // ===== 过滤条件 =====
  private Expression dataFilter;                    // 数据行过滤表达式
  private Expression fileFilter;                    // 文件级过滤表达式
  private Expression partitionFilter;               // 分区过滤表达式
  private Predicate<ManifestEntry<DataFile>> manifestEntryPredicate; // Entry谓词

  // ===== 扫描行为控制 =====
  private boolean ignoreDeleted;                    // 忽略DELETED状态条目
  private boolean ignoreExisting;                   // 忽略EXISTING状态条目
  private boolean ignoreResiduals;                  // 忽略剩余谓词计算

  // ===== 列裁剪与统计信息 =====
  private List<String> columns;                     // 选择的列 (默认ALL_COLUMNS)
  private boolean caseSensitive;                    // 大小写敏感
  private Set<Integer> columnsToKeepStats;          // 保留统计信息的列

  // ===== 性能优化 =====
  private ExecutorService executorService;          // 并行扫描线程池
  private ScanMetrics scanMetrics;                  // 扫描指标收集器
}
```

### 3.3 ManifestGroup核心方法调用链

```java
// 入口方法
public CloseableIterable<FileScanTask> planFiles() {
  return plan(ManifestGroup::createFileScanTasks);
}

// 泛型扫描计划方法
public <T extends ScanTask> CloseableIterable<T> plan(CreateTasksFunction<T> createTasksFunc) {
  // 1. 构建DeleteFileIndex (并行读取delete manifests)
  DeleteFileIndex deleteFiles = deleteIndexBuilder
    .scanMetrics(scanMetrics)
    .build();  // DeleteFileIndex.Builder:468

  // 2. 创建ResidualEvaluator缓存 (按PartitionSpec分组)
  LoadingCache<Integer, ResidualEvaluator> residualCache =
    Caffeine.newBuilder().build(specId -> {
      PartitionSpec spec = specsById.get(specId);
      Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : dataFilter;
      return ResidualEvaluator.of(spec, filter, caseSensitive);
    });

  // 3. 创建TaskContext缓存 (封装schema, spec, deletes, residuals)
  LoadingCache<Integer, TaskContext> taskContextCache =
    Caffeine.newBuilder().build(specId ->
      new TaskContext(
        specsById.get(specId),
        deleteFiles,
        residualCache.get(specId),
        dropStats,
        columnsToKeepStats,
        scanMetrics
      )
    );

  // 4. 生成分组的扫描任务
  Iterable<CloseableIterable<T>> tasks = entries(
    (manifest, entries) -> {
      int specId = manifest.partitionSpecId();
      TaskContext ctx = taskContextCache.get(specId);
      return createTasksFunc.apply(entries, ctx);
    }
  );

  // 5. 并行或串行执行
  if (executorService != null) {
    return new ParallelIterable<>(tasks, executorService);
  } else {
    return CloseableIterable.concat(tasks);
  }
}

// Manifest过滤与Entry迭代
private <T> Iterable<CloseableIterable<T>> entries(
    BiFunction<ManifestFile, CloseableIterable<ManifestEntry<DataFile>>, CloseableIterable<T>> entryFn) {

  // 1. 创建ManifestEvaluator缓存 (投影partition filter)
  LoadingCache<Integer, ManifestEvaluator> evalCache =
    Caffeine.newBuilder().build(specId -> {
      PartitionSpec spec = specsById.get(specId);
      Expression projected = Projections.inclusive(spec, caseSensitive).project(dataFilter);
      return ManifestEvaluator.forPartitionFilter(
        Expressions.and(partitionFilter, projected),
        spec,
        caseSensitive
      );
    });

  // 2. 过滤Manifest Files
  CloseableIterable<ManifestFile> matchingManifests =
    CloseableIterable.filter(
      scanMetrics.skippedDataManifests(),
      dataManifests,
      manifest -> evalCache.get(manifest.partitionSpecId()).eval(manifest)
    );

  // 3. 应用ignoreDeleted/ignoreExisting过滤
  if (ignoreDeleted) {
    matchingManifests = CloseableIterable.filter(
      scanMetrics.skippedDataManifests(),
      matchingManifests,
      m -> m.hasAddedFiles() || m.hasExistingFiles()
    );
  }
  if (ignoreExisting) {
    matchingManifests = CloseableIterable.filter(
      scanMetrics.skippedDataManifests(),
      matchingManifests,
      m -> m.hasAddedFiles() || m.hasDeletedFiles()
    );
  }

  // 4. 遍历每个Manifest,读取并过滤entries
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

        // 读取entries
        CloseableIterable<ManifestEntry<DataFile>> entries;
        if (ignoreDeleted) {
          entries = reader.liveEntries();
        } else {
          entries = reader.entries();
        }

        // 应用Entry级过滤
        if (ignoreExisting) {
          entries = CloseableIterable.filter(
            scanMetrics.skippedDataFiles(),
            entries,
            entry -> entry.status() != ManifestEntry.Status.EXISTING
          );
        }
        if (evaluator != null) {
          entries = CloseableIterable.filter(
            scanMetrics.skippedDataFiles(),
            entries,
            entry -> evaluator.eval((GenericDataFile) entry.file())
          );
        }
        entries = CloseableIterable.filter(
          scanMetrics.skippedDataFiles(),
          entries,
          manifestEntryPredicate
        );

        // 转换为目标类型
        iterable = entryFn.apply(manifest, entries);
        return iterable.iterator();
      }
    }
  );
}

// FileScanTask创建函数
private static CloseableIterable<FileScanTask> createFileScanTasks(
    CloseableIterable<ManifestEntry<DataFile>> entries,
    TaskContext ctx) {
  return CloseableIterable.transform(entries, entry -> {
    // 1. 复制DataFile (可选择保留统计信息)
    DataFile dataFile = ContentFileUtil.copy(
      entry.file(),
      ctx.shouldKeepStats(),
      ctx.columnsToKeepStats()
    );

    // 2. 查找关联的删除文件
    DeleteFile[] deleteFiles = ctx.deletes().forEntry(entry);

    // 3. 更新扫描指标
    ScanMetricsUtil.fileTask(ctx.scanMetrics(), dataFile, deleteFiles);

    // 4. 创建BaseFileScanTask
    return new BaseFileScanTask(
      dataFile,
      deleteFiles,
      ctx.schemaAsString(),
      ctx.specAsString(),
      ctx.residuals()
    );
  });
}
```

---

## 四、多层过滤机制详解

Iceberg的读取性能优化核心在于多层渐进式过滤,从粗粒度到细粒度逐步缩小扫描范围:

### 4.1 Level 1: Manifest List Filter (ManifestEvaluator)

**位置**: `api/src/main/java/org/apache/iceberg/expressions/ManifestEvaluator.java`

**触发时机**: `ManifestGroup.entries()` 方法中 (Line 269-275)

**过滤原理**:
```java
public class ManifestEvaluator {
  private final Expression expr;  // 绑定到PartitionSpec的分区过滤表达式

  public boolean eval(ManifestFile manifest) {
    List<PartitionFieldSummary> stats = manifest.partitions();
    if (stats == null) {
      return ROWS_MIGHT_MATCH;  // 无统计信息,保守返回true
    }
    return new ManifestEvalVisitor().eval(manifest);
  }

  // 内部Visitor实现
  private class ManifestEvalVisitor extends BoundExpressionVisitor<Boolean> {
    @Override
    public <T> Boolean eq(BoundReference<T> ref, Literal<T> lit) {
      int pos = Accessors.toPosition(ref.accessor());
      PartitionFieldSummary fieldStats = stats.get(pos);

      // 检查lower/upper bound
      if (fieldStats.lowerBound() == null) {
        return ROWS_CANNOT_MATCH;  // 所有值为null
      }

      T lower = Conversions.fromByteBuffer(ref.type(), fieldStats.lowerBound());
      if (lit.comparator().compare(lower, lit.value()) > 0) {
        return ROWS_CANNOT_MATCH;  // lower > 目标值
      }

      T upper = Conversions.fromByteBuffer(ref.type(), fieldStats.upperBound());
      if (lit.comparator().compare(upper, lit.value()) < 0) {
        return ROWS_CANNOT_MATCH;  // upper < 目标值
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean in(BoundReference<T> ref, Set<T> literalSet) {
      // 检查IN谓词,过滤掉范围外的值
      // 如果所有值都在范围外,返回ROWS_CANNOT_MATCH
    }
  }
}
```

**优化效果**:
- 快速跳过不包含目标分区的整个Manifest文件
- 避免读取Manifest文件内容 (节省I/O)
- 示例: `WHERE dt='2023-10-22'` 可跳过 `dt` 范围为 `[2023-01-01, 2023-06-30]` 的manifest

### 4.2 Level 2: Manifest File Filter (Partition + Metrics Evaluator)

**位置**: `core/src/main/java/org/apache/iceberg/ManifestReader.java:229-255`

**过滤步骤**:

```java
CloseableIterable<ManifestEntry<F>> entries(boolean onlyLive) {
  if (hasRowFilter() || hasPartitionFilter() || partitionSet != null) {
    // Step 1: 创建Partition Evaluator
    Evaluator evaluator = evaluator();
    // 内部实现:
    // Expression projected = Projections.inclusive(spec, caseSensitive).project(rowFilter);
    // Expression finalPartFilter = Expressions.and(projected, partFilter);
    // return new Evaluator(spec.partitionType(), finalPartFilter, caseSensitive);

    // Step 2: 创建Metrics Evaluator
    InclusiveMetricsEvaluator metricsEvaluator = metricsEvaluator();
    // 内部实现:
    // return new InclusiveMetricsEvaluator(spec.schema(), rowFilter, caseSensitive);

    // Step 3: 确保统计列投影
    boolean requireStatsProjection = requireStatsProjection(rowFilter, columns);
    Collection<String> projectColumns =
      requireStatsProjection ? withStatsColumns(columns) : columns;

    // Step 4: 读取并过滤entries
    CloseableIterable<ManifestEntry<F>> entries =
      open(projection(fileSchema, fileProjection, projectColumns, caseSensitive));

    return CloseableIterable.filter(
      scanMetrics.skippedDataFiles(),
      onlyLive ? filterLiveEntries(entries) : entries,
      entry ->
        entry != null &&
        evaluator.eval(entry.file().partition()) &&          // 分区过滤
        metricsEvaluator.eval(entry.file()) &&               // 统计信息过滤
        inPartitionSet(entry.file())                         // PartitionSet过滤
    );
  }
  // 无过滤条件,直接返回
  return open(projection(...));
}
```

### 4.3 Level 3: DataFile Metrics Filter (InclusiveMetricsEvaluator)

**位置**: `api/src/main/java/org/apache/iceberg/expressions/InclusiveMetricsEvaluator.java`

**核心原理**:

```java
public class InclusiveMetricsEvaluator {
  private final Expression expr;  // 绑定到表schema的行过滤表达式

  public boolean eval(ContentFile<?> file) {
    if (file.recordCount() == 0) {
      return ROWS_CANNOT_MATCH;
    }

    MetricsEvalVisitor visitor = new MetricsEvalVisitor();
    visitor.valueCounts = file.valueCounts();
    visitor.nullCounts = file.nullValueCounts();
    visitor.nanCounts = file.nanValueCounts();
    visitor.lowerBounds = file.lowerBounds();
    visitor.upperBounds = file.upperBounds();

    return ExpressionVisitors.visitEvaluator(expr, visitor);
  }

  private class MetricsEvalVisitor extends BoundVisitor<Boolean> {
    @Override
    public <T> Boolean eq(Bound<T> term, Literal<T> lit) {
      int id = term.ref().fieldId();

      // 检查null/NaN
      if (containsNullsOnly(id) || containsNaNsOnly(id)) {
        return ROWS_CANNOT_MATCH;
      }

      // 检查lower bound
      T lower = lowerBound(term);
      if (lower != null && !NaNUtil.isNaN(lower)) {
        if (lit.comparator().compare(lower, lit.value()) > 0) {
          return ROWS_CANNOT_MATCH;  // lower > 目标值
        }
      }

      // 检查upper bound
      T upper = upperBound(term);
      if (upper == null) {
        return ROWS_MIGHT_MATCH;
      }
      if (lit.comparator().compare(upper, lit.value()) < 0) {
        return ROWS_CANNOT_MATCH;  // upper < 目标值
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean lt(Bound<T> term, Literal<T> lit) {
      int id = term.ref().fieldId();
      if (containsNullsOnly(id) || containsNaNsOnly(id)) {
        return ROWS_CANNOT_MATCH;
      }

      T lower = lowerBound(term);
      if (null == lower || NaNUtil.isNaN(lower)) {
        return ROWS_MIGHT_MATCH;  // NaN表示不可靠边界
      }

      // 如果 lower >= lit.value(), 则所有值都 >= lit.value()
      // 不满足 < 条件
      int cmp = lit.comparator().compare(lower, lit.value());
      if (cmp >= 0) {
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }

    private boolean containsNullsOnly(Integer id) {
      return valueCounts != null &&
             valueCounts.containsKey(id) &&
             nullCounts != null &&
             nullCounts.containsKey(id) &&
             valueCounts.get(id) - nullCounts.get(id) == 0;
    }

    private boolean containsNaNsOnly(Integer id) {
      return nanCounts != null &&
             nanCounts.containsKey(id) &&
             valueCounts != null &&
             nanCounts.get(id).equals(valueCounts.get(id));
    }
  }
}
```

**支持的谓词类型**:
- `=`, `!=`, `<`, `<=`, `>`, `>=`
- `IN`, `NOT IN`
- `IS NULL`, `IS NOT NULL`
- `IS NaN`, `IS NOT NaN`
- `STARTS_WITH`, `NOT_STARTS_WITH`
- 复杂表达式: `AND`, `OR`, `NOT`

**优化示例**:
```sql
-- 查询: WHERE age > 30
-- DataFile1: lower_bounds={age: 18}, upper_bounds={age: 25}
--   -> 18 <= age <= 25, 全部 < 30, 跳过整个文件

-- DataFile2: lower_bounds={age: 35}, upper_bounds={age: 60}
--   -> 35 <= age <= 60, 可能包含满足条件的行, 读取文件

-- DataFile3: lower_bounds={age: 25}, upper_bounds={age: 45}
--   -> 25 <= age <= 45, 部分重叠, 读取文件并应用residual filter
```

---

## 五、统计信息Predicate Pushdown机制

### 5.1 Projection投影原理

Iceberg使用`Projections`类将数据行过滤器投影到分区字段,实现分区裁剪:

```java
// api/src/main/java/org/apache/iceberg/expressions/Projections.java

public class Projections {
  /**
   * 创建包容性投影 (Inclusive Projection)
   * - 如果投影后表达式为true,原始行可能满足条件
   * - 如果投影后表达式为false,原始行一定不满足条件
   */
  public static ProjectionEvaluator inclusive(PartitionSpec spec, boolean caseSensitive) {
    return new InclusiveProjection(spec, caseSensitive);
  }

  private static class InclusiveProjection extends ProjectionEvaluator {
    @Override
    public <T> UnboundPredicate<T> predicate(UnboundPredicate<T> pred) {
      // 示例: WHERE ts >= '2023-10-22 10:00:00'
      // PartitionSpec: day(ts)
      // 投影结果: day >= '2023-10-22'

      Expression result = ProjectionUtil.projectStrict(
        pred,
        spec.partitionType(),
        spec,
        caseSensitive
      );

      return result instanceof UnboundPredicate ?
        (UnboundPredicate<T>) result :
        Expressions.alwaysTrue();
    }
  }
}

// 投影示例
PartitionSpec spec = PartitionSpec.builderFor(schema)
  .year("ts")
  .bucket("user_id", 16)
  .build();

Expression rowFilter = Expressions.and(
  Expressions.greaterThanOrEqual("ts", "2023-01-01"),
  Expressions.equal("user_id", 12345)
);

Expression projected = Projections.inclusive(spec, true).project(rowFilter);
// 结果:
// AND(
//   ts_year >= 2023,
//   user_id_bucket == hash(12345) % 16
// )
```

### 5.2 统计信息使用决策树

```
┌─────────────────────────────────────────────────────────────┐
│              是否需要保留统计信息?                            │
└─────────────────────┬───────────────────────────────────────┘
                      │
        ┌─────────────┴─────────────┐
        │                           │
        ▼                           ▼
  有rowFilter               无rowFilter
        │                           │
        ├───────────────┐            │
        ▼               ▼            ▼
  有Equality    无Equality       直接丢弃统计信息
   Deletes       Deletes       (dropStats=true)
        │               │
        ▼               ▼
  保留统计信息    根据columns决定
  (需要过滤      ├─────────────┐
   equality      ▼             ▼
   deletes)   columns     columns包含
            不包含统计列   统计列或"*"
                 │             │
                 ▼             ▼
            补充统计列      保留统计信息
            (withStatsColumns)
                 │
                 └──────────┬──────────┘
                            │
                            ▼
                  ┌─────────────────────┐
                  │ 统计列完整列表:      │
                  │ - value_counts      │
                  │ - null_value_counts │
                  │ - nan_value_counts  │
                  │ - lower_bounds      │
                  │ - upper_bounds      │
                  │ - record_count      │
                  └─────────────────────┘
```

**源码实现** (ManifestGroup.java:187-189):
```java
boolean dropStats = ManifestReader.dropStats(columns);
if (deleteFiles.hasEqualityDeletes()) {
  select(ManifestReader.withStatsColumns(columns));
}

// ManifestReader.java:364-376
static boolean dropStats(Collection<String> columns) {
  if (columns != null && !columns.containsAll(ALL_COLUMNS)) {
    Set<String> intersection = Sets.intersection(Sets.newHashSet(columns), STATS_COLUMNS);
    // 仅当未选择任何统计列,或仅选择record_count时丢弃统计信息
    return intersection.isEmpty() ||
           intersection.equals(Sets.newHashSet("record_count"));
  }
  return false;
}

static List<String> withStatsColumns(Collection<String> columns) {
  if (columns.containsAll(ALL_COLUMNS)) {
    return Lists.newArrayList(columns);
  } else {
    List<String> projectColumns = Lists.newArrayList(columns);
    projectColumns.addAll(STATS_COLUMNS);
    return projectColumns;
  }
}
```

### 5.3 Residual Expression计算

当分区过滤无法完全应用时,剩余谓词(Residual)需要在数据读取阶段继续过滤:

```java
// api/src/main/java/org/apache/iceberg/expressions/ResidualEvaluator.java

public class ResidualEvaluator {
  private final PartitionSpec spec;
  private final Expression expr;
  private final boolean caseSensitive;

  public static ResidualEvaluator of(PartitionSpec spec, Expression expr, boolean caseSensitive) {
    return new ResidualEvaluator(spec, expr, caseSensitive);
  }

  /**
   * 计算给定分区的剩余表达式
   * @param partitionData 分区值
   * @return 无法通过分区过滤的剩余表达式
   */
  public Expression residualFor(StructLike partitionData) {
    return new ResidualVisitor(spec, partitionData, caseSensitive).eval(expr);
  }

  private static class ResidualVisitor extends ExpressionVisitors.BoundVisitor<Expression> {
    @Override
    public Expression predicate(BoundPredicate<?> pred) {
      // 检查谓词是否可以通过分区完全过滤
      if (canFilterByPartition(pred)) {
        boolean matches = evaluateAgainstPartition(pred);
        return matches ? Expressions.alwaysTrue() : Expressions.alwaysFalse();
      }

      // 无法通过分区过滤,保留原始谓词
      return pred;
    }
  }
}

// 示例
PartitionSpec spec = PartitionSpec.builderFor(schema)
  .year("ts")
  .build();

Expression filter = Expressions.and(
  Expressions.greaterThanOrEqual("ts", "2023-10-22 10:00:00"),
  Expressions.equal("name", "Alice")
);

StructLike partition = ...; // {ts_year: 2023}
ResidualEvaluator evaluator = ResidualEvaluator.of(spec, filter, true);
Expression residual = evaluator.residualFor(partition);
// 结果:
// AND(
//   ts >= "2023-10-22 10:00:00",  // 无法通过年分区完全过滤,保留
//   name == "Alice"               // 无分区字段,保留
// )
```

---

## 六、索引拦截机制

### 6.1 Delete Files索引结构

**DeleteFileIndex** (core/src/main/java/org/apache/iceberg/DeleteFileIndex.java) 提供高效的删除文件查找:

```java
class DeleteFileIndex {
  // ===== 索引结构 =====
  private final EqualityDeletes globalDeletes;                   // 全局Equality Deletes
  private final PartitionMap<EqualityDeletes> eqDeletesByPartition; // 分区级Equality
  private final PartitionMap<PositionDeletes> posDeletesByPartition; // 分区级Position
  private final Map<String, PositionDeletes> posDeletesByPath;   // 路径级Position
  private final Map<String, DeleteFile> dvByPath;                // Deletion Vectors

  /**
   * 查找给定DataFile的所有删除文件
   * @param sequenceNumber 数据序列号
   * @param file 数据文件
   * @return 关联的删除文件数组
   */
  DeleteFile[] forDataFile(long sequenceNumber, DataFile file) {
    if (isEmpty) {
      return EMPTY_DELETES;
    }

    // 1. 查找全局Equality Deletes
    DeleteFile[] global = findGlobalDeletes(sequenceNumber, file);

    // 2. 查找分区级Equality Deletes
    DeleteFile[] eqPartition = findEqPartitionDeletes(sequenceNumber, file);

    // 3. 查找Deletion Vector
    DeleteFile dv = findDV(sequenceNumber, file);
    if (dv != null && global == null && eqPartition == null) {
      return new DeleteFile[] {dv};
    } else if (dv != null) {
      return concat(global, eqPartition, new DeleteFile[] {dv});
    } else {
      // 4. 查找分区级Position Deletes
      DeleteFile[] posPartition = findPosPartitionDeletes(sequenceNumber, file);

      // 5. 查找路径级Position Deletes
      DeleteFile[] posPath = findPathDeletes(sequenceNumber, file);

      return concat(global, eqPartition, posPartition, posPath);
    }
  }

  // ===== Equality Deletes快速过滤 =====
  private static boolean canContainEqDeletesForFile(
      DataFile dataFile,
      EqualityDeleteFile deleteFile) {

    Map<Integer, ByteBuffer> dataLowers = dataFile.lowerBounds();
    Map<Integer, ByteBuffer> dataUppers = dataFile.upperBounds();

    for (Types.NestedField field : deleteFile.equalityFields()) {
      // 1. Null值检查
      if (containsNull(dataNullCounts, field) &&
          containsNull(deleteNullCounts, field)) {
        continue;  // 数据和删除都有null,必须应用
      }

      if (allNull(dataNullCounts, dataValueCounts, field) &&
          allNonNull(deleteNullCounts, field)) {
        return false;  // 数据全为null,但删除文件无null删除
      }

      if (allNull(deleteNullCounts, deleteValueCounts, field) &&
          allNonNull(dataNullCounts, field)) {
        return false;  // 删除文件仅删除null,但数据无null
      }

      // 2. 范围检查
      int id = field.fieldId();
      ByteBuffer dataLower = dataLowers.get(id);
      ByteBuffer dataUpper = dataUppers.get(id);
      Object deleteLower = deleteFile.lowerBound(id);
      Object deleteUpper = deleteFile.upperBound(id);

      if (dataLower == null || dataUpper == null ||
          deleteLower == null || deleteUpper == null) {
        continue;  // 缺少边界信息,保守处理
      }

      if (!rangesOverlap(field, dataLower, dataUpper, deleteLower, deleteUpper)) {
        return false;  // 范围不重叠,无需应用此删除文件
      }
    }

    return true;
  }

  private static <T> boolean rangesOverlap(...) {
    Comparator<T> comparator = Comparators.forType(type);
    T dataLower = Conversions.fromByteBuffer(type, dataLowerBuf);
    T dataUpper = Conversions.fromByteBuffer(type, dataUpperBuf);

    // 检查: dataLower <= deleteUpper && deleteLower <= dataUpper
    if (comparator.compare(dataLower, deleteUpper) > 0) {
      return false;  // dataLower > deleteUpper
    }
    if (comparator.compare(deleteLower, dataUpper) > 0) {
      return false;  // deleteLower > dataUpper
    }
    return true;
  }
}
```

### 6.2 Position Deletes索引优化

Position Deletes按`dataSequenceNumber`排序,使用二分查找快速定位:

```java
static class PositionDeletes {
  private long[] seqs = null;         // 排序后的序列号数组
  private DeleteFile[] files = null;  // 对应的删除文件数组

  public DeleteFile[] filter(long seq) {
    indexIfNeeded();

    // 二分查找起始位置
    int start = findStartIndex(seqs, seq);

    if (start >= files.length) {
      return EMPTY_DELETES;  // 所有删除文件序列号 < seq
    }

    if (start == 0) {
      return files;  // 所有删除文件都适用
    }

    // 返回适用的删除文件子集
    int matchingFilesCount = files.length - start;
    DeleteFile[] matchingFiles = new DeleteFile[matchingFilesCount];
    System.arraycopy(files, start, matchingFiles, 0, matchingFilesCount);
    return matchingFiles;
  }

  private static int findStartIndex(long[] seqs, long seq) {
    int pos = Arrays.binarySearch(seqs, seq);
    int start;
    if (pos < 0) {
      // 未找到,返回插入点 -(pos + 1)
      start = -(pos + 1);
    } else {
      // 找到,向前查找第一个 >= seq 的位置
      start = pos;
      while (start > 0 && seqs[start - 1] >= seq) {
        start -= 1;
      }
    }
    return start;
  }
}
```

### 6.3 Deletion Vectors (V3新特性)

Iceberg V3引入Deletion Vectors (Puffin格式),更高效的位图删除:

```java
// 检查Deletion Vector
private DeleteFile findDV(long seq, DataFile dataFile) {
  if (dvByPath == null) {
    return null;
  }

  DeleteFile dv = dvByPath.get(dataFile.location());
  if (dv != null) {
    ValidationException.check(
      dv.dataSequenceNumber() >= seq,
      "DV data sequence number (%s) must be >= data file sequence number (%s)",
      dv.dataSequenceNumber(),
      seq
    );
  }
  return dv;
}

// DV识别 (ContentFileUtil.java)
public static boolean isDV(DeleteFile deleteFile) {
  return deleteFile.content() == FileContent.POSITION_DELETES &&
         deleteFile.referencedDataFile() != null &&
         deleteFile.format() == FileFormat.PUFFIN;
}
```

---

## 七、ScanTask类型体系

### 7.1 ScanTask继承结构

```
                    ┌──────────────┐
                    │  ScanTask    │ (interface)
                    │ - sizeBytes()│
                    │ - estimatedRowsCount()
                    │ - filesCount()│
                    └──────┬───────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
        ▼                  ▼                  ▼
┌───────────────┐  ┌──────────────┐  ┌────────────────┐
│ FileScanTask  │  │  DataTask    │  │CombinedScanTask│
│ - file()      │  │              │  │ - files()      │
│ - deletes()   │  │              │  │                │
│ - residual()  │  │              │  │                │
│ - split()     │  │              │  │                │
└───────┬───────┘  └──────────────┘  └────────────────┘
        │
        ├─────────────────────┬──────────────────┐
        ▼                     ▼                  ▼
┌───────────────┐    ┌────────────────┐  ┌──────────────┐
│BaseFileScanTask│   │SplitScanTask   │  │MergeableScanTask│
│ (完整文件)     │    │ (文件分片)      │  │ (interface)    │
│               │    │ - offset       │  │ - canMerge()   │
│               │    │ - length       │  │ - merge()      │
└───────────────┘    └────────────────┘  └──────────────┘
```

### 7.2 FileScanTask详解

**BaseFileScanTask** (core/src/main/java/org/apache/iceberg/BaseFileScanTask.java):
```java
public class BaseFileScanTask extends BaseContentScanTask<FileScanTask, DataFile>
    implements FileScanTask {

  private final DeleteFile[] deletes;           // 关联的删除文件
  private transient volatile List<DeleteFile> deleteList = null;
  private transient volatile long deletesSizeBytes = 0L;

  public BaseFileScanTask(
      DataFile file,
      DeleteFile[] deletes,
      String schemaString,                      // JSON序列化的Schema
      String specString,                        // JSON序列化的PartitionSpec
      ResidualEvaluator residuals) {            // 剩余谓词计算器
    super(file, schemaString, specString, residuals);
    this.deletes = deletes != null ? deletes : new DeleteFile[0];
  }

  @Override
  public List<DeleteFile> deletes() {
    if (deleteList == null) {
      this.deleteList = ImmutableList.copyOf(deletes);
    }
    return deleteList;
  }

  @Override
  public long sizeBytes() {
    return length() + deletesSizeBytes();  // 数据文件 + 删除文件总大小
  }

  @Override
  public int filesCount() {
    return 1 + deletes.length;  // 数据文件 + 删除文件数量
  }

  /**
   * 分割任务为多个子任务
   * @param splitSize 目标分割大小
   * @return 分割后的任务迭代器
   */
  @Override
  public Iterable<FileScanTask> split(long splitSize) {
    long len = length();
    if (len <= splitSize) {
      return ImmutableList.of(this);  // 无需分割
    }

    List<FileScanTask> splitTasks = Lists.newArrayList();
    for (long offset = 0; offset < len; offset += splitSize) {
      long actualLen = Math.min(splitSize, len - offset);
      splitTasks.add(new SplitScanTask(offset, actualLen, this, deletesSizeBytes()));
    }

    return splitTasks;
  }
}
```

**SplitScanTask** (文件分片任务):
```java
static final class SplitScanTask implements FileScanTask, MergeableScanTask<SplitScanTask> {
  private final long len;                    // 分片长度
  private final long offset;                 // 文件内偏移量
  private final FileScanTask fileScanTask;   // 父任务引用

  @Override
  public long start() {
    return offset;
  }

  @Override
  public long length() {
    return len;
  }

  @Override
  public DataFile file() {
    return fileScanTask.file();  // 共享父任务的DataFile
  }

  @Override
  public List<DeleteFile> deletes() {
    return fileScanTask.deletes();  // 共享删除文件列表
  }

  @Override
  public boolean canMerge(ScanTask other) {
    if (other instanceof SplitScanTask) {
      SplitScanTask that = (SplitScanTask) other;
      // 必须是同一文件的相邻分片
      return file().equals(that.file()) &&
             offset + len == that.start();
    }
    return false;
  }

  @Override
  public SplitScanTask merge(ScanTask other) {
    SplitScanTask that = (SplitScanTask) other;
    return new SplitScanTask(
      offset,
      len + that.length(),
      fileScanTask,
      deletesSizeBytes
    );
  }
}
```

### 7.3 CombinedScanTask (任务打包)

```java
// api/src/main/java/org/apache/iceberg/CombinedScanTask.java
public interface CombinedScanTask extends ScanTask {
  /**
   * 组合的文件扫描任务列表
   * @return FileScanTask集合
   */
  List<FileScanTask> files();

  @Override
  default long sizeBytes() {
    return files().stream()
      .mapToLong(FileScanTask::sizeBytes)
      .sum();
  }

  @Override
  default int filesCount() {
    return files().stream()
      .mapToInt(FileScanTask::filesCount)
      .sum();
  }
}

// 实现类: BaseCombinedScanTask
public class BaseCombinedScanTask implements CombinedScanTask {
  private final List<FileScanTask> tasks;

  public BaseCombinedScanTask(List<FileScanTask> tasks) {
    this.tasks = ImmutableList.copyOf(tasks);
  }

  @Override
  public List<FileScanTask> files() {
    return tasks;
  }
}
```

### 7.4 任务分割与组合算法

**分割逻辑** (util/TableScanUtil.java):
```java
public static CloseableIterable<FileScanTask> splitFiles(
    CloseableIterable<FileScanTask> tasks,
    long splitSize) {

  return CloseableIterable.flatMap(tasks, task -> {
    if (task.length() <= splitSize) {
      return CloseableIterable.withNoopClose(ImmutableList.of(task));
    } else {
      // 分割大文件
      return CloseableIterable.withNoopClose(task.split(splitSize));
    }
  });
}
```

**组合逻辑** (Bin-packing算法):
```java
public static CloseableIterable<CombinedScanTask> planTasks(
    CloseableIterable<FileScanTask> splitFiles,
    long splitSize,
    int lookback,
    long openFileCost) {

  List<List<FileScanTask>> bins = Lists.newArrayList();
  List<FileScanTask> currentBin = Lists.newArrayList();
  long currentSize = 0;

  try (CloseableIterator<FileScanTask> iter = splitFiles.iterator()) {
    while (iter.hasNext()) {
      FileScanTask task = iter.next();
      long taskSize = task.sizeBytes() + openFileCost;

      // 尝试放入当前bin
      if (currentSize + taskSize <= splitSize) {
        currentBin.add(task);
        currentSize += taskSize;
      } else {
        // 当前bin已满,尝试回溯优化
        if (!currentBin.isEmpty()) {
          bins.add(currentBin);
        }

        // 回溯查找可容纳的bin
        boolean packed = false;
        int start = Math.max(0, bins.size() - lookback);
        for (int i = bins.size() - 1; i >= start; i--) {
          List<FileScanTask> bin = bins.get(i);
          long binSize = bin.stream().mapToLong(FileScanTask::sizeBytes).sum();
          if (binSize + taskSize <= splitSize) {
            bin.add(task);
            packed = true;
            break;
          }
        }

        if (!packed) {
          // 创建新bin
          currentBin = Lists.newArrayList(task);
          currentSize = taskSize;
        } else {
          currentBin = Lists.newArrayList();
          currentSize = 0;
        }
      }
    }

    if (!currentBin.isEmpty()) {
      bins.add(currentBin);
    }
  }

  // 转换为CombinedScanTask
  return CloseableIterable.transform(
    CloseableIterable.withNoopClose(bins),
    tasks -> new BaseCombinedScanTask(tasks)
  );
}
```

---

## 八、完整读取流程时序图

```
User Code                TableScan              ManifestGroup           ManifestReader       DeleteFileIndex
    │                       │                        │                        │                     │
    │  newScan()            │                        │                        │                     │
    ├──────────────────────>│                        │                        │                     │
    │  .filter(expr)        │                        │                        │                     │
    ├──────────────────────>│                        │                        │                     │
    │  .select(cols)        │                        │                        │                     │
    ├──────────────────────>│                        │                        │                     │
    │                       │                        │                        │                     │
    │  planFiles()          │                        │                        │                     │
    ├──────────────────────>│  new ManifestGroup()   │                        │                     │
    │                       ├───────────────────────>│                        │                     │
    │                       │                        │                        │                     │
    │                       │  planFiles()           │                        │                     │
    │                       ├───────────────────────>│                        │                     │
    │                       │                        │  build()               │                     │
    │                       │                        ├───────────────────────────────────────────────>│
    │                       │                        │                        │  loadDeleteFiles()  │
    │                       │                        │                        │<────────────────────│
    │                       │                        │                        │  (并行读取delete manifests)
    │                       │                        │<───────────────────────────────────────────────│
    │                       │                        │  DeleteFileIndex       │                     │
    │                       │                        │                        │                     │
    │                       │  entries()             │                        │                     │
    │                       │                        │<───────────────────────┤                     │
    │                       │                        │                        │                     │
    │                       │                        │  过滤Manifests         │                     │
    │                       │                        │  (ManifestEvaluator)   │                     │
    │                       │                        │                        │                     │
    │                       │                        │  对每个Manifest:        │                     │
    │                       │                        │  ┌─────────────────────┐                     │
    │                       │                        │  │ ManifestFiles.read()│                     │
    │                       │                        │  └────────┬────────────┘                     │
    │                       │                        │           │                                  │
    │                       │                        │           │  .filterRows()                   │
    │                       │                        │           ├────────────────>│                │
    │                       │                        │           │  .filterPartitions()             │
    │                       │                        │           ├────────────────>│                │
    │                       │                        │           │  .select()      │                │
    │                       │                        │           ├────────────────>│                │
    │                       │                        │           │                 │                │
    │                       │                        │           │  entries()      │                │
    │                       │                        │           ├────────────────>│                │
    │                       │                        │           │                 │  open(manifest)│
    │                       │                        │           │                 ├────────────────┤
    │                       │                        │           │                 │  (Avro reader) │
    │                       │                        │           │                 │<───────────────┤
    │                       │                        │           │                 │                │
    │                       │                        │           │                 │  filter:       │
    │                       │                        │           │                 │  - evaluator   │
    │                       │                        │           │                 │    .eval(partition)
    │                       │                        │           │                 │  - metricsEvaluator
    │                       │                        │           │                 │    .eval(file) │
    │                       │                        │           │<────────────────┤                │
    │                       │                        │           │  Filtered entries                │
    │                       │                        │<──────────┘                 │                │
    │                       │                        │                             │                │
    │                       │  createFileScanTasks() │                             │                │
    │                       │                        │  对每个entry:                │                │
    │                       │                        │  ┌──────────────────────────┐                │
    │                       │                        │  │ DataFile dataFile =     │                │
    │                       │                        │  │   entry.file().copy()   │                │
    │                       │                        │  │                         │                │
    │                       │                        │  │ DeleteFile[] deletes =  │  forEntry()    │
    │                       │                        │  │   deleteIndex.forEntry()├───────────────>│
    │                       │                        │  │                         │                │
    │                       │                        │  │                         │  1. findGlobalDeletes()
    │                       │                        │  │                         │  2. findEqPartitionDeletes()
    │                       │                        │  │                         │  3. findDV()  │
    │                       │                        │  │                         │  4. findPosPartitionDeletes()
    │                       │                        │  │                         │  5. findPathDeletes()
    │                       │                        │  │                         │<───────────────│
    │                       │                        │  │                         │  DeleteFile[]  │
    │                       │                        │  │                         │                │
    │                       │                        │  │ new BaseFileScanTask(   │                │
    │                       │                        │  │   dataFile,             │                │
    │                       │                        │  │   deletes,              │                │
    │                       │                        │  │   schema,               │                │
    │                       │                        │  │   spec,                 │                │
    │                       │                        │  │   residuals             │                │
    │                       │                        │  │ )                       │                │
    │                       │                        │  └──────────────────────────┘                │
    │                       │<───────────────────────┤                             │                │
    │                       │  Iterable<FileScanTask>│                             │                │
    │<──────────────────────┤                        │                             │                │
    │  FileScanTask[]       │                        │                             │                │
    │                       │                        │                             │                │
    │  forEach(task -> {...})                        │                             │                │
    │  - task.file()        │                        │                             │                │
    │  - task.deletes()     │                        │                             │                │
    │  - task.residual()    │                        │                             │                │
    │  读取数据并应用删除     │                        │                             │                │
```

---

## 九、性能优化最佳实践

### 9.1 分区设计优化

```java
// ❌ 错误: 分区粒度过细
PartitionSpec.builderFor(schema)
  .hour("timestamp")  // 每小时一个分区,产生过多小文件
  .build();

// ✅ 正确: 合理粒度
PartitionSpec.builderFor(schema)
  .day("timestamp")   // 每天一个分区
  .bucket("user_id", 16)  // 16个bucket
  .build();
```

### 9.2 统计信息利用

```sql
-- 利用列统计信息的查询优化
-- 原始查询
SELECT * FROM orders WHERE amount > 1000;

-- Iceberg优化流程:
-- 1. Manifest Level: 检查 amount 的 lower/upper bounds
--    跳过 upper_bound < 1000 的 manifests
-- 2. File Level: 检查每个 DataFile 的 amount lower_bound
--    跳过 lower_bound > 1000 的文件 (全部不满足)
-- 3. Row Level: 读取文件并应用 residual filter
```

### 9.3 列裁剪优化

```java
// ❌ 低效: 读取所有列
table.newScan()
  .filter(Expressions.equal("region", "us-west"))
  .planFiles();

// ✅ 高效: 仅读取需要的列
table.newScan()
  .filter(Expressions.equal("region", "us-west"))
  .select("id", "name", "timestamp")  // 列裁剪
  .planFiles();

// 效果:
// - ManifestReader 仅读取 id, name, timestamp 的统计信息
// - 减少内存占用
// - 如果无 delete files,可以丢弃所有统计信息 (dropStats=true)
```

### 9.4 并行扫描配置

```java
// 配置并行扫描线程池
ExecutorService executor = Executors.newFixedThreadPool(
  Runtime.getRuntime().availableProcessors()
);

table.newScan()
  .filter(filter)
  .option("executor-service", executor)
  .planFiles();

// 内部效果:
// - 并行读取多个 manifest files
// - 并行构建 DeleteFileIndex
// - 使用 ParallelIterable 加速任务生成
```

### 9.5 任务分割调优

```java
// 调整分割参数
table.newScan()
  .option("split-size", "268435456")         // 256MB (默认128MB)
  .option("split-lookback", "5")             // 回溯5个bins (默认10)
  .option("split-open-file-cost", "8388608") // 8MB (默认4MB)
  .planTasks();

// 参数解释:
// - split-size: 目标任务大小,影响并行度
// - split-lookback: bin-packing回溯深度,影响打包效率
// - split-open-file-cost: 文件打开开销估算,避免过度组合小文件
```

---

## 十、关键源码位置索引

| 组件 | 源码路径 | 核心方法 |
|------|---------|---------|
| TableScan入口 | core/src/main/java/org/apache/iceberg/DataTableScan.java | doPlanFiles():64 |
| ManifestGroup | core/src/main/java/org/apache/iceberg/ManifestGroup.java | planFiles():171<br>plan():175<br>entries():242 |
| ManifestReader | core/src/main/java/org/apache/iceberg/ManifestReader.java | entries():229<br>liveEntries():302 |
| ManifestEvaluator | api/src/main/java/org/apache/iceberg/expressions/ManifestEvaluator.java | eval():76<br>ManifestEvalVisitor:83 |
| InclusiveMetricsEvaluator | api/src/main/java/org/apache/iceberg/expressions/InclusiveMetricsEvaluator.java | eval():75<br>MetricsEvalVisitor:83 |
| DeleteFileIndex | core/src/main/java/org/apache/iceberg/DeleteFileIndex.java | forDataFile():148<br>Builder.build():468 |
| BaseFileScanTask | core/src/main/java/org/apache/iceberg/BaseFileScanTask.java | 构造函数:34<br>split():162 |
| ScanTask接口 | api/src/main/java/org/apache/iceberg/ScanTask.java | - |
| Projections | api/src/main/java/org/apache/iceberg/expressions/Projections.java | inclusive():project() |
| ResidualEvaluator | api/src/main/java/org/apache/iceberg/expressions/ResidualEvaluator.java | residualFor() |

---

## 十一、总结

### 11.1 核心设计理念

1. **分层过滤架构**: Manifest List → Manifest File → DataFile → Row Group,每层利用不同粒度的统计信息
2. **延迟加载**: 仅在需要时读取 manifest 和 data files
3. **统计信息驱动**: 充分利用 min/max、null counts、nan counts 实现数据跳过
4. **并行优化**: 支持并行读取 manifests 和构建 delete index
5. **灵活任务生成**: 支持文件分割与组合,适配不同执行引擎

### 11.2 ManifestGroup关键价值

- 统一管理 data 和 delete manifests
- 编排多层过滤策略 (partition filter、row filter、metrics filter)
- 高效构建 DeleteFileIndex (序列号索引 + 统计信息过滤)
- 抽象扫描任务生成 (支持自定义 CreateTasksFunction)

### 11.3 性能优化要点

1. **合理设计分区**: 避免过细分区,推荐 day/hour + bucket 组合
2. **启用列裁剪**: select() 仅投影需要的列
3. **利用统计信息**: 查询条件尽量包含分区字段和有统计信息的列
4. **配置并行扫描**: 大规模表启用 executor-service
5. **调优任务大小**: 根据集群资源调整 split-size

---

**文档版本**: v1.0
**生成时间**: 2025-10-22
**适用版本**: Apache Iceberg 1.10.x
**作者**: 基于源码深度分析生成
