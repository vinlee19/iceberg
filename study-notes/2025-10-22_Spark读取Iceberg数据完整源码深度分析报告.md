# 2025-10-22 Spark读取Iceberg数据完整源码深度分析报告

## 文档概述

本文档基于Apache Iceberg 1.10.x + Spark 3.5源码,深度剖析从FileScanTask生成到实际数据读取的完整链路,涵盖DataSource V2集成、Task分配、格式优化、分区裁剪、Delete Files应用、延迟物化及Metrics监控全流程。

---

## 目录

1. [Spark DataSource V2集成架构](#一spark-datasource-v2集成架构)
2. [FileScanTask消费与分配机制](#二filescantas消费与分配机制)
3. [数据读取核心流程](#三数据读取核心流程)
4. [文件格式优化详解](#四文件格式优化详解)
5. [分区裁剪机制](#五分区裁剪机制)
6. [Delete Files应用机制](#六delete-files应用机制)
7. [延迟物化优化](#七延迟物化优化)
8. [Metrics监控体系](#八metrics监控体系)
9. [完整读取流程时序图](#九完整读取流程时序图)
10. [性能调优最佳实践](#十性能调优最佳实践)

---

## 一、Spark DataSource V2集成架构

### 1.1 DataSource V2核心接口层级

```
                    ┌─────────────────┐
                    │  TableProvider  │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  Table (V2)     │
                    │  - name()       │
                    │  - schema()     │
                    │  - capabilities()│
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ SupportsRead    │
                    │  - newScanBuilder()│
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────────────────┐
                    │  ScanBuilder                │
                    │  - build()                  │
                    │  - pushFilters()            │
                    │  - pruneColumns()           │
                    └────────┬────────────────────┘
                             │
                             ▼
        ┌────────────────────┴────────────────────┐
        │                                         │
        ▼                                         ▼
┌──────────────────┐                    ┌─────────────────┐
│  Scan            │                    │  Batch          │
│  - toBatch()     │───────────────────>│  - planInputPartitions()│
│  - readSchema()  │                    │  - createReaderFactory()│
│  - estimateStatistics()│              └─────────┬───────┘
└──────────────────┘                              │
                                                  ▼
                                    ┌──────────────────────────┐
                                    │ PartitionReaderFactory   │
                                    │  - createReader(partition)│
                                    └────────┬─────────────────┘
                                             │
                                             ▼
                                    ┌──────────────────────┐
                                    │  PartitionReader     │
                                    │  - next()            │
                                    │  - get()             │
                                    │  - close()           │
                                    └──────────────────────┘
```

### 1.2 Iceberg实现类映射

| DataSource V2接口 | Iceberg实现类 | 源码位置 |
|------------------|--------------|---------|
| TableProvider | SparkTableProvider | spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/source/SparkTableProvider.scala |
| Table | SparkTable | spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/source/SparkTable.scala |
| ScanBuilder | SparkScanBuilder | spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/source/SparkScanBuilder.scala |
| Scan | SparkBatchQueryScan | spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatchQueryScan.java:65 |
| Batch | - (内嵌在Scan中) | - |
| PartitionReaderFactory | - (内嵌创建) | SparkScan.java |
| PartitionReader | RowDataReader / BatchDataReader | RowDataReader.java:43 / BatchDataReader.java:43 |

### 1.3 Scan创建与配置流程

```java
// 源码位置: SparkScanBuilder.build()
public Scan build() {
  Schema expectedSchema = // 列裁剪后的schema
  List<Expression> icebergFilters = // 转换的Iceberg表达式

  Scan<?, FileScanTask, ?> scan = table.newScan()
    .caseSensitive(caseSensitive)
    .filter(filterExpression)
    .select(expectedSchema.columns());

  // 应用snapshot选择
  if (snapshotId != null) {
    scan = scan.useSnapshot(snapshotId);
  } else if (asOfTimestamp != null) {
    scan = scan.asOfTime(asOfTimestamp);
  }

  // 创建SparkBatchQueryScan
  return new SparkBatchQueryScan(
    spark,
    table,
    scan,
    readConf,
    expectedSchema,
    icebergFilters,
    scanReportSupplier
  );
}
```

---

## 二、FileScanTask消费与分配机制

### 2.1 Task规划与分组流程

```
┌────────────────────────────────────────────────────────────────┐
│          SparkBatchQueryScan (继承 SparkPartitioningAwareScan) │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Step 1: 调用 Iceberg Scan.planFiles()                    │  │
│  │ ┌──────────────────────────────────────────────────────┐ │  │
│  │ │ protected synchronized List<T> tasks() {             │ │  │
│  │ │   if (tasks == null) {                               │ │  │
│  │ │     try (CloseableIterable<? extends ScanTask>       │ │  │
│  │ │          taskIterable = scan.planFiles()) {          │ │  │
│  │ │       List<T> plannedTasks = Lists.newArrayList();   │ │  │
│  │ │       for (ScanTask task : taskIterable) {           │ │  │
│  │ │         plannedTasks.add((PartitionScanTask) task);  │ │  │
│  │ │       }                                              │ │  │
│  │ │       this.tasks = plannedTasks;                     │ │  │
│  │ │     }                                                │ │  │
│  │ │   }                                                  │ │  │
│  │ │   return tasks;                                      │ │  │
│  │ │ }                                                    │ │  │
│  │ └──────────────────────────────────────────────────────┘ │  │
│  │ 源码: SparkPartitioningAwareScan.java:172-194            │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Step 2: 任务分组 (Grouping & Binpacking)                 │  │
│  │ ┌──────────────────────────────────────────────────────┐ │  │
│  │ │ protected synchronized List<ScanTaskGroup<T>>        │ │  │
│  │ │   taskGroups() {                                     │ │  │
│  │ │   if (taskGroups == null) {                          │ │  │
│  │ │     if (groupingKeyType().fields().isEmpty()) {      │ │  │
│  │ │       // 无数据分组 - 简单bin-packing                  │ │  │
│  │ │       CloseableIterable<ScanTaskGroup<T>>            │ │  │
│  │ │         plannedTaskGroups =                          │ │  │
│  │ │         TableScanUtil.planTaskGroups(                │ │  │
│  │ │           tasks(),                                   │ │  │
│  │ │           adjustSplitSize(tasks(), targetSplitSize),│ │  │
│  │ │           splitLookback,                             │ │  │
│  │ │           splitOpenFileCost                          │ │  │
│  │ │         );                                           │ │  │
│  │ │       this.taskGroups = Lists.newArrayList(          │ │  │
│  │ │         plannedTaskGroups);                          │ │  │
│  │ │                                                      │ │  │
│  │ │     } else {                                         │ │  │
│  │ │       // 保留数据分组 - 按groupingKey分组              │ │  │
│  │ │       List<ScanTaskGroup<T>> plannedTaskGroups =     │ │  │
│  │ │         TableScanUtil.planTaskGroups(                │ │  │
│  │ │           tasks(),                                   │ │  │
│  │ │           adjustSplitSize(tasks(), targetSplitSize),│ │  │
│  │ │           splitLookback,                             │ │  │
│  │ │           splitOpenFileCost,                         │ │  │
│  │ │           groupingKeyType()  // 分组键                │ │  │
│  │ │         );                                           │ │  │
│  │ │       this.taskGroups = plannedTaskGroups;           │ │  │
│  │ │     }                                                │ │  │
│  │ │   }                                                  │ │  │
│  │ │   return taskGroups;                                 │ │  │
│  │ │ }                                                    │ │  │
│  │ └──────────────────────────────────────────────────────┘ │  │
│  │ 源码: SparkPartitioningAwareScan.java:197-234            │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

### 2.2 TaskGroup到InputPartition转换

```java
// 源码位置: SparkScan.planInputPartitions()
@Override
public InputPartition[] planInputPartitions() {
  List<ScanTaskGroup<T>> taskGroups = taskGroups();

  InputPartition[] partitions = new InputPartition[taskGroups.size()];

  for (int index = 0; index < taskGroups.size(); index++) {
    ScanTaskGroup<T> taskGroup = taskGroups.get(index);

    // 计算preferredLocations (数据本地性优化)
    String[] preferredLocations =
      computePreferredLocations(taskGroup);

    // 创建SparkInputPartition
    partitions[index] = new SparkInputPartition(
      groupingKeyType(),
      taskGroup,
      tableBroadcast,
      branch,
      SchemaParser.toJson(expectedSchema),
      caseSensitive,
      preferredLocations,
      cacheDeleteFilesOnExecutors
    );
  }

  return partitions;
}
```

**SparkInputPartition核心字段**:
```java
class SparkInputPartition implements InputPartition, HasPartitionKey {
  private final Types.StructType groupingKeyType;      // 分组键类型
  private final ScanTaskGroup<?> taskGroup;            // 任务组
  private final Broadcast<Table> tableBroadcast;       // 广播的Table对象
  private final String branch;                         // 分支名称
  private final String expectedSchemaString;           // JSON序列化schema
  private final boolean caseSensitive;                 // 大小写敏感
  private final String[] preferredLocations;           // 优先位置(数据本地性)
  private final boolean cacheDeleteFilesOnExecutors;   // 是否缓存删除文件

  @Override
  public InternalRow partitionKey() {
    // 返回分组键,用于Spark的分区感知调度
    return new StructInternalRow(groupingKeyType)
      .setStruct(taskGroup.groupingKey());
  }

  @Override
  public String[] preferredLocations() {
    // 返回优先位置,Spark会尽量在这些节点调度Task
    return preferredLocations;
  }
}
```

### 2.3 数据分组优化 (Preserve Data Grouping)

当启用`read.split.planning-lookback`和`read.preserve-data-grouping`时:

```
场景1: 无数据分组 (preserve-data-grouping=false)
┌─────────────────────────────────────────────────────────────┐
│ TaskGroup 1: [File1(100MB), File2(50MB)]  = 150MB         │
│ TaskGroup 2: [File3(120MB), File4(30MB)]  = 150MB         │
│ TaskGroup 3: [File5(80MB), File6(70MB)]   = 150MB         │
└─────────────────────────────────────────────────────────────┘
优点: 负载均衡,充分并行
缺点: 可能破坏数据局部性

场景2: 保留数据分组 (preserve-data-grouping=true)
┌─────────────────────────────────────────────────────────────┐
│ TaskGroup 1: 所有 partition=2023-01-01 的文件             │
│ TaskGroup 2: 所有 partition=2023-01-02 的文件             │
│ TaskGroup 3: 所有 partition=2023-01-03 的文件             │
└─────────────────────────────────────────────────────────────┘
优点: 保持数据局部性,有利于后续聚合操作
缺点: 可能负载不均衡

配置示例:
spark.read
  .option("read.split.target-size", "134217728")  // 128MB
  .option("read.preserve-data-grouping", "true")
  .table("catalog.db.table")
```

---

## 三、数据读取核心流程

### 3.1 PartitionReader创建流程

```
SparkScan.createReaderFactory()
    │
    └─> 返回 PartitionReaderFactory 匿名实现
        │
        └─> createReader(InputPartition partition)
            │
            ├─> 类型检查: partition instanceof SparkInputPartition
            │
            ├─> 判断读取模式:
            │   ├─ vectorized=true  -> BatchDataReader (列式)
            │   └─ vectorized=false -> RowDataReader (行式)
            │
            └─> 创建Reader实例
```

**源码实现**:
```java
// SparkScan.createReaderFactory()
@Override
public PartitionReaderFactory createReaderFactory() {
  return new PartitionReaderFactory() {
    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      SparkInputPartition sparkPartition = (SparkInputPartition) partition;

      if (batchReadsEnabled) {
        // 向量化读取 (ColumnarBatch)
        return new BatchDataReader(
          sparkPartition,
          parquetBatchReadConf(),
          orcBatchReadConf()
        );
      } else {
        // 行式读取 (InternalRow)
        return new RowDataReader(sparkPartition);
      }
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(
        InputPartition partition) {
      SparkInputPartition sparkPartition = (SparkInputPartition) partition;
      return new BatchDataReader(
        sparkPartition,
        parquetBatchReadConf(),
        orcBatchReadConf()
      );
    }
  };
}
```

### 3.2 RowDataReader核心流程

```java
// 源码位置: RowDataReader.java:43
class RowDataReader extends BaseRowReader<FileScanTask>
    implements PartitionReader<InternalRow> {

  RowDataReader(SparkInputPartition partition) {
    super(
      partition.table(),
      partition.taskGroup(),
      SnapshotUtil.schemaFor(partition.table(), partition.branch()),
      partition.expectedSchema(),
      partition.isCaseSensitive(),
      partition.cacheDeleteFilesOnExecutors()
    );
    numSplits = taskGroup.tasks().size();
  }

  @Override
  protected CloseableIterator<InternalRow> open(FileScanTask task) {
    String filePath = task.file().location();

    // Step 1: 创建DeleteFilter
    SparkDeleteFilter deleteFilter = new SparkDeleteFilter(
      filePath,
      task.deletes(),
      counter(),
      true  // isRowReader
    );

    // Step 2: 获取需要读取的schema (可能包含删除列)
    Schema requiredSchema = deleteFilter.requiredSchema();
    Map<Integer, ?> idToConstant = constantsMap(task, requiredSchema);

    // Step 3: 更新InputFileBlockHolder (Spark的filename()函数)
    InputFileBlockHolder.set(filePath, task.start(), task.length());

    // Step 4: 打开文件并应用删除过滤
    return deleteFilter.filter(
      open(task, requiredSchema, idToConstant)
    ).iterator();
  }

  protected CloseableIterable<InternalRow> open(
      FileScanTask task,
      Schema readSchema,
      Map<Integer, ?> idToConstant) {

    if (task.isDataTask()) {
      // DataTask: 直接从内存数据创建
      return newDataIterable(task.asDataTask(), readSchema);
    } else {
      // FileScanTask: 从文件读取
      InputFile inputFile = getInputFile(task.file().location());
      return newIterable(
        inputFile,
        task.file().format(),
        task.start(),
        task.length(),
        task.residual(),      // 剩余谓词
        readSchema,
        idToConstant          // 常量列
      );
    }
  }
}
```

### 3.3 BaseReader文件格式分发

```java
// 源码位置: BaseRowReader.newIterable()
protected CloseableIterable<InternalRow> newIterable(
    InputFile inputFile,
    FileFormat format,
    long start,
    long length,
    Expression residual,
    Schema readSchema,
    Map<Integer, ?> idToConstant) {

  switch (format) {
    case PARQUET:
      return newParquetIterable(
        inputFile, start, length, residual, readSchema, idToConstant);

    case AVRO:
      return newAvroIterable(
        inputFile, start, length, residual, readSchema, idToConstant);

    case ORC:
      return newOrcIterable(
        inputFile, start, length, residual, readSchema, idToConstant);

    default:
      throw new UnsupportedOperationException(
        "Cannot read unknown format: " + format);
  }
}
```

---

## 四、文件格式优化详解

### 4.1 Parquet优化策略

#### 4.1.1 向量化读取 (Vectorized Read)

```java
// 源码位置: BaseBatchReader.newParquetIterable()
private CloseableIterable<ColumnarBatch> newParquetIterable(
    InputFile inputFile,
    long start,
    long length,
    Expression residual,
    Map<Integer, ?> idToConstant,
    SparkDeleteFilter deleteFilter) {

  Schema requiredSchema = deleteFilter != null ?
    deleteFilter.requiredSchema() : expectedSchema();

  return Parquet.read(inputFile)
    .project(requiredSchema)           // 列裁剪
    .split(start, length)              // 文件分片
    .createBatchedReaderFunc(fileSchema -> {
      if (parquetConf.readerType() == ParquetReaderType.COMET) {
        // Comet向量化读取 (原生C++实现)
        return VectorizedSparkParquetReaders.buildCometReader(
          requiredSchema, fileSchema, idToConstant, deleteFilter
        );
      } else {
        // Iceberg向量化读取
        return VectorizedSparkParquetReaders.buildReader(
          requiredSchema, fileSchema, idToConstant, deleteFilter
        );
      }
    })
    .recordsPerBatch(parquetConf.batchSize())  // 批大小 (默认4096)
    .filter(residual)                          // 剩余谓词下推
    .caseSensitive(caseSensitive())
    .reuseContainers()                         // 重用内存容器
    .withNameMapping(nameMapping())            // Schema演化支持
    .build();
}
```

**Parquet优化特性**:

1. **列裁剪 (Column Pruning)**:
   ```java
   // 仅读取需要的列
   Schema projection = SchemaUtil.select(fullSchema, neededColumns);
   Parquet.read(file).project(projection).build();

   // 示例:
   // Full schema: id, name, age, address, email
   // SELECT name, age -> 仅读取 name, age 两列
   ```

2. **谓词下推 (Predicate Pushdown)**:
   ```java
   // Parquet内部会使用RowGroup统计信息跳过不匹配的RowGroup
   Expression filter = Expressions.greaterThan("age", 30);
   Parquet.read(file)
     .filter(filter)  // 下推到Parquet Reader
     .build();

   // 流程:
   // 1. 读取Parquet Footer
   // 2. 检查每个RowGroup的min/max统计信息
   // 3. 如果 max(age) <= 30, 跳过整个RowGroup
   ```

3. **向量化执行 (Vectorization)**:
   ```
   传统行式读取:
   for (int i = 0; i < rows; i++) {
     InternalRow row = reader.next();
     // 处理单行
   }

   向量化批读取:
   ColumnarBatch batch = reader.nextBatch();
   // batch包含4096行 (可配置)
   // 列存储,利用SIMD指令加速
   for (int i = 0; i < batch.numRows(); i++) {
     // 批量处理
   }
   ```

4. **内存重用 (Container Reuse)**:
   ```java
   .reuseContainers()

   // Spark会eagerly消费ColumnarBatch
   // 允许重用底层内存,避免每次分配
   // 性能提升: ~20-30%
   ```

#### 4.1.2 Parquet Reader层级结构

```
VectorizedSparkParquetReaders.buildReader()
    │
    ├─> VectorizedColumnIterator
    │   ├─> PageReader (读取Parquet Page)
    │   ├─> ValuesReader (解码值)
    │   │   ├─ DictionaryValuesReader
    │   │   ├─ PlainValuesReader
    │   │   └─ DeltaEncodedValuesReader
    │   └─> DefinitionLevelReader (处理NULL)
    │
    └─> ColumnarBatch
        ├─ ColumnVector[] (每列一个)
        │  ├─ OnHeapColumnVector
        │  └─ OffHeapColumnVector
        └─ numRows: int
```

### 4.2 ORC优化策略

```java
// 源码位置: BaseBatchReader.newOrcIterable()
private CloseableIterable<ColumnarBatch> newOrcIterable(
    InputFile inputFile,
    long start,
    long length,
    Expression residual,
    Map<Integer, ?> idToConstant) {

  // ORC不支持常量列和元数据列,需要排除
  Set<Integer> constantFieldIds = idToConstant.keySet();
  Set<Integer> metadataFieldIds = MetadataColumns.metadataFieldIds();
  Schema schemaWithoutConstantAndMetadataFields =
    TypeUtil.selectNot(expectedSchema(),
      Sets.union(constantFieldIds, metadataFieldIds));

  return ORC.read(inputFile)
    .project(schemaWithoutConstantAndMetadataFields)
    .split(start, length)
    .createBatchedReaderFunc(fileSchema ->
      VectorizedSparkOrcReaders.buildReader(
        expectedSchema(),
        fileSchema,
        idToConstant
      )
    )
    .recordsPerBatch(orcConf.batchSize())  // 默认1024
    .filter(residual)
    .caseSensitive(caseSensitive())
    .withNameMapping(nameMapping())
    .build();
}
```

**ORC特有优化**:

1. **Stripe级别跳过**:
   ```
   ORC文件结构:
   ┌──────────────────┐
   │  File Header     │
   ├──────────────────┤
   │  Stripe 1        │
   │   - Index Stream │  <- 存储min/max统计
   │   - Data Stream  │
   ├──────────────────┤
   │  Stripe 2        │
   ├──────────────────┤
   │  File Footer     │
   │   - Stripe Stats │  <- 每个Stripe的统计信息
   └──────────────────┘

   跳过逻辑:
   for (Stripe stripe : stripes) {
     if (stripe.stats.max(col) < predicateValue) {
       skip(stripe);  // 跳过整个Stripe
     }
   }
   ```

2. **Bloom Filter索引**:
   ```java
   // ORC支持内建Bloom Filter
   ORC.read(file)
     .filter(Expressions.in("user_id", Arrays.asList(1, 2, 3)))
     .build();

   // ORC会检查Bloom Filter快速判断值是否存在
   ```

3. **ZSTD压缩优化**:
   ```
   ORC默认使用ZSTD压缩
   - 压缩率: ~3x
   - 解压速度: 500MB/s+
   - 比Snappy压缩率高30%,速度略慢10%
   ```

### 4.3 Avro优化策略

```java
// 源码位置: BaseRowReader.newAvroIterable()
protected CloseableIterable<InternalRow> newAvroIterable(
    InputFile inputFile,
    long start,
    long length,
    Expression residual,
    Schema readSchema,
    Map<Integer, ?> idToConstant) {

  return Avro.read(inputFile)
    .project(readSchema)
    .split(start, length)
    .createReaderFunc(fileSchema ->
      new SparkPlannedAvroReader(
        fileSchema,
        readSchema,
        idToConstant
      )
    )
    .filter(residual)
    .caseSensitive(caseSensitive())
    .withNameMapping(nameMapping())
    .build();
}
```

**Avro特点**:

1. **行式存储**: 不支持向量化读取
2. **Schema演化**: 通过`nameMapping`支持列名变更
3. **适用场景**: 流式写入,实时查询需求

### 4.4 格式性能对比

| 特性 | Parquet | ORC | Avro |
|------|---------|-----|------|
| 存储模式 | 列式 | 列式 | 行式 |
| 向量化读取 | ✅ 支持 | ✅ 支持 | ❌ 不支持 |
| 压缩率 | 高 (3-5x) | 高 (3-4x) | 中 (2-3x) |
| 读取速度 | 快 | 快 | 中等 |
| 谓词下推 | ✅ RowGroup级 | ✅ Stripe级 | ⚠️ 有限 |
| Schema演化 | ✅ 完整支持 | ✅ 完整支持 | ✅ 完整支持 |
| 索引支持 | RowGroup Stats | Stripe Stats + Bloom Filter | ❌ |
| 最佳场景 | OLAP分析 | OLAP分析 | 流式写入 |

**推荐配置**:
```sql
-- Parquet (推荐用于大多数OLAP场景)
CREATE TABLE catalog.db.table (
  id BIGINT,
  name STRING,
  data STRUCT<...>
)
USING iceberg
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'zstd',
  'write.parquet.row-group-size-bytes' = '134217728',  -- 128MB
  'write.parquet.page-size-bytes' = '1048576'          -- 1MB
);

-- ORC (Hive迁移场景)
TBLPROPERTIES (
  'write.format.default' = 'orc',
  'write.orc.compression-codec' = 'zstd',
  'write.orc.stripe-size-bytes' = '67108864'  -- 64MB
);
```

---

## 五、分区裁剪机制

### 5.1 静态分区裁剪 (Static Partition Pruning)

**发生时机**: 查询规划阶段 (Scan.planFiles()之前)

```java
// 示例查询
SELECT * FROM catalog.db.table
WHERE partition_col = '2023-10-22'
  AND regular_col > 100;

// Iceberg处理流程
TableScan scan = table.newScan()
  .filter(Expressions.and(
    Expressions.equal("partition_col", "2023-10-22"),  // 分区过滤
    Expressions.greaterThan("regular_col", 100)        // 数据过滤
  ));

// 内部执行:
// 1. Projections.inclusive(spec).project(filter)
//    -> 将 partition_col = '2023-10-22' 投影到分区
//
// 2. ManifestEvaluator.eval(manifest)
//    -> 检查 manifest.partitions[] 是否包含 '2023-10-22'
//    -> 如果不包含,跳过整个manifest
//
// 3. ManifestReader.filterPartitions(partitionFilter)
//    -> 在读取manifest entries时过滤分区
```

**优化效果**:
```
示例表: 1000个分区 (按日期), 每分区10个data files

查询: WHERE dt = '2023-10-22'

无分区裁剪:
- 扫描 1000 manifests
- 读取 10,000 data files
- 扫描时间: ~5s

静态分区裁剪:
- 扫描 1 manifest
- 读取 10 data files
- 扫描时间: ~50ms
- 性能提升: 100x
```

### 5.2 动态分区裁剪 (Dynamic Partition Pruning, DPP)

**触发条件**: Join查询中,一侧为小表,另一侧为大分区表

```sql
-- 示例查询
SELECT *
FROM fact_table f
JOIN dim_table d ON f.dim_id = d.id
WHERE d.region = 'us-west';

-- Spark执行计划:
-- 1. 扫描 dim_table, 筛选 region='us-west', 收集 dim_id 值
-- 2. 将 dim_id 值广播为 runtime filter
-- 3. 应用 runtime filter 到 fact_table 扫描
```

**Iceberg实现**:
```java
// 源码位置: SparkBatchQueryScan.filter()
@Override
public void filter(Predicate[] predicates) {
  Expression runtimeFilterExpr = convertRuntimeFilters(predicates);

  if (runtimeFilterExpr != Expressions.alwaysTrue()) {
    // Step 1: 创建分区Evaluator
    Map<Integer, Evaluator> evaluatorsBySpecId = Maps.newHashMap();
    for (PartitionSpec spec : specs()) {
      Expression inclusiveExpr = Projections.inclusive(spec, caseSensitive())
        .project(runtimeFilterExpr);
      Evaluator inclusive = new Evaluator(spec.partitionType(), inclusiveExpr);
      evaluatorsBySpecId.put(spec.specId(), inclusive);
    }

    // Step 2: 过滤已规划的tasks
    List<PartitionScanTask> filteredTasks = tasks().stream()
      .filter(task -> {
        Evaluator evaluator = evaluatorsBySpecId.get(task.spec().specId());
        return evaluator.eval(task.partition());  // 评估分区是否匹配
      })
      .collect(Collectors.toList());

    LOG.info(
      "{} of {} task(s) for table {} matched runtime filter {}",
      filteredTasks.size(),
      tasks().size(),
      table().name(),
      ExpressionUtil.toSanitizedString(runtimeFilterExpr)
    );

    // Step 3: 重置tasks (如果有效果)
    if (filteredTasks.size() < tasks().size()) {
      resetTasks(filteredTasks);  // 仅保留匹配的tasks
    }

    // 保存runtime filter用于equals/hashCode
    runtimeFilterExpressions.add(runtimeFilterExpr);
  }
}
```

**DPP优化效果**:
```
场景: Star Schema查询

fact_table: 1TB, 1000 partitions (partition_key)
dim_table: 10MB, 100 rows

Query:
SELECT * FROM fact_table f
JOIN dim_table d ON f.partition_key = d.key
WHERE d.category = 'electronics';  -- 筛选后仅10个key

无DPP:
- 扫描所有1000个分区
- 读取1TB数据
- 执行时间: ~10min

启用DPP:
- Runtime Filter: partition_key IN (k1, k2, ..., k10)
- 扫描10个分区
- 读取10GB数据
- 执行时间: ~1min
- 性能提升: 10x

配置:
spark.sql.optimizer.dynamicPartitionPruning.enabled=true
spark.sql.optimizer.dynamicPartitionPruning.useStats=true
spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio=0.5
```

### 5.3 SupportsRuntimeV2Filtering接口

```java
// SparkBatchQueryScan实现了SupportsRuntimeV2Filtering
interface SupportsRuntimeV2Filtering {
  /**
   * 返回可用于runtime filtering的属性
   * Spark Optimizer会查找这些属性的等值条件
   */
  NamedReference[] filterAttributes();

  /**
   * 应用runtime filters
   * @param predicates Spark下推的谓词 (通常是IN表达式)
   */
  void filter(Predicate[] predicates);
}

// Iceberg实现
@Override
public NamedReference[] filterAttributes() {
  Set<Integer> partitionFieldSourceIds = Sets.newHashSet();

  // 收集所有分区字段的sourceId
  for (PartitionSpec spec : specs()) {
    for (PartitionField field : spec.fields()) {
      partitionFieldSourceIds.add(field.sourceId());
    }
  }

  // 仅返回在readSchema中的分区字段
  return partitionFieldSourceIds.stream()
    .filter(fieldId -> expectedSchema().findField(fieldId) != null)
    .map(fieldId -> Spark3Util.toNamedReference(quotedNameById.get(fieldId)))
    .toArray(NamedReference[]::new);
}
```

---

## 六、Delete Files应用机制

### 6.1 SparkDeleteFilter架构

```java
// 源码位置: SparkDeleteFilter.java
class SparkDeleteFilter {
  private final String filePath;                    // 数据文件路径
  private final List<DeleteFile> deleteFiles;       // 关联的删除文件
  private final Counter counter;                    // 删除计数器
  private final boolean isRowReader;                // 行式/列式标识

  // 延迟初始化
  private Schema requiredSchema = null;
  private DeleteFilter<InternalRow> deletes = null;

  public Schema requiredSchema() {
    if (requiredSchema == null) {
      this.requiredSchema = computeRequiredSchema();
    }
    return requiredSchema;
  }

  private Schema computeRequiredSchema() {
    if (deleteFiles.isEmpty()) {
      return expectedSchema;
    }

    // 收集所有equality delete字段
    Set<Integer> equalityDeleteFieldIds = Sets.newHashSet();
    for (DeleteFile deleteFile : deleteFiles) {
      if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
        equalityDeleteFieldIds.addAll(deleteFile.equalityFieldIds());
      }
    }

    if (equalityDeleteFieldIds.isEmpty()) {
      // 仅有position deletes,无需额外列
      return expectedSchema;
    } else {
      // 需要读取equality delete字段
      return SchemaUtil.union(expectedSchema,
        SchemaUtil.select(tableSchema, equalityDeleteFieldIds));
    }
  }

  public CloseableIterable<InternalRow> filter(
      CloseableIterable<InternalRow> rows) {
    if (deleteFiles.isEmpty()) {
      return rows;  // 无删除文件,直接返回
    }

    if (deletes == null) {
      this.deletes = buildDeleteFilter();
    }

    return deletes.filter(rows);
  }

  private DeleteFilter<InternalRow> buildDeleteFilter() {
    // 按类型分组删除文件
    List<DeleteFile> posDeletes = Lists.newArrayList();
    List<DeleteFile> eqDeletes = Lists.newArrayList();
    DeleteFile dv = null;

    for (DeleteFile deleteFile : deleteFiles) {
      switch (deleteFile.content()) {
        case POSITION_DELETES:
          if (ContentFileUtil.isDV(deleteFile)) {
            dv = deleteFile;
          } else {
            posDeletes.add(deleteFile);
          }
          break;
        case EQUALITY_DELETES:
          eqDeletes.add(deleteFile);
          break;
      }
    }

    // 构建过滤器链
    if (dv != null) {
      // Deletion Vectors优先
      return new DeletionVectorFilter(dv, counter);
    } else {
      DeleteFilter<InternalRow> filter =
        new PositionDeleteFilter(filePath, posDeletes, counter);

      if (!eqDeletes.isEmpty()) {
        filter = new EqualityDeleteFilter(
          filePath,
          eqDeletes,
          tableSchema,
          expectedSchema,
          filter,
          counter
        );
      }

      return filter;
    }
  }
}
```

### 6.2 Position Deletes应用流程

```
Position Delete文件内容:
┌──────────────┬─────────────┐
│ file_path    │ pos         │
├──────────────┼─────────────┤
│ data1.parquet│ 100         │
│ data1.parquet│ 523         │
│ data1.parquet│ 1024        │
│ data2.parquet│ 42          │
└──────────────┴─────────────┘

应用逻辑:
class PositionDeleteFilter implements DeleteFilter<InternalRow> {
  private final Set<Long> deletedPositions;  // 缓存删除位置

  PositionDeleteFilter(String filePath, List<DeleteFile> posDeletes) {
    this.deletedPositions = loadDeletedPositions(filePath, posDeletes);
  }

  private Set<Long> loadDeletedPositions(
      String filePath,
      List<DeleteFile> posDeletes) {
    Set<Long> positions = Sets.newHashSet();

    for (DeleteFile deleteFile : posDeletes) {
      CloseableIterable<Record> deletes =
        openPositionDeletes(deleteFile);

      for (Record delete : deletes) {
        String deletePath = delete.getField("file_path");
        Long deletePos = delete.getField("pos");

        if (filePath.equals(deletePath)) {
          positions.add(deletePos);
        }
      }
    }

    return positions;
  }

  @Override
  public CloseableIterable<InternalRow> filter(
      CloseableIterable<InternalRow> rows) {
    return new FilteredIterable(rows) {
      private long currentPos = 0;

      @Override
      protected boolean shouldKeep(InternalRow row) {
        long pos = currentPos++;
        boolean isDeleted = deletedPositions.contains(pos);
        if (isDeleted) {
          counter.increment();  // 统计删除数量
        }
        return !isDeleted;
      }
    };
  }
}
```

**Position Delete优化**:

1. **Sorted Position Deletes**:
   ```
   如果position deletes按pos排序:
   - 使用双指针算法
   - 时间复杂度: O(N + M) vs O(N * log M)
   ```

2. **Position Delete Index**:
   ```java
   // 使用Roaring Bitmap存储删除位置
   RoaringBitmap deletedPositions = new RoaringBitmap();
   for (Long pos : positions) {
     deletedPositions.add(pos.intValue());
   }

   // 检查: O(1)
   boolean isDeleted = deletedPositions.contains(currentPos);
   ```

### 6.3 Equality Deletes应用流程

```
Equality Delete文件内容 (假设equality字段: id, name):
┌──────┬────────┐
│ id   │ name   │
├──────┼────────┤
│ 100  │ Alice  │
│ 523  │ Bob    │
│ 1024 │ Carol  │
└──────┴────────┘

应用逻辑:
class EqualityDeleteFilter implements DeleteFilter<InternalRow> {
  private final Set<InternalRow> deletedRows;  // 缓存删除行

  EqualityDeleteFilter(
      String filePath,
      List<DeleteFile> eqDeletes,
      Schema tableSchema,
      Schema expectedSchema,
      DeleteFilter<InternalRow> baseFilter) {

    this.deletedRows = loadDeletedRows(eqDeletes);
  }

  private Set<InternalRow> loadDeletedRows(List<DeleteFile> eqDeletes) {
    Set<InternalRow> rows = Sets.newHashSet();

    for (DeleteFile deleteFile : eqDeletes) {
      CloseableIterable<InternalRow> deletes =
        openEqualityDeletes(deleteFile);

      for (InternalRow delete : deletes) {
        // 仅保留equality字段
        InternalRow projected = projectEqualityFields(delete);
        rows.add(projected);
      }
    }

    return rows;
  }

  @Override
  public CloseableIterable<InternalRow> filter(
      CloseableIterable<InternalRow> rows) {
    // 先应用baseFilter (通常是PositionDeleteFilter)
    CloseableIterable<InternalRow> posFiltered = baseFilter.filter(rows);

    return new FilteredIterable(posFiltered) {
      @Override
      protected boolean shouldKeep(InternalRow row) {
        // 投影到equality字段
        InternalRow projected = projectEqualityFields(row);

        boolean isDeleted = deletedRows.contains(projected);
        if (isDeleted) {
          counter.increment();
        }
        return !isDeleted;
      }
    };
  }
}
```

**Equality Delete优化**:

1. **Equality Schema优化**:
   ```java
   // 仅读取必需的列
   Schema deleteSchema = deleteFile.equalitySchema();
   Schema dataReadSchema = SchemaUtil.union(
     expectedSchema,
     deleteSchema
   );
   ```

2. **Bloom Filter加速**:
   ```java
   // 对于大量equality deletes, 先用Bloom Filter快速判断
   BloomFilter<InternalRow> bloomFilter = buildBloomFilter(deletedRows);

   protected boolean shouldKeep(InternalRow row) {
     if (!bloomFilter.mightContain(row)) {
       return true;  // 快速路径: 一定不在删除集合中
     }
     return !deletedRows.contains(row);  // 慢路径: 精确检查
   }
   ```

### 6.4 Deletion Vectors (V3新特性)

```java
// Deletion Vector文件 (Puffin格式)
class DeletionVectorFilter implements DeleteFilter<InternalRow> {
  private final RoaringBitmap deletedPositions;

  DeletionVectorFilter(DeleteFile dv, Counter counter) {
    // 从Puffin文件读取RoaringBitmap
    this.deletedPositions = loadDeletionVector(dv);
  }

  private RoaringBitmap loadDeletionVector(DeleteFile dv) {
    InputFile file = fileIO.newInputFile(dv.location());
    PuffinReader reader = Puffin.read(file).build();

    // Puffin文件包含多个blob
    for (Blob blob : reader.blobs()) {
      if (blob.type().equals("deletion-vector")) {
        // 反序列化RoaringBitmap
        return RoaringBitmap.deserialize(blob.inputStream());
      }
    }

    throw new IllegalStateException("No deletion vector found");
  }

  @Override
  public CloseableIterable<InternalRow> filter(
      CloseableIterable<InternalRow> rows) {
    return new FilteredIterable(rows) {
      private long currentPos = 0;

      @Override
      protected boolean shouldKeep(InternalRow row) {
        long pos = currentPos++;
        boolean isDeleted = deletedPositions.contains((int) pos);
        if (isDeleted) {
          counter.increment();
        }
        return !isDeleted;
      }
    };
  }
}
```

**Deletion Vector优势**:

1. **压缩存储**: RoaringBitmap压缩率极高
   ```
   100万删除位置:
   - Position Deletes文件: ~16MB (每条16字节)
   - Deletion Vector: ~100KB (RoaringBitmap压缩)
   - 压缩比: 160:1
   ```

2. **快速查询**: O(1)时间复杂度
   ```java
   // RoaringBitmap内部使用分段数组
   boolean contains(int position) // O(log N) ~ O(1)
   ```

3. **增量更新**: 支持位图合并
   ```java
   RoaringBitmap merged = RoaringBitmap.or(dv1, dv2);
   ```

---

## 七、延迟物化优化

### 7.1 延迟物化概念

**定义**: 延迟读取非过滤列,直到确认行通过所有过滤条件后才读取完整数据。

```
传统读取:
1. 读取所有列 (id, name, age, address, email)
2. 应用过滤: age > 30
3. 投影: SELECT name, email

延迟物化:
1. 读取过滤列: age
2. 应用过滤: age > 30
3. 读取投影列: name, email (仅对通过的行)
```

### 7.2 Iceberg延迟物化实现

**Parquet延迟物化**:
```java
// VectorizedSparkParquetReaders.buildReader()
public VectorizedReader<InternalRow> buildReader(
    Schema expectedSchema,
    Schema fileSchema,
    Map<Integer, ?> idToConstant,
    SparkDeleteFilter deleteFilter) {

  if (deleteFilter != null) {
    // 有delete files时,使用延迟物化
    return buildLateMateria​lizedReader(
      expectedSchema,
      fileSchema,
      idToConstant,
      deleteFilter
    );
  } else {
    // 无delete files,直接读取
    return buildStandardReader(
      expectedSchema,
      fileSchema,
      idToConstant
    );
  }
}

private VectorizedReader<InternalRow> buildLateMaterializedReader(
    Schema expectedSchema,
    Schema fileSchema,
    Map<Integer, ?> idToConstant,
    SparkDeleteFilter deleteFilter) {

  // Step 1: 确定需要的列
  Schema requiredSchema = deleteFilter.requiredSchema();
  // requiredSchema = expectedSchema + equality delete字段

  // Step 2: 分离过滤列和投影列
  Set<Integer> filterFieldIds = Sets.newHashSet();
  for (DeleteFile df : deleteFilter.deleteFiles()) {
    if (df.content() == FileContent.EQUALITY_DELETES) {
      filterFieldIds.addAll(df.equalityFieldIds());
    }
  }

  List<Types.NestedField> filterFields = Lists.newArrayList();
  List<Types.NestedField> projectionFields = Lists.newArrayList();

  for (Types.NestedField field : requiredSchema.columns()) {
    if (filterFieldIds.contains(field.fieldId())) {
      filterFields.add(field);
    } else {
      projectionFields.add(field);
    }
  }

  // Step 3: 创建两阶段Reader
  return new TwoPhaseVectorizedReader(
    fileSchema,
    Schema.of(filterFields),      // 第一阶段读取
    Schema.of(projectionFields),  // 第二阶段读取
    deleteFilter
  );
}
```

**TwoPhaseVectorizedReader流程**:
```java
class TwoPhaseVectorizedReader implements VectorizedReader<InternalRow> {
  private final VectorizedReader<ColumnarBatch> phase1Reader;
  private final VectorizedReader<ColumnarBatch> phase2Reader;
  private final SparkDeleteFilter deleteFilter;

  @Override
  public ColumnarBatch read() {
    // Phase 1: 读取过滤列
    ColumnarBatch filterBatch = phase1Reader.read();
    int numRows = filterBatch.numRows();

    // 应用delete filter, 标记保留的行
    boolean[] keepRows = new boolean[numRows];
    int keepCount = 0;

    for (int i = 0; i < numRows; i++) {
      InternalRow row = filterBatch.getRow(i);
      if (deleteFilter.shouldKeep(row)) {
        keepRows[i] = true;
        keepCount++;
      }
    }

    if (keepCount == 0) {
      // 所有行都被删除,跳过phase2
      return emptyBatch();
    }

    if (keepCount == numRows) {
      // 所有行都保留,正常读取phase2
      ColumnarBatch projectionBatch = phase2Reader.read();
      return combineBatches(filterBatch, projectionBatch);
    }

    // Phase 2: 仅读取保留行的投影列
    ColumnarBatch sparseBatch = phase2Reader.readSparse(keepRows);
    return combineBatches(filterBatch, sparseBatch);
  }
}
```

### 7.3 延迟物化性能收益

```
测试场景:
- 表schema: id, name, age, address, email, description (6列)
- 数据量: 10亿行
- 过滤条件: age > 30 (过滤掉50%行)
- 投影: SELECT name, email

传统读取:
- 读取所有6列
- 数据量: 10GB
- 时间: 60s

延迟物化:
- Phase1: 读取age (1列)
- Phase2: 读取name, email (2列,仅50%行)
- 数据量: 2GB + 1GB = 3GB
- 时间: 20s
- 性能提升: 3x

最佳场景:
- 过滤率高 (>50%)
- 投影列少
- 宽表 (列数多)
```

---

## 八、Metrics监控体系

### 8.1 ScanMetrics层级结构

```
ScanMetrics (接口)
    │
    ├─> ScanReport (扫描报告)
    │   ├─ table(): Table
    │   ├─ snapshotId(): long
    │   ├─ filter(): Expression
    │   ├─ scanMetrics(): ScanMetrics
    │   └─ projectedFieldNames(): List<String>
    │
    └─> 具体指标
        ├─ totalPlanningDuration: Timer
        ├─ resultDataFiles: Counter
        ├─ resultDeleteFiles: Counter
        ├─ scannedDataManifests: Counter
        ├─ skippedDataManifests: Counter
        ├─ totalDataManifests: Counter
        ├─ totalDeleteManifests: Counter
        ├─ scannedDeleteManifests: Counter
        ├─ skippedDeleteManifests: Counter
        ├─ totalFileSizeInBytes: Counter
        ├─ totalDeleteFileSizeInBytes: Counter
        ├─ skippedDataFiles: Counter
        ├─ skippedDeleteFiles: Counter
        └─ indexedDeleteFiles: Counter
```

### 8.2 Spark Task Metrics

```java
// 源码位置: RowDataReader.currentMetricsValues()
@Override
public CustomTaskMetric[] currentMetricsValues() {
  return new CustomTaskMetric[] {
    new TaskNumSplits(numSplits),      // 任务分片数
    new TaskNumDeletes(counter().get()) // 删除行数
  };
}

// CustomTaskMetric实现
class TaskNumSplits implements CustomTaskMetric {
  private final long numSplits;

  @Override
  public String name() {
    return "num_splits";
  }

  @Override
  public long value() {
    return numSplits;
  }
}

class TaskNumDeletes implements CustomTaskMetric {
  private final long numDeletes;

  @Override
  public String name() {
    return "num_deletes";
  }

  @Override
  public long value() {
    return numDeletes;
  }
}
```

### 8.3 完整Metrics示例

```
Spark UI - SQL Tab - Scan Node:

IcebergScan(table=catalog.db.table)
├─ number of files read: 156
├─ number of files skipped: 844
├─ data files size: 23.5 GB
├─ delete files size: 342 MB
├─ number of delete files: 45
├─ scan planning time: 1.2s
├─ rows scanned: 1,250,000,000
├─ rows deleted: 12,500,000
├─ number of splits: 234
└─ custom metrics:
    ├─ num_splits: 234
    └─ num_deletes: 12,500,000

Manifest Level:
├─ total data manifests: 50
├─ scanned data manifests: 12
├─ skipped data manifests: 38
├─ total delete manifests: 10
├─ scanned delete manifests: 5
└─ skipped delete manifests: 5
```

### 8.4 Metrics收集源码

```java
// 源码位置: ScanMetricsUtil.java
public class ScanMetricsUtil {
  // 记录文件任务
  public static void fileTask(
      ScanMetrics metrics,
      DataFile dataFile,
      DeleteFile[] deleteFiles) {
    metrics.resultDataFiles().increment();
    metrics.totalFileSizeInBytes().increment(dataFile.fileSizeInBytes());

    if (deleteFiles != null && deleteFiles.length > 0) {
      metrics.resultDeleteFiles().increment(deleteFiles.length);
      for (DeleteFile deleteFile : deleteFiles) {
        metrics.totalDeleteFileSizeInBytes()
          .increment(deleteFile.fileSizeInBytes());
      }
    }
  }

  // 记录跳过的数据文件
  public static void skippedDataFile(ScanMetrics metrics) {
    metrics.skippedDataFiles().increment();
  }

  // 记录索引的删除文件
  public static void indexedDeleteFile(
      ScanMetrics metrics,
      DeleteFile deleteFile) {
    metrics.indexedDeleteFiles().increment();
  }
}
```

### 8.5 性能诊断指南

**高延迟场景诊断**:

1. **Planning Time过高** (> 5s):
   ```
   可能原因:
   - Manifest文件过多 (>1000)
   - Delete manifests未合并
   - 未启用并行扫描

   解决方案:
   - 执行 rewrite_manifests procedure
   - 设置 read.split.planning-lookback=10
   - 启用 planWith(executorService)
   ```

2. **Skipped Files过少** (< 50%):
   ```
   可能原因:
   - 分区设计不合理
   - 缺少合适的过滤条件
   - 统计信息不准确

   解决方案:
   - 重新设计分区策略
   - 添加分区过滤条件
   - 执行 rewrite_data_files 更新统计信息
   ```

3. **Delete Files Size过大** (> 10% of data size):
   ```
   可能原因:
   - 删除操作频繁
   - 未执行compaction

   解决方案:
   - 执行 rewrite_data_files procedure
   - 配置自动compaction
   - 考虑使用Deletion Vectors (V3)
   ```

---

## 九、完整读取流程时序图

```
┌─────────┐  ┌──────────┐  ┌─────────────┐  ┌──────────────┐  ┌────────────┐  ┌──────────┐
│ Spark   │  │  Scan    │  │ Manifest    │  │  Partition   │  │   Reader   │  │  Delete  │
│ Driver  │  │ Builder  │  │   Group     │  │   Reader     │  │  Factory   │  │  Filter  │
└────┬────┘  └────┬─────┘  └──────┬──────┘  └──────┬───────┘  └─────┬──────┘  └────┬─────┘
     │            │                │                │                │              │
     │ 1. spark.table()            │                │                │              │
     ├───────────>│                │                │                │              │
     │            │                │                │                │              │
     │ 2. filter().select()        │                │                │              │
     ├───────────>│                │                │                │              │
     │            │                │                │                │              │
     │ 3. build() │                │                │                │              │
     ├───────────>│                │                │                │              │
     │            │ new SparkBatchQueryScan()       │                │              │
     │            ├───────────────>│                │                │              │
     │            │                │                │                │              │
     │ 4. planInputPartitions()    │                │                │              │
     ├───────────>│                │                │                │              │
     │            │ planFiles()    │                │                │              │
     │            ├───────────────>│                │                │              │
     │            │                │ 扫描Manifests  │                │              │
     │            │                ├──────────────>│                │              │
     │            │                │ 过滤DataFiles  │                │              │
     │            │                │<───────────────┤                │              │
     │            │                │ 构建DeleteIndex│                │              │
     │            │                │ 生成FileScanTask                │              │
     │            │<───────────────┤                │                │              │
     │            │                │                │                │              │
     │            │ taskGroups()   │                │                │              │
     │            │ (bin-packing)  │                │                │              │
     │            │                │                │                │              │
     │            │ 创建InputPartition[]            │                │              │
     │<───────────┤                │                │                │              │
     │ [partition1, partition2, ...]                │                │              │
     │            │                │                │                │              │
     │ 5. 调度Tasks到Executors      │                │                │              │
     │──────────────────────────────────────────────────────────────>│              │
     │            │                │                │ 6. createReader(partition)    │
     │            │                │                │<───────────────┤              │
     │            │                │                │                │              │
     │            │                │                │  new RowDataReader/BatchDataReader
     │            │                │                │                │              │
     │            │                │                │ 7. next()      │              │
     │            │                │                │<───────────────┤              │
     │            │                │                │   open(task)   │              │
     │            │                │                │                │ new SparkDeleteFilter
     │            │                │                │                ├─────────────>│
     │            │                │                │                │ requiredSchema()
     │            │                │                │                │<──────────────┤
     │            │                │                │                │              │
     │            │                │                │  打开Parquet/ORC/Avro文件      │
     │            │                │                │  (向量化/行式读取)              │
     │            │                │                │                │              │
     │            │                │                │ 8. 应用DeleteFilter            │
     │            │                │                │                ├─────────────>│
     │            │                │                │                │ filter(rows) │
     │            │                │                │                │              │
     │            │                │                │                │ Position/Equality/DV
     │            │                │                │                │ 过滤删除行    │
     │            │                │                │                │<──────────────┤
     │            │                │                │                │ filtered rows│
     │            │                │                │<───────────────┤              │
     │            │                │                │  InternalRow   │              │
     │<──────────────────────────────────────────────────────────────┤              │
     │  返回数据到Spark                │                │                │              │
     │            │                │                │                │              │
```

---

## 十、性能调优最佳实践

### 10.1 读取配置优化

```python
# Spark配置
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
spark.conf.set("spark.sql.files.openCostInBytes", "4194304")      # 4MB
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Iceberg配置
spark.read \
  .option("read.split.target-size", "134217728") \       # 128MB split
  .option("read.split.planning-lookback", "10") \        # bin-packing回溯
  .option("read.split.open-file-cost", "4194304") \      # 4MB打开成本
  .option("read.preserve-data-grouping", "true") \       # 保留数据分组
  .option("vectorization.enabled", "true") \             # 启用向量化
  .option("parquet.vectorization.batch-size", "4096") \  # 批大小
  .option("read.parquet.vectorization.enabled", "true") \
  .table("catalog.db.table")
```

### 10.2 分区设计最佳实践

```sql
-- ❌ 错误: 分区过细
CREATE TABLE catalog.db.table (
  id BIGINT,
  ts TIMESTAMP,
  data STRING
)
PARTITIONED BY (hours(ts));  -- 每小时一个分区

问题:
- 小文件过多
- Manifest碎片化
- 规划时间长

-- ✅ 正确: 合理分区粒度
CREATE TABLE catalog.db.table (
  id BIGINT,
  ts TIMESTAMP,
  region STRING,
  data STRING
)
PARTITIONED BY (
  days(ts),              -- 按天分区
  bucket(16, region)     -- 16个region bucket
);

优点:
- 平衡文件大小
- 良好的数据本地性
- 快速规划
```

### 10.3 Delete Files优化

```sql
-- 场景1: 大量小删除 -> 定期Compaction
CALL catalog.system.rewrite_data_files(
  table => 'catalog.db.table',
  strategy => 'sort',
  sort_order => 'id ASC',
  where => 'dt >= current_date() - INTERVAL 7 DAYS'
);

-- 场景2: 启用Deletion Vectors (V3)
ALTER TABLE catalog.db.table
SET TBLPROPERTIES (
  'format-version' = '3',
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode' = 'merge-on-read'
);

-- 场景3: 控制删除文件数量
ALTER TABLE catalog.db.table
SET TBLPROPERTIES (
  'write.delete.distribution-mode' = 'hash',
  'write.delete.max-files-per-partition' = '10'
);
```

### 10.4 向量化读取优化

```python
# 配置向量化批大小
spark.conf.set("spark.sql.parquet.columnarReaderBatchSize", "4096")
spark.conf.set("spark.sql.orc.columnarReaderBatchSize", "1024")

# Parquet向量化配置
spark.read \
  .option("parquet.vectorization.enabled", "true") \
  .option("parquet.vectorization.batch-size", "4096") \
  .table("catalog.db.table")

# 使用Comet (原生C++向量化引擎)
spark.read \
  .option("spark.comet.enabled", "true") \
  .option("spark.comet.exec.enabled", "true") \
  .table("catalog.db.table")
```

### 10.5 动态分区裁剪优化

```sql
-- 启用DPP
SET spark.sql.optimizer.dynamicPartitionPruning.enabled=true;
SET spark.sql.optimizer.dynamicPartitionPruning.useStats=true;
SET spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio=0.5;

-- 示例查询
SELECT /*+ BROADCAST(d) */ f.*
FROM fact_table f
JOIN dim_table d ON f.dim_key = d.key
WHERE d.category = 'electronics';

-- Explain结果:
-- DynamicPruningExpression (dim_key IN dynamicpruning#123)
```

### 10.6 Metrics监控与诊断

```python
# 获取ScanReport
scan_report = spark.read \
  .option("scan-report.enabled", "true") \
  .table("catalog.db.table") \
  .explain("cost")

# 检查关键指标
if scan_report.metrics.skipped_data_manifests < total_manifests * 0.5:
    print("警告: 分区裁剪效果差, 考虑优化分区设计")

if scan_report.metrics.total_delete_file_size > total_data_size * 0.1:
    print("警告: Delete files过多, 执行Compaction")

if scan_report.metrics.total_planning_duration > 5000:  # 5秒
    print("警告: 规划时间过长, 考虑并行规划或合并Manifests")
```

---

## 十一、关键源码位置索引

| 组件 | 源码路径 | 核心方法 |
|------|---------|---------|
| SparkBatchQueryScan | spark/v3.5/.../SparkBatchQueryScan.java:65 | filter():127<br>estimateStatistics():216 |
| SparkPartitioningAwareScan | spark/v3.5/.../SparkPartitioningAwareScan.java:59 | tasks():172<br>taskGroups():197 |
| SparkInputPartition | spark/v3.5/.../SparkInputPartition.java:33 | partitionKey():70<br>preferredLocations():65 |
| RowDataReader | spark/v3.5/.../RowDataReader.java:43 | open():86 |
| BatchDataReader | spark/v3.5/.../BatchDataReader.java:43 | open():101 |
| BaseBatchReader | spark/v3.5/.../BaseBatchReader.java:43 | newParquetIterable():83<br>newOrcIterable():117 |
| SparkDeleteFilter | spark/v3.5/.../SparkDeleteFilter.java | requiredSchema()<br>filter() |
| VectorizedSparkParquetReaders | spark/v3.5/.../VectorizedSparkParquetReaders.java | buildReader()<br>buildCometReader() |

---

## 十二、总结

### 12.1 核心设计理念

1. **DataSource V2深度集成**: 充分利用Spark的分区感知、统计信息、Runtime Filtering
2. **多层优化策略**: Planning阶段(Manifest过滤) → Execution阶段(Delete Filter) → Format阶段(向量化)
3. **格式自适应**: Parquet/ORC/Avro不同优化路径
4. **灵活任务分配**: 支持数据分组保留和负载均衡两种模式

### 12.2 性能优化关键路径

1. **减少扫描数据量**: 分区裁剪(静态+动态) → Manifest过滤 → File统计信息过滤
2. **加速数据读取**: 向量化读取 → 列裁剪 → 谓词下推 → 延迟物化
3. **高效删除处理**: Deletion Vectors → Position Delete索引 → Equality Delete Bloom Filter
4. **合理任务分配**: 数据本地性 → Bin-packing → 并行度控制

### 12.3 监控与诊断

- **Planning Metrics**: 跟踪Manifest/File跳过率
- **Execution Metrics**: 监控删除行数、分片数量
- **Custom Metrics**: num_splits, num_deletes

---

**文档版本**: v1.0
**生成时间**: 2025-10-22
**适用版本**: Apache Iceberg 1.10.x + Spark 3.5
**作者**: 基于源码深度分析生成
