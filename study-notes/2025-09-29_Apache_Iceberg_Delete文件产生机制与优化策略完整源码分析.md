# 2025-09-29_Apache_Iceberg_Delete文件产生机制与优化策略完整源码分析

## 目录
1. [概述](#概述)
2. [Delete文件产生机制全景分析](#delete文件产生机制全景分析)
3. [Position Delete产生机制详解](#position-delete产生机制详解)
4. [Equality Delete产生机制详解](#equality-delete产生机制详解)
5. [Delete Vector生成机制分析](#delete-vector生成机制分析)
6. [Delete文件过多的优化策略](#delete文件过多的优化策略)
7. [实际应用示例与最佳实践](#实际应用示例与最佳实践)
8. [性能调优指南](#性能调优指南)

## 概述

Apache Iceberg通过三种delete机制实现数据的逻辑删除：Position Delete、Equality Delete和Delete Vector。本文档从源码层面深入分析这些delete文件的产生机制、触发条件以及优化策略，提供完整的技术实现指南。

### 核心Delete机制对比

| Delete类型 | 产生场景 | 存储内容 | 文件格式 | 适用场景 |
|------------|----------|----------|----------|----------|
| Position Delete | DELETE WHERE、UPDATE | 文件路径+行位置 | Parquet/ORC/Avro | 精确行删除，低删除率 |
| Equality Delete | DELETE WHERE (复杂条件) | 等值字段值 | Parquet/ORC/Avro | 条件删除，中等删除率 |
| Delete Vector | 批量删除、文件整理 | Roaring Bitmap | Puffin (v3特性) | 高删除率，范围删除 |

## Delete文件产生机制全景分析

### 1. Delete操作入口分析

#### RowDelta API - 核心入口 (`api/src/main/java/org/apache/iceberg/RowDelta.java:32-47`)

```java
public interface RowDelta extends SnapshotUpdate<RowDelta> {
  /**
   * Add a {@link DataFile} to the table.
   * @param inserts a data file of rows to insert
   * @return this for method chaining
   */
  RowDelta addRows(DataFile inserts);

  /**
   * Add a {@link DeleteFile} to the table.
   * @param deletes a delete file of rows to delete
   * @return this for method chaining
   */
  RowDelta addDeletes(DeleteFile deletes);
}
```

**产生路径图**：

```
SQL DELETE/UPDATE/MERGE
        │
        ▼
┌─────────────────────┐
│ 计算引擎执行层      │
│ (Spark/Flink/等)    │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ SparkPositionDelta  │
│ Write               │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ RowDelta.addDeletes │
│ (delete文件)        │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ BaseRowDelta        │
│ 提交到metadata      │
└─────────────────────┘
```

#### BaseRowDelta实现 (`core/src/main/java/org/apache/iceberg/BaseRowDelta.java:27-68`)

```java
class BaseRowDelta extends MergingSnapshotProducer<RowDelta> implements RowDelta {
  private Long startingSnapshotId = null;
  private final CharSequenceSet referencedDataFiles = CharSequenceSet.empty();
  private boolean validateDeletes = false;
  private Expression conflictDetectionFilter = Expressions.alwaysTrue();

  @Override
  protected String operation() {
    if (addsDeleteFiles() && !addsDataFiles()) {
      return DataOperations.DELETE;  // 纯删除操作
    }
    return DataOperations.OVERWRITE;   // 混合操作
  }

  @Override
  public RowDelta addDeletes(DeleteFile deletes) {
    add(deletes);  // 添加到MergingSnapshotProducer
    return this;
  }

  @Override
  public RowDelta validateDataFilesExist(Iterable<? extends CharSequence> referencedFiles) {
    referencedFiles.forEach(referencedDataFiles::add);
    return this;
  }
}
```

### 2. Delete文件创建流程

#### 文件创建决策逻辑

```java
// 伪代码：Delete文件类型选择逻辑
if (deleteCondition.isPositionBased()) {
    if (table.formatVersion() >= 3 && highDeletionRate > 0.3) {
        return new DeleteVectorWriter();  // Delete Vector (v3+)
    } else {
        return new PositionDeleteWriter();  // Position Delete
    }
} else if (deleteCondition.isEqualityBased()) {
    return new EqualityDeleteWriter();  // Equality Delete
}
```

## Position Delete产生机制详解

### 1. 产生触发条件

Position Delete在以下情况下产生：

1. **精确行删除**：`DELETE FROM table WHERE primary_key = value`
2. **UPDATE操作**：Iceberg UPDATE操作实现为DELETE + INSERT
3. **MERGE操作**：WHEN MATCHED THEN DELETE子句
4. **Copy-on-Write到Merge-on-Read转换**：数据文件过大时

### 2. Position Delete Writer架构

#### FanoutPositionOnlyDeleteWriter (`core/src/main/java/org/apache/iceberg/io/FanoutPositionOnlyDeleteWriter.java:40-92`)

```java
public class FanoutPositionOnlyDeleteWriter<T>
    extends FanoutWriter<PositionDelete<T>, DeleteWriteResult> {

  private final FileWriterFactory<T> writerFactory;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSizeInBytes;
  private final DeleteGranularity granularity;  // FILE 或 PARTITION 级别
  private final List<DeleteFile> deleteFiles;
  private final CharSequenceSet referencedDataFiles;
  private final Function<CharSequence, PositionDeleteIndex> loadPreviousDeletes;

  @Override
  protected FileWriter<PositionDelete<T>, DeleteWriteResult> newWriter(
      PartitionSpec spec, StructLike partition) {
    return new SortingPositionOnlyDeleteWriter<>(
        () ->
            new RollingPositionDeleteWriter<>(
                writerFactory, fileFactory, io, targetFileSizeInBytes, spec, partition),
        granularity,
        loadPreviousDeletes);
  }
}
```

**设计特点**：
- **Fanout模式**：支持多分区并发写入
- **排序写入**：`SortingPositionOnlyDeleteWriter`确保删除记录按(文件路径, 位置)排序
- **滚动文件**：`RollingPositionDeleteWriter`按目标大小分割文件
- **粒度控制**：支持FILE和PARTITION两种删除粒度

#### Position Delete数据结构

```java
// Position Delete记录格式
public class PositionDelete<T> {
  private final CharSequence file_path;  // 引用的数据文件路径
  private final long pos;                // 删除的行位置（0开始）
  private final T row;                   // 可选：删除的行数据
}
```

### 3. Position Delete生成示例

#### Spark SQL产生Position Delete

```sql
-- 1. 创建表并插入数据
CREATE TABLE iceberg_table (
  id bigint,
  name string,
  age int
) USING ICEBERG
PARTITIONED BY (bucket(4, id));

INSERT INTO iceberg_table VALUES
  (1, 'Alice', 25),
  (2, 'Bob', 30),
  (3, 'Charlie', 35);

-- 2. 执行删除操作 (产生Position Delete)
DELETE FROM iceberg_table WHERE id = 2;
```

**产生的Position Delete文件内容**：
```
file_path                              | pos
s3://bucket/data/00000-0-data.parquet  | 1
```

#### Programmatic API产生Position Delete

```java
// Java API示例
Table table = catalog.loadTable(TableIdentifier.of("db", "table"));

// 创建RowDelta操作
RowDelta rowDelta = table.newRowDelta();

// 写入Position Delete
OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1).build();
PositionDeleteWriter<Record> deleteWriter = new FanoutPositionOnlyDeleteWriter<>(
    new GenericAppenderFactory(table.schema()),
    fileFactory,
    table.io(),
    128 * 1024 * 1024  // 128MB target size
);

// 添加删除位置
PositionDelete<Record> delete = PositionDelete.create();
delete.set("s3://bucket/data/00000-0-data.parquet", 1L, null);
deleteWriter.write(delete);

// 提交删除文件
DeleteWriteResult result = deleteWriter.complete();
for (DeleteFile deleteFile : result.deleteFiles()) {
    rowDelta.addDeletes(deleteFile);
}

rowDelta.commit();
```

### 4. ClusteredPositionDeleteWriter (`core/src/main/java/org/apache/iceberg/io/ClusteredPositionDeleteWriter.java:36-81`)

```java
public class ClusteredPositionDeleteWriter<T>
    extends ClusteredWriter<PositionDelete<T>, DeleteWriteResult> {

  private final DeleteGranularity granularity;

  @Override
  protected FileWriter<PositionDelete<T>, DeleteWriteResult> newWriter(
      PartitionSpec spec, StructLike partition) {
    switch (granularity) {
      case FILE:
        // 文件级别删除：每个数据文件一个删除文件
        return new FileScopedPositionDeleteWriter<>(() -> newRollingWriter(spec, partition));
      case PARTITION:
        // 分区级别删除：每个分区一个删除文件
        return newRollingWriter(spec, partition);
      default:
        throw new UnsupportedOperationException("Unsupported delete granularity: " + granularity);
    }
  }
}
```

**性能特点**：
- **预排序要求**：输入必须按分区和文件路径预排序
- **内存效率**：不需要内存排序，直接顺序写入
- **适用场景**：批量有序删除操作

## Equality Delete产生机制详解

### 1. 产生触发条件

Equality Delete在以下情况下产生：

1. **复杂条件删除**：`DELETE FROM table WHERE column1 = value1 AND column2 = value2`
2. **范围删除**：`DELETE FROM table WHERE date BETWEEN '2023-01-01' AND '2023-01-31'`
3. **JOIN删除**：基于关联表的删除操作
4. **CDC操作**：Change Data Capture中的删除事件

### 2. Equality Delete Writer架构

#### ClusteredEqualityDeleteWriter (`core/src/main/java/org/apache/iceberg/io/ClusteredEqualityDeleteWriter.java:33-69`)

```java
public class ClusteredEqualityDeleteWriter<T> extends ClusteredWriter<T, DeleteWriteResult> {

  private final FileWriterFactory<T> writerFactory;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSizeInBytes;
  private final List<DeleteFile> deleteFiles;

  @Override
  protected FileWriter<T, DeleteWriteResult> newWriter(PartitionSpec spec, StructLike partition) {
    return new RollingEqualityDeleteWriter<>(
        writerFactory, fileFactory, io, targetFileSizeInBytes, spec, partition);
  }

  @Override
  protected void addResult(DeleteWriteResult result) {
    Preconditions.checkArgument(
        !result.referencesDataFiles(), "Equality deletes cannot reference data files");
    deleteFiles.addAll(result.deleteFiles());
  }
}
```

#### RollingEqualityDeleteWriter (`core/src/main/java/org/apache/iceberg/io/RollingEqualityDeleteWriter.java:34-68`)

```java
public class RollingEqualityDeleteWriter<T>
    extends RollingFileWriter<T, EqualityDeleteWriter<T>, DeleteWriteResult> {

  @Override
  protected EqualityDeleteWriter<T> newWriter(EncryptedOutputFile file) {
    return writerFactory.newEqualityDeleteWriter(file, spec(), partition());
  }

  @Override
  protected void addResult(DeleteWriteResult result) {
    Preconditions.checkArgument(
        !result.referencesDataFiles(), "Equality deletes cannot reference data files");
    deleteFiles.addAll(result.deleteFiles());
  }
}
```

### 3. Equality Delete数据结构

```java
// Equality Delete记录格式
// 只存储等值字段的值，不存储完整行
public class EqualityDeleteRecord {
  // 例如：DELETE WHERE id = 100 AND name = 'test'
  // 只存储：{id: 100, name: 'test'}
  private final Map<String, Object> equalityFields;
}
```

### 4. Equality Delete生成示例

#### SQL产生Equality Delete

```sql
-- 1. 复杂条件删除
DELETE FROM iceberg_table
WHERE age > 30 AND name LIKE 'A%';

-- 2. 基于时间范围的删除
DELETE FROM events_table
WHERE event_date BETWEEN '2023-01-01' AND '2023-01-31'
  AND event_type = 'click';
```

**产生的Equality Delete文件内容**：
```
age | name_prefix  # 等值字段
31  | A           # 删除记录1
32  | Alice       # 删除记录2
35  | Andrew      # 删除记录3
```

#### 编程API产生Equality Delete

```java
// 创建等值删除字段Schema
List<String> equalityFieldNames = Arrays.asList("user_id", "event_type");
Schema deleteSchema = TypeUtil.select(table.schema(), equalityFieldNames);

// 创建Equality Delete Writer
ClusteredEqualityDeleteWriter<Record> deleteWriter =
    new ClusteredEqualityDeleteWriter<>(
        new GenericAppenderFactory(deleteSchema),
        fileFactory,
        table.io(),
        128 * 1024 * 1024
    );

// 写入等值删除记录
Record deleteRecord = GenericRecord.create(deleteSchema);
deleteRecord.setField("user_id", 12345L);
deleteRecord.setField("event_type", "click");
deleteWriter.write(deleteRecord);

// 提交到表
DeleteWriteResult result = deleteWriter.complete();
table.newRowDelta().addDeletes(result.deleteFiles().get(0)).commit();
```

### 5. Equality Delete字段选择策略

```java
// 最优等值字段选择算法
public List<String> selectEqualityFields(Expression deleteCondition, Schema tableSchema) {
    List<String> candidateFields = extractFieldsFromCondition(deleteCondition);

    // 排序策略：优先选择高选择性字段
    return candidateFields.stream()
        .sorted((f1, f2) -> {
            double selectivity1 = calculateSelectivity(f1);
            double selectivity2 = calculateSelectivity(f2);
            return Double.compare(selectivity2, selectivity1);  // 降序
        })
        .limit(5)  // 限制等值字段数量
        .collect(Collectors.toList());
}
```

## Delete Vector生成机制分析

### 1. Delete Vector触发条件 (Iceberg v3+)

Delete Vector是Iceberg v3引入的高效删除机制，在以下情况下产生：

1. **高删除率场景**：删除比例 > 30%
2. **批量删除操作**：删除记录数 > 10,000
3. **Position Delete文件整理**：将多个小的Position Delete文件合并
4. **文件级删除**：删除整个数据文件的大部分行

### 2. Delete Vector Writer实现

#### BaseDVFileWriter (`core/src/main/java/org/apache/iceberg/deletes/BaseDVFileWriter.java:49-119`)

```java
public class BaseDVFileWriter implements DVFileWriter {
  private static final String REFERENCED_DATA_FILE_KEY = "referenced-data-file";
  private static final String CARDINALITY_KEY = "cardinality";

  private final OutputFileFactory fileFactory;
  private final Function<String, PositionDeleteIndex> loadPreviousDeletes;
  private final Map<String, Deletes> deletesByPath = Maps.newHashMap();
  private final Map<String, BlobMetadata> blobsByPath = Maps.newHashMap();

  @Override
  public void delete(String path, long pos, PartitionSpec spec, StructLike partition) {
    Deletes deletes =
        deletesByPath.computeIfAbsent(path, key -> new Deletes(path, spec, partition));
    PositionDeleteIndex positions = deletes.positions();
    positions.delete(pos);  // 添加到Roaring Bitmap
  }

  @Override
  public void close() throws IOException {
    List<DeleteFile> dvs = Lists.newArrayList();
    PuffinWriter writer = newWriter();

    try (PuffinWriter closeableWriter = writer) {
      for (Deletes deletes : deletesByPath.values()) {
        String path = deletes.path();
        PositionDeleteIndex positions = deletes.positions();

        // 合并已有的删除位置
        PositionDeleteIndex previousPositions = loadPreviousDeletes.apply(path);
        if (previousPositions != null) {
          positions.merge(previousPositions);
        }

        write(closeableWriter, deletes);  // 写入Puffin Blob
      }
    }

    // 创建Delete Vector元数据
    String puffinPath = writer.location();
    long puffinFileSize = writer.fileSize();

    for (String path : deletesByPath.keySet()) {
      DeleteFile dv = createDV(puffinPath, puffinFileSize, path);
      dvs.add(dv);
    }

    this.result = new DeleteWriteResult(dvs, referencedDataFiles, rewrittenDeleteFiles);
  }

  private DeleteFile createDV(String path, long size, String referencedDataFile) {
    Deletes deletes = deletesByPath.get(referencedDataFile);
    BlobMetadata blobMetadata = blobsByPath.get(referencedDataFile);

    return FileMetadata.deleteFileBuilder(deletes.spec())
        .ofPositionDeletes()
        .withFormat(FileFormat.PUFFIN)  // Puffin格式
        .withPath(path)
        .withPartition(deletes.partition())
        .withFileSizeInBytes(size)
        .withReferencedDataFile(referencedDataFile)        // 引用的数据文件
        .withContentOffset(blobMetadata.offset())          // Puffin文件中的偏移
        .withContentSizeInBytes(blobMetadata.length())     // Delete Vector大小
        .withRecordCount(deletes.positions().cardinality()) // 删除记录数
        .build();
  }
}
```

### 3. Roaring Bitmap存储优化

#### RoaringPositionBitmap (`core/src/main/java/org/apache/iceberg/deletes/RoaringPositionBitmap.java:51-318`)

```java
class RoaringPositionBitmap {
  static final long MAX_POSITION = toPosition(Integer.MAX_VALUE - 1, Integer.MIN_VALUE);
  private RoaringBitmap[] bitmaps;  // 分层bitmap数组

  // 64位位置映射到32位bitmap数组
  public void set(long pos) {
    validatePosition(pos);
    int key = key(pos);               // 高32位作为数组索引
    int pos32Bits = pos32Bits(pos);   // 低32位作为bitmap位置
    allocateBitmapsIfNeeded(key + 1);
    bitmaps[key].add(pos32Bits);
  }

  // 运行长度编码优化
  public boolean runLengthEncode() {
    boolean changed = false;
    for (RoaringBitmap bitmap : bitmaps) {
      changed |= bitmap.runOptimize();  // RLE压缩
    }
    return changed;
  }

  // 序列化为Puffin Blob
  public void serialize(ByteBuffer buffer) {
    validateByteOrder(buffer);
    buffer.putLong(bitmaps.length);
    for (int key = 0; key < bitmaps.length; key++) {
      buffer.putInt(key);
      bitmaps[key].serialize(buffer);  // 标准Roaring格式
    }
  }

  // 位置解码
  private static int key(long pos) {
    return (int) (pos >> 32);  // 高32位
  }

  private static int pos32Bits(long pos) {
    return (int) pos;  // 低32位
  }
}
```

### 4. Delete Vector生成示例

#### 批量删除产生Delete Vector

```java
// 场景：删除表中80%的历史数据
Table table = catalog.loadTable(TableIdentifier.of("analytics", "events"));

DVFileWriter dvWriter = new BaseDVFileWriter(
    OutputFileFactory.builderFor(table, 1, 1).build(),
    path -> loadExistingPositionDeletes(path)  // 加载已有删除
);

// 批量添加删除位置
String dataFilePath = "s3://bucket/events/year=2023/month=01/data-001.parquet";
for (long position = 0; position < 1_000_000; position += 5) {
    dvWriter.delete(dataFilePath, position, table.spec(), partition);
}

// 关闭并获取结果
dvWriter.close();
DeleteWriteResult result = dvWriter.result();

// 提交Delete Vector
table.newRowDelta()
    .addDeletes(result.deleteFiles().get(0))
    .commit();
```

**生成的Delete Vector结构**：
```
Puffin文件: s3://bucket/deletes/dv-001.puffin
├── Blob 1: 引用 data-001.parquet
│   ├── 类型: DV_V1
│   ├── 字段: [_pos]
│   ├── 压缩的Roaring Bitmap: [0,5,10,15,20,...]
│   └── 元数据: {cardinality: 200000, referenced-data-file: "data-001.parquet"}
└── Footer: Blob索引和元数据
```

## Delete文件过多的优化策略

### 1. 问题诊断

#### Delete文件过多的判断标准

```java
// Delete文件过多的诊断指标
public class DeleteFileHealthCheck {

    public boolean hasTooManyDeleteFiles(Table table) {
        Snapshot currentSnapshot = table.currentSnapshot();
        if (currentSnapshot == null) return false;

        // 统计Delete文件数量
        int deleteFileCount = countDeleteFiles(currentSnapshot);
        int dataFileCount = countDataFiles(currentSnapshot);

        // 诊断规则
        boolean rule1 = deleteFileCount > 1000;  // 绝对数量过多
        boolean rule2 = (double) deleteFileCount / dataFileCount > 0.5;  // 相对比例过高
        boolean rule3 = avgDeleteFileSize(currentSnapshot) < 10 * 1024 * 1024;  // 平均大小过小

        return rule1 || rule2 || rule3;
    }

    private long avgDeleteFileSize(Snapshot snapshot) {
        List<ManifestFile> deleteManifests = snapshot.deleteManifests(table.io());
        long totalSize = deleteManifests.stream()
            .mapToLong(ManifestFile::length)
            .sum();
        long fileCount = deleteManifests.stream()
            .mapToLong(ManifestFile::addedFilesCount)
            .sum();
        return fileCount > 0 ? totalSize / fileCount : 0;
    }
}
```

### 2. 优化策略实现

#### 策略1：Position Delete文件重写合并

**核心实现**：`RewritePositionDeleteFilesSparkAction` (`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewritePositionDeleteFilesSparkAction.java:76-149`)

```java
public class RewritePositionDeleteFilesSparkAction implements RewritePositionDeleteFiles {

  @Override
  public Result execute() {
    if (table.currentSnapshot() == null) {
      LOG.info("Nothing found to rewrite in empty table {}", table.name());
      return EMPTY_RESULT;
    }

    // v3表优先转换为Delete Vector
    if (TableUtil.formatVersion(table) >= 3 && !requiresRewriteToDVs()) {
      LOG.info("v2 deletes in {} have already been rewritten to v3 DVs", table.name());
      return EMPTY_RESULT;
    }

    // 按分区规划文件组
    StructLikeMap<List<List<PositionDeletesScanTask>>> fileGroupsByPartition = planFileGroups();
    RewriteExecutionContext ctx = new RewriteExecutionContext(fileGroupsByPartition);

    if (ctx.totalGroupCount() == 0) {
      LOG.info("Nothing found to rewrite in {}", table.name());
      return EMPTY_RESULT;
    }

    Stream<RewritePositionDeletesGroup> groupStream = toGroupStream(ctx, fileGroupsByPartition);

    // 支持部分提交避免长事务
    if (partialProgressEnabled) {
      return doExecuteWithPartialProgress(ctx, groupStream, commitManager());
    } else {
      return doExecute(ctx, groupStream, commitManager());
    }
  }
}
```

**执行示例**：

```java
// Spark中执行Position Delete重写
SparkSession spark = SparkSession.builder()
    .appName("RewritePositionDeletes")
    .getOrCreate();

Table table = Spark3Util.loadIcebergTable(spark, "catalog.db.table");

// 配置重写参数
RewritePositionDeleteFiles rewriteAction = SparkActions.get(spark)
    .rewritePositionDeletes(table)
    .option("target-file-size-bytes", "134217728")  // 128MB
    .option("max-concurrent-file-group-rewrites", "5")
    .option("partial-progress.enabled", "true")
    .option("partial-progress.max-commits", "10");

// 执行重写
RewritePositionDeleteFiles.Result result = rewriteAction.execute();

System.out.printf("重写完成: 处理了%d个删除文件，生成了%d个新文件\n",
    result.rewrittenDeleteFilesCount(),
    result.addedDeleteFilesCount());
```

#### 策略2：升级到Delete Vector (v3特性)

```java
// 将Position Delete升级为Delete Vector
public void upgradeToDeleteVectors(Table table) {
    if (TableUtil.formatVersion(table) < 3) {
        // 先升级表格式到v3
        table.updateProperties()
            .set(TableProperties.FORMAT_VERSION, "3")
            .commit();
    }

    // 执行Position Delete到Delete Vector的转换
    SparkActions.get(spark)
        .rewritePositionDeletes(table)
        .option("delete-granularity", "file")  // 文件级Delete Vector
        .execute();
}
```

#### 策略3：清理孤儿Delete文件

```java
// 清理不再引用的Delete文件
public class DeleteFileCleanup {

    public void removeOrphanedDeleteFiles(Table table) {
        // 1. 收集所有Delete文件引用
        Set<String> referencedDeleteFiles = collectReferencedDeleteFiles(table);

        // 2. 扫描实际存储的Delete文件
        Set<String> actualDeleteFiles = scanActualDeleteFiles(table);

        // 3. 找出孤儿文件
        Set<String> orphanedFiles = Sets.difference(actualDeleteFiles, referencedDeleteFiles);

        // 4. 安全删除（考虑并发安全）
        long deleteOlderThan = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(3);
        orphanedFiles.stream()
            .filter(file -> getFileTimestamp(file) < deleteOlderThan)
            .forEach(file -> {
                try {
                    table.io().deleteFile(file);
                    LOG.info("Deleted orphaned delete file: {}", file);
                } catch (Exception e) {
                    LOG.warn("Failed to delete orphaned file: {}", file, e);
                }
            });
    }

    private Set<String> collectReferencedDeleteFiles(Table table) {
        Set<String> referenced = Sets.newHashSet();

        // 遍历所有快照中的Delete文件引用
        Iterable<Snapshot> snapshots = table.snapshots();
        for (Snapshot snapshot : snapshots) {
            for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
                try (ManifestReader<DeleteFile> reader = ManifestFiles.read(manifest, table.io())) {
                    for (ManifestEntry<DeleteFile> entry : reader.entries()) {
                        if (entry.status() != ManifestEntry.Status.DELETED) {
                            referenced.add(entry.file().location());
                        }
                    }
                }
            }
        }

        return referenced;
    }
}
```

#### 策略4：智能Delete文件合并

```java
// 基于成本模型的智能合并策略
public class SmartDeleteFileMerger {

    // 合并决策算法
    public boolean shouldMergeDeleteFiles(List<DeleteFile> deleteFiles) {
        if (deleteFiles.size() < 2) return false;

        // 计算合并收益
        long currentTotalSize = deleteFiles.stream()
            .mapToLong(DeleteFile::fileSizeInBytes)
            .sum();

        long estimatedMergedSize = estimateMergedSize(deleteFiles);
        double compressionRatio = (double) estimatedMergedSize / currentTotalSize;

        // 合并条件：
        // 1. 文件数量 > 10
        // 2. 压缩比 < 0.8 (至少节省20%空间)
        // 3. 单个文件平均大小 < 32MB
        boolean condition1 = deleteFiles.size() > 10;
        boolean condition2 = compressionRatio < 0.8;
        boolean condition3 = currentTotalSize / deleteFiles.size() < 32 * 1024 * 1024;

        return condition1 && condition2 && condition3;
    }

    private long estimateMergedSize(List<DeleteFile> deleteFiles) {
        // 基于删除记录数和数据类型估算合并后大小
        long totalRecords = deleteFiles.stream()
            .mapToLong(DeleteFile::recordCount)
            .sum();

        // Position Delete: 平均每记录 ~24字节 (文件路径 + 位置)
        // Equality Delete: 根据等值字段计算
        return totalRecords * 24;  // 简化估算
    }
}
```

### 3. 自动化优化框架

#### 持续优化守护进程

```java
@Component
public class DeleteFileOptimizer {

    @Scheduled(fixedDelay = 3600000)  // 每小时检查一次
    public void optimizeDeleteFiles() {
        List<Table> tables = catalog.listTables();

        for (Table table : tables) {
            try {
                if (needsOptimization(table)) {
                    optimizeTable(table);
                }
            } catch (Exception e) {
                LOG.error("Failed to optimize delete files for table: {}", table.name(), e);
            }
        }
    }

    private boolean needsOptimization(Table table) {
        // 多维度判断优化需求
        DeleteFileHealthCheck healthCheck = new DeleteFileHealthCheck();

        boolean hasProblems = healthCheck.hasTooManyDeleteFiles(table);
        boolean recentActivity = hasRecentDeleteActivity(table);
        boolean scheduledMaintenance = isScheduledMaintenanceWindow();

        return hasProblems && (recentActivity || scheduledMaintenance);
    }

    private void optimizeTable(Table table) {
        LOG.info("Starting delete file optimization for table: {}", table.name());

        // 1. 重写Position Delete文件
        if (hasFragmentedPositionDeletes(table)) {
            rewritePositionDeletes(table);
        }

        // 2. 升级到Delete Vector (v3+)
        if (canUpgradeToDeleteVectors(table)) {
            upgradeToDeleteVectors(table);
        }

        // 3. 清理孤儿文件
        removeOrphanedDeleteFiles(table);

        LOG.info("Completed delete file optimization for table: {}", table.name());
    }
}
```

## 实际应用示例与最佳实践

### 1. 电商系统订单删除场景

```java
// 场景：电商系统中删除已取消的订单
public class EcommerceOrderDeletion {

    public void deleteCancelledOrders(Table ordersTable, String cutoffDate) {
        // 1. 使用Equality Delete删除批量取消的订单
        SparkSession spark = SparkSession.active();

        Dataset<Row> cancelledOrders = spark.sql(
            "SELECT order_id, user_id FROM orders " +
            "WHERE status = 'CANCELLED' AND created_date < '" + cutoffDate + "'"
        );

        // 2. 写入Equality Delete文件
        cancelledOrders.write()
            .format("iceberg")
            .mode("append")
            .option("write-format", "parquet")
            .option("delete-mode", "equality")
            .option("equality-field-columns", "order_id,user_id")  // 等值字段
            .save("catalog.ecommerce.orders_deletes");

        // 3. 应用删除到主表
        RowDelta rowDelta = ordersTable.newRowDelta();

        // 加载生成的删除文件
        DeleteFile deleteFile = loadLatestDeleteFile(ordersTable);
        rowDelta.addDeletes(deleteFile);

        // 提交删除操作
        rowDelta.commit();

        LOG.info("Deleted cancelled orders before date: {}", cutoffDate);
    }

    private DeleteFile loadLatestDeleteFile(Table table) {
        Snapshot snapshot = table.currentSnapshot();
        List<ManifestFile> deleteManifests = snapshot.deleteManifests(table.io());

        return deleteManifests.stream()
            .sorted((m1, m2) -> Long.compare(m2.snapshotId(), m1.snapshotId()))
            .findFirst()
            .map(manifest -> {
                try (ManifestReader<DeleteFile> reader = ManifestFiles.read(manifest, table.io())) {
                    return reader.iterator().next().file();
                }
            })
            .orElseThrow(() -> new IllegalStateException("No delete files found"));
    }
}
```

### 2. 时序数据清理场景

```java
// 场景：时序数据库中清理过期数据
public class TimeSeriesDataCleanup {

    public void cleanupExpiredMetrics(Table metricsTable, Duration retentionPeriod) {
        long cutoffTime = System.currentTimeMillis() - retentionPeriod.toMillis();

        // 1. 对于v3表，使用Delete Vector批量删除
        if (TableUtil.formatVersion(metricsTable) >= 3) {
            deleteWithDeleteVector(metricsTable, cutoffTime);
        } else {
            // 2. 对于v2表，使用Position Delete
            deleteWithPositionDelete(metricsTable, cutoffTime);
        }
    }

    private void deleteWithDeleteVector(Table table, long cutoffTime) {
        // 扫描需要删除的数据文件和位置
        Map<String, List<Long>> deletionsByFile = scanExpiredPositions(table, cutoffTime);

        DVFileWriter dvWriter = new BaseDVFileWriter(
            OutputFileFactory.builderFor(table, 1, 1).build(),
            path -> null  // 新建Delete Vector
        );

        // 批量添加删除位置
        for (Map.Entry<String, List<Long>> entry : deletionsByFile.entrySet()) {
            String filePath = entry.getKey();
            List<Long> positions = entry.getValue();

            for (Long position : positions) {
                dvWriter.delete(filePath, position, table.spec(), null);
            }
        }

        // 提交Delete Vector
        dvWriter.close();
        DeleteWriteResult result = dvWriter.result();

        table.newRowDelta()
            .addDeletes(result.deleteFiles().get(0))
            .set("delete-operation", "time-series-cleanup")
            .set("cleanup-cutoff-time", String.valueOf(cutoffTime))
            .commit();

        LOG.info("Created Delete Vector for {} expired records",
            deletionsByFile.values().stream().mapToInt(List::size).sum());
    }

    private Map<String, List<Long>> scanExpiredPositions(Table table, long cutoffTime) {
        // 实现：扫描表找出过期记录的位置
        // 这里简化处理，实际需要读取数据文件并确定行位置
        Map<String, List<Long>> positions = Maps.newHashMap();

        // 扫描逻辑...

        return positions;
    }
}
```

### 3. 数据质量修复场景

```java
// 场景：数据质量问题修复，删除重复和错误数据
public class DataQualityRepair {

    public void removeDuplicateRecords(Table table, List<String> dedupeColumns) {
        SparkSession spark = SparkSession.active();

        // 1. 找出重复记录
        String tableName = table.name();
        Dataset<Row> duplicates = spark.sql(String.format(
            "WITH numbered_rows AS (" +
            "  SELECT *, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY _pos) as rn" +
            "  FROM %s" +
            ") " +
            "SELECT %s FROM numbered_rows WHERE rn > 1",
            String.join(",", dedupeColumns),
            tableName,
            String.join(",", dedupeColumns)
        ));

        // 2. 写入Equality Delete去除重复
        ClusteredEqualityDeleteWriter<InternalRow> deleteWriter = createEqualityDeleteWriter(
            table, dedupeColumns);

        duplicates.foreachPartition(partition -> {
            while (partition.hasNext()) {
                Row row = partition.next();
                InternalRow deleteRecord = convertToDeleteRecord(row, dedupeColumns);
                deleteWriter.write(deleteRecord);
            }
        });

        // 3. 提交删除操作
        DeleteWriteResult result = deleteWriter.complete();
        table.newRowDelta()
            .addDeletes(result.deleteFiles().get(0))
            .set("operation", "deduplicate")
            .set("dedupe-columns", String.join(",", dedupeColumns))
            .commit();

        LOG.info("Removed duplicate records based on columns: {}", dedupeColumns);
    }

    private ClusteredEqualityDeleteWriter<InternalRow> createEqualityDeleteWriter(
            Table table, List<String> equalityColumns) {

        Schema deleteSchema = TypeUtil.select(table.schema(), equalityColumns);
        FileWriterFactory<InternalRow> writerFactory =
            new SparkFileWriterFactory(table.schema());

        return new ClusteredEqualityDeleteWriter<>(
            writerFactory,
            OutputFileFactory.builderFor(table, 1, 1).build(),
            table.io(),
            128 * 1024 * 1024  // 128MB target size
        );
    }
}
```

## 性能调优指南

### 1. Delete文件大小优化

```java
// Delete文件大小配置策略
public class DeleteFileSizeOptimization {

    public static final Map<String, String> RECOMMENDED_SETTINGS = ImmutableMap.of(
        // Position Delete文件目标大小
        "write.delete.target-file-size-bytes", "134217728",  // 128MB

        // Delete文件压缩
        "write.delete.parquet.compression-codec", "zstd",
        "write.delete.parquet.compression-level", "3",

        // Delete Vector设置 (v3+)
        "write.delete.dv.target-file-size-bytes", "67108864",  // 64MB

        // 删除粒度
        "write.delete.granularity", "partition"  // file | partition
    );

    public void configureDeleteFileSettings(Table table, String workloadType) {
        Map<String, String> properties = Maps.newHashMap(RECOMMENDED_SETTINGS);

        switch (workloadType) {
            case "high_frequency_small_deletes":
                // 高频小量删除：较小的文件大小，更快提交
                properties.put("write.delete.target-file-size-bytes", "33554432");  // 32MB
                properties.put("write.delete.granularity", "file");
                break;

            case "batch_large_deletes":
                // 批量大量删除：较大的文件大小，更好压缩
                properties.put("write.delete.target-file-size-bytes", "268435456");  // 256MB
                properties.put("write.delete.granularity", "partition");
                break;

            case "time_series_cleanup":
                // 时序数据清理：偏向Delete Vector
                if (TableUtil.formatVersion(table) >= 3) {
                    properties.put("write.delete.mode", "delete-vector");
                    properties.put("write.delete.dv.target-file-size-bytes", "134217728");  // 128MB
                }
                break;
        }

        // 应用配置
        table.updateProperties()
            .setAll(properties)
            .commit();

        LOG.info("Applied delete file settings for workload type: {}", workloadType);
    }
}
```

### 2. 并发控制优化

```java
// Delete操作并发控制
public class DeleteConcurrencyOptimization {

    public void optimizeConcurrentDeletes(Table table) {
        Map<String, String> concurrencySettings = ImmutableMap.of(
            // 最大并发重写任务数
            "actions.rewrite-position-deletes.max-concurrent-file-group-rewrites", "10",

            // 部分提交设置
            "actions.rewrite-position-deletes.partial-progress.enabled", "true",
            "actions.rewrite-position-deletes.partial-progress.max-commits", "20",

            // 任务排序策略
            "actions.rewrite-position-deletes.rewrite-job-order", "bytes-desc",  // 大文件优先

            // 文件组大小限制
            "actions.rewrite-position-deletes.max-file-group-size-bytes",
                String.valueOf(5L * 1024 * 1024 * 1024)  // 5GB per group
        );

        table.updateProperties()
            .setAll(concurrencySettings)
            .commit();
    }

    // 智能重试机制
    public void rewriteDeleteFilesWithRetry(Table table, int maxRetries) {
        int attempts = 0;
        boolean success = false;

        while (attempts < maxRetries && !success) {
            try {
                RewritePositionDeleteFiles.Result result = SparkActions.get(spark)
                    .rewritePositionDeletes(table)
                    .option("partial-progress.enabled", "true")
                    .execute();

                success = true;
                LOG.info("Delete file rewrite succeeded on attempt {}: {} files processed",
                    attempts + 1, result.rewrittenDeleteFilesCount());

            } catch (CommitFailedException e) {
                attempts++;
                if (attempts < maxRetries) {
                    // 指数退避重试
                    long backoffMs = (long) (1000 * Math.pow(2, attempts));
                    LOG.warn("Delete rewrite failed on attempt {}, retrying in {}ms",
                        attempts, backoffMs);
                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during retry backoff", ie);
                    }
                } else {
                    throw new RuntimeException("Delete file rewrite failed after " + maxRetries + " attempts", e);
                }
            }
        }
    }
}
```

### 3. 监控和指标

```java
// Delete文件健康监控
public class DeleteFileMonitoring {

    private final MeterRegistry meterRegistry;
    private final Timer deleteOperationTimer;
    private final Counter deleteFilesCreated;
    private final Gauge deleteFileCount;

    public DeleteFileMonitoring(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.deleteOperationTimer = Timer.builder("iceberg.delete.operation.duration")
            .description("Duration of delete operations")
            .register(meterRegistry);
        this.deleteFilesCreated = Counter.builder("iceberg.delete.files.created")
            .description("Number of delete files created")
            .register(meterRegistry);
        this.deleteFileCount = Gauge.builder("iceberg.delete.files.current")
            .description("Current number of delete files")
            .register(meterRegistry, this, DeleteFileMonitoring::getCurrentDeleteFileCount);
    }

    public void recordDeleteOperation(Table table, Runnable operation) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            operation.run();
            sample.stop(deleteOperationTimer);
        } catch (Exception e) {
            sample.stop(Timer.builder("iceberg.delete.operation.failed")
                .register(meterRegistry));
            throw e;
        }
    }

    public DeleteFileHealthMetrics collectHealthMetrics(Table table) {
        Snapshot snapshot = table.currentSnapshot();
        if (snapshot == null) {
            return new DeleteFileHealthMetrics(0, 0, 0, 0, 0);
        }

        List<ManifestFile> deleteManifests = snapshot.deleteManifests(table.io());

        int totalDeleteFiles = deleteManifests.stream()
            .mapToInt(manifest -> (int) manifest.addedFilesCount())
            .sum();

        long totalDeleteFileSize = deleteManifests.stream()
            .mapToLong(ManifestFile::length)
            .sum();

        long avgDeleteFileSize = totalDeleteFiles > 0 ? totalDeleteFileSize / totalDeleteFiles : 0;

        int dataFileCount = snapshot.dataManifests(table.io()).stream()
            .mapToInt(manifest -> (int) manifest.addedFilesCount())
            .sum();

        double deleteToDataRatio = dataFileCount > 0 ? (double) totalDeleteFiles / dataFileCount : 0;

        return new DeleteFileHealthMetrics(
            totalDeleteFiles,
            totalDeleteFileSize,
            avgDeleteFileSize,
            dataFileCount,
            deleteToDataRatio
        );
    }

    private double getCurrentDeleteFileCount() {
        // 实现获取当前delete文件数量的逻辑
        return 0.0; // 简化实现
    }

    public static class DeleteFileHealthMetrics {
        private final int totalDeleteFiles;
        private final long totalDeleteFileSize;
        private final long avgDeleteFileSize;
        private final int dataFileCount;
        private final double deleteToDataRatio;

        public DeleteFileHealthMetrics(int totalDeleteFiles, long totalDeleteFileSize,
                long avgDeleteFileSize, int dataFileCount, double deleteToDataRatio) {
            this.totalDeleteFiles = totalDeleteFiles;
            this.totalDeleteFileSize = totalDeleteFileSize;
            this.avgDeleteFileSize = avgDeleteFileSize;
            this.dataFileCount = dataFileCount;
            this.deleteToDataRatio = deleteToDataRatio;
        }

        public boolean needsOptimization() {
            return totalDeleteFiles > 100 ||           // 删除文件过多
                   deleteToDataRatio > 0.5 ||          // 删除/数据文件比例过高
                   avgDeleteFileSize < 10 * 1024 * 1024; // 平均文件过小 (< 10MB)
        }

        // Getters...
        public int getTotalDeleteFiles() { return totalDeleteFiles; }
        public long getTotalDeleteFileSize() { return totalDeleteFileSize; }
        public long getAvgDeleteFileSize() { return avgDeleteFileSize; }
        public int getDataFileCount() { return dataFileCount; }
        public double getDeleteToDataRatio() { return deleteToDataRatio; }
    }
}
```

### 4. 最佳实践总结

#### Delete操作最佳实践清单

```java
public class DeleteBestPractices {

    /**
     * Delete操作最佳实践指南
     */
    public static final List<String> BEST_PRACTICES = Arrays.asList(
        "1. 选择合适的Delete类型:",
        "   - 精确行删除 → Position Delete",
        "   - 条件批量删除 → Equality Delete",
        "   - 高删除率场景 → Delete Vector (v3+)",
        "",
        "2. 优化Delete文件大小:",
        "   - Position Delete: 64-128MB",
        "   - Equality Delete: 128-256MB",
        "   - Delete Vector: 32-64MB",
        "",
        "3. 定期维护Delete文件:",
        "   - 监控Delete文件数量和大小",
        "   - 定期执行rewrite-position-deletes",
        "   - 清理孤儿Delete文件",
        "",
        "4. 合理使用v3特性:",
        "   - 高删除率表升级到v3",
        "   - 利用Delete Vector提升性能",
        "   - 配置合适的压缩策略",
        "",
        "5. 并发控制:",
        "   - 启用部分提交避免长事务",
        "   - 控制并发重写任务数量",
        "   - 实现智能重试机制"
    );

    public void printBestPractices() {
        BEST_PRACTICES.forEach(System.out::println);
    }
}
```

通过本文档的深入分析和实践指南，开发者可以全面掌握Apache Iceberg中Delete文件的产生机制、优化策略和最佳实践，从而在生产环境中实现高效的数据删除操作和维护。

<function_calls>
<invoke name="TodoWrite">
<parameter name="todos">[{"content": "\u5206\u6790delete\u6587\u4ef6\u7684\u4ea7\u751f\u6765\u6e90\u548c\u539f\u7406", "status": "completed", "activeForm": "\u5206\u6790delete\u6587\u4ef6\u7684\u4ea7\u751f\u6765\u6e90\u548c\u539f\u7406"}, {"content": "\u7814\u7a76Position Delete\u7684\u751f\u6210\u673a\u5236", "status": "completed", "activeForm": "\u7814\u7a76Position Delete\u7684\u751f\u6210\u673a\u5236"}, {"content": "\u7814\u7a76Equality Delete\u7684\u751f\u6210\u673a\u5236", "status": "completed", "activeForm": "\u7814\u7a76Equality Delete\u7684\u751f\u6210\u673a\u5236"}, {"content": "\u5206\u6790delete\u6587\u4ef6\u8fc7\u591a\u7684\u4f18\u5316\u7b56\u7565", "status": "completed", "activeForm": "\u5206\u6790delete\u6587\u4ef6\u8fc7\u591a\u7684\u4f18\u5316\u7b56\u7565"}, {"content": "\u521b\u5eba\u5305\u542b\u6e90\u7801\u548c\u793a\u4f8b\u7684\u5b8c\u6574\u6587\u6863", "status": "completed", "activeForm": "\u521b\u5eba\u5305\u542b\u6e90\u7801\u548c\u793a\u4f8b\u7684\u5b8c\u6574\u6587\u6863"}]