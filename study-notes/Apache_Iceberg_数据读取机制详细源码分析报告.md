# Apache Iceberg 数据读取机制详细源码分析报告

## 概述

本报告深入分析Apache Iceberg从metadata到读取数据的完整过程，基于对核心源码的详细研究，展示了从TableMetadata开始，通过Snapshot找到ManifestList，再通过ManifestList找到具体的Manifest文件，最后通过Manifest找到DataFile并读取数据的完整流程。

## 核心组件结构图

```
┌─────────────────┐      ┌──────────────────┐      ┌─────────────────┐
│   TableMetadata │─────▶│     Snapshot     │─────▶│  ManifestList   │
│                 │      │                  │      │     (.avro)     │
│ - currentSnapId │      │ - snapshotId     │      │                 │
│ - snapshots     │      │ - manifestList   │      │  ┌─────────────┐│
│ - refs          │      │   Location       │      │  │ManifestFile ││
│ - schemas       │      │ - timestampMs    │      │  │  Entry1     ││
│ - specs         │      │ - operation      │      │  │ManifestFile ││
└─────────────────┘      │ - summary        │      │  │  Entry2     ││
                         └──────────────────┘      │  │   ...       ││
                                 │                 └─────────────────┘
                                 │
                         ┌──────────────────┐
                         │  BaseSnapshot    │
                         │ .cacheManifests()│
                         │ .allManifests()  │
                         └──────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        ManifestLists.read()                        │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  InternalData.read(FileFormat.AVRO, manifestList)           │   │
│  │    .setRootType(GenericManifestFile.class)                  │   │
│  │    .project(ManifestFile.schema())                          │   │
│  │    .build()                                                 │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────┐      ┌──────────────────┐      ┌─────────────────┐
│  ManifestFile   │─────▶│   ManifestReader │─────▶│  ManifestEntry  │
│                 │      │                  │      │                 │
│ - path          │      │ .entries()       │      │ - status        │
│ - length        │      │ .iterator()      │      │ - dataSequence  │
│ - partitionSpec │      │ .select()        │      │ - file          │
│ - content       │      │ .filterRows()    │      │                 │
│ - sequenceNum   │      │ .filterPartition │      │ ┌─────────────┐ │
│ - snapshotId    │      │ .caseSensitive() │      │ │  DataFile   │ │
│ - addedFiles    │      │                  │      │ │             │ │
│ - deletedFiles  │      └──────────────────┘      │ │- file_path  │ │
│ - addedRows     │                                │ │- file_format│ │
│ - deletedRows   │                                │ │- record_cnt │ │
│ - partitions    │                                │ │- file_size  │ │
└─────────────────┘                                │ │- partition  │ │
                                                   │ │- column_sz  │ │
                                                   │ │- value_cnts │ │
                                                   │ │- null_cnts  │ │
                                                   │ │- lower_bnds │ │
                                                   │ │- upper_bnds │ │
                                                   │ └─────────────┘ │
                                                   └─────────────────┘
                                                           │
                                                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        ManifestGroup                               │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  .filterData(expression)                                    │   │
│  │  .filterFiles(expression)                                   │   │
│  │  .filterPartitions(expression)                              │   │
│  │  .ignoreDeleted()                                           │   │
│  │  .ignoreExisting()                                          │   │
│  │  .select(columns)                                           │   │
│  │  .planFiles()                                               │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────┐      ┌──────────────────┐      ┌─────────────────┐
│  FileScanTask   │◀─────│   TableScan      │◀─────│  Table.newScan()│
│                 │      │                  │      │                 │
│ - file          │      │ .select()        │      │ .option()       │
│ - deletes       │      │ .filter()        │      │ .filter()       │
│ - start         │      │ .planFiles()     │      │ .select()       │
│ - length        │      │ .planTasks()     │      │                 │
│ - residual      │      │                  │      └─────────────────┘
│ - schema        │      └──────────────────┘
└─────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     数据读取执行                                      │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  1. 打开DataFile (Parquet/ORC/Avro)                         │   │
│  │  2. 应用predicate pushdown                                  │   │
│  │  3. 读取指定列数据                                            │   │
│  │  4. 应用Delete Files (if any)                              │   │
│  │  5. 返回Record/Row迭代器                                     │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

## 详细过程分析

### 1. TableMetadata 结构

**核心类位置**: `/core/src/main/java/org/apache/iceberg/TableMetadata.java`

```java
public class TableMetadata implements Serializable {
    // 当前快照ID
    private final long currentSnapshotId;

    // 所有快照的供应商（延迟加载）
    private SerializableSupplier<List<Snapshot>> snapshotsSupplier;

    // 快照引用（分支和标签）
    private volatile Map<String, SnapshotRef> refs;

    // 快照索引
    private volatile Map<Long, Snapshot> snapshotsById;

    // 获取当前快照
    public Snapshot currentSnapshot() {
        return currentSnapshotId != null ? snapshot(currentSnapshotId) : null;
    }
}
```

### 2. Snapshot 接口与 BaseSnapshot 实现

**API接口位置**: `/api/src/main/java/org/apache/iceberg/Snapshot.java`
**实现类位置**: `/core/src/main/java/org/apache/iceberg/BaseSnapshot.java`

```java
public interface Snapshot {
    // 快照基本属性
    long snapshotId();
    Long parentId();
    long timestampMillis();
    String operation();
    Map<String, String> summary();

    // 核心方法：获取manifest list位置
    String manifestListLocation();

    // 获取所有manifest文件
    List<ManifestFile> allManifests(FileIO io);
    List<ManifestFile> dataManifests(FileIO io);
    List<ManifestFile> deleteManifests(FileIO io);
}
```

**BaseSnapshot实现的关键方法**:

```java
class BaseSnapshot implements Snapshot {
    private final String manifestListLocation;
    private transient List<ManifestFile> allManifests = null;

    private void cacheManifests(FileIO fileIO) {
        if (allManifests == null) {
            // 读取manifest list文件获取所有manifest
            this.allManifests = ManifestLists.read(fileIO.newInputFile(manifestListLocation));
        }

        if (dataManifests == null || deleteManifests == null) {
            // 按内容类型过滤manifest
            this.dataManifests = ImmutableList.copyOf(
                Iterables.filter(allManifests,
                    manifest -> manifest.content() == ManifestContent.DATA));
            this.deleteManifests = ImmutableList.copyOf(
                Iterables.filter(allManifests,
                    manifest -> manifest.content() == ManifestContent.DELETES));
        }
    }
}
```

### 3. ManifestList 读取机制

**核心类位置**: `/core/src/main/java/org/apache/iceberg/ManifestLists.java`

```java
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

**ManifestList文件结构**:
- 格式：Avro
- 内容：ManifestFile记录的列表
- 每个记录包含：manifest路径、长度、分区规格ID、内容类型、序列号、快照ID、文件统计信息等

### 4. ManifestFile 接口设计

**接口位置**: `/api/src/main/java/org/apache/iceberg/ManifestFile.java`

```java
public interface ManifestFile {
    // 基本属性
    String path();                    // manifest文件路径
    long length();                    // 文件长度
    int partitionSpecId();           // 分区规格ID
    ManifestContent content();       // 内容类型(DATA/DELETES)
    long sequenceNumber();           // 序列号
    Long snapshotId();               // 关联的快照ID

    // 统计信息
    Integer addedFilesCount();       // 新增文件数
    Integer existingFilesCount();    // 现有文件数
    Integer deletedFilesCount();     // 删除文件数
    Long addedRowsCount();           // 新增行数
    Long existingRowsCount();        // 现有行数
    Long deletedRowsCount();         // 删除行数

    // 分区摘要
    List<PartitionFieldSummary> partitions();

    // 加密元数据
    ByteBuffer keyMetadata();
}
```

### 5. ManifestReader 读取机制

**核心类位置**: `/core/src/main/java/org/apache/iceberg/ManifestReader.java`

```java
public class ManifestReader<F extends ContentFile<F>>
    extends CloseableGroup implements CloseableIterable<F> {

    private final InputFile file;
    private final FileType content;
    private final PartitionSpec spec;
    private final Schema fileSchema;

    // 过滤器配置
    private PartitionSet partitionSet = null;
    private Expression partFilter = alwaysTrue();
    private Expression rowFilter = alwaysTrue();
    private Schema fileProjection = null;
    private Collection<String> columns = null;

    // 读取manifest entries
    public CloseableIterable<ManifestEntry<F>> entries() {
        // 使用Avro读取器读取manifest文件
        // 应用分区过滤器和行过滤器
        // 返回ManifestEntry迭代器
    }
}
```

### 6. DataFile 接口设计

**接口位置**: `/api/src/main/java/org/apache/iceberg/DataFile.java`

```java
public interface DataFile extends ContentFile<DataFile> {
    // 基本文件信息
    // FILE_PATH = required(100, "file_path", StringType.get())
    // FILE_FORMAT = required(101, "file_format", StringType.get())
    // RECORD_COUNT = required(103, "record_count", LongType.get())
    // FILE_SIZE = required(104, "file_size_in_bytes", LongType.get())

    // 列级统计信息
    // COLUMN_SIZES = optional(108, "column_sizes", MapType.of(...))
    // VALUE_COUNTS = optional(109, "value_counts", MapType.of(...))
    // NULL_VALUE_COUNTS = optional(110, "null_value_counts", MapType.of(...))
    // LOWER_BOUNDS = optional(125, "lower_bounds", MapType.of(...))
    // UPPER_BOUNDS = optional(128, "upper_bounds", MapType.of(...))

    // 分区信息
    // SPEC_ID = optional(141, "spec_id", IntegerType.get())
    // int PARTITION_ID = 102; // 分区数据

    // 高级特性
    // SPLIT_OFFSETS = optional(132, "split_offsets", ListType.of(...))
    // SORT_ORDER_ID = optional(140, "sort_order_id", IntegerType.get())
    // FIRST_ROW_ID = optional(142, "first_row_id", LongType.get())
}
```

### 7. ManifestGroup 扫描协调

**核心类位置**: `/core/src/main/java/org/apache/iceberg/ManifestGroup.java`

```java
class ManifestGroup {
    private final FileIO io;
    private final Set<ManifestFile> dataManifests;
    private final DeleteFileIndex.Builder deleteIndexBuilder;

    // 过滤器链
    private Expression dataFilter;
    private Expression fileFilter;
    private Expression partitionFilter;
    private boolean ignoreDeleted;
    private boolean ignoreExisting;

    // 核心方法：计划文件扫描
    public CloseableIterable<FileScanTask> planFiles() {
        // 1. 过滤manifest文件
        // 2. 并行读取通过过滤的manifest
        // 3. 应用文件级过滤器
        // 4. 构建FileScanTask
        // 5. 关联DeleteFile索引
    }
}
```

### 8. FileScanTask 执行单元

**接口位置**: `/api/src/main/java/org/apache/iceberg/FileScanTask.java`

```java
public interface FileScanTask extends ContentScanTask<DataFile>, SplittableScanTask<FileScanTask> {
    // 关联的delete文件列表
    List<DeleteFile> deletes();

    // 读取schema
    Schema schema();

    // 计算总大小（数据文件 + delete文件）
    default long sizeBytes() {
        return length() + ScanTaskUtil.contentSizeInBytes(deletes());
    }

    // 文件数量（1个数据文件 + N个delete文件）
    default int filesCount() {
        return 1 + deletes().size();
    }
}
```

## 完整的数据读取流程

### 步骤1: 从TableMetadata获取Snapshot

```java
// 1. 获取表的metadata
TableMetadata metadata = table.operations().current();

// 2. 获取当前快照
Snapshot currentSnapshot = metadata.currentSnapshot();
if (currentSnapshot == null) {
    return Collections.emptyList(); // 空表
}
```

### 步骤2: 从Snapshot获取ManifestList

```java
// 3. 获取manifest list位置
String manifestListLocation = currentSnapshot.manifestListLocation();

// 4. 读取manifest list获取所有manifest文件
List<ManifestFile> allManifests = currentSnapshot.allManifests(fileIO);

// 5. 过滤出数据manifest（排除delete manifest）
List<ManifestFile> dataManifests = currentSnapshot.dataManifests(fileIO);
```

**BaseSnapshot.cacheManifests()关键代码**:
```java
private void cacheManifests(FileIO fileIO) {
    if (allManifests == null) {
        // 核心：通过ManifestLists.read()读取manifest list文件
        this.allManifests = ManifestLists.read(fileIO.newInputFile(manifestListLocation));
    }
}
```

### 步骤3: 从Manifest获取DataFile列表

```java
// 6. 创建ManifestGroup来协调读取
ManifestGroup manifestGroup = new ManifestGroup(fileIO, dataManifests, deleteManifests)
    .filterData(dataFilter)
    .filterFiles(fileFilter)
    .filterPartitions(partitionFilter)
    .specsById(table.specs())
    .caseSensitive(caseSensitive)
    .select(selectedColumns)
    .scanMetrics(scanMetrics);

// 7. 读取每个manifest文件获取DataFile
for (ManifestFile manifest : dataManifests) {
    try (ManifestReader<DataFile> reader = ManifestFiles.readDataManifest(manifest, fileIO, null)) {
        for (ManifestEntry<DataFile> entry : reader.entries()) {
            if (entry.status() != ManifestEntry.Status.DELETED) {
                DataFile dataFile = entry.file();
                // 处理dataFile
            }
        }
    }
}
```

### 步骤4: 构建FileScanTask

```java
// 8. 将DataFile转换为FileScanTask
public CloseableIterable<FileScanTask> planFiles() {
    DeleteFileIndex deleteFileIndex = deleteIndexBuilder.build();

    return CloseableIterable.transform(
        manifestGroup.entries(),
        entry -> {
            DataFile dataFile = entry.file();
            List<DeleteFile> deletes = deleteFileIndex.forDataFile(dataFile);

            return new BaseFileScanTask(
                dataFile,
                deletes,
                schemaString,
                specString,
                residualFilter
            );
        }
    );
}
```

### 步骤5: 执行数据读取

```java
// 9. 遍历FileScanTask并读取数据
try (CloseableIterable<FileScanTask> fileTasks = tableScan.planFiles()) {
    for (FileScanTask task : fileTasks) {
        DataFile dataFile = task.file();
        List<DeleteFile> deletes = task.deletes();

        // 10. 打开数据文件（Parquet/ORC/Avro）
        InputFile inputFile = fileIO.newInputFile(dataFile.path().toString());

        // 11. 根据文件格式创建相应的Reader
        CloseableIterable<InternalRow> records;
        switch (dataFile.format()) {
            case PARQUET:
                records = Parquet.read(inputFile)
                    .project(schema)
                    .filter(residualFilter)
                    .build();
                break;
            case ORC:
                records = ORC.read(inputFile)
                    .project(schema)
                    .filter(residualFilter)
                    .build();
                break;
            case AVRO:
                records = Avro.read(inputFile)
                    .project(schema)
                    .build();
                break;
        }

        // 12. 应用delete文件（如果有）
        if (!deletes.isEmpty()) {
            records = Deletes.filter(records, deletes, schema);
        }

        // 13. 处理读取的记录
        for (InternalRow record : records) {
            // 业务逻辑处理
            processRecord(record);
        }
    }
}
```

## 关键技术细节

### 1. 延迟加载机制

Iceberg使用延迟加载来优化性能：

- **TableMetadata**: 快照列表通过`SerializableSupplier`延迟加载
- **BaseSnapshot**: manifest列表在首次访问时才读取
- **ManifestReader**: 文件内容按需读取，支持流式处理

### 2. 过滤器下推优化

多层次的过滤器下推：

```java
// 分区级过滤
Expression partitionFilter = Expressions.and(
    Expressions.greaterThan("date", "2023-01-01"),
    Expressions.lessThan("date", "2023-12-31")
);

// 文件级过滤
Expression fileFilter = Expressions.and(
    Expressions.greaterThan("record_count", 1000),
    Expressions.notNull("file_path")
);

// 行级过滤（下推到文件格式层）
Expression rowFilter = Expressions.and(
    Expressions.equal("status", "active"),
    Expressions.greaterThan("amount", 100)
);
```

### 3. 并行处理支持

```java
// ManifestGroup支持并行读取manifest文件
ManifestGroup manifestGroup = new ManifestGroup(fileIO, manifests)
    .planWith(executorService); // 并行执行

// 支持按CPU核数自动配置并行度
int parallelism = Runtime.getRuntime().availableProcessors();
```

### 4. 列裁剪支持

```java
// 只读取需要的列
List<String> selectedColumns = Arrays.asList("id", "name", "amount");
ManifestReader<DataFile> reader = ManifestFiles.readDataManifest(manifest, fileIO, null)
    .select(selectedColumns);
```

### 5. 统计信息利用

DataFile包含丰富的统计信息用于查询优化：

- **列大小**: 每列的存储大小
- **值计数**: 每列的值数量（包括null和NaN）
- **空值计数**: 每列的null值数量
- **边界值**: 每列的最小值和最大值
- **NaN计数**: 浮点列的NaN值数量

### 6. 文件格式支持

Iceberg支持多种文件格式：

- **Parquet**: 列式存储，支持复杂嵌套类型，压缩效率高
- **ORC**: 列式存储，主要用于Hive生态
- **Avro**: 行式存储，schema演化友好

### 7. Delete文件处理

```java
// Position Delete: 按行号删除
// Equality Delete: 按键值删除
List<DeleteFile> deletes = deleteFileIndex.forDataFile(dataFile);

// 在读取时应用删除逻辑
CloseableIterable<InternalRow> filteredRecords =
    Deletes.filter(records, deletes, schema);
```

## 性能优化要点

### 1. Manifest文件缓存
- BaseSnapshot缓存manifest列表避免重复读取
- ManifestFile对象可复用
- 支持manifest文件的本地缓存

### 2. 分区裁剪
- 利用ManifestFile的分区摘要信息
- 在manifest级别过滤不相关分区
- 减少需要读取的manifest文件数量

### 3. 文件裁剪
- 利用DataFile的统计信息
- 文件级别的min/max值过滤
- 空值统计辅助过滤

### 4. 并行扫描
- 多个manifest文件并行读取
- FileScanTask支持拆分以提高并行度
- 利用executor pool进行任务调度

## 总结

Apache Iceberg的数据读取机制通过精心设计的多层架构实现了高效的数据访问：

1. **TableMetadata**: 提供表级元数据和快照管理
2. **Snapshot**: 提供时间点一致性视图和manifest list引用
3. **ManifestList**: 提供manifest文件的索引和元数据
4. **ManifestFile**: 提供数据文件的索引和统计信息
5. **DataFile**: 提供实际数据文件的详细元数据
6. **FileScanTask**: 提供可执行的扫描任务单元

这种设计使得Iceberg能够：
- 支持大规模数据集的高效扫描
- 实现细粒度的过滤器下推优化
- 提供灵活的并行处理能力
- 支持多种文件格式和存储系统
- 实现ACID事务和时间旅行查询

整个架构体现了现代数据湖系统的设计理念：元数据驱动、延迟加载、过滤器下推、并行处理和格式无关性。