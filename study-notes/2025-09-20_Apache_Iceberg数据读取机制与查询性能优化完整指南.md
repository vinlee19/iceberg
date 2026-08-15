# 2025-09-20 Apache Iceberg数据读取机制与查询性能优化完整指南

## 文档信息

- **创建日期**: 2025年9月20日
- **主题**: Apache Iceberg数据读取机制深度分析与性能优化实践指南
- **版本**: v1.0
- **适用版本**: Apache Iceberg 1.9.x

## 目录

1. [概述](#概述)
2. [数据读取机制详细分析](#数据读取机制详细分析)
3. [索引与谓词下推优化](#索引与谓词下推优化)
4. [Limit下推与查询优化技术](#limit下推与查询优化技术)
5. [小文件合并机制与策略](#小文件合并机制与策略)
6. [Delete文件优化与清理机制](#delete文件优化与清理机制)
7. [性能优化最佳实践](#性能优化最佳实践)
8. [实践案例与解决方案](#实践案例与解决方案)

---

## 概述

Apache Iceberg作为现代数据湖的核心组件，通过其独特的架构设计实现了高效的数据访问和管理。本指南将深入分析Iceberg的数据读取机制，并提供全面的性能优化策略，特别关注索引优化、谓词下推、小文件治理和删除文件管理等关键技术。

### 核心价值

- **元数据驱动**: 通过丰富的元数据支持高效的查询优化
- **多层过滤**: 从分区到文件再到行级的渐进式过滤
- **ACID支持**: 完整的事务性保证和时间旅行能力
- **格式无关**: 支持Parquet、ORC、Avro等多种文件格式
- **自动优化**: 提供多种自动化的性能优化机制

---

## 数据读取机制详细分析

### 整体架构流程图

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

### 关键组件深度分析

#### 1. TableMetadata - 表级元数据管理

**核心功能**:
- 管理表的schema演化历史
- 维护分区规格(PartitionSpec)
- 管理快照(Snapshot)索引
- 提供表属性配置

**关键源码位置**: `/core/src/main/java/org/apache/iceberg/TableMetadata.java`

```java
public class TableMetadata implements Serializable {
    // 当前激活的快照ID
    private final long currentSnapshotId;

    // 延迟加载的快照供应商
    private SerializableSupplier<List<Snapshot>> snapshotsSupplier;

    // 快照索引: snapshotId -> Snapshot
    private volatile Map<Long, Snapshot> snapshotsById;

    // 分支和标签引用
    private volatile Map<String, SnapshotRef> refs;

    // 获取当前快照 - 查询入口点
    public Snapshot currentSnapshot() {
        return currentSnapshotId != null ? snapshot(currentSnapshotId) : null;
    }
}
```

#### 2. Snapshot - 时间点一致性视图

**核心功能**:
- 提供特定时间点的表状态
- 管理manifest list文件引用
- 支持增量读取和时间旅行

**关键源码位置**:
- API: `/api/src/main/java/org/apache/iceberg/Snapshot.java`
- 实现: `/core/src/main/java/org/apache/iceberg/BaseSnapshot.java`

```java
class BaseSnapshot implements Snapshot {
    private final String manifestListLocation;
    private transient List<ManifestFile> allManifests = null;

    private void cacheManifests(FileIO fileIO) {
        if (allManifests == null) {
            // 核心: 读取manifest list文件
            this.allManifests = ManifestLists.read(fileIO.newInputFile(manifestListLocation));
        }

        // 按内容类型分离data和delete manifest
        if (dataManifests == null || deleteManifests == null) {
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

#### 3. ManifestList - 文件索引中心

**核心功能**:
- 存储所有manifest文件的元数据
- 支持快速的manifest文件定位
- 提供文件级别的统计信息

**关键源码位置**: `/core/src/main/java/org/apache/iceberg/ManifestLists.java`

```java
class ManifestLists {
    // 读取manifest list文件，返回manifest文件列表
    static List<ManifestFile> read(InputFile manifestList) {
        try (CloseableIterable<ManifestFile> files =
            InternalData.read(FileFormat.AVRO, manifestList)
                .setRootType(GenericManifestFile.class)
                .project(ManifestFile.schema())
                .build()) {
            return Lists.newLinkedList(files);
        }
    }
}
```

#### 4. ManifestFile & ManifestReader - 文件内容管理

**核心功能**:
- 存储数据文件的详细元数据
- 支持过滤器下推到文件级别
- 提供分区和列级统计信息

**ManifestFile Schema**:
```java
// 基本信息
String path();                    // manifest文件路径
long length();                    // 文件长度
int partitionSpecId();           // 分区规格ID
ManifestContent content();       // 内容类型(DATA/DELETES)

// 统计信息
Integer addedFilesCount();       // 新增文件数
Integer deletedFilesCount();     // 删除文件数
Long addedRowsCount();           // 新增行数
Long deletedRowsCount();         // 删除行数

// 分区摘要
List<PartitionFieldSummary> partitions();
```

#### 5. DataFile - 数据文件元数据

**核心功能**:
- 存储数据文件的完整元数据
- 提供列级统计信息用于查询优化
- 支持分区和排序信息

**DataFile Schema**:
```java
// 基本文件信息 (ID 100-105)
FILE_PATH = required(100, "file_path", StringType.get())
FILE_FORMAT = required(101, "file_format", StringType.get())
RECORD_COUNT = required(103, "record_count", LongType.get())
FILE_SIZE = required(104, "file_size_in_bytes", LongType.get())

// 列级统计信息 (ID 108-139)
COLUMN_SIZES = optional(108, "column_sizes", MapType.of(...))      // 列大小统计
VALUE_COUNTS = optional(109, "value_counts", MapType.of(...))      // 值计数统计
NULL_VALUE_COUNTS = optional(110, "null_value_counts", MapType.of(...)) // 空值统计
LOWER_BOUNDS = optional(125, "lower_bounds", MapType.of(...))      // 最小值边界
UPPER_BOUNDS = optional(128, "upper_bounds", MapType.of(...))      // 最大值边界
NAN_VALUE_COUNTS = optional(137, "nan_value_counts", MapType.of(...)) // NaN统计

// 高级特性 (ID 140+)
SORT_ORDER_ID = optional(140, "sort_order_id", IntegerType.get())  // 排序ID
SPEC_ID = optional(141, "spec_id", IntegerType.get())             // 分区规格ID
SPLIT_OFFSETS = optional(132, "split_offsets", ListType.of(...))   // 拆分偏移量
```

---

## 索引与谓词下推优化

### 多层次过滤器架构

Iceberg实现了从分区到文件再到行级的多层次过滤器下推优化:

```
┌─────────────────────────────────────────────────────────────┐
│                    查询过滤器下推层次                         │
├─────────────────────────────────────────────────────────────┤
│ 1. 分区级过滤 (Partition Level)                            │
│    ├─ ManifestEvaluator.forPartitionFilter()              │
│    ├─ 基于PartitionFieldSummary过滤manifest文件            │
│    └─ 跳过不相关分区的manifest                             │
├─────────────────────────────────────────────────────────────┤
│ 2. 文件级过滤 (File Level)                                │
│    ├─ InclusiveMetricsEvaluator.eval()                    │
│    ├─ 基于DataFile统计信息过滤                              │
│    └─ 跳过不包含匹配数据的文件                              │
├─────────────────────────────────────────────────────────────┤
│ 3. 行级过滤 (Row Level)                                   │
│    ├─ ResidualEvaluator.residualFor()                     │
│    ├─ 下推到文件格式层(Parquet/ORC)                        │
│    └─ 在数据读取时应用剩余过滤条件                          │
└─────────────────────────────────────────────────────────────┘
```

### 1. 分区级过滤优化

#### ManifestEvaluator实现

**源码位置**: `/api/src/main/java/org/apache/iceberg/expressions/ManifestEvaluator.java`

```java
public class ManifestEvaluator {
    // 为行过滤器创建manifest评估器
    public static ManifestEvaluator forRowFilter(
        Expression rowFilter, PartitionSpec spec, boolean caseSensitive) {
        return new ManifestEvaluator(
            spec, Projections.inclusive(spec, caseSensitive).project(rowFilter), caseSensitive);
    }

    // 评估manifest是否可能包含匹配数据
    public boolean eval(ManifestFile manifest) {
        return new ManifestEvalVisitor().eval(manifest);
    }

    private class ManifestEvalVisitor extends BoundExpressionVisitor<Boolean> {
        private boolean eval(ManifestFile manifest) {
            this.stats = manifest.partitions(); // 获取分区摘要统计
            if (stats == null) {
                return ROWS_MIGHT_MATCH; // 无统计信息时保守处理
            }
            return ExpressionVisitors.visitEvaluator(expr, this);
        }
    }
}
```

#### 分区投影机制

**源码位置**: `/api/src/main/java/org/apache/iceberg/expressions/Projections.java`

```java
public class Projections {
    // 创建包容性投影评估器
    public static ProjectionEvaluator inclusive(PartitionSpec spec, boolean caseSensitive) {
        return new InclusiveProjection(spec, caseSensitive);
    }

    // 将行表达式投影到分区表达式
    public abstract static class ProjectionEvaluator extends ExpressionVisitor<Expression> {
        public abstract Expression project(Expression expr);
    }
}
```

#### 优化示例

```java
// 原始查询过滤器
Expression rowFilter = Expressions.and(
    Expressions.greaterThan("date", "2023-01-01"),
    Expressions.lessThan("date", "2023-12-31"),
    Expressions.equal("status", "active")
);

// 1. 投影到分区过滤器 (假设按date分区)
Expression partitionFilter = Projections.inclusive(spec).project(rowFilter);
// 结果: date >= 2023-01-01 AND date <= 2023-12-31

// 2. 创建manifest评估器
ManifestEvaluator evaluator = ManifestEvaluator.forRowFilter(rowFilter, spec, true);

// 3. 过滤manifest文件
List<ManifestFile> filteredManifests = dataManifests.stream()
    .filter(evaluator::eval)
    .collect(Collectors.toList());
```

### 2. 文件级过滤优化

#### InclusiveMetricsEvaluator实现

**源码位置**: `/api/src/main/java/org/apache/iceberg/expressions/InclusiveMetricsEvaluator.java`

```java
public class InclusiveMetricsEvaluator {
    private final Expression expr;

    public InclusiveMetricsEvaluator(Schema schema, Expression unbound, boolean caseSensitive) {
        StructType struct = schema.asStruct();
        this.expr = Binder.bind(struct, rewriteNot(unbound), caseSensitive);
    }

    // 评估文件是否可能包含匹配记录
    public boolean eval(ContentFile<?> file) {
        return new MetricsEvalVisitor().eval(file);
    }

    private class MetricsEvalVisitor extends ExpressionVisitors.BoundVisitor<Boolean> {
        private Map<Integer, Long> valueCounts;
        private Map<Integer, Long> nullCounts;
        private Map<Integer, ByteBuffer> lowerBounds;
        private Map<Integer, ByteBuffer> upperBounds;

        private boolean eval(ContentFile<?> file) {
            if (file.recordCount() == 0) {
                return ROWS_CANNOT_MATCH;
            }

            // 提取文件统计信息
            this.valueCounts = file.valueCounts();
            this.nullCounts = file.nullValueCounts();
            this.lowerBounds = file.lowerBounds();
            this.upperBounds = file.upperBounds();

            return ExpressionVisitors.visitEvaluator(expr, this);
        }

        @Override
        public Boolean lessThan(BoundLiteralPredicate<T> pred) {
            Integer id = pred.ref().fieldId();
            ByteBuffer lower = lowerBounds.get(id);

            if (lower == null) {
                return ROWS_MIGHT_MATCH; // 无边界信息，保守处理
            }

            T lowerValue = Conversions.fromByteBuffer(pred.ref().type(), lower);
            return pred.literal().comparator().compare(lowerValue, pred.literal().value()) < 0;
        }

        // 其他谓词评估方法...
    }
}
```

#### 统计信息利用策略

```java
// 文件级过滤示例
public boolean canSkipFile(DataFile file, Expression filter) {
    InclusiveMetricsEvaluator evaluator = new InclusiveMetricsEvaluator(schema, filter, true);

    // 利用以下统计信息进行过滤:
    // 1. 记录数 - 跳过空文件
    if (file.recordCount() == 0) return true;

    // 2. 空值统计 - 优化NULL检查
    if (isNotNullFilter(filter) && allValuesAreNull(file, getColumnId(filter))) {
        return true;
    }

    // 3. 边界值 - 范围查询优化
    if (isRangeFilter(filter) && !overlapsRange(file, filter)) {
        return true;
    }

    // 4. 值计数 - 存在性检查
    if (isExistsFilter(filter) && getValueCount(file, getColumnId(filter)) == 0) {
        return true;
    }

    return !evaluator.eval(file);
}
```

### 3. 行级过滤与残差处理

#### ResidualEvaluator实现

**源码位置**: `/api/src/main/java/org/apache/iceberg/expressions/ResidualEvaluator.java`

```java
public class ResidualEvaluator {
    // 计算分区过滤后的残差表达式
    public static Expression residualFor(PartitionSpec spec, Expression expr, boolean caseSensitive) {
        return ExpressionVisitors.visit(expr, new ResidualVisitor(spec, caseSensitive));
    }

    private static class ResidualVisitor extends ExpressionVisitors.ExpressionVisitor<Expression> {
        // 对于无法在分区级别完全评估的表达式，返回残差
        @Override
        public Expression predicate(BoundPredicate<T> pred) {
            if (canBeEvaluatedInPartition(pred)) {
                return Expressions.alwaysTrue(); // 已在分区级别处理
            }
            return pred; // 需要在行级别处理
        }
    }
}
```

#### 下推到文件格式层

```java
// 读取文件时应用行级过滤
public CloseableIterable<InternalRow> readFile(FileScanTask task) {
    DataFile file = task.file();
    Expression residual = task.residual();

    switch (file.format()) {
        case PARQUET:
            return Parquet.read(fileIO.newInputFile(file.path().toString()))
                .project(schema)
                .filter(residual)  // 下推到Parquet层
                .build();

        case ORC:
            return ORC.read(fileIO.newInputFile(file.path().toString()))
                .project(schema)
                .filter(residual)  // 下推到ORC层
                .build();

        case AVRO:
            // Avro不支持谓词下推，在读取后过滤
            CloseableIterable<InternalRow> records = Avro.read(inputFile).build();
            return Expressions.filterRows(records, residual, schema);
    }
}
```

---

## Limit下推与查询优化技术

### Limit下推机制

虽然Iceberg本身不直接支持limit下推，但在计算引擎层面可以实现多种优化策略:

#### 1. 文件级Limit优化

```java
public class OptimizedTableScan {
    private long limit = -1;

    public TableScan limit(long limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public CloseableIterable<FileScanTask> planFiles() {
        if (limit > 0) {
            return new LimitedFileScanIterable(super.planFiles(), limit);
        }
        return super.planFiles();
    }

    private static class LimitedFileScanIterable implements CloseableIterable<FileScanTask> {
        private final CloseableIterable<FileScanTask> wrapped;
        private final long limit;
        private long processedRecords = 0;

        @Override
        public CloseableIterator<FileScanTask> iterator() {
            return new CloseableIterator<FileScanTask>() {
                private final CloseableIterator<FileScanTask> wrappedIter = wrapped.iterator();

                @Override
                public boolean hasNext() {
                    return processedRecords < limit && wrappedIter.hasNext();
                }

                @Override
                public FileScanTask next() {
                    FileScanTask task = wrappedIter.next();
                    processedRecords += task.file().recordCount();

                    // 如果超出limit，可以截断文件
                    if (processedRecords > limit) {
                        long excess = processedRecords - limit;
                        long newLength = task.file().recordCount() - excess;
                        return createTruncatedTask(task, newLength);
                    }

                    return task;
                }
            };
        }
    }
}
```

#### 2. 分区排序优化

```java
// 基于分区统计信息的智能扫描
public class PartitionOrderedScan {

    public CloseableIterable<FileScanTask> planFilesWithLimit(long limit, SortOrder sortOrder) {
        // 1. 获取所有manifest文件
        List<ManifestFile> manifests = snapshot.dataManifests(fileIO);

        // 2. 根据分区统计信息排序
        List<ManifestFile> sortedManifests = sortManifestsByPartition(manifests, sortOrder);

        // 3. 按顺序扫描直到满足limit
        return new OrderedLimitScan(sortedManifests, limit);
    }

    private List<ManifestFile> sortManifestsByPartition(List<ManifestFile> manifests, SortOrder sortOrder) {
        return manifests.stream()
            .sorted((m1, m2) -> comparePartitionBounds(m1, m2, sortOrder))
            .collect(Collectors.toList());
    }

    private int comparePartitionBounds(ManifestFile m1, ManifestFile m2, SortOrder sortOrder) {
        List<PartitionFieldSummary> p1 = m1.partitions();
        List<PartitionFieldSummary> p2 = m2.partitions();

        for (SortField field : sortOrder.fields()) {
            int fieldId = field.sourceId();
            int partitionIndex = getPartitionIndex(fieldId);

            if (partitionIndex >= 0) {
                ByteBuffer bound1 = field.direction() == SortDirection.ASC ?
                    p1.get(partitionIndex).lowerBound() : p1.get(partitionIndex).upperBound();
                ByteBuffer bound2 = field.direction() == SortDirection.ASC ?
                    p2.get(partitionIndex).lowerBound() : p2.get(partitionIndex).upperBound();

                int cmp = compareBounds(bound1, bound2, field.direction());
                if (cmp != 0) return cmp;
            }
        }

        return 0;
    }
}
```

### 查询计划优化

#### 1. 统计信息驱动的查询计划

```java
public class StatisticsBasedPlanner {

    public QueryPlan optimizePlan(TableScan scan, Expression filter, List<String> projectedColumns) {
        // 1. 估算过滤后的数据量
        long estimatedRows = estimateFilteredRows(scan, filter);

        // 2. 基于数据量选择执行策略
        if (estimatedRows < SMALL_TABLE_THRESHOLD) {
            return createSinglePartitionPlan(scan, filter, projectedColumns);
        } else {
            return createDistributedPlan(scan, filter, projectedColumns);
        }
    }

    private long estimateFilteredRows(TableScan scan, Expression filter) {
        long totalRows = 0;
        long filteredRows = 0;

        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                DataFile file = task.file();
                totalRows += file.recordCount();

                // 基于统计信息估算选择性
                double selectivity = estimateSelectivity(file, filter);
                filteredRows += (long) (file.recordCount() * selectivity);
            }
        }

        return filteredRows;
    }

    private double estimateSelectivity(DataFile file, Expression filter) {
        if (filter == null || filter == Expressions.alwaysTrue()) {
            return 1.0;
        }

        // 基于列统计信息估算选择性
        return new SelectivityEstimator(file).estimate(filter);
    }
}
```

#### 2. 列裁剪优化

```java
public class ColumnPruningOptimizer {

    public Schema pruneSchema(Schema tableSchema, List<String> requiredColumns, Expression filter) {
        Set<String> allRequiredColumns = Sets.newHashSet(requiredColumns);

        // 1. 添加过滤器中引用的列
        addFilterColumns(filter, allRequiredColumns);

        // 2. 添加分区列（如果需要）
        addPartitionColumns(tableSchema, allRequiredColumns);

        // 3. 构建裁剪后的schema
        return SchemaUtil.select(tableSchema, allRequiredColumns);
    }

    private void addFilterColumns(Expression filter, Set<String> columns) {
        ExpressionVisitors.visit(filter, new ExpressionVisitor<Void>() {
            @Override
            public Void reference(BoundReference<?> ref) {
                columns.add(ref.name());
                return null;
            }
        });
    }

    // 优化文件读取时的列选择
    public ManifestReader<DataFile> createOptimizedReader(ManifestFile manifest,
                                                         FileIO fileIO,
                                                         List<String> requiredColumns) {
        ManifestReader<DataFile> reader = ManifestFiles.readDataManifest(manifest, fileIO, null);

        if (requiredColumns != null && !requiredColumns.isEmpty()) {
            // 只读取需要的列的统计信息
            List<String> statsColumns = getStatsColumns(requiredColumns);
            reader = reader.select(statsColumns);
        }

        return reader;
    }
}
```

---

## 小文件合并机制与策略

### 小文件问题分析

小文件问题是数据湖系统的常见挑战，会导致:

1. **查询性能下降**: 大量小文件增加文件系统开销
2. **元数据膨胀**: 过多的文件元数据影响查询规划
3. **存储效率低**: 小文件无法充分利用列式存储优势
4. **并行度受限**: 文件数量限制了可并行处理的任务数

### Iceberg文件合并架构

```
┌─────────────────────────────────────────────────────────────┐
│                   文件重写系统架构                           │
├─────────────────────────────────────────────────────────────┤
│                   RewriteDataFiles                         │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ 1. 文件选择策略 (File Selection Strategy)           │   │
│  │    ├─ SizeBasedFileRewriter                        │   │
│  │    ├─ 基于文件大小阈值选择                          │   │
│  │    └─ 支持自定义选择逻辑                           │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ 2. 文件分组策略 (File Grouping Strategy)           │   │
│  │    ├─ BinPacking Algorithm                         │   │
│  │    ├─ 按目标大小分组文件                            │   │
│  │    └─ 考虑分区边界约束                             │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ 3. 重写执行策略 (Rewrite Execution)               │   │
│  │    ├─ SparkBinPackDataRewriter                     │   │
│  │    ├─ SparkSortDataRewriter                        │   │
│  │    └─ 支持并行重写和增量提交                        │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### 1. SizeBasedFileRewriter - 基于大小的文件重写器

**源码位置**: `/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewriter.java`

```java
public abstract class SizeBasedFileRewriter<T extends ContentScanTask<F>, F extends ContentFile<F>>
    implements FileRewriter<T, F> {

    // 核心配置参数
    public static final String TARGET_FILE_SIZE_BYTES = "target-file-size-bytes";
    public static final String MIN_FILE_SIZE_BYTES = "min-file-size-bytes";
    public static final String MAX_FILE_SIZE_BYTES = "max-file-size-bytes";
    public static final String MIN_INPUT_FILES = "min-input-files";
    public static final String MAX_FILE_GROUP_SIZE_BYTES = "max-file-group-size-bytes";

    // 默认阈值
    public static final double MIN_FILE_SIZE_DEFAULT_RATIO = 0.75;  // 75% of target
    public static final double MAX_FILE_SIZE_DEFAULT_RATIO = 1.80;  // 180% of target
    public static final int MIN_INPUT_FILES_DEFAULT = 5;

    @Override
    public Set<FileGroupInfo> planFileGroups(Iterable<T> dataFiles) {
        // 1. 过滤需要重写的文件
        List<T> filesToRewrite = selectFilesToRewrite(dataFiles);

        // 2. 按分区分组
        Map<StructLike, List<T>> filesByPartition = groupByPartition(filesToRewrite);

        // 3. 在每个分区内使用bin-packing算法分组
        Set<FileGroupInfo> fileGroups = Sets.newHashSet();
        for (Map.Entry<StructLike, List<T>> entry : filesByPartition.entrySet()) {
            fileGroups.addAll(packFilesIntoGroups(entry.getValue()));
        }

        return fileGroups;
    }

    private List<T> selectFilesToRewrite(Iterable<T> dataFiles) {
        List<T> filesToRewrite = Lists.newArrayList();

        for (T file : dataFiles) {
            if (shouldRewrite(file)) {
                filesToRewrite.add(file);
            }
        }

        return filesToRewrite;
    }

    private boolean shouldRewrite(T file) {
        long fileSize = file.file().fileSizeInBytes();

        // 检查文件大小是否在合理范围内
        return fileSize < minFileSizeBytes() || fileSize > maxFileSizeBytes();
    }

    private Set<FileGroupInfo> packFilesIntoGroups(List<T> files) {
        // 使用bin-packing算法将文件分组
        List<List<T>> packedGroups = BinPacking.packEnd(
            files,
            maxFileGroupSizeBytes(),
            file -> file.file().fileSizeInBytes(),
            false // 不要求精确匹配
        );

        return packedGroups.stream()
            .filter(this::shouldRewriteGroup)
            .map(this::toFileGroupInfo)
            .collect(Collectors.toSet());
    }

    private boolean shouldRewriteGroup(List<T> group) {
        // 只有满足以下条件之一的组才会被重写:
        // 1. 文件数量超过阈值
        // 2. 重写后能产生至少一个目标大小的文件

        if (group.size() >= minInputFiles()) {
            return true;
        }

        long totalSize = group.stream()
            .mapToLong(file -> file.file().fileSizeInBytes())
            .sum();

        return totalSize >= targetFileSizeBytes();
    }
}
```

### 2. BinPacking算法实现

**源码位置**: `/core/src/main/java/org/apache/iceberg/util/BinPacking.java`

```java
public class BinPacking {

    // First Fit Decreasing (FFD) bin packing算法
    public static <T> List<List<T>> packEnd(Iterable<T> items,
                                           long targetWeight,
                                           Function<T, Long> weightFunc,
                                           boolean checkMagnitude) {

        // 1. 按权重降序排序
        List<Weighted<T>> sortedItems = StreamSupport.stream(items.spliterator(), false)
            .map(item -> new Weighted<>(item, weightFunc.apply(item)))
            .sorted((a, b) -> Long.compare(b.weight(), a.weight()))
            .collect(Collectors.toList());

        // 2. 使用FFD算法分组
        List<Bin<T>> bins = Lists.newArrayList();

        for (Weighted<T> item : sortedItems) {
            boolean placed = false;

            // 尝试放入现有的bin
            for (Bin<T> bin : bins) {
                if (bin.canAdd(item.weight(), targetWeight)) {
                    bin.add(item);
                    placed = true;
                    break;
                }
            }

            // 如果无法放入现有bin，创建新bin
            if (!placed) {
                Bin<T> newBin = new Bin<>();
                newBin.add(item);
                bins.add(newBin);
            }
        }

        // 3. 转换为结果格式
        return bins.stream()
            .map(Bin::items)
            .collect(Collectors.toList());
    }

    private static class Bin<T> {
        private final List<Weighted<T>> items = Lists.newArrayList();
        private long currentWeight = 0;

        boolean canAdd(long itemWeight, long targetWeight) {
            return currentWeight + itemWeight <= targetWeight;
        }

        void add(Weighted<T> item) {
            items.add(item);
            currentWeight += item.weight();
        }

        List<T> items() {
            return items.stream()
                .map(Weighted::item)
                .collect(Collectors.toList());
        }
    }
}
```

### 3. SparkBinPackDataRewriter - Spark实现

**源码位置**: `/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkBinPackDataRewriter.java`

```java
class SparkBinPackDataRewriter extends SparkSizeBasedDataRewriter {

    @Override
    public String description() {
        return "BIN-PACK";
    }

    @Override
    protected void doRewrite(String groupId, List<FileScanTask> group) {
        // 1. 读取文件并打包到适当大小的分片
        Dataset<Row> scanDF = spark()
            .read()
            .format("iceberg")
            .option(SparkReadOptions.SCAN_TASK_SET_ID, groupId)
            .option(SparkReadOptions.SPLIT_SIZE, splitSize(inputSize(group)))
            .option(SparkReadOptions.FILE_OPEN_COST, "0")
            .load(groupId);

        // 2. 将打包的数据写入新文件，每个分片变成一个新文件
        scanDF.write()
            .format("iceberg")
            .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, groupId)
            .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, writeMaxFileSize())
            .option(SparkWriteOptions.DISTRIBUTION_MODE, distributionMode(group).modeName())
            .option(SparkWriteOptions.OUTPUT_SPEC_ID, outputSpecId())
            .mode("append")
            .save(groupId);
    }

    // 如果原始规格与输出规格不匹配，则调用shuffle
    private DistributionMode distributionMode(List<FileScanTask> group) {
        boolean requiresRepartition = !group.get(0).spec().equals(outputSpec());
        return requiresRepartition ? DistributionMode.RANGE : DistributionMode.NONE;
    }
}
```

### 4. 文件合并最佳实践

#### 配置优化参数

```java
// 表属性配置
Map<String, String> tableProperties = Maps.newHashMap();

// 目标文件大小 (默认128MB)
tableProperties.put("write.target-file-size-bytes", "134217728");

// 小文件阈值 (75% of target = 96MB)
tableProperties.put("write.rewrite.min-file-size-bytes", "100663296");

// 大文件阈值 (180% of target = 230MB)
tableProperties.put("write.rewrite.max-file-size-bytes", "241172480");

// 最小输入文件数
tableProperties.put("write.rewrite.min-input-files", "5");

// 最大文件组大小 (5GB)
tableProperties.put("write.rewrite.max-file-group-size-bytes", "5368709120");

// 并发重写组数
tableProperties.put("write.rewrite.max-concurrent-file-group-rewrites", "5");
```

#### 自动化合并策略

```java
public class AutoCompactionService {

    public void runAutoCompaction(Table table) {
        RewriteDataFiles rewriteAction = Actions.forTable(table).rewriteDataFiles();

        // 1. 配置重写参数
        rewriteAction = rewriteAction
            .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, table.properties()
                .getOrDefault("write.target-file-size-bytes", "134217728"))
            .option(RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_REWRITES, "5")
            .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, "true");

        // 2. 设置过滤条件（可选）
        if (shouldFilterByPartition()) {
            rewriteAction = rewriteAction.filter(getPartitionFilter());
        }

        // 3. 执行重写
        RewriteDataFiles.Result result = rewriteAction.execute();

        // 4. 记录结果
        LOG.info("Compaction completed: {} files rewritten, {} files added, {} bytes processed",
            result.rewrittenFilesCount(), result.addedFilesCount(), result.rewrittenBytesCount());
    }

    private boolean shouldRunCompaction(Table table) {
        // 基于以下指标决定是否运行合并:
        // 1. 小文件比例
        // 2. 上次合并时间
        // 3. 数据增长量

        TableStatistics stats = getTableStatistics(table);

        double smallFileRatio = (double) stats.smallFilesCount() / stats.totalFilesCount();
        long timeSinceLastCompaction = System.currentTimeMillis() - stats.lastCompactionTime();

        return smallFileRatio > 0.3 || timeSinceLastCompaction > TimeUnit.HOURS.toMillis(24);
    }
}
```

#### 分区级合并优化

```java
public class PartitionAwareCompaction {

    public void compactPartitions(Table table, List<StructLike> partitions) {
        for (StructLike partition : partitions) {
            try {
                compactSinglePartition(table, partition);
            } catch (Exception e) {
                LOG.warn("Failed to compact partition {}: {}", partition, e.getMessage());
                // 继续处理其他分区
            }
        }
    }

    private void compactSinglePartition(Table table, StructLike partition) {
        Expression partitionFilter = createPartitionFilter(table.spec(), partition);

        RewriteDataFiles rewriteAction = Actions.forTable(table)
            .rewriteDataFiles()
            .filter(partitionFilter)
            .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, "134217728");

        RewriteDataFiles.Result result = rewriteAction.execute();

        if (result.rewrittenFilesCount() > 0) {
            LOG.info("Compacted partition {}: {} -> {} files",
                partition, result.rewrittenFilesCount(), result.addedFilesCount());
        }
    }

    private Expression createPartitionFilter(PartitionSpec spec, StructLike partition) {
        List<Expression> predicates = Lists.newArrayList();

        List<PartitionField> fields = spec.fields();
        for (int i = 0; i < fields.size(); i++) {
            PartitionField field = fields.get(i);
            Object value = partition.get(i, Object.class);

            if (value != null) {
                predicates.add(Expressions.equal(field.name(), value));
            } else {
                predicates.add(Expressions.isNull(field.name()));
            }
        }

        return Expressions.and(predicates.toArray(new Expression[0]));
    }
}
```

---

## Delete文件优化与清理机制

### Delete文件架构分析

Iceberg支持两种类型的delete文件：

1. **Position Delete**: 基于行位置的删除
2. **Equality Delete**: 基于字段值的删除

```
┌─────────────────────────────────────────────────────────────┐
│                   Delete文件系统架构                        │
├─────────────────────────────────────────────────────────────┤
│  Position Delete Files                                     │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Structure: (file_path, position)                   │   │
│  │ - 精确指定要删除的行位置                            │   │
│  │ - 适用于基于主键的删除操作                          │   │
│  │ - 读取时需要与数据文件匹配                          │   │
│  └─────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│  Equality Delete Files                                     │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Structure: equality_ids + deleted record values    │   │
│  │ - 存储完整的删除记录                               │   │
│  │ - 基于指定字段的值匹配删除                          │   │
│  │ - 支持复合键删除                                   │   │
│  └─────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│  Delete File Integration                                   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ DeleteFileIndex                                     │   │
│  │ ├─ 为每个DataFile构建对应的DeleteFile列表           │   │
│  │ ├─ 支持快速查找和过滤                              │   │
│  │ └─ 优化delete文件的读取顺序                         │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### 1. Delete文件读取与应用

#### DeleteFileIndex实现

**源码位置**: `/core/src/main/java/org/apache/iceberg/DeleteFileIndex.java`

```java
public class DeleteFileIndex {

    // 为数据文件查找对应的delete文件
    public List<DeleteFile> forDataFile(DataFile dataFile) {
        List<DeleteFile> deleteFiles = Lists.newArrayList();

        // 1. 查找position delete文件
        List<DeleteFile> positionDeletes = findPositionDeletes(dataFile);
        deleteFiles.addAll(positionDeletes);

        // 2. 查找equality delete文件
        List<DeleteFile> equalityDeletes = findEqualityDeletes(dataFile);
        deleteFiles.addAll(equalityDeletes);

        return deleteFiles;
    }

    private List<DeleteFile> findPositionDeletes(DataFile dataFile) {
        // Position delete文件通过引用数据文件路径来关联
        return deleteFiles.stream()
            .filter(deleteFile -> deleteFile.content() == FileContent.POSITION_DELETES)
            .filter(deleteFile -> {
                String referencedDataFile = deleteFile.referencedDataFile();
                return referencedDataFile != null &&
                       referencedDataFile.equals(dataFile.path().toString());
            })
            .collect(Collectors.toList());
    }

    private List<DeleteFile> findEqualityDeletes(DataFile dataFile) {
        // Equality delete文件通过分区和序列号来关联
        return deleteFiles.stream()
            .filter(deleteFile -> deleteFile.content() == FileContent.EQUALITY_DELETES)
            .filter(deleteFile -> canApplyEqualityDelete(deleteFile, dataFile))
            .collect(Collectors.toList());
    }

    private boolean canApplyEqualityDelete(DeleteFile deleteFile, DataFile dataFile) {
        // 1. 检查分区兼容性
        if (!partitionsOverlap(deleteFile.partition(), dataFile.partition())) {
            return false;
        }

        // 2. 检查序列号 - delete文件必须在数据文件之后创建
        return deleteFile.dataSequenceNumber() >= dataFile.dataSequenceNumber();
    }

    // 构建delete文件索引的Builder
    public static class Builder {
        private final FileIO fileIO;
        private final Map<Integer, PartitionSpec> specsById;
        private final List<ManifestFile> deleteManifests;

        public DeleteFileIndex build() {
            Map<StructLike, List<DeleteFile>> deleteFilesByPartition = Maps.newHashMap();

            // 读取所有delete manifest文件
            for (ManifestFile manifest : deleteManifests) {
                try (ManifestReader<DeleteFile> reader =
                     ManifestFiles.readDeleteManifest(manifest, fileIO, specsById)) {

                    for (ManifestEntry<DeleteFile> entry : reader.entries()) {
                        if (entry.status() != ManifestEntry.Status.DELETED) {
                            DeleteFile deleteFile = entry.file();
                            StructLike partition = entry.file().partition();

                            deleteFilesByPartition
                                .computeIfAbsent(partition, k -> Lists.newArrayList())
                                .add(deleteFile);
                        }
                    }
                }
            }

            return new DeleteFileIndex(deleteFilesByPartition);
        }
    }
}
```

#### Delete文件应用逻辑

**源码位置**: `/core/src/main/java/org/apache/iceberg/deletes/Deletes.java`

```java
public class Deletes {

    // 对数据记录应用delete文件
    public static <T> CloseableIterable<T> filter(CloseableIterable<T> records,
                                                  List<DeleteFile> deleteFiles,
                                                  Schema schema) {
        if (deleteFiles.isEmpty()) {
            return records;
        }

        // 1. 分离position delete和equality delete
        List<DeleteFile> positionDeletes = deleteFiles.stream()
            .filter(f -> f.content() == FileContent.POSITION_DELETES)
            .collect(Collectors.toList());

        List<DeleteFile> equalityDeletes = deleteFiles.stream()
            .filter(f -> f.content() == FileContent.EQUALITY_DELETES)
            .collect(Collectors.toList());

        CloseableIterable<T> filtered = records;

        // 2. 应用position delete
        if (!positionDeletes.isEmpty()) {
            Set<Long> deletedPositions = loadDeletedPositions(positionDeletes);
            filtered = filterByPosition(filtered, deletedPositions);
        }

        // 3. 应用equality delete
        if (!equalityDeletes.isEmpty()) {
            filtered = filterByEquality(filtered, equalityDeletes, schema);
        }

        return filtered;
    }

    private static Set<Long> loadDeletedPositions(List<DeleteFile> positionDeletes) {
        Set<Long> deletedPositions = Sets.newHashSet();

        for (DeleteFile deleteFile : positionDeletes) {
            try (CloseableIterable<Record> deletes =
                 readPositionDeleteFile(deleteFile)) {

                for (Record delete : deletes) {
                    Long position = delete.getField("pos");
                    if (position != null) {
                        deletedPositions.add(position);
                    }
                }
            }
        }

        return deletedPositions;
    }

    private static <T> CloseableIterable<T> filterByPosition(CloseableIterable<T> records,
                                                           Set<Long> deletedPositions) {
        return new CloseableIterable<T>() {
            @Override
            public CloseableIterator<T> iterator() {
                return new FilteringIterator<T>(records.iterator()) {
                    private long currentPosition = 0;

                    @Override
                    protected boolean shouldKeep(T record) {
                        boolean keep = !deletedPositions.contains(currentPosition);
                        currentPosition++;
                        return keep;
                    }
                };
            }
        };
    }

    private static <T> CloseableIterable<T> filterByEquality(CloseableIterable<T> records,
                                                           List<DeleteFile> equalityDeletes,
                                                           Schema schema) {
        // 构建equality delete索引
        Map<Object, Set<List<Object>>> deleteIndex = buildEqualityDeleteIndex(equalityDeletes, schema);

        return new CloseableIterable<T>() {
            @Override
            public CloseableIterator<T> iterator() {
                return new FilteringIterator<T>(records.iterator()) {
                    @Override
                    protected boolean shouldKeep(T record) {
                        return !isDeletedByEquality(record, deleteIndex, schema);
                    }
                };
            }
        };
    }
}
```

### 2. Delete文件合并优化

#### RewritePositionDeleteFiles实现

**源码位置**: `/api/src/main/java/org/apache/iceberg/actions/RewritePositionDeleteFiles.java`

```java
public interface RewritePositionDeleteFiles
    extends SnapshotUpdate<RewritePositionDeleteFiles, RewritePositionDeleteFiles.Result> {

    // 核心配置
    String PARTIAL_PROGRESS_ENABLED = "partial-progress.enabled";
    String MAX_FILE_GROUP_SIZE_BYTES = "max-file-group-size-bytes";
    String TARGET_FILE_SIZE_BYTES = "target-file-size-bytes";
    String MIN_FILE_SIZE_BYTES = "min-file-size-bytes";
    String MIN_INPUT_FILES = "min-input-files";

    // 默认值
    long MAX_FILE_GROUP_SIZE_BYTES_DEFAULT = 1024L * 1024L * 1024L * 100L; // 100 GB
    int MIN_INPUT_FILES_DEFAULT = 5;

    // 配置方法
    RewritePositionDeleteFiles filter(Expression expression);
    RewritePositionDeleteFiles option(String name, String value);
    RewritePositionDeleteFiles sort(SortOrder sortOrder);

    // 执行重写
    Result execute();

    interface Result {
        List<DeleteFile> rewrittenDeleteFiles();
        List<DeleteFile> addedDeleteFiles();
        long rewrittenBytesCount();
        long addedBytesCount();
    }
}
```

#### SizeBasedPositionDeletesRewriter实现

**源码位置**: `/core/src/main/java/org/apache/iceberg/actions/SizeBasedPositionDeletesRewriter.java`

```java
public class SizeBasedPositionDeletesRewriter {

    public RewritePositionDeleteFiles.Result rewriteDeletes(List<DeleteFile> deleteFiles) {
        // 1. 分析delete文件特征
        DeleteFileAnalysis analysis = analyzeDeleteFiles(deleteFiles);

        // 2. 选择需要重写的文件
        List<DeleteFile> filesToRewrite = selectFilesToRewrite(deleteFiles, analysis);

        // 3. 分组文件
        List<List<DeleteFile>> fileGroups = groupDeleteFiles(filesToRewrite);

        // 4. 重写每个组
        List<DeleteFile> newDeleteFiles = Lists.newArrayList();
        for (List<DeleteFile> group : fileGroups) {
            newDeleteFiles.addAll(rewriteDeleteFileGroup(group));
        }

        return new RewriteResultImpl(filesToRewrite, newDeleteFiles);
    }

    private DeleteFileAnalysis analyzeDeleteFiles(List<DeleteFile> deleteFiles) {
        long totalSize = 0;
        long smallFilesCount = 0;
        long largeFilesCount = 0;
        Map<String, List<DeleteFile>> filesByDataFile = Maps.newHashMap();

        for (DeleteFile deleteFile : deleteFiles) {
            totalSize += deleteFile.fileSizeInBytes();

            if (deleteFile.fileSizeInBytes() < minDeleteFileSizeBytes()) {
                smallFilesCount++;
            } else if (deleteFile.fileSizeInBytes() > maxDeleteFileSizeBytes()) {
                largeFilesCount++;
            }

            // 按引用的数据文件分组
            String referencedFile = deleteFile.referencedDataFile();
            if (referencedFile != null) {
                filesByDataFile.computeIfAbsent(referencedFile, k -> Lists.newArrayList())
                    .add(deleteFile);
            }
        }

        return new DeleteFileAnalysis(totalSize, smallFilesCount, largeFilesCount, filesByDataFile);
    }

    private List<DeleteFile> selectFilesToRewrite(List<DeleteFile> deleteFiles,
                                                DeleteFileAnalysis analysis) {
        List<DeleteFile> selected = Lists.newArrayList();

        for (DeleteFile deleteFile : deleteFiles) {
            if (shouldRewriteDeleteFile(deleteFile, analysis)) {
                selected.add(deleteFile);
            }
        }

        return selected;
    }

    private boolean shouldRewriteDeleteFile(DeleteFile deleteFile, DeleteFileAnalysis analysis) {
        long fileSize = deleteFile.fileSizeInBytes();

        // 重写条件:
        // 1. 文件太小
        if (fileSize < minDeleteFileSizeBytes()) {
            return true;
        }

        // 2. 文件太大
        if (fileSize > maxDeleteFileSizeBytes()) {
            return true;
        }

        // 3. 同一数据文件有太多delete文件
        String referencedFile = deleteFile.referencedDataFile();
        if (referencedFile != null) {
            List<DeleteFile> relatedDeletes = analysis.filesByDataFile().get(referencedFile);
            if (relatedDeletes != null && relatedDeletes.size() > maxDeleteFilesPerDataFile()) {
                return true;
            }
        }

        return false;
    }

    private List<List<DeleteFile>> groupDeleteFiles(List<DeleteFile> deleteFiles) {
        // 使用改进的bin-packing算法，考虑数据文件关联性
        Map<String, List<DeleteFile>> filesByDataFile = Maps.newHashMap();
        List<DeleteFile> orphanedDeletes = Lists.newArrayList();

        // 1. 按引用的数据文件分组
        for (DeleteFile deleteFile : deleteFiles) {
            String referencedFile = deleteFile.referencedDataFile();
            if (referencedFile != null) {
                filesByDataFile.computeIfAbsent(referencedFile, k -> Lists.newArrayList())
                    .add(deleteFile);
            } else {
                orphanedDeletes.add(deleteFile);
            }
        }

        List<List<DeleteFile>> groups = Lists.newArrayList();

        // 2. 对每个数据文件的delete文件进行分组
        for (List<DeleteFile> relatedDeletes : filesByDataFile.values()) {
            groups.addAll(packDeleteFiles(relatedDeletes));
        }

        // 3. 处理孤立的delete文件
        if (!orphanedDeletes.isEmpty()) {
            groups.addAll(packDeleteFiles(orphanedDeletes));
        }

        return groups;
    }

    private List<List<DeleteFile>> packDeleteFiles(List<DeleteFile> deleteFiles) {
        return BinPacking.packEnd(
            deleteFiles,
            maxFileGroupSizeBytes(),
            DeleteFile::fileSizeInBytes,
            false // 允许不精确匹配
        );
    }
}
```

### 3. Delete文件清理策略

#### 过期Delete文件清理

```java
public class DeleteFileCleanupService {

    public void cleanupExpiredDeleteFiles(Table table) {
        // 1. 获取当前活跃的数据文件
        Set<String> activeDataFiles = getActiveDataFiles(table);

        // 2. 扫描所有delete文件
        List<DeleteFile> allDeleteFiles = getAllDeleteFiles(table);

        // 3. 识别过期的delete文件
        List<DeleteFile> expiredDeleteFiles = findExpiredDeleteFiles(allDeleteFiles, activeDataFiles);

        // 4. 安全删除过期文件
        if (!expiredDeleteFiles.isEmpty()) {
            removeExpiredDeleteFiles(table, expiredDeleteFiles);
        }
    }

    private Set<String> getActiveDataFiles(Table table) {
        Set<String> activeFiles = Sets.newHashSet();

        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                activeFiles.add(task.file().path().toString());
            }
        }

        return activeFiles;
    }

    private List<DeleteFile> findExpiredDeleteFiles(List<DeleteFile> deleteFiles,
                                                  Set<String> activeDataFiles) {
        List<DeleteFile> expired = Lists.newArrayList();

        for (DeleteFile deleteFile : deleteFiles) {
            if (isExpired(deleteFile, activeDataFiles)) {
                expired.add(deleteFile);
            }
        }

        return expired;
    }

    private boolean isExpired(DeleteFile deleteFile, Set<String> activeDataFiles) {
        // Position delete文件的过期条件
        if (deleteFile.content() == FileContent.POSITION_DELETES) {
            String referencedFile = deleteFile.referencedDataFile();
            return referencedFile != null && !activeDataFiles.contains(referencedFile);
        }

        // Equality delete文件需要更复杂的分析
        // 需要检查是否还有相关的数据文件存在
        return false; // 暂时保守处理
    }

    private void removeExpiredDeleteFiles(Table table, List<DeleteFile> expiredFiles) {
        if (expiredFiles.isEmpty()) {
            return;
        }

        LOG.info("Removing {} expired delete files", expiredFiles.size());

        // 创建删除操作
        DeleteFiles deleteOp = table.newDelete();

        for (DeleteFile expiredFile : expiredFiles) {
            deleteOp.deleteFile(expiredFile);
        }

        // 提交删除
        deleteOp.commit();

        LOG.info("Successfully removed {} expired delete files", expiredFiles.size());
    }
}
```

#### Delete文件统计和监控

```java
public class DeleteFileMonitor {

    public DeleteFileStatistics analyzeDeleteFiles(Table table) {
        long totalDeleteFiles = 0;
        long totalDeleteFileSize = 0;
        long positionDeleteFiles = 0;
        long equalityDeleteFiles = 0;
        long smallDeleteFiles = 0;
        long largeDeleteFiles = 0;

        Map<String, Long> deleteFilesByDataFile = Maps.newHashMap();

        // 扫描所有delete文件
        Snapshot currentSnapshot = table.currentSnapshot();
        if (currentSnapshot != null) {
            List<ManifestFile> deleteManifests = currentSnapshot.deleteManifests(table.io());

            for (ManifestFile manifest : deleteManifests) {
                try (ManifestReader<DeleteFile> reader =
                     ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {

                    for (ManifestEntry<DeleteFile> entry : reader.entries()) {
                        if (entry.status() != ManifestEntry.Status.DELETED) {
                            DeleteFile deleteFile = entry.file();

                            totalDeleteFiles++;
                            totalDeleteFileSize += deleteFile.fileSizeInBytes();

                            if (deleteFile.content() == FileContent.POSITION_DELETES) {
                                positionDeleteFiles++;

                                String referencedFile = deleteFile.referencedDataFile();
                                if (referencedFile != null) {
                                    deleteFilesByDataFile.merge(referencedFile, 1L, Long::sum);
                                }
                            } else {
                                equalityDeleteFiles++;
                            }

                            if (deleteFile.fileSizeInBytes() < 1024 * 1024) { // < 1MB
                                smallDeleteFiles++;
                            } else if (deleteFile.fileSizeInBytes() > 100 * 1024 * 1024) { // > 100MB
                                largeDeleteFiles++;
                            }
                        }
                    }
                }
            }
        }

        return new DeleteFileStatistics(
            totalDeleteFiles, totalDeleteFileSize,
            positionDeleteFiles, equalityDeleteFiles,
            smallDeleteFiles, largeDeleteFiles,
            deleteFilesByDataFile
        );
    }

    public void reportDeleteFileHealth(Table table) {
        DeleteFileStatistics stats = analyzeDeleteFiles(table);

        LOG.info("Delete file statistics for table {}:", table.name());
        LOG.info("  Total delete files: {}", stats.totalFiles());
        LOG.info("  Total delete file size: {} MB", stats.totalSize() / (1024 * 1024));
        LOG.info("  Position delete files: {}", stats.positionDeleteFiles());
        LOG.info("  Equality delete files: {}", stats.equalityDeleteFiles());
        LOG.info("  Small files (< 1MB): {}", stats.smallFiles());
        LOG.info("  Large files (> 100MB): {}", stats.largeFiles());

        // 检查健康状况
        if (stats.smallFiles() > stats.totalFiles() * 0.5) {
            LOG.warn("High ratio of small delete files: {}/{}",
                stats.smallFiles(), stats.totalFiles());
        }

        // 检查数据文件的delete文件数量分布
        long maxDeletesPerDataFile = stats.deleteFilesByDataFile().values().stream()
            .mapToLong(Long::longValue)
            .max()
            .orElse(0);

        if (maxDeletesPerDataFile > 10) {
            LOG.warn("Some data files have too many delete files: {}", maxDeletesPerDataFile);
        }
    }
}
```

---

## 性能优化最佳实践

### 1. 表设计最佳实践

#### 分区策略优化

```java
public class PartitioningStrategy {

    // 推荐的分区策略
    public PartitionSpec designOptimalPartitioning(Schema schema, QueryPatterns queryPatterns) {
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);

        // 1. 时间分区 - 使用month/day粒度而不是timestamp
        if (queryPatterns.hasTimeRangeQueries()) {
            String timeColumn = queryPatterns.getPrimaryTimeColumn();

            if (queryPatterns.getTypicalTimeRange().getDays() <= 7) {
                builder.day(timeColumn); // 短时间范围查询使用day分区
            } else {
                builder.month(timeColumn); // 长时间范围查询使用month分区
            }
        }

        // 2. 高基数维度分区 - 使用bucket而不是identity
        for (String column : queryPatterns.getHighCardinalityFilters()) {
            int bucketCount = calculateOptimalBuckets(schema.findField(column), queryPatterns);
            builder.bucket(column, bucketCount);
        }

        // 3. 低基数维度分区 - 可以使用identity
        for (String column : queryPatterns.getLowCardinalityFilters()) {
            builder.identity(column);
        }

        return builder.build();
    }

    private int calculateOptimalBuckets(Types.NestedField field, QueryPatterns queryPatterns) {
        // 基于数据分布和查询模式计算最优bucket数量
        long estimatedCardinality = queryPatterns.getEstimatedCardinality(field.name());
        int typicalConcurrency = queryPatterns.getTypicalConcurrency();

        // 目标：每个bucket包含合理数量的数据，支持并行处理
        int optimalBuckets = (int) Math.min(
            Math.max(estimatedCardinality / 1000, typicalConcurrency),
            1024 // 最大bucket数量限制
        );

        // 使用2的幂次，便于扩展和负载均衡
        return Integer.highestOneBit(optimalBuckets);
    }
}
```

#### Schema演化策略

```java
public class SchemaEvolutionOptimizer {

    public void optimizeSchemaForQueries(Table table, List<QueryPattern> queries) {
        Schema currentSchema = table.schema();
        Schema.Builder newSchemaBuilder = new Schema.Builder();

        // 1. 分析列使用频率
        Map<String, Integer> columnUsage = analyzeColumnUsage(queries);

        // 2. 重新排列列顺序 - 常用列在前
        List<Types.NestedField> sortedFields = currentSchema.columns().stream()
            .sorted((f1, f2) -> Integer.compare(
                columnUsage.getOrDefault(f2.name(), 0),
                columnUsage.getOrDefault(f1.name(), 0)
            ))
            .collect(Collectors.toList());

        // 3. 添加计算列（如果有益）
        for (Types.NestedField field : sortedFields) {
            newSchemaBuilder.addField(field);

            // 为日期类型添加预计算的年、月、日列
            if (field.type().equals(Types.DateType.get()) &&
                isFrequentlyUsedForTimeRangeQueries(field.name(), queries)) {

                newSchemaBuilder.addField(Types.NestedField.optional(
                    newSchemaBuilder.getNextId(),
                    field.name() + "_year",
                    Types.IntegerType.get()
                ));

                newSchemaBuilder.addField(Types.NestedField.optional(
                    newSchemaBuilder.getNextId(),
                    field.name() + "_month",
                    Types.IntegerType.get()
                ));
            }
        }

        Schema newSchema = newSchemaBuilder.build();

        // 4. 应用schema演化
        if (!newSchema.sameSchema(currentSchema)) {
            table.updateSchema()
                .unionByNameWith(newSchema)
                .commit();
        }
    }
}
```

### 2. 写入优化策略

#### 批量写入优化

```java
public class OptimizedWriter {

    public void writeDataOptimally(Table table, Dataset<Row> data) {
        // 1. 设置写入属性
        Map<String, String> writeOptions = Maps.newHashMap();

        // 目标文件大小 - 基于集群配置调整
        writeOptions.put("write.target-file-size-bytes",
            calculateOptimalFileSize(table, data));

        // 写入分布模式
        writeOptions.put("write.distribution-mode", "hash");

        // 写入格式优化
        writeOptions.put("write.format.default", "parquet");
        writeOptions.put("write.parquet.compression-codec", "zstd");
        writeOptions.put("write.parquet.compression-level", "3");

        // 2. 数据预处理
        Dataset<Row> optimizedData = preprocessDataForWrite(data, table);

        // 3. 执行写入
        optimizedData.write()
            .format("iceberg")
            .options(writeOptions)
            .mode("append")
            .save(table.location());
    }

    private Dataset<Row> preprocessDataForWrite(Dataset<Row> data, Table table) {
        // 1. 重分区以优化文件大小和分布
        int optimalPartitions = calculateOptimalPartitions(data, table);
        data = data.repartition(optimalPartitions);

        // 2. 排序以提高压缩率和查询性能
        SortOrder sortOrder = table.sortOrder();
        if (!sortOrder.isUnsorted()) {
            data = applySortOrder(data, sortOrder);
        }

        // 3. 预聚合（如果适用）
        if (shouldPreaggregate(table)) {
            data = performPreAggregation(data);
        }

        return data;
    }

    private String calculateOptimalFileSize(Table table, Dataset<Row> data) {
        // 基于以下因素计算最优文件大小:
        // 1. 集群配置（CPU核数、内存）
        // 2. 数据特征（行数、列数、数据类型）
        // 3. 查询模式（扫描 vs 点查询）

        long estimatedRowSize = estimateAverageRowSize(data);
        long totalRows = data.count();
        int clusterCores = getClusterCoreCount();

        // 目标：每个文件包含适当数量的行，便于并行处理
        long targetRowsPerFile = Math.max(
            totalRows / (clusterCores * 4), // 每核心4个任务
            100_000 // 最小行数
        );

        long targetFileSize = targetRowsPerFile * estimatedRowSize;

        // 限制在合理范围内 (64MB - 1GB)
        return String.valueOf(Math.max(64 * 1024 * 1024,
                                     Math.min(targetFileSize, 1024 * 1024 * 1024)));
    }
}
```

#### 流式写入优化

```java
public class StreamingWriteOptimizer {

    public void optimizeStreamingWrite(Table table,
                                     StreamingQuery query,
                                     Duration triggerInterval) {

        // 1. 配置检查点和状态管理
        query.option("checkpointLocation", getCheckpointLocation(table))
             .option("maxFilesPerTrigger", "1000")
             .option("maxBytesPerTrigger", "1073741824"); // 1GB

        // 2. 启用文件合并
        schedulePeriodicCompaction(table, triggerInterval);

        // 3. 监控写入性能
        query.addStreamingQueryListener(new WritePerformanceMonitor(table));
    }

    private void schedulePeriodicCompaction(Table table, Duration triggerInterval) {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        long compactionIntervalMs = triggerInterval.toMillis() * 10; // 每10个触发间隔合并一次

        scheduler.scheduleAtFixedRate(() -> {
            try {
                runCompactionIfNeeded(table);
            } catch (Exception e) {
                LOG.error("Compaction failed for table " + table.name(), e);
            }
        }, compactionIntervalMs, compactionIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void runCompactionIfNeeded(Table table) {
        // 检查是否需要合并
        TableStatistics stats = analyzeTableStatistics(table);

        if (stats.smallFileRatio() > 0.3 ||
            stats.avgFileSize() < 32 * 1024 * 1024) { // 平均文件大小 < 32MB

            LOG.info("Running compaction for table {}", table.name());

            Actions.forTable(table)
                .rewriteDataFiles()
                .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, "134217728") // 128MB
                .execute();
        }
    }
}
```

### 3. 读取优化策略

#### 查询优化配置

```java
public class QueryOptimizer {

    public TableScan optimizeTableScan(Table table,
                                     Expression filter,
                                     List<String> selectedColumns,
                                     QueryContext context) {

        TableScan scan = table.newScan();

        // 1. 应用列裁剪
        if (selectedColumns != null && !selectedColumns.isEmpty()) {
            scan = scan.select(selectedColumns);
        }

        // 2. 应用过滤器
        if (filter != null && filter != Expressions.alwaysTrue()) {
            scan = scan.filter(filter);
        }

        // 3. 设置扫描选项
        scan = applyScanOptions(scan, context);

        // 4. 选择合适的快照
        scan = selectOptimalSnapshot(scan, context);

        return scan;
    }

    private TableScan applyScanOptions(TableScan scan, QueryContext context) {
        // 1. 设置分片大小
        long optimalSplitSize = calculateOptimalSplitSize(context);
        scan = scan.option("split-size", String.valueOf(optimalSplitSize));

        // 2. 设置读取线程数
        int readerThreads = calculateOptimalReaderThreads(context);
        scan = scan.option("reader-pool-size", String.valueOf(readerThreads));

        // 3. 启用向量化读取（如果支持）
        if (context.supportsVectorizedRead()) {
            scan = scan.option("read.vectorized.enabled", "true");
            scan = scan.option("read.vectorized.batch-size", "4096");
        }

        // 4. 配置缓存策略
        if (context.shouldCacheFiles()) {
            scan = scan.option("cache.enabled", "true");
            scan = scan.option("cache.size", context.getCacheSize());
        }

        return scan;
    }

    private long calculateOptimalSplitSize(QueryContext context) {
        // 基于以下因素计算最优分片大小:
        // 1. 可用内存
        // 2. CPU核数
        // 3. 网络带宽
        // 4. 查询类型（扫描 vs 聚合）

        long availableMemory = context.getAvailableMemoryPerExecutor();
        int cpuCores = context.getCpuCoresPerExecutor();

        // 保守估计：每个split使用128MB内存
        long maxSplitsPerExecutor = availableMemory / (128 * 1024 * 1024);

        // 确保每个核心至少有2个split以支持并行
        long targetSplits = cpuCores * 2;

        if (maxSplitsPerExecutor < targetSplits) {
            // 内存受限，减小split大小
            return 64 * 1024 * 1024; // 64MB
        } else {
            // 内存充足，使用标准split大小
            return 128 * 1024 * 1024; // 128MB
        }
    }
}
```

#### 缓存策略优化

```java
public class CachingStrategy {

    public void configureCaching(Table table, QueryWorkload workload) {
        Map<String, String> tableProperties = Maps.newHashMap();

        // 1. 分析查询模式
        CacheStrategy strategy = analyzeCacheStrategy(workload);

        switch (strategy) {
            case HOT_DATA_CACHING:
                configureHotDataCaching(tableProperties, workload);
                break;

            case METADATA_CACHING:
                configureMetadataCaching(tableProperties);
                break;

            case PARTITION_CACHING:
                configurePartitionCaching(tableProperties, workload);
                break;

            case NO_CACHING:
            default:
                // 禁用缓存以节省内存
                tableProperties.put("read.cache.enabled", "false");
                break;
        }

        // 应用缓存配置
        table.updateProperties()
            .putAll(tableProperties)
            .commit();
    }

    private void configureHotDataCaching(Map<String, String> properties, QueryWorkload workload) {
        // 缓存经常访问的热数据
        properties.put("read.cache.enabled", "true");
        properties.put("read.cache.type", "memory");

        // 基于工作负载计算缓存大小
        long hotDataSize = estimateHotDataSize(workload);
        properties.put("read.cache.size", String.valueOf(hotDataSize));

        // 设置缓存策略
        properties.put("read.cache.policy", "lru");
        properties.put("read.cache.ttl", "3600"); // 1小时TTL
    }

    private void configureMetadataCaching(Map<String, String> properties) {
        // 缓存元数据以加速查询规划
        properties.put("metadata.cache.enabled", "true");
        properties.put("metadata.cache.size", "134217728"); // 128MB
        properties.put("metadata.cache.ttl", "1800"); // 30分钟TTL
    }

    private void configurePartitionCaching(Map<String, String> properties, QueryWorkload workload) {
        // 缓存经常访问的分区
        List<String> hotPartitions = identifyHotPartitions(workload);

        properties.put("read.cache.enabled", "true");
        properties.put("read.cache.type", "partition");
        properties.put("read.cache.partitions", String.join(",", hotPartitions));
    }
}
```

### 4. 监控和调优

#### 性能监控系统

```java
public class IcebergPerformanceMonitor {

    private final MeterRegistry meterRegistry;
    private final Table table;

    public void startMonitoring() {
        // 1. 监控查询性能
        monitorQueryPerformance();

        // 2. 监控文件统计
        monitorFileStatistics();

        // 3. 监控元数据操作
        monitorMetadataOperations();

        // 4. 监控存储使用
        monitorStorageUsage();
    }

    private void monitorQueryPerformance() {
        Timer.Sample sample = Timer.start(meterRegistry);

        // 监控查询延迟
        meterRegistry.timer("iceberg.query.duration",
            "table", table.name(),
            "operation", "scan")
            .record(() -> {
                // 查询执行逻辑
            });

        // 监控文件扫描数量
        meterRegistry.counter("iceberg.files.scanned",
            "table", table.name())
            .increment();

        // 监控数据量
        meterRegistry.counter("iceberg.bytes.scanned",
            "table", table.name())
            .increment();
    }

    private void monitorFileStatistics() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        scheduler.scheduleAtFixedRate(() -> {
            try {
                FileStatistics stats = analyzeFileStatistics();

                meterRegistry.gauge("iceberg.files.total",
                    Tags.of("table", table.name()), stats.totalFiles());

                meterRegistry.gauge("iceberg.files.small.ratio",
                    Tags.of("table", table.name()), stats.smallFileRatio());

                meterRegistry.gauge("iceberg.files.avg.size",
                    Tags.of("table", table.name()), stats.averageFileSize());

            } catch (Exception e) {
                LOG.error("Failed to collect file statistics", e);
            }
        }, 0, 5, TimeUnit.MINUTES);
    }

    public void generatePerformanceReport() {
        PerformanceReport report = new PerformanceReport.Builder()
            .withQueryMetrics(collectQueryMetrics())
            .withFileMetrics(collectFileMetrics())
            .withStorageMetrics(collectStorageMetrics())
            .withRecommendations(generateRecommendations())
            .build();

        // 输出报告
        LOG.info("Performance Report for table {}:\n{}", table.name(), report.toString());
    }

    private List<Recommendation> generateRecommendations() {
        List<Recommendation> recommendations = Lists.newArrayList();

        FileStatistics fileStats = analyzeFileStatistics();

        // 文件大小建议
        if (fileStats.smallFileRatio() > 0.3) {
            recommendations.add(new Recommendation(
                "HIGH",
                "Small Files",
                "Consider running compaction. " +
                String.format("%.1f%% of files are smaller than 32MB",
                    fileStats.smallFileRatio() * 100)
            ));
        }

        // 分区建议
        PartitionStatistics partStats = analyzePartitionStatistics();
        if (partStats.averageFilesPerPartition() > 100) {
            recommendations.add(new Recommendation(
                "MEDIUM",
                "Partition Strategy",
                "Consider using bucket partitioning to reduce files per partition"
            ));
        }

        // 缓存建议
        QueryStatistics queryStats = analyzeQueryStatistics();
        if (queryStats.cacheHitRatio() < 0.5 && queryStats.repeatQueryRatio() > 0.3) {
            recommendations.add(new Recommendation(
                "LOW",
                "Caching",
                "Enable caching for frequently accessed data"
            ));
        }

        return recommendations;
    }
}
```

---

## 实践案例与解决方案

### 案例1: 大规模日志数据优化

#### 问题描述
一个日志分析平台每天产生100GB+的日志数据，存在以下问题：
- 小文件过多（平均文件大小5MB）
- 查询延迟高（分钟级别）
- 存储成本上升

#### 解决方案

```java
public class LogDataOptimization {

    public void optimizeLogTable(Table logTable) {
        // 1. 优化分区策略
        optimizePartitioning(logTable);

        // 2. 实施自动压缩
        enableAutoCompaction(logTable);

        // 3. 优化查询模式
        optimizeQueryPatterns(logTable);

        // 4. 清理历史数据
        implementDataRetention(logTable);
    }

    private void optimizePartitioning(Table table) {
        // 基于时间的分层分区：年/月/日/小时
        PartitionSpec newSpec = PartitionSpec.builderFor(table.schema())
            .year("timestamp")
            .month("timestamp")
            .day("timestamp")
            .hour("timestamp")
            .bucket("user_id", 16) // 基于用户ID的bucket分区
            .build();

        // 应用新的分区策略
        table.updateSpec()
            .addField(Expressions.year("timestamp"))
            .addField(Expressions.month("timestamp"))
            .addField(Expressions.day("timestamp"))
            .addField(Expressions.hour("timestamp"))
            .addField(Expressions.bucket("user_id", 16))
            .commit();
    }

    private void enableAutoCompaction(Table table) {
        // 配置自动压缩
        table.updateProperties()
            .put("write.target-file-size-bytes", "268435456") // 256MB
            .put("write.format.default", "parquet")
            .put("write.parquet.compression-codec", "zstd")
            .put("write.parquet.compression-level", "3")
            .commit();

        // 启动后台压缩任务
        ScheduledExecutorService compactionScheduler = Executors.newScheduledThreadPool(1);
        compactionScheduler.scheduleAtFixedRate(() -> {
            runHourlyCompaction(table);
        }, 1, 1, TimeUnit.HOURS);
    }

    private void runHourlyCompaction(Table table) {
        // 只压缩最近的分区以减少影响
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime oneHourAgo = now.minusHours(1);

        Expression partitionFilter = Expressions.and(
            Expressions.greaterThanOrEqual("timestamp_hour", oneHourAgo.getHour()),
            Expressions.lessThan("timestamp_hour", now.getHour())
        );

        Actions.forTable(table)
            .rewriteDataFiles()
            .filter(partitionFilter)
            .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, "268435456")
            .execute();
    }
}
```

**效果**:
- 文件数量减少90%
- 查询性能提升5-10倍
- 存储成本降低30%

### 案例2: 实时数据仓库优化

#### 问题描述
一个实时数据仓库系统遇到以下挑战：
- 频繁的delete操作产生大量delete文件
- 查询性能受delete文件影响严重
- 存储空间浪费

#### 解决方案

```java
public class RealTimeWarehouseOptimization {

    public void optimizeRealTimeTable(Table table) {
        // 1. 优化delete策略
        optimizeDeleteStrategy(table);

        // 2. 实施delete文件合并
        implementDeleteFileCompaction(table);

        // 3. 优化upsert模式
        optimizeUpsertPattern(table);

        // 4. 监控和告警
        setupMonitoring(table);
    }

    private void optimizeDeleteStrategy(Table table) {
        // 配置delete文件合并参数
        table.updateProperties()
            .put("write.delete.target-file-size-bytes", "67108864") // 64MB
            .put("write.delete.min-file-size-bytes", "16777216")   // 16MB
            .put("write.delete.max-file-size-bytes", "134217728")  // 128MB
            .put("write.delete.min-input-files", "3")
            .commit();
    }

    private void implementDeleteFileCompaction(Table table) {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        // 每30分钟执行一次delete文件合并
        scheduler.scheduleAtFixedRate(() -> {
            try {
                compactDeleteFiles(table);
            } catch (Exception e) {
                LOG.error("Delete file compaction failed", e);
            }
        }, 30, 30, TimeUnit.MINUTES);
    }

    private void compactDeleteFiles(Table table) {
        // 获取delete文件统计
        DeleteFileStatistics stats = analyzeDeleteFiles(table);

        if (shouldCompactDeleteFiles(stats)) {
            LOG.info("Starting delete file compaction for table {}", table.name());

            Actions.forTable(table)
                .rewritePositionDeleteFiles()
                .option(RewritePositionDeleteFiles.TARGET_FILE_SIZE_BYTES, "67108864")
                .execute();

            LOG.info("Delete file compaction completed");
        }
    }

    private boolean shouldCompactDeleteFiles(DeleteFileStatistics stats) {
        // 基于以下条件决定是否合并:
        // 1. 小delete文件比例过高
        // 2. delete文件总数过多
        // 3. 平均delete文件大小过小

        return stats.smallFileRatio() > 0.4 ||
               stats.totalFiles() > 1000 ||
               stats.averageFileSize() < 16 * 1024 * 1024;
    }

    private void optimizeUpsertPattern(Table table) {
        // 使用merge语法而不是delete+insert
        // 配置upsert优化参数
        table.updateProperties()
            .put("write.upsert.target-file-size-bytes", "268435456")
            .put("write.upsert.partial-progress.enabled", "true")
            .put("write.upsert.partial-progress.max-commits", "5")
            .commit();
    }
}
```

**效果**:
- Delete文件数量减少80%
- 查询性能提升3-5倍
- 存储利用率提升40%

### 案例3: 历史数据归档优化

#### 问题描述
一个金融系统需要处理10年的历史数据：
- 数据量庞大（TB级别）
- 查询模式复杂（时间范围查询为主）
- 需要支持法规审计要求

#### 解决方案

```java
public class HistoricalDataArchiveOptimization {

    public void optimizeArchiveTable(Table table) {
        // 1. 实施数据分层存储
        implementTieredStorage(table);

        // 2. 优化历史数据压缩
        optimizeHistoricalCompression(table);

        // 3. 实施智能分区修剪
        implementIntelligentPartitionPruning(table);

        // 4. 配置冷数据处理
        configureColdDataHandling(table);
    }

    private void implementTieredStorage(Table table) {
        // 基于数据年龄配置存储层级
        LocalDate cutoffDate = LocalDate.now().minusYears(2);

        Expression hotDataFilter = Expressions.greaterThanOrEqual("date", cutoffDate.toString());
        Expression coldDataFilter = Expressions.lessThan("date", cutoffDate.toString());

        // 热数据：使用高性能存储
        table.updateProperties()
            .put("write.data.path", "s3://hot-storage/data/")
            .put("write.metadata.path", "s3://hot-storage/metadata/")
            .commit();

        // 冷数据：迁移到低成本存储
        migrateColdData(table, coldDataFilter);
    }

    private void migrateColdData(Table table, Expression coldDataFilter) {
        // 创建冷数据表
        Table coldTable = createColdDataTable(table);

        // 迁移数据
        TableScan coldDataScan = table.newScan().filter(coldDataFilter);

        try (CloseableIterable<FileScanTask> tasks = coldDataScan.planFiles()) {
            for (FileScanTask task : tasks) {
                migrateSingleFile(task, coldTable);
            }
        }

        // 从原表删除已迁移的数据
        table.newDelete()
            .deleteFromRowFilter(coldDataFilter)
            .commit();
    }

    private void optimizeHistoricalCompression(Table table) {
        // 对历史数据使用更高的压缩比
        LocalDate oneYearAgo = LocalDate.now().minusYears(1);
        Expression historicalFilter = Expressions.lessThan("date", oneYearAgo.toString());

        Actions.forTable(table)
            .rewriteDataFiles()
            .filter(historicalFilter)
            .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, "536870912") // 512MB
            .option("compression-codec", "zstd")
            .option("compression-level", "9") // 最高压缩比
            .execute();
    }

    private void implementIntelligentPartitionPruning(Table table) {
        // 基于查询模式优化分区策略
        PartitionSpec optimizedSpec = PartitionSpec.builderFor(table.schema())
            .year("date")      // 粗粒度分区用于长期查询
            .month("date")     // 中粒度分区用于月度报告
            .bucket("account_id", 32) // 基于账户的分布
            .build();

        // 应用优化后的分区策略
        table.updateSpec()
            .addField(Expressions.year("date"))
            .addField(Expressions.month("date"))
            .addField(Expressions.bucket("account_id", 32))
            .commit();
    }

    private void configureColdDataHandling(Table table) {
        // 配置冷数据的访问策略
        table.updateProperties()
            .put("read.cache.enabled", "false") // 禁用缓存节省内存
            .put("read.vectorized.enabled", "true") // 启用向量化读取
            .put("read.batch-size", "8192") // 增大批处理大小
            .put("read.target-scan-size-bytes", "1073741824") // 1GB扫描块
            .commit();
    }
}
```

**效果**:
- 存储成本降低60%
- 查询性能保持稳定
- 满足合规要求

---

## 总结

本指南深入分析了Apache Iceberg的数据读取机制，并提供了全面的性能优化策略。关键要点包括：

### 核心技术亮点

1. **多层次过滤架构**: 从分区到文件再到行级的渐进式过滤优化
2. **智能索引机制**: 基于统计信息的自动查询优化
3. **灵活的文件管理**: 支持多种合并策略和清理机制
4. **ACID事务支持**: 完整的事务性保证和并发控制

### 性能优化核心策略

1. **分区设计优化**: 基于查询模式设计合理的分区策略
2. **文件大小控制**: 保持合适的文件大小以平衡查询性能和存储效率
3. **压缩和编码**: 选择合适的压缩算法和编码方式
4. **缓存策略**: 根据访问模式配置合理的缓存策略

### 运维最佳实践

1. **监控和告警**: 建立完整的监控体系
2. **自动化运维**: 实施自动化的优化和维护任务
3. **成本优化**: 通过数据分层和生命周期管理降低成本
4. **性能调优**: 持续监控和调优以保持最佳性能

Apache Iceberg通过其先进的架构设计为现代数据湖提供了强大的数据管理能力。正确应用本指南中的优化策略，可以显著提升查询性能、降低存储成本，并简化数据管理复杂度。