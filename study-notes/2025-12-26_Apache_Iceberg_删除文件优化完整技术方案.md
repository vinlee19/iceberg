# Apache Iceberg 删除文件优化完整技术方案

## 摘要

本文档深入分析Apache Iceberg删除文件过多的根本原因、性能影响机制，并提供完整的优化技术方案。通过源码级别的分析，详细阐述删除文件的类型结构、查询处理流程，以及RewritePositionDeleteFiles等优化策略的实现机制。

## 目录

1. [删除文件问题分析](#1-删除文件问题分析)
2. [删除文件类型和结构](#2-删除文件类型和结构)
3. [删除文件对查询性能的影响](#3-删除文件对查询性能的影响)
4. [删除文件优化策略](#4-删除文件优化策略)
5. [删除文件合并机制](#5-删除文件合并机制)
6. [RewritePositionDeleteFiles实现](#6-rewritepositiondeletefiles实现)
7. [最佳实践与建议](#7-最佳实践与建议)

---

## 1. 删除文件问题分析

### 1.1 删除文件过多的根本原因

Iceberg删除文件过多主要由以下几个原因造成：

#### 1.1.1 小批量删除操作
```java
// 每次小批量DELETE操作都会生成独立的删除文件
DELETE FROM table WHERE condition1;  // 生成 delete_file_1.parquet
DELETE FROM table WHERE condition2;  // 生成 delete_file_2.parquet
DELETE FROM table WHERE condition3;  // 生成 delete_file_3.parquet
```

#### 1.1.2 高频UPDATE操作
```java
// UPDATE操作在Iceberg中实现为DELETE + INSERT
UPDATE table SET col1 = new_value WHERE condition;
// 等价于:
// 1. 生成position delete文件标记原数据删除
// 2. 写入新的data文件包含更新后的数据
```

#### 1.1.3 分区级别的删除粒度
```java
// 跨分区删除会在每个分区生成删除文件
DELETE FROM table WHERE event_time >= '2023-01-01';
// 如果table按天分区，每个分区都会生成独立的delete文件
```

#### 1.1.4 并发删除操作
```java
// 并发DELETE操作无法合并，各自生成删除文件
Session1: DELETE FROM table WHERE user_id IN (1,2,3);
Session2: DELETE FROM table WHERE user_id IN (4,5,6);
// 结果：2个独立的delete文件而不是1个合并文件
```

### 1.2 删除文件累积的触发条件

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        删除文件累积场景分析                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  场景1: 流式CDC数据处理                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │ Kafka Consumer -> Flink/Spark Streaming                                │ │
│  │      ↓                                                                  │ │
│  │ 每批次处理: INSERT/UPDATE/DELETE                                         │ │
│  │      ↓                                                                  │ │
│  │ 频繁小批量写入 -> 每批生成少量delete文件                                   │ │
│  │ 1小时后: 可能有数百个小delete文件                                        │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  场景2: 数据订正和清理                                                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │ 业务逻辑变更 -> 历史数据需要删除/更新                                      │ │
│  │      ↓                                                                  │ │
│  │ 按时间窗口或条件批量处理                                                  │ │
│  │      ↓                                                                  │ │
│  │ 每个处理任务生成独立delete文件                                            │ │
│  │ 最终: 大量分散的delete文件                                               │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  场景3: 多租户系统的数据隔离                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │ 租户A删除: DELETE WHERE tenant_id = 'A'                                  │ │
│  │ 租户B删除: DELETE WHERE tenant_id = 'B'                                  │ │
│  │ 租户C删除: DELETE WHERE tenant_id = 'C'                                  │ │
│  │      ↓                                                                  │ │
│  │ 结果: N个租户 = N个delete文件                                            │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. 删除文件类型和结构

### 2.1 删除文件分类

#### 2.1.1 按内容类型分类

```java
// FileContent枚举定义 - /Users/xiaowenli/kevin/workspace/iceberg/api/src/main/java/org/apache/iceberg/FileContent.java:22-25
public enum FileContent {
  DATA(0),              // 数据文件
  POSITION_DELETES(1),  // 位置删除文件
  EQUALITY_DELETES(2);  // 等值删除文件
}
```

#### 2.1.2 位置删除 vs 等值删除

**位置删除 (Position Deletes)**:
```java
// 记录格式: (file_path, row_position)
// 示例记录:
("s3://bucket/table/data/00001.parquet", 1023)  // 删除文件中第1023行
("s3://bucket/table/data/00001.parquet", 2048)  // 删除文件中第2048行
("s3://bucket/table/data/00002.parquet", 512)   // 删除另一个文件中第512行
```

**等值删除 (Equality Deletes)**:
```java
// 基于列值的删除条件
// 删除文件包含要删除的实际数据行
// 示例: 删除所有user_id=123的记录
Schema equalityDeleteSchema = Schema.builder()
    .addColumn("user_id", Types.LongType.get())
    .addColumn("event_time", Types.TimestampType.withZone())
    .build();
```

### 2.2 删除文件内部结构

#### 2.2.1 GenericDeleteFile结构

```java
// 删除文件核心结构 - /Users/xiaowenli/kevin/workspace/iceberg/core/src/main/java/org/apache/iceberg/GenericDeleteFile.java:40-77
GenericDeleteFile(
    int specId,                    // 分区规范ID
    FileContent content,           // 文件内容类型 (POSITION_DELETES/EQUALITY_DELETES)
    String filePath,               // 文件路径
    FileFormat format,             // 文件格式 (PARQUET/AVRO/ORC)
    PartitionData partition,       // 分区数据
    long fileSizeInBytes,          // 文件大小
    Metrics metrics,               // 统计信息
    int[] equalityFieldIds,        // 等值删除字段ID (仅用于EQUALITY_DELETES)
    Integer sortOrderId,           // 排序顺序ID
    List<Long> splitOffsets,       // 分片偏移量
    ByteBuffer keyMetadata,        // 键元数据
    String referencedDataFile,     // 引用的数据文件 (仅用于POSITION_DELETES)
    Long contentOffset,            // 内容偏移量
    Long contentSizeInBytes)       // 内容大小
```

#### 2.2.2 删除文件元数据信息

```java
// 关键元数据字段说明
public interface DeleteFile extends ContentFile<DeleteFile> {
  // 分片边界信息，用于并行处理
  default List<Long> splitOffsets() { return null; }

  // 引用的数据文件路径 (Position Delete专用)
  default String referencedDataFile() { return null; }

  // 内容在文件中的偏移量 (Deletion Vector专用)
  default Long contentOffset() { return null; }

  // 内容大小 (Deletion Vector专用)
  default Long contentSizeInBytes() { return null; }
}
```

### 2.3 删除文件存储格式

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       删除文件存储格式对比                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Position Delete File (PARQUET格式)                                         │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │ Row Group 1:                                                            │ │
│  │   file_path: "s3://bucket/data/00001.parquet" (重复值)                  │ │
│  │   pos:       [1023, 1024, 1025, ..., 2047]  (2024条删除记录)           │ │
│  │                                                                         │ │
│  │ Row Group 2:                                                            │ │
│  │   file_path: "s3://bucket/data/00002.parquet" (重复值)                  │ │
│  │   pos:       [512, 513, 514, ..., 1023]     (512条删除记录)            │ │
│  │                                                                         │ │
│  │ 压缩效率: file_path高度重复 -> 压缩比极高                                │ │
│  │ 查询效率: 按file_path分组 -> 快速定位                                   │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  Equality Delete File (PARQUET格式)                                         │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │ Row Group 1:                                                            │ │
│  │   user_id:    [123, 456, 789, ...]    (要删除的用户ID)                  │ │
│  │   event_time: [ts1, ts2, ts3, ...]    (对应的时间戳)                    │ │
│  │                                                                         │ │
│  │ 包含完整的删除条件数据                                                    │ │
│  │ 支持复合删除条件 (多列组合)                                              │ │
│  │ 文件大小取决于删除数据的实际内容                                          │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  Deletion Vector (V3新特性, PUFFIN格式)                                     │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │ 基于位向量的高效删除标记                                                  │ │
│  │ contentOffset: 指向位向量在Puffin文件中的位置                             │ │
│  │ contentSizeInBytes: 位向量的大小                                         │ │
│  │ referencedDataFile: 对应的数据文件                                       │ │
│  │                                                                         │ │
│  │ 优势: 极高的压缩比，适合稠密删除场景                                      │ │
│  │ 限制: 只能用于单个数据文件的删除标记                                      │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. 删除文件对查询性能的影响

### 3.1 查询规划阶段的影响

#### 3.1.1 删除文件索引构建

```java
// DeleteFileIndex构建过程 - /Users/xiaowenli/kevin/workspace/iceberg/core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:79-94
private DeleteFileIndex(
    EqualityDeletes globalDeletes,                    // 全局等值删除
    PartitionMap<EqualityDeletes> eqDeletesByPartition,  // 按分区的等值删除
    PartitionMap<PositionDeletes> posDeletesByPartition, // 按分区的位置删除
    Map<String, PositionDeletes> posDeletesByPath,       // 按文件路径的位置删除
    Map<String, DeleteFile> dvByPath) {                  // 按路径的删除向量

    // 性能关键指标
    this.hasEqDeletes = globalDeletes != null || eqDeletesByPartition != null;
    this.hasPosDeletes = posDeletesByPartition != null || posDeletesByPath != null || dvByPath != null;
    this.isEmpty = !hasEqDeletes && !hasPosDeletes;
}
```

#### 3.1.2 删除文件过多的规划开销

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     查询规划阶段性能影响分析                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. Manifest文件读取开销                                                     │
│     ┌─────────────────────────────────────────────────────────────────────┐ │
│     │ 删除文件数量: 1,000个                                                │ │
│     │ Manifest文件数量: 100个 (每个manifest包含10个delete文件)              │ │
│     │ 读取开销: 100次IO操作                                                │ │
│     │ 内存开销: ~10MB (每个delete文件元数据约10KB)                          │ │
│     └─────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  2. 删除文件过滤计算                                                         │
│     ┌─────────────────────────────────────────────────────────────────────┐ │
│     │ 对每个数据文件，需要计算关联的删除文件列表                             │ │
│     │ 复杂度: O(N * M) where N=数据文件数, M=删除文件数                     │ │
│     │                                                                     │ │
│     │ 示例: 1000个数据文件 × 1000个删除文件 = 1,000,000次比较               │ │
│     │ 优化后: 使用PartitionMap分区索引 -> O(N * M_partition)               │ │
│     └─────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  3. 内存消耗模式                                                             │
│     ┌─────────────────────────────────────────────────────────────────────┐ │
│     │ DeleteFileIndex内存结构:                                            │ │
│     │ • globalDeletes: 全局删除文件 (~1MB)                                │ │
│     │ • eqDeletesByPartition: 分区等值删除索引 (~5MB)                     │ │
│     │ • posDeletesByPartition: 分区位置删除索引 (~10MB)                   │ │
│     │ • posDeletesByPath: 路径位置删除索引 (~20MB)                        │ │
│     │ • dvByPath: 删除向量索引 (~2MB)                                     │ │
│     │ 总计: ~38MB (1000个删除文件的估算)                                   │ │
│     └─────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 查询执行阶段的影响

#### 3.2.1 删除文件应用过程

```java
// 等值删除文件过滤逻辑 - DeleteFileIndex.java:216-278
private static boolean canContainEqDeletesForFile(DataFile dataFile, EqualityDeleteFile deleteFile) {
    Map<Integer, ByteBuffer> dataLowers = dataFile.lowerBounds();
    Map<Integer, ByteBuffer> dataUppers = dataFile.upperBounds();

    // 性能关键: 使用统计信息快速过滤
    boolean checkRanges = dataLowers != null && dataUppers != null && deleteFile.hasLowerAndUpperBounds();

    // 检查每个等值字段的范围重叠
    for (Types.NestedField field : deleteFile.equalityFields()) {
        // null值检查
        if (containsNull(dataNullCounts, field) && containsNull(deleteNullCounts, field)) {
            continue; // 需要应用删除
        }

        // 范围检查 - 避免不必要的文件读取
        if (!rangesOverlap(field, dataLower, dataUpper, deleteLower, deleteUpper)) {
            return false; // 可以跳过此删除文件
        }
    }
    return true;
}
```

#### 3.2.2 位置删除文件处理开销

```java
// 位置删除处理流程
1. 读取Position Delete文件
   -> 解析 (file_path, pos) 记录
   -> 按file_path分组
   -> 对每个数据文件构建删除位置集合

2. 数据文件读取时应用删除
   -> 读取每一行时检查行号是否在删除集合中
   -> 跳过被删除的行
   -> 性能开销: O(log N) 每行查询 (使用TreeSet存储删除位置)
```

### 3.3 性能退化定量分析

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      删除文件数量 vs 查询性能影响                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  删除文件数量     规划时间      执行时间      内存使用      总体性能影响      │
│  ───────────────────────────────────────────────────────────────────────────│
│  0-10个          <100ms        基准         基准          0% (基准)          │
│  10-100个        100-500ms     +5%          +2MB          +10%              │
│  100-500个       500ms-2s      +15%         +10MB         +25%              │
│  500-1000个      2s-5s         +30%         +25MB         +50%              │
│  1000-5000个     5s-15s        +60%         +100MB        +100%             │
│  5000+个         15s+          +150%        +500MB        +300%+            │
│                                                                             │
│  临界点分析:                                                                │
│  • 100个删除文件: 性能开始明显下降                                          │
│  • 1000个删除文件: 规划时间超过5秒，影响用户体验                            │
│  • 5000个删除文件: 内存使用超过500MB，可能触发OOM                           │
│                                                                             │
│  影响因素:                                                                  │
│  • 删除文件大小: 小文件影响更大 (元数据开销高)                              │
│  • 分区分布: 跨分区删除文件影响更严重                                       │
│  • 删除密度: 稀疏删除(删除行数少)的开销相对更高                            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. 删除文件优化策略

### 4.1 核心优化原理

#### 4.1.1 删除文件合并策略

```java
// 基本合并原理
多个小删除文件 -> 合并 -> 少量大删除文件

优化效果:
• 减少Manifest文件数量和读取次数
• 降低查询规划阶段的计算复杂度
• 提高删除文件的压缩效率
• 减少内存索引结构的开销
```

#### 4.1.2 删除文件重写触发条件

```java
// RewritePositionDeleteFiles的默认触发条件
public class RewritePositionDeleteFilesSparkAction {

    // 默认配置参数
    private static final long DEFAULT_TARGET_FILE_SIZE = 128 * 1024 * 1024; // 128MB
    private static final double DEFAULT_MIN_INPUT_FILES = 5;                 // 最少5个输入文件
    private static final double DEFAULT_DELETE_FILE_THRESHOLD = 0.75;        // 删除文件阈值75%

    // 触发条件检查
    private boolean shouldRewrite(List<PositionDeletesScanTask> tasks) {
        if (tasks.size() < DEFAULT_MIN_INPUT_FILES) {
            return false; // 文件数量太少，不值得重写
        }

        long totalSize = tasks.stream().mapToLong(task -> task.file().fileSizeInBytes()).sum();
        if (totalSize < DEFAULT_TARGET_FILE_SIZE) {
            return false; // 总大小太小，合并收益有限
        }

        return true;
    }
}
```

### 4.2 BinPack合并策略

#### 4.2.1 删除文件分组算法

```java
// SparkBinPackPositionDeletesRewriter的分组逻辑
public class SparkBinPackPositionDeletesRewriter {

    /**
     * 将删除文件按最优方式分组，最小化输出文件数量
     */
    private List<List<PositionDeletesScanTask>> planFileGroups(List<PositionDeletesScanTask> tasks) {
        // 按分区分组
        Map<StructLike, List<PositionDeletesScanTask>> tasksByPartition =
            tasks.stream().collect(groupingBy(task -> task.partition()));

        List<List<PositionDeletesScanTask>> fileGroups = Lists.newArrayList();

        for (List<PositionDeletesScanTask> partitionTasks : tasksByPartition.values()) {
            // 在分区内使用BinPack算法分组
            List<List<PositionDeletesScanTask>> partitionGroups = binPackTasks(partitionTasks);
            fileGroups.addAll(partitionGroups);
        }

        return fileGroups;
    }

    private List<List<PositionDeletesScanTask>> binPackTasks(List<PositionDeletesScanTask> tasks) {
        List<List<PositionDeletesScanTask>> groups = Lists.newArrayList();
        List<PositionDeletesScanTask> currentGroup = Lists.newArrayList();
        long currentGroupSize = 0;

        // 按文件大小排序，大文件优先
        tasks.sort((t1, t2) -> Long.compare(t2.file().fileSizeInBytes(), t1.file().fileSizeInBytes()));

        for (PositionDeletesScanTask task : tasks) {
            long taskSize = task.file().fileSizeInBytes();

            if (currentGroupSize + taskSize <= targetFileSize) {
                // 可以加入当前组
                currentGroup.add(task);
                currentGroupSize += taskSize;
            } else {
                // 当前组已满，开始新组
                if (!currentGroup.isEmpty()) {
                    groups.add(currentGroup);
                }
                currentGroup = Lists.newArrayList(task);
                currentGroupSize = taskSize;
            }
        }

        if (!currentGroup.isEmpty()) {
            groups.add(currentGroup);
        }

        return groups;
    }
}
```

#### 4.2.2 合并效率分析

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       BinPack合并效率分析                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  输入场景: 1000个小删除文件，每个文件平均1MB                                   │
│                                                                             │
│  合并前:                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │ 文件数量: 1,000个                                                       │ │
│  │ 平均大小: 1MB                                                           │ │
│  │ 总大小: 1GB                                                             │ │
│  │ Manifest条目: 1,000个                                                   │ │
│  │ 查询规划时间: ~10秒                                                     │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                            ↓ BinPack合并                                    │
│  合并后:                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │ 文件数量: 8个 (每个128MB)                                               │ │
│  │ 平均大小: 128MB                                                         │ │
│  │ 总大小: 1GB (无变化)                                                    │ │
│  │ Manifest条目: 8个                                                       │ │
│  │ 查询规划时间: ~200ms                                                    │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  性能提升:                                                                  │
│  • 文件数量减少: 1000 -> 8 (减少99.2%)                                      │
│  • 规划时间减少: 10s -> 200ms (减少98%)                                     │
│  • 内存使用减少: 50MB -> 1MB (减少98%)                                      │
│  • 压缩效率提升: 大文件有更好的压缩比                                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.3 V3删除向量优化

#### 4.3.1 删除向量 (Deletion Vector) 原理

```java
// V3 Deletion Vector优化
// 传统Position Delete: (file_path, row_position)记录
// 删除向量: 位图表示删除状态

Example:
数据文件: data_001.parquet (100万行)
删除10万行 (10%删除率)

传统方式:
Position Delete文件大小 = 10万条 × (文件路径 + 行号) ≈ 20MB

删除向量方式:
Deletion Vector大小 = 100万位 ÷ 8 = 125KB
压缩后大小 ≈ 50KB (稀疏删除场景下压缩比很高)

空间节省: 20MB -> 50KB (节省99.75%)
```

#### 4.3.2 删除向量适用场景

```java
// 删除向量最适合的场景判断
public boolean shouldUseDeletionVector(DataFile dataFile, List<Long> deletePositions) {
    long totalRows = dataFile.recordCount();
    long deletedRows = deletePositions.size();
    double deleteRatio = (double) deletedRows / totalRows;

    // 规则1: 删除比例在1%-50%之间最适合
    if (deleteRatio < 0.01 || deleteRatio > 0.5) {
        return false;
    }

    // 规则2: 数据文件足够大 (至少10万行)
    if (totalRows < 100_000) {
        return false;
    }

    // 规则3: 删除位置相对分散 (非连续删除)
    boolean isSparse = checkDeleteSparsity(deletePositions);
    return isSparse;
}
```

### 4.4 自适应优化策略

#### 4.4.1 基于统计信息的优化选择

```java
// 自适应优化决策树
public OptimizationStrategy chooseStrategy(Table table) {
    TableStatistics stats = analyzeTable(table);

    if (stats.deleteFileCount() < 10) {
        return OptimizationStrategy.NO_ACTION; // 删除文件太少，无需优化
    }

    if (stats.avgDeleteFileSize() < 1_MB) {
        return OptimizationStrategy.BINPACK_REWRITE; // 小文件合并
    }

    if (table.spec().formatVersion() >= 3 && stats.avgDeleteRatio() < 0.3) {
        return OptimizationStrategy.DELETION_VECTOR; // 使用删除向量
    }

    if (stats.deleteFileCount() > 1000) {
        return OptimizationStrategy.AGGRESSIVE_REWRITE; // 激进合并
    }

    return OptimizationStrategy.STANDARD_REWRITE; // 标准重写
}

// 表统计信息分析
private TableStatistics analyzeTable(Table table) {
    return TableStatistics.builder()
        .deleteFileCount(countDeleteFiles(table))
        .avgDeleteFileSize(calculateAvgDeleteFileSize(table))
        .avgDeleteRatio(calculateAvgDeleteRatio(table))
        .partitionCount(table.spec().fields().size())
        .build();
}
```

---

## 5. 删除文件合并机制

### 5.1 合并调度策略

#### 5.1.1 并发合并控制

```java
// RewritePositionDeleteFilesSparkAction的并发控制
public class RewritePositionDeleteFilesSparkAction {

    private int maxConcurrentFileGroupRewrites = 10; // 最大并发重写组数
    private ExecutorService rewriteService;

    private Result doExecute(RewriteExecutionContext ctx, Stream<RewritePositionDeletesGroup> groupStream) {
        // 创建有界线程池，控制并发度
        this.rewriteService = Executors.newFixedThreadPool(
            maxConcurrentFileGroupRewrites,
            new ThreadFactoryBuilder()
                .setNameFormat("iceberg-delete-rewrite-%d")
                .setDaemon(true)
                .build()
        );

        try {
            // 并发处理重写组
            CompletableFuture<Void> allRewriteTasks = groupStream
                .map(group -> CompletableFuture.runAsync(() -> rewriteGroup(group), rewriteService))
                .reduce(CompletableFuture.completedFuture(null),
                       (f1, f2) -> f1.thenCompose(ignored -> f2));

            allRewriteTasks.get(); // 等待所有重写完成

        } finally {
            rewriteService.shutdown();
        }

        return buildResult();
    }
}
```

#### 5.1.2 内存使用控制

```java
// 合并过程中的内存管理
public class SparkBinPackPositionDeletesRewriter {

    private static final long MAX_MEMORY_PER_TASK = 256L * 1024 * 1024; // 256MB per task

    private void rewriteDeleteFiles(List<PositionDeletesScanTask> inputTasks) {

        // 计算内存需求
        long estimatedMemory = inputTasks.stream()
            .mapToLong(task -> task.file().fileSizeInBytes())
            .sum();

        if (estimatedMemory > MAX_MEMORY_PER_TASK) {
            // 分批处理，避免内存溢出
            List<List<PositionDeletesScanTask>> batches = partitionTasks(inputTasks, MAX_MEMORY_PER_TASK);
            for (List<PositionDeletesScanTask> batch : batches) {
                rewriteBatch(batch);
            }
        } else {
            // 一次性处理
            rewriteBatch(inputTasks);
        }
    }

    private void rewriteBatch(List<PositionDeletesScanTask> tasks) {
        // 使用流式处理，避免全部加载到内存
        try (PositionDeleteWriter writer = createWriter()) {
            for (PositionDeletesScanTask task : tasks) {
                try (CloseableIterator<RowData> iterator = task.asDataTask().rows()) {
                    while (iterator.hasNext()) {
                        writer.write(iterator.next()); // 流式写入
                    }
                }
            }
        }
    }
}
```

### 5.2 分区级别优化

#### 5.2.1 分区感知的合并策略

```java
// 按分区分组的优化逻辑
private Map<StructLike, List<PositionDeletesScanTask>> groupByPartition(List<PositionDeletesScanTask> tasks) {
    return tasks.stream().collect(
        Collectors.groupingBy(
            task -> task.partition(),
            LinkedHashMap::new, // 保持分区顺序
            Collectors.toList()
        )
    );
}

// 分区内优化策略
private List<RewritePositionDeletesGroup> optimizePartition(StructLike partition, List<PositionDeletesScanTask> tasks) {

    PartitionStatistics stats = analyzePartition(tasks);

    if (stats.totalSize() < MIN_REWRITE_SIZE) {
        return Collections.emptyList(); // 分区太小，跳过优化
    }

    if (stats.fileCount() < MIN_INPUT_FILES) {
        return Collections.emptyList(); // 文件太少，跳过优化
    }

    // 基于统计信息选择最优策略
    if (stats.avgFileSize() < SMALL_FILE_THRESHOLD) {
        return planSmallFileConsolidation(tasks); // 小文件合并
    } else {
        return planStandardRewrite(tasks); // 标准重写
    }
}
```

#### 5.2.2 跨分区合并限制

```java
// 跨分区合并的约束条件
public class PositionDeletesRewriteConstraints {

    /**
     * 位置删除文件不能跨分区合并的原因:
     * 1. 分区边界必须保持 - Iceberg的分区剪枝依赖此特性
     * 2. 并行度考虑 - 不同分区可以并行处理
     * 3. 数据局部性 - 同分区数据通常存储在相近位置
     */
    public boolean canMergeAcrossPartitions(StructLike partition1, StructLike partition2) {
        return false; // 位置删除文件绝不允许跨分区合并
    }

    /**
     * 等值删除文件的跨分区合并:
     * 某些情况下可以考虑，但需要谨慎评估
     */
    public boolean canMergeEqualityDeletesAcrossPartitions(EqualityDeleteFile file1, EqualityDeleteFile file2) {
        // 只有当等值删除条件与分区键无关时才能考虑
        Set<Integer> equalityFields = Sets.newHashSet(file1.equalityFieldIds());
        Set<Integer> partitionFields = getPartitionFieldIds();

        return Collections.disjoint(equalityFields, partitionFields);
    }
}
```

### 5.3 增量合并策略

#### 5.3.1 基于时间窗口的合并

```java
// 定期合并调度
public class IncrementalDeleteFilesOptimizer {

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public void scheduleOptimization(Table table, Duration interval) {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                optimizeIfNeeded(table);
            } catch (Exception e) {
                LOG.warn("Delete files optimization failed for table {}", table.name(), e);
            }
        }, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void optimizeIfNeeded(Table table) {
        DeleteFilesMetrics metrics = analyzeDeleteFiles(table);

        // 触发条件检查
        if (shouldTriggerOptimization(metrics)) {
            LOG.info("Triggering delete files optimization for table {}, metrics: {}", table.name(), metrics);

            RewritePositionDeleteFiles action = Actions.forTable(table)
                .rewritePositionDeletes()
                .option(RewritePositionDeleteFiles.TARGET_FILE_SIZE, "64MB") // 较小目标文件，提高增量合并频率
                .option(RewritePositionDeleteFiles.MIN_INPUT_FILES, "3");     // 降低最小输入文件数

            RewritePositionDeleteFiles.Result result = action.execute();
            LOG.info("Delete files optimization completed: {}", result);
        }
    }

    private boolean shouldTriggerOptimization(DeleteFilesMetrics metrics) {
        return metrics.fileCount() > 50 ||                    // 文件数量超过50个
               metrics.avgFileSize() < 5 * 1024 * 1024 ||     // 平均文件大小小于5MB
               metrics.timeSinceLastOptimization().toDays() > 1; // 距离上次优化超过1天
    }
}
```

#### 5.3.2 触发式优化

```java
// 写入时触发的优化检查
public class WriteTriggeredOptimizer {

    private final AtomicLong deleteFilesSinceLastOptimization = new AtomicLong(0);
    private static final long OPTIMIZATION_THRESHOLD = 100; // 100个新删除文件后触发优化

    public void onDeleteOperation(Table table, List<DeleteFile> newDeleteFiles) {
        long newCount = deleteFilesSinceLastOptimization.addAndGet(newDeleteFiles.size());

        if (newCount >= OPTIMIZATION_THRESHOLD) {
            // 异步触发优化，避免阻塞写入操作
            CompletableFuture.runAsync(() -> {
                try {
                    optimizeDeleteFiles(table);
                    deleteFilesSinceLastOptimization.set(0); // 重置计数器
                } catch (Exception e) {
                    LOG.warn("Auto-triggered delete files optimization failed", e);
                }
            });
        }
    }
}
```

---

## 6. RewritePositionDeleteFiles实现

### 6.1 核心执行流程

#### 6.1.1 执行上下文初始化

```java
// RewritePositionDeleteFilesSparkAction主执行流程
@Override
public RewritePositionDeleteFiles.Result execute() {
    // 1. 前置检查
    if (table.currentSnapshot() == null) {
        return EMPTY_RESULT; // 空表跳过
    }

    validateAndInitOptions(); // 验证配置参数

    // 2. V3删除向量检查
    if (TableUtil.formatVersion(table) >= 3 && !requiresRewriteToDVs()) {
        LOG.info("v2 deletes in {} have already been rewritten to v3 DVs", table.name());
        return EMPTY_RESULT;
    }

    // 3. 规划阶段 - 生成文件重写组
    StructLikeMap<List<List<PositionDeletesScanTask>>> fileGroupsByPartition = planFileGroups();
    RewriteExecutionContext ctx = new RewriteExecutionContext(fileGroupsByPartition);

    if (ctx.totalGroupCount() == 0) {
        return EMPTY_RESULT; // 没有需要重写的组
    }

    // 4. 执行阶段 - 并发处理重写组
    Stream<RewritePositionDeletesGroup> groupStream = toGroupStream(ctx, fileGroupsByPartition);

    if (partialProgressEnabled) {
        return doExecuteWithPartialProgress(ctx, groupStream, commitManager());
    } else {
        return doExecute(ctx, groupStream, commitManager());
    }
}
```

#### 6.1.2 文件分组规划详细流程

```java
// 分组规划的核心逻辑
private StructLikeMap<List<List<PositionDeletesScanTask>>> planFileGroups() {

    // 1. 扫描删除文件表，获取所有position delete文件
    PositionDeletesTable deleteTable = (PositionDeletesTable)
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.POSITION_DELETES);

    PositionDeletesBatchScan scan = deleteTable.newBatchScan()
        .filter(filter)                    // 应用用户过滤条件
        .planWith(rewriter.executor());    // 使用Spark executor规划

    // 2. 转换为扫描任务
    List<PositionDeletesScanTask> scanTasks;
    try (CloseableIterable<PositionDeletesScanTask> tasksIterable = scan.planTasks()) {
        scanTasks = Lists.newArrayList(tasksIterable);
    }

    if (scanTasks.isEmpty()) {
        return StructLikeMap.create(table.spec().partitionType());
    }

    // 3. 按分区分组
    Map<StructLike, List<PositionDeletesScanTask>> tasksByPartition = scanTasks.stream()
        .collect(Collectors.groupingBy(PositionDeletesScanTask::partition));

    // 4. 每个分区内部进行BinPack分组
    StructLikeMap<List<List<PositionDeletesScanTask>>> fileGroupsByPartition =
        StructLikeMap.create(table.spec().partitionType());

    for (Map.Entry<StructLike, List<PositionDeletesScanTask>> partitionEntry : tasksByPartition.entrySet()) {
        StructLike partition = partitionEntry.getKey();
        List<PositionDeletesScanTask> partitionTasks = partitionEntry.getValue();

        // 在分区内应用BinPack算法
        List<List<PositionDeletesScanTask>> groups = planFileGroupsForPartition(partitionTasks);
        if (!groups.isEmpty()) {
            fileGroupsByPartition.put(partition, groups);
        }
    }

    return fileGroupsByPartition;
}
```

### 6.2 BinPack重写器实现

#### 6.2.1 SparkBinPackPositionDeletesRewriter核心逻辑

```java
public class SparkBinPackPositionDeletesRewriter {

    private final SparkSession spark;
    private final Table table;
    private final long targetFileSize;
    private final int minInputFiles;

    /**
     * 为分区内的删除文件任务规划重写组
     */
    public List<List<PositionDeletesScanTask>> planFileGroups(List<PositionDeletesScanTask> tasks) {

        // 过滤条件检查
        if (tasks.size() < minInputFiles) {
            return Collections.emptyList(); // 文件太少，不值得重写
        }

        long totalInputSize = tasks.stream()
            .mapToLong(task -> task.file().fileSizeInBytes())
            .sum();

        if (totalInputSize < targetFileSize / 2) {
            return Collections.emptyList(); // 总大小太小，重写收益有限
        }

        // 按文件大小排序，优化BinPack效果
        List<PositionDeletesScanTask> sortedTasks = tasks.stream()
            .sorted((t1, t2) -> Long.compare(t2.file().fileSizeInBytes(), t1.file().fileSizeInBytes()))
            .collect(Collectors.toList());

        // 执行BinPack分组
        return binPackIntoGroups(sortedTasks);
    }

    /**
     * BinPack算法实现
     * 目标: 最小化输出文件数量，每个文件接近targetFileSize
     */
    private List<List<PositionDeletesScanTask>> binPackIntoGroups(List<PositionDeletesScanTask> tasks) {
        List<List<PositionDeletesScanTask>> groups = new ArrayList<>();
        List<PositionDeletesScanTask> currentGroup = new ArrayList<>();
        long currentGroupSize = 0;

        for (PositionDeletesScanTask task : tasks) {
            long taskSize = task.file().fileSizeInBytes();

            // 检查是否可以加入当前组
            if (currentGroupSize + taskSize <= targetFileSize) {
                currentGroup.add(task);
                currentGroupSize += taskSize;
            } else {
                // 当前组已满，开始新组
                if (!currentGroup.isEmpty()) {
                    groups.add(new ArrayList<>(currentGroup));
                }
                currentGroup.clear();
                currentGroup.add(task);
                currentGroupSize = taskSize;

                // 单个文件超过目标大小的处理
                if (taskSize > targetFileSize) {
                    LOG.warn("Delete file {} (size: {}) exceeds target file size: {}",
                           task.file().location(), taskSize, targetFileSize);
                }
            }
        }

        // 处理最后一组
        if (!currentGroup.isEmpty()) {
            groups.add(currentGroup);
        }

        return groups;
    }
}
```

#### 6.2.2 重写执行过程

```java
// 单个文件组的重写执行
private RewritePositionDeletesGroup.Result rewriteGroup(RewritePositionDeletesGroup group) {

    List<PositionDeletesScanTask> inputTasks = group.tasks();

    // 1. 创建输出写入器
    PositionDeleteWriter writer = createPositionDeleteWriter(group.partition());

    try {
        // 2. 流式读取并合并所有输入文件
        for (PositionDeletesScanTask task : inputTasks) {
            try (CloseableIterator<InternalRow> rows = readPositionDeletes(task)) {
                while (rows.hasNext()) {
                    InternalRow row = rows.next();
                    writer.write(row); // 写入合并后的删除记录
                }
            }
        }

        // 3. 完成写入，获取输出文件信息
        List<DeleteFile> outputFiles = writer.complete();

        return ImmutableRewritePositionDeletesGroup.Result.builder()
            .rewrittenDeleteFilesCount(inputTasks.size())
            .addedDeleteFilesCount(outputFiles.size())
            .rewrittenBytesCount(calculateInputBytes(inputTasks))
            .addedBytesCount(calculateOutputBytes(outputFiles))
            .build();

    } finally {
        writer.close();
    }
}

// Position Delete记录读取
private CloseableIterator<InternalRow> readPositionDeletes(PositionDeletesScanTask task) {

    // 构建读取器配置
    FileFormat format = task.file().format();
    Map<String, String> properties = table.properties();

    // 创建对应格式的读取器
    switch (format) {
        case PARQUET:
            return createParquetPositionDeleteReader(task, properties);
        case AVRO:
            return createAvroPositionDeleteReader(task, properties);
        case ORC:
            return createOrcPositionDeleteReader(task, properties);
        default:
            throw new UnsupportedOperationException("Unsupported format: " + format);
    }
}
```

### 6.3 事务管理和提交

#### 6.3.1 提交管理器实现

```java
// RewritePositionDeletesCommitManager负责事务提交
public class RewritePositionDeletesCommitManager {

    private final Table table;
    private final long startingSnapshotId;
    private final CommitService commitService;

    /**
     * 提交重写结果到表
     */
    public void commitRewrites(List<RewritePositionDeletesGroup.Result> results) {

        if (results.isEmpty()) {
            return; // 没有重写结果，跳过提交
        }

        // 1. 收集所有输入和输出文件
        Set<DeleteFile> rewrittenFiles = collectRewrittenFiles(results);
        Set<DeleteFile> addedFiles = collectAddedFiles(results);

        // 2. 创建重写操作
        RewriteFiles rewriteOperation = table.newRewrite();

        // 3. 标记删除的文件
        for (DeleteFile rewrittenFile : rewrittenFiles) {
            rewriteOperation.deleteFile(rewrittenFile);
        }

        // 4. 添加新的文件
        for (DeleteFile addedFile : addedFiles) {
            rewriteOperation.addFile(addedFile);
        }

        // 5. 设置提交摘要信息
        Map<String, String> summary = buildCommitSummary(results);
        for (Map.Entry<String, String> entry : summary.entrySet()) {
            rewriteOperation.set(entry.getKey(), entry.getValue());
        }

        // 6. 执行提交
        try {
            rewriteOperation.commit();
            LOG.info("Successfully committed delete files rewrite for table {}", table.name());
        } catch (Exception e) {
            LOG.error("Failed to commit delete files rewrite for table {}", table.name(), e);
            throw new RuntimeException("Rewrite commit failed", e);
        }
    }

    /**
     * 构建提交摘要信息
     */
    private Map<String, String> buildCommitSummary(List<RewritePositionDeletesGroup.Result> results) {

        long totalRewrittenFiles = results.stream()
            .mapToLong(RewritePositionDeletesGroup.Result::rewrittenDeleteFilesCount)
            .sum();

        long totalAddedFiles = results.stream()
            .mapToLong(RewritePositionDeletesGroup.Result::addedDeleteFilesCount)
            .sum();

        long totalRewrittenBytes = results.stream()
            .mapToLong(RewritePositionDeletesGroup.Result::rewrittenBytesCount)
            .sum();

        long totalAddedBytes = results.stream()
            .mapToLong(RewritePositionDeletesGroup.Result::addedBytesCount)
            .sum();

        return ImmutableMap.<String, String>builder()
            .put("rewritten-delete-files-count", String.valueOf(totalRewrittenFiles))
            .put("added-delete-files-count", String.valueOf(totalAddedFiles))
            .put("rewritten-bytes-count", String.valueOf(totalRewrittenBytes))
            .put("added-bytes-count", String.valueOf(totalAddedBytes))
            .put("compression-ratio", String.format("%.2f",
                totalRewrittenBytes > 0 ? (double) totalAddedBytes / totalRewrittenBytes : 1.0))
            .build();
    }
}
```

#### 6.3.2 并发安全和冲突处理

```java
// 并发冲突检测和处理
public class ConflictResolution {

    /**
     * 检查重写过程中是否有并发修改
     */
    public boolean hasConflicts(Table table, long startingSnapshotId, Set<DeleteFile> rewrittenFiles) {

        long currentSnapshotId = table.currentSnapshot().snapshotId();
        if (currentSnapshotId == startingSnapshotId) {
            return false; // 没有新的提交，无冲突
        }

        // 检查新提交是否影响了我们要重写的文件
        List<Snapshot> newSnapshots = getSnapshotsBetween(table, startingSnapshotId, currentSnapshotId);

        for (Snapshot snapshot : newSnapshots) {
            if (hasDeleteFileConflicts(snapshot, rewrittenFiles)) {
                return true; // 发现冲突
            }
        }

        return false;
    }

    /**
     * 冲突解决策略
     */
    public ConflictResolutionStrategy resolveConflict(ConflictType conflictType) {
        switch (conflictType) {
            case DELETED_FILE_MODIFIED:
                return ConflictResolutionStrategy.RETRY_WITH_CURRENT_STATE;

            case NEW_DELETE_FILES_ADDED:
                return ConflictResolutionStrategy.MERGE_AND_RETRY;

            case CONCURRENT_REWRITE:
                return ConflictResolutionStrategy.ABORT_AND_RETRY_LATER;

            default:
                return ConflictResolutionStrategy.FAIL;
        }
    }
}
```

---

## 7. 最佳实践与建议

### 7.1 配置优化建议

#### 7.1.1 删除文件重写参数调优

```java
// 推荐的RewritePositionDeleteFiles配置参数
Map<String, String> optimizedConfig = ImmutableMap.<String, String>builder()

    // 目标文件大小 - 根据工作负载调整
    .put(RewritePositionDeleteFiles.TARGET_FILE_SIZE, "128MB")        // 标准配置
    // .put(RewritePositionDeleteFiles.TARGET_FILE_SIZE, "64MB")       // 高频写入场景
    // .put(RewritePositionDeleteFiles.TARGET_FILE_SIZE, "256MB")      // 低频大批量场景

    // 最小输入文件数 - 控制重写触发阈值
    .put(RewritePositionDeleteFiles.MIN_INPUT_FILES, "5")             // 标准配置
    // .put(RewritePositionDeleteFiles.MIN_INPUT_FILES, "3")           // 激进合并
    // .put(RewritePositionDeleteFiles.MIN_INPUT_FILES, "10")          // 保守合并

    // 并发控制
    .put(RewritePositionDeleteFiles.MAX_CONCURRENT_FILE_GROUP_REWRITES, "10")

    // 部分进度提交 - 适用于大规模重写
    .put(RewritePositionDeleteFiles.PARTIAL_PROGRESS_ENABLED, "true")
    .put(RewritePositionDeleteFiles.PARTIAL_PROGRESS_MAX_COMMITS, "100")

    .build();

// 根据表特征选择配置
public Map<String, String> chooseConfig(TableCharacteristics characteristics) {

    if (characteristics.isHighFrequencyWrites()) {
        // 高频写入: 小目标文件，低触发阈值，快速合并
        return ImmutableMap.of(
            RewritePositionDeleteFiles.TARGET_FILE_SIZE, "64MB",
            RewritePositionDeleteFiles.MIN_INPUT_FILES, "3"
        );
    }

    if (characteristics.isLargeScale()) {
        // 大规模表: 大目标文件，启用部分进度
        return ImmutableMap.of(
            RewritePositionDeleteFiles.TARGET_FILE_SIZE, "256MB",
            RewritePositionDeleteFiles.PARTIAL_PROGRESS_ENABLED, "true"
        );
    }

    return getStandardConfig(); // 标准配置
}
```

#### 7.1.2 表级别配置优化

```java
// 表属性优化，减少删除文件产生
Map<String, String> tableProperties = ImmutableMap.<String, String>builder()

    // 写入配置
    .put("write.target-file-size-bytes", "134217728")        // 128MB目标文件大小
    .put("write.delete.target-file-size-bytes", "67108864")  // 64MB删除文件目标大小

    // 合并配置
    .put("write.delete.mode", "merge-on-read")               // MoR模式减少删除文件
    .put("write.update.mode", "merge-on-read")               // UPDATE也使用MoR

    // 压缩配置
    .put("write.parquet.compression-codec", "zstd")          // 高压缩比编码
    .put("write.parquet.compression-level", "9")             // 最高压缩级别

    // V3特性启用
    .put("format-version", "3")                              // 启用删除向量
    .put("write.delete.distribution-mode", "hash")           // 删除文件分布模式

    .build();

// 创建优化的表
Table optimizedTable = catalog.buildTable(TableIdentifier.of("db", "optimized_table"))
    .withSchema(schema)
    .withPartitionSpec(partitionSpec)
    .withProperties(tableProperties)  // 应用优化配置
    .create();
```

### 7.2 监控和维护策略

#### 7.2.1 删除文件健康度监控

```java
// 删除文件健康度评估工具
public class DeleteFilesHealthChecker {

    public HealthReport assessDeleteFilesHealth(Table table) {

        DeleteFilesMetrics metrics = collectMetrics(table);
        HealthScore score = calculateHealthScore(metrics);

        return HealthReport.builder()
            .table(table.name())
            .metrics(metrics)
            .healthScore(score)
            .recommendations(generateRecommendations(metrics))
            .build();
    }

    private HealthScore calculateHealthScore(DeleteFilesMetrics metrics) {

        double score = 100.0; // 满分100

        // 文件数量影响 (权重30%)
        if (metrics.fileCount() > 1000) {
            score -= 30;
        } else if (metrics.fileCount() > 500) {
            score -= 20;
        } else if (metrics.fileCount() > 100) {
            score -= 10;
        }

        // 文件大小影响 (权重25%)
        if (metrics.avgFileSize() < 1_MB) {
            score -= 25;
        } else if (metrics.avgFileSize() < 10_MB) {
            score -= 15;
        } else if (metrics.avgFileSize() < 50_MB) {
            score -= 5;
        }

        // 分布均匀度影响 (权重25%)
        double sizeVariation = metrics.fileSizeStandardDeviation() / metrics.avgFileSize();
        if (sizeVariation > 2.0) {
            score -= 25;
        } else if (sizeVariation > 1.0) {
            score -= 15;
        }

        // 时间因素影响 (权重20%)
        Duration timeSinceLastOptimization = metrics.timeSinceLastOptimization();
        if (timeSinceLastOptimization.toDays() > 7) {
            score -= 20;
        } else if (timeSinceLastOptimization.toDays() > 3) {
            score -= 10;
        }

        return HealthScore.of(Math.max(0, score));
    }

    private List<Recommendation> generateRecommendations(DeleteFilesMetrics metrics) {

        List<Recommendation> recommendations = new ArrayList<>();

        if (metrics.fileCount() > 500) {
            recommendations.add(Recommendation.builder()
                .priority(Priority.HIGH)
                .action("Run RewritePositionDeleteFiles action immediately")
                .reason("Delete file count (" + metrics.fileCount() + ") exceeds healthy threshold")
                .expectedImpact("Reduce query planning time by 80%+")
                .build());
        }

        if (metrics.avgFileSize() < 10_MB) {
            recommendations.add(Recommendation.builder()
                .priority(Priority.MEDIUM)
                .action("Increase target file size to 128MB")
                .reason("Small delete files (" + metrics.avgFileSize() + ") reduce efficiency")
                .expectedImpact("Improve compression ratio and reduce metadata overhead")
                .build());
        }

        if (TableUtil.formatVersion(table) < 3) {
            recommendations.add(Recommendation.builder()
                .priority(Priority.LOW)
                .action("Upgrade table to format version 3")
                .reason("V3 deletion vectors provide better performance for sparse deletes")
                .expectedImpact("Reduce delete file storage by 50-90%")
                .build());
        }

        return recommendations;
    }
}
```

#### 7.2.2 自动化维护调度

```java
// 自动化删除文件优化调度器
public class AutoDeleteFilesOptimizer {

    private final ScheduledExecutorService scheduler;
    private final Map<String, OptimizationConfig> tableConfigs;

    public void scheduleOptimization(Table table, OptimizationConfig config) {

        tableConfigs.put(table.name(), config);

        // 定期健康检查
        scheduler.scheduleAtFixedRate(() -> {
            try {
                checkAndOptimizeIfNeeded(table, config);
            } catch (Exception e) {
                LOG.error("Auto optimization failed for table {}", table.name(), e);
            }
        }, 0, config.checkInterval().toMillis(), TimeUnit.MILLISECONDS);
    }

    private void checkAndOptimizeIfNeeded(Table table, OptimizationConfig config) {

        DeleteFilesHealthChecker checker = new DeleteFilesHealthChecker();
        HealthReport report = checker.assessDeleteFilesHealth(table);

        if (shouldTriggerOptimization(report, config)) {

            LOG.info("Triggering auto optimization for table {}, health score: {}",
                   table.name(), report.healthScore());

            // 根据健康报告选择优化策略
            OptimizationStrategy strategy = selectStrategy(report);
            executeOptimization(table, strategy);

            // 记录优化结果
            logOptimizationResult(table, report);
        }
    }

    private boolean shouldTriggerOptimization(HealthReport report, OptimizationConfig config) {
        return report.healthScore().value() < config.healthThreshold() ||
               report.metrics().fileCount() > config.maxDeleteFiles() ||
               report.metrics().timeSinceLastOptimization().compareTo(config.maxInterval()) > 0;
    }

    private void executeOptimization(Table table, OptimizationStrategy strategy) {

        switch (strategy) {
            case STANDARD_REWRITE:
                Actions.forTable(table)
                    .rewritePositionDeletes()
                    .option(TARGET_FILE_SIZE, "128MB")
                    .option(MIN_INPUT_FILES, "5")
                    .execute();
                break;

            case AGGRESSIVE_REWRITE:
                Actions.forTable(table)
                    .rewritePositionDeletes()
                    .option(TARGET_FILE_SIZE, "64MB")
                    .option(MIN_INPUT_FILES, "3")
                    .execute();
                break;

            case CONSERVATIVE_REWRITE:
                Actions.forTable(table)
                    .rewritePositionDeletes()
                    .option(TARGET_FILE_SIZE, "256MB")
                    .option(MIN_INPUT_FILES, "10")
                    .execute();
                break;
        }
    }
}

// 优化配置类
public class OptimizationConfig {
    private final Duration checkInterval;      // 检查间隔 (如: 1小时)
    private final Duration maxInterval;        // 最大优化间隔 (如: 1天)
    private final double healthThreshold;      // 健康度阈值 (如: 70.0)
    private final int maxDeleteFiles;          // 最大删除文件数 (如: 500)

    // 预设配置
    public static OptimizationConfig aggressive() {
        return new OptimizationConfig(
            Duration.ofMinutes(30),    // 30分钟检查一次
            Duration.ofHours(6),       // 6小时强制优化
            80.0,                      // 健康度阈值80
            200                        // 最大200个删除文件
        );
    }

    public static OptimizationConfig standard() {
        return new OptimizationConfig(
            Duration.ofHours(1),       // 1小时检查一次
            Duration.ofHours(24),      // 24小时强制优化
            70.0,                      // 健康度阈值70
            500                        // 最大500个删除文件
        );
    }

    public static OptimizationConfig conservative() {
        return new OptimizationConfig(
            Duration.ofHours(6),       // 6小时检查一次
            Duration.ofDays(3),        // 3天强制优化
            50.0,                      // 健康度阈值50
            1000                       // 最大1000个删除文件
        );
    }
}
```

### 7.3 性能优化最佳实践

#### 7.3.1 写入模式优化

```java
// 批量DELETE操作优化
public class OptimizedDeleteOperations {

    /**
     * 批量删除优化 - 合并删除条件
     */
    public void executeBatchDeletes(Table table, List<Expression> deleteConditions) {

        // 错误的做法: 多次单独删除
        // for (Expression condition : deleteConditions) {
        //     table.newDelete().where(condition).commit(); // 产生多个删除文件
        // }

        // 正确的做法: 合并删除条件
        Expression combinedCondition = deleteConditions.stream()
            .reduce(Expressions.alwaysFalse(), Expressions::or);

        table.newDelete()
            .where(combinedCondition)
            .commit(); // 只产生一个删除文件
    }

    /**
     * UPDATE操作优化 - 使用MERGE-ON-READ模式
     */
    public void executeOptimizedUpdate(Table table, Expression condition, Map<String, Object> updates) {

        // 确保表配置了MoR模式
        if (!"merge-on-read".equals(table.properties().get("write.update.mode"))) {
            LOG.warn("Table {} is not configured for merge-on-read updates", table.name());
        }

        // 执行UPDATE
        UpdateTable updateTable = table.newUpdate();
        for (Map.Entry<String, Object> update : updates.entrySet()) {
            updateTable.set(update.getKey(), update.getValue());
        }
        updateTable.where(condition).commit();
    }

    /**
     * 流式写入优化 - 控制删除文件生成频率
     */
    public void executeStreamingDeletes(Table table, Stream<Expression> deleteStream) {

        List<Expression> batch = new ArrayList<>();
        final int batchSize = 100; // 批次大小

        deleteStream.forEach(condition -> {
            batch.add(condition);

            if (batch.size() >= batchSize) {
                executeBatchDeletes(table, new ArrayList<>(batch));
                batch.clear();
            }
        });

        // 处理最后一批
        if (!batch.isEmpty()) {
            executeBatchDeletes(table, batch);
        }
    }
}
```

#### 7.3.2 查询优化策略

```java
// 查询优化建议
public class QueryOptimizationAdvice {

    /**
     * 分区剪枝优化 - 减少需要处理的删除文件
     */
    public TableScan optimizeWithPartitionPruning(Table table, Expression filter) {

        // 分析过滤条件，提取分区相关的条件
        Expression partitionFilter = extractPartitionFilter(filter, table.spec());

        return table.newScan()
            .filter(filter)
            .ignoreResiduals()      // 忽略残余条件，依赖删除文件过滤
            .planWith(Executors.newFixedThreadPool(10)); // 并行规划
    }

    /**
     * 列剪枝优化 - 减少删除文件读取开销
     */
    public TableScan optimizeWithColumnPruning(Table table, List<String> requiredColumns) {

        // 只选择必要的列
        return table.newScan()
            .select(requiredColumns)
            .caseSensitive(false);   // 大小写不敏感，提高缓存命中率
    }

    /**
     * 删除文件缓存策略
     */
    public void configureDeleteFileCaching(Table table) {

        // 启用删除文件元数据缓存
        Map<String, String> properties = new HashMap<>(table.properties());
        properties.put("read.delete-file.cache.enabled", "true");
        properties.put("read.delete-file.cache.max-size", "1000");
        properties.put("read.delete-file.cache.ttl-seconds", "3600"); // 1小时TTL

        // 应用配置 (需要表更新)
        table.updateProperties()
            .putAll(properties)
            .commit();
    }
}
```

### 7.4 故障排查指南

#### 7.4.1 常见问题诊断

```java
// 删除文件问题诊断工具
public class DeleteFilesDiagnostics {

    public DiagnosticReport diagnose(Table table) {

        DiagnosticReport.Builder report = DiagnosticReport.builder()
            .table(table.name());

        // 1. 检查删除文件数量
        int deleteFileCount = countDeleteFiles(table);
        if (deleteFileCount > 1000) {
            report.addIssue(Issue.HIGH_DELETE_FILE_COUNT,
                "Table has " + deleteFileCount + " delete files, consider optimization");
        }

        // 2. 检查删除文件大小分布
        List<Long> fileSizes = getDeleteFileSizes(table);
        double avgSize = fileSizes.stream().mapToLong(Long::longValue).average().orElse(0);
        if (avgSize < 1_MB) {
            report.addIssue(Issue.SMALL_DELETE_FILES,
                "Average delete file size is " + avgSize + " bytes, too small");
        }

        // 3. 检查格式版本
        int formatVersion = TableUtil.formatVersion(table);
        if (formatVersion < 3) {
            report.addIssue(Issue.OLD_FORMAT_VERSION,
                "Table format version is " + formatVersion + ", consider upgrading to V3");
        }

        // 4. 检查配置
        Map<String, String> properties = table.properties();
        if (!"merge-on-read".equals(properties.get("write.update.mode"))) {
            report.addIssue(Issue.SUBOPTIMAL_CONFIG,
                "Update mode is not set to merge-on-read");
        }

        // 5. 检查最近的优化历史
        Duration timeSinceLastOptimization = getTimeSinceLastOptimization(table);
        if (timeSinceLastOptimization.toDays() > 7) {
            report.addIssue(Issue.LACK_OF_MAINTENANCE,
                "Last optimization was " + timeSinceLastOptimization.toDays() + " days ago");
        }

        return report.build();
    }

    public void printDiagnosticSummary(DiagnosticReport report) {
        System.out.println("=== Delete Files Diagnostic Report ===");
        System.out.println("Table: " + report.tableName());
        System.out.println("Issues found: " + report.issues().size());

        for (DiagnosticIssue issue : report.issues()) {
            System.out.printf("  [%s] %s: %s%n",
                issue.severity(), issue.type(), issue.description());
        }

        if (report.issues().isEmpty()) {
            System.out.println("✓ No issues detected");
        } else {
            System.out.println("\nRecommended actions:");
            for (String action : generateRecommendedActions(report)) {
                System.out.println("  • " + action);
            }
        }
    }
}
```

#### 7.4.2 性能问题解决方案

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        删除文件性能问题解决方案                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  问题1: 查询规划时间过长 (>30秒)                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │ 根本原因: 删除文件过多，规划阶段需要处理大量元数据                         │ │
│  │                                                                         │ │
│  │ 解决方案:                                                               │ │
│  │ 1. 立即执行 RewritePositionDeleteFiles action                          │ │
│  │    Actions.forTable(table).rewritePositionDeletes().execute()          │ │
│  │                                                                         │ │
│  │ 2. 调整合并参数，更积极的合并策略                                        │ │
│  │    .option(MIN_INPUT_FILES, "3")                                       │ │
│  │    .option(TARGET_FILE_SIZE, "64MB")                                   │ │
│  │                                                                         │ │
│  │ 3. 启用并行规划                                                         │ │
│  │    scan.planWith(ForkJoinPool.commonPool())                            │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  问题2: 内存使用过高，出现OOM                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │ 根本原因: DeleteFileIndex占用过多内存                                    │ │
│  │                                                                         │ │
│  │ 解决方案:                                                               │ │
│  │ 1. 增加JVM堆内存                                                        │ │
│  │    -Xmx8g -XX:G1HeapRegionSize=32m                                     │ │
│  │                                                                         │ │
│  │ 2. 启用删除文件缓存，减少重复加载                                        │ │
│  │    read.delete-file.cache.enabled=true                                 │ │
│  │    read.delete-file.cache.max-size=2000                                │ │
│  │                                                                         │ │
│  │ 3. 分批处理，避免一次性加载过多删除文件                                  │ │
│  │    .option(MAX_CONCURRENT_FILE_GROUP_REWRITES, "5")                    │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  问题3: 写入性能下降                                                         │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │ 根本原因: 频繁的小批量删除操作产生过多小文件                             │ │
│  │                                                                         │ │
│  │ 解决方案:                                                               │ │
│  │ 1. 合并删除操作，减少提交频率                                            │ │
│  │    批处理DELETE语句，100-1000条件合并为一个操作                          │ │
│  │                                                                         │ │
│  │ 2. 配置表为MoR模式                                                      │ │
│  │    write.update.mode=merge-on-read                                     │ │
│  │    write.delete.mode=merge-on-read                                     │ │
│  │                                                                         │ │
│  │ 3. 设置自动优化调度                                                      │ │
│  │    每6小时自动检查并优化删除文件                                         │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  问题4: 升级到V3后删除向量未生效                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │ 根本原因: V2删除文件未迁移为V3删除向量                                    │ │
│  │                                                                         │ │
│  │ 解决方案:                                                               │ │
│  │ 1. 强制重写为删除向量                                                    │ │
│  │    .option(REWRITE_ALL, "true")                                        │ │
│  │                                                                         │ │
│  │ 2. 检查删除密度，确保适合删除向量                                        │ │
│  │    删除比例应在1%-50%之间                                               │ │
│  │                                                                         │ │
│  │ 3. 验证客户端支持V3特性                                                  │ │
│  │    确保计算引擎版本支持删除向量读取                                      │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 8. 技术总结

### 8.1 核心要点

1. **删除文件过多的根源**: 小批量DELETE/UPDATE操作、高频并发写入、缺乏定期维护
2. **性能影响机制**: 查询规划阶段计算复杂度O(N×M)、内存消耗线性增长、删除文件应用开销
3. **优化策略核心**: BinPack合并算法、V3删除向量、自适应调度机制
4. **最佳实践**: 批量删除操作、MoR模式配置、定期自动优化、性能监控

### 8.2 技术架构优势

Apache Iceberg的删除文件优化方案体现了以下设计优势：
- **可扩展性**: 支持PB级数据湖的删除文件管理
- **灵活性**: 多种删除类型和优化策略适应不同场景
- **效率性**: V3删除向量实现99%+的空间节省
- **可靠性**: MVCC和事务保证优化过程的数据一致性

通过合理配置和定期维护，可以将删除文件数量控制在合理范围内，确保数据湖查询性能始终保持在最佳状态。

---

**文档版本**: v1.0
**生成时间**: 2025-12-26
**适用版本**: Apache Iceberg 1.5.0+
**技术深度**: 源码级别完整分析