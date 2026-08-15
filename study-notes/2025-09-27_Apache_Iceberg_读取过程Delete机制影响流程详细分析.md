# 2025-01-27_Apache_Iceberg_读取过程Delete机制影响流程详细分析

## 目录
1. [概述](#概述)
2. [读取流程总体架构](#读取流程总体架构)
3. [Position Delete影响流程详细分析](#position-delete影响流程详细分析)
4. [Equality Delete影响流程详细分析](#equality-delete影响流程详细分析)
5. [Delete Vector处理流程](#delete-vector处理流程)
6. [完整读取流程线路图](#完整读取流程线路图)
7. [性能优化机制](#性能优化机制)
8. [关键代码路径总结](#关键代码路径总结)

## 概述

Apache Iceberg在数据读取过程中需要处理三种类型的删除：Position Delete、Equality Delete和Delete Vector。本文档详细分析这些删除机制如何在读取流程中生效，提供源码级别的流程追踪。

## 读取流程总体架构

### 核心组件关系图

```
┌─────────────────────────────────────────────────────────────────┐
│                    Iceberg读取流程架构                            │
│                                                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐           │
│  │   Snapshot  │────│  Manifest   │────│ FileScanTask│           │
│  │             │    │             │    │             │           │
│  │ - Data Files│    │ - Data List │    │ - DataFile  │           │
│  │ - DeleteFiles│   │ - DeleteList│    │ - DeleteFiles│          │
│  └─────────────┘    └─────────────┘    └─────────────┘           │
│                                                  │               │
│                                                  ▼               │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              Reader实现层                                    │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │ │
│  │  │RowDataReader│  │BatchDataReader│ │ChangelogReader│        │ │
│  │  └─────────────┘  └─────────────┘  └─────────────┘         │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                  │                               │
│                                  ▼                               │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │               SparkDeleteFilter                              │ │
│  │  - 继承自DeleteFilter<InternalRow>                          │ │
│  │  - 处理Position Delete和Equality Delete                    │ │
│  │  - 提供缓存机制                                             │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                  │                               │
│                                  ▼                               │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │               删除处理核心层                                  │ │
│  │                                                             │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │ │
│  │  │Position     │  │Equality     │  │Delete Vector│         │ │
│  │  │Delete Index │  │Delete Set   │  │Bitmap       │         │ │
│  │  └─────────────┘  └─────────────┘  └─────────────┘         │ │
│  └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### 关键源码入口点

| 组件 | 文件路径 | 关键方法 |
|------|----------|----------|
| 文件扫描任务 | `api/src/main/java/org/apache/iceberg/FileScanTask.java:31` | `deletes()` |
| 基础文件扫描任务 | `core/src/main/java/org/apache/iceberg/BaseFileScanTask.java:55` | `deletes()` |
| 行数据读取器 | `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/RowDataReader.java:83` | `open()` |
| 批量数据读取器 | `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/BatchDataReader.java:91` | `open()` |
| Spark删除过滤器 | `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/BaseReader.java:193` | `SparkDeleteFilter` |

## Position Delete影响流程详细分析

### 流程步骤详解

#### 1. 扫描计划阶段 (`core/src/main/java/org/apache/iceberg/util/TableScanUtil.java:85-102`)

```java
// 权重函数包含删除文件大小
Function<FileScanTask, Long> weightFunc =
    file ->
        Math.max(
            file.length() + ScanTaskUtil.contentSizeInBytes(file.deletes()),
            (1 + file.deletes().size()) * openFileCost);
```

**关键点**：
- 删除文件大小被计入任务权重计算
- 影响Bin Packing算法的任务分组策略

#### 2. 文件扫描任务创建 (`core/src/main/java/org/apache/iceberg/BaseFileScanTask.java:34-42`)

```java
public BaseFileScanTask(
    DataFile file,
    DeleteFile[] deletes,    // 删除文件数组
    String schemaString,
    String specString,
    ResidualEvaluator residuals) {
  super(file, schemaString, specString, residuals);
  this.deletes = deletes != null ? deletes : new DeleteFile[0];
}
```

**关键点**：
- 每个FileScanTask携带关联的删除文件列表
- 删除文件在任务创建时就确定

#### 3. 读取器初始化 (`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/RowDataReader.java:83-97`)

```java
@Override
protected CloseableIterator<InternalRow> open(FileScanTask task) {
  String filePath = task.file().location();
  LOG.debug("Opening data file {}", filePath);
  SparkDeleteFilter deleteFilter =
      new SparkDeleteFilter(filePath, task.deletes(), counter(), true);

  // 获取删除过滤器要求的Schema
  Schema requiredSchema = deleteFilter.requiredSchema();
  Map<Integer, ?> idToConstant = constantsMap(task, requiredSchema);

  // 设置Spark文件上下文
  InputFileBlockHolder.set(filePath, task.start(), task.length());

  return deleteFilter.filter(open(task, requiredSchema, idToConstant)).iterator();
}
```

**关键点**：
- 创建SparkDeleteFilter时传入删除文件列表
- requiredSchema包含了删除处理所需的额外列（如_pos）
- 最终返回的是经过删除过滤的迭代器

#### 4. Position Delete过滤器应用 (`data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:250-258`)

```java
private CloseableIterable<T> applyPosDeletes(CloseableIterable<T> records) {
  if (posDeletes.isEmpty()) {
    return records;
  }

  PositionDeleteIndex positionIndex = deletedRowPositions();
  Predicate<T> isDeleted = record -> positionIndex.isDeleted(pos(record));
  return createDeleteIterable(records, isDeleted);
}
```

**Position Delete流程图**：

```
数据文件读取
    │
    ▼
┌─────────────────┐
│ 读取原始记录流   │
│ (包含_pos列)    │
└─────────────────┘
    │
    ▼
┌─────────────────┐
│ 加载Position    │
│ Delete文件      │
│                │
│ 1. 读取delete   │
│    文件内容     │
│ 2. 构建Position │
│    DeleteIndex  │
│ 3. 创建Bitmap   │
└─────────────────┘
    │
    ▼
┌─────────────────┐
│ 对每条记录检查   │
│ positionIndex.  │
│ isDeleted(pos)  │
└─────────────────┘
    │
    ▼
┌─────────────────┐
│ 过滤删除的记录   │
│ 或标记为已删除   │
└─────────────────┘
```

#### 5. Position Delete索引构建 (`core/src/main/java/org/apache/iceberg/deletes/Deletes.java:124-156`)

```java
public static <T extends StructLike> CharSequenceMap<PositionDeleteIndex> toPositionIndexes(
    CloseableIterable<T> posDeletes, DeleteFile file) {
  CharSequenceMap<PositionDeleteIndex> indexes = CharSequenceMap.create();

  try (CloseableIterable<T> deletes = posDeletes) {
    for (T delete : deletes) {
      CharSequence filePath = (CharSequence) FILENAME_ACCESSOR.get(delete);
      long position = (long) POSITION_ACCESSOR.get(delete);
      PositionDeleteIndex index =
          indexes.computeIfAbsent(filePath, key -> new BitmapPositionDeleteIndex(file));
      index.delete(position);  // 将位置添加到bitmap
    }
  } catch (IOException e) {
    throw new UncheckedIOException("Failed to close position delete source", e);
  }

  return indexes;
}
```

**关键点**：
- 按数据文件路径组织Position Delete索引
- 使用BitmapPositionDeleteIndex实现O(1)查找
- 每个数据文件对应一个独立的位置索引

## Equality Delete影响流程详细分析

### 流程步骤详解

#### 1. Equality Delete过滤器应用 (`data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:181-214`)

```java
private List<Predicate<T>> applyEqDeletes() {
  if (isInDeleteSets != null) {
    return isInDeleteSets;
  }

  isInDeleteSets = Lists.newArrayList();
  if (eqDeletes.isEmpty()) {
    return isInDeleteSets;
  }

  // 按等值字段ID分组删除文件
  Multimap<Set<Integer>, DeleteFile> filesByDeleteIds =
      Multimaps.newMultimap(Maps.newHashMap(), Lists::newArrayList);
  for (DeleteFile delete : eqDeletes) {
    filesByDeleteIds.put(Sets.newHashSet(delete.equalityFieldIds()), delete);
  }

  for (Map.Entry<Set<Integer>, Collection<DeleteFile>> entry :
      filesByDeleteIds.asMap().entrySet()) {
    Set<Integer> ids = entry.getKey();
    Iterable<DeleteFile> deletes = entry.getValue();

    Schema deleteSchema = TypeUtil.select(requiredSchema, ids);

    // 创建行投影以匹配删除行
    StructProjection projectRow = StructProjection.create(requiredSchema, deleteSchema);

    StructLikeSet deleteSet = deleteLoader().loadEqualityDeletes(deletes, deleteSchema);
    Predicate<T> isInDeleteSet =
        record -> deleteSet.contains(projectRow.wrap(asStructLike(record)));
    isInDeleteSets.add(isInDeleteSet);
  }

  return isInDeleteSets;
}
```

**Equality Delete流程图**：

```
数据文件读取
    │
    ▼
┌─────────────────┐
│ 读取原始记录流   │
│ (包含等值字段)   │
└─────────────────┘
    │
    ▼
┌─────────────────┐
│ 按等值字段ID     │
│ 分组删除文件     │
│                │
│ 例如:           │
│ [id,name] -> 3个文件 │
│ [email] -> 2个文件   │
└─────────────────┘
    │
    ▼
┌─────────────────┐
│ 对每组删除文件:  │
│                │
│ 1. 选择Schema   │
│ 2. 创建投影     │
│ 3. 加载删除集合 │
│ 4. 创建谓词     │
└─────────────────┘
    │
    ▼
┌─────────────────┐
│ 对每条记录:     │
│                │
│ 1. 应用投影     │
│ 2. 检查是否在   │
│    删除集合中   │
│ 3. 组合多个谓词 │
└─────────────────┘
    │
    ▼
┌─────────────────┐
│ 过滤删除的记录   │
│ 或标记为已删除   │
└─────────────────┘
```

#### 2. Equality Delete集合构建 (`core/src/main/java/org/apache/iceberg/deletes/Deletes.java:113-122`)

```java
public static StructLikeSet toEqualitySet(
    CloseableIterable<StructLike> eqDeletes, Types.StructType eqType) {
  try (CloseableIterable<StructLike> deletes = eqDeletes) {
    StructLikeSet deleteSet = StructLikeSet.create(eqType);
    Iterables.addAll(deleteSet, deletes);  // 添加所有删除条件到集合
    return deleteSet;
  } catch (IOException e) {
    throw new UncheckedIOException("Failed to close equality delete source", e);
  }
}
```

#### 3. Schema投影处理 (`data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:267-318`)

```java
private static Schema fileProjection(
    Schema tableSchema,
    Schema requestedSchema,
    List<DeleteFile> posDeletes,
    List<DeleteFile> eqDeletes,
    boolean needRowPosCol) {

  if (posDeletes.isEmpty() && eqDeletes.isEmpty()) {
    return requestedSchema;
  }

  Set<Integer> requiredIds = Sets.newLinkedHashSet();

  // Position Delete需要_pos列
  if (needRowPosCol && !posDeletes.isEmpty()) {
    requiredIds.add(MetadataColumns.ROW_POSITION.fieldId());
  }

  // Equality Delete需要等值字段
  for (DeleteFile eqDelete : eqDeletes) {
    requiredIds.addAll(eqDelete.equalityFieldIds());
  }

  Set<Integer> missingIds = Sets.newLinkedHashSet(
      Sets.difference(requiredIds, TypeUtil.getProjectedIds(requestedSchema)));

  if (missingIds.isEmpty()) {
    return requestedSchema;
  }

  // 添加缺失的列到Schema
  List<Types.NestedField> columns = Lists.newArrayList(requestedSchema.columns());
  for (int fieldId : missingIds) {
    if (fieldId == MetadataColumns.ROW_POSITION.fieldId() ||
        fieldId == MetadataColumns.IS_DELETED.fieldId()) {
      continue; // 元数据列在最后添加
    }

    Types.NestedField field = tableSchema.asStruct().field(fieldId);
    Preconditions.checkArgument(field != null,
        "Cannot find required field for ID %s", fieldId);
    columns.add(field);
  }

  // 添加元数据列
  if (missingIds.contains(MetadataColumns.ROW_POSITION.fieldId())) {
    columns.add(MetadataColumns.ROW_POSITION);
  }

  if (missingIds.contains(MetadataColumns.IS_DELETED.fieldId())) {
    columns.add(MetadataColumns.IS_DELETED);
  }

  return new Schema(columns);
}
```

**关键点**：
- 动态扩展Schema包含删除处理所需的字段
- Position Delete添加`_pos`列
- Equality Delete添加等值字段
- 支持`_deleted`列用于标记模式

## Delete Vector处理流程

### Delete Vector加载 (`data/src/main/java/org/apache/iceberg/data/BaseDeleteLoader.java:170-180`)

```java
@Override
public PositionDeleteIndex loadPositionDeletes(
    Iterable<DeleteFile> deleteFiles, CharSequence filePath) {
  if (ContentFileUtil.containsSingleDV(deleteFiles)) {
    DeleteFile dv = Iterables.getOnlyElement(deleteFiles);
    validateDV(dv, filePath);
    return readDV(dv);  // 直接读取删除向量
  } else {
    return getOrReadPosDeletes(deleteFiles, filePath);
  }
}

private PositionDeleteIndex readDV(DeleteFile dv) {
  LOG.trace("Opening DV file {}", dv.location());
  InputFile inputFile = loadInputFile.apply(dv);
  long offset = dv.contentOffset();       // Puffin文件中的偏移
  int length = dv.contentSizeInBytes().intValue();  // 删除向量大小
  byte[] bytes = readBytes(inputFile, offset, length);
  return PositionDeleteIndex.deserialize(bytes, dv);  // 反序列化Bitmap
}
```

**Delete Vector流程图**：

```
检测Delete Vector
    │
    ▼
┌─────────────────┐
│ 验证DV有效性    │
│ - contentOffset │
│ - contentSize   │
│ - 数据文件引用  │
└─────────────────┘
    │
    ▼
┌─────────────────┐
│ 从Puffin文件    │
│ 读取指定偏移    │
│ 的字节数据      │
└─────────────────┘
    │
    ▼
┌─────────────────┐
│ 反序列化为      │
│ RoaringBitmap   │
│ - 验证魔数      │
│ - 验证CRC       │
│ - 验证基数      │
└─────────────────┘
    │
    ▼
┌─────────────────┐
│ 创建Position    │
│ DeleteIndex     │
└─────────────────┘
```

## 完整读取流程线路图

### 整体处理流程

```
┌─────────────────────────────────────────────────────────────────┐
│                    Iceberg数据读取完整流程                      │
│                                                                 │
│  ┌─────────────┐                                               │
│  │   开始扫描   │                                               │
│  └─────┬───────┘                                               │
│        │                                                       │
│        ▼                                                       │
│  ┌─────────────┐                                               │
│  │ 读取Snapshot │                                               │
│  │ 和Manifests  │                                               │
│  └─────┬───────┘                                               │
│        │                                                       │
│        ▼                                                       │
│  ┌─────────────┐       ┌─────────────┐                         │
│  │ 创建FileScan │────→  │ 关联Delete  │                         │
│  │ Task        │       │ Files       │                         │
│  └─────┬───────┘       └─────────────┘                         │
│        │                                                       │
│        ▼                                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              任务分组和权重计算                         │   │
│  │  - 包含delete文件大小                                  │   │
│  │  - BinPacking算法                                     │   │
│  └─────┬───────────────────────────────────────────────────┘   │
│        │                                                       │
│        ▼                                                       │
│  ┌─────────────┐                                               │
│  │   执行器     │                                               │
│  │ 分配任务     │                                               │
│  └─────┬───────┘                                               │
│        │                                                       │
│        ▼                                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                Reader.open()                            │   │
│  │                                                         │   │
│  │  1. 创建SparkDeleteFilter                              │   │
│  │  2. 计算requiredSchema                                 │   │
│  │  3. 设置文件上下文                                      │   │
│  └─────┬───────────────────────────────────────────────────┘   │
│        │                                                       │
│        ▼                                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              DeleteFilter初始化                        │   │
│  │                                                         │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │   │
│  │  │Position     │  │Equality     │  │Schema       │     │   │
│  │  │Deletes      │  │Deletes      │  │Projection   │     │   │
│  │  │分类         │  │分类         │  │计算         │     │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘     │   │
│  └─────┬───────────────────────────────────────────────────┘   │
│        │                                                       │
│        ▼                                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              数据文件读取                               │   │
│  │                                                         │   │
│  │  1. 根据requiredSchema读取                             │   │
│  │  2. 包含_pos列(Position Delete)                       │   │
│  │  3. 包含等值字段(Equality Delete)                     │   │
│  │  4. 包含_deleted列(标记模式)                          │   │
│  └─────┬───────────────────────────────────────────────────┘   │
│        │                                                       │
│        ▼                                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              并发删除文件加载                           │   │
│  │                                                         │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │   │
│  │  │Position     │  │Equality     │  │Delete Vector│     │   │
│  │  │Delete Files │  │Delete Files │  │(DV) Files   │     │   │
│  │  │             │  │             │  │             │     │   │
│  │  │→ Bitmap索引 │  │→ StructSet  │  │→ 直接读取   │     │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘     │   │
│  └─────┬───────────────────────────────────────────────────┘   │
│        │                                                       │
│        ▼                                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │               记录级别过滤                              │   │
│  │                                                         │   │
│  │  对每条记录:                                            │   │
│  │  1. 检查Position Delete (O(1) bitmap查找)              │   │
│  │  2. 检查Equality Delete (哈希集合查找)                 │   │
│  │  3. 应用组合谓词                                        │   │
│  └─────┬───────────────────────────────────────────────────┘   │
│        │                                                       │
│        ▼                                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              输出处理                                   │   │
│  │                                                         │   │
│  │  ┌─────────────┐              ┌─────────────┐           │   │
│  │  │ 过滤模式     │              │ 标记模式     │           │   │
│  │  │             │              │             │           │   │
│  │  │ 跳过删除的   │              │ 设置_deleted │           │   │
│  │  │ 记录        │              │ 为true      │           │   │
│  │  └─────────────┘              └─────────────┘           │   │
│  └─────┬───────────────────────────────────────────────────┘   │
│        │                                                       │
│        ▼                                                       │
│  ┌─────────────┐                                               │
│  │   返回结果   │                                               │
│  └─────────────┘                                               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 关键决策点流程

```
开始读取FileScanTask
        │
        ▼
┌──────────────────┐
│ task.deletes()   │
│ 是否为空?        │
└────┬─────────────┘
     │
     ▼
   ┌───┐    否    ┌─────────────────┐
   │是 │────────→ │ 直接读取数据文件 │
   └───┘          └─────────────────┘
     │
     ▼ 否
┌─────────────────┐
│ 创建             │
│ SparkDeleteFilter│
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│ 计算required     │
│ Schema          │
│                 │
│ +_pos (如果有   │
│  Position Delete) │
│ +等值字段 (如果有│
│  Equality Delete) │
│ +_deleted (标记模式)│
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│ 按删除类型分类   │
│ deleteFiles     │
└────┬────────────┘
     │
     ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ Position Delete │     │ Equality Delete │     │ Delete Vector   │
│                 │     │                 │     │                 │
│ 1. 加载Delete   │     │ 1. 按等值字段ID │     │ 1. 验证DV       │
│    文件内容     │     │    分组         │     │ 2. 读取Puffin   │
│ 2. 构建Bitmap   │     │ 2. 构建StructSet│     │    偏移数据     │
│    索引         │     │ 3. 创建投影     │     │ 3. 反序列化     │
│ 3. 缓存索引     │     │ 4. 缓存集合     │     │    Bitmap       │
└────┬────────────┘     └────┬────────────┘     └────┬────────────┘
     │                       │                       │
     └───────────────────────┼───────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ 读取数据文件     │
                    │ (含扩展Schema)   │
                    └────┬────────────┘
                         │
                         ▼
                    ┌─────────────────┐
                    │ 逐行应用过滤器   │
                    │                 │
                    │ for each record: │
                    │   if posDeleted  │
                    │   if eqDeleted   │
                    │   then filter/mark│
                    └────┬────────────┘
                         │
                         ▼
                    ┌─────────────────┐
                    │ 返回过滤后的     │
                    │ 记录迭代器       │
                    └─────────────────┘
```

## 性能优化机制

### 1. 缓存机制 (`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/BaseReader.java:227-244`)

```java
private class CachingDeleteLoader extends BaseDeleteLoader {
  private final SparkExecutorCache cache;

  CachingDeleteLoader(Function<DeleteFile, InputFile> loadInputFile) {
    super(loadInputFile);
    this.cache = SparkExecutorCache.getOrCreate();
  }

  @Override
  protected boolean canCache(long size) {
    return cache != null && size < cache.maxEntrySize();
  }

  @Override
  protected <V> V getOrLoad(String key, Supplier<V> valueSupplier, long valueSize) {
    return cache.getOrLoad(table().name(), key, valueSupplier, valueSize);
  }
}
```

**缓存策略**：
- Position Delete索引按文件路径缓存
- Equality Delete集合按删除文件缓存
- 缓存大小基于估算的内存使用量

### 2. 并发处理 (`data/src/main/java/org/apache/iceberg/data/BaseDeleteLoader.java:273-283`)

```java
private <I, O> Iterable<O> execute(Iterable<I> objects, Function<I, O> func) {
  Queue<O> output = new ConcurrentLinkedQueue<>();

  Tasks.foreach(objects)
      .executeWith(workerPool)           // 使用工作线程池
      .stopOnFailure()
      .onFailure((object, exc) -> LOG.error("Failed to process {}", object, exc))
      .run(object -> output.add(func.apply(object)));

  return output;
}
```

**并发优化**：
- 并发加载多个删除文件
- 使用线程池管理工作负载
- 失败快速停止机制

### 3. 内存优化

#### Delete Vector压缩 (`core/src/main/java/org/apache/iceberg/deletes/RoaringPositionBitmap.java:145-151`)

```java
public boolean runLengthEncode() {
  boolean changed = false;
  for (RoaringBitmap bitmap : bitmaps) {
    changed |= bitmap.runOptimize();    // 运行长度编码
  }
  return changed;
}
```

#### Position Delete内存估算 (`data/src/main/java/org/apache/iceberg/data/BaseDeleteLoader.java:285-290`)

```java
private long estimatePosDeletesSize(DeleteFile deleteFile) {
  // Roaring bitmap平均每个值约1字节
  return deleteFile.recordCount();
}
```

## 关键代码路径总结

### 读取流程关键路径

| 步骤 | 源码位置 | 关键方法 | 作用 |
|------|----------|----------|------|
| 1. 任务创建 | `BaseFileScanTask.java:34` | 构造函数 | 关联删除文件到扫描任务 |
| 2. 权重计算 | `TableScanUtil.java:91` | `weightFunc` | 包含删除文件大小的任务权重 |
| 3. 读取器打开 | `RowDataReader.java:83` | `open()` | 创建删除过滤器 |
| 4. 过滤器初始化 | `BaseReader.java:196` | `SparkDeleteFilter` | 扩展Schema, 分类删除文件 |
| 5. Position Delete处理 | `DeleteFilter.java:250` | `applyPosDeletes()` | 构建位置索引, 应用位置过滤 |
| 6. Equality Delete处理 | `DeleteFilter.java:181` | `applyEqDeletes()` | 构建等值集合, 应用等值过滤 |
| 7. Delete Vector处理 | `BaseDeleteLoader.java:182` | `readDV()` | 直接读取和反序列化 |
| 8. 记录过滤 | `DeleteFilter.java:177` | `filter()` | 组合应用所有删除逻辑 |

### 删除文件加载路径

| 删除类型 | 加载路径 | 数据结构 | 查找复杂度 |
|----------|----------|----------|------------|
| Position Delete | `BaseDeleteLoader.loadPositionDeletes()` → `Deletes.toPositionIndex()` → `BitmapPositionDeleteIndex` | Roaring Bitmap | O(1) |
| Equality Delete | `BaseDeleteLoader.loadEqualityDeletes()` → `Deletes.toEqualitySet()` → `StructLikeSet` | Hash Set | O(1) |
| Delete Vector | `BaseDeleteLoader.readDV()` → `PositionDeleteIndex.deserialize()` → `BitmapPositionDeleteIndex` | Roaring Bitmap | O(1) |

### Schema扩展路径

| 扩展类型 | 触发条件 | 添加字段 | 源码位置 |
|----------|----------|----------|----------|
| Position列 | `!posDeletes.isEmpty() && needRowPosCol` | `MetadataColumns.ROW_POSITION` | `DeleteFilter.java:278` |
| 等值字段 | `eqDelete.equalityFieldIds()` | 等值删除字段 | `DeleteFilter.java:282` |
| 删除标记列 | `hasIsDeletedColumn` | `MetadataColumns.IS_DELETED` | `DeleteFilter.java:313` |

这个完整的流程分析展示了Iceberg如何在读取过程中高效处理三种不同的删除机制，通过精心设计的缓存、并发和压缩策略，在保证数据一致性的同时实现了优秀的查询性能。