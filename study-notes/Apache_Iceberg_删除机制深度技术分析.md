# Apache Iceberg 删除机制深度技术分析

## 概述

Apache Iceberg是一个开放的表格式，专为大型分析数据集而设计。本文档深入分析了Iceberg核心模块中的删除机制，包括删除数据在元数据中的存储表示、不同类型的删除操作、数据结构设计以及小文件优化策略。

## 1. Iceberg删除机制架构

### 1.1 删除机制设计原理

Iceberg采用Copy-on-Write（COW）和Merge-on-Read（MOR）混合策略来处理删除操作。删除操作不会立即修改数据文件，而是生成删除文件（Delete Files）来标记被删除的记录。

### 1.2 核心组件架构图

```
┌─────────────────────────────────────────────────────────────┐
│                  Iceberg Delete Architecture                 │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    │
│  │ Data Files  │    │Delete Files │    │  Manifests  │    │
│  │             │    │             │    │             │    │
│  │ - Parquet   │    │ - Equality  │    │ - Data      │    │
│  │ - ORC       │    │ - Position  │    │ - Delete    │    │
│  │ - Avro      │    │ - Deletion  │    │             │    │
│  │             │    │   Vector    │    │             │    │
│  └─────────────┘    └─────────────┘    └─────────────┘    │
│         │                   │                   │         │
│         └─────────┬─────────────────────────────┘         │
│                   │                                       │
│  ┌─────────────────────────────────────────────────────┐  │
│  │              Snapshot Metadata                      │  │
│  │  - Table Schema                                     │  │
│  │  - Partition Specification                          │  │
│  │  │  - Manifest List                                 │  │
│  │  - Sort Order                                       │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## 2. 删除文件类型详细分析

### 2.1 位置删除（Position Delete）

#### 2.1.1 数据结构

位置删除是Iceberg中最精确的删除方式，通过文件路径和行位置来标识被删除的记录。

```java
// PositionDelete.java 核心结构
public class PositionDelete<R> implements StructLike {
    private CharSequence path;  // 数据文件路径
    private long pos;          // 行位置（0-based）
    private R row;             // 可选的行数据
}
```

#### 2.1.2 存储格式

位置删除文件使用固定的Schema：
```
Schema:
├── file_path: string (required)  // 数据文件路径
└── pos: long (required)         // 行位置
```

#### 2.1.3 索引结构 - Roaring Bitmap

Iceberg使用Roaring Bitmap来高效存储和查询位置删除信息：

```java
// RoaringPositionBitmap 关键特性
class RoaringPositionBitmap {
    // 支持64位位置，但优化32位场景
    static final long MAX_POSITION = toPosition(Integer.MAX_VALUE - 1, Integer.MIN_VALUE);
    
    // 内部使用32位Roaring Bitmap数组
    private RoaringBitmap[] bitmaps;
    
    // 位置分解：高32位作为key，低32位作为position
    private static int key(long pos) {
        return (int) (pos >> 32);
    }
    
    private static int pos32Bits(long pos) {
        return (int) pos;
    }
}
```

**Roaring Bitmap优势：**
- 内存效率：稀疏数据压缩率高
- 查询性能：O(1)复杂度的位置查找
- 运行长度编码：连续删除位置的高效压缩

#### 2.1.4 序列化格式

```
Position Delete Index 序列化格式:
┌────────────────┬────────────────┬────────────────┬────────────────┐
│ Length (4B)    │ Magic (4B)     │ Bitmap Data    │ CRC32 (4B)     │
│ Big-Endian     │ Little-Endian  │ Little-Endian  │ Big-Endian     │
└────────────────┴────────────────┴────────────────┴────────────────┘

其中Bitmap Data格式:
┌────────────────┬────────────────┬────────────────┬────────────────┐
│ Bitmap Count   │ Key1 (4B)      │ Roaring1       │ ...            │
│ (8B)           │                │                │                │
└────────────────┴────────────────┴────────────────┴────────────────┘
```

### 2.2 等值删除（Equality Delete）

#### 2.2.1 原理与应用场景

等值删除基于指定列的值来删除记录，适用于：
- 根据主键删除
- 根据业务键删除
- 批量条件删除

#### 2.2.2 数据结构

```java
// EqualityDeleteWriter.java 关键字段
public class EqualityDeleteWriter<T> {
    private final int[] equalityFieldIds;  // 等值字段ID数组
    private final SortOrder sortOrder;     // 排序顺序
}
```

#### 2.2.3 存储格式

等值删除文件存储被删除记录在等值字段上的值：

```
等值删除文件Schema示例（删除字段：id, name）:
Schema:
├── id: int (required)
└── name: string (required)

文件内容示例：
┌─────┬─────────┐
│ id  │  name   │
├─────┼─────────┤
│ 100 │ "Alice" │
│ 200 │ "Bob"   │
│ 300 │ "Carol" │
└─────┴─────────┘
```

### 2.3 删除向量（Deletion Vector）

#### 2.3.1 概念与优势

删除向量是Iceberg V3引入的新特性，将位置删除信息作为Puffin文件存储：

**优势：**
- 单文件对应：一个删除向量对应一个数据文件
- 直接访问：通过偏移量直接访问
- 高效压缩：使用Roaring Bitmap压缩

#### 2.3.2 Puffin文件结构

```
Puffin文件格式:
┌─────────────┬─────────────┬─────────────┬─────────────┐
│   Header    │   Blob1     │   Blob2     │   Footer    │
│             │             │             │             │
│ - Magic     │ - DV Data   │ - DV Data   │ - Metadata  │
│ - Version   │ - Checksum  │ - Checksum  │ - Index     │
└─────────────┴─────────────┴─────────────┴─────────────┘
```

#### 2.3.3 删除向量Blob格式

```java
// BaseDVFileWriter.java 中的Blob创建
private Blob toBlob(PositionDeleteIndex positions, String path) {
    return new Blob(
        StandardBlobTypes.DV_V1,                    // Blob类型
        ImmutableList.of(MetadataColumns.ROW_POSITION.fieldId()), // 字段ID
        -1,                                         // Snapshot ID
        -1,                                         // Sequence Number
        positions.serialize(),                      // 序列化的位图数据
        null,                                       // 未压缩
        ImmutableMap.of(
            "referenced-data-file", path,          // 引用的数据文件
            "cardinality", String.valueOf(positions.cardinality()) // 基数
        )
    );
}
```

## 3. 删除索引实现详细分析

### 3.1 PositionDeleteIndex接口设计

```java
public interface PositionDeleteIndex {
    // 标记删除位置
    void delete(long position);
    void delete(long posStart, long posEnd);
    
    // 查询删除状态
    boolean isDeleted(long position);
    boolean isEmpty();
    
    // 合并操作
    void merge(PositionDeleteIndex that);
    
    // 迭代操作
    void forEach(LongConsumer consumer);
    
    // 序列化支持
    ByteBuffer serialize();
    static PositionDeleteIndex deserialize(byte[] bytes, DeleteFile deleteFile);
}
```

### 3.2 BitmapPositionDeleteIndex实现

#### 3.2.1 核心算法

```java
class BitmapPositionDeleteIndex implements PositionDeleteIndex {
    private final RoaringPositionBitmap bitmap;
    private final List<DeleteFile> deleteFiles;
    
    @Override
    public void delete(long position) {
        bitmap.set(position);  // 直接设置位图位置
    }
    
    @Override
    public boolean isDeleted(long position) {
        return bitmap.contains(position);  // O(1)查找复杂度
    }
    
    @Override
    public void merge(PositionDeleteIndex that) {
        if (that instanceof BitmapPositionDeleteIndex) {
            bitmap.setAll(((BitmapPositionDeleteIndex) that).bitmap);  // 位图OR操作
        } else {
            that.forEach(this::delete);  // 逐位合并
        }
    }
}
```

#### 3.2.2 内存优化策略

**稀疏位图优化：**
```java
// 动态分配策略
private void allocateBitmapsIfNeeded(int requiredLength) {
    if (bitmaps.length < requiredLength) {
        if (bitmaps.length == 0 && requiredLength == 1) {
            this.bitmaps = new RoaringBitmap[] {new RoaringBitmap()};
        } else {
            RoaringBitmap[] newBitmaps = new RoaringBitmap[requiredLength];
            System.arraycopy(bitmaps, 0, newBitmaps, 0, bitmaps.length);
            // 只为需要的key分配Bitmap
            for (int key = bitmaps.length; key < requiredLength; key++) {
                newBitmaps[key] = new RoaringBitmap();
            }
        }
    }
}
```

**运行长度编码：**
```java
// 压缩优化
public boolean runLengthEncode() {
    boolean changed = false;
    for (RoaringBitmap bitmap : bitmaps) {
        changed |= bitmap.runOptimize();  // 应用RLE压缩
    }
    return changed;
}
```

## 4. 删除文件过滤与合并

### 4.1 删除过滤机制

```java
// Deletes.java 中的过滤逻辑
public static <T> CloseableIterable<T> filter(
    CloseableIterable<T> rows, 
    Function<T, StructLike> rowToDeleteKey, 
    StructLikeSet deleteSet) {
    
    if (deleteSet.isEmpty()) {
        return rows;  // 无删除时直接返回
    }
    
    return new EqualitySetDeleteFilter<>(rowToDeleteKey, deleteSet).filter(rows);
}

// 位置删除过滤
public static <T> CloseableIterable<T> filterDeleted(
    CloseableIterable<T> rows, 
    Predicate<T> isDeleted, 
    DeleteCounter counter) {
    
    return new Filter<T>() {
        @Override
        protected boolean shouldKeep(T item) {
            boolean deleted = isDeleted.test(item);
            if (deleted) {
                counter.increment();  // 统计删除数量
            }
            return !deleted;
        }
    }.filter(rows);
}
```

### 4.2 删除文件索引构建

```java
// 构建位置删除索引映射
public static <T extends StructLike> CharSequenceMap<PositionDeleteIndex> 
    toPositionIndexes(CloseableIterable<T> posDeletes, DeleteFile file) {
    
    CharSequenceMap<PositionDeleteIndex> indexes = CharSequenceMap.create();
    
    try (CloseableIterable<T> deletes = posDeletes) {
        for (T delete : deletes) {
            CharSequence filePath = (CharSequence) FILENAME_ACCESSOR.get(delete);
            long position = (long) POSITION_ACCESSOR.get(delete);
            
            PositionDeleteIndex index = indexes.computeIfAbsent(
                filePath, 
                key -> new BitmapPositionDeleteIndex(file)
            );
            index.delete(position);
        }
    }
    
    return indexes;
}
```

## 5. 小文件优化策略

### 5.1 问题分析

**小文件问题：**
- 大量小删除文件影响查询性能
- 增加元数据开销
- 降低压缩效率

### 5.2 BinPack重写策略

```java
// BinPackRewritePositionDeletePlanner.java 关键逻辑
public class BinPackRewritePositionDeletePlanner {
    
    @Override
    protected long defaultTargetFileSize() {
        return PropertyUtil.propertyAsLong(
            table().properties(),
            TableProperties.DELETE_TARGET_FILE_SIZE_BYTES,
            TableProperties.DELETE_TARGET_FILE_SIZE_BYTES_DEFAULT  // 默认128MB
        );
    }
    
    // 文件分组策略
    private StructLikeMap<List<List<PositionDeletesScanTask>>> planFileGroups() {
        // 按分区分组
        StructLikeMap<List<PositionDeletesScanTask>> filesByPartition = 
            groupByPartition(partitionType, fileTasks);
        
        // 在分区内按大小分组
        return filesByPartition.transformValues(
            tasks -> planFileGroups(tasks)  // Bin Packing算法
        );
    }
}
```

### 5.3 文件合并算法

**Bin Packing算法实现：**

```java
// SizeBasedFileRewritePlanner中的关键逻辑
protected List<List<T>> planFileGroups(List<T> files) {
    List<List<T>> groups = Lists.newArrayList();
    List<T> currentGroup = Lists.newArrayList();
    long currentGroupSize = 0L;
    
    // 按文件大小排序
    files.sort(Comparator.comparing(this::inputFileSize).reversed());
    
    for (T file : files) {
        long fileSize = inputFileSize(file);
        
        if (currentGroupSize + fileSize <= writeMaxFileSize()) {
            currentGroup.add(file);
            currentGroupSize += fileSize;
        } else {
            if (!currentGroup.isEmpty()) {
                groups.add(currentGroup);
            }
            currentGroup = Lists.newArrayList(file);
            currentGroupSize = fileSize;
        }
    }
    
    if (!currentGroup.isEmpty()) {
        groups.add(currentGroup);
    }
    
    return groups;
}
```

### 5.4 删除向量合并优化

```java
// BaseDVFileWriter中的合并逻辑
@Override
public void close() throws IOException {
    PuffinWriter writer = newWriter();
    
    try (PuffinWriter closeableWriter = writer) {
        for (Deletes deletes : deletesByPath.values()) {
            String path = deletes.path();
            PositionDeleteIndex positions = deletes.positions();
            
            // 加载历史删除数据
            PositionDeleteIndex previousPositions = loadPreviousDeletes.apply(path);
            if (previousPositions != null) {
                positions.merge(previousPositions);  // 合并历史删除
                
                // 标记要重写的删除文件
                for (DeleteFile previousDeleteFile : previousPositions.deleteFiles()) {
                    if (ContentFileUtil.isFileScoped(previousDeleteFile)) {
                        rewrittenDeleteFiles.add(previousDeleteFile);
                    }
                }
            }
            
            write(closeableWriter, deletes);  // 写入合并后的删除数据
        }
    }
}
```

## 6. 性能优化与最佳实践

### 6.1 删除性能优化

**1. 位置删除索引优化：**
```java
// 预分配和批量操作
public void deleteBatch(long[] positions) {
    Arrays.sort(positions);  // 排序以提高局部性
    
    for (long position : positions) {
        delete(position);
    }
    
    runLengthEncode();  // 应用压缩优化
}
```

**2. 等值删除优化：**
```java
// 使用排序提高查找效率
public static StructLikeSet toEqualitySet(
    CloseableIterable<StructLike> eqDeletes, 
    Types.StructType eqType) {
    
    try (CloseableIterable<StructLike> deletes = eqDeletes) {
        StructLikeSet deleteSet = StructLikeSet.create(eqType);
        deleteSet.addAll(deletes);  // 批量添加
        return deleteSet;
    }
}
```

### 6.2 内存使用优化

**1. 延迟加载：**
```java
// 按需加载删除索引
CharSequenceMap<PositionDeleteIndex> indexes = CharSequenceMap.create();
for (T delete : deletes) {
    CharSequence filePath = getFilePath(delete);
    PositionDeleteIndex index = indexes.computeIfAbsent(
        filePath, 
        key -> new BitmapPositionDeleteIndex(file)  // 延迟创建
    );
}
```

**2. 索引缓存：**
```java
// 使用LRU缓存避免重复构建索引
private final Map<String, PositionDeleteIndex> indexCache = 
    new ConcurrentHashMap<>();

public PositionDeleteIndex getOrCreateIndex(String dataFile) {
    return indexCache.computeIfAbsent(dataFile, this::buildIndex);
}
```

### 6.3 查询优化策略

**1. 索引预过滤：**
```java
// 在文件级别预过滤
public boolean canContainDeletes(DeleteFile deleteFile, String dataFilePath) {
    return deleteFile.referencedDataFile() == null || 
           deleteFile.referencedDataFile().equals(dataFilePath);
}
```

**2. 批量检查：**
```java
// 批量位置检查
public boolean[] areDeleted(long[] positions) {
    boolean[] results = new boolean[positions.length];
    for (int i = 0; i < positions.length; i++) {
        results[i] = isDeleted(positions[i]);
    }
    return results;
}
```

## 7. 监控与诊断

### 7.1 删除文件统计

```java
// DeleteCounter统计信息
public class DeleteCounter {
    private long deletedRecords = 0L;
    
    public void increment() {
        deletedRecords++;
    }
    
    public long count() {
        return deletedRecords;
    }
}
```

### 7.2 性能指标

**关键指标：**
- 删除文件数量和大小
- 删除记录数量
- 索引构建时间
- 查询过滤时间
- 内存使用量

```java
// 监控删除文件大小分布
public Map<String, Long> getDeleteFileSizeDistribution() {
    Map<String, Long> distribution = new HashMap<>();
    
    for (DeleteFile deleteFile : deleteFiles) {
        String sizeRange = getSizeRange(deleteFile.fileSizeInBytes());
        distribution.merge(sizeRange, 1L, Long::sum);
    }
    
    return distribution;
}
```

## 8. 不同删除类型对比

| 特性 | 位置删除 | 等值删除 | 删除向量 |
|------|---------|----------|---------|
| 精确度 | 极高（精确到行） | 中等（基于列值） | 极高（精确到行） |
| 存储效率 | 高（压缩位图） | 中等（存储删除值） | 极高（单文件映射） |
| 查询性能 | 优秀（O(1)查找） | 良好（哈希查找） | 优秀（直接访问） |
| 适用场景 | 精确行删除 | 条件删除 | 大量删除优化 |
| 文件格式 | Parquet/ORC/Avro | Parquet/ORC/Avro | Puffin |

## 9. 总结与展望

### 9.1 技术优势

1. **多层次删除策略**：支持位置删除、等值删除和删除向量
2. **高效索引结构**：基于Roaring Bitmap的压缩索引
3. **智能文件合并**：BinPack算法优化小文件问题
4. **灵活存储格式**：支持多种文件格式和压缩算法

### 9.2 优化建议

1. **合理配置删除文件大小阈值**
2. **定期执行删除文件合并操作**
3. **监控删除文件数量和分布**
4. **根据数据特点选择合适的删除类型**

### 9.3 发展趋势

1. **删除向量标准化**：与Delta Lake等格式兼容
2. **更高效的压缩算法**：探索新的位图压缩技术
3. **智能删除策略**：基于数据访问模式的自适应优化
4. **云原生优化**：针对对象存储的删除操作优化

---

*本文档基于Apache Iceberg 1.9.x版本源码分析，详细剖析了删除机制的设计原理和实现细节。*