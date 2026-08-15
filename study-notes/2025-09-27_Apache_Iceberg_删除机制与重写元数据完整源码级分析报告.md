# Apache Iceberg 删除机制与重写元数据完整源码级分析报告

## 目录
1. [概述](#概述)
2. [Equality Delete（等值删除）机制源码分析](#equality-delete等值删除机制源码分析)
3. [Position Delete（位置删除）机制源码分析](#position-delete位置删除机制源码分析)
4. [Delete Vector（删除向量）机制源码分析](#delete-vector删除向量机制源码分析)
5. [Rewrite操作元数据处理源码分析](#rewrite操作元数据处理源码分析)
6. [核心删除过滤流程](#核心删除过滤流程)
7. [性能优化与缓存机制](#性能优化与缓存机制)
8. [总结与最佳实践](#总结与最佳实践)

## 概述

Apache Iceberg支持三种删除机制来实现数据的逻辑删除，避免了重写整个数据文件的开销：

1. **Equality Delete（等值删除）**: 基于列值匹配的删除
2. **Position Delete（位置删除）**: 基于行位置的精确删除
3. **Delete Vector（删除向量）**: 基于Roaring Bitmap的高效位置删除

本报告通过深入分析Iceberg源码，详细说明这三种删除机制的实现原理、数据过滤流程以及rewrite操作中的元数据处理机制。

## Equality Delete（等值删除）机制源码分析

### 核心接口与类

#### DeleteFile接口 (`api/src/main/java/org/apache/iceberg/DeleteFile.java`)

```java
public interface DeleteFile extends ContentFile<DeleteFile> {
  // 获取引用的数据文件路径（仅用于删除向量）
  default String referencedDataFile() {
    return null;
  }

  // 获取内容偏移量（仅用于删除向量）
  default Long contentOffset() {
    return null;
  }

  // 获取内容大小（仅用于删除向量）
  default Long contentSizeInBytes() {
    return null;
  }
}
```

### 等值删除过滤实现

#### Deletes类核心过滤逻辑 (`core/src/main/java/org/apache/iceberg/deletes/Deletes.java`)

```java
public static <T> CloseableIterable<T> filter(
    CloseableIterable<T> rows,
    Function<T, StructLike> rowToDeleteKey,
    StructLikeSet deleteSet) {
  if (deleteSet.isEmpty()) {
    return rows;
  }

  EqualitySetDeleteFilter<T> equalityFilter =
      new EqualitySetDeleteFilter<>(rowToDeleteKey, deleteSet);
  return equalityFilter.filter(rows);
}

// 将等值删除转换为StructLikeSet
public static StructLikeSet toEqualitySet(
    CloseableIterable<StructLike> eqDeletes, Types.StructType eqType) {
  try (CloseableIterable<StructLike> deletes = eqDeletes) {
    StructLikeSet deleteSet = StructLikeSet.create(eqType);
    Iterables.addAll(deleteSet, deletes);
    return deleteSet;
  } catch (IOException e) {
    throw new UncheckedIOException("Failed to close equality delete source", e);
  }
}
```

#### 等值删除过滤器实现

```java
private static class EqualitySetDeleteFilter<T> extends Filter<T> {
  private final StructLikeSet deletes;
  private final Function<T, StructLike> extractEqStruct;

  protected EqualitySetDeleteFilter(Function<T, StructLike> extractEq, StructLikeSet deletes) {
    this.extractEqStruct = extractEq;
    this.deletes = deletes;
  }

  @Override
  protected boolean shouldKeep(T row) {
    return !deletes.contains(extractEqStruct.apply(row));
  }
}
```

### DeleteFilter等值删除处理 (`data/src/main/java/org/apache/iceberg/data/DeleteFilter.java`)

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

## Position Delete（位置删除）机制源码分析

### 位置删除索引接口

#### PositionDeleteIndex接口 (`core/src/main/java/org/apache/iceberg/deletes/PositionDeleteIndex.java`)

```java
public interface PositionDeleteIndex {
  // 设置删除位置
  void delete(long position);

  // 设置删除位置范围
  void delete(long posStart, long posEnd);

  // 合并其他索引
  default void merge(PositionDeleteIndex that) {
    if (!that.deleteFiles().isEmpty()) {
      throw new UnsupportedOperationException(getClass().getName() + " does not support merge");
    }
    that.forEach(this::delete);
  }

  // 检查位置是否被删除
  boolean isDeleted(long position);

  // 遍历所有删除位置
  default void forEach(LongConsumer consumer) {
    if (isNotEmpty()) {
      throw new UnsupportedOperationException(getClass().getName() + " does not support forEach");
    }
  }

  // 序列化索引
  default ByteBuffer serialize() {
    throw new UnsupportedOperationException(getClass().getName() + " does not support serialize");
  }

  // 反序列化索引
  static PositionDeleteIndex deserialize(byte[] bytes, DeleteFile deleteFile) {
    return BitmapPositionDeleteIndex.deserialize(bytes, deleteFile);
  }
}
```

### 位置删除索引处理 (`core/src/main/java/org/apache/iceberg/deletes/Deletes.java`)

```java
// 构建位置删除索引
public static <T extends StructLike> CharSequenceMap<PositionDeleteIndex> toPositionIndexes(
    CloseableIterable<T> posDeletes, DeleteFile file) {
  CharSequenceMap<PositionDeleteIndex> indexes = CharSequenceMap.create();

  try (CloseableIterable<T> deletes = posDeletes) {
    for (T delete : deletes) {
      CharSequence filePath = (CharSequence) FILENAME_ACCESSOR.get(delete);
      long position = (long) POSITION_ACCESSOR.get(delete);
      PositionDeleteIndex index =
          indexes.computeIfAbsent(filePath, key -> new BitmapPositionDeleteIndex(file));
      index.delete(position);
    }
  } catch (IOException e) {
    throw new UncheckedIOException("Failed to close position delete source", e);
  }

  return indexes;
}

// 为特定数据文件提取位置
public static PositionDeleteIndex toPositionIndex(
    CharSequence dataLocation, CloseableIterable<T> posDeletes, DeleteFile file) {
  CloseableIterable<Long> positions = extractPositions(dataLocation, posDeletes);
  List<DeleteFile> files = ImmutableList.of(file);
  return toPositionIndex(positions, files);
}

// 数据文件过滤器
private static class DataFileFilter<T extends StructLike> extends Filter<T> {
  private final CharSequence dataLocation;

  DataFileFilter(CharSequence dataLocation) {
    this.dataLocation = dataLocation;
  }

  @Override
  protected boolean shouldKeep(T posDelete) {
    return Comparators.filePath()
            .compare(dataLocation, (CharSequence) FILENAME_ACCESSOR.get(posDelete))
        == 0;
  }
}
```

### Bitmap位置删除索引实现

#### BitmapPositionDeleteIndex (`core/src/main/java/org/apache/iceberg/deletes/BitmapPositionDeleteIndex.java`)

```java
class BitmapPositionDeleteIndex implements PositionDeleteIndex {
  private static final int LENGTH_SIZE_BYTES = 4;
  private static final int MAGIC_NUMBER_SIZE_BYTES = 4;
  private static final int CRC_SIZE_BYTES = 4;
  private static final int BITMAP_DATA_OFFSET = 4;
  private static final int MAGIC_NUMBER = 1681511377;

  private final RoaringPositionBitmap bitmap;
  private final List<DeleteFile> deleteFiles;

  @Override
  public void delete(long position) {
    bitmap.set(position);
  }

  @Override
  public void delete(long posStart, long posEnd) {
    bitmap.setRange(posStart, posEnd);
  }

  @Override
  public boolean isDeleted(long position) {
    return bitmap.contains(position);
  }

  // 序列化格式：
  // - 4字节长度（大端序）
  // - 4字节魔数（小端序）
  // - Roaring位图数据（小端序）
  // - 4字节CRC32校验和（大端序）
  @Override
  public ByteBuffer serialize() {
    bitmap.runLengthEncode(); // 运行长度编码
    int bitmapDataLength = computeBitmapDataLength(bitmap);
    byte[] bytes = new byte[LENGTH_SIZE_BYTES + bitmapDataLength + CRC_SIZE_BYTES];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    buffer.putInt(bitmapDataLength);
    serializeBitmapData(bytes, bitmapDataLength, bitmap);
    int crcOffset = LENGTH_SIZE_BYTES + bitmapDataLength;
    int crc = computeChecksum(bytes, bitmapDataLength);
    buffer.putInt(crcOffset, crc);
    buffer.rewind();
    return buffer;
  }
}
```

## Delete Vector（删除向量）机制源码分析

### Roaring位图实现

#### RoaringPositionBitmap (`core/src/main/java/org/apache/iceberg/deletes/RoaringPositionBitmap.java`)

```java
class RoaringPositionBitmap {
  static final long MAX_POSITION = toPosition(Integer.MAX_VALUE - 1, Integer.MIN_VALUE);
  private static final long BITMAP_COUNT_SIZE_BYTES = 8L;
  private static final long BITMAP_KEY_SIZE_BYTES = 4L;

  private RoaringBitmap[] bitmaps;

  // 将64位位置分解为32位键值对
  public void set(long pos) {
    validatePosition(pos);
    int key = key(pos);               // 高32位作为键
    int pos32Bits = pos32Bits(pos);   // 低32位作为位置
    allocateBitmapsIfNeeded(key + 1);
    bitmaps[key].add(pos32Bits);
  }

  public void setRange(long posStartInclusive, long posEndExclusive) {
    for (long pos = posStartInclusive; pos < posEndExclusive; pos++) {
      set(pos);
    }
  }

  // 合并另一个位图
  public void setAll(RoaringPositionBitmap that) {
    allocateBitmapsIfNeeded(that.bitmaps.length);
    for (int key = 0; key < that.bitmaps.length; key++) {
      bitmaps[key].or(that.bitmaps[key]);
    }
  }

  public boolean contains(long pos) {
    validatePosition(pos);
    int key = key(pos);
    int pos32Bits = pos32Bits(pos);
    return key < bitmaps.length && bitmaps[key].contains(pos32Bits);
  }

  // 序列化格式：
  // - 8字节位图数量
  // - 对于每个32位Roaring位图（按键排序）：
  //   - 4字节键
  //   - 标准格式序列化的32位Roaring位图
  public void serialize(ByteBuffer buffer) {
    validateByteOrder(buffer);
    buffer.putLong(bitmaps.length);
    for (int key = 0; key < bitmaps.length; key++) {
      buffer.putInt(key);
      bitmaps[key].serialize(buffer);
    }
  }

  // 分解64位位置为高低32位
  private static int key(long pos) {
    return (int) (pos >> 32);
  }

  private static int pos32Bits(long pos) {
    return (int) pos;
  }

  private static long toPosition(int key, int pos32Bits) {
    return (((long) key) << 32) | (((long) pos32Bits) & 0xFFFFFFFFL);
  }
}
```

### 删除向量文件写入器

#### BaseDVFileWriter (`core/src/main/java/org/apache/iceberg/deletes/BaseDVFileWriter.java`)

```java
public class BaseDVFileWriter implements DVFileWriter {
  private static final String REFERENCED_DATA_FILE_KEY = "referenced-data-file";
  private static final String CARDINALITY_KEY = "cardinality";

  private final Map<String, Deletes> deletesByPath = Maps.newHashMap();
  private final Map<String, BlobMetadata> blobsByPath = Maps.newHashMap();

  @Override
  public void delete(String path, long pos, PartitionSpec spec, StructLike partition) {
    Deletes deletes =
        deletesByPath.computeIfAbsent(path, key -> new Deletes(path, spec, partition));
    PositionDeleteIndex positions = deletes.positions();
    positions.delete(pos);
  }

  @Override
  public void close() throws IOException {
    if (result == null) {
      List<DeleteFile> dvs = Lists.newArrayList();
      CharSequenceSet referencedDataFiles = CharSequenceSet.empty();
      List<DeleteFile> rewrittenDeleteFiles = Lists.newArrayList();

      PuffinWriter writer = newWriter();

      try (PuffinWriter closeableWriter = writer) {
        for (Deletes deletes : deletesByPath.values()) {
          String path = deletes.path();
          PositionDeleteIndex positions = deletes.positions();
          PositionDeleteIndex previousPositions = loadPreviousDeletes.apply(path);

          // 合并现有删除
          if (previousPositions != null) {
            positions.merge(previousPositions);
            for (DeleteFile previousDeleteFile : previousPositions.deleteFiles()) {
              if (ContentFileUtil.isFileScoped(previousDeleteFile)) {
                rewrittenDeleteFiles.add(previousDeleteFile);
              }
            }
          }

          write(closeableWriter, deletes);
          referencedDataFiles.add(path);
        }
      }

      // 为每个数据文件创建删除向量元数据
      String puffinPath = writer.location();
      long puffinFileSize = writer.fileSize();

      for (String path : deletesByPath.keySet()) {
        DeleteFile dv = createDV(puffinPath, puffinFileSize, path);
        dvs.add(dv);
      }

      this.result = new DeleteWriteResult(dvs, referencedDataFiles, rewrittenDeleteFiles);
    }
  }

  // 创建删除向量元数据
  private DeleteFile createDV(String path, long size, String referencedDataFile) {
    Deletes deletes = deletesByPath.get(referencedDataFile);
    BlobMetadata blobMetadata = blobsByPath.get(referencedDataFile);
    return FileMetadata.deleteFileBuilder(deletes.spec())
        .ofPositionDeletes()
        .withFormat(FileFormat.PUFFIN)
        .withPath(path)
        .withPartition(deletes.partition())
        .withFileSizeInBytes(size)
        .withReferencedDataFile(referencedDataFile)        // 引用的数据文件
        .withContentOffset(blobMetadata.offset())          // Puffin文件中的偏移
        .withContentSizeInBytes(blobMetadata.length())     // 删除向量大小
        .withRecordCount(deletes.positions().cardinality()) // 删除记录数
        .build();
  }

  // 将位置删除索引写入Puffin Blob
  private Blob toBlob(PositionDeleteIndex positions, String path) {
    return new Blob(
        StandardBlobTypes.DV_V1,
        ImmutableList.of(MetadataColumns.ROW_POSITION.fieldId()),
        -1, /* snapshot ID继承 */
        -1, /* sequence number继承 */
        positions.serialize(),
        null, /* 未压缩 */
        ImmutableMap.of(
            REFERENCED_DATA_FILE_KEY, path,
            CARDINALITY_KEY, String.valueOf(positions.cardinality())));
  }
}
```

### 删除加载器实现

#### BaseDeleteLoader (`data/src/main/java/org/apache/iceberg/data/BaseDeleteLoader.java`)

```java
public class BaseDeleteLoader implements DeleteLoader {

  // 加载位置删除（支持删除向量和位置删除文件）
  @Override
  public PositionDeleteIndex loadPositionDeletes(
      Iterable<DeleteFile> deleteFiles, CharSequence filePath) {
    if (ContentFileUtil.containsSingleDV(deleteFiles)) {
      DeleteFile dv = Iterables.getOnlyElement(deleteFiles);
      validateDV(dv, filePath);
      return readDV(dv);
    } else {
      return getOrReadPosDeletes(deleteFiles, filePath);
    }
  }

  // 读取删除向量
  private PositionDeleteIndex readDV(DeleteFile dv) {
    LOG.trace("Opening DV file {}", dv.location());
    InputFile inputFile = loadInputFile.apply(dv);
    long offset = dv.contentOffset();
    int length = dv.contentSizeInBytes().intValue();
    byte[] bytes = readBytes(inputFile, offset, length);
    return PositionDeleteIndex.deserialize(bytes, dv);
  }

  // 读取位置删除文件
  private PositionDeleteIndex readPosDeletes(DeleteFile deleteFile, CharSequence filePath) {
    Expression filter = Expressions.equal(MetadataColumns.DELETE_FILE_PATH.name(), filePath);
    CloseableIterable<Record> deletes = openDeletes(deleteFile, POS_DELETE_SCHEMA, filter);
    return Deletes.toPositionIndex(filePath, deletes, deleteFile);
  }

  // 验证删除向量有效性
  private void validateDV(DeleteFile dv, CharSequence filePath) {
    Preconditions.checkArgument(
        dv.contentOffset() != null,
        "Invalid DV, offset cannot be null: %s",
        ContentFileUtil.dvDesc(dv));
    Preconditions.checkArgument(
        dv.contentSizeInBytes() != null,
        "Invalid DV, length is null: %s",
        ContentFileUtil.dvDesc(dv));
    Preconditions.checkArgument(
        filePath.toString().equals(dv.referencedDataFile()),
        "DV is expected to reference %s, not %s",
        filePath,
        dv.referencedDataFile());
  }

  // 从输入文件读取字节
  private static byte[] readBytes(InputFile inputFile, long offset, int length) {
    try (SeekableInputStream stream = inputFile.newStream()) {
      byte[] bytes = new byte[length];

      if (stream instanceof RangeReadable) {
        RangeReadable rangeReadable = (RangeReadable) stream;
        rangeReadable.readFully(offset, bytes);  // 范围读取
      } else {
        stream.seek(offset);
        ByteStreams.readFully(stream, bytes);    // 顺序读取
      }

      return bytes;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
```

## Rewrite操作元数据处理源码分析

### RewriteFiles接口

#### RewriteFiles API (`api/src/main/java/org/apache/iceberg/RewriteFiles.java`)

```java
public interface RewriteFiles extends SnapshotUpdate<RewriteFiles> {
  // 删除数据文件
  default RewriteFiles deleteFile(DataFile dataFile) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement deleteFile");
  }

  // 删除删除文件
  default RewriteFiles deleteFile(DeleteFile deleteFile) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement deleteFile");
  }

  // 添加数据文件
  default RewriteFiles addFile(DataFile dataFile) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement addFile");
  }

  // 添加删除文件
  default RewriteFiles addFile(DeleteFile deleteFile) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement addFile");
  }

  // 添加带序列号的删除文件
  default RewriteFiles addFile(DeleteFile deleteFile, long dataSequenceNumber) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement addFile");
  }

  // 设置数据序列号
  default RewriteFiles dataSequenceNumber(long sequenceNumber) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement dataSequenceNumber");
  }

  // 验证起始快照
  RewriteFiles validateFromSnapshot(long snapshotId);
}
```

### 重写位置删除文件提交管理器

#### RewritePositionDeletesCommitManager (`core/src/main/java/org/apache/iceberg/actions/RewritePositionDeletesCommitManager.java`)

```java
public class RewritePositionDeletesCommitManager {
  private final Table table;
  private final long startingSnapshotId;
  private final Map<String, String> snapshotProperties;

  // 提交重写操作
  public void commit(Set<RewritePositionDeletesGroup> fileGroups) {
    RewriteFiles rewriteFiles = table.newRewrite().validateFromSnapshot(startingSnapshotId);

    for (RewritePositionDeletesGroup group : fileGroups) {
      // 删除旧的删除文件
      for (DeleteFile file : group.rewrittenDeleteFiles()) {
        rewriteFiles.deleteFile(file);
      }

      // 添加新的删除文件（使用最大数据序列号）
      for (DeleteFile file : group.addedDeleteFiles()) {
        rewriteFiles.addFile(file, group.maxRewrittenDataSequenceNumber());
      }
    }

    snapshotProperties.forEach(rewriteFiles::set);
    rewriteFiles.commit();
  }

  // 清理文件组
  public void abort(RewritePositionDeletesGroup fileGroup) {
    Preconditions.checkState(
        fileGroup.addedDeleteFiles() != null,
        "Cannot abort a fileGroup that was not rewritten");

    Iterable<String> filePaths =
        Iterables.transform(fileGroup.addedDeleteFiles(), ContentFile::location);
    CatalogUtil.deleteFiles(table.io(), filePaths, "position delete", true);
  }

  // 提交或清理
  public void commitOrClean(Set<RewritePositionDeletesGroup> rewriteGroups) {
    try {
      commit(rewriteGroups);
    } catch (CommitStateUnknownException e) {
      LOG.error(
          "Commit state unknown for {}, cannot clean up files because they may have been committed successfully.",
          rewriteGroups, e);
      throw e;
    } catch (Exception e) {
      if (e instanceof CleanableFailure) {
        LOG.error(
            "Cannot commit groups {}, attempting to clean up written files",
            rewriteGroups, e);
        rewriteGroups.forEach(this::abort);
      }
      throw e;
    }
  }
}
```

### 合并快照生产者

#### MergingSnapshotProducer (`core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java`)

```java
abstract class MergingSnapshotProducer<ThisT> extends SnapshotProducer<ThisT> {

  // 数据文件和删除文件的管理器
  private final ManifestMergeManager<DataFile> mergeManager;
  private final ManifestFilterManager<DataFile> filterManager;
  private final ManifestMergeManager<DeleteFile> deleteMergeManager;
  private final ManifestFilterManager<DeleteFile> deleteFilterManager;

  // 更新数据结构
  private final Map<Integer, DataFileSet> newDataFilesBySpec = Maps.newHashMap();
  private Long newDataFilesDataSequenceNumber;
  private final Map<Integer, DeleteFileSet> newDeleteFilesBySpec = Maps.newHashMap();
  private final Set<String> newDVRefs = Sets.newHashSet();
  private final List<ManifestFile> appendManifests = Lists.newArrayList();
  private final List<ManifestFile> rewrittenAppendManifests = Lists.newArrayList();

  // 缓存新的清单文件
  private final List<ManifestFile> cachedNewDataManifests = Lists.newLinkedList();
  private boolean hasNewDataFiles = false;
  private final List<ManifestFile> cachedNewDeleteManifests = Lists.newLinkedList();
  private boolean hasNewDeleteFiles = false;

  MergingSnapshotProducer(String tableName, TableOperations ops) {
    super(ops);
    this.tableName = tableName;

    // 配置清单合并参数
    long targetSizeBytes = ops.current()
        .propertyAsLong(MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
    int minCountToMerge = ops.current()
        .propertyAsInt(MANIFEST_MIN_MERGE_COUNT, MANIFEST_MIN_MERGE_COUNT_DEFAULT);
    boolean mergeEnabled = ops.current()
        .propertyAsBoolean(TableProperties.MANIFEST_MERGE_ENABLED,
                           TableProperties.MANIFEST_MERGE_ENABLED_DEFAULT);

    this.mergeManager = new DataFileMergeManager(targetSizeBytes, minCountToMerge, mergeEnabled);
    this.filterManager = new DataFileFilterManager();
    this.deleteMergeManager = new DeleteFileMergeManager(targetSizeBytes, minCountToMerge, mergeEnabled);
    this.deleteFilterManager = new DeleteFileFilterManager();
  }
}
```

## 核心删除过滤流程

### 统一删除过滤器

#### DeleteFilter核心流程 (`data/src/main/java/org/apache/iceberg/data/DeleteFilter.java`)

```java
public abstract class DeleteFilter<T> {
  private final String filePath;
  private final List<DeleteFile> posDeletes;
  private final List<DeleteFile> eqDeletes;
  private final Schema requiredSchema;
  private final Accessor<StructLike> posAccessor;
  private final boolean hasIsDeletedColumn;

  // 过滤记录的主要方法
  public CloseableIterable<T> filter(CloseableIterable<T> records) {
    return applyEqDeletes(applyPosDeletes(records));
  }

  // 应用位置删除
  private CloseableIterable<T> applyPosDeletes(CloseableIterable<T> records) {
    if (posDeletes.isEmpty()) {
      return records;
    }

    PositionDeleteIndex positionIndex = deletedRowPositions();
    Predicate<T> isDeleted = record -> positionIndex.isDeleted(pos(record));
    return createDeleteIterable(records, isDeleted);
  }

  // 应用等值删除
  private CloseableIterable<T> applyEqDeletes(CloseableIterable<T> records) {
    Predicate<T> isEqDeleted = applyEqDeletes().stream()
        .reduce(Predicate::or)
        .orElse(t -> false);
    return createDeleteIterable(records, isEqDeleted);
  }

  // 创建删除迭代器
  private CloseableIterable<T> createDeleteIterable(
      CloseableIterable<T> records, Predicate<T> isDeleted) {
    return hasIsDeletedColumn
        ? Deletes.markDeleted(records, isDeleted, this::markRowDeleted)  // 标记删除
        : Deletes.filterDeleted(records, isDeleted, counter);            // 过滤删除
  }

  // 获取删除行位置索引
  public PositionDeleteIndex deletedRowPositions() {
    if (deleteRowPositions == null && !posDeletes.isEmpty()) {
      this.deleteRowPositions = deleteLoader().loadPositionDeletes(posDeletes, filePath);
    }
    return deleteRowPositions;
  }

  // 构建文件投影Schema
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

    // 位置删除需要行位置列
    if (needRowPosCol && !posDeletes.isEmpty()) {
      requiredIds.add(MetadataColumns.ROW_POSITION.fieldId());
    }

    // 等值删除需要等值字段
    for (DeleteFile eqDelete : eqDeletes) {
      requiredIds.addAll(eqDelete.equalityFieldIds());
    }

    Set<Integer> missingIds = Sets.newLinkedHashSet(
        Sets.difference(requiredIds, TypeUtil.getProjectedIds(requestedSchema)));

    if (missingIds.isEmpty()) {
      return requestedSchema;
    }

    // 添加缺失的列
    List<Types.NestedField> columns = Lists.newArrayList(requestedSchema.columns());
    for (int fieldId : missingIds) {
      if (fieldId == MetadataColumns.ROW_POSITION.fieldId() ||
          fieldId == MetadataColumns.IS_DELETED.fieldId()) {
        continue; // 在最后添加_pos和_deleted
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
}
```

## 性能优化与缓存机制

### 删除文件缓存策略

#### BaseDeleteLoader缓存实现

```java
public class BaseDeleteLoader implements DeleteLoader {

  // 缓存控制接口
  protected boolean canCache(long size) {
    return false;  // 默认不缓存，子类可覆盖
  }

  protected <V> V getOrLoad(String key, Supplier<V> valueSupplier, long valueSize) {
    throw new UnsupportedOperationException(getClass().getName() + " does not support caching");
  }

  // 加载等值删除（支持缓存）
  private Iterable<StructLike> getOrReadEqDeletes(DeleteFile deleteFile, Schema projection) {
    long estimatedSize = estimateEqDeletesSize(deleteFile, projection);
    if (canCache(estimatedSize)) {
      String cacheKey = deleteFile.location();
      return getOrLoad(cacheKey, () -> readEqDeletes(deleteFile, projection), estimatedSize);
    } else {
      return readEqDeletes(deleteFile, projection);
    }
  }

  // 加载位置删除（支持缓存）
  private PositionDeleteIndex getOrReadPosDeletes(DeleteFile deleteFile, CharSequence filePath) {
    long estimatedSize = estimatePosDeletesSize(deleteFile);
    if (canCache(estimatedSize)) {
      String cacheKey = deleteFile.location();
      CharSequenceMap<PositionDeleteIndex> indexes =
          getOrLoad(cacheKey, () -> readPosDeletes(deleteFile), estimatedSize);
      return indexes.getOrDefault(filePath, PositionDeleteIndex.empty());
    } else {
      return readPosDeletes(deleteFile, filePath);
    }
  }

  // 估算位置删除内存使用（每个位置约1字节）
  private long estimatePosDeletesSize(DeleteFile deleteFile) {
    return deleteFile.recordCount();
  }

  // 估算等值删除内存使用
  private long estimateEqDeletesSize(DeleteFile deleteFile, Schema projection) {
    try {
      long recordCount = deleteFile.recordCount();
      int recordSize = estimateRecordSize(projection);
      return LongMath.checkedMultiply(recordCount, recordSize);
    } catch (ArithmeticException e) {
      return Long.MAX_VALUE;
    }
  }
}
```

### 并发处理优化

#### 并发删除处理

```java
public class BaseDeleteLoader implements DeleteLoader {
  private final ExecutorService workerPool;

  public BaseDeleteLoader(Function<DeleteFile, InputFile> loadInputFile) {
    this(loadInputFile, ThreadPools.getDeleteWorkerPool());
  }

  // 并发执行删除文件处理
  private <I, O> Iterable<O> execute(Iterable<I> objects, Function<I, O> func) {
    Queue<O> output = new ConcurrentLinkedQueue<>();

    Tasks.foreach(objects)
        .executeWith(workerPool)
        .stopOnFailure()
        .onFailure((object, exc) -> LOG.error("Failed to process {}", object, exc))
        .run(object -> output.add(func.apply(object)));

    return output;
  }

  @Override
  public StructLikeSet loadEqualityDeletes(Iterable<DeleteFile> deleteFiles, Schema projection) {
    // 并发加载所有等值删除文件
    Iterable<Iterable<StructLike>> deletes =
        execute(deleteFiles, deleteFile -> getOrReadEqDeletes(deleteFile, projection));
    StructLikeSet deleteSet = StructLikeSet.create(projection.asStruct());
    Iterables.addAll(deleteSet, Iterables.concat(deletes));
    return deleteSet;
  }

  @Override
  public PositionDeleteIndex loadPositionDeletes(
      Iterable<DeleteFile> deleteFiles, CharSequence filePath) {
    if (ContentFileUtil.containsSingleDV(deleteFiles)) {
      DeleteFile dv = Iterables.getOnlyElement(deleteFiles);
      validateDV(dv, filePath);
      return readDV(dv);
    } else {
      return getOrReadPosDeletes(deleteFiles, filePath);
    }
  }

  // 并发加载位置删除
  private PositionDeleteIndex getOrReadPosDeletes(
      Iterable<DeleteFile> deleteFiles, CharSequence filePath) {
    Iterable<PositionDeleteIndex> deletes =
        execute(deleteFiles, deleteFile -> getOrReadPosDeletes(deleteFile, filePath));
    return PositionDeleteIndexUtil.merge(deletes);
  }
}
```

## 总结与最佳实践

### 删除机制比较

| 删除类型 | 使用场景 | 优势 | 劣势 | 性能特点 |
|---------|----------|------|------|----------|
| Equality Delete | 按列值删除，如删除特定用户数据 | 表达力强，支持复杂条件 | 需要读取完整记录进行匹配 | 过滤开销与删除记录数相关 |
| Position Delete | 精确行删除，如DELETE WHERE语句 | 精确定位，过滤高效 | 需要维护行位置信息 | O(1)位置查找，内存使用约1字节/位置 |
| Delete Vector | 高删除率场景，如大批量删除 | 压缩效率高，范围读取优化 | 文件作用域限制 | Roaring bitmap压缩，支持范围操作 |

### 关键设计原则

1. **分层设计**: API层定义接口，Core层实现逻辑，Data层处理具体格式
2. **格式无关**: 删除机制独立于存储格式（Parquet、ORC、Avro）
3. **缓存友好**: 支持删除文件缓存，减少重复I/O
4. **并发安全**: 使用不可变数据结构，支持并发读取
5. **压缩优化**: Delete Vector使用Roaring bitmap实现高效压缩

### 元数据管理最佳实践

1. **快照验证**: 使用`validateFromSnapshot()`确保一致性
2. **序列号管理**: 正确设置数据序列号避免提交冲突
3. **部分提交**: 启用`PARTIAL_PROGRESS_ENABLED`支持大规模重写
4. **文件清理**: 实现proper cleanup机制避免孤儿文件
5. **事务语义**: 利用ACID保证操作原子性

### 性能调优建议

1. **选择合适的删除类型**:
   - 低删除率: Position Delete
   - 中等删除率: Equality Delete
   - 高删除率: Delete Vector

2. **优化删除文件大小**:
   - 控制删除文件数量，避免过多小文件
   - 使用rewrite操作合并删除文件
   - 配置合适的目标文件大小

3. **缓存策略**:
   - 为频繁访问的删除文件启用缓存
   - 根据内存限制调整缓存大小估算
   - 使用适当的缓存替换策略

4. **并发优化**:
   - 利用线程池并发处理删除文件
   - 避免在关键路径上进行同步操作
   - 使用适当的并发控制参数

通过深入理解这些源码实现和设计原理，可以更好地使用Apache Iceberg的删除功能，并针对具体场景进行性能优化。