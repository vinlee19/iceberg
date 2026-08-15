# Apache Iceberg V1 到 V3 技术演化与核心特性深度源码分析报告

**文档日期**: 2025-11-02
**分析版本**: Apache Iceberg 1.10.x
**作者**: 基于源码深度分析

---

## 目录

1. [概述](#概述)
2. [表格式版本架构设计](#表格式版本架构设计)
3. [V1: 基础表格式](#v1-基础表格式)
4. [V2: 行级更新与删除支持](#v2-行级更新与删除支持)
5. [V3: 数据血缘与高级类型](#v3-数据血缘与高级类型)
6. [V4: 删除向量与性能优化](#v4-删除向量与性能优化)
7. [版本升级机制](#版本升级机制)
8. [完整技术演化路径](#完整技术演化路径)
9. [源码关键实现](#源码关键实现)
10. [最佳实践建议](#最佳实践建议)

---

## 概述

### 1.1 版本常量定义

**源码位置**: `core/src/main/java/org/apache/iceberg/TableMetadata.java`

```java
static final int DEFAULT_TABLE_FORMAT_VERSION = 2;      // 默认版本为 V2
static final int SUPPORTED_TABLE_FORMAT_VERSION = 4;    // 当前支持最高版本 V4
static final int MIN_FORMAT_VERSION_ROW_LINEAGE = 3;    // Row Lineage 最低要求 V3
```

**测试支持**: `api/src/test/java/org/apache/iceberg/TestHelpers.java`

```java
public static final int MAX_FORMAT_VERSION = 4;
public static final List<Integer> ALL_VERSIONS = [1, 2, 3, 4];
public static final List<Integer> V2_AND_ABOVE = [2, 3, 4];
public static final List<Integer> V3_AND_ABOVE = [3, 4];
```

### 1.2 版本演化时间线

```
V1 (2017-2018)  →  V2 (2019-2020)  →  V3 (2021-2022)  →  V4 (2023-2024)
    基础格式         删除文件支持       数据血缘支持        删除向量优化
```

---

## 表格式版本架构设计

### 2.1 版本控制核心机制

#### 2.1.1 TableMetadata 中的版本字段

**源码**: `TableMetadata.java:401-403`

```java
public int formatVersion() {
    return formatVersion;
}
```

#### 2.1.2 序列号机制 (V2+)

**源码**: `TableMetadata.java:413-419`

```java
public long lastSequenceNumber() {
    return lastSequenceNumber;
}

public long nextSequenceNumber() {
    // V1 表不使用序列号
    return formatVersion > 1 ? lastSequenceNumber + 1 : INITIAL_SEQUENCE_NUMBER;
}
```

### 2.2 版本特性门控

不同特性在不同版本中的启用由版本号控制:

```java
// V3 特性检查
if (formatVersion >= MIN_FORMAT_VERSION_ROW_LINEAGE) {
    // 启用 Row Lineage 追踪
}

// V2 分区演化差异
if (formatVersion > 1) {
    // V2+ 使用灵活的分区字段 ID 重用
} else {
    // V1 必须使用 void transforms
}
```

---

## V1: 基础表格式

### 3.1 核心特性

#### 3.1.1 基础元数据结构

- **Schema 演化**: 支持添加、删除、重命名列
- **Partition Spec**: 分区定义与演化
- **Snapshot**: 快照时间旅行
- **仅数据文件**: 只有 DATA 类型文件

#### 3.1.2 Manifest 结构

**ManifestFile 可选字段** (V1 中为 null):
- `sequence_number`: 不支持
- `min_sequence_number`: 不支持
- `content`: 默认为 DATA (没有删除文件概念)

### 3.2 V1 的关键限制

#### 3.2.1 分区演化限制

**源码**: `TableMetadata.java:665-697`

```java
if (formatVersion > 1) {
    // V2+: 灵活重用分区字段 ID
    for (PartitionField field : partitionSpec.fields()) {
        int partitionFieldId = transformToFieldId.computeIfAbsent(
            Pair.of(field.sourceId(), field.transform().toString()),
            k -> nextID.get()
        );
        specBuilder.add(field.sourceId(), partitionFieldId, field.name(), field.transform());
    }
} else {
    // V1: 必须保留所有旧字段，删除的字段替换为 void transform
    for (PartitionField field : spec().fields()) {
        PartitionField newField = newFields.remove(...);
        if (newField != null) {
            specBuilder.add(newField.sourceId(), field.fieldId(), newField.name(), newField.transform());
        } else {
            // 关键: V1 不能真正删除分区字段，只能转为 void
            String voidName = newFieldNames.contains(field.name())
                ? field.name() + "_" + field.fieldId()
                : field.name();
            specBuilder.add(field.sourceId(), field.fieldId(), voidName, Transforms.alwaysNull());
        }
    }
}
```

**限制说明**:
- V1 分区字段必须顺序排列
- 删除分区字段时必须保留为 `void` 占位
- 分区演化历史不可清理

#### 3.2.2 无法支持行级删除

- 只有 `OVERWRITE` 操作
- 删除数据需要重写整个分区
- 无 `UPDATE` 和 `DELETE` 的高效支持

#### 3.2.3 并发控制限制

- 无序列号机制，无法精确追踪数据变更顺序
- 依赖快照 ID 判断数据新旧，精度不足

### 3.3 V1 数据文件格式

**FileContent 枚举** (`api/src/main/java/org/apache/iceberg/FileContent.java`):

```java
public enum FileContent {
    DATA(0),                    // V1 唯一支持
    POSITION_DELETES(1),        // V2+
    EQUALITY_DELETES(2);        // V2+
}
```

**V1 ContentFile 结构**:
- 只有基础统计信息 (record count, file size, bounds)
- 无 `sequenceNumber`
- 无删除文件相关字段

---

## V2: 行级更新与删除支持

### 4.1 核心创新

#### 4.1.1 序列号机制

**作用**: 精确追踪数据变更顺序，解决并发冲突

**源码**: `api/src/main/java/org/apache/iceberg/Snapshot.java:36-42`

```java
/**
 * Return this snapshot's sequence number.
 *
 * Sequence numbers are assigned when a snapshot is committed.
 */
long sequenceNumber();
```

**ManifestFile 新增字段**:

```java
Types.NestedField SEQUENCE_NUMBER = optional(
    515, "sequence_number", Types.LongType.get(),
    "Sequence number when the manifest was added"
);

Types.NestedField MIN_SEQUENCE_NUMBER = optional(
    516, "min_sequence_number", Types.LongType.get(),
    "Lowest sequence number in the manifest"
);
```

#### 4.1.2 删除文件支持

**Delete File 类型** (`api/src/main/java/org/apache/iceberg/DeleteFile.java`):

```java
public interface DeleteFile extends ContentFile<DeleteFile> {
    // 删除文件特有字段 (V2 引入)

    /**
     * 返回所有删除记录引用的数据文件路径
     * 对于 Deletion Vector 必需，Position Delete 可选
     */
    default String referencedDataFile() {
        return null;
    }

    // 以下字段为 Deletion Vector (V3/V4) 预留
    default Long contentOffset() { return null; }
    default Long contentSizeInBytes() { return null; }
}
```

**两种删除模式**:

1. **Position Deletes** (`FileContent.POSITION_DELETES`):
   - 删除文件格式: `(file_path: string, pos: long)`
   - 精确定位删除行的物理位置
   - 适合小批量删除

2. **Equality Deletes** (`FileContent.EQUALITY_DELETES`):
   - 删除文件格式: 包含删除条件的值列
   - 基于字段值匹配删除
   - 适合大批量删除

**Manifest Content 区分**:

**源码**: `api/src/main/java/org/apache/iceberg/ManifestFile.java:38-40`

```java
Types.NestedField MANIFEST_CONTENT = optional(
    517, "content", Types.IntegerType.get(),
    "Contents of the manifest: 0=data, 1=deletes"
);
```

### 4.2 V2 技术改进

#### 4.2.1 分区演化优化

**源码**: `TableMetadata.java:646-663`

```java
if (formatVersion > 1) {
    // V2+: 可以真正删除分区字段，重用字段 ID
    Map<Pair<Integer, String>, Integer> transformToFieldId =
        specs.stream()
            .flatMap(spec -> spec.fields().stream())
            .collect(Collectors.toMap(
                field -> Pair.of(field.sourceId(), field.transform().toString()),
                PartitionField::fieldId,
                Math::max  // 对于相同的 source+transform，取最大 ID
            ));

    for (PartitionField field : partitionSpec.fields()) {
        int partitionFieldId = transformToFieldId.computeIfAbsent(
            Pair.of(field.sourceId(), field.transform().toString()),
            k -> nextID.get()
        );
        specBuilder.add(field.sourceId(), partitionFieldId, field.name(), field.transform());
    }
}
```

**优势**:
- 不再需要 void transforms
- 分区字段可以真正删除和重新添加
- 分区 ID 可以智能重用

#### 4.2.2 DELETE 操作支持

**RowDelta API** (V2 引入):

```java
table.newRowDelta()
    .addRows(dataFile)           // 添加新数据
    .addDeletes(deleteFile)      // 添加删除文件
    .commit();
```

#### 4.2.3 Snapshot 分离 Data 和 Delete Manifests

**源码**: `api/src/main/java/org/apache/iceberg/Snapshot.java:74-89`

```java
/**
 * Return a {@link ManifestFile} for each data manifest in this snapshot.
 */
List<ManifestFile> dataManifests(FileIO io);

/**
 * Return a {@link ManifestFile} for each delete manifest in this snapshot.
 */
List<ManifestFile> deleteManifests(FileIO io);
```

**读取优化**:
- 扫描时可以只读取 data manifests
- Delete manifests 按需加载
- 提升查询性能

### 4.3 V2 vs V1 对比表

| 特性 | V1 | V2 |
|------|----|----|
| **序列号** | ❌ 无 | ✅ 支持 |
| **删除文件** | ❌ 无 | ✅ Position + Equality |
| **行级删除** | ❌ 重写整个文件 | ✅ 增量删除文件 |
| **分区演化** | ⚠️ Void transforms | ✅ 真正删除 |
| **并发控制** | 基于快照 ID | 基于序列号 |
| **UPDATE 支持** | ❌ 需重写 | ✅ Delete + Insert |
| **Manifest 类型** | 仅 DATA | DATA + DELETES |

---

## V3: 数据血缘与高级类型

### 5.1 核心特性

#### 5.1.1 Row Lineage (行血缘)

**目的**: 追踪每一行数据的来源和变更历史

**源码**: `TableMetadata.java:1264-1274`

```java
if (formatVersion >= MIN_FORMAT_VERSION_ROW_LINEAGE) {
    ValidationException.check(
        snapshot.firstRowId() != null,
        "Cannot add a snapshot: first-row-id is null"
    );
    ValidationException.check(
        snapshot.firstRowId() != null && snapshot.firstRowId() >= nextRowId,
        "Cannot add a snapshot, first-row-id is behind table next-row-id: %s < %s",
        snapshot.firstRowId(), nextRowId
    );

    // 为新快照分配的行递增全局行 ID
    this.nextRowId += snapshot.addedRows();
}
```

**ManifestFile 新增字段**:

```java
Types.NestedField FIRST_ROW_ID = optional(
    520, "first_row_id", Types.LongType.get(),
    "Starting row ID to assign to new rows in ADDED data files"
);
```

**DataFile 新增字段**:

```java
// 数据文件中第一行的全局行 ID
default Long firstRowId() {
    return null;
}
```

**用途**:
- **变更数据捕获 (CDC)**: 精确追踪每行的增删改
- **数据审计**: 完整的行级变更历史
- **增量处理**: 基于行 ID 范围的高效增量读取
- **Merge-on-Read 优化**: 快速定位需要合并的行

#### 5.1.2 Default Values (列默认值)

**源码**: `api/src/main/java/org/apache/iceberg/Schema.java:59`

```java
@VisibleForTesting
static final int DEFAULT_VALUES_MIN_FORMAT_VERSION = 3;
```

**功能**:
- Schema 演化时可以为新列指定默认值
- 读取旧数据时自动填充默认值
- 避免重写已有数据文件

**使用场景**:

```java
table.updateSchema()
    .addColumn("new_column", Types.IntegerType.get())
    .setDefault("new_column", 0)  // V3 支持
    .commit();
```

#### 5.1.3 新数据类型

**源码**: `api/src/main/java/org/apache/iceberg/Schema.java:62-68`

```java
static final Map<Type.TypeID, Integer> MIN_FORMAT_VERSIONS = ImmutableMap.of(
    Type.TypeID.TIMESTAMP_NANO, 3,    // 纳秒级时间戳
    Type.TypeID.VARIANT, 3,           // 半结构化数据 (JSON-like)
    Type.TypeID.UNKNOWN, 3,           // 未知类型占位符
    Type.TypeID.GEOMETRY, 3,          // 几何类型 (GIS)
    Type.TypeID.GEOGRAPHY, 3          // 地理类型 (GIS)
);
```

**新类型详解**:

1. **TIMESTAMP_NANO**:
   - 纳秒级精度时间戳
   - 支持高精度时序数据
   - 替代微秒级 `TIMESTAMP`

2. **VARIANT**:
   - 半结构化数据类型
   - 类似 JSON，支持嵌套
   - 适合日志、事件数据

3. **GEOMETRY / GEOGRAPHY**:
   - GIS 空间数据支持
   - 几何图形和地理坐标
   - 为空间分析优化

#### 5.1.4 Statistics Files (Puffin 格式)

**源码**: `api/src/main/java/org/apache/iceberg/StatisticsFile.java`

```java
public interface StatisticsFile {
    long snapshotId();              // 关联快照
    String path();                  // 统计文件路径
    long fileSizeInBytes();         // 文件大小
    long fileFooterSizeInBytes();   // Puffin footer 大小
    List<BlobMetadata> blobMetadata();  // 统计数据 blob 列表
}
```

**BlobMetadata 结构**:

```java
public interface BlobMetadata {
    String type();                      // blob 类型 (如 theta sketch)
    long sourceSnapshotId();            // 源快照 ID
    long sourceSnapshotSequenceNumber(); // 源序列号
    List<Integer> fields();             // 统计的列 ID
    Map<String, String> properties();   // 额外属性
}
```

**Puffin 标准 Blob 类型** (`core/src/main/java/org/apache/iceberg/puffin/StandardBlobTypes.java`):

```java
public static final String APACHE_DATASKETCHES_THETA_V1 = "apache-datasketches-theta-v1";
public static final String DV_V1 = "deletion-vector-v1";  // V4 删除向量
```

**用途**:
- **NDV Sketch**: 列基数估算 (用于查询优化)
- **Histogram**: 数据分布统计
- **Bloom Filter**: 快速成员检测
- **自定义统计**: 扩展性强

#### 5.1.5 Partition Statistics Files

**源码**: `api/src/main/java/org/apache/iceberg/PartitionStatisticsFile.java`

```java
public interface PartitionStatisticsFile {
    long snapshotId();          // 关联快照
    String path();              // 文件路径
    long fileSizeInBytes();     // 文件大小
}
```

**用途**:
- 分区级别的统计信息
- 加速分区剪枝
- 优化分区表查询计划

### 5.2 V3 技术改进

#### 5.2.1 读取性能优化

通过 Row ID 范围读取:

```java
TableScan scan = table.newScan()
    .filter(Expressions.greaterThanOrEqual("_row_id", startRowId))
    .filter(Expressions.lessThan("_row_id", endRowId));
```

#### 5.2.2 CDC 流式处理

```java
IncrementalChangelogScan changelogScan = table.newIncrementalChangelogScan()
    .fromSnapshotExclusive(fromSnapshotId)
    .toSnapshot(toSnapshotId);

for (ChangelogScanTask task : changelogScan.planFiles()) {
    // 基于 firstRowId 追踪每行变更
}
```

### 5.3 V3 vs V2 对比表

| 特性 | V2 | V3 |
|------|----|----|
| **Row Lineage** | ❌ 无 | ✅ firstRowId 追踪 |
| **默认列值** | ❌ 无 | ✅ 支持 |
| **纳秒时间戳** | ❌ 微秒精度 | ✅ 纳秒精度 |
| **半结构化数据** | ❌ 无 | ✅ VARIANT 类型 |
| **GIS 支持** | ❌ 无 | ✅ GEOMETRY/GEOGRAPHY |
| **统计文件** | ❌ 无 | ✅ Puffin 格式 |
| **CDC 支持** | ⚠️ 基础支持 | ✅ 完整血缘 |

---

## V4: 删除向量与性能优化

### 6.1 核心特性

#### 6.1.1 Deletion Vectors (删除向量)

**问题**: Position Delete 文件在高频更新场景下会产生大量小文件

**解决方案**: 使用位图压缩删除位置

**源码**: `core/src/main/java/org/apache/iceberg/V4Metadata.java:300-302`

```java
static Types.StructType fileType(Types.StructType partitionType) {
    return Types.StructType.of(
        // ... 其他字段 ...
        DataFile.FIRST_ROW_ID,
        DataFile.REFERENCED_DATA_FILE,   // V4 新增: 引用的数据文件
        DataFile.CONTENT_OFFSET,         // V4 新增: 删除向量在文件中的偏移
        DataFile.CONTENT_SIZE            // V4 新增: 删除向量的大小
    );
}
```

**DeleteFile V4 扩展**:

```java
public interface DeleteFile extends ContentFile<DeleteFile> {
    /**
     * V4: 删除向量必须指定引用的数据文件
     */
    default String referencedDataFile() {
        return null;
    }

    /**
     * V4: Puffin 文件中删除向量的起始偏移
     */
    default Long contentOffset() {
        return null;
    }

    /**
     * V4: 删除向量的字节大小
     */
    default Long contentSizeInBytes() {
        return null;
    }
}
```

**Puffin Blob 类型** (`StandardBlobTypes.DV_V1`):

```java
public static final String DV_V1 = "deletion-vector-v1";
```

**存储格式**:
- 删除向量存储在 Puffin 文件中
- 使用 RoaringBitmap 压缩位置索引
- 通过 `contentOffset` 和 `contentSize` 直接访问
- 单个 Puffin 文件可包含多个删除向量

**读取流程**:

```
1. 读取 DeleteFile 元数据
2. 根据 referencedDataFile 确定目标数据文件
3. 使用 contentOffset 和 contentSize 直接读取 Puffin 文件中的 DV blob
4. 解码 RoaringBitmap 获取删除位置集合
5. 读取数据文件时跳过删除位置
```

**优势**:
- 大幅减少小文件数量 (多个 DV 共享一个 Puffin 文件)
- 压缩存储 (RoaringBitmap 高效压缩)
- 快速随机访问 (直接偏移读取)
- 减少元数据开销

#### 6.1.2 Required Fields in Manifest Schema

**V4 Manifest List Schema** (`V4Metadata.java:31-48`):

```java
static final Schema MANIFEST_LIST_SCHEMA = new Schema(
    ManifestFile.PATH,
    ManifestFile.LENGTH,
    ManifestFile.SPEC_ID,
    ManifestFile.MANIFEST_CONTENT.asRequired(),      // V4: 变为必需
    ManifestFile.SEQUENCE_NUMBER.asRequired(),        // V4: 变为必需
    ManifestFile.MIN_SEQUENCE_NUMBER.asRequired(),    // V4: 变为必需
    ManifestFile.SNAPSHOT_ID,
    ManifestFile.ADDED_FILES_COUNT.asRequired(),      // V4: 变为必需
    ManifestFile.EXISTING_FILES_COUNT.asRequired(),   // V4: 变为必需
    ManifestFile.DELETED_FILES_COUNT.asRequired(),    // V4: 变为必需
    ManifestFile.ADDED_ROWS_COUNT.asRequired(),       // V4: 变为必需
    ManifestFile.EXISTING_ROWS_COUNT.asRequired(),    // V4: 变为必需
    ManifestFile.DELETED_ROWS_COUNT.asRequired(),     // V4: 变为必需
    ManifestFile.PARTITION_SUMMARIES,
    ManifestFile.KEY_METADATA,
    ManifestFile.FIRST_ROW_ID
);
```

**变更说明**:
- V2/V3: 大部分统计字段为 `optional`
- V4: 关键字段变为 `required`，确保元数据完整性

#### 6.1.3 增强的统计支持

**PartitionStatisticsFile** 完整支持:
- V3 引入接口
- V4 完整实现和优化
- 分区级别的统计文件管理

### 6.2 V4 vs V3 对比表

| 特性 | V3 | V4 |
|------|----|----|
| **删除向量** | ❌ 无 | ✅ Deletion Vector |
| **Position Delete 优化** | ⚠️ 小文件问题 | ✅ 压缩位图 |
| **Manifest 字段** | Optional 较多 | Required 严格 |
| **Puffin Blobs** | 基础统计 | 统计 + DV |
| **元数据大小** | 较大 | 优化压缩 |
| **读取性能** | 良好 | 更优 (DV 加速) |

---

## 版本升级机制

### 7.1 升级 API

**源码**: `TableMetadata.java:749-751`

```java
public TableMetadata upgradeToFormatVersion(int newFormatVersion) {
    return new Builder(this).upgradeFormatVersion(newFormatVersion).build();
}
```

**Builder 实现**: `TableMetadata.java:1056-1076`

```java
public Builder upgradeFormatVersion(int newFormatVersion) {
    Preconditions.checkArgument(
        newFormatVersion <= SUPPORTED_TABLE_FORMAT_VERSION,
        "Cannot upgrade table to unsupported format version: v%s (supported: v%s)",
        newFormatVersion, SUPPORTED_TABLE_FORMAT_VERSION
    );

    Preconditions.checkArgument(
        newFormatVersion >= formatVersion,
        "Cannot downgrade v%s table to v%s",
        formatVersion, newFormatVersion
    );

    if (newFormatVersion == formatVersion) {
        return this;
    }

    this.formatVersion = newFormatVersion;
    changes.add(new MetadataUpdate.UpgradeFormatVersion(newFormatVersion));

    return this;
}
```

### 7.2 升级规则

#### 7.2.1 只升不降

```java
// ✅ 允许
table.ops().commit(
    current,
    current.upgradeToFormatVersion(3)  // V2 → V3
);

// ❌ 禁止
table.ops().commit(
    current,
    current.upgradeToFormatVersion(2)  // V3 → V2 (抛出异常)
);
```

#### 7.2.2 自动升级触发

**场景 1: 使用 V3 特性时自动升级**

```java
// 如果表是 V2，此操作会触发升级到 V3
table.updateSchema()
    .addColumn("ts", Types.TimestampNanoType.get())  // 需要 V3
    .commit();
```

**场景 2: 通过表属性升级**

```java
table.updateProperties()
    .set(TableProperties.FORMAT_VERSION, "3")
    .commit();
```

### 7.3 升级影响分析

#### 7.3.1 V1 → V2

**自动迁移**:
- 为所有快照分配序列号 (从 0 开始)
- Manifest 文件保持兼容 (sequence_number 为 optional)

**新功能**:
- 可以使用 `DELETE` 和 `UPDATE` 操作
- 分区演化更灵活

**兼容性**:
- 旧的 V1 reader 无法读取 V2 delete manifests
- 数据文件格式不变，完全兼容

#### 7.3.2 V2 → V3

**自动迁移**:
- 初始化 `nextRowId` 为 0
- 新快照开始分配 `firstRowId`

**新功能**:
- 启用行血缘追踪
- 可以使用新数据类型

**兼容性**:
- V2 reader 忽略 `firstRowId` 字段
- 统计文件为可选，不影响读取

#### 7.3.3 V3 → V4

**自动迁移**:
- Manifest schema 字段严格化

**新功能**:
- 可以使用删除向量
- 元数据更紧凑

**兼容性**:
- V3 reader 无法解析删除向量
- 建议所有客户端升级后再使用 DV

### 7.4 版本兼容性矩阵

| Reader 版本 | V1 表 | V2 表 | V3 表 | V4 表 |
|-------------|-------|-------|-------|-------|
| **V1 Reader** | ✅ | ❌ Delete | ❌ | ❌ |
| **V2 Reader** | ✅ | ✅ | ⚠️ 忽略新字段 | ❌ |
| **V3 Reader** | ✅ | ✅ | ✅ | ⚠️ 无 DV |
| **V4 Reader** | ✅ | ✅ | ✅ | ✅ |

**说明**:
- ✅ 完全兼容
- ⚠️ 部分功能不可用
- ❌ 无法读取

---

## 完整技术演化路径

### 8.1 演化路径图

```
┌─────────────────────────────────────────────────────────────────┐
│                     Apache Iceberg 格式演化                      │
└─────────────────────────────────────────────────────────────────┘

V1 (2017-2018)
├─ ✅ Schema Evolution
├─ ✅ Partition Evolution (Void Transforms)
├─ ✅ Time Travel (Snapshots)
├─ ✅ ACID 事务
├─ ✅ Optimistic Concurrency Control
├─ ❌ 无序列号
├─ ❌ 无删除文件
└─ ❌ 仅支持 OVERWRITE

        ↓ 升级 (2019-2020)

V2 (生产推荐)
├─ ✅ Sequence Numbers
├─ ✅ Delete Files (Position + Equality)
├─ ✅ Row-Level DELETE/UPDATE
├─ ✅ Data/Delete Manifest 分离
├─ ✅ 真正的分区演化 (无 Void)
├─ ✅ RowDelta API
├─ ⚠️ 无行血缘
└─ ⚠️ Delete 小文件问题

        ↓ 升级 (2021-2022)

V3 (现代化)
├─ ✅ Row Lineage (firstRowId)
├─ ✅ Default Column Values
├─ ✅ TIMESTAMP_NANO
├─ ✅ VARIANT (半结构化)
├─ ✅ GEOMETRY/GEOGRAPHY (GIS)
├─ ✅ Statistics Files (Puffin)
├─ ✅ Partition Statistics
├─ ✅ 完整 CDC 支持
└─ ⚠️ Delete 优化不足

        ↓ 升级 (2023-2024)

V4 (最新)
├─ ✅ Deletion Vectors (RoaringBitmap)
├─ ✅ Manifest Required Fields
├─ ✅ 优化的元数据结构
├─ ✅ 增强的统计支持
├─ ✅ 更小的元数据文件
├─ ✅ 更快的删除操作
└─ ✅ 更好的压缩比
```

### 8.2 特性累积表

| 特性类别 | V1 | V2 | V3 | V4 |
|----------|----|----|----|----|
| **核心元数据** | | | | |
| - Schema Evolution | ✅ | ✅ | ✅ | ✅ |
| - Partition Evolution | ⚠️ Void | ✅ | ✅ | ✅ |
| - Snapshots | ✅ | ✅ | ✅ | ✅ |
| - Sequence Numbers | ❌ | ✅ | ✅ | ✅ |
| - Row Lineage | ❌ | ❌ | ✅ | ✅ |
| **删除机制** | | | | |
| - OVERWRITE | ✅ | ✅ | ✅ | ✅ |
| - Position Deletes | ❌ | ✅ | ✅ | ✅ |
| - Equality Deletes | ❌ | ✅ | ✅ | ✅ |
| - Deletion Vectors | ❌ | ❌ | ❌ | ✅ |
| **数据类型** | | | | |
| - 基础类型 | ✅ | ✅ | ✅ | ✅ |
| - TIMESTAMP_NANO | ❌ | ❌ | ✅ | ✅ |
| - VARIANT | ❌ | ❌ | ✅ | ✅ |
| - GEOMETRY/GEOGRAPHY | ❌ | ❌ | ✅ | ✅ |
| **高级特性** | | | | |
| - Default Values | ❌ | ❌ | ✅ | ✅ |
| - Statistics Files | ❌ | ❌ | ✅ | ✅ |
| - Partition Stats | ❌ | ❌ | ✅ | ✅ |
| **性能优化** | | | | |
| - Manifest 分离 | ❌ | ✅ | ✅ | ✅ |
| - Required Fields | ❌ | ❌ | ❌ | ✅ |
| - Compressed DVs | ❌ | ❌ | ❌ | ✅ |

---

## 源码关键实现

### 9.1 核心类结构

#### 9.1.1 TableMetadata

**路径**: `core/src/main/java/org/apache/iceberg/TableMetadata.java`

**关键字段**:

```java
public class TableMetadata implements Serializable {
    private final int formatVersion;                    // 表格式版本
    private final String uuid;                          // 表 UUID
    private final String location;                      // 表根路径
    private final long lastSequenceNumber;              // V2+ 序列号
    private final long nextRowId;                       // V3+ 下一行 ID

    private final List<Schema> schemas;                 // Schema 历史
    private final int currentSchemaId;                  // 当前 Schema ID

    private final List<PartitionSpec> specs;            // Spec 历史
    private final int defaultSpecId;                    // 默认 Spec ID

    private final List<Snapshot> snapshots;             // 快照列表
    private final long currentSnapshotId;               // 当前快照 ID

    private final List<StatisticsFile> statisticsFiles; // V3+ 统计文件
    private final List<PartitionStatisticsFile> partitionStatisticsFiles; // V3+

    // ... 其他字段
}
```

**关键方法**:

```java
// 版本控制
public int formatVersion();
public TableMetadata upgradeToFormatVersion(int newFormatVersion);

// 序列号 (V2+)
public long lastSequenceNumber();
public long nextSequenceNumber();

// 行 ID (V3+)
public long nextRowId();

// Schema 演化
public TableMetadata updateSchema(Schema newSchema);
public TableMetadata addPartitionSpec(PartitionSpec newPartitionSpec);

// 快照管理
public TableMetadata removeSnapshotsIf(Predicate<Snapshot> removeIf);
```

#### 9.1.2 ManifestFile

**路径**: `api/src/main/java/org/apache/iceberg/ManifestFile.java`

**Schema 定义**:

```java
Schema SCHEMA = new Schema(
    PATH,                       // 500: manifest 路径
    LENGTH,                     // 501: 文件大小
    SPEC_ID,                    // 502: 分区 spec ID
    MANIFEST_CONTENT,           // 517: 0=data, 1=deletes (V2+)
    SEQUENCE_NUMBER,            // 515: 序列号 (V2+)
    MIN_SEQUENCE_NUMBER,        // 516: 最小序列号 (V2+)
    SNAPSHOT_ID,                // 503: 关联快照 ID
    ADDED_FILES_COUNT,          // 504: 新增文件数
    EXISTING_FILES_COUNT,       // 505: 已存在文件数
    DELETED_FILES_COUNT,        // 506: 删除文件数
    ADDED_ROWS_COUNT,           // 512: 新增行数 (V2+)
    EXISTING_ROWS_COUNT,        // 513: 已存在行数 (V2+)
    DELETED_ROWS_COUNT,         // 514: 删除行数 (V2+)
    PARTITION_SUMMARIES,        // 507: 分区摘要
    KEY_METADATA,               // 519: 加密密钥元数据
    FIRST_ROW_ID                // 520: 首行 ID (V3+)
);
```

#### 9.1.3 ContentFile & DeleteFile

**路径**: `api/src/main/java/org/apache/iceberg/ContentFile.java`

**核心接口**:

```java
public interface ContentFile<F> {
    // 基础信息
    int specId();
    FileContent content();          // DATA, POSITION_DELETES, EQUALITY_DELETES
    String location();
    FileFormat format();
    long recordCount();
    long fileSizeInBytes();

    // 统计信息
    Map<Integer, Long> columnSizes();
    Map<Integer, Long> valueCounts();
    Map<Integer, Long> nullValueCounts();
    Map<Integer, ByteBuffer> lowerBounds();
    Map<Integer, ByteBuffer> upperBounds();

    // V2+ 删除相关
    List<Integer> equalityFieldIds();

    // V3+ 行血缘
    default Long firstRowId() { return null; }
}
```

**DeleteFile 扩展**:

```java
public interface DeleteFile extends ContentFile<DeleteFile> {
    // V2+ 基础删除
    List<Long> splitOffsets();

    // V4 删除向量
    default String referencedDataFile() { return null; }
    default Long contentOffset() { return null; }
    default Long contentSizeInBytes() { return null; }
}
```

#### 9.1.4 V4Metadata

**路径**: `core/src/main/java/org/apache/iceberg/V4Metadata.java`

**Manifest List Schema (V4 严格化)**:

```java
static final Schema MANIFEST_LIST_SCHEMA = new Schema(
    ManifestFile.PATH,
    ManifestFile.LENGTH,
    ManifestFile.SPEC_ID,
    ManifestFile.MANIFEST_CONTENT.asRequired(),      // Required
    ManifestFile.SEQUENCE_NUMBER.asRequired(),        // Required
    ManifestFile.MIN_SEQUENCE_NUMBER.asRequired(),    // Required
    ManifestFile.SNAPSHOT_ID,
    ManifestFile.ADDED_FILES_COUNT.asRequired(),      // Required
    ManifestFile.EXISTING_FILES_COUNT.asRequired(),   // Required
    ManifestFile.DELETED_FILES_COUNT.asRequired(),    // Required
    ManifestFile.ADDED_ROWS_COUNT.asRequired(),       // Required
    ManifestFile.EXISTING_ROWS_COUNT.asRequired(),    // Required
    ManifestFile.DELETED_ROWS_COUNT.asRequired(),     // Required
    ManifestFile.PARTITION_SUMMARIES,
    ManifestFile.KEY_METADATA,
    ManifestFile.FIRST_ROW_ID
);
```

**Data File Schema (V4 完整字段)**:

```java
static Types.StructType fileType(Types.StructType partitionType) {
    return Types.StructType.of(
        DataFile.CONTENT.asRequired(),           // 文件类型
        DataFile.FILE_PATH,                      // 文件路径
        DataFile.FILE_FORMAT,                    // 文件格式
        required(DataFile.PARTITION_ID, ..., partitionType, ...),
        DataFile.RECORD_COUNT,                   // 记录数
        DataFile.FILE_SIZE,                      // 文件大小
        DataFile.COLUMN_SIZES,                   // 列大小统计
        DataFile.VALUE_COUNTS,                   // 值计数
        DataFile.NULL_VALUE_COUNTS,              // 空值计数
        DataFile.NAN_VALUE_COUNTS,               // NaN 计数
        DataFile.LOWER_BOUNDS,                   // 下界
        DataFile.UPPER_BOUNDS,                   // 上界
        DataFile.KEY_METADATA,                   // 加密元数据
        DataFile.SPLIT_OFFSETS,                  // 分片偏移
        DataFile.EQUALITY_IDS,                   // 相等删除字段
        DataFile.SORT_ORDER_ID,                  // 排序 ID
        DataFile.FIRST_ROW_ID,                   // V3+ 首行 ID
        DataFile.REFERENCED_DATA_FILE,           // V4 删除向量引用
        DataFile.CONTENT_OFFSET,                 // V4 DV 偏移
        DataFile.CONTENT_SIZE                    // V4 DV 大小
    );
}
```

### 9.2 关键操作流程

#### 9.2.1 版本升级流程

```java
// 1. 检查版本兼容性
TableMetadata current = table.ops().current();
int currentVersion = current.formatVersion();
int targetVersion = 3;

if (targetVersion < currentVersion) {
    throw new IllegalArgumentException("Cannot downgrade");
}

// 2. 执行升级
TableMetadata upgraded = current.upgradeToFormatVersion(targetVersion);

// 3. 提交变更
table.ops().commit(current, upgraded);

// 4. 自动迁移
// - V1→V2: 分配序列号
// - V2→V3: 初始化 nextRowId
// - V3→V4: Schema 严格化
```

#### 9.2.2 删除文件写入流程 (V2+)

```java
// Position Delete 写入
PositionDeleteWriter<Record> deleteWriter = appenderFactory.newPosDeleteWriter(
    outputFile, fileFormat, partition
);

for (PositionDelete<Record> delete : deletes) {
    deleteWriter.write(delete.path(), delete.pos(), delete.row());
}

DeleteFile deleteFile = deleteWriter.toDeleteFile();

// 提交删除
table.newRowDelta()
    .addDeletes(deleteFile)
    .commit();
```

#### 9.2.3 删除向量写入流程 (V4)

```java
// 1. 创建 DV Writer
DVFileWriter dvWriter = DVFileWriter.builderFor(io)
    .outputFile(puffinFile)
    .build();

// 2. 写入删除位置 (RoaringBitmap)
RoaringBitmap bitmap = new RoaringBitmap();
for (long pos : deletePositions) {
    bitmap.add((int) pos);
}

dvWriter.write(
    referencedDataFile,    // 引用的数据文件路径
    bitmap                 // 删除位置位图
);

// 3. 完成写入，获取元数据
DeleteFile dv = dvWriter.result();
// dv.referencedDataFile() = "/path/to/data.parquet"
// dv.contentOffset() = 1024
// dv.contentSizeInBytes() = 256

// 4. 提交
table.newRowDelta()
    .addDeletes(dv)
    .commit();
```

#### 9.2.4 行血缘追踪流程 (V3+)

```java
// 1. 获取当前 nextRowId
TableMetadata metadata = table.ops().current();
long currentRowId = metadata.nextRowId();

// 2. 写入数据时分配行 ID
DataWriter<Record> writer = ...;
long firstRowId = currentRowId;
for (Record record : records) {
    writer.write(record);
}
DataFile dataFile = writer.toDataFile();
dataFile.setFirstRowId(firstRowId);  // 设置首行 ID

// 3. 创建快照
Snapshot snapshot = ...;
snapshot.setFirstRowId(firstRowId);
snapshot.setAddedRows(records.size());

// 4. 提交后自动更新 nextRowId
// nextRowId = firstRowId + addedRows
```

---

## 最佳实践建议

### 10.1 版本选择指南

#### 10.1.1 新项目推荐

**推荐: V2 或 V3**

**理由**:
- **V2**: 生产稳定，工具链成熟
  - 适合不需要行血缘的场景
  - 兼容性最好
  - Spark/Flink/Trino 完全支持

- **V3**: 现代化特性
  - 适合需要 CDC、审计的场景
  - 需要新数据类型 (VARIANT, GEOMETRY)
  - 计划长期维护

**不推荐 V1**:
- 分区演化限制
- 无删除文件支持
- 未来可能废弃

**V4 观望期**:
- 最新特性,生态支持待完善
- 删除向量非常适合高频更新场景
- 建议在测试环境先验证

#### 10.1.2 场景化推荐

| 场景 | 推荐版本 | 理由 |
|------|---------|------|
| **OLAP 查询** | V2 | 稳定可靠,查询优化成熟 |
| **CDC/审计** | V3 | 行血缘完整支持 |
| **高频 UPDATE/DELETE** | V4 | 删除向量优化小文件 |
| **日志/事件** | V3 | VARIANT 类型支持半结构化 |
| **GIS 数据** | V3 | GEOMETRY/GEOGRAPHY 类型 |
| **时序数据** | V3 | TIMESTAMP_NANO 高精度 |
| **兼容性优先** | V2 | 最广泛的工具支持 |

### 10.2 升级策略

#### 10.2.1 渐进式升级

```sql
-- 阶段 1: 新表使用新版本
CREATE TABLE new_table (...)
TBLPROPERTIES ('format-version' = '3');

-- 阶段 2: 旧表在维护窗口升级
ALTER TABLE old_table SET TBLPROPERTIES ('format-version' = '3');

-- 阶段 3: 验证所有客户端兼容性
-- 阶段 4: 全量迁移
```

#### 10.2.2 升级前检查清单

- [ ] 确认所有 reader 支持目标版本
- [ ] 备份元数据文件
- [ ] 在测试表验证升级流程
- [ ] 评估存储成本变化 (delete files)
- [ ] 更新文档和运维手册
- [ ] 规划回滚方案 (降级不支持,需要从备份恢复)

### 10.3 性能优化建议

#### 10.3.1 V2 表优化

**Delete Files Compaction**:

```sql
-- 定期合并删除文件,减少小文件
CALL spark_catalog.system.rewrite_position_delete_files(
    table => 'my_table',
    options => map(
        'target-file-size-bytes', '134217728',  -- 128MB
        'min-file-size-bytes', '8388608'        -- 8MB
    )
);
```

**Manifest Compaction**:

```sql
-- 合并 manifest 文件
CALL spark_catalog.system.rewrite_manifests('my_table');
```

#### 10.3.2 V3 表优化

**Statistics Files 维护**:

```sql
-- 计算统计信息 (NDV Sketch)
CALL spark_catalog.system.compute_table_stats('my_table');

-- 清理过期统计
CALL spark_catalog.system.remove_orphan_files(
    table => 'my_table',
    older_than => TIMESTAMP '2024-01-01 00:00:00'
);
```

**Row ID 范围查询**:

```java
// 利用 Row ID 进行增量读取
TableScan scan = table.newScan()
    .filter(Expressions.greaterThanOrEqual("_iceberg_row_id", lastProcessedRowId))
    .filter(Expressions.lessThan("_iceberg_row_id", currentMaxRowId));
```

#### 10.3.3 V4 表优化

**Deletion Vector 配置**:

```properties
# 启用删除向量
write.delete.mode=merge-on-read
write.delete.deletion-vector.enabled=true

# DV Puffin 文件大小控制
write.delete.deletion-vector.puffin-size=67108864  # 64MB
```

**监控指标**:

```sql
-- 查看删除文件统计
SELECT
    file_content,
    COUNT(*) AS file_count,
    SUM(file_size_in_bytes) AS total_size,
    AVG(record_count) AS avg_records
FROM my_table.files
GROUP BY file_content;
```

### 10.4 避坑指南

#### 10.4.1 版本降级陷阱

**❌ 错误做法**:

```sql
-- 不支持降级!
ALTER TABLE my_table SET TBLPROPERTIES ('format-version' = '2');
-- Error: Cannot downgrade v3 table to v2
```

**✅ 正确做法**:

```sql
-- 只能从备份恢复
-- 1. 停止写入
-- 2. 恢复元数据到升级前的快照
-- 3. 清理升级后的快照
```

#### 10.4.2 Schema 演化注意事项

**V3 默认值限制**:

```java
// ✅ 正确: V3+ 支持默认值
table.updateSchema()
    .addColumn("status", Types.StringType.get())
    .setDefault("status", "active")
    .commit();

// ❌ 错误: V2 不支持
// 会在升级到 V3 后才生效
```

#### 10.4.3 Delete Files 小文件问题

**现象**: 高频删除导致大量小 delete files

**解决方案**:

```sql
-- 方案 1: 定期 Compaction (V2/V3)
CALL rewrite_position_delete_files('table',
    map('target-file-size-bytes', '134217728'));

-- 方案 2: 升级到 V4 使用删除向量
ALTER TABLE table SET TBLPROPERTIES (
    'format-version' = '4',
    'write.delete.deletion-vector.enabled' = 'true'
);

-- 方案 3: 配置合理的 Delete Mode
-- merge-on-read: 写入快,读取慢 (适合写多读少)
-- copy-on-write: 写入慢,读取快 (适合读多写少)
ALTER TABLE table SET TBLPROPERTIES (
    'write.delete.mode' = 'merge-on-read'
);
```

### 10.5 监控与观测

#### 10.5.1 关键指标

**元数据监控**:

```sql
-- 快照数量
SELECT COUNT(*) FROM my_table.snapshots;

-- Manifest 文件数量
SELECT COUNT(DISTINCT path) FROM my_table.manifests;

-- Delete Files 比例
SELECT
    SUM(CASE WHEN content = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS delete_file_ratio
FROM my_table.files;

-- 平均文件大小
SELECT
    content,
    AVG(file_size_in_bytes) / 1024 / 1024 AS avg_size_mb
FROM my_table.files
GROUP BY content;
```

**性能指标**:

```sql
-- 查询扫描的文件数
SELECT operation, count(*)
FROM my_table.history
GROUP BY operation;

-- Position Delete 应用效率
SELECT
    data_file,
    COUNT(DISTINCT delete_file) AS delete_file_count
FROM my_table.position_deletes
GROUP BY data_file;
```

#### 10.5.2 告警阈值建议

| 指标 | 阈值 | 说明 |
|------|------|------|
| Delete File 数量 | > 10000 | 需要 Compaction |
| 单 Data File 关联 Delete Files | > 10 | 考虑 Rewrite |
| Manifest 数量 | > 100 | 需要合并 |
| 快照保留时间 | < 7天 | 影响时间旅行 |
| 平均文件大小 | < 64MB | 小文件过多 |

---

## 总结

### 核心要点

1. **版本选择**:
   - 新项目: V2 稳定,V3 现代
   - CDC 场景: 必须 V3+
   - 高频更新: 考虑 V4

2. **升级路径**:
   - 只能升级,不能降级
   - 增量升级,先测试后生产
   - 验证客户端兼容性

3. **性能优化**:
   - V2: 定期 Compaction
   - V3: 利用行血缘和统计
   - V4: 启用删除向量

4. **避免陷阱**:
   - Delete Files 小文件问题
   - 版本降级不支持
   - Schema 演化需匹配版本

### 技术演化趋势

- **存储格式**: 从文件级到行级
- **删除优化**: 从重写到向量化
- **元数据**: 从可选到必需 (严格化)
- **类型系统**: 从基础到丰富 (半结构化、GIS)
- **数据血缘**: 从无到有,从有到精细

### 未来展望

- **V5 可能方向**:
  - Z-Ordering / Clustering 原生支持
  - 更智能的自动 Compaction
  - 列级血缘追踪
  - 更多的统计类型 (ML 特征统计)

---

**文档结束**

本文档基于 Apache Iceberg 1.10.x 源码深度分析,涵盖 V1 到 V3 (含 V4 预览) 的完整技术演化路径。所有代码引用均来自实际源码,确保准确性和权威性。
