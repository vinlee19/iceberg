# Apache Iceberg v3 设计文档

## 概述

Apache Iceberg v3 是 Iceberg 表格式的最新版本，引入了扩展数据类型、增强的元数据能力和新的表操作功能。v3 版本在保持向后兼容性的同时，为分析型工作负载提供了更强大的功能支持。

## 版本演进

### v1 → v2 → v3 演进路径

- **v1 (分析型数据表)**: 基础表格式，支持不可变文件格式（Parquet、Avro、ORC）
- **v2 (行级删除)**: 引入删除文件支持行级更新和删除操作
- **v3 (扩展类型和能力)**: 新增数据类型、默认值支持、多参数转换、行溯源跟踪等

## v3 核心特性

### 1. 新增数据类型

v3 版本扩展了 Iceberg 的类型系统，支持更多现代分析场景：

```java
// 新增的数据类型
- nanosecond timestamp(tz): 纳秒级时间戳（带时区）
- unknown: 未知类型
- variant: 变体类型（类似 JSON）
- geometry: 几何类型
- geography: 地理类型
```

### 2. 列默认值支持

v3 引入了对列默认值的完整支持：

```java
// Schema 字段支持默认值
{
  "id": 1,
  "name": "x",
  "required": true,
  "type": "long",
  "initial-default": 1,    // 初始默认值
  "write-default": 1       // 写入默认值
}
```

**特性说明**:
- `initial-default`: 为现有行提供默认值
- `write-default`: 为新插入行提供默认值
- 支持复杂类型的默认值设置

### 3. 行溯源跟踪 (Row Lineage)

v3 版本引入了行级数据溯源功能：

**Row ID 系统**:
```java
// 每个数据文件都有 first-row-id
DataFile {
  ...
  Long firstRowId();     // 文件中第一行的 row ID
  ...
}

// 表级别维护 next-row-id
TableMetadata {
  ...
  "next-row-id": 12345   // 下一个分配的 row ID
  ...
}
```

**Row ID 分配机制**:
- 每个新创建的数据文件获得连续的 row ID 范围
- `first-row-id` 标识文件中第一行的 ID
- 行 ID 在表的整个生命周期中保持唯一且递增
- 支持跨快照的行级数据溯源

### 4. 增强的删除文件支持

v3 版本扩展了删除文件的元数据结构：

```java
// 删除文件新增字段
DeleteFile {
  ...
  String referencedDataFile();     // 引用的数据文件路径
  Long contentOffset();            // 内容在文件中的偏移量
  Long contentSizeInBytes();       // 内容大小（字节）
  ...
}
```

**应用场景**:
- 精确定位删除操作影响的数据范围
- 优化删除文件的读取性能
- 支持部分文件内容的删除操作

### 5. 二进制删除向量 (Binary Deletion Vectors)

v3 引入了高效的删除向量机制：

- 使用位图表示删除的行
- 显著减少删除文件的存储开销
- 提高删除操作的查询性能
- 支持批量删除操作的优化

## 元数据架构变化

### 1. V3Metadata 类结构

```java
class V3Metadata {
  // v3 专用的 manifest list schema
  static final Schema MANIFEST_LIST_SCHEMA = new Schema(
    ManifestFile.PATH,
    ManifestFile.LENGTH,
    ManifestFile.SPEC_ID,
    ManifestFile.MANIFEST_CONTENT.asRequired(),
    ManifestFile.SEQUENCE_NUMBER.asRequired(),
    ManifestFile.MIN_SEQUENCE_NUMBER.asRequired(),
    ManifestFile.SNAPSHOT_ID,
    ManifestFile.ADDED_FILES_COUNT.asRequired(),
    ManifestFile.EXISTING_FILES_COUNT.asRequired(),
    ManifestFile.DELETED_FILES_COUNT.asRequired(),
    ManifestFile.ADDED_ROWS_COUNT.asRequired(),
    ManifestFile.EXISTING_ROWS_COUNT.asRequired(),
    ManifestFile.DELETED_ROWS_COUNT.asRequired(),
    ManifestFile.PARTITION_SUMMARIES,
    ManifestFile.KEY_METADATA,
    ManifestFile.FIRST_ROW_ID        // v3 新增
  );
}
```

### 2. 文件结构扩展

**数据文件结构**:
```java
// v3 数据文件增加的字段
DataFile {
  FileContent content;               // 文件内容类型
  String location;                   // 文件位置
  FileFormat format;                 // 文件格式
  StructLike partition;              // 分区信息
  long recordCount;                  // 记录数
  long fileSizeInBytes;              // 文件大小
  Map<Integer, Long> columnSizes;    // 列大小统计
  Map<Integer, Long> valueCounts;    // 值计数统计
  Map<Integer, Long> nullValueCounts; // 空值计数
  Map<Integer, Long> nanValueCounts;  // NaN值计数
  Map<Integer, ByteBuffer> lowerBounds; // 下界值
  Map<Integer, ByteBuffer> upperBounds; // 上界值
  ByteBuffer keyMetadata;            // 键元数据
  List<Long> splitOffsets;           // 分割偏移
  List<Integer> equalityFieldIds;    // 相等性字段ID
  Integer sortOrderId;               // 排序顺序ID
  Long firstRowId;                   // v3: 首行ID
  String referencedDataFile;         // v3: 引用的数据文件
  Long contentOffset;                // v3: 内容偏移
  Long contentSizeInBytes;           // v3: 内容大小
}
```

### 3. 兼容性包装器

v3 提供了向前兼容的包装器机制：

```java
// ManifestFileWrapper 处理 v3 特有字段
static class ManifestFileWrapper implements ManifestFile, StructLike {
  private final long commitSnapshotId;
  private final long sequenceNumber;
  private ManifestFile wrapped;
  private Long wrappedFirstRowId;  // v3 特有字段包装
  
  // 处理 first-row-id 的分配和继承逻辑
  private Object get(int pos) {
    switch (pos) {
      case 15:  // FIRST_ROW_ID 位置
        if (wrappedFirstRowId != null) {
          return wrappedFirstRowId;
        } else if (wrapped.content() != ManifestContent.DATA) {
          return null;
        } else {
          return wrapped.firstRowId();
        }
      // ... 其他字段处理
    }
  }
}
```

## 表操作增强

### 1. 分区统计文件

v3 支持更细粒度的分区级统计信息：

```java
// 分区统计文件结构
PartitionStatisticsFile {
  Long snapshotId;                   // 快照ID
  String statisticsPath;             // 统计文件路径
  Long fileSizeInBytes;              // 文件大小
}
```

### 2. 多参数转换函数

v3 扩展了分区和排序的转换能力：

- 支持多字段组合的分区转换
- 更灵活的数据分布策略
- 增强的查询优化能力

### 3. 统计文件管理

```java
// 表级统计文件管理
StatisticsFile {
  Long snapshotId;                   // 关联快照
  String statisticsPath;             // 统计文件路径
  Long fileSizeInBytes;              // 文件大小
  Long footerSizeInBytes;            // 文件尾大小
  Map<String, String> keyMetadata;   // 键值元数据
}
```

## 升级和迁移机制

### 1. 表格式版本升级

```java
// 通过表属性控制格式版本
TableProperties.FORMAT_VERSION = "3"

// 升级触发条件
table.updateProperties()
  .set(TableProperties.FORMAT_VERSION, "3")
  .commit();
```

### 2. 向后兼容性保证

**读取兼容性**:
- v3 表可以被较旧的引擎读取（忽略新字段）
- 新字段设计为可选，不影响基本功能
- 渐进式功能启用，不强制升级

**写入兼容性**:
- 升级到 v3 后，新功能逐步启用
- Row ID 分配仅对新数据生效
- 现有数据保持不变，避免大规模重写

### 3. Row ID 初始化策略

```java
// v3 升级时的 Row ID 初始化
When a table is upgraded to v3:
1. next-row-id 初始化为 0
2. 现有快照保持不变（first-row-id 为 null）
3. 首次提交时为现有数据文件分配 Row ID
4. 新数据文件自动获得连续的 Row ID
```

## 性能优化

### 1. 元数据缓存优化

- 分区统计信息缓存
- 删除文件索引优化
- manifest 文件重用机制

### 2. 查询规划优化

- 基于 Row ID 的快速定位
- 删除向量的位图操作优化
- 分区级统计的查询过滤

### 3. 存储优化

- 删除向量的压缩存储
- 统计文件的增量更新
- 元数据文件的批量操作

## 实现细节

### 1. 关键类和接口

```java
// 核心实现类
org.apache.iceberg.V3Metadata              // v3 元数据处理
org.apache.iceberg.ManifestWriter          // manifest 写入器
org.apache.iceberg.ManifestListWriter      // manifest list 写入器
org.apache.iceberg.TableMetadata           // 表元数据
org.apache.iceberg.PartitionStatisticsFile // 分区统计文件
```

### 2. 配置属性

```java
// v3 相关的表属性
TableProperties.FORMAT_VERSION              // 格式版本控制
TableProperties.DEFAULT_WRITE_METRICS_MODE  // 指标写入模式
// 其他 v3 特定配置...
```

### 3. 引擎集成

**Spark 集成**:
- 支持 Spark 3.4+ 版本
- Row ID 列的自动暴露
- 删除向量的原生支持

**Flink 集成**:
- 流式处理中的 Row ID 分配
- 增量删除操作支持

## 使用场景

### 1. 数据溯源跟踪

```sql
-- 使用 Row ID 进行数据溯源
SELECT _row_id, data_columns, _file_path, _pos
FROM table_name
WHERE condition;
```

### 2. 高效删除操作

```sql
-- 基于 Row ID 的精确删除
DELETE FROM table_name 
WHERE _row_id IN (select_row_ids);
```

### 3. 变体数据处理

```sql
-- 处理半结构化数据
SELECT variant_column:property::string
FROM table_name
WHERE variant_column:type = 'specific_type';
```

## 最佳实践

### 1. 升级策略

1. **渐进式升级**: 先升级元数据格式，后启用新功能
2. **测试验证**: 在非生产环境充分测试兼容性
3. **监控性能**: 关注升级后的查询性能变化

### 2. Row ID 使用

1. **避免依赖**: 不要在应用逻辑中硬编码 Row ID 值
2. **性能考虑**: Row ID 查询通常比全表扫描更高效
3. **一致性保证**: Row ID 在表的生命周期中保持稳定

### 3. 删除操作优化

1. **批量删除**: 使用删除向量进行批量操作
2. **分区对齐**: 删除操作与分区边界对齐时性能更佳
3. **压缩策略**: 定期压缩以清理删除标记

## 限制和注意事项

### 1. 当前限制

- **开发状态**: v3 仍在积极开发中，未正式发布
- **引擎支持**: 不是所有计算引擎都完全支持 v3 功能
- **向前兼容**: 某些新功能可能不被旧版本引擎识别

### 2. 性能考虑

- Row ID 分配可能影响写入性能
- 复杂的删除向量操作需要额外的计算资源
- 统计文件维护增加存储开销

### 3. 运维考虑

- 需要更新监控和管理工具以支持新的元数据结构
- 备份和恢复策略需要考虑新的文件类型
- 跨版本的兼容性测试变得更加重要

## 总结

Apache Iceberg v3 通过引入扩展数据类型、行溯源跟踪、增强的删除支持等功能，为现代数据湖分析工作负载提供了更强大的能力。虽然仍在开发中，但其设计理念和架构已经展现出对未来数据处理需求的前瞻性思考。

v3 的设计强调兼容性和渐进式采用，使得用户可以在不破坏现有工作负载的前提下，逐步利用新功能带来的优势。随着开发的完成和社区的采用，v3 将成为企业级数据湖架构的重要基石。