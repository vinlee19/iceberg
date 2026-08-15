# Doris Iceberg ExpireSnapshots 返回结果格式说明

**生成日期：** 2025-11-15
**对齐标准：** Apache Spark ExpireSnapshots 输出格式

---

## 返回结果 Schema

### 完整输出列定义

| 列名 | 类型 | 说明 |
|------|------|------|
| `deleted_data_files_count` | BIGINT | Number of data files deleted by this operation |
| `deleted_position_delete_files_count` | BIGINT | Number of position delete files deleted by this operation |
| `deleted_equality_delete_files_count` | BIGINT | Number of equality delete files deleted by this operation |
| `deleted_manifest_files_count` | BIGINT | Number of manifest files deleted by this operation |
| `deleted_manifest_lists_count` | BIGINT | Number of manifest list files deleted by this operation |
| `deleted_statistics_files_count` | BIGINT | Number of statistics files deleted by this operation |

---

## 文件分类逻辑

### 1. Data Files (数据文件)
**识别规则：**
```
文件路径以 .parquet, .orc, 或 .avro 结尾
且不在 /metadata/ 目录下
```

**示例：**
```
s3://bucket/warehouse/db/table/data/part=2025-01-01/00000-0-data-00001.parquet
hdfs://namenode/warehouse/db/table/data/00001-1-data-00002.orc
```

**计数器：** `deletedDataFilesCount`

---

### 2. Position Delete Files (位置删除文件)
**识别规则：**
```
文件路径包含以下任一模式：
- 'pos-delete'
- 'position-delete'
- '-deletes' 或 'delete-' (默认归类为 position delete)
```

**示例：**
```
s3://bucket/warehouse/db/table/data/part=2025-01-01/00000-0-pos-deletes-00001.parquet
hdfs://namenode/warehouse/db/table/data/00001-1-position-delete-00002.parquet
```

**计数器：** `deletedPositionDeleteFilesCount`

**说明：**
- Position Delete 文件记录要删除的行的物理位置（文件路径 + 行号）
- 用于 DELETE 和 UPDATE 操作（Copy-on-Write 模式）

---

### 3. Equality Delete Files (等值删除文件)
**识别规则：**
```
文件路径包含以下任一模式：
- 'eq-delete'
- 'equality-delete'
```

**示例：**
```
s3://bucket/warehouse/db/table/data/part=2025-01-01/00000-0-eq-deletes-00001.parquet
hdfs://namenode/warehouse/db/table/data/00001-1-equality-delete-00002.parquet
```

**计数器：** `deletedEqualityDeleteFilesCount`

**说明：**
- Equality Delete 文件记录要删除的行的列值（基于主键或指定列）
- 用于 DELETE 和 MERGE 操作（Merge-on-Read 模式）

---

### 4. Manifest Files (清单文件)
**识别规则：**
```
文件路径满足以下条件：
1. 包含 '/metadata/'
2. 包含 '-m'
3. 以 '.avro' 结尾
```

**示例：**
```
s3://bucket/warehouse/db/table/metadata/12345678-1234-1234-1234-123456789abc-m0.avro
s3://bucket/warehouse/db/table/metadata/87654321-4321-4321-4321-cba987654321-m1.avro
```

**计数器：** `deletedManifestFilesCount`

**文件命名规则：**
```
<uuid>-m<sequence>.avro
```

**说明：**
- Manifest 文件记录一组数据文件的元数据
- 每个 Manifest 包含多个 Data File 的路径、大小、行数、分区信息等

---

### 5. Manifest List Files (清单列表文件)
**识别规则：**
```
文件路径满足以下条件：
1. 包含 '/metadata/'
2. 包含 'snap-'
3. 以 '.avro' 结尾
```

**示例：**
```
s3://bucket/warehouse/db/table/metadata/snap-1234567890-1-abc123.avro
s3://bucket/warehouse/db/table/metadata/snap-9876543210-0-def456.avro
```

**计数器：** `deletedManifestListsCount`

**文件命名规则：**
```
snap-<snapshot-id>-<attempt>-<hash>.avro
```

**说明：**
- Manifest List 文件是快照的顶层元数据文件
- 每个快照对应一个 Manifest List
- Manifest List 记录该快照包含的所有 Manifest 文件

---

### 6. Statistics Files (统计信息文件)
**识别规则：**
```
文件路径满足以下条件：
1. 包含 '/metadata/'
2. 以 '.stats' 结尾
```

**示例：**
```
s3://bucket/warehouse/db/table/metadata/12345678-1234-1234-1234-123456789abc.stats
hdfs://namenode/warehouse/db/table/metadata/87654321-4321-4321-4321-cba987654321.stats
```

**计数器：** `deletedStatisticsFilesCount`

**说明：**
- Statistics 文件存储表的统计信息（列级统计、NDV、Min/Max 等）
- 用于查询优化（谓词下推、分区裁剪等）
- Iceberg V2 表支持 Puffin 格式的统计文件

---

## 文件分类流程图

```
收到待删除文件路径
    ↓
是否包含 '/metadata/' ?
    ├─ No → 检查文件扩展名
    │          ├─ .parquet/.orc/.avro → Data File
    │          └─ 其他 → 忽略
    │
    └─ Yes → 继续分类
           ↓
       是否包含 '-m' 且以 '.avro' 结尾?
           ├─ Yes → Manifest File
           │
           └─ No → 是否包含 'snap-' 且以 '.avro' 结尾?
                  ├─ Yes → Manifest List File
                  │
                  └─ No → 是否以 '.stats' 结尾?
                         ├─ Yes → Statistics File
                         │
                         └─ No → 是否包含 delete 相关模式?
                                ├─ 包含 'pos-delete' → Position Delete File
                                ├─ 包含 'eq-delete' → Equality Delete File
                                └─ 包含 '-deletes' → Position Delete File (默认)
```

---

## 使用示例

### SQL 调用

```sql
CALL iceberg_execute_action(
    'expire_snapshots',
    'catalog.db.table',
    'older_than' = '2025-01-01 00:00:00',
    'retain_last' = '5'
);
```

### 返回结果示例

```
+---------------------------+------------------------------------+------------------------------------+------------------------------+--------------------------------+----------------------------------+
| deleted_data_files_count  | deleted_position_delete_files_count| deleted_equality_delete_files_count| deleted_manifest_files_count | deleted_manifest_lists_count   | deleted_statistics_files_count   |
+---------------------------+------------------------------------+------------------------------------+------------------------------+--------------------------------+----------------------------------+
| 1250                      | 85                                 | 42                                 | 150                          | 45                             | 12                               |
+---------------------------+------------------------------------+------------------------------------+------------------------------+--------------------------------+----------------------------------+
```

### 结果解读

**场景：** 过期了 45 个快照

| 文件类型 | 数量 | 说明 |
|---------|------|------|
| Data Files | 1,250 | 删除了 1,250 个数据文件（.parquet/.orc/.avro） |
| Position Delete Files | 85 | 删除了 85 个位置删除文件 |
| Equality Delete Files | 42 | 删除了 42 个等值删除文件 |
| Manifest Files | 150 | 删除了 150 个清单文件 |
| Manifest Lists | 45 | 删除了 45 个清单列表文件（对应 45 个快照） |
| Statistics Files | 12 | 删除了 12 个统计信息文件 |

**总删除文件数：** 1,584 个文件

---

## 实现代码片段

### 文件分类逻辑

```java
expireSnapshots = expireSnapshots.deleteWith(file -> {
    // Classify files based on path patterns
    if (file.contains("/metadata/") && file.contains("-m") && file.endsWith(".avro")) {
        // Manifest files: metadata/<uuid>-m<number>.avro
        deletedManifestFilesCount.incrementAndGet();
    } else if (file.contains("/metadata/snap-") && file.endsWith(".avro")) {
        // Manifest list files: metadata/snap-<snapshot-id>-<attempt>.avro
        deletedManifestListsCount.incrementAndGet();
    } else if (file.contains("/metadata/") && file.endsWith(".stats")) {
        // Statistics files: metadata/<uuid>.stats
        deletedStatisticsFilesCount.incrementAndGet();
    } else if (file.contains("-deletes") || file.contains("delete-")) {
        // Delete files (position delete or equality delete)
        if (file.contains("pos-delete") || file.contains("position-delete")) {
            deletedPositionDeleteFilesCount.incrementAndGet();
        } else if (file.contains("eq-delete") || file.contains("equality-delete")) {
            deletedEqualityDeleteFilesCount.incrementAndGet();
        } else {
            // Default to position delete for generic delete files
            deletedPositionDeleteFilesCount.incrementAndGet();
        }
    } else if (file.endsWith(".parquet") || file.endsWith(".orc") || file.endsWith(".avro")) {
        // Data files
        deletedDataFilesCount.incrementAndGet();
    }
    // Actually delete the file
    icebergTable.io().deleteFile(file);
});
```

### 返回结果构造

```java
return Lists.newArrayList(
    String.valueOf(deletedDataFilesCount.get()),
    String.valueOf(deletedPositionDeleteFilesCount.get()),
    String.valueOf(deletedEqualityDeleteFilesCount.get()),
    String.valueOf(deletedManifestFilesCount.get()),
    String.valueOf(deletedManifestListsCount.get()),
    String.valueOf(deletedStatisticsFilesCount.get())
);
```

### Schema 定义

```java
@Override
protected List<Column> getResultSchema() {
    return Lists.newArrayList(
        new Column("deleted_data_files_count", Type.BIGINT, false,
                "Number of data files deleted by this operation"),
        new Column("deleted_position_delete_files_count", Type.BIGINT, false,
                "Number of position delete files deleted by this operation"),
        new Column("deleted_equality_delete_files_count", Type.BIGINT, false,
                "Number of equality delete files deleted by this operation"),
        new Column("deleted_manifest_files_count", Type.BIGINT, false,
                "Number of manifest files deleted by this operation"),
        new Column("deleted_manifest_lists_count", Type.BIGINT, false,
                "Number of manifest list files deleted by this operation"),
        new Column("deleted_statistics_files_count", Type.BIGINT, false,
                "Number of statistics files deleted by this operation")
    );
}
```

---

## 与 Spark 实现对比

### Spark ExpireSnapshots 输出

```scala
// Spark ExpireSnapshotsSparkAction.java
Dataset<Row> result = spark.createDataFrame(
    Lists.newArrayList(
        RowFactory.create(
            deletedDataFilesCount,
            deletedPositionDeleteFilesCount,
            deletedEqualityDeleteFilesCount,
            deletedManifestFilesCount,
            deletedManifestListsCount,
            deletedStatisticsFilesCount
        )
    ),
    schema
);
```

### Doris 实现

```java
// Doris IcebergExpireSnapshotsAction.java
return Lists.newArrayList(
    String.valueOf(deletedDataFilesCount.get()),
    String.valueOf(deletedPositionDeleteFilesCount.get()),
    String.valueOf(deletedEqualityDeleteFilesCount.get()),
    String.valueOf(deletedManifestFilesCount.get()),
    String.valueOf(deletedManifestListsCount.get()),
    String.valueOf(deletedStatisticsFilesCount.get())
);
```

### 对比总结

| 维度 | Spark | Doris | 差异 |
|------|-------|-------|------|
| **列数量** | 6 | 6 | ✅ 完全一致 |
| **列名** | 完全相同 | 完全相同 | ✅ 完全一致 |
| **列类型** | long | BIGINT | ✅ 语义一致 |
| **列描述** | 完全相同 | 完全相同 | ✅ 完全一致 |
| **返回格式** | Dataset<Row> | List<String> | ⚠️ 框架差异（不影响内容）|

**结论：** Doris 实现的返回结果格式与 Spark 完全对齐！

---

## Iceberg 文件层次结构

```
Table
  ↓
Snapshot (当前快照)
  ↓
Manifest List (snap-<id>.avro)
  ├─ Manifest 1 (<uuid>-m0.avro)
  │    ├─ Data File 1.parquet
  │    ├─ Data File 2.parquet
  │    ├─ Position Delete File 1.parquet
  │    └─ Equality Delete File 1.parquet
  │
  ├─ Manifest 2 (<uuid>-m1.avro)
  │    ├─ Data File 3.parquet
  │    └─ Data File 4.parquet
  │
  └─ Statistics File (<uuid>.stats)
```

**ExpireSnapshots 删除逻辑：**
1. 计算要过期的快照
2. 从过期快照的 Manifest List 开始遍历
3. 读取每个 Manifest，获取 Data File 和 Delete File 列表
4. 过滤出不再被任何保留快照引用的文件
5. 删除这些文件并统计

---

## 常见问题

### Q1: 为什么 Manifest Lists 数量通常等于过期快照数量？

**A:** 每个快照对应一个 Manifest List 文件。如果过期 10 个快照，通常会删除 10 个 Manifest List 文件。

```
过期快照数 ≈ deleted_manifest_lists_count
```

---

### Q2: 为什么 Data Files 数量可能为 0？

**A:** 如果多个快照共享相同的数据文件，过期部分快照不会删除这些共享文件。

**示例：**
```
Snapshot 1 (保留) → Data File A, B
Snapshot 2 (过期) → Data File B, C
Snapshot 3 (保留) → Data File C, D

过期 Snapshot 2 后:
- Data File B: 被 Snapshot 1 引用，不删除
- Data File C: 被 Snapshot 3 引用，不删除
→ deleted_data_files_count = 0
```

---

### Q3: Position Delete 和 Equality Delete 如何区分？

**A:** 通过文件路径中的关键字：

| Delete 类型 | 路径模式 | 示例 |
|------------|---------|------|
| Position Delete | `pos-delete`, `position-delete` | `00000-0-pos-deletes-00001.parquet` |
| Equality Delete | `eq-delete`, `equality-delete` | `00000-0-eq-deletes-00001.parquet` |
| Generic Delete | `-deletes`, `delete-` | `00000-0-deletes-00001.parquet` (默认为 Position) |

---

### Q4: Statistics Files 何时会被创建？

**A:** 以下操作会生成 Statistics 文件：

1. **显式收集统计信息**：
   ```sql
   ANALYZE TABLE catalog.db.table COMPUTE STATISTICS;
   ```

2. **写入时自动收集**（如果启用）：
   ```sql
   CREATE TABLE ... TBLPROPERTIES (
       'write.metadata.metrics.default' = 'full'
   );
   ```

3. **Iceberg V2 表的 Puffin 统计**：
   ```sql
   ALTER TABLE ... SET TBLPROPERTIES (
       'write.metadata.metrics.default' = 'full',
       'write.metadata.metrics.max-inferred-column-defaults' = '100'
   );
   ```

---

## 性能考量

### 文件分类性能

```java
// 分类逻辑基于字符串匹配（高效）
// 时间复杂度: O(文件路径长度)
// 空间复杂度: O(1)

if (file.contains("/metadata/")) {  // 快速前缀检查
    if (file.contains("-m") && file.endsWith(".avro")) {
        // Manifest
    }
}
```

### 统计信息收集

```java
// 使用 AtomicLong 保证线程安全（并发删除场景）
final AtomicLong deletedDataFilesCount = new AtomicLong(0);

// increment 操作性能
// CAS (Compare-And-Swap) 原子操作
// 高并发下可能有轻微性能损失，但可接受
```

---

## 最佳实践

### 1. 监控删除统计

```sql
-- 定期检查每种文件类型的删除趋势
SELECT
    operation_time,
    deleted_data_files_count,
    deleted_position_delete_files_count,
    deleted_equality_delete_files_count,
    deleted_manifest_files_count,
    deleted_manifest_lists_count,
    deleted_statistics_files_count
FROM expire_snapshots_history
ORDER BY operation_time DESC
LIMIT 10;
```

### 2. 异常告警规则

```sql
-- 告警 1: Data Files 删除过多（可能误操作）
IF deleted_data_files_count > 10000 THEN
    ALERT "Large number of data files deleted"
END IF

-- 告警 2: Manifest Lists 与预期快照数不符
IF deleted_manifest_lists_count != expected_snapshot_count THEN
    ALERT "Unexpected manifest list count"
END IF

-- 告警 3: 所有计数器为 0（可能配置错误）
IF SUM(all_counters) = 0 THEN
    ALERT "No files deleted - check configuration"
END IF
```

### 3. 性能优化建议

- **小表（< 1,000 文件）**：无需特殊优化
- **中型表（1,000 - 10,000 文件）**：设置 `max_concurrent_deletes = 5-10`
- **大型表（> 10,000 文件）**：设置 `max_concurrent_deletes = 10-20`

---

**生成日期：** 2025-11-15
**文档版本：** 1.0
**对齐标准：** Apache Spark ExpireSnapshots
**实现状态：** ✅ 已完成
