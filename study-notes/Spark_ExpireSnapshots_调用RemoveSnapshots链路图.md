# Spark ExpireSnapshots 调用 RemoveSnapshots 完整链路图

## 问题：Spark Action 在哪里调用了 RemoveSnapshots？

## 答案：完整调用链路

```java
ExpireSnapshotsSparkAction.expireFiles()                    // Spark Action 层
    ↓
table.expireSnapshots()                                     // Table 接口调用
    ↓
BaseTable.expireSnapshots()                                 // BaseTable 实现
    ↓
new RemoveSnapshots(ops)                                    // 创建 RemoveSnapshots 实例
    ↓
RemoveSnapshots                                             // Core 层实现
```

---

## 详细源码定位

### 第一层：ExpireSnapshotsSparkAction.expireFiles()

**文件位置：** `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/ExpireSnapshotsSparkAction.java`

**行号：** 148-187

```java
public Dataset<FileInfo> expireFiles() {
  if (expiredFileDS == null) {
    // 1. 获取过期前的元数据
    TableMetadata originalMetadata = ops.current();

    // 2. ⭐ 关键调用点：调用 table.expireSnapshots()
    org.apache.iceberg.ExpireSnapshots expireSnapshots = table.expireSnapshots();

    // 3. 配置过期策略
    for (long id : expiredSnapshotIds) {
      expireSnapshots = expireSnapshots.expireSnapshotId(id);
    }

    if (expireOlderThanValue != null) {
      expireSnapshots = expireSnapshots.expireOlderThan(expireOlderThanValue);
    }

    if (retainLastValue != null) {
      expireSnapshots = expireSnapshots.retainLast(retainLastValue);
    }

    if (cleanExpiredMetadata != null) {
      expireSnapshots.cleanExpiredMetadata(cleanExpiredMetadata);
    }

    // 4. ⭐ 关键：禁用文件删除，仅提交快照过期
    expireSnapshots.cleanExpiredFiles(false).commit();

    // 5. 获取过期后的元数据
    TableMetadata updatedMetadata = ops.refresh();

    // 6. 使用 Spark 计算过期文件
    Dataset<FileInfo> validFileDS = fileDS(updatedMetadata);
    Set<Long> deletedSnapshotIds = findExpiredSnapshotIds(originalMetadata, updatedMetadata);
    Dataset<FileInfo> deleteCandidateFileDS = fileDS(originalMetadata, deletedSnapshotIds);

    // 7. Anti-Join 计算差集
    this.expiredFileDS = deleteCandidateFileDS.except(validFileDS);
  }

  return expiredFileDS;
}
```

**关键点：**
- 第 154 行：`table.expireSnapshots()` - 调用 Table 接口方法
- 第 172 行：`cleanExpiredFiles(false)` - 禁用 Core 的文件删除
- 第 172 行：`.commit()` - 提交元数据变更

---

### 第二层：Table 接口定义

**文件位置：** `api/src/main/java/org/apache/iceberg/Table.java`

**行号：** 约 280-290（接口定义）

```java
public interface Table {
  // ... 其他方法 ...

  /**
   * Create a new {@link ExpireSnapshots} API to expire snapshots from this table.
   *
   * @return a new {@link ExpireSnapshots}
   */
  ExpireSnapshots expireSnapshots();

  // ... 其他方法 ...
}
```

---

### 第三层：BaseTable 实现

**文件位置：** `core/src/main/java/org/apache/iceberg/BaseTable.java`

**行号：** 229-231

```java
@Override
public ExpireSnapshots expireSnapshots() {
  return new RemoveSnapshots(ops);  // ⭐ 创建 RemoveSnapshots 实例
}
```

**关键点：**
- 直接 new 一个 `RemoveSnapshots` 对象
- 传入 `TableOperations` 对象（ops）

---

### 第四层：RemoveSnapshots 构造函数

**文件位置：** `core/src/main/java/org/apache/iceberg/RemoveSnapshots.java`

**行号：** 83-102

```java
class RemoveSnapshots implements ExpireSnapshots {
  private static final Logger LOG = LoggerFactory.getLogger(RemoveSnapshots.class);

  private static final ExecutorService DEFAULT_DELETE_EXECUTOR_SERVICE =
      MoreExecutors.newDirectExecutorService();

  private final TableOperations ops;
  private final Set<Long> idsToRemove = Sets.newHashSet();
  private final long now;
  private final long defaultMaxRefAgeMs;
  private boolean cleanExpiredFiles = true;
  private TableMetadata base;
  private long defaultExpireOlderThan;
  private int defaultMinNumSnapshots;
  private Consumer<String> deleteFunc = null;
  private ExecutorService deleteExecutorService = DEFAULT_DELETE_EXECUTOR_SERVICE;
  private ExecutorService planExecutorService;
  private Boolean incrementalCleanup;
  private boolean specifiedSnapshotId = false;
  private boolean cleanExpiredMetadata = false;

  RemoveSnapshots(TableOperations ops) {
    this.ops = ops;
    this.base = ops.current();
    ValidationException.check(
        PropertyUtil.propertyAsBoolean(base.properties(), GC_ENABLED, GC_ENABLED_DEFAULT),
        "Cannot expire snapshots: GC is disabled (deleting files may corrupt other tables)");

    long defaultMaxSnapshotAgeMs =
        PropertyUtil.propertyAsLong(
            base.properties(), MAX_SNAPSHOT_AGE_MS, MAX_SNAPSHOT_AGE_MS_DEFAULT);

    this.now = System.currentTimeMillis();
    this.defaultExpireOlderThan = now - defaultMaxSnapshotAgeMs;
    this.defaultMinNumSnapshots =
        PropertyUtil.propertyAsInt(
            base.properties(), MIN_SNAPSHOTS_TO_KEEP, MIN_SNAPSHOTS_TO_KEEP_DEFAULT);

    this.defaultMaxRefAgeMs =
        PropertyUtil.propertyAsLong(base.properties(), MAX_REF_AGE_MS, MAX_REF_AGE_MS_DEFAULT);
  }

  // ... 其他方法 ...
}
```

---

## 完整调用时序图

```
User/SQL                 ExpireSnapshotsSparkAction         Table            BaseTable         RemoveSnapshots
  │                              │                            │                 │                     │
  │  execute()                   │                            │                 │                     │
  │─────────────────────────────>│                            │                 │                     │
  │                              │                            │                 │                     │
  │                              │  expireFiles()             │                 │                     │
  │                              │────────────┐               │                 │                     │
  │                              │            │               │                 │                     │
  │                              │<───────────┘               │                 │                     │
  │                              │                            │                 │                     │
  │                              │  table.expireSnapshots()   │                 │                     │
  │                              │───────────────────────────>│                 │                     │
  │                              │                            │                 │                     │
  │                              │                            │  (interface)    │                     │
  │                              │                            │────────────────>│                     │
  │                              │                            │                 │                     │
  │                              │                            │                 │  new RemoveSnapshots(ops)
  │                              │                            │                 │────────────────────>│
  │                              │                            │                 │                     │
  │                              │                            │                 │  RemoveSnapshots    │
  │                              │                            │                 │<────────────────────│
  │                              │                            │                 │                     │
  │                              │                            │  RemoveSnapshots│                     │
  │                              │                            │<────────────────│                     │
  │                              │                            │                 │                     │
  │                              │  ExpireSnapshots (interface)                 │                     │
  │                              │<───────────────────────────│                 │                     │
  │                              │                            │                 │                     │
  │                              │  .expireOlderThan()        │                 │                     │
  │                              │  .retainLast()             │                 │                     │
  │                              │  .cleanExpiredFiles(false) │                 │                     │
  │                              │───────────────────────────────────────────────────────────────────>│
  │                              │                            │                 │                     │
  │                              │  .commit()                 │                 │                     │
  │                              │───────────────────────────────────────────────────────────────────>│
  │                              │                            │                 │                     │
  │                              │                            │                 │  internalApply()    │
  │                              │                            │                 │  (计算要删除的快照)│
  │                              │                            │                 │                     │
  │                              │                            │                 │  ops.commit(base, updated)
  │                              │                            │                 │  (提交元数据变更)  │
  │                              │                            │                 │                     │
  │                              │  Committed                 │                 │                     │
  │                              │<───────────────────────────────────────────────────────────────────│
  │                              │                            │                 │                     │
  │                              │  ops.refresh()             │                 │                     │
  │                              │  (获取更新后的元数据)      │                 │                     │
  │                              │                            │                 │                     │
  │                              │  fileDS(updated)           │                 │                     │
  │                              │  fileDS(original, expired) │                 │                     │
  │                              │  (使用 Spark 计算过期文件) │                 │                     │
  │                              │                            │                 │                     │
  │                              │  except (Anti-Join)        │                 │                     │
  │                              │  (计算文件差集)            │                 │                     │
  │                              │                            │                 │                     │
  │                              │  deleteFiles()             │                 │                     │
  │                              │  (删除过期文件)            │                 │                     │
  │                              │                            │                 │                     │
  │  Result                      │                            │                 │                     │
  │<─────────────────────────────│                            │                 │                     │
  │                              │                            │                 │                     │
```

---

## 关键设计点分析

### 1. 为什么要调用 Core 的 RemoveSnapshots？

**原因：**
- Core 层包含完整的快照过期逻辑（计算保留/过期快照）
- Core 层负责元数据更新（移除快照引用）
- Core 层保证元数据一致性（乐观锁、重试机制）

**Spark 层的增强：**
- 使用 Spark 分布式计算识别过期文件（替代 Core 的单节点扫描）
- 提供更高效的文件删除（批量删除、并发删除）

### 2. cleanExpiredFiles(false) 的作用

```java
expireSnapshots.cleanExpiredFiles(false).commit();
```

**关键点：**
- `cleanExpiredFiles(false)` - 禁用 Core 的文件删除逻辑
- 仅让 Core 完成快照过期和元数据更新
- 文件删除由 Spark 层接管

**如果不禁用会怎样？**
```java
// 错误示例：不禁用文件删除
expireSnapshots.cleanExpiredFiles(true).commit();  // ❌

// 会导致：
// 1. Core 单节点扫描 Manifest（慢）
// 2. Core 串行或低并发删除文件（慢）
// 3. 无法利用 Spark 的分布式计算能力
// 4. 文件可能被删除两次（Core 一次，Spark 一次）
```

### 3. 为什么 Spark 不直接实现快照过期逻辑？

**原因：**
1. **复用性**：Core 的快照过期逻辑复杂且稳定
2. **一致性**：Core 保证元数据更新的原子性
3. **兼容性**：所有引擎（Spark、Flink、Trino）共享同一套逻辑
4. **分工明确**：
   - Core 负责：快照过期逻辑、元数据更新
   - Spark 负责：分布式文件识别、高效删除

---

## 其他 Table 实现

### BaseTransaction.expireSnapshots()

**文件位置：** `core/src/main/java/org/apache/iceberg/BaseTransaction.java:232-234`

```java
@Override
public ExpireSnapshots expireSnapshots() {
  return appendUpdate(new RemoveSnapshots(transactionOps));
}
```

**说明：** 在事务中调用 expireSnapshots

---

### SerializableTable.expireSnapshots()

**文件位置：** `core/src/main/java/org/apache/iceberg/SerializableTable.java:408-410`

```java
@Override
public ExpireSnapshots expireSnapshots() {
  throw new UnsupportedOperationException(errorMsg("expireSnapshots"));
}
```

**说明：** 序列化表不支持 expireSnapshots（只读）

---

### BaseReadOnlyTable.expireSnapshots()

**文件位置：** `core/src/main/java/org/apache/iceberg/BaseReadOnlyTable.java:110-112`

```java
@Override
public ExpireSnapshots expireSnapshots() {
  throw new UnsupportedOperationException(
      "Cannot expire snapshots from a " + descriptor + " table");
}
```

**说明：** 只读表不支持 expireSnapshots

---

## 总结

### 调用链路

```
ExpireSnapshotsSparkAction.expireFiles() (Line 154)
    ↓
table.expireSnapshots()
    ↓
BaseTable.expireSnapshots() (Line 229)
    ↓
new RemoveSnapshots(ops) (Line 230)
    ↓
RemoveSnapshots.commit() (通过 Spark Action 调用)
```

### 关键代码位置

| 组件 | 文件 | 行号 | 关键代码 |
|------|------|------|---------|
| Spark Action | `ExpireSnapshotsSparkAction.java` | 154 | `table.expireSnapshots()` |
| Spark Action | `ExpireSnapshotsSparkAction.java` | 172 | `cleanExpiredFiles(false).commit()` |
| BaseTable | `BaseTable.java` | 229-231 | `new RemoveSnapshots(ops)` |
| RemoveSnapshots | `RemoveSnapshots.java` | 83-102 | 构造函数 |
| RemoveSnapshots | `RemoveSnapshots.java` | 339-358 | `commit()` 方法 |

### 执行流程

1. **Spark Action 创建 RemoveSnapshots**
   ```java
   org.apache.iceberg.ExpireSnapshots expireSnapshots = table.expireSnapshots();
   // → BaseTable.expireSnapshots()
   // → new RemoveSnapshots(ops)
   ```

2. **配置过期策略**
   ```java
   expireSnapshots.expireSnapshotId(id);
   expireSnapshots.expireOlderThan(timestampMillis);
   expireSnapshots.retainLast(numSnapshots);
   ```

3. **禁用文件删除并提交**
   ```java
   expireSnapshots.cleanExpiredFiles(false).commit();
   // → RemoveSnapshots.commit()
   // → internalApply() (计算过期快照)
   // → ops.commit(base, updated) (更新元数据)
   ```

4. **Spark 接管文件删除**
   ```java
   Dataset<FileInfo> validFileDS = fileDS(updatedMetadata);
   Dataset<FileInfo> deleteCandidateFileDS = fileDS(originalMetadata, deletedSnapshotIds);
   this.expiredFileDS = deleteCandidateFileDS.except(validFileDS);
   ```

### 设计优势

✅ **复用 Core 逻辑**：不重复实现快照过期逻辑
✅ **分工明确**：Core 负责元数据，Spark 负责文件操作
✅ **性能优化**：利用 Spark 分布式计算
✅ **一致性保证**：Core 的元数据更新机制
✅ **灵活控制**：通过 `cleanExpiredFiles(false)` 控制行为

---

**生成日期：** 2025-11-15
**Iceberg 版本：** 1.10.x
