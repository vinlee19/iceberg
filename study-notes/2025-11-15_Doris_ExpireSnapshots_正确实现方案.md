# Doris ExpireSnapshots 正确实现方案

**日期：** 2025-11-15
**问题：** 当前实现通过路径模式猜测文件类型，不够准确
**正确方案：** 使用 Iceberg Core API 读取 Manifest 获取真实文件类型

---

## 问题分析

### 当前实现的问题

```java
// ❌ 错误：通过路径模式猜测
expireSnapshots.deleteWith(file -> {
    if (file.contains("pos-delete")) {
        deletedPositionDeleteFilesCount.incrementAndGet();
    } else if (file.contains("eq-delete")) {
        deletedEqualityDeleteFilesCount.incrementAndGet();
    }
    // ...
});
```

**问题：**
1. **不准确**：文件路径命名可能不规范
2. **不可靠**：依赖文件路径约定，容易出错
3. **不完整**：无法区分同一目录下的不同类型文件

### Spark 的正确做法

```java
// ✅ 正确：从 Manifest 读取文件的真实类型
static FileInfo toFileInfo(ContentFile<?> file) {
    return new FileInfo(file.location(), file.content().toString());
    // file.content() 返回: DATA, POSITION_DELETES, EQUALITY_DELETES
}
```

**优势：**
1. **准确**：直接读取 Manifest 中存储的文件类型
2. **可靠**：基于 Iceberg 元数据，不依赖路径约定
3. **完整**：支持所有 Iceberg 文件内容类型

---

## 正确实现方案

### 方案 1: 使用 Iceberg Core API 遍历 Manifest（推荐）

#### 核心思路

```
1. 调用 table.expireSnapshots().cleanExpiredFiles(false).commit()
2. 比较过期前后的元数据
3. 遍历已过期快照的 Manifest List
4. 读取每个 Manifest，获取文件路径和类型 (ContentFile.content())
5. 过滤出不再被保留快照引用的文件
6. 按类型分类删除并统计
```

#### 实现代码

```java
@Override
protected List<String> executeAction(TableIf table) throws UserException {
    Table icebergTable = ((IcebergExternalTable) table).getIcebergTable();
    TableOperations ops = ((HasTableOperations) icebergTable).operations();

    // 获取参数
    String olderThan = namedArguments.getString(OLDER_THAN);
    Integer retainLast = namedArguments.getInt(RETAIN_LAST);
    Integer maxConcurrentDeletes = namedArguments.getInt(MAX_CONCURRENT_DELETES);
    String snapshotIds = namedArguments.getString(SNAPSHOT_IDS);
    Boolean cleanExpiredMetadata = namedArguments.getBoolean(CLEAN_EXPIRED_METADATA);

    try {
        // Step 1: 获取过期前的元数据
        TableMetadata originalMetadata = ops.current();

        // Step 2: 创建 ExpireSnapshots 操作
        org.apache.iceberg.ExpireSnapshots expireSnapshots = icebergTable.expireSnapshots();

        // Step 3: 配置参数
        if (olderThan != null) {
            long timestampMs = parseTimestamp(olderThan);
            expireSnapshots = expireSnapshots.expireOlderThan(timestampMs);
        }

        if (retainLast != null) {
            expireSnapshots = expireSnapshots.retainLast(retainLast);
        }

        if (snapshotIds != null && !snapshotIds.isEmpty()) {
            String[] idArray = snapshotIds.split(",");
            for (String idStr : idArray) {
                long snapshotId = Long.parseLong(idStr.trim());
                expireSnapshots = expireSnapshots.expireSnapshotId(snapshotId);
            }
        }

        if (cleanExpiredMetadata != null && cleanExpiredMetadata) {
            expireSnapshots = expireSnapshots.cleanExpiredMetadata(true);
        }

        // Step 4: 禁用文件删除，仅提交元数据变更
        expireSnapshots.cleanExpiredFiles(false).commit();

        // Step 5: 获取过期后的元数据
        TableMetadata updatedMetadata = ops.refresh();

        // Step 6: 计算过期的快照 ID
        Set<Long> expiredSnapshotIds = findExpiredSnapshotIds(originalMetadata, updatedMetadata);

        // Step 7: 收集过期前的所有文件（按类型分类）
        Map<String, Set<String>> filesBeforeByType = collectFiles(originalMetadata, expiredSnapshotIds, icebergTable.io());

        // Step 8: 收集过期后的所有有效文件
        Set<String> validFiles = collectAllFiles(updatedMetadata, icebergTable.io());

        // Step 9: 计算需要删除的文件（按类型）
        Map<String, List<String>> filesToDeleteByType = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : filesBeforeByType.entrySet()) {
            String type = entry.getKey();
            Set<String> files = entry.getValue();
            List<String> toDelete = files.stream()
                .filter(file -> !validFiles.contains(file))
                .collect(Collectors.toList());
            filesToDeleteByType.put(type, toDelete);
        }

        // Step 10: 统计并删除文件
        final AtomicLong deletedDataFilesCount = new AtomicLong(0);
        final AtomicLong deletedPositionDeleteFilesCount = new AtomicLong(0);
        final AtomicLong deletedEqualityDeleteFilesCount = new AtomicLong(0);
        final AtomicLong deletedManifestFilesCount = new AtomicLong(0);
        final AtomicLong deletedManifestListsCount = new AtomicLong(0);
        final AtomicLong deletedStatisticsFilesCount = new AtomicLong(0);

        // 删除数据文件
        deleteFilesByType(filesToDeleteByType.getOrDefault("DATA", Lists.newArrayList()),
                icebergTable.io(), deletedDataFilesCount, maxConcurrentDeletes);

        // 删除 Position Delete 文件
        deleteFilesByType(filesToDeleteByType.getOrDefault("POSITION_DELETES", Lists.newArrayList()),
                icebergTable.io(), deletedPositionDeleteFilesCount, maxConcurrentDeletes);

        // 删除 Equality Delete 文件
        deleteFilesByType(filesToDeleteByType.getOrDefault("EQUALITY_DELETES", Lists.newArrayList()),
                icebergTable.io(), deletedEqualityDeleteFilesCount, maxConcurrentDeletes);

        // 删除 Manifest 文件
        List<String> manifestFiles = collectManifestFiles(originalMetadata, expiredSnapshotIds);
        List<String> validManifests = collectManifestFiles(updatedMetadata, null);
        List<String> expiredManifests = manifestFiles.stream()
            .filter(m -> !validManifests.contains(m))
            .collect(Collectors.toList());
        deleteFilesByType(expiredManifests, icebergTable.io(), deletedManifestFilesCount, maxConcurrentDeletes);

        // 删除 Manifest List 文件
        List<String> manifestListFiles = collectManifestListFiles(originalMetadata, expiredSnapshotIds);
        deleteFilesByType(manifestListFiles, icebergTable.io(), deletedManifestListsCount, maxConcurrentDeletes);

        // 删除 Statistics 文件
        List<String> statsFiles = collectStatisticsFiles(originalMetadata, expiredSnapshotIds, updatedMetadata);
        deleteFilesByType(statsFiles, icebergTable.io(), deletedStatisticsFilesCount, maxConcurrentDeletes);

        // Step 11: 失效缓存
        Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache((ExternalTable) table);

        // Step 12: 返回统计信息
        return Lists.newArrayList(
            String.valueOf(deletedDataFilesCount.get()),
            String.valueOf(deletedPositionDeleteFilesCount.get()),
            String.valueOf(deletedEqualityDeleteFilesCount.get()),
            String.valueOf(deletedManifestFilesCount.get()),
            String.valueOf(deletedManifestListsCount.get()),
            String.valueOf(deletedStatisticsFilesCount.get())
        );

    } catch (Exception e) {
        throw new UserException("Failed to expire snapshots: " + e.getMessage(), e);
    }
}

/**
 * 计算过期的快照 ID
 */
private Set<Long> findExpiredSnapshotIds(TableMetadata originalMetadata, TableMetadata updatedMetadata) {
    Set<Long> retainedSnapshots = updatedMetadata.snapshots().stream()
        .map(Snapshot::snapshotId)
        .collect(Collectors.toSet());

    return originalMetadata.snapshots().stream()
        .map(Snapshot::snapshotId)
        .filter(id -> !retainedSnapshots.contains(id))
        .collect(Collectors.toSet());
}

/**
 * 收集指定快照的所有文件（按类型分类）
 */
private Map<String, Set<String>> collectFiles(
        TableMetadata metadata,
        Set<Long> snapshotIds,
        FileIO io) {

    Map<String, Set<String>> filesByType = new HashMap<>();
    filesByType.put("DATA", new HashSet<>());
    filesByType.put("POSITION_DELETES", new HashSet<>());
    filesByType.put("EQUALITY_DELETES", new HashSet<>());

    for (Snapshot snapshot : metadata.snapshots()) {
        if (snapshotIds == null || snapshotIds.contains(snapshot.snapshotId())) {
            // 读取 Manifest List
            List<ManifestFile> manifests = snapshot.allManifests(io);

            for (ManifestFile manifest : manifests) {
                // 读取 Manifest 内容
                try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, io)) {
                    for (DataFile file : reader) {
                        String type = file.content().toString(); // DATA, POSITION_DELETES, EQUALITY_DELETES
                        filesByType.get(type).add(file.location());
                    }
                }
            }
        }
    }

    return filesByType;
}

/**
 * 收集所有有效文件
 */
private Set<String> collectAllFiles(TableMetadata metadata, FileIO io) {
    Set<String> allFiles = new HashSet<>();

    for (Snapshot snapshot : metadata.snapshots()) {
        List<ManifestFile> manifests = snapshot.allManifests(io);

        for (ManifestFile manifest : manifests) {
            try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, io)) {
                for (DataFile file : reader) {
                    allFiles.add(file.location());
                }
            }
        }
    }

    return allFiles;
}

/**
 * 收集 Manifest 文件
 */
private List<String> collectManifestFiles(TableMetadata metadata, Set<Long> snapshotIds) {
    List<String> manifestFiles = new ArrayList<>();

    for (Snapshot snapshot : metadata.snapshots()) {
        if (snapshotIds == null || snapshotIds.contains(snapshot.snapshotId())) {
            for (ManifestFile manifest : snapshot.allManifests(metadata.io())) {
                manifestFiles.add(manifest.path());
            }
        }
    }

    return manifestFiles;
}

/**
 * 收集 Manifest List 文件
 */
private List<String> collectManifestListFiles(TableMetadata metadata, Set<Long> snapshotIds) {
    List<String> manifestListFiles = new ArrayList<>();

    for (Snapshot snapshot : metadata.snapshots()) {
        if (snapshotIds == null || snapshotIds.contains(snapshot.snapshotId())) {
            manifestListFiles.add(snapshot.manifestListLocation());
        }
    }

    return manifestListFiles;
}

/**
 * 收集 Statistics 文件
 */
private List<String> collectStatisticsFiles(
        TableMetadata originalMetadata,
        Set<Long> expiredSnapshotIds,
        TableMetadata updatedMetadata) {

    // 收集过期快照引用的统计文件
    Set<String> expiredStatsFiles = new HashSet<>();
    for (Snapshot snapshot : originalMetadata.snapshots()) {
        if (expiredSnapshotIds.contains(snapshot.snapshotId())) {
            StatisticsFile stats = snapshot.statisticsFiles();
            if (stats != null) {
                expiredStatsFiles.add(stats.path());
            }
        }
    }

    // 收集保留快照引用的统计文件
    Set<String> validStatsFiles = new HashSet<>();
    for (Snapshot snapshot : updatedMetadata.snapshots()) {
        StatisticsFile stats = snapshot.statisticsFiles();
        if (stats != null) {
            validStatsFiles.add(stats.path());
        }
    }

    // 计算需要删除的统计文件
    return expiredStatsFiles.stream()
        .filter(file -> !validStatsFiles.contains(file))
        .collect(Collectors.toList());
}

/**
 * 按类型删除文件（支持并发）
 */
private void deleteFilesByType(
        List<String> files,
        FileIO io,
        AtomicLong counter,
        Integer maxConcurrentDeletes) throws IOException {

    if (files.isEmpty()) {
        return;
    }

    if (maxConcurrentDeletes != null && maxConcurrentDeletes > 1) {
        // 并发删除
        ExecutorService executor = Executors.newFixedThreadPool(maxConcurrentDeletes);
        try {
            List<Future<Void>> futures = new ArrayList<>();
            for (String file : files) {
                futures.add(executor.submit(() -> {
                    io.deleteFile(file);
                    counter.incrementAndGet();
                    return null;
                }));
            }

            // 等待所有删除完成
            for (Future<Void> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdown();
        }
    } else {
        // 串行删除
        for (String file : files) {
            io.deleteFile(file);
            counter.incrementAndGet();
        }
    }
}
```

---

## 方案对比

### 当前实现（路径模式匹配）

**优点：**
- ✅ 实现简单
- ✅ 无需遍历 Manifest

**缺点：**
- ❌ 不准确（依赖路径命名约定）
- ❌ 可能误分类
- ❌ 不支持自定义命名方案

### 正确实现（从 Manifest 读取）

**优点：**
- ✅ 完全准确（基于 Iceberg 元数据）
- ✅ 支持所有 Iceberg 表
- ✅ 与 Spark 实现一致

**缺点：**
- ⚠️ 需要遍历 Manifest（性能开销）
- ⚠️ 实现较复杂

---

## 性能优化

### 1. 使用 ManifestReader 批量读取

```java
try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, io)) {
    // 批量处理，避免逐个 I/O
    for (DataFile file : reader) {
        // ...
    }
}
```

### 2. 并发读取 Manifest

```java
ExecutorService executor = Executors.newFixedThreadPool(10);
List<Future<Map<String, Set<String>>>> futures = new ArrayList<>();

for (ManifestFile manifest : manifests) {
    futures.add(executor.submit(() -> readManifest(manifest, io)));
}

// 合并结果
Map<String, Set<String>> allFiles = new HashMap<>();
for (Future<Map<String, Set<String>>> future : futures) {
    mergeFileMaps(allFiles, future.get());
}
```

### 3. 缓存 Manifest 内容

```java
// 如果表有很多快照，可以缓存 Manifest 内容避免重复读取
Map<String, List<DataFile>> manifestCache = new HashMap<>();
```

---

## 完整流程图

```
User SQL
    ↓
IcebergExpireSnapshotsAction
    ↓
┌──────────────────────────────────────────────────┐
│ Step 1: 获取过期前元数据                          │
│   originalMetadata = ops.current()                │
└──────────────────────────────────────────────────┘
    ↓
┌──────────────────────────────────────────────────┐
│ Step 2: 配置并执行 ExpireSnapshots                │
│   table.expireSnapshots()                         │
│       .expireOlderThan(...)                       │
│       .retainLast(...)                            │
│       .cleanExpiredFiles(false)  // 不删除文件    │
│       .commit()                  // 仅更新元数据  │
└──────────────────────────────────────────────────┘
    ↓
┌──────────────────────────────────────────────────┐
│ Step 3: 获取过期后元数据                          │
│   updatedMetadata = ops.refresh()                 │
└──────────────────────────────────────────────────┘
    ↓
┌──────────────────────────────────────────────────┐
│ Step 4: 计算过期快照 ID                           │
│   expiredSnapshotIds =                            │
│       originalSnapshots - updatedSnapshots        │
└──────────────────────────────────────────────────┘
    ↓
┌──────────────────────────────────────────────────┐
│ Step 5: 遍历过期快照的 Manifest                   │
│   for snapshot in expiredSnapshotIds:             │
│       manifestList = snapshot.manifestList()      │
│       for manifest in manifestList:               │
│           reader = ManifestFiles.read(manifest)   │
│           for file in reader:                     │
│               type = file.content()  // ⭐ 关键    │
│               path = file.location()              │
│               filesByType[type].add(path)         │
└──────────────────────────────────────────────────┘
    ↓
┌──────────────────────────────────────────────────┐
│ Step 6: 收集有效文件（保留快照）                   │
│   validFiles = collectAllFiles(updatedMetadata)   │
└──────────────────────────────────────────────────┘
    ↓
┌──────────────────────────────────────────────────┐
│ Step 7: 计算需要删除的文件                        │
│   toDelete = filesByType - validFiles             │
└──────────────────────────────────────────────────┘
    ↓
┌──────────────────────────────────────────────────┐
│ Step 8: 按类型删除文件并统计                      │
│   deleteFiles(DATA files) → counter1              │
│   deleteFiles(POSITION_DELETES) → counter2        │
│   deleteFiles(EQUALITY_DELETES) → counter3        │
│   deleteFiles(Manifests) → counter4               │
│   deleteFiles(Manifest Lists) → counter5          │
│   deleteFiles(Statistics) → counter6              │
└──────────────────────────────────────────────────┘
    ↓
┌──────────────────────────────────────────────────┐
│ Step 9: 返回统计结果（6 列）                       │
│   [counter1, counter2, ..., counter6]             │
└──────────────────────────────────────────────────┘
```

---

## 关键 API

### 1. TableMetadata

```java
TableOperations ops = ((HasTableOperations) table).operations();
TableMetadata metadata = ops.current();

// 获取所有快照
List<Snapshot> snapshots = metadata.snapshots();

// 获取快照详情
for (Snapshot snapshot : snapshots) {
    long id = snapshot.snapshotId();
    String manifestListLocation = snapshot.manifestListLocation();
    List<ManifestFile> manifests = snapshot.allManifests(io);
}
```

### 2. ManifestFile

```java
// 读取 Manifest
ManifestReader<DataFile> reader = ManifestFiles.read(manifest, io);

// 遍历文件
for (DataFile file : reader) {
    String path = file.location();
    FileContent content = file.content(); // DATA, POSITION_DELETES, EQUALITY_DELETES
}
```

### 3. FileContent (Enum)

```java
public enum FileContent {
    DATA(0),
    POSITION_DELETES(1),
    EQUALITY_DELETES(2);
}
```

---

## 实现建议

### 短期方案（当前可用）

保留当前的路径模式匹配实现，但添加警告：

```java
/**
 * WARNING: This implementation uses path pattern matching to classify file types.
 * For production use, consider using Manifest-based classification for accuracy.
 */
@Override
protected List<String> executeAction(TableIf table) throws UserException {
    // ... 当前实现 ...
}
```

### 长期方案（生产级）

实现基于 Manifest 的文件分类：

```java
/**
 * Production-ready implementation using Iceberg Manifest API
 * for accurate file type classification.
 */
@Override
protected List<String> executeAction(TableIf table) throws UserException {
    // ... 使用 ManifestReader 的实现 ...
}
```

---

## 总结

| 方案 | 准确性 | 性能 | 复杂度 | 推荐度 |
|------|--------|------|--------|--------|
| **路径模式匹配** | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐ | 适合演示/测试 |
| **Manifest 读取** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | **推荐生产使用** |

**建议：**
1. 当前实现可用于快速验证功能
2. 生产环境建议使用 Manifest 读取方案
3. 添加配置开关允许用户选择实现方式

---

**日期：** 2025-11-15
**文档版本：** 1.0
**状态：** 待实现
