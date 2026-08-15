# 2025-12-19 Apache Iceberg 小文件优化完整指南：rewrite_data_files 策略深度详解

## 摘要

本文档深入分析 Apache Iceberg 小文件问题的根源，详细解析 rewrite_data_files 机制的核心原理，并全面对比不同 compaction 策略的适用场景。通过源码级分析和实际案例，为数据工程师提供优化 Iceberg 表性能的完整解决方案。

## 目录

1. [小文件问题的根本原因分析](#1-小文件问题的根本原因分析)
2. [rewrite_data_files 核心机制详解](#2-rewrite_data_files-核心机制详解)
3. [不同 Compaction 策略深度对比](#3-不同-compaction-策略深度对比)
4. [配置优化最佳实践](#4-配置优化最佳实践)
5. [性能监控与调优策略](#5-性能监控与调优策略)
6. [生产环境实战案例](#6-生产环境实战案例)

---

## 1. 小文件问题的根本原因分析

### 1.1 小文件产生的根本机制

Apache Iceberg 小文件问题主要由以下几个因素造成：

#### 1.1.1 写入模式影响

```java
// 流式写入导致的小文件问题
// BaseRewriteDataFilesAction.java:80
long targetFileSize = PropertyUtil.propertyAsLong(
    table.properties(),
    TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
    TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

// 流式写入时，数据到达频率不规律，容易产生小于目标大小的文件
```

**小文件产生的主要原因：**

1. **流式数据写入**
   - Kafka Connect、Flink 流处理等产生的小批次数据
   - 实时数据摄入时的不规律数据流量
   - 微批处理导致的碎片化文件

2. **分区策略不当**
   - 过度分区导致每个分区数据量过小
   - 动态分区写入时的数据倾斜
   - 高基数分区字段的使用

3. **删除操作碎片化**
   - Row-level 删除操作产生的删除文件
   - MERGE 操作导致的文件分裂
   - UPDATE 操作的写入放大效应

#### 1.1.2 文件大小阈值机制

```java
// TableProperties.java:214
public static final long SPLIT_SIZE_DEFAULT = 128 * 1024 * 1024; // 128 MB

// 默认目标文件大小：128MB
// 当实际文件远小于此值时，查询性能会显著下降
```

### 1.2 小文件对性能的影响分析

#### 1.2.1 查询性能影响

```java
// ManifestGroup 中的文件扫描开销
// core/src/main/java/org/apache/iceberg/ManifestGroup.java
public CloseableIterable<FileScanTask> planFiles() {
    // 每个文件都需要单独的扫描任务
    // 小文件过多导致任务调度开销增大
    return manifestGroup.planFiles();
}
```

**性能影响量化分析：**

1. **元数据开销增加**
   - 每个文件都需要单独的 Manifest Entry
   - Manifest 文件数量增加，读取开销上升
   - 分区修剪效果下降

2. **任务调度开销**
   - 过多的 FileScanTask 增加调度复杂度
   - 并行度受限于文件数量而非数据大小
   - 网络 I/O 次数大幅增加

3. **存储系统压力**
   - 对象存储的 LIST 操作开销增大
   - 文件系统 inode 消耗增加
   - 备份和恢复时间延长

---

## 2. rewrite_data_files 核心机制详解

### 2.1 RewriteDataFiles 接口架构

#### 2.1.1 核心接口设计

```java
// api/src/main/java/org/apache/iceberg/actions/RewriteDataFiles.java:32
public interface RewriteDataFiles
    extends SnapshotUpdate<RewriteDataFiles, RewriteDataFiles.Result> {

    // 核心配置参数
    String MAX_FILE_GROUP_SIZE_BYTES = "max-file-group-size-bytes";
    long MAX_FILE_GROUP_SIZE_BYTES_DEFAULT = 1024L * 1024L * 1024L * 100L; // 100 GB

    String MAX_CONCURRENT_FILE_GROUP_REWRITES = "max-concurrent-file-group-rewrites";
    int MAX_CONCURRENT_FILE_GROUP_REWRITES_DEFAULT = 5;

    String TARGET_FILE_SIZE_BYTES = "target-file-size-bytes";
    String USE_STARTING_SEQUENCE_NUMBER = "use-starting-sequence-number";
    boolean USE_STARTING_SEQUENCE_NUMBER_DEFAULT = true;
}
```

#### 2.1.2 文件分组策略

```java
// BinPackRewriteFilePlanner.java:58
public class BinPackRewriteFilePlanner
    extends SizeBasedFileRewritePlanner<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> {

    // 删除文件阈值配置
    public static final String DELETE_FILE_THRESHOLD = "delete-file-threshold";
    public static final int DELETE_FILE_THRESHOLD_DEFAULT = Integer.MAX_VALUE;

    // 删除比例阈值配置
    public static final String DELETE_RATIO_THRESHOLD = "delete-ratio-threshold";
    public static final double DELETE_RATIO_THRESHOLD_DEFAULT = 0.3; // 30%
}
```

### 2.2 文件重写执行流程

#### 2.2.1 基础执行框架

```java
// BaseRewriteDataFilesAction.java:57
public abstract class BaseRewriteDataFilesAction<ThisT>
    extends BaseSnapshotUpdateAction<ThisT, RewriteDataFilesActionResult> {

    // 核心执行逻辑
    @Override
    protected RewriteDataFilesActionResult doExecute() {
        // 1. 扫描并识别需要重写的文件
        List<FileScanTask> filesToRewrite = findFilesToRewrite();

        // 2. 根据策略分组文件
        List<RewriteFileGroup> fileGroups = planFileGroups(filesToRewrite);

        // 3. 并行执行重写操作
        List<FileGroupRewriteResult> results = executeRewrite(fileGroups);

        // 4. 提交新的快照
        return commitResults(results);
    }
}
```

#### 2.2.2 事务性保证机制

```java
// RewriteDataFilesCommitManager.java 中的事务管理
public class RewriteDataFilesCommitManager {
    // 确保原子性提交
    public void commitFileGroups(Set<RewriteFileGroup> fileGroups) {
        RewriteFiles rewriteFiles = table.newRewrite();

        // 添加新文件，删除旧文件
        for (RewriteFileGroup group : fileGroups) {
            rewriteFiles.rewriteFiles(group.filesToDelete(), group.filesToAdd());
        }

        // 原子性提交
        rewriteFiles.commit();
    }
}
```

---

## 3. 不同 Compaction 策略深度对比

### 3.1 BIN-PACK 策略详解

#### 3.1.1 核心原理

```java
// SparkBinPackDataRewriter.java:43
protected void doRewrite(String groupId, List<FileScanTask> group) {
    // 读取文件并按要求大小打包
    Dataset<Row> scanDF = spark()
        .read()
        .format("iceberg")
        .option(SparkReadOptions.SCAN_TASK_SET_ID, groupId)
        .option(SparkReadOptions.SPLIT_SIZE, splitSize(inputSize(group)))
        .option(SparkReadOptions.FILE_OPEN_COST, "0")
        .load(groupId);

    // 写入打包后的数据，每个分片成为新文件
    scanDF.write()
        .format("iceberg")
        .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, groupId)
        .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, writeMaxFileSize())
        .option(SparkWriteOptions.DISTRIBUTION_MODE, distributionMode(group).modeName())
        .mode("append")
        .save(groupId);
}
```

#### 3.1.2 适用场景分析

**最佳适用场景：**
- 大量小文件需要合并
- 数据分布相对均匀
- 不需要特殊的数据排序
- 快速执行要求高

**配置示例：**
```sql
-- Spark SQL 中的 BIN-PACK 策略调用
CALL spark_catalog.system.rewrite_data_files(
    table => 'my_database.my_table',
    strategy => 'binpack',
    options => map(
        'target-file-size-bytes', '268435456',  -- 256MB
        'max-file-group-size-bytes', '107374182400',  -- 100GB
        'max-concurrent-file-group-rewrites', '10'
    )
);
```

**性能特征：**
- ✅ 执行速度快，无需排序
- ✅ 资源消耗相对较低
- ✅ 适合大规模数据处理
- ❌ 不改善数据局部性
- ❌ 对范围查询优化有限

### 3.2 SORT 策略详解

#### 3.2.1 核心实现机制

```java
// SparkShufflingDataRewriter.java:51
abstract class SparkShufflingDataRewriter extends SparkSizeBasedDataRewriter {
    // 压缩因子控制
    public static final String COMPRESSION_FACTOR = "compression-factor";
    public static final double COMPRESSION_FACTOR_DEFAULT = 1.0;

    // 每文件的 shuffle 分区数
    public static final String SHUFFLE_PARTITIONS_PER_FILE = "shuffle-partitions-per-file";
    public static final int SHUFFLE_PARTITIONS_PER_FILE_DEFAULT = 1;
}
```

#### 3.2.2 排序策略配置

**使用表级排序：**
```sql
-- 基于表的 SortOrder 进行重写
CALL spark_catalog.system.rewrite_data_files(
    table => 'my_database.my_table',
    strategy => 'sort',
    options => map(
        'target-file-size-bytes', '134217728',  -- 128MB
        'compression-factor', '1.2',
        'shuffle-partitions-per-file', '2'
    )
);
```

**自定义排序顺序：**
```sql
-- 指定自定义排序字段
CALL spark_catalog.system.rewrite_data_files(
    table => 'my_database.my_table',
    strategy => 'sort',
    sort_order => 'event_time DESC NULLS LAST, user_id ASC',
    options => map(
        'target-file-size-bytes', '268435456'  -- 256MB
    )
);
```

**适用场景：**
- 频繁的范围查询
- 需要提升数据局部性
- 时间序列数据优化
- 复杂的过滤条件查询

### 3.3 Z-ORDER 策略详解

#### 3.3.1 Z-Order 核心算法

```java
// SparkZOrderDataRewriter.java:47
class SparkZOrderDataRewriter extends SparkShufflingDataRewriter {
    private static final String Z_COLUMN = "ICEZVALUE";
    private static final Schema Z_SCHEMA =
        new Schema(Types.NestedField.required(0, Z_COLUMN, Types.BinaryType.get()));

    // Z-Order 配置参数
    public static final String MAX_OUTPUT_SIZE = "max-output-size";
    public static final int MAX_OUTPUT_SIZE_DEFAULT = Integer.MAX_VALUE;

    public static final String VAR_LENGTH_CONTRIBUTION = "var-length-contribution";
    public static final int VAR_LENGTH_CONTRIBUTION_DEFAULT = ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE;
}
```

#### 3.3.2 Z-Order 使用示例

**多维度查询优化：**
```sql
-- 对多个查询维度进行 Z-Order 优化
CALL spark_catalog.system.rewrite_data_files(
    table => 'my_database.events_table',
    strategy => 'z-order',
    sort_order => 'user_id, event_time, region_id',
    options => map(
        'target-file-size-bytes', '268435456',  -- 256MB
        'max-output-size', '2147483647',        -- 2GB
        'var-length-contribution', '8'          -- 8 bytes for var-length
    )
);
```

**Z-Order 优势分析：**

1. **多维度查询优化**
   ```sql
   -- 以下查询都能受益于 Z-Order
   SELECT * FROM events_table
   WHERE user_id = 12345 AND event_time > '2023-01-01';

   SELECT * FROM events_table
   WHERE region_id = 'us-west' AND user_id BETWEEN 10000 AND 20000;
   ```

2. **数据聚集效应**
   - 相关数据在物理存储上更接近
   - 减少 I/O 扫描范围
   - 提升压缩比率

3. **配置调优要点**
   ```java
   // 针对不同数据类型的优化配置
   Map<String, String> zOrderOptions = Map.of(
       "target-file-size-bytes", "268435456",     // 256MB 适合 Z-Order
       "max-output-size", String.valueOf(1024),   // 限制输出大小
       "var-length-contribution", "16",           // 字符串字段贡献字节数
       "compression-factor", "1.1"                // 考虑压缩后的大小
   );
   ```

### 3.4 策略对比矩阵

| 策略 | 执行速度 | 资源消耗 | 查询优化效果 | 适用数据量 | 最佳场景 |
|------|----------|----------|-------------|-----------|----------|
| **BIN-PACK** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐ | 任意 | 快速文件合并 |
| **SORT** | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | 中大型 | 范围查询优化 |
| **Z-ORDER** | ⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐⭐ | 大型 | 多维度查询 |

---

## 4. 配置优化最佳实践

### 4.1 核心配置参数详解

#### 4.1.1 文件大小相关配置

```java
// 核心文件大小配置
public static final String WRITE_TARGET_FILE_SIZE_BYTES = "write.target-file-size-bytes";
public static final long WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT = 128L * 1024 * 1024; // 128MB

// 分片大小配置
public static final String SPLIT_SIZE = "read.split.target-size";
public static final long SPLIT_SIZE_DEFAULT = 128 * 1024 * 1024; // 128 MB
```

**不同场景的文件大小建议：**

```sql
-- 1. 高频查询表（OLAP 场景）
ALTER TABLE my_table SET TBLPROPERTIES (
    'write.target-file-size-bytes' = '268435456',  -- 256MB，提升查询性能
    'read.split.target-size' = '268435456'         -- 匹配写入大小
);

-- 2. 流式写入表（实时场景）
ALTER TABLE streaming_table SET TBLPROPERTIES (
    'write.target-file-size-bytes' = '134217728',  -- 128MB，平衡实时性和性能
    'read.split.target-size' = '67108864'          -- 64MB，提升并行度
);

-- 3. 归档存储表（存储优化）
ALTER TABLE archive_table SET TBLPROPERTIES (
    'write.target-file-size-bytes' = '1073741824', -- 1GB，最大化压缩效果
    'read.split.target-size' = '536870912'         -- 512MB
);
```

#### 4.1.2 Compaction 触发条件配置

```sql
-- 基于删除比例的触发配置
CALL spark_catalog.system.rewrite_data_files(
    table => 'my_table',
    strategy => 'binpack',
    options => map(
        'delete-file-threshold', '50',          -- 50个删除文件时触发
        'delete-ratio-threshold', '0.2',        -- 删除比例20%时触发
        'min-input-files', '3',                 -- 最少3个文件才合并
        'min-file-size-bytes', '67108864'       -- 小于64MB的文件参与合并
    )
);
```

#### 4.1.3 并发控制优化

```sql
-- 大表的并发重写配置
CALL spark_catalog.system.rewrite_data_files(
    table => 'large_table',
    strategy => 'binpack',
    options => map(
        'max-concurrent-file-group-rewrites', '20',    -- 增加并发度
        'max-file-group-size-bytes', '53687091200',    -- 50GB per group
        'partial-progress.enabled', 'true',            -- 启用部分进度提交
        'partial-progress.max-commits', '10'           -- 最多10次中间提交
    )
);
```

### 4.2 分区级别优化策略

#### 4.2.1 动态分区处理

```java
// 分区感知的文件重写策略
public class PartitionAwareCompaction {
    public void compactByPartition(String tableName) {
        // 获取需要优化的分区列表
        List<String> partitionsToCompact = identifyProblematicPartitions(tableName);

        for (String partition : partitionsToCompact) {
            // 基于分区的选择性重写
            rewriteDataFiles(tableName)
                .filter(Expressions.equal("date_partition", partition))
                .binPack()
                .option("target-file-size-bytes", calculateOptimalSize(partition))
                .execute();
        }
    }
}
```

#### 4.2.2 分区维度的配置示例

```sql
-- 按分区优化不同配置
-- 热分区（当前月份）- 优化查询性能
CALL spark_catalog.system.rewrite_data_files(
    table => 'events_table',
    strategy => 'z-order',
    sort_order => 'user_id, event_time',
    where => "date_partition >= '2025-12-01'",
    options => map('target-file-size-bytes', '268435456')  -- 256MB
);

-- 温分区（近6个月）- 平衡性能和存储
CALL spark_catalog.system.rewrite_data_files(
    table => 'events_table',
    strategy => 'sort',
    sort_order => 'event_time DESC',
    where => "date_partition >= '2025-06-01' AND date_partition < '2025-12-01'",
    options => map('target-file-size-bytes', '536870912')  -- 512MB
);

-- 冷分区（历史数据）- 最大化压缩
CALL spark_catalog.system.rewrite_data_files(
    table => 'events_table',
    strategy => 'binpack',
    where => "date_partition < '2025-06-01'",
    options => map('target-file-size-bytes', '1073741824') -- 1GB
);
```

### 4.3 自动化优化策略

#### 4.3.1 基于指标的触发机制

```python
# Python 脚本：基于指标的自动优化
import pyarrow as pa
import pyiceberg
from pyiceberg.catalog import Catalog

def auto_optimize_table(catalog: Catalog, table_name: str):
    table = catalog.load_table(table_name)

    # 收集表统计信息
    file_stats = analyze_file_distribution(table)

    # 决策优化策略
    if file_stats.small_file_ratio > 0.3:
        # 小文件过多，使用 BIN-PACK
        strategy = "binpack"
        target_size = "268435456"  # 256MB
    elif file_stats.avg_file_age > 30:  # 30天
        # 老数据，使用大文件策略
        strategy = "binpack"
        target_size = "1073741824"  # 1GB
    else:
        # 新数据，保持查询优化
        strategy = "sort"
        target_size = "134217728"  # 128MB

    # 执行优化
    execute_compaction(table_name, strategy, target_size)

def analyze_file_distribution(table):
    # 分析文件大小分布
    files = table.scan().plan_files()
    total_files = len(files)
    small_files = sum(1 for f in files if f.file.file_size_in_bytes < 64 * 1024 * 1024)

    return FileStats(
        small_file_ratio=small_files / total_files,
        avg_file_size=sum(f.file.file_size_in_bytes for f in files) / total_files,
        avg_file_age=calculate_avg_age(files)
    )
```

#### 4.3.2 调度系统集成

```yaml
# Airflow DAG 配置示例
dag:
  dag_id: iceberg_auto_compaction
  schedule_interval: "0 2 * * *"  # 每天凌晨2点执行

tasks:
  - task_id: analyze_tables
    operator: PythonOperator
    python_callable: analyze_all_tables

  - task_id: compact_high_priority
    operator: SparkSubmitOperator
    application: compaction_job.py
    conf:
      spark.sql.adaptive.enabled: "true"
      spark.sql.adaptive.coalescePartitions.enabled: "true"

  - task_id: compact_low_priority
    operator: SparkSubmitOperator
    application: compaction_job.py
    conf:
      spark.sql.shuffle.partitions: "200"
      spark.executor.memory: "8g"
```

---

## 5. 性能监控与调优策略

### 5.1 关键性能指标监控

#### 5.1.1 文件分布指标

```sql
-- 查询文件大小分布
SELECT
    CASE
        WHEN file_size_in_bytes < 64*1024*1024 THEN 'Small (<64MB)'
        WHEN file_size_in_bytes < 128*1024*1024 THEN 'Medium (64-128MB)'
        WHEN file_size_in_bytes < 256*1024*1024 THEN 'Large (128-256MB)'
        ELSE 'Very Large (>256MB)'
    END as file_size_category,
    COUNT(*) as file_count,
    SUM(file_size_in_bytes) / (1024*1024*1024) as total_size_gb,
    AVG(file_size_in_bytes) / (1024*1024) as avg_size_mb
FROM my_database.my_table.files
GROUP BY 1
ORDER BY 1;

-- 分区级别的文件统计
SELECT
    partition_key,
    COUNT(*) as file_count,
    MIN(file_size_in_bytes)/(1024*1024) as min_size_mb,
    MAX(file_size_in_bytes)/(1024*1024) as max_size_mb,
    AVG(file_size_in_bytes)/(1024*1024) as avg_size_mb,
    SUM(file_size_in_bytes)/(1024*1024*1024) as total_size_gb
FROM (
    SELECT
        CONCAT_WS('/', partition.date_partition, partition.region) as partition_key,
        file_size_in_bytes
    FROM my_database.my_table.files
)
GROUP BY partition_key
ORDER BY file_count DESC;
```

#### 5.1.2 删除文件影响分析

```sql
-- 分析删除文件的影响
SELECT
    f.partition_key,
    COUNT(CASE WHEN f.content = 0 THEN 1 END) as data_files,
    COUNT(CASE WHEN f.content = 1 THEN 1 END) as delete_files,
    COUNT(CASE WHEN f.content = 1 THEN 1 END) * 1.0 /
        COUNT(CASE WHEN f.content = 0 THEN 1 END) as delete_ratio,
    SUM(CASE WHEN f.content = 0 THEN f.file_size_in_bytes ELSE 0 END)/(1024*1024*1024) as data_size_gb,
    SUM(CASE WHEN f.content = 1 THEN f.file_size_in_bytes ELSE 0 END)/(1024*1024) as delete_size_mb
FROM my_database.my_table.all_files f
GROUP BY f.partition_key
HAVING delete_ratio > 0.1  -- 删除比例超过10%的分区
ORDER BY delete_ratio DESC;
```

### 5.2 性能基准测试

#### 5.2.1 Compaction 前后性能对比

```python
# 性能测试脚本
import time
import statistics
from pyspark.sql import SparkSession

def benchmark_query_performance(spark: SparkSession, table_name: str, queries: list):
    """执行查询性能基准测试"""
    results = {}

    for query_name, sql in queries:
        execution_times = []

        # 预热查询
        spark.sql(sql).count()

        # 执行5次测试
        for _ in range(5):
            start_time = time.time()
            result_count = spark.sql(sql).count()
            end_time = time.time()
            execution_times.append(end_time - start_time)

        results[query_name] = {
            'avg_time': statistics.mean(execution_times),
            'min_time': min(execution_times),
            'max_time': max(execution_times),
            'std_dev': statistics.stdev(execution_times),
            'result_count': result_count
        }

    return results

# 测试查询集合
test_queries = [
    ("range_query", """
        SELECT COUNT(*) FROM events_table
        WHERE event_time BETWEEN '2025-12-01' AND '2025-12-15'
    """),
    ("filter_query", """
        SELECT * FROM events_table
        WHERE user_id = 12345 AND region = 'us-west'
        LIMIT 1000
    """),
    ("aggregation_query", """
        SELECT region, COUNT(*), SUM(revenue)
        FROM events_table
        WHERE date_partition >= '2025-12-01'
        GROUP BY region
    """)
]

# 执行对比测试
print("=== Before Compaction ===")
before_results = benchmark_query_performance(spark, "events_table", test_queries)

# 执行 Compaction
spark.sql("""
    CALL spark_catalog.system.rewrite_data_files(
        table => 'events_table',
        strategy => 'z-order',
        sort_order => 'user_id, event_time, region'
    )
""")

print("=== After Compaction ===")
after_results = benchmark_query_performance(spark, "events_table", test_queries)

# 生成性能对比报告
for query_name in test_queries:
    before_time = before_results[query_name]['avg_time']
    after_time = after_results[query_name]['avg_time']
    improvement = (before_time - after_time) / before_time * 100

    print(f"{query_name}: {before_time:.2f}s -> {after_time:.2f}s ({improvement:.1f}% improvement)")
```

### 5.3 资源使用优化

#### 5.3.1 Spark 配置调优

```python
# 针对不同 Compaction 策略的 Spark 配置
compaction_configs = {
    "binpack_small_files": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize": "64MB",
        "spark.executor.memory": "4g",
        "spark.executor.cores": "4",
        "spark.sql.shuffle.partitions": "200"
    },

    "sort_medium_files": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        "spark.executor.memory": "8g",
        "spark.executor.cores": "5",
        "spark.sql.shuffle.partitions": "400"
    },

    "zorder_large_files": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.executor.memory": "16g",
        "spark.executor.cores": "8",
        "spark.sql.shuffle.partitions": "800"
    }
}

def configure_spark_for_compaction(strategy: str, table_size_gb: int):
    """根据策略和表大小配置 Spark"""
    base_config = compaction_configs.get(strategy, compaction_configs["binpack_small_files"])

    # 根据表大小调整配置
    if table_size_gb > 1000:  # 1TB+
        base_config["spark.executor.memory"] = "32g"
        base_config["spark.executor.cores"] = "16"
        base_config["spark.sql.shuffle.partitions"] = str(table_size_gb * 2)

    return base_config
```

---

## 6. 生产环境实战案例

### 6.1 案例一：电商订单表优化

#### 6.1.1 问题背景
- **表规模**：10TB，1000万+文件
- **分区策略**：按日期+地区分区
- **主要问题**：小文件过多（平均8MB），查询性能差

#### 6.1.2 优化方案

```sql
-- 第一阶段：历史数据合并（使用 BIN-PACK）
CALL spark_catalog.system.rewrite_data_files(
    table => 'ecommerce.orders',
    strategy => 'binpack',
    where => "order_date < '2025-11-01'",  -- 历史数据
    options => map(
        'target-file-size-bytes', '1073741824',    -- 1GB，最大化压缩
        'max-concurrent-file-group-rewrites', '50',
        'max-file-group-size-bytes', '107374182400', -- 100GB
        'partial-progress.enabled', 'true'
    )
);

-- 第二阶段：近期数据排序优化（使用 SORT）
CALL spark_catalog.system.rewrite_data_files(
    table => 'ecommerce.orders',
    strategy => 'sort',
    sort_order => 'order_date DESC, customer_id ASC',
    where => "order_date >= '2025-11-01'",  -- 近期数据
    options => map(
        'target-file-size-bytes', '268435456',     -- 256MB，优化查询
        'compression-factor', '1.3'
    )
);

-- 第三阶段：热点查询优化（使用 Z-ORDER）
CALL spark_catalog.system.rewrite_data_files(
    table => 'ecommerce.orders',
    strategy => 'z-order',
    sort_order => 'customer_id, product_category, order_date',
    where => "order_date >= '2025-12-01'",  -- 当月数据
    options => map(
        'target-file-size-bytes', '134217728',     -- 128MB
        'max-output-size', '1073741824'            -- 1GB max
    )
);
```

#### 6.1.3 效果评估

**优化前后对比：**
```sql
-- 查询性能对比报告
SELECT
    'Before Optimization' as phase,
    COUNT(*) as file_count,
    MIN(file_size_in_bytes)/(1024*1024) as min_size_mb,
    AVG(file_size_in_bytes)/(1024*1024) as avg_size_mb,
    MAX(file_size_in_bytes)/(1024*1024) as max_size_mb
FROM ecommerce.orders.files
WHERE snapshot_id = 8901234567890  -- 优化前快照
UNION ALL
SELECT
    'After Optimization' as phase,
    COUNT(*) as file_count,
    MIN(file_size_in_bytes)/(1024*1024) as min_size_mb,
    AVG(file_size_in_bytes)/(1024*1024) as avg_size_mb,
    MAX(file_size_in_bytes)/(1024*1024) as max_size_mb
FROM ecommerce.orders.files
WHERE snapshot_id = 8901234567891; -- 优化后快照
```

**结果总结：**
- 文件数量减少：1000万+ → 5万+ (减少99.5%)
- 平均文件大小提升：8MB → 256MB (提升32倍)
- 查询性能提升：平均提升60-80%
- 存储成本下降：压缩效果提升15%

### 6.2 案例二：IoT 传感器数据优化

#### 6.2.1 业务特点
- **数据特征**：高频小批次写入，每分钟1000+文件
- **查询模式**：主要按设备ID和时间范围查询
- **数据量**：每天500GB新增，文件大小1-5MB

#### 6.2.2 自动化优化流程

```python
# 自动化优化脚本
from datetime import datetime, timedelta
import logging

class IoTDataCompactionManager:
    def __init__(self, spark_session, catalog_name, table_name):
        self.spark = spark_session
        self.catalog = catalog_name
        self.table = f"{catalog_name}.{table_name}"
        self.logger = logging.getLogger(__name__)

    def daily_compaction_routine(self):
        """每日自动优化例程"""

        # 1. 压缩昨天的数据（实时性要求较低）
        yesterday = datetime.now() - timedelta(days=1)
        yesterday_partition = yesterday.strftime('%Y-%m-%d')

        self.logger.info(f"Starting compaction for partition: {yesterday_partition}")

        # 检查文件分布
        file_stats = self._analyze_partition_files(yesterday_partition)

        if file_stats['small_file_ratio'] > 0.7:  # 小文件比例超过70%
            self._execute_binpack_compaction(yesterday_partition)

        # 2. 优化过去7天的热查询数据（使用Z-ORDER）
        week_ago = datetime.now() - timedelta(days=7)
        week_partition = week_ago.strftime('%Y-%m-%d')

        if self._should_zorder_optimize(week_partition):
            self._execute_zorder_compaction(week_partition)

        # 3. 归档30天前的数据（大文件策略）
        month_ago = datetime.now() - timedelta(days=30)
        month_partition = month_ago.strftime('%Y-%m-%d')

        self._execute_archive_compaction(month_partition)

    def _execute_binpack_compaction(self, partition_date):
        """执行BIN-PACK压缩"""
        sql = f"""
        CALL {self.catalog}.system.rewrite_data_files(
            table => '{self.table}',
            strategy => 'binpack',
            where => "date_partition = '{partition_date}'",
            options => map(
                'target-file-size-bytes', '134217728',  -- 128MB
                'max-concurrent-file-group-rewrites', '10',
                'delete-ratio-threshold', '0.15'
            )
        )
        """

        self.spark.sql(sql).collect()
        self.logger.info(f"BIN-PACK compaction completed for {partition_date}")

    def _execute_zorder_compaction(self, partition_date):
        """执行Z-ORDER优化"""
        sql = f"""
        CALL {self.catalog}.system.rewrite_data_files(
            table => '{self.table}',
            strategy => 'z-order',
            sort_order => 'device_id, sensor_type, timestamp',
            where => "date_partition = '{partition_date}'",
            options => map(
                'target-file-size-bytes', '268435456',  -- 256MB
                'max-output-size', '536870912'          -- 512MB max
            )
        )
        """

        self.spark.sql(sql).collect()
        self.logger.info(f"Z-ORDER compaction completed for {partition_date}")

    def _execute_archive_compaction(self, partition_date):
        """执行归档压缩"""
        sql = f"""
        CALL {self.catalog}.system.rewrite_data_files(
            table => '{self.table}',
            strategy => 'binpack',
            where => "date_partition = '{partition_date}'",
            options => map(
                'target-file-size-bytes', '1073741824',  -- 1GB
                'max-file-group-size-bytes', '107374182400'  -- 100GB
            )
        )
        """

        self.spark.sql(sql).collect()
        self.logger.info(f"Archive compaction completed for {partition_date}")

# 使用示例
if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("IoT Data Compaction") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

    manager = IoTDataCompactionManager(spark, "iot_catalog", "sensor_data")
    manager.daily_compaction_routine()
```

### 6.3 案例三：数据湖 ETL 管道优化

#### 6.3.1 复杂 ETL 场景
- **数据源**：多个上游系统，不同写入模式
- **处理逻辑**：复杂的 MERGE 操作
- **性能要求**：SLA 要求2小时内完成处理

#### 6.3.2 端到端优化方案

```sql
-- 第一步：识别需要优化的表
WITH table_stats AS (
    SELECT
        table_name,
        COUNT(*) as file_count,
        AVG(file_size_in_bytes) as avg_file_size,
        SUM(file_size_in_bytes) as total_size,
        COUNT(CASE WHEN file_size_in_bytes < 64*1024*1024 THEN 1 END) as small_files
    FROM INFORMATION_SCHEMA.FILES
    WHERE table_schema = 'etl_pipeline'
    GROUP BY table_name
),
compaction_candidates AS (
    SELECT
        table_name,
        file_count,
        avg_file_size/(1024*1024) as avg_size_mb,
        total_size/(1024*1024*1024) as total_size_gb,
        small_files * 100.0 / file_count as small_file_percentage
    FROM table_stats
    WHERE file_count > 100  -- 超过100个文件
    AND (small_files * 100.0 / file_count > 30  -- 小文件比例超过30%
         OR file_count > 10000)  -- 或者文件数超过1万
)
SELECT * FROM compaction_candidates
ORDER BY small_file_percentage DESC, file_count DESC;

-- 第二步：执行分层优化
-- Layer 1: 原始数据层（ODS）- 快速合并
CREATE OR REPLACE PROCEDURE optimize_ods_tables()
LANGUAGE SQL
AS $$
DECLARE
    table_record RECORD;
BEGIN
    FOR table_record IN
        SELECT table_name FROM compaction_candidates
        WHERE table_name LIKE 'ods_%'
    LOOP
        CALL spark_catalog.system.rewrite_data_files(
            table => 'etl_pipeline.' || table_record.table_name,
            strategy => 'binpack',
            options => map(
                'target-file-size-bytes', '268435456',  -- 256MB
                'max-concurrent-file-group-rewrites', '20'
            )
        );
    END LOOP;
END;
$$;

-- Layer 2: 数据仓库层（DW）- 查询优化
CREATE OR REPLACE PROCEDURE optimize_dw_tables()
LANGUAGE SQL
AS $$
DECLARE
    table_record RECORD;
BEGIN
    FOR table_record IN
        SELECT table_name FROM compaction_candidates
        WHERE table_name LIKE 'dw_%'
    LOOP
        CALL spark_catalog.system.rewrite_data_files(
            table => 'etl_pipeline.' || table_record.table_name,
            strategy => 'z-order',
            sort_order => CASE
                WHEN table_record.table_name LIKE '%_fact' THEN 'date_key, customer_key'
                WHEN table_record.table_name LIKE '%_dim' THEN 'surrogate_key'
                ELSE 'created_date'
            END,
            options => map(
                'target-file-size-bytes', '134217728',  -- 128MB
                'compression-factor', '1.2'
            )
        );
    END LOOP;
END;
$$;

-- 第三步：定时执行优化
-- 通过 Airflow 或其他调度系统执行
```

#### 6.3.3 监控与告警

```python
# 监控脚本
import boto3
import json
from datetime import datetime

class CompactionMonitor:
    def __init__(self, aws_region='us-west-2'):
        self.cloudwatch = boto3.client('cloudwatch', region_name=aws_region)
        self.sns = boto3.client('sns', region_name=aws_region)

    def check_table_health(self, table_name):
        """检查表的健康状况"""
        metrics = self._collect_table_metrics(table_name)

        alerts = []

        # 检查小文件比例
        if metrics['small_file_ratio'] > 0.4:
            alerts.append({
                'severity': 'WARNING',
                'message': f'Small file ratio {metrics["small_file_ratio"]:.1%} exceeds threshold',
                'metric': 'small_file_ratio',
                'value': metrics['small_file_ratio']
            })

        # 检查查询性能
        if metrics['avg_query_time'] > metrics['baseline_query_time'] * 1.5:
            alerts.append({
                'severity': 'CRITICAL',
                'message': f'Query performance degraded by {(metrics["avg_query_time"]/metrics["baseline_query_time"]-1):.1%}',
                'metric': 'query_performance',
                'value': metrics['avg_query_time']
            })

        # 发送告警
        if alerts:
            self._send_alerts(table_name, alerts)

        return alerts

    def _send_alerts(self, table_name, alerts):
        """发送告警通知"""
        message = {
            'table': table_name,
            'timestamp': datetime.now().isoformat(),
            'alerts': alerts
        }

        self.sns.publish(
            TopicArn='arn:aws:sns:us-west-2:123456789:iceberg-alerts',
            Message=json.dumps(message, indent=2),
            Subject=f'Iceberg Table Alert: {table_name}'
        )

# 集成到监控系统
def lambda_handler(event, context):
    """AWS Lambda 监控函数"""
    monitor = CompactionMonitor()

    # 检查关键表
    critical_tables = [
        'etl_pipeline.dw_sales_fact',
        'etl_pipeline.dw_customer_dim',
        'analytics.user_events'
    ]

    for table in critical_tables:
        alerts = monitor.check_table_health(table)

        if alerts:
            print(f"Alerts found for {table}: {len(alerts)} issues")
```

---

## 结论与建议

### 7.1 最佳实践总结

1. **策略选择原则**
   - **BIN-PACK**：适合快速文件合并，优先处理小文件问题
   - **SORT**：适合有明确查询模式的场景，提升范围查询性能
   - **Z-ORDER**：适合多维度查询，复杂过滤条件的场景

2. **配置参数建议**
   - 文件大小：根据查询模式在128MB-1GB间选择
   - 并发度：基于集群资源和表大小动态调整
   - 触发条件：结合删除比例和文件数量制定策略

3. **运维管理要点**
   - 建立监控体系，及时发现性能问题
   - 实施分层优化，区别对待不同业务场景
   - 自动化执行，减少人工干预

### 7.2 技术发展趋势

随着 Iceberg 生态的不断完善，小文件优化将朝着更加智能化、自动化的方向发展：

1. **智能策略选择**：基于查询模式自动选择最优压缩策略
2. **实时优化**：支持写入过程中的实时文件合并
3. **成本感知优化**：结合存储成本进行优化决策
4. **多引擎协同**：Spark、Flink、Trino 等引擎的统一优化

通过系统性的优化实践，能够将 Iceberg 表的查询性能提升数倍，并显著降低存储和计算成本，为构建高性能数据湖奠定坚实基础。