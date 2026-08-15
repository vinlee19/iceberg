# Apache Iceberg 数据读取机制详细流程图补充

## 1. Time Travel 数据读取详细流程图

### 1.1 Time Travel 核心架构图

```
Time Travel 数据读取架构:

┌─────────────────────────────────────────────────────────────────────────┐
│                        Time Travel Query Flow                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Client Query                                                          │
│       │                                                                │
│       ▼                                                                │
│  ┌─────────────────┐                                                  │
│  │ Time Travel     │                                                  │
│  │ Request         │                                                  │
│  │                 │                                                  │
│  │ - asOfTime()    │                                                  │
│  │ - useSnapshot() │                                                  │
│  │ - useRef()      │                                                  │
│  └─────────────────┘                                                  │
│           │                                                            │
│           ▼                                                            │
│  ┌─────────────────┐       ┌─────────────────┐                       │
│  │ SnapshotUtil    │────── │ Table History   │                       │
│  │                 │       │                 │                       │
│  │ - asOfTime()    │       │ - HistoryEntry[]│                       │
│  │ - schemaFor()   │       │ - Timestamps    │                       │
│  │ - ancestorIds() │       │ - SnapshotIds   │                       │
│  └─────────────────┘       └─────────────────┘                       │
│           │                                                            │
│           ▼                                                            │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │              Snapshot Resolution Process                        │  │
│  │                                                                 │  │
│  │  Time Travel Type:                                              │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │  │
│  │  │asOfTime()   │  │useSnapshot()│  │useRef()     │             │  │
│  │  │             │  │             │  │             │             │  │
│  │  │Binary Search│  │Direct Lookup│  │Ref Lookup   │             │  │
│  │  │by Timestamp │  │by ID        │  │Branch/Tag   │             │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘             │  │
│  └─────────────────────────────────────────────────────────────────┘  │
│           │                                                            │
│           ▼                                                            │
│  ┌─────────────────┐                                                  │
│  │Target Snapshot  │                                                  │
│  │                 │                                                  │
│  │ - SnapshotId    │                                                  │
│  │ - Schema        │                                                  │
│  │ - ManifestList  │                                                  │
│  │ - Timestamp     │                                                  │
│  └─────────────────┘                                                  │
│           │                                                            │
│           ▼                                                            │
│  ┌─────────────────┐                                                  │
│  │Regular Read     │                                                  │
│  │Flow Continues   │                                                  │
│  │(See Main Flow)  │                                                  │
│  └─────────────────┘                                                  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Time Travel 详细执行流程

```java
Time Travel 执行步骤详解:

Step 1: 时间戳解析
┌─────────────────────────────────────┐
│ Input: timestampMillis              │
│        "2024-01-15T10:30:00.000Z"   │
└─────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────┐
│ SnapshotUtil.nullableSnapshotId     │
│ AsOfTime(table, timestampMillis)    │
│                                     │
│ for (HistoryEntry entry : history)  │
│   if (entry.timestamp <= target)    │
│     snapshotId = entry.snapshotId   │
│                                     │
│ Binary Search through History       │
└─────────────────────────────────────┘
                    │
                    ▼
Step 2: 快照验证和Schema解析
┌─────────────────────────────────────┐
│ Preconditions.checkArgument(        │
│   snapshotId != null)               │
│                                     │
│ Snapshot snapshot =                 │
│   table.snapshot(snapshotId)        │
│                                     │
│ Schema schema = useSnapshotSchema() │
│   ? SnapshotUtil.schemaFor(table,   │
│       snapshotId)                   │
│   : tableSchema()                   │
└─────────────────────────────────────┘
                    │
                    ▼
Step 3: 扫描上下文更新
┌─────────────────────────────────────┐
│ TableScanContext newContext =       │
│   context().useSnapshotId(          │
│     snapshotId)                     │
│                                     │
│ return newRefinedScan(              │
│   table, schema, newContext)        │
└─────────────────────────────────────┘
                    │
                    ▼
Step 4: 标准读取流程
┌─────────────────────────────────────┐
│ Follow Standard Data Reading Flow   │
│ with Historical Snapshot Context    │
└─────────────────────────────────────┘
```

### 1.3 Time Travel 三种模式对比

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Time Travel Modes Comparison                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Mode 1: asOfTime(timestampMillis)                                     │
│  ┌───────────────────────────────────────────────────────────────────┐ │
│  │ Timeline: S1───S2───S3───S4───S5                                  │ │
│  │           ↑    ↑    ↑    ↑    ↑                                   │ │
│  │           T1   T2   T3   T4   T5                                  │ │
│  │                                                                   │ │
│  │ Query: asOfTime(T2.5)  → Returns S2 (Latest before T2.5)         │ │
│  │ Query: asOfTime(T3)    → Returns S3 (Exact match)                │ │
│  │ Query: asOfTime(T0)    → Error (No snapshot before T0)           │ │
│  └───────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  Mode 2: useSnapshot(snapshotId)                                       │
│  ┌───────────────────────────────────────────────────────────────────┐ │
│  │ Direct Snapshot Access:                                           │ │
│  │                                                                   │ │
│  │ Input: snapshotId = 12345                                         │ │
│  │ Process: table.snapshot(12345)                                    │ │
│  │ Result: Direct access to specific snapshot                        │ │
│  │         OR Error if snapshot doesn't exist                        │ │
│  └───────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  Mode 3: useRef(branchOrTag)                                           │
│  ┌───────────────────────────────────────────────────────────────────┐ │
│  │ Branch/Tag Resolution:                                             │ │
│  │                                                                   │ │
│  │ Main Branch:     S1───S2───S3───S4───S5                           │ │
│  │                            │                                      │ │
│  │ Feature Branch:            └───F1───F2                            │ │
│  │                                                                   │ │
│  │ Tag "v1.0":               S3                                      │ │
│  │                                                                   │ │
│  │ Query: useRef("main")      → S5 (Latest on main)                  │ │
│  │ Query: useRef("feature")   → F2 (Latest on feature)               │ │
│  │ Query: useRef("v1.0")      → S3 (Tagged snapshot)                 │ │
│  └───────────────────────────────────────────────────────────────────┘ │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## 2. Parquet 文件存储结构和读取流程

### 2.1 Parquet 文件存储结构图

```
Parquet File Structure:

┌─────────────────────────────────────────────────────────────────────────┐
│                           Parquet File Layout                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  File Header                                                            │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Magic Number: "PAR1" (4 bytes)                                  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Row Group 1                                                            │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Column Chunk 1 (e.g., "id" column)                              │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ Page Header                                                 │ │   │
│  │ │ ┌─────────────────────────────────────────────────────────┐ │ │   │
│  │ │ │ - Page Type (DATA_PAGE/INDEX_PAGE/DICTIONARY_PAGE)      │ │ │   │
│  │ │ │ - Uncompressed Size                                     │ │ │   │
│  │ │ │ - Compressed Size                                       │ │ │   │
│  │ │ │ - CRC Checksum                                          │ │ │   │
│  │ │ │ - Statistics (Min/Max/Null Count)                       │ │ │   │
│  │ │ └─────────────────────────────────────────────────────────┘ │ │   │
│  │ │ Data Page 1                                                 │ │   │
│  │ │ ┌─────────────────────────────────────────────────────────┐ │ │   │
│  │ │ │ Repetition Levels (if nested)                          │ │ │   │
│  │ │ │ Definition Levels (for nulls)                          │ │ │   │
│  │ │ │ Encoded Values (Dictionary/Plain/Delta)                │ │ │   │
│  │ │ └─────────────────────────────────────────────────────────┘ │ │   │
│  │ │ Data Page 2, Data Page 3, ... Data Page N                  │ │   │
│  │ │                                                             │ │   │
│  │ │ Dictionary Page (if dictionary encoding used)               │ │   │
│  │ │ ┌─────────────────────────────────────────────────────────┐ │ │   │
│  │ │ │ Dictionary Values                                       │ │ │   │
│  │ │ │ [Value1, Value2, Value3, ..., ValueN]                  │ │ │   │
│  │ │ └─────────────────────────────────────────────────────────┘ │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ Column Chunk 2 (e.g., "name" column)                           │   │
│  │ Column Chunk 3 (e.g., "created_at" column)                     │   │
│  │ ...                                                             │   │
│  │ Column Chunk N                                                  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Row Group 2, Row Group 3, ... Row Group M                             │
│                                                                         │
│  Column Index (Page Index) - Optional (Parquet 1.12+)                  │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ For each Column Chunk:                                          │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ Page Statistics Index                                       │ │   │
│  │ │ - Min/Max values for each page                              │ │   │
│  │ │ - Null counts for each page                                 │ │   │
│  │ │ - Page locations and sizes                                  │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ Offset Index                                                │ │   │
│  │ │ - Page locations within the row group                      │ │   │
│  │ │ - First row index of each page                              │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Bloom Filter (Optional)                                                │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ For selected columns:                                           │   │
│  │ - Bloom filter for each column chunk                           │   │
│  │ - Configurable false positive rate                             │   │
│  │ - Used for fast existence checks                               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  File Footer                                                            │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Row Group Metadata                                              │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ For each Row Group:                                         │ │   │
│  │ │ - Total byte size                                           │ │   │
│  │ │ - Number of rows                                            │ │   │
│  │ │ - Column metadata for each column                           │ │   │
│  │ │   * Data type                                               │ │   │
│  │ │   * Encodings used                                          │ │   │
│  │ │   * Path in schema                                          │ │   │
│  │ │   * Statistics (min, max, null_count, distinct_count)       │ │   │
│  │ │   * Compression codec                                       │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ Schema Definition                                               │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ message schema {                                            │ │   │
│  │ │   required int64 id;                                        │ │   │
│  │ │   optional binary name (UTF8);                              │ │   │
│  │ │   required int64 created_at;                                │ │   │
│  │ │   optional binary status (UTF8);                            │ │   │
│  │ │ }                                                           │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ Key-Value Metadata                                              │   │
│  │ Version, Created By, etc.                                       │   │
│  │                                                                 │   │
│  │ Footer Length (4 bytes)                                         │   │
│  │ Magic Number: "PAR1" (4 bytes)                                  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Parquet 读取优化流程

```java
Parquet Reading Optimization Flow:

Step 1: 文件打开和元数据读取
┌─────────────────────────────────────────────────────────────────────────┐
│ ParquetFileReader.open(inputFile)                                       │
│                                                                         │
│ 1. Read Footer (from end of file)                                       │
│    ┌─────────────────────────────────────────────────────────────────┐ │
│    │ - Magic number verification                                     │ │
│    │ - Footer length                                                 │ │
│    │ - File metadata (schema, row groups, key-value pairs)           │ │
│    │ - Row group metadata (statistics, column metadata)              │ │
│    └─────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│ 2. Schema Compatibility Check                                           │
│    ┌─────────────────────────────────────────────────────────────────┐ │
│    │ - Map Iceberg schema to Parquet schema                         │ │
│    │ - Handle schema evolution (missing columns, type promotion)     │ │
│    │ - Create column projections                                     │ │
│    └─────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
Step 2: Row Group过滤 (File-level Statistics)
┌─────────────────────────────────────────────────────────────────────────┐
│ InclusiveMetricsEvaluator.eval(RowGroupMetadata)                        │
│                                                                         │
│ For each Row Group:                                                     │
│ ┌─────────────────────────────────────────────────────────────────────┐ │
│ │ Check Filter Predicate against Row Group Statistics:                │ │
│ │                                                                     │ │
│ │ Example: WHERE id > 1000 AND status = 'ACTIVE'                     │ │
│ │                                                                     │ │
│ │ Row Group Statistics:                                               │ │
│ │ - id: min=500, max=2000, null_count=0                             │ │
│ │ - status: min='ACTIVE', max='INACTIVE', null_count=10              │ │
│ │                                                                     │ │
│ │ Evaluation:                                                         │ │
│ │ - id > 1000: max(2000) > 1000 ✓ (cannot skip)                     │ │
│ │ - status = 'ACTIVE': 'ACTIVE' in [min, max] ✓ (cannot skip)       │ │
│ │                                                                     │ │
│ │ Result: Include this Row Group                                      │ │
│ └─────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
Step 3: Page-level过滤 (Page Index - Parquet 1.12+)
┌─────────────────────────────────────────────────────────────────────────┐
│ if (pageIndexEnabled && hasPageIndex) {                                 │
│                                                                         │
│   For each Column Chunk with Page Index:                               │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │ Read Column Index (Page Statistics)                             │   │
│   │ ┌─────────────────────────────────────────────────────────────┐ │   │
│   │ │ For each Data Page in Column Chunk:                        │ │   │
│   │ │ - Page min/max values                                      │ │   │
│   │ │ - Page null counts                                         │ │   │
│   │ │ - Page row counts                                          │ │   │
│   │ └─────────────────────────────────────────────────────────────┘ │   │
│   │                                                                 │   │
│   │ Apply Filter to Page Statistics                                 │   │
│   │ ┌─────────────────────────────────────────────────────────────┐ │   │
│   │ │ Skip pages where predicate cannot be satisfied             │ │   │
│   │ │ Example: Page has max(id)=800, but filter is id>1000       │ │   │
│   │ │ → Skip entire page                                         │ │   │
│   │ └─────────────────────────────────────────────────────────────┘ │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│ }                                                                       │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
Step 4: Dictionary过滤
┌─────────────────────────────────────────────────────────────────────────┐
│ if (dictionaryFilterEnabled) {                                          │
│                                                                         │
│   For Dictionary-encoded Columns:                                      │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │ Read Dictionary Page                                            │   │
│   │ ┌─────────────────────────────────────────────────────────────┐ │   │
│   │ │ Dictionary: [ACTIVE, INACTIVE, PENDING, CANCELLED]         │ │   │
│   │ └─────────────────────────────────────────────────────────────┘ │   │
│   │                                                                 │   │
│   │ Apply Filter to Dictionary                                      │   │
│   │ ┌─────────────────────────────────────────────────────────────┐ │   │
│   │ │ Example: status = 'ACTIVE'                                  │ │   │
│   │ │ Result: Only dictionary entry 0 ('ACTIVE') matches         │ │   │
│   │ │ → Can skip rows with other dictionary indices               │ │   │
│   │ └─────────────────────────────────────────────────────────────┘ │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│ }                                                                       │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
Step 5: Bloom Filter过滤 (Optional)
┌─────────────────────────────────────────────────────────────────────────┐
│ if (bloomFilterEnabled && hasBloomFilter) {                             │
│                                                                         │
│   For Equality Predicates:                                             │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │ Example: id = 12345                                             │   │
│   │                                                                 │   │
│   │ bloomFilter.mightContain(12345)                                 │   │
│   │ ┌─────────────────────────────────────────────────────────────┐ │   │
│   │ │ if (false) {                                                │ │   │
│   │ │   // Definitely not present, skip row group               │ │   │
│   │ │   skip this row group                                      │ │   │
│   │ │ } else {                                                   │ │   │
│   │ │   // Might be present, continue processing                 │ │   │
│   │ │ }                                                          │ │   │
│   │ └─────────────────────────────────────────────────────────────┘ │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│ }                                                                       │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
Step 6: 向量化读取 (Vectorized Reading)
┌─────────────────────────────────────────────────────────────────────────┐
│ if (vectorizationEnabled) {                                             │
│   VectorizedParquetReader.readBatch(batchSize)                         │
│ } else {                                                                │
│   ParquetReader.read() // Row-by-row                                   │
│ }                                                                       │
│                                                                         │
│ ┌─────────────────────────────────────────────────────────────────────┐ │
│ │ Batch Processing (Vectorized):                                      │ │
│ │                                                                     │ │
│ │ 1. Read column vectors in batches (e.g., 1024 rows)                │ │
│ │ 2. Apply remaining filters on vectors                              │ │
│ │ 3. Handle null values using definition levels                      │ │
│ │ 4. Decode values (dictionary → actual values)                      │ │
│ │ 5. Apply type conversions if needed                                │ │
│ └─────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.3 Parquet 读取代码示例

```java
// Parquet读取优化示例
public class ParquetReadingOptimization {
    
    public CloseableIterable<Record> readParquetFile(
        InputFile file, Schema projection, Expression filter) {
        
        // 1. 创建Parquet读取选项
        ParquetReadOptions.Builder optionsBuilder = ParquetReadOptions.builder();
        
        // 2. 配置统计信息过滤
        if (filter != Expressions.alwaysTrue()) {
            optionsBuilder.withRecordFilter(
                ParquetFilters.convert(projection, filter, caseSensitive)
            );
        }
        
        // 3. 启用Page Index过滤 (Parquet 1.12+)
        optionsBuilder.usePageChecksumVerification(true);
        optionsBuilder.useStatisticsFilter(true);
        
        // 4. 启用字典过滤
        optionsBuilder.useDictionaryFilter(true);
        
        // 5. 启用布隆过滤器 (如果配置)
        if (hasBloomFilterConfig(projection)) {
            configureBloomFilters(optionsBuilder, projection);
        }
        
        // 6. 选择读取模式
        if (enableVectorization) {
            return new VectorizedParquetReader<>(
                file, projection, filter, optionsBuilder.build()
            );
        } else {
            return new ParquetReader<>(
                file, projection, filter, optionsBuilder.build()
            );
        }
    }
    
    private void configureBloomFilters(
        ParquetReadOptions.Builder builder, Schema projection) {
        
        for (Types.NestedField field : projection.asStruct().fields()) {
            String columnName = field.name();
            String configKey = PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + columnName;
            
            if (Boolean.parseBoolean(tableProperties.get(configKey))) {
                builder.enableBloomFilter(columnName);
            }
        }
    }
}
```

## 3. ORC 文件存储结构和读取流程

### 3.1 ORC 文件存储结构图

```
ORC File Structure:

┌─────────────────────────────────────────────────────────────────────────┐
│                             ORC File Layout                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  File Header                                                            │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Magic: "ORC" (3 bytes)                                          │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Stripe 1                                                               │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Index Data                                                      │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ Row Index (for each column)                                 │ │   │
│  │ │ ┌─────────────────────────────────────────────────────────┐ │ │   │
│  │ │ │ RowIndexEntry:                                          │ │ │   │
│  │ │ │ - positions: [stream position for each encoding]        │ │ │   │
│  │ │ │ - statistics: ColumnStatistics                          │ │ │   │
│  │ │ │   * min/max values                                      │ │ │   │
│  │ │ │   * null count                                          │ │ │   │
│  │ │ │   * hasNull flag                                        │ │ │   │
│  │ │ │   * sum (for numeric types)                             │ │ │   │
│  │ │ └─────────────────────────────────────────────────────────┘ │ │   │
│  │ │ Bloom Filter Index (optional)                              │ │   │
│  │ │ ┌─────────────────────────────────────────────────────────┐ │ │   │
│  │ │ │ For each column with bloom filter:                      │ │ │   │
│  │ │ │ - Bloom filter data per row group                       │ │ │   │
│  │ │ │ - Configurable hash functions and bits                  │ │ │   │
│  │ │ └─────────────────────────────────────────────────────────┘ │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ Column Data Streams                                             │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ Column 0 (Root Struct)                                      │ │   │
│  │ │ ┌─────────────────────────────────────────────────────────┐ │ │   │
│  │ │ │ PRESENT Stream (bit stream for nulls)                   │ │ │   │
│  │ │ │ [1,1,0,1,1,0,1...] (1=non-null, 0=null)                │ │ │   │
│  │ │ └─────────────────────────────────────────────────────────┘ │ │   │
│  │ │                                                             │ │   │
│  │ │ Column 1 (e.g., "id" - BIGINT)                             │ │   │
│  │ │ ┌─────────────────────────────────────────────────────────┐ │ │   │
│  │ │ │ PRESENT Stream: [1,1,1,1,1,1...]                        │ │ │   │
│  │ │ │ DATA Stream: [1001,1002,1003,1004...] (varint encoded) │ │ │   │
│  │ │ └─────────────────────────────────────────────────────────┘ │ │   │
│  │ │                                                             │ │   │
│  │ │ Column 2 (e.g., "name" - STRING)                           │ │   │
│  │ │ ┌─────────────────────────────────────────────────────────┐ │ │   │
│  │ │ │ PRESENT Stream: [1,0,1,1,0,1...]                        │ │ │   │
│  │ │ │ LENGTH Stream: [5,0,7,4,0,6...] (string lengths)        │ │ │   │
│  │ │ │ DATA Stream: [Alice,Bob,Charlie...] (concatenated)      │ │ │   │
│  │ │ │                                                         │ │ │   │
│  │ │ │ Dictionary Encoding (optional):                         │ │ │   │
│  │ │ │ DICTIONARY Stream: [Alice,Bob,Charlie,David]            │ │ │   │
│  │ │ │ DATA Stream: [0,1,2,3,0,1...] (dictionary indices)     │ │ │   │
│  │ │ └─────────────────────────────────────────────────────────┘ │ │   │
│  │ │                                                             │ │   │
│  │ │ Column 3 (e.g., "created_at" - TIMESTAMP)                  │ │   │
│  │ │ ┌─────────────────────────────────────────────────────────┐ │ │   │
│  │ │ │ PRESENT Stream: [1,1,1,1...]                            │ │ │   │
│  │ │ │ DATA Stream: [timestamp values] (varint encoded)        │ │ │   │
│  │ │ │ SECONDARY Stream: [nanoseconds] (for sub-second part)   │ │ │   │
│  │ │ └─────────────────────────────────────────────────────────┘ │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ Stripe Footer                                                   │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ Stream Information:                                         │ │   │
│  │ │ - For each column and stream type                           │ │   │
│  │ │ - Stream kind (PRESENT, DATA, LENGTH, DICTIONARY, etc.)     │ │   │
│  │ │ - Column ID                                                 │ │   │
│  │ │ - Stream length                                             │ │   │
│  │ │                                                             │ │   │
│  │ │ Column Encodings:                                           │ │   │
│  │ │ - Encoding type for each column                             │ │   │
│  │ │ - Dictionary size (if dictionary encoding)                 │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Stripe 2, Stripe 3, ... Stripe N                                      │
│                                                                         │
│  File Footer                                                            │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Type Information (Schema)                                       │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ struct<                                                     │ │   │
│  │ │   id:bigint,                                                │ │   │
│  │ │   name:string,                                              │ │   │
│  │ │   created_at:timestamp                                      │ │   │
│  │ │ >                                                           │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ Stripe Information                                              │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ For each Stripe:                                            │ │   │
│  │ │ - offset: file position of stripe                           │ │   │
│  │ │ - indexLength: size of index data                           │ │   │
│  │ │ - dataLength: size of column data                           │ │   │
│  │ │ - footerLength: size of stripe footer                       │ │   │
│  │ │ - numberOfRows: total rows in stripe                        │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ File Statistics                                                 │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ For each column:                                            │ │   │
│  │ │ - numberOfValues: total count                               │ │   │
│  │ │ - hasNull: contains null values                             │ │   │
│  │ │ - min/max: column-specific min/max values                   │ │   │
│  │ │ - sum: sum of numeric values                                │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ Metadata                                                        │   │
│  │ User-defined key-value pairs                                    │   │
│  │                                                                 │   │
│  │ PostScript                                                      │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ - footerLength: length of file footer                      │ │   │
│  │ │ - compression: compression codec used                       │ │   │
│  │ │ - compressionBlockSize: compression block size              │ │   │
│  │ │ - version: ORC format version                               │ │   │
│  │ │ - metadataLength: user metadata length                     │ │   │
│  │ │ - magic: "ORC" (3 bytes)                                   │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ PostScript Length (1 byte)                                      │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.2 ORC 读取优化流程

```java
ORC Reading Optimization Flow:

Step 1: 文件打开和PostScript读取
┌─────────────────────────────────────────────────────────────────────────┐
│ OrcFile.createReader(path, options)                                     │
│                                                                         │
│ 1. Read PostScript (from end of file)                                   │
│    ┌─────────────────────────────────────────────────────────────────┐ │
│    │ - Read last byte (PostScript length)                           │ │
│    │ - Read PostScript (compression, version, footer length)        │ │
│    │ - Determine compression codec                                   │ │
│    └─────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│ 2. Read File Footer                                                     │
│    ┌─────────────────────────────────────────────────────────────────┐ │
│    │ - Type information (schema)                                     │ │
│    │ - Stripe information (locations, sizes, row counts)            │ │
│    │ - File statistics (column-level min/max/null counts)           │ │
│    │ - User metadata                                                 │ │
│    └─────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
Step 2: Schema Evolution和投影
┌─────────────────────────────────────────────────────────────────────────┐
│ SchemaEvolution.createReader(fileSchema, readerSchema)                  │
│                                                                         │
│ ┌─────────────────────────────────────────────────────────────────────┐ │
│ │ Handle Schema Changes:                                              │ │
│ │                                                                     │ │
│ │ File Schema:    struct<id:bigint, name:string, age:int>            │ │
│ │ Reader Schema:  struct<id:bigint, name:string, salary:double>      │ │
│ │                                                                     │ │
│ │ Evolution Rules:                                                    │ │
│ │ - id: exact match ✓                                               │ │
│ │ - name: exact match ✓                                             │ │
│ │ - age: not requested, skip                                         │ │
│ │ - salary: missing in file, use default/null                       │ │
│ └─────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
Step 3: Stripe级别过滤 (File Statistics)
┌─────────────────────────────────────────────────────────────────────────┐
│ SearchArgument evaluation at Stripe level                               │
│                                                                         │
│ For each Stripe:                                                        │
│ ┌─────────────────────────────────────────────────────────────────────┐ │
│ │ Convert Iceberg Expression to ORC SearchArgument:                   │ │
│ │                                                                     │ │
│ │ Example: (id > 1000) AND (name = 'Alice')                          │ │
│ │                                                                     │ │
│ │ SearchArgument:                                                     │ │
│ │ - PredicateLeaf(id, GREATER_THAN, 1000)                           │ │
│ │ - PredicateLeaf(name, EQUALS, 'Alice')                            │ │
│ │ - ExpressionTree: AND(leaf1, leaf2)                               │ │
│ │                                                                     │ │
│ │ Stripe Statistics Check:                                            │ │
│ │ - id column: min=500, max=2000, nullCount=0                       │ │
│ │   → max(2000) > 1000, so stripe might contain matches             │ │
│ │ - name column: hasValues=true                                      │ │
│ │   → need to check row groups for exact match                      │ │
│ │                                                                     │ │
│ │ Result: Include stripe for further processing                       │ │
│ └─────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
Step 4: Row Group级别过滤 (Row Index)
┌─────────────────────────────────────────────────────────────────────────┐
│ For each included Stripe:                                               │
│                                                                         │
│ 1. Read Row Index                                                       │
│    ┌─────────────────────────────────────────────────────────────────┐ │
│    │ For each Column:                                                │ │
│    │ ┌─────────────────────────────────────────────────────────────┐ │ │
│    │ │ RowIndexEntry[] entries = readRowIndex(columnId)            │ │ │
│    │ │                                                             │ │ │
│    │ │ Each entry covers ~10,000 rows:                             │ │ │
│    │ │ - Entry 0: rows 0-9999, min=500, max=1200                  │ │ │
│    │ │ - Entry 1: rows 10000-19999, min=1201, max=1800            │ │ │
│    │ │ - Entry 2: rows 20000-29999, min=1801, max=2000            │ │ │
│    │ └─────────────────────────────────────────────────────────────┘ │ │
│    └─────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│ 2. Apply SearchArgument to Row Groups                                   │
│    ┌─────────────────────────────────────────────────────────────────┐ │
│    │ Predicate: id > 1000                                            │ │
│    │                                                                 │ │
│    │ Row Group 0: max(1200) > 1000 ✓ (include, partial match)       │ │
│    │ Row Group 1: min(1201) > 1000 ✓ (include, all match)           │ │
│    │ Row Group 2: min(1801) > 1000 ✓ (include, all match)           │ │
│    │                                                                 │ │
│    │ Result: All row groups need processing                          │ │
│    └─────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
Step 5: Bloom Filter检查 (Optional)
┌─────────────────────────────────────────────────────────────────────────┐
│ if (bloomFilterEnabled && hasEqualityPredicates) {                      │
│                                                                         │
│   For each Equality Predicate:                                         │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │ Example: name = 'Alice'                                         │   │
│   │                                                                 │   │
│   │ For each Row Group:                                             │   │
│   │ ┌─────────────────────────────────────────────────────────────┐ │   │
│   │ │ BloomFilter bloomFilter = readBloomFilter(column, rowGroup) │ │   │
│   │ │                                                             │ │   │
│   │ │ if (!bloomFilter.testString("Alice")) {                     │ │   │
│   │ │   // Definitely not present                                 │ │   │
│   │ │   skipRowGroup(rowGroup);                                   │ │   │
│   │ │ }                                                           │ │   │
│   │ │ // else: might be present, continue                         │ │   │
│   │ └─────────────────────────────────────────────────────────────┘ │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│ }                                                                       │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
Step 6: 向量化读取 (VectorizedRowBatch)
┌─────────────────────────────────────────────────────────────────────────┐
│ Create RecordReader with selected row groups                            │
│                                                                         │
│ VectorizedRowBatch batch = schema.createRowBatch(1024);                 │
│                                                                         │
│ while (recordReader.nextBatch(batch)) {                                 │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │ Process Vectorized Batch:                                       │   │
│   │                                                                 │   │
│   │ For each Column Vector:                                         │   │
│   │ ┌─────────────────────────────────────────────────────────────┐ │   │
│   │ │ LongColumnVector idVector = batch.cols[0]                   │ │   │
│   │ │ BytesColumnVector nameVector = batch.cols[1]                │ │   │
│   │ │                                                             │ │   │
│   │ │ Apply remaining filters:                                    │ │   │
│   │ │ - Vector operations for numeric predicates                  │ │   │
│   │ │ - String comparisons for text predicates                   │ │   │
│   │ │ - Null handling using isNull[] arrays                      │ │   │
│   │ └─────────────────────────────────────────────────────────────┘ │   │
│   │                                                                 │   │
│   │ Convert to output format (Iceberg Records)                      │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│ }                                                                       │
└─────────────────────────────────────────────────────────────────────────┘
```

## 4. Avro 文件存储结构和读取流程

### 4.1 Avro 文件存储结构图

```
Avro File Structure:

┌─────────────────────────────────────────────────────────────────────────┐
│                            Avro File Layout                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  File Header                                                            │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Magic: "Obj" + 0x01 (4 bytes)                                   │   │
│  │                                                                 │   │
│  │ File Metadata (Map<string, bytes>)                              │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ "avro.schema": JSON schema definition                       │ │   │
│  │ │ {                                                           │ │   │
│  │ │   "type": "record",                                         │ │   │
│  │ │   "name": "User",                                           │ │   │
│  │ │   "fields": [                                               │ │   │
│  │ │     {"name": "id", "type": "long"},                         │ │   │
│  │ │     {"name": "name", "type": ["null", "string"]},           │ │   │
│  │ │     {"name": "created_at", "type": "long"}                  │ │   │
│  │ │   ]                                                         │ │   │
│  │ │ }                                                           │ │   │
│  │ │                                                             │ │   │
│  │ │ "avro.codec": compression codec (e.g., "deflate", "snappy")│ │   │
│  │ │ Custom metadata: user-defined key-value pairs              │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ 16-byte Sync Marker                                             │   │
│  │ [random 16 bytes unique to this file]                          │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Data Block 1                                                           │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Block Header                                                    │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ Object Count (varint): number of objects in this block     │ │   │
│  │ │ Block Size (varint): byte size of serialized objects       │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ Serialized Objects                                              │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ Object 1: (Binary Avro Encoding)                           │ │   │
│  │ │ ┌─────────────────────────────────────────────────────────┐ │ │   │
│  │ │ │ id: varint encoded (e.g., 1001)                         │ │ │   │
│  │ │ │ name: union index + string length + UTF-8 bytes         │ │ │   │
│  │ │ │   Union index: 1 (non-null string)                     │ │ │   │
│  │ │ │   String length: 5 (varint)                            │ │ │   │
│  │ │ │   String data: "Alice" (UTF-8)                          │ │ │   │
│  │ │ │ created_at: varint encoded timestamp                    │ │ │   │
│  │ │ └─────────────────────────────────────────────────────────┘ │ │   │
│  │ │                                                             │ │   │
│  │ │ Object 2: (Binary Avro Encoding)                           │ │   │
│  │ │ ┌─────────────────────────────────────────────────────────┐ │ │   │
│  │ │ │ id: 1002                                                │ │ │   │
│  │ │ │ name: union index 0 (null) - no additional data         │ │ │   │
│  │ │ │ created_at: timestamp                                   │ │ │   │
│  │ │ └─────────────────────────────────────────────────────────┘ │ │   │
│  │ │                                                             │ │   │
│  │ │ Object 3, Object 4, ... Object N                           │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ Compressed Data (if codec specified)                            │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ If avro.codec = "deflate":                                  │ │   │
│  │ │ - Entire block is compressed using deflate                  │ │   │
│  │ │ - Object count and block size are uncompressed              │ │   │
│  │ │                                                             │ │   │
│  │ │ If avro.codec = "snappy":                                   │ │   │
│  │ │ - CRC32 checksum (4 bytes)                                  │ │   │
│  │ │ - Compressed block size (4 bytes)                           │ │   │
│  │ │ - Compressed data                                           │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ 16-byte Sync Marker                                             │   │
│  │ [same as header sync marker]                                    │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Data Block 2, Data Block 3, ... Data Block M                          │
│                                                                         │
│  特殊数据类型编码示例:                                                 │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Complex Types Encoding:                                         │   │
│  │                                                                 │   │
│  │ Arrays: [element1, element2, ...]                               │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ Count (varint): number of elements                          │ │   │
│  │ │ Element1, Element2, ..., ElementN (recursively encoded)     │ │   │
│  │ │ Zero terminator (varint 0)                                  │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ Maps: {key1: value1, key2: value2, ...}                        │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ Count (varint): number of key-value pairs                   │ │   │
│  │ │ Key1 (string), Value1, Key2 (string), Value2, ...          │ │   │
│  │ │ Zero terminator (varint 0)                                  │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ Unions: [null, string, int]                                     │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ Union index (varint): which type is selected                │ │   │
│  │ │ Value: encoded according to selected type                   │ │   │
│  │ │ Example: index=1, "hello" → 1 + 5 + "hello"               │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ Records: nested structures                                      │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ Field1, Field2, ..., FieldN                                │ │   │
│  │ │ (encoded in schema-defined order)                           │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Avro 读取流程

```java
Avro Reading Process:

Step 1: 文件头解析和Schema读取
┌─────────────────────────────────────────────────────────────────────────┐
│ AvroFileReader.open(inputFile)                                          │
│                                                                         │
│ 1. Read Magic Number                                                    │
│    ┌─────────────────────────────────────────────────────────────────┐ │
│    │ bytes[4] = read(4)                                              │ │
│    │ if (!Arrays.equals(bytes, AVRO_MAGIC)) {                        │ │
│    │   throw new IOException("Not a valid Avro file");               │ │
│    │ }                                                               │ │
│    └─────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│ 2. Read File Metadata                                                   │
│    ┌─────────────────────────────────────────────────────────────────┐ │
│    │ Map<String, byte[]> metadata = readMetadata()                   │ │
│    │                                                                 │ │
│    │ // Extract schema                                               │ │
│    │ String schemaJson = new String(                                 │ │
│    │   metadata.get("avro.schema"), StandardCharsets.UTF_8)         │ │
│    │ Schema writerSchema = new Schema.Parser().parse(schemaJson)     │ │
│    │                                                                 │ │
│    │ // Extract codec                                                │ │
│    │ String codecName = new String(                                  │ │
│    │   metadata.get("avro.codec"), StandardCharsets.UTF_8)          │ │
│    │ Codec codec = CodecFactory.fromString(codecName)                │ │
│    └─────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│ 3. Read Sync Marker                                                     │
│    ┌─────────────────────────────────────────────────────────────────┐ │
│    │ byte[16] syncMarker = read(16)                                  │ │
│    │ // Used to separate data blocks and recover from corruption     │ │
│    └─────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
Step 2: Schema Evolution处理
┌─────────────────────────────────────────────────────────────────────────┐
│ ResolvingDecoder decoder = DecoderFactory.get()                         │
│   .resolvingDecoder(writerSchema, readerSchema, binaryDecoder)          │
│                                                                         │
│ Schema Evolution Rules:                                                 │
│ ┌─────────────────────────────────────────────────────────────────────┐ │
│ │ Writer Schema: {"name", "age", "city"}                              │ │
│ │ Reader Schema: {"name", "salary", "department"}                     │ │
│ │                                                                     │ │
│ │ Resolution:                                                         │ │
│ │ - name: present in both → direct mapping                           │ │
│ │ - age: only in writer → skip during reading                        │ │
│ │ - city: only in writer → skip during reading                       │ │
│ │ - salary: only in reader → use default value or null               │ │
│ │ - department: only in reader → use default value or null           │ │
│ └─────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
Step 3: Block级别读取
┌─────────────────────────────────────────────────────────────────────────┐
│ while (hasNextBlock()) {                                                │
│                                                                         │
│   1. Read Block Header                                                  │
│      ┌─────────────────────────────────────────────────────────────────┐ │
│      │ long objectCount = decoder.readVarLong()                        │ │
│      │ long blockSize = decoder.readVarLong()                          │ │
│      │                                                                 │ │
│      │ // Prepare for reading 'objectCount' objects                    │ │
│      │ // of total size 'blockSize' bytes                              │ │
│      └─────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│   2. Handle Compression                                                 │
│      ┌─────────────────────────────────────────────────────────────────┐ │
│      │ if (codec != null) {                                            │ │
│      │   byte[] compressedData = read((int)blockSize)                  │ │
│      │   byte[] decompressedData = codec.decompress(compressedData)    │ │
│      │   decoder = BinaryDecoder.fromBytes(decompressedData)           │ │
│      │ }                                                               │ │
│      └─────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│   3. Read Objects from Block                                            │
│      ┌─────────────────────────────────────────────────────────────────┐ │
│      │ for (int i = 0; i < objectCount; i++) {                        │ │
│      │   GenericRecord record = datumReader.read(null, decoder)        │ │
│      │   processRecord(record)                                         │ │
│      │ }                                                               │ │
│      └─────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│   4. Verify Sync Marker                                                 │
│      ┌─────────────────────────────────────────────────────────────────┐ │
│      │ byte[] blockSyncMarker = read(16)                               │ │
│      │ if (!Arrays.equals(blockSyncMarker, fileSyncMarker)) {          │ │
│      │   throw new IOException("Invalid sync marker")                  │ │
│      │ }                                                               │ │
│      └─────────────────────────────────────────────────────────────────┘ │
│ }                                                                       │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
Step 4: 对象反序列化 (详细编码解析)
┌─────────────────────────────────────────────────────────────────────────┐
│ Object Deserialization Process:                                        │
│                                                                         │
│ For each field in record schema:                                        │
│ ┌─────────────────────────────────────────────────────────────────────┐ │
│ │ Field Type: LONG (varint encoding)                                  │ │
│ │ ┌─────────────────────────────────────────────────────────────────┐ │ │
│ │ │ long value = decoder.readVarLong()                              │ │ │
│ │ │ // Varint decoding: variable-length integer                     │ │ │
│ │ │ // 0x08 → 4, 0x96 0x01 → 75, etc.                              │ │ │
│ │ └─────────────────────────────────────────────────────────────────┘ │ │
│ │                                                                     │ │
│ │ Field Type: STRING (length + UTF-8 bytes)                           │ │
│ │ ┌─────────────────────────────────────────────────────────────────┐ │ │
│ │ │ long length = decoder.readVarLong()                             │ │ │
│ │ │ byte[] bytes = new byte[(int)length]                            │ │ │
│ │ │ decoder.readBytes(bytes)                                        │ │ │
│ │ │ String value = new String(bytes, StandardCharsets.UTF_8)        │ │ │
│ │ └─────────────────────────────────────────────────────────────────┘ │ │
│ │                                                                     │ │
│ │ Field Type: UNION [null, string]                                    │ │
│ │ ┌─────────────────────────────────────────────────────────────────┐ │ │
│ │ │ int unionIndex = decoder.readIndex()                            │ │ │
│ │ │ if (unionIndex == 0) {                                          │ │ │
│ │ │   // null value                                                 │ │ │
│ │ │   value = null                                                  │ │ │
│ │ │ } else if (unionIndex == 1) {                                   │ │ │
│ │ │   // string value                                               │ │ │
│ │ │   value = readString(decoder)                                   │ │ │
│ │ │ }                                                               │ │ │
│ │ └─────────────────────────────────────────────────────────────────┘ │ │
│ │                                                                     │ │
│ │ Field Type: ARRAY                                                   │ │
│ │ ┌─────────────────────────────────────────────────────────────────┐ │ │
│ │ │ List<T> array = new ArrayList<>()                               │ │ │
│ │ │ long count = decoder.readVarLong()                              │ │ │
│ │ │ while (count != 0) {                                            │ │ │
│ │ │   if (count < 0) {                                              │ │ │
│ │ │     count = -count                                              │ │ │
│ │ │     decoder.readVarLong() // skip size                          │ │ │
│ │ │   }                                                             │ │ │
│ │ │   for (int i = 0; i < count; i++) {                            │ │ │
│ │ │     array.add(readElement(decoder))                             │ │ │
│ │ │   }                                                             │ │ │
│ │ │   count = decoder.readVarLong()                                 │ │ │
│ │ │ }                                                               │ │ │
│ │ └─────────────────────────────────────────────────────────────────┘ │ │
│ └─────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

## 5. MOR表合并读取数据流程

### 5.1 MOR表读取架构图

```java
MOR (Merge-on-Read) Table Reading Architecture:

┌─────────────────────────────────────────────────────────────────────────┐
│                        MOR Table Reading Flow                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Query Request                                                          │
│       │                                                                 │
│       ▼                                                                 │
│  ┌─────────────────┐                                                   │
│  │ DataTableScan   │                                                   │
│  │ (MOR Mode)      │                                                   │
│  └─────────────────┘                                                   │
│           │                                                             │
│           ▼                                                             │
│  ┌─────────────────┐       ┌─────────────────┐                        │
│  │   Snapshot      │────── │  ManifestList   │                        │
│  │                 │       │                 │                        │
│  │ - Data Manifests│       │ ┌─────────────┐ │                        │
│  │ - Delete        │       │ │Data Manifest│ │                        │
│  │   Manifests     │       │ │   File 1    │ │                        │
│  │                 │       │ └─────────────┘ │                        │
│  └─────────────────┘       │ ┌─────────────┐ │                        │
│                             │ │Data Manifest│ │                        │
│                             │ │   File 2    │ │                        │
│                             │ └─────────────┘ │                        │
│                             │ ┌─────────────┐ │                        │
│                             │ │Delete       │ │                        │
│                             │ │Manifest 1   │ │                        │
│                             │ └─────────────┘ │                        │
│                             │ ┌─────────────┐ │                        │
│                             │ │Delete       │ │                        │
│                             │ │Manifest 2   │ │                        │
│                             │ └─────────────┘ │                        │
│                             └─────────────────┘                        │
│                                     │                                   │
│                                     ▼                                   │
│                           ┌─────────────────┐                          │
│                           │ ManifestGroup   │                          │
│                           │                 │                          │
│                           │ Processing:     │                          │
│                           │ 1. Data Files   │                          │
│                           │ 2. Delete Files │                          │
│                           │ 3. Build Index  │                          │
│                           └─────────────────┘                          │
│                                     │                                   │
│                                     ▼                                   │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                DeleteFileIndex Building                         │   │
│  │                                                                 │   │
│  │  Delete Manifest 1        Delete Manifest 2                    │   │
│  │  ┌─────────────────┐      ┌─────────────────┐                  │   │
│  │  │Equality Deletes │      │Position Deletes │                  │   │
│  │  │                 │      │                 │                  │   │
│  │  │File: data1.parq │      │File: data2.parq │                  │   │
│  │  │Cols: [id, name] │      │Positions:[1,5,9]│                  │   │
│  │  │Values:          │      │                 │                  │   │
│  │  │(123, "Alice")   │      │File: data3.parq │                  │   │
│  │  │(456, "Bob")     │      │Positions:[2,7]  │                  │   │
│  │  └─────────────────┘      └─────────────────┘                  │   │
│  │           │                         │                           │   │
│  │           ▼                         ▼                           │   │
│  │  ┌─────────────────┐      ┌─────────────────┐                  │   │
│  │  │EqualityDeletes  │      │PositionDeletes  │                  │   │
│  │  │Index            │      │Index            │                  │   │
│  │  │                 │      │                 │                  │   │
│  │  │ByPartition:     │      │ByDataFile:      │                  │   │
│  │  │ p1 → {deletes1} │      │ data2 → {1,5,9} │                  │   │
│  │  │ p2 → {deletes2} │      │ data3 → {2,7}   │                  │   │
│  │  │Global: {global} │      │                 │                  │   │
│  │  └─────────────────┘      └─────────────────┘                  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                     │                                   │
│                                     ▼                                   │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                  FileScanTask Creation                          │   │
│  │                                                                 │   │
│  │  For each Data File:                                            │   │
│  │  ┌─────────────────────────────────────────────────────────────┐ │   │
│  │  │ DataFile: data1.parquet                                     │ │   │
│  │  │ ┌─────────────────────────────────────────────────────────┐ │ │   │
│  │  │ │ 1. Find applicable delete files:                       │ │ │   │
│  │  │ │    - Global equality deletes                            │ │ │   │
│  │  │ │    - Partition-scoped equality deletes                  │ │ │   │
│  │  │ │    - Position deletes for this file                    │ │ │   │
│  │  │ │    - Deletion vectors for this file                    │ │ │   │
│  │  │ │                                                         │ │ │   │
│  │  │ │ 2. Check sequence numbers:                              │ │ │   │
│  │  │ │    DataFile.sequenceNumber = 100                       │ │ │   │
│  │  │ │    DeleteFile.sequenceNumber = 105                     │ │ │   │
│  │  │ │    → Apply delete (105 > 100)                          │ │ │   │
│  │  │ │                                                         │ │ │   │
│  │  │ │ 3. Create FileScanTask:                                 │ │ │   │
│  │  │ │    - DataFile: data1.parquet                           │ │ │   │
│  │  │ │    - DeleteFiles: [eq_del1, pos_del1]                  │ │ │   │
│  │  │ │    - Schema: projected schema                           │ │ │   │
│  │  │ │    - Residual: remaining predicates                    │ │ │   │
│  │  │ └─────────────────────────────────────────────────────────┘ │ │   │
│  │  └─────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 5.2 MOR合并读取详细流程

```java
MOR Merge-on-Read Detailed Process:

Step 1: Delete文件索引构建
┌─────────────────────────────────────────────────────────────────────────┐
│ DeleteFileIndex.Builder.build()                                         │
│                                                                         │
│ 1. 按类型分组Delete Manifests                                           │
│    ┌─────────────────────────────────────────────────────────────────┐ │
│    │ for (ManifestFile manifest : deleteManifests) {                 │ │
│    │   switch (manifest.content()) {                                 │ │
│    │     case EQUALITY_DELETES:                                      │ │
│    │       equalityDeleteManifests.add(manifest)                     │ │
│    │     case POSITION_DELETES:                                      │ │
│    │       positionDeleteManifests.add(manifest)                     │ │
│    │     case DELETION_VECTORS:                                      │ │
│    │       dvManifests.add(manifest)                                 │ │
│    │   }                                                             │ │
│    │ }                                                               │ │
│    └─────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│ 2. 构建等值删除索引                                                     │
│    ┌─────────────────────────────────────────────────────────────────┐ │
│    │ Map<PartitionKey, EqualityDeletes> eqDeletesByPartition          │ │
│    │ EqualityDeletes globalDeletes                                    │ │
│    │                                                                 │ │
│    │ for (ManifestFile manifest : equalityDeleteManifests) {         │ │
│    │   try (ManifestReader<DeleteFile> reader =                      │ │
│    │        ManifestFiles.read(manifest, io, specs)) {               │ │
│    │     for (ManifestEntry<DeleteFile> entry : reader.entries()) {  │ │
│    │       DeleteFile deleteFile = entry.file()                      │ │
│    │       if (isGlobalDelete(deleteFile)) {                         │ │
│    │         globalDeletes.add(deleteFile)                           │ │
│    │       } else {                                                  │ │
│    │         PartitionKey key = deleteFile.partition()               │ │
│    │         eqDeletesByPartition.get(key).add(deleteFile)           │ │
│    │       }                                                         │ │
│    │     }                                                           │ │
│    │   }                                                             │ │
│    │ }                                                               │ │
│    └─────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│ 3. 构建位置删除索引                                                     │
│    ┌─────────────────────────────────────────────────────────────────┐ │
│    │ Map<String, PositionDeletes> posDeletesByPath                   │ │
│    │ Map<PartitionKey, PositionDeletes> posDeletesByPartition        │ │
│    │                                                                 │ │
│    │ for (ManifestFile manifest : positionDeleteManifests) {         │ │
│    │   // 读取位置删除文件内容                                       │ │
│    │   try (CloseableIterable<Record> deletes =                      │ │
│    │        readPositionDeletes(manifest)) {                         │ │
│    │     for (Record delete : deletes) {                            │ │
│    │       String dataFilePath = delete.getField("file_path")        │ │
│    │       long position = delete.getField("pos")                    │ │
│    │       posDeletesByPath.get(dataFilePath).add(position)          │ │
│    │     }                                                           │ │
│    │   }                                                             │ │
│    │ }                                                               │ │
│    └─────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
Step 2: FileScanTask创建 - 删除文件关联
┌─────────────────────────────────────────────────────────────────────────┐
│ For each DataFile in manifests:                                         │
│                                                                         │
│ createFileScanTasks(manifestEntries, taskContext)                       │
│                                                                         │
│ ┌─────────────────────────────────────────────────────────────────────┐ │
│ │ for (ManifestEntry<DataFile> entry : manifestEntries) {             │ │
│ │                                                                     │ │
│ │   DataFile dataFile = entry.file()                                  │ │
│ │   long dataSequenceNumber = entry.dataSequenceNumber()              │ │
│ │                                                                     │ │
│ │   // 查找适用的删除文件                                             │ │
│ │   DeleteFile[] deleteFiles = deleteIndex.forEntry(entry)            │ │
│ │                                                                     │ │
│ │   ┌─────────────────────────────────────────────────────────────┐   │ │
│ │   │ deleteIndex.forDataFile(dataSequenceNumber, dataFile):      │   │ │
│ │   │                                                             │   │ │
│ │   │ 1. Global equality deletes                                  │   │ │
│ │   │    if (globalDeletes != null) {                             │   │ │
│ │   │      globalDeleteFiles = globalDeletes.filter(             │   │ │
│ │   │        dataSequenceNumber, dataFile)                       │   │ │
│ │   │    }                                                        │   │ │
│ │   │                                                             │   │ │
│ │   │ 2. Partition-scoped equality deletes                        │   │ │
│ │   │    PartitionKey partitionKey = dataFile.partition()         │   │ │
│ │   │    EqualityDeletes eqDeletes =                              │   │ │
│ │   │      eqDeletesByPartition.get(partitionKey)                 │   │ │
│ │   │    if (eqDeletes != null) {                                 │   │ │
│ │   │      partitionDeleteFiles = eqDeletes.filter(              │   │ │
│ │   │        dataSequenceNumber, dataFile)                       │   │ │
│ │   │    }                                                        │   │ │
│ │   │                                                             │   │ │
│ │   │ 3. Position deletes by file path                           │   │ │
│ │   │    String filePath = dataFile.path()                       │   │ │
│ │   │    PositionDeletes posDeletes =                            │   │ │
│ │   │      posDeletesByPath.get(filePath)                        │   │ │
│ │   │    if (posDeletes != null) {                               │   │ │
│ │   │      pathDeleteFiles = posDeletes.filter(                  │   │ │
│ │   │        dataSequenceNumber)                                 │   │ │
│ │   │    }                                                        │   │ │
│ │   │                                                             │   │ │
│ │   │ 4. Deletion vectors                                         │   │ │
│ │   │    DeleteFile dv = dvByPath.get(filePath)                  │   │ │
│ │   │    if (dv != null &&                                       │   │ │
│ │   │        dv.dataSequenceNumber() >= dataSequenceNumber) {    │   │ │
│ │   │      deletionVectorFile = dv                               │   │ │
│ │   │    }                                                        │   │ │
│ │   └─────────────────────────────────────────────────────────────┘   │ │
│ │                                                                     │ │
│ │   // 合并所有适用的删除文件                                         │ │
│ │   DeleteFile[] allDeleteFiles = concat(                            │ │
│ │     globalDeleteFiles, partitionDeleteFiles,                       │ │
│ │     pathDeleteFiles, deletionVectorFile)                           │ │
│ │                                                                     │ │
│ │   // 创建FileScanTask                                              │ │
│ │   FileScanTask task = new BaseFileScanTask(                        │ │
│ │     dataFile, allDeleteFiles, schemaAsString,                      │ │
│ │     specAsString, residualFilter)                                   │ │
│ │                                                                     │ │
│ │   yield task                                                        │ │
│ │ }                                                                   │ │
│ └─────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
Step 3: 运行时数据合并读取
┌─────────────────────────────────────────────────────────────────────────┐
│ For each FileScanTask:                                                  │
│                                                                         │
│ processFileScanTask(task)                                               │
│                                                                         │
│ ┌─────────────────────────────────────────────────────────────────────┐ │
│ │ DataFile dataFile = task.file()                                     │ │
│ │ DeleteFile[] deleteFiles = task.deletes()                           │ │
│ │                                                                     │ │
│ │ // 1. 读取数据文件                                                   │ │
│ │ try (CloseableIterable<Record> dataRecords =                        │ │
│ │      readDataFile(dataFile, projection)) {                          │ │
│ │                                                                     │ │
│ │   // 2. 构建删除过滤器                                               │ │
│ │   DeleteFilter deleteFilter = buildDeleteFilter(deleteFiles)        │ │
│ │                                                                     │ │
│ │   ┌─────────────────────────────────────────────────────────────┐   │ │
│ │   │ buildDeleteFilter(deleteFiles):                             │   │ │
│ │   │                                                             │   │ │
│ │   │ EqualityDeleteFilter eqFilter = null                        │   │ │
│ │   │ PositionDeleteFilter posFilter = null                       │   │ │
│ │   │                                                             │   │ │
│ │   │ for (DeleteFile deleteFile : deleteFiles) {                 │   │ │
│ │   │   switch (deleteFile.content()) {                           │   │ │
│ │   │     case EQUALITY_DELETES:                                  │   │ │
│ │   │       if (eqFilter == null) {                               │   │ │
│ │   │         eqFilter = new EqualityDeleteFilter(               │   │ │
│ │   │           schema, deleteFile.equalityFieldIds())            │   │ │
│ │   │       }                                                     │   │ │
│ │   │       eqFilter.addDeleteFile(deleteFile)                   │   │ │
│ │   │       break                                                 │   │ │
│ │   │                                                             │   │ │
│ │   │     case POSITION_DELETES:                                  │   │ │
│ │   │       if (posFilter == null) {                              │   │ │
│ │   │         posFilter = new PositionDeleteFilter()              │   │ │
│ │   │       }                                                     │   │ │
│ │   │       posFilter.addDeleteFile(deleteFile)                  │   │ │
│ │   │       break                                                 │   │ │
│ │   │   }                                                         │   │ │
│ │   │ }                                                           │   │ │
│ │   │                                                             │   │ │
│ │   │ return new CombinedDeleteFilter(eqFilter, posFilter)        │   │ │
│ │   └─────────────────────────────────────────────────────────────┘   │ │
│ │                                                                     │ │
│ │   // 3. 应用删除过滤                                                 │ │
│ │   try (CloseableIterable<Record> filteredRecords =                  │ │
│ │        deleteFilter.filter(dataRecords)) {                          │ │
│ │     for (Record record : filteredRecords) {                        │ │
│ │       yield record  // 输出未被删除的记录                           │ │
│ │     }                                                               │ │
│ │   }                                                                 │ │
│ │ }                                                                   │ │
│ └─────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

## 6. 过滤数据读取详细流程

### 6.1 多层过滤架构

```java
Multi-Level Filtering Architecture:

┌─────────────────────────────────────────────────────────────────────────┐
│                      Multi-Level Data Filtering                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Level 1: Query Planning (表级别)                                       │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Input: SELECT * FROM table                                      │   │
│  │        WHERE id > 1000 AND status = 'ACTIVE'                    │   │
│  │        AND created_at > '2024-01-01'                             │   │
│  │                                                                 │   │
│  │ Parse to Iceberg Expression:                                    │   │
│  │ Expressions.and(                                                │   │
│  │   Expressions.greaterThan("id", 1000),                          │   │
│  │   Expressions.equal("status", "ACTIVE"),                        │   │
│  │   Expressions.greaterThan("created_at", timestamp)              │   │
│  │ )                                                               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                      │                                  │
│                                      ▼                                  │
│  Level 2: Snapshot Filtering                                            │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Time Travel / Branch Selection:                                 │   │
│  │                                                                 │   │
│  │ if (timeTravel) {                                               │   │
│  │   snapshot = SnapshotUtil.snapshotIdAsOfTime(table, timestamp) │   │
│  │ } else {                                                        │   │
│  │   snapshot = table.currentSnapshot()                           │   │
│  │ }                                                               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                      │                                  │
│                                      ▼                                  │
│  Level 3: Manifest Filtering (文件组级别)                               │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ ManifestEvaluator.eval(manifestFile):                           │   │
│  │                                                                 │   │
│  │ For each Manifest File:                                         │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ Partition Bounds Check:                                     │ │   │
│  │ │                                                             │ │   │
│  │ │ Manifest covers partitions:                                 │ │   │
│  │ │ - date_partition IN ['2024-01-01', '2024-01-31']           │ │   │
│  │ │ - region_partition IN ['US', 'EU']                         │ │   │
│  │ │                                                             │ │   │
│  │ │ Filter: created_at > '2024-01-01'                           │ │   │
│  │ │ Result: Manifest covers needed partitions ✓                │ │   │
│  │ │                                                             │ │   │
│  │ │ Added/Existing/Deleted Files Count:                         │ │   │
│  │ │ - hasAddedFiles: true                                       │ │   │
│  │ │ - hasExistingFiles: true                                    │ │   │
│  │ │ - ignoreDeleted: true → skip deleted-only manifests        │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ Result: Include/Skip entire manifest                            │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                      │                                  │
│                                      ▼                                  │
│  Level 4: Data File Filtering (文件级别)                                │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ InclusiveMetricsEvaluator.eval(dataFile):                       │   │
│  │                                                                 │   │
│  │ For each Data File in included manifests:                       │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ File Statistics Check:                                      │ │   │
│  │ │                                                             │ │   │
│  │ │ DataFile metrics:                                           │ │   │
│  │ │ - recordCount: 50000                                        │ │   │
│  │ │ - nullValueCounts: {id: 0, status: 1000, created_at: 0}    │ │   │
│  │ │ - lowerBounds: {id: 500, status: "ACTIVE", ...}           │ │   │
│  │ │ - upperBounds: {id: 2000, status: "INACTIVE", ...}        │ │   │
│  │ │                                                             │ │   │
│  │ │ Filter Evaluation:                                          │ │   │
│  │ │ 1. id > 1000:                                              │ │   │
│  │ │    upperBound(2000) > 1000 ✓ (cannot skip)                │ │   │
│  │ │                                                             │ │   │
│  │ │ 2. status = 'ACTIVE':                                      │ │   │
│  │ │    'ACTIVE' in [lowerBound, upperBound] ✓                 │ │   │
│  │ │    nullCount(1000) < recordCount(50000) ✓ (has values)    │ │   │
│  │ │                                                             │ │   │
│  │ │ 3. created_at > '2024-01-01':                              │ │   │
│  │ │    upperBound > '2024-01-01' ✓                             │ │   │
│  │ │                                                             │ │   │
│  │ │ Result: Include file (cannot be skipped)                   │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                      │                                  │
│                                      ▼                                  │
│  Level 5: Row Group/Stripe Filtering (格式特定)                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Format-Specific Statistics Filtering:                           │   │
│  │                                                                 │   │
│  │ Parquet (Row Group Level):                                      │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ For each Row Group:                                         │ │   │
│  │ │   RowGroupMetadata metadata = readRowGroupMetadata()        │ │   │
│  │ │   if (metricsEvaluator.eval(metadata.getColumns())) {      │ │   │
│  │ │     includeRowGroup(rowGroup)                               │ │   │
│  │ │   }                                                         │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ ORC (Stripe Level):                                             │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ For each Stripe:                                            │ │   │
│  │ │   SearchArgument searchArg = convertFilter(filter)          │ │   │
│  │ │   if (searchArg.evaluate(stripeStatistics)) {               │ │   │
│  │ │     includeStripe(stripe)                                   │ │   │
│  │ │   }                                                         │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                      │                                  │
│                                      ▼                                  │
│  Level 6: Page/Row Group Detail Filtering                               │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Fine-Grained Filtering:                                         │   │
│  │                                                                 │   │
│  │ Parquet Page Index (1.12+):                                     │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ For each Data Page:                                         │ │   │
│  │ │   if (pageStatistics.canSkip(filter)) {                    │ │   │
│  │ │     skipPage(page)                                          │ │   │
│  │ │   }                                                         │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ Dictionary Filtering:                                           │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ if (dictionaryFilter.canSkip(dictionary, filter)) {         │ │   │
│  │ │   skipColumnChunk(columnChunk)                              │ │   │
│  │ │ }                                                           │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  │                                                                 │   │
│  │ Bloom Filter:                                                   │   │
│  │ ┌─────────────────────────────────────────────────────────────┐ │   │
│  │ │ if (!bloomFilter.mightContain(filterValue)) {               │ │   │
│  │ │   skipRowGroup(rowGroup)  // Definitely not present         │ │   │
│  │ │ }                                                           │ │   │
│  │ └─────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                      │                                  │
│                                      ▼                                  │
│  Level 7: Row-Level Filtering                                           │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Final Row Processing:                                           │   │
│  │                                                                 │   │
│  │ for (Record record : readRecords()) {                           │   │
│  │   // Apply remaining predicates that couldn't be pushed down    │   │
│  │   if (residualFilter.eval(record)) {                           │   │
│  │     if (!isDeleted(record, deleteFiles)) {                     │   │
│  │       yield record                                              │   │
│  │     }                                                           │   │
│  │   }                                                             │   │
│  │ }                                                               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 6.2 过滤性能优化流程

```java
Filter Performance Optimization Flow:

Step 1: 表达式优化和谓词下推
┌─────────────────────────────────────────────────────────────────────────┐
│ Expression Optimization and Predicate Pushdown                          │
│                                                                         │
│ Original Query:                                                         │
│ WHERE (id > 1000 AND status = 'ACTIVE') OR (id < 100 AND type = 'VIP') │
│                                                                         │
│ 1. 表达式标准化                                                         │
│    ┌─────────────────────────────────────────────────────────────────┐ │
│    │ ExpressionUtil.sanitize(schema, expression, caseSensitive)      │ │
│    │ - Convert column names to field IDs                            │ │
│    │ - Validate types and handle case sensitivity                   │ │
│    │ - Simplify constant expressions                                │ │
│    └─────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│ 2. 谓词分离                                                             │
│    ┌─────────────────────────────────────────────────────────────────┐ │
│    │ Projections.inclusive(partitionSpec, caseSensitive)             │ │
│    │   .project(dataFilter)                                         │ │
│    │                                                                 │ │
│    │ Result:                                                         │ │
│    │ - partitionFilter: expressions that can be evaluated           │ │
│    │                   using partition data only                    │ │
│    │ - dataFilter: expressions that require data file access        │ │
│    │ - residualFilter: expressions that must be evaluated           │ │
│    │                  at record level                               │ │
│    └─────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│ 3. 统计信息兼容性检查                                                   │
│    ┌─────────────────────────────────────────────────────────────────┐ │
│    │ requireStatsProjection(rowFilter, columns):                     │ │
│    │                                                                 │ │
│    │ if (filter references columns not in projection) {              │ │
│    │   columns = withStatsColumns(columns)  // Add min/max cols     │ │
│    │ }                                                               │ │
│    │                                                                 │ │
│    │ Ensure statistics columns are available:                        │ │
│    │ - value_counts, null_value_counts, lower_bounds, upper_bounds   │ │
│    └─────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
Step 2: 统计信息评估器创建
┌─────────────────────────────────────────────────────────────────────────┐
│ Statistics Evaluator Creation                                           │
│                                                                         │
│ InclusiveMetricsEvaluator metricsEvaluator =                            │
│   new InclusiveMetricsEvaluator(fileSchema, rowFilter, caseSensitive)   │
│                                                                         │
│ ┌─────────────────────────────────────────────────────────────────────┐ │
│ │ Evaluator Initialization:                                           │ │
│ │                                                                     │ │
│ │ For each predicate in filter:                                       │ │
│ │ ┌─────────────────────────────────────────────────────────────────┐ │ │
│ │ │ Example: id > 1000                                              │ │ │
│ │ │                                                                 │ │ │
│ │ │ Create BoundPredicate:                                          │ │ │
│ │ │ - operation: GREATER_THAN                                       │ │ │
│ │ │ - term: BoundTerm(fieldId=1, type=LONG)                        │ │ │
│ │ │ - literal: Literal.of(1000)                                    │ │ │
│ │ │                                                                 │ │ │
│ │ │ Evaluation strategy:                                            │ │ │
│ │ │ - Use upper_bounds for GREATER_THAN checks                     │ │ │
│ │ │ - Use lower_bounds for LESS_THAN checks                        │ │ │
│ │ │ - Use both bounds for EQUALS checks                            │ │ │
│ │ │ - Use null_counts for NOT_NULL/IS_NULL checks                  │ │ │
│ │ └─────────────────────────────────────────────────────────────────┘ │ │
│ └─────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
Step 3: 运行时过滤执行
┌─────────────────────────────────────────────────────────────────────────┐
│ Runtime Filter Execution                                                │
│                                                                         │
│ For each DataFile:                                                      │
│                                                                         │
│ ┌─────────────────────────────────────────────────────────────────────┐ │
│ │ boolean includeFile = metricsEvaluator.eval(dataFile)               │ │
│ │                                                                     │ │
│ │ Detailed evaluation process:                                        │ │
│ │ ┌─────────────────────────────────────────────────────────────────┐ │ │
│ │ │ DataFile metrics extraction:                                    │ │ │
│ │ │                                                                 │ │ │
│ │ │ Map<Integer, Long> valueCounts = dataFile.valueCounts()         │ │ │
│ │ │ Map<Integer, Long> nullCounts = dataFile.nullValueCounts()      │ │ │
│ │ │ Map<Integer, ByteBuffer> lowerBounds = dataFile.lowerBounds()   │ │ │
│ │ │ Map<Integer, ByteBuffer> upperBounds = dataFile.upperBounds()   │ │ │
│ │ │                                                                 │ │ │
│ │ │ For predicate "id > 1000":                                      │ │ │
│ │ │ ┌─────────────────────────────────────────────────────────────┐ │ │ │
│ │ │ │ ByteBuffer upperBoundBytes = upperBounds.get(fieldId)       │ │ │ │
│ │ │ │ if (upperBoundBytes == null) return true; // Unknown        │ │ │ │
│ │ │ │                                                             │ │ │ │
│ │ │ │ Long upperBound = Conversions.fromByteBuffer(               │ │ │ │
│ │ │ │   Types.LongType.get(), upperBoundBytes)                    │ │ │ │
│ │ │ │                                                             │ │ │ │
│ │ │ │ if (upperBound <= 1000) {                                   │ │ │ │
│ │ │ │   return false; // Can skip - all values <= 1000           │ │ │ │
│ │ │ │ } else {                                                    │ │ │ │
│ │ │ │   return true;  // Might contain matches                   │ │ │ │
│ │ │ │ }                                                           │ │ │ │
│ │ │ └─────────────────────────────────────────────────────────────┘ │ │ │
│ │ │                                                                 │ │ │
│ │ │ For predicate "status = 'ACTIVE'":                              │ │ │
│ │ │ ┌─────────────────────────────────────────────────────────────┐ │ │ │
│ │ │ │ Check if value is within bounds:                            │ │ │ │
│ │ │ │ lowerBound = "ACTIVE", upperBound = "INACTIVE"             │ │ │ │
│ │ │ │ "ACTIVE" >= "ACTIVE" AND "ACTIVE" <= "INACTIVE" ✓          │ │ │ │
│ │ │ │                                                             │ │ │ │
│ │ │ │ Check null count:                                           │ │ │ │
│ │ │ │ if (nullCount == valueCount) return false; // All nulls    │ │ │ │
│ │ │ │                                                             │ │ │ │
│ │ │ │ return true; // Might contain 'ACTIVE'                     │ │ │ │
│ │ │ └─────────────────────────────────────────────────────────────┘ │ │ │
│ │ └─────────────────────────────────────────────────────────────────┘ │ │
│ └─────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│ if (includeFile) {                                                      │
│   // Continue processing this file                                      │
│   filesToProcess.add(dataFile)                                          │
│ } else {                                                                │
│   // Skip this file entirely                                            │
│   skippedFiles.increment()                                              │
│ }                                                                       │
└─────────────────────────────────────────────────────────────────────────┘
```

现在更新完整的技术文档：

<function_calls>
<invoke name="TodoWrite">
<parameter name="todos">[{"content": "\u521b\u5efaTime Travel\u6570\u636e\u8bfb\u53d6\u8be6\u7ec6\u6d41\u7a0b\u56fe", "status": "completed", "activeForm": "\u6b63\u5728\u521b\u5efaTime Travel\u6570\u636e\u8bfb\u53d6\u8be6\u7ec6\u6d41\u7a0b\u56fe"}, {"content": "\u7ed8\u5236Parquet\u6587\u4ef6\u5b58\u50a8\u7ed3\u6784\u548c\u8bfb\u53d6\u6d41\u7a0b\u56fe", "status": "completed", "activeForm": "\u6b63\u5728\u7ed8\u5236Parquet\u6587\u4ef6\u5b58\u50a8\u7ed3\u6784\u548c\u8bfb\u53d6\u6d41\u7a0b\u56fe"}, {"content": "\u7ed8\u5236ORC\u6587\u4ef6\u5b58\u50a8\u7ed3\u6784\u548c\u8bfb\u53d6\u6d41\u7a0b\u56fe", "status": "completed", "activeForm": "\u6b63\u5728\u7ed8\u5236ORC\u6587\u4ef6\u5b58\u50a8\u7ed3\u6784\u548c\u8bfb\u53d6\u6d41\u7a0b\u56fe"}, {"content": "\u7ed8\u5236Avro\u6587\u4ef6\u5b58\u50a8\u7ed3\u6784\u548c\u8bfb\u53d6\u6d41\u7a0b\u56fe", "status": "completed", "activeForm": "\u6b63\u5728\u7ed8\u5236Avro\u6587\u4ef6\u5b58\u50a8\u7ed3\u6784\u548c\u8bfb\u53d6\u6d41\u7a0b\u56fe"}, {"content": "\u8be6\u7ec6\u5206\u6790MOR\u8868\u5408\u5e76\u8bfb\u53d6\u6570\u636e\u6d41\u7a0b", "status": "completed", "activeForm": "\u6b63\u5728\u5206\u6790MOR\u8868\u5408\u5e76\u8bfb\u53d6\u6570\u636e\u6d41\u7a0b"}, {"content": "\u5206\u6790\u8fc7\u6ee4\u6570\u636e\u8bfb\u53d6\u7684\u8be6\u7ec6\u8fc7\u7a0b", "status": "completed", "activeForm": "\u6b63\u5728\u5206\u6790\u8fc7\u6ee4\u6570\u636e\u8bfb\u53d6\u7684\u8be6\u7ec6\u8fc7\u7a0b"}, {"content": "\u66f4\u65b0\u6280\u672f\u6587\u6863\u5e76\u6574\u5408\u6240\u6709\u6d41\u7a0b\u56fe", "status": "in_progress", "activeForm": "\u6b63\u5728\u66f4\u65b0\u6280\u672f\u6587\u6863\u5e76\u6574\u5408\u6240\u6709\u6d41\u7a0b\u56fe"}]