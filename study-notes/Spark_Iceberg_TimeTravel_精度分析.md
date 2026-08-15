# Spark Iceberg Time Travel 日期格式精度分析

## 问题回答：Spark Iceberg是否接受`2022-06-29 18:40:37.11111`格式？

### 简答：**不完全支持高精度时间戳字符串**

根据源码分析，Spark Iceberg的Time Travel功能对时间戳格式有以下限制：

## 1. 当前支持的格式

### 1.1 标准支持格式
- **Unix时间戳（秒精度）**：`1656507980`
- **标准日期时间**：`'2022-06-29 18:40:37'`（秒精度，不含微秒）
- **快照ID**：`12345678901234567890`

### 1.2 具体实现限制

**在TestSelect.java中的证据** (`/spark/v3.5/spark/src/test/java/.../TestSelect.java:437`):
```java
// 明确注释：AS OF期望长整型格式的时间戳为秒精度
// AS OF expects the timestamp if given in long format will be of seconds precision
SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
String formattedDate = sdf.format(new Date(timestamp));
```

## 2. 为什么不支持`2022-06-29 18:40:37.11111`？

### 2.1 解析器限制
Spark使用`SimpleDateFormat`且格式固定为`"yyyy-MM-dd HH:mm:ss"`，不包含毫秒或微秒部分。

### 2.2 内部处理机制
1. **SQL语法解析**：Spark SQL解析器将时间戳字符串按固定格式解析
2. **长整型转换**：最终转换为毫秒级的长整型时间戳
3. **精度丢失**：微秒部分在解析过程中被忽略

### 2.3 时间精度转换
```java
// SparkCachedTableCatalog.java:84
public SparkTable loadTable(Identifier ident, long timestampMicros) {
    // Spark传递微秒，但Iceberg使用毫秒表示快照
    long timestampMillis = TimeUnit.MICROSECONDS.toMillis(timestampMicros);
    long snapshotId = SnapshotUtil.snapshotIdAsOfTime(table.first(), timestampMillis);
}
```

## 3. 解决方案

### 3.1 使用毫秒级Unix时间戳
```sql
-- 推荐：直接使用毫秒时间戳
SELECT * FROM table_name TIMESTAMP AS OF 1656507980463
```

### 3.2 DataFrame API选项
```scala
// 使用毫秒时间戳
spark.read
  .format("iceberg")  
  .option("as-of-timestamp", "1656507980463")
  .load("table_name")
```

### 3.3 编程转换
```java
// 如需高精度，先转换为毫秒时间戳
long timestampMs = Instant.parse("2022-06-29T18:40:37.11111Z").toEpochMilli();
```

## 4. 不同引擎对比

| 引擎 | 支持格式 | 精度限制 | 备注 |
|------|----------|----------|------|
| **Spark** | `yyyy-MM-dd HH:mm:ss` | 秒级 | 不支持微秒/毫秒字符串 |
| **Flink** | 仅数字时间戳 | 毫秒级 | 不支持日期字符串 |

## 5. 最佳实践建议

1. **API调用**：使用毫秒级Unix时间戳（性能最佳，精度最高）
2. **交互式查询**：使用`yyyy-MM-dd HH:mm:ss`格式（可读性好，秒级精度足够）
3. **高精度需求**：
   - 预先计算目标时间的毫秒时间戳
   - 使用快照ID进行精确查询
   - 考虑使用分支/标签作为时间标记

## 结论

**Spark Iceberg目前不支持`2022-06-29 18:40:37.11111`这种包含微秒的时间戳格式**。如果需要高精度的时间旅行查询，建议使用毫秒级的Unix时间戳或快照ID。