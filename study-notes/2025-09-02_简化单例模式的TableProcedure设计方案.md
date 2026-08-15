# 简化单例模式的TableProcedure设计方案

## 1. 方案概述

### 1.1 设计目标
采用最简单的静态初始化单例模式，通过`private static final`方式创建Procedure实例，避免复杂的懒加载和同步机制，提供更直观、更高效的实现方案。

### 1.2 简化单例模式优势
1. **实现简单**：无需复杂的同步机制和懒加载逻辑
2. **性能最优**：JVM保证类加载时的线程安全，无运行时同步开销
3. **内存高效**：每种Procedure只有一个实例
4. **代码清晰**：易于理解和维护
5. **启动快速**：避免首次访问时的初始化延迟

### 1.3 适用场景
- Procedure数量相对固定且有限
- 系统启动时就确定要使用的Procedure类型
- 追求最佳性能和最简实现

## 2. 核心架构设计

### 2.1 简化的抽象基类

```java
public abstract class TableProcedure {
    private static final Logger LOG = LogManager.getLogger(TableProcedure.class);
    
    protected final String procedureName;
    protected final List<ProcedureParameter> parameters;
    protected final String description;
    protected final Set<String> supportedTableTypes;
    
    protected TableProcedure(String procedureName, List<ProcedureParameter> parameters,
                           String description, Set<String> supportedTableTypes) {
        this.procedureName = procedureName;
        this.parameters = Collections.unmodifiableList(new ArrayList<>(parameters));
        this.description = description;
        this.supportedTableTypes = Collections.unmodifiableSet(new HashSet<>(supportedTableTypes));
    }
    
    // Template Method - 定义执行流程
    public final void execute(List<Expression> args, TableIf table, ConnectContext ctx) 
            throws UserException {
        
        // 1. 验证表类型支持
        validateTableType(table);
        
        // 2. 验证参数
        validateArguments(args, table);
        
        // 3. 处理参数
        List<Object> processedArgs = processArguments(args, table);
        
        // 4. 构建表上下文
        TableContext tableContext = buildTableContext(table, ctx);
        
        // 5. 执行核心逻辑
        executeInternal(processedArgs, tableContext, ctx);
    }
    
    protected void validateTableType(TableIf table) throws UserException {
        if (!(table instanceof ExternalTable)) {
            throw new UserException("Procedure only supports external tables");
        }
        
        ExternalTable externalTable = (ExternalTable) table;
        String catalogType = externalTable.getCatalog().getType();
        
        if (!supportedTableTypes.contains(catalogType)) {
            throw new UserException(String.format(
                "Procedure %s does not support table type: %s", 
                procedureName, catalogType));
        }
    }
    
    public void validateArguments(List<Expression> args, TableIf table) throws UserException {
        if (args.size() > parameters.size()) {
            throw new UserException(String.format(
                "Too many arguments for procedure %s. Expected: %d, Got: %d",
                procedureName, parameters.size(), args.size()));
        }
        
        for (int i = 0; i < args.size(); i++) {
            ProcedureParameter param = parameters.get(i);
            Expression arg = args.get(i);
            param.validate(arg);
        }
        
        for (int i = args.size(); i < parameters.size(); i++) {
            ProcedureParameter param = parameters.get(i);
            if (param.isRequired() && param.getDefaultValue() == null) {
                throw new UserException(String.format(
                    "Required parameter %s is missing", param.getName()));
            }
        }
    }
    
    protected List<Object> processArguments(List<Expression> args, TableIf table) throws UserException {
        List<Object> result = new ArrayList<>();
        
        for (int i = 0; i < parameters.size(); i++) {
            ProcedureParameter param = parameters.get(i);
            
            if (i < args.size()) {
                Expression arg = args.get(i);
                result.add(param.convertValue(arg));
            } else {
                Object defaultValue = resolveDefaultValue(param, table);
                result.add(defaultValue);
            }
        }
        
        return result;
    }
    
    protected Object resolveDefaultValue(ProcedureParameter param, TableIf table) throws UserException {
        if (param.getDefaultValue() != null) {
            return param.getDefaultValue();
        }
        
        switch (param.getName().toLowerCase()) {
            case "table":
            case "table_name":
                return table.getName();
            case "database":
            case "db_name":
                return ((ExternalTable) table).getDbName();
            case "catalog":
            case "catalog_name":
                return ((ExternalTable) table).getCatalog().getName();
            default:
                if (param.isRequired()) {
                    throw new UserException(String.format(
                        "Required parameter %s has no default value", param.getName()));
                }
                return null;
        }
    }
    
    protected TableContext buildTableContext(TableIf table, ConnectContext ctx) {
        ExternalTable externalTable = (ExternalTable) table;
        return new TableContext(
            externalTable,
            externalTable.getCatalog(),
            ctx.getCurrentUserIdentity(),
            ctx
        );
    }
    
    // 子类实现具体执行逻辑
    protected abstract void executeInternal(List<Object> args, TableContext tableContext, 
                                          ConnectContext ctx) throws UserException;
    
    // Getters
    public String getProcedureName() { return procedureName; }
    public List<ProcedureParameter> getParameters() { return parameters; }
    public String getDescription() { return description; }
    public Set<String> getSupportedTableTypes() { return supportedTableTypes; }
}
```

## 3. 具体Procedure实现

### 3.1 IcebergSnapshotProcedure

```java
public class IcebergSnapshotProcedure extends TableProcedure {
    // 简单的静态单例 - JVM保证线程安全
    private static final IcebergSnapshotProcedure INSTANCE = new IcebergSnapshotProcedure();
    
    // 私有构造函数
    private IcebergSnapshotProcedure() {
        super("snapshot", buildParameters(), 
              "Create a snapshot of the Iceberg table",
              Sets.newHashSet("iceberg"));
    }
    
    // 获取单例实例
    public static IcebergSnapshotProcedure getInstance() {
        return INSTANCE;
    }
    
    private static List<ProcedureParameter> buildParameters() {
        return Arrays.asList(
            new ProcedureParameter("operation", Type.VARCHAR, false, 
                "Snapshot operation: append, overwrite, merge", "append"),
            new ProcedureParameter("where_condition", Type.VARCHAR, false, 
                "Filter condition for the operation", null),
            new ProcedureParameter("options", Type.VARCHAR, false, 
                "Additional options in JSON format", "{}")
        );
    }
    
    @Override
    protected void executeInternal(List<Object> args, TableContext tableContext, 
                                 ConnectContext ctx) throws UserException {
        String operation = (String) args.get(0);
        String whereCondition = (String) args.get(1);
        String options = (String) args.get(2);
        
        try {
            // 获取Iceberg原生表对象
            org.apache.iceberg.Table icebergTable = 
                tableContext.getNativeTable(org.apache.iceberg.Table.class);
            
            // 解析选项
            Map<String, Object> optionsMap = parseOptions(options);
            
            // 执行不同类型的snapshot操作
            Snapshot snapshot = executeSnapshotOperation(
                icebergTable, operation, whereCondition, optionsMap);
            
            LOG.info("Snapshot created successfully. Snapshot ID: {}, Table: {}.{}.{}", 
                    snapshot.snapshotId(),
                    tableContext.getCatalog().getName(),
                    tableContext.getTable().getDbName(),
                    tableContext.getTable().getName());
                    
        } catch (Exception e) {
            throw new UserException(String.format(
                "Failed to create snapshot for table %s: %s", 
                tableContext.getTable().getName(), e.getMessage()), e);
        }
    }
    
    private Map<String, Object> parseOptions(String options) throws UserException {
        try {
            if (Strings.isNullOrEmpty(options) || "{}".equals(options.trim())) {
                return Maps.newHashMap();
            }
            
            Map<String, Object> result = Maps.newHashMap();
            String cleaned = options.trim().replaceAll("[{}]", "");
            
            if (!cleaned.isEmpty()) {
                String[] pairs = cleaned.split(",");
                for (String pair : pairs) {
                    String[] kv = pair.split(":", 2);
                    if (kv.length == 2) {
                        String key = kv[0].trim().replaceAll("[\"\']", "");
                        String value = kv[1].trim().replaceAll("[\"\']", "");
                        result.put(key, value);
                    }
                }
            }
            
            return result;
        } catch (Exception e) {
            throw new UserException("Invalid options format: " + options, e);
        }
    }
    
    private Snapshot executeSnapshotOperation(org.apache.iceberg.Table table, String operation,
                                            String whereCondition, Map<String, Object> options) 
            throws UserException {
        
        switch (operation.toLowerCase()) {
            case "append":
                return executeAppendSnapshot(table, options);
                
            case "overwrite":
                return executeOverwriteSnapshot(table, whereCondition, options);
                
            case "merge":
                throw new UserException("Merge operation not yet implemented");
                
            default:
                throw new UserException("Unsupported snapshot operation: " + operation);
        }
    }
    
    private Snapshot executeAppendSnapshot(org.apache.iceberg.Table table, 
                                         Map<String, Object> options) {
        AppendFiles append = table.newAppend();
        
        // 应用选项
        if (options.containsKey("snapshot-property")) {
            String value = (String) options.get("snapshot-property");
            String[] parts = value.split("=", 2);
            if (parts.length == 2) {
                append.set(parts[0], parts[1]);
            }
        }
        
        append.commit();
        return table.currentSnapshot();
    }
    
    private Snapshot executeOverwriteSnapshot(org.apache.iceberg.Table table, 
                                            String whereCondition, Map<String, Object> options) {
        OverwriteFiles overwrite = table.newOverwrite();
        
        // 应用WHERE条件
        if (!Strings.isNullOrEmpty(whereCondition)) {
            // 解析WHERE条件并应用过滤器
            // overwrite.overwriteByRowFilter(parseFilter(whereCondition));
        }
        
        // 应用选项
        if (options.containsKey("validate-from-snapshot-id")) {
            Long snapshotId = Long.parseLong((String) options.get("validate-from-snapshot-id"));
            overwrite.validateFromSnapshot(snapshotId);
        }
        
        overwrite.commit();
        return table.currentSnapshot();
    }
}
```

### 3.2 IcebergExpireSnapshotsProcedure

```java
public class IcebergExpireSnapshotsProcedure extends TableProcedure {
    private static final IcebergExpireSnapshotsProcedure INSTANCE = new IcebergExpireSnapshotsProcedure();
    
    private IcebergExpireSnapshotsProcedure() {
        super("expire_snapshots", buildParameters(), 
              "Expire old snapshots of the Iceberg table",
              Sets.newHashSet("iceberg"));
    }
    
    public static IcebergExpireSnapshotsProcedure getInstance() {
        return INSTANCE;
    }
    
    private static List<ProcedureParameter> buildParameters() {
        return Arrays.asList(
            new ProcedureParameter("older_than", Type.DATETIME, false, 
                "Expire snapshots older than this timestamp", null),
            new ProcedureParameter("retain_last", Type.INT, false, 
                "Number of snapshots to retain", 5),
            new ProcedureParameter("max_snapshot_age_ms", Type.BIGINT, false, 
                "Maximum age of snapshots in milliseconds", null)
        );
    }
    
    @Override
    protected void executeInternal(List<Object> args, TableContext tableContext, 
                                 ConnectContext ctx) throws UserException {
        Long olderThanMillis = (Long) args.get(0);
        Integer retainLast = (Integer) args.get(1);
        Long maxSnapshotAgeMs = (Long) args.get(2);
        
        try {
            org.apache.iceberg.Table icebergTable = 
                tableContext.getNativeTable(org.apache.iceberg.Table.class);
            
            ExpireSnapshots expireSnapshots = icebergTable.expireSnapshots();
            
            // 设置过期条件
            if (olderThanMillis != null) {
                expireSnapshots.expireOlderThan(olderThanMillis);
            }
            
            if (retainLast != null && retainLast > 0) {
                expireSnapshots.retainLast(retainLast);
            }
            
            if (maxSnapshotAgeMs != null) {
                long cutoffTime = System.currentTimeMillis() - maxSnapshotAgeMs;
                expireSnapshots.expireOlderThan(cutoffTime);
            }
            
            expireSnapshots.commit();
            
            LOG.info("Snapshots expired successfully for table: {}.{}.{}", 
                    tableContext.getCatalog().getName(),
                    tableContext.getTable().getDbName(),
                    tableContext.getTable().getName());
                    
        } catch (Exception e) {
            throw new UserException(String.format(
                "Failed to expire snapshots for table %s: %s", 
                tableContext.getTable().getName(), e.getMessage()), e);
        }
    }
}
```

### 3.3 PaimonCompactProcedure

```java
public class PaimonCompactProcedure extends TableProcedure {
    private static final PaimonCompactProcedure INSTANCE = new PaimonCompactProcedure();
    
    private PaimonCompactProcedure() {
        super("compact", buildParameters(), 
              "Compact small files for the Paimon table",
              Sets.newHashSet("paimon"));
    }
    
    public static PaimonCompactProcedure getInstance() {
        return INSTANCE;
    }
    
    private static List<ProcedureParameter> buildParameters() {
        return Arrays.asList(
            new ProcedureParameter("partitions", Type.VARCHAR, false, 
                "Partition filter expression", null),
            new ProcedureParameter("order_strategy", Type.VARCHAR, false, 
                "Order strategy: none, zorder, hilbert", "none"),
            new ProcedureParameter("order_columns", Type.VARCHAR, false, 
                "Columns for ordering", null),
            new ProcedureParameter("where_condition", Type.VARCHAR, false, 
                "Additional filter condition", null)
        );
    }
    
    @Override
    protected void executeInternal(List<Object> args, TableContext tableContext, 
                                 ConnectContext ctx) throws UserException {
        String partitions = (String) args.get(0);
        String orderStrategy = (String) args.get(1);
        String orderColumns = (String) args.get(2);
        String whereCondition = (String) args.get(3);
        
        try {
            org.apache.paimon.table.Table paimonTable = 
                tableContext.getNativeTable(org.apache.paimon.table.Table.class);
            
            // 构建压缩计划
            CompactionPlan compactionPlan = buildCompactionPlan(
                paimonTable, partitions, whereCondition);
            
            if (compactionPlan.isEmpty()) {
                LOG.info("No files need compaction for table: {}", 
                        tableContext.getTable().getName());
                return;
            }
            
            // 执行压缩
            executeCompaction(paimonTable, compactionPlan, orderStrategy, orderColumns);
            
            LOG.info("Table compaction completed. Table: {}.{}.{}, " +
                    "Compacted files: {}", 
                    tableContext.getCatalog().getName(),
                    tableContext.getTable().getDbName(),
                    tableContext.getTable().getName(),
                    compactionPlan.files().size());
                    
        } catch (Exception e) {
            throw new UserException(String.format(
                "Failed to compact table %s: %s", 
                tableContext.getTable().getName(), e.getMessage()), e);
        }
    }
    
    private CompactionPlan buildCompactionPlan(org.apache.paimon.table.Table table,
                                              String partitions, String whereCondition) 
            throws Exception {
        CompactionPlanner planner = table.store().newCompaction();
        
        // 应用分区过滤
        if (!Strings.isNullOrEmpty(partitions)) {
            Map<String, String> partitionFilter = parsePartitionFilter(partitions);
            // 这里需要根据Paimon API设置分区过滤
        }
        
        return planner.plan();
    }
    
    private Map<String, String> parsePartitionFilter(String partitions) {
        Map<String, String> result = Maps.newHashMap();
        String[] pairs = partitions.split(",");
        for (String pair : pairs) {
            String[] kv = pair.trim().split("=");
            if (kv.length == 2) {
                result.put(kv[0].trim(), kv[1].trim());
            }
        }
        return result;
    }
    
    private void executeCompaction(org.apache.paimon.table.Table table,
                                  CompactionPlan plan, String orderStrategy, 
                                  String orderColumns) throws Exception {
        CompactionTask compactionTask = table.store().newCompaction();
        compactionTask.compact(plan);
    }
}
```

## 4. 简化的注册中心

### 4.1 ProcedureRegistry

```java
public class ProcedureRegistry {
    private static final Logger LOG = LogManager.getLogger(ProcedureRegistry.class);
    
    // 静态初始化所有Procedure单例
    private static final Map<String, TableProcedure> ALL_PROCEDURES = 
        Maps.newHashMapWithExpectedSize(20);
    
    // 按catalog类型分组的索引
    private static final Map<String, List<TableProcedure>> PROCEDURES_BY_TYPE = 
        Maps.newHashMapWithExpectedSize(5);
    
    static {
        initializeProcedures();
    }
    
    private static void initializeProcedures() {
        LOG.info("Initializing procedure registry...");
        
        // 注册Iceberg Procedures
        registerProcedure(IcebergSnapshotProcedure.getInstance());
        registerProcedure(IcebergExpireSnapshotsProcedure.getInstance());
        registerProcedure(IcebergRollbackToSnapshotProcedure.getInstance());
        registerProcedure(IcebergCreateBranchProcedure.getInstance());
        registerProcedure(IcebergDropBranchProcedure.getInstance());
        registerProcedure(IcebergCreateTagProcedure.getInstance());
        registerProcedure(IcebergDropTagProcedure.getInstance());
        registerProcedure(IcebergRemoveOrphanFilesProcedure.getInstance());
        registerProcedure(IcebergRewriteDataFilesProcedure.getInstance());
        registerProcedure(IcebergRewriteManifestsProcedure.getInstance());
        
        // 注册Paimon Procedures
        registerProcedure(PaimonCompactProcedure.getInstance());
        registerProcedure(PaimonCreateBranchProcedure.getInstance());
        registerProcedure(PaimonDropBranchProcedure.getInstance());
        registerProcedure(PaimonCreateTagProcedure.getInstance());
        registerProcedure(PaimonDropTagProcedure.getInstance());
        registerProcedure(PaimonExpireSnapshotsProcedure.getInstance());
        registerProcedure(PaimonRemoveOrphanFilesProcedure.getInstance());
        
        LOG.info("Registered {} procedures for {} catalog types", 
                ALL_PROCEDURES.size(), PROCEDURES_BY_TYPE.size());
    }
    
    private static void registerProcedure(TableProcedure procedure) {
        String procedureName = procedure.getProcedureName().toLowerCase();
        ALL_PROCEDURES.put(procedureName, procedure);
        
        // 按catalog类型建立索引
        for (String catalogType : procedure.getSupportedTableTypes()) {
            PROCEDURES_BY_TYPE.computeIfAbsent(catalogType, k -> Lists.newArrayList())
                              .add(procedure);
        }
        
        LOG.debug("Registered procedure: {} for types: {}", 
                procedureName, procedure.getSupportedTableTypes());
    }
    
    /**
     * 获取Procedure实例
     */
    public static Optional<TableProcedure> getProcedure(String procedureName) {
        return Optional.ofNullable(ALL_PROCEDURES.get(procedureName.toLowerCase()));
    }
    
    /**
     * 获取指定类型支持的所有Procedures
     */
    public static List<TableProcedure> getProceduresByType(String catalogType) {
        List<TableProcedure> procedures = PROCEDURES_BY_TYPE.get(catalogType);
        return procedures != null ? Lists.newArrayList(procedures) : Lists.newArrayList();
    }
    
    /**
     * 获取所有Procedure
     */
    public static List<TableProcedure> getAllProcedures() {
        return Lists.newArrayList(ALL_PROCEDURES.values());
    }
    
    /**
     * 检查Procedure是否存在
     */
    public static boolean exists(String procedureName) {
        return ALL_PROCEDURES.containsKey(procedureName.toLowerCase());
    }
    
    /**
     * 获取支持的Catalog类型
     */
    public static Set<String> getSupportedCatalogTypes() {
        return Sets.newHashSet(PROCEDURES_BY_TYPE.keySet());
    }
}
```

## 5. ExternalCatalog集成

### 5.1 简化的ExternalCatalog扩展

```java
public abstract class ExternalCatalog implements CatalogIf<ExternalDatabase<? extends ExternalTable>>, 
                                               Writable, GsonPostProcessable {
    // 现有代码...
    
    /**
     * 获取指定名称的Procedure
     */
    public final Optional<TableProcedure> getProcedure(String procedureName) {
        Optional<TableProcedure> procedure = ProcedureRegistry.getProcedure(procedureName);
        
        // 验证该procedure是否支持当前catalog类型
        if (procedure.isPresent() && 
            procedure.get().getSupportedTableTypes().contains(getType())) {
            return procedure;
        }
        
        return Optional.empty();
    }
    
    /**
     * 列出所有支持的Procedures
     */
    public final List<ProcedureInfo> listProcedures() {
        return ProcedureRegistry.getProceduresByType(getType())
            .stream()
            .map(proc -> new ProcedureInfo(
                proc.getProcedureName(),
                proc.getParameters(),
                proc.getDescription(),
                getType()
            ))
            .collect(Collectors.toList());
    }
    
    /**
     * 检查是否支持Procedures
     */
    public boolean supportsProcedures() {
        return !ProcedureRegistry.getProceduresByType(getType()).isEmpty();
    }
    
    /**
     * 执行指定的Procedure
     */
    public final void executeProcedure(String procedureName, List<Expression> args, 
                                      TableIf table, ConnectContext ctx) throws UserException {
        Optional<TableProcedure> procedure = getProcedure(procedureName);
        if (!procedure.isPresent()) {
            throw new UserException(String.format(
                "Procedure '%s' not found in catalog '%s' of type '%s'", 
                procedureName, getName(), getType()));
        }
        
        procedure.get().execute(args, table, ctx);
    }
}
```

## 6. 使用示例

### 6.1 基本使用

```sql
-- 创建catalogs
CREATE CATALOG iceberg_catalog PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "hms",
    "hive.metastore.uris" = "thrift://localhost:9083"
);

-- 查看支持的procedures
SHOW PROCEDURES FROM iceberg_catalog;
-- +------------------+------------------+--------------------------------------------------+--------+
-- | Catalog          | Procedure        | Parameters                                       | Type   |
-- +------------------+------------------+--------------------------------------------------+--------+
-- | iceberg_catalog  | snapshot         | operation VARCHAR DEFAULT 'append',...          | ICEBERG|
-- | iceberg_catalog  | expire_snapshots | older_than DATETIME,...                          | ICEBERG|
-- +------------------+------------------+--------------------------------------------------+--------+

-- 执行procedures
ALTER TABLE iceberg_catalog.sales_db.orders 
EXECUTE snapshot('append');

ALTER TABLE iceberg_catalog.sales_db.orders 
EXECUTE expire_snapshots(timestamp('2024-01-01 00:00:00'), 5);

-- Paimon示例
ALTER TABLE paimon_catalog.warehouse_db.inventory 
EXECUTE compact('dt=2024-01-01', 'zorder', 'product_id,category');
```

## 7. 性能对比

### 7.1 静态单例 vs 懒加载单例

| 特性 | 静态单例 | 懒加载单例 |
|------|----------|------------|
| **实现复杂度** | 极简 | 复杂 |
| **线程安全** | JVM保证 | 需要同步机制 |
| **首次访问性能** | 无延迟 | 有初始化开销 |
| **运行时性能** | 最佳 | 略有同步开销 |
| **内存使用** | 启动时分配 | 按需分配 |
| **代码维护** | 易维护 | 需要注意同步 |

### 7.2 内存使用分析

```java
// 假设有10种Procedure，每个Catalog创建实例的内存对比：

// 原始方案（每个Catalog一个实例）
// 5个Catalog × 10个Procedure × 每个实例1KB = 50KB

// 简化单例方案
// 10个Procedure × 每个实例1KB = 10KB
// 内存节省：80%
```

## 8. 测试验证

### 8.1 单例正确性测试

```java
@Test
public void testStaticSingleton() {
    // 验证多次获取是同一个实例
    TableProcedure p1 = IcebergSnapshotProcedure.getInstance();
    TableProcedure p2 = IcebergSnapshotProcedure.getInstance();
    assertThat(p1).isSameAs(p2);
}

@Test
public void testRegistryInitialization() {
    // 验证注册表正确初始化
    Optional<TableProcedure> procedure = ProcedureRegistry.getProcedure("snapshot");
    assertThat(procedure).isPresent();
    assertThat(procedure.get()).isInstanceOf(IcebergSnapshotProcedure.class);
}

@Test
public void testConcurrentAccess() throws Exception {
    // 验证并发访问的正确性
    int threadCount = 100;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    Set<TableProcedure> instances = ConcurrentHashMap.newKeySet();
    
    List<Future<Void>> futures = new ArrayList<>();
    for (int i = 0; i < threadCount; i++) {
        futures.add(executor.submit(() -> {
            instances.add(IcebergSnapshotProcedure.getInstance());
            return null;
        }));
    }
    
    for (Future<Void> future : futures) {
        future.get();
    }
    
    // 所有线程应该获得同一个实例
    assertThat(instances).hasSize(1);
}
```

## 9. 总结

### 9.1 简化方案优势

1. **极简实现**：只需要`private static final INSTANCE = new XxxProcedure()`
2. **性能最优**：无任何同步开销，JVM保证线程安全
3. **内存高效**：每种Procedure只有一个实例，节省大量内存
4. **维护简单**：代码清晰，无复杂的同步逻辑
5. **启动快速**：类加载时完成初始化，无运行时延迟

### 9.2 适用场景

- Procedure数量有限且相对固定
- 系统对启动时间不敏感
- 追求最简实现和最佳运行时性能
- 内存资源需要优化的环境

### 9.3 实施建议

1. **渐进迁移**：先改造核心Procedure，逐步扩展
2. **充分测试**：重点测试并发访问和内存使用
3. **监控完善**：监控Procedure执行性能和错误率
4. **文档更新**：更新开发规范，说明单例实现要求

通过这种简化的静态单例方案，在保持功能完整性的同时，获得了最佳的性能和最简的实现，是一个实用且高效的解决方案。