# 基于单例模式的TableProcedure架构优化设计方案

## 1. 方案概述

### 1.1 设计目标
本方案将TableProcedure从实例化模式改为单例模式，每个Procedure类型只维护一个全局实例，通过这种方式优化内存使用、提高性能并简化管理。

### 1.2 单例模式的优势
1. **内存优化**：每种Procedure只有一个实例，大幅减少内存占用
2. **性能提升**：避免重复创建对象的开销
3. **线程安全**：通过合理设计确保并发执行的安全性
4. **配置统一**：所有Catalog共享同一套Procedure配置
5. **管理简化**：集中管理Procedure实例，便于监控和维护

### 1.3 潜在挑战
1. **线程安全**：多个并发请求可能同时访问同一个Procedure实例
2. **状态管理**：Procedure实例不能维护请求相关的状态
3. **上下文传递**：需要通过参数传递所有必要的上下文信息
4. **初始化控制**：需要合理的初始化时机和策略

## 2. 单例架构设计

### 2.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                    ProcedureRegistry                            │
│                   (单例管理中心)                                │
├─────────────────────────────────────────────────────────────────┤
│  procedure_name -> Singleton TableProcedure Instance           │
├─────────────────────────────────────────────────────────────────┤
│ SnapshotProcedure.getInstance() │ CompactProcedure.getInstance()│
├─────────────────────────────────────────────────────────────────┤
│              AbstractSingletonProcedure                        │
│                   (单例基类)                                    │
├─────────────────────────────────────────────────────────────────┤
│          ExecutionContext        │         ProcedureMetrics     │
│        (执行上下文对象)           │          (指标收集)          │
├─────────────────────────────────────────────────────────────────┤
│  IcebergExternalCatalog │ PaimonExternalCatalog │ OtherCatalogs │
│     (引用单例实例)       │    (引用单例实例)      │               │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 核心组件设计

#### 2.2.1 AbstractSingletonProcedure抽象基类

```java
public abstract class AbstractSingletonProcedure {
    private static final Logger LOG = LogManager.getLogger(AbstractSingletonProcedure.class);
    
    // Procedure基本信息（不可变）
    protected final String procedureName;
    protected final List<ProcedureParameter> parameters;
    protected final String description;
    protected final Set<String> supportedTableTypes;
    
    // 线程安全的执行计数器
    private final AtomicLong executionCounter = new AtomicLong(0);
    private final AtomicLong successCounter = new AtomicLong(0);
    private final AtomicLong errorCounter = new AtomicLong(0);
    
    // 构造函数 - 只在实例创建时调用一次
    protected AbstractSingletonProcedure(String procedureName, 
                                        List<ProcedureParameter> parameters,
                                        String description, 
                                        Set<String> supportedTableTypes) {
        this.procedureName = procedureName;
        this.parameters = Collections.unmodifiableList(new ArrayList<>(parameters));
        this.description = description;
        this.supportedTableTypes = Collections.unmodifiableSet(new HashSet<>(supportedTableTypes));
        
        // 注册到全局监控
        registerMetrics();
    }
    
    /**
     * 主要执行方法 - 线程安全的模板方法
     * 所有状态都通过ExecutionContext传递，不依赖实例状态
     */
    public final void execute(List<Expression> args, TableIf table, ConnectContext ctx) 
            throws UserException {
        
        long executionId = executionCounter.incrementAndGet();
        long startTime = System.currentTimeMillis();
        
        // 创建本次执行的上下文对象
        ExecutionContext executionContext = createExecutionContext(
            executionId, args, table, ctx, startTime);
        
        try {
            // 执行前置验证（无状态）
            validateExecution(executionContext);
            
            // 执行核心逻辑（无状态）
            executeInternal(executionContext);
            
            // 记录成功
            successCounter.incrementAndGet();
            recordExecutionMetrics(executionContext, true, null);
            
        } catch (Exception e) {
            // 记录失败
            errorCounter.incrementAndGet();
            recordExecutionMetrics(executionContext, false, e);
            throw e;
        }
    }
    
    /**
     * 创建执行上下文 - 每次执行都创建新的上下文对象
     */
    protected ExecutionContext createExecutionContext(long executionId,
                                                     List<Expression> args,
                                                     TableIf table, 
                                                     ConnectContext ctx,
                                                     long startTime) throws UserException {
        
        // 验证表类型支持
        validateTableTypeSupport(table);
        
        // 验证并转换参数
        List<Object> processedArgs = validateAndProcessArguments(args, table);
        
        // 构建表上下文
        TableContext tableContext = buildTableContext(table, ctx);
        
        return new ExecutionContext(
            executionId, procedureName, processedArgs, 
            tableContext, ctx, startTime);
    }
    
    /**
     * 验证表类型支持
     */
    private void validateTableTypeSupport(TableIf table) throws UserException {
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
    
    /**
     * 参数验证和处理 - 无状态方法
     */
    protected List<Object> validateAndProcessArguments(List<Expression> args, TableIf table) 
            throws UserException {
        
        if (args.size() > parameters.size()) {
            throw new UserException(String.format(
                "Too many arguments for procedure %s. Expected: %d, Got: %d",
                procedureName, parameters.size(), args.size()));
        }
        
        List<Object> result = new ArrayList<>();
        
        for (int i = 0; i < parameters.size(); i++) {
            ProcedureParameter param = parameters.get(i);
            
            if (i < args.size()) {
                // 用户提供的参数
                Expression arg = args.get(i);
                param.validate(arg);
                result.add(param.convertValue(arg));
            } else {
                // 使用默认值或从表上下文推导
                Object defaultValue = resolveDefaultValue(param, table);
                result.add(defaultValue);
            }
        }
        
        return result;
    }
    
    /**
     * 解析默认参数值 - 无状态方法
     */
    protected Object resolveDefaultValue(ProcedureParameter param, TableIf table) 
            throws UserException {
        
        if (param.getDefaultValue() != null) {
            return param.getDefaultValue();
        }
        
        // 从表上下文推导默认值
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
    
    /**
     * 构建表上下文 - 每次执行创建新对象
     */
    protected TableContext buildTableContext(TableIf table, ConnectContext ctx) {
        ExternalTable externalTable = (ExternalTable) table;
        return new TableContext(
            externalTable,
            externalTable.getCatalog(),
            ctx.getCurrentUserIdentity(),
            ctx
        );
    }
    
    /**
     * 执行前置验证 - 子类可重写
     */
    protected void validateExecution(ExecutionContext context) throws UserException {
        // 默认实现：检查权限
        checkPermissions(context);
    }
    
    /**
     * 权限检查 - 无状态方法
     */
    protected void checkPermissions(ExecutionContext context) throws UserException {
        UserIdentity user = context.getConnectContext().getCurrentUserIdentity();
        String catalogName = context.getTableContext().getCatalog().getName();
        
        if (!Env.getCurrentEnv().getAccessManager().checkCtlPriv(
                user, catalogName, PrivPredicate.LOAD)) {
            throw new UserException(String.format(
                "User %s has no privilege to execute procedure in catalog %s", 
                user, catalogName));
        }
    }
    
    /**
     * 核心执行逻辑 - 子类必须实现的抽象方法
     */
    protected abstract void executeInternal(ExecutionContext context) throws UserException;
    
    /**
     * 注册监控指标
     */
    private void registerMetrics() {
        ProcedureMetrics.registerProcedure(procedureName);
    }
    
    /**
     * 记录执行指标
     */
    private void recordExecutionMetrics(ExecutionContext context, boolean success, Exception error) {
        long duration = System.currentTimeMillis() - context.getStartTime();
        String catalogType = context.getTableContext().getCatalog().getType();
        
        ProcedureMetrics.recordExecution(catalogType, procedureName, success, duration / 1000.0);
        
        if (success) {
            LOG.info("Procedure execution completed. ID: {}, Procedure: {}, " +
                    "Table: {}.{}.{}, Duration: {}ms",
                    context.getExecutionId(), procedureName,
                    catalogType,
                    context.getTableContext().getTable().getDbName(),
                    context.getTableContext().getTable().getName(),
                    duration);
        } else {
            LOG.error("Procedure execution failed. ID: {}, Procedure: {}, " +
                    "Table: {}.{}.{}, Duration: {}ms, Error: {}",
                    context.getExecutionId(), procedureName,
                    catalogType,
                    context.getTableContext().getTable().getDbName(),
                    context.getTableContext().getTable().getName(),
                    duration, error.getMessage());
        }
    }
    
    // Getter methods (线程安全)
    public String getProcedureName() { return procedureName; }
    public List<ProcedureParameter> getParameters() { return parameters; }
    public String getDescription() { return description; }
    public Set<String> getSupportedTableTypes() { return supportedTableTypes; }
    
    // 统计信息 (线程安全)
    public long getExecutionCount() { return executionCounter.get(); }
    public long getSuccessCount() { return successCounter.get(); }
    public long getErrorCount() { return errorCounter.get(); }
    public double getSuccessRate() {
        long total = getExecutionCount();
        return total == 0 ? 0.0 : (double) getSuccessCount() / total;
    }
}
```

#### 2.2.2 ExecutionContext执行上下文

```java
public class ExecutionContext {
    private final long executionId;
    private final String procedureName;
    private final List<Object> processedArgs;
    private final TableContext tableContext;
    private final ConnectContext connectContext;
    private final long startTime;
    
    // 执行过程中的动态数据
    private final Map<String, Object> executionData;
    
    public ExecutionContext(long executionId, String procedureName,
                           List<Object> processedArgs, TableContext tableContext,
                           ConnectContext connectContext, long startTime) {
        this.executionId = executionId;
        this.procedureName = procedureName;
        this.processedArgs = Collections.unmodifiableList(new ArrayList<>(processedArgs));
        this.tableContext = tableContext;
        this.connectContext = connectContext;
        this.startTime = startTime;
        this.executionData = new ConcurrentHashMap<>();
    }
    
    // 线程安全的数据存储
    public void putData(String key, Object value) {
        executionData.put(key, value);
    }
    
    @SuppressWarnings("unchecked")
    public <T> T getData(String key, Class<T> type) {
        Object value = executionData.get(key);
        return type.isInstance(value) ? (T) value : null;
    }
    
    public <T> T getData(String key, Class<T> type, T defaultValue) {
        T value = getData(key, type);
        return value != null ? value : defaultValue;
    }
    
    // Getters (不可变数据)
    public long getExecutionId() { return executionId; }
    public String getProcedureName() { return procedureName; }
    public List<Object> getProcessedArgs() { return processedArgs; }
    public TableContext getTableContext() { return tableContext; }
    public ConnectContext getConnectContext() { return connectContext; }
    public long getStartTime() { return startTime; }
    
    // 便利方法
    public Object getArg(int index) {
        return index < processedArgs.size() ? processedArgs.get(index) : null;
    }
    
    @SuppressWarnings("unchecked")
    public <T> T getArg(int index, Class<T> type) {
        Object arg = getArg(index);
        return type.isInstance(arg) ? (T) arg : null;
    }
    
    public <T> T getArg(int index, Class<T> type, T defaultValue) {
        T arg = getArg(index, type);
        return arg != null ? arg : defaultValue;
    }
}
```

#### 2.2.3 SingletonProcedureRegistry单例注册管理

```java
public class SingletonProcedureRegistry {
    private static final Logger LOG = LogManager.getLogger(SingletonProcedureRegistry.class);
    
    // 线程安全的单例存储
    private static final ConcurrentMap<String, AbstractSingletonProcedure> PROCEDURE_INSTANCES = 
        new ConcurrentHashMap<>();
    
    // 按类型分组的索引 (用于快速查找)
    private static final ConcurrentMap<String, Set<String>> TYPE_TO_PROCEDURES = 
        new ConcurrentHashMap<>();
    
    // 初始化锁
    private static final Object INIT_LOCK = new Object();
    private static volatile boolean initialized = false;
    
    /**
     * 确保注册表已初始化
     */
    public static void ensureInitialized() {
        if (!initialized) {
            synchronized (INIT_LOCK) {
                if (!initialized) {
                    initializeBuiltinProcedures();
                    initialized = true;
                }
            }
        }
    }
    
    /**
     * 初始化内置Procedures
     */
    private static void initializeBuiltinProcedures() {
        LOG.info("Initializing singleton procedures...");
        
        // Iceberg Procedures
        registerProcedure(IcebergSnapshotProcedure.getInstance());
        registerProcedure(IcebergRollbackToSnapshotProcedure.getInstance());
        registerProcedure(IcebergExpireSnapshotsProcedure.getInstance());
        registerProcedure(IcebergCreateBranchProcedure.getInstance());
        registerProcedure(IcebergDropBranchProcedure.getInstance());
        registerProcedure(IcebergCreateTagProcedure.getInstance());
        registerProcedure(IcebergDropTagProcedure.getInstance());
        registerProcedure(IcebergRemoveOrphanFilesProcedure.getInstance());
        registerProcedure(IcebergRewriteDataFilesProcedure.getInstance());
        registerProcedure(IcebergRewriteManifestsProcedure.getInstance());
        
        // Paimon Procedures
        registerProcedure(PaimonCompactProcedure.getInstance());
        registerProcedure(PaimonCreateBranchProcedure.getInstance());
        registerProcedure(PaimonDropBranchProcedure.getInstance());
        registerProcedure(PaimonCreateTagProcedure.getInstance());
        registerProcedure(PaimonDropTagProcedure.getInstance());
        registerProcedure(PaimonExpireSnapshotsProcedure.getInstance());
        registerProcedure(PaimonRemoveOrphanFilesProcedure.getInstance());
        
        LOG.info("Initialized {} singleton procedures", PROCEDURE_INSTANCES.size());
    }
    
    /**
     * 注册单例Procedure
     */
    private static void registerProcedure(AbstractSingletonProcedure procedure) {
        String procedureName = procedure.getProcedureName().toLowerCase();
        
        PROCEDURE_INSTANCES.put(procedureName, procedure);
        
        // 更新类型索引
        for (String catalogType : procedure.getSupportedTableTypes()) {
            TYPE_TO_PROCEDURES.computeIfAbsent(catalogType, k -> ConcurrentHashMap.newKeySet())
                              .add(procedureName);
        }
        
        LOG.debug("Registered singleton procedure: {} for types: {}", 
                procedureName, procedure.getSupportedTableTypes());
    }
    
    /**
     * 获取Procedure实例
     */
    public static Optional<AbstractSingletonProcedure> getProcedure(String procedureName) {
        ensureInitialized();
        return Optional.ofNullable(PROCEDURE_INSTANCES.get(procedureName.toLowerCase()));
    }
    
    /**
     * 获取指定类型支持的所有Procedures
     */
    public static List<AbstractSingletonProcedure> getProceduresByType(String catalogType) {
        ensureInitialized();
        
        Set<String> procedureNames = TYPE_TO_PROCEDURES.get(catalogType);
        if (procedureNames == null) {
            return Collections.emptyList();
        }
        
        return procedureNames.stream()
            .map(PROCEDURE_INSTANCES::get)
            .filter(Objects::nonNull)
            .sorted((a, b) -> a.getProcedureName().compareTo(b.getProcedureName()))
            .collect(Collectors.toList());
    }
    
    /**
     * 获取所有Procedure实例
     */
    public static List<AbstractSingletonProcedure> getAllProcedures() {
        ensureInitialized();
        return new ArrayList<>(PROCEDURE_INSTANCES.values());
    }
    
    /**
     * 检查Procedure是否存在
     */
    public static boolean exists(String procedureName) {
        ensureInitialized();
        return PROCEDURE_INSTANCES.containsKey(procedureName.toLowerCase());
    }
    
    /**
     * 获取支持的Catalog类型
     */
    public static Set<String> getSupportedCatalogTypes() {
        ensureInitialized();
        return new HashSet<>(TYPE_TO_PROCEDURES.keySet());
    }
    
    /**
     * 获取统计信息
     */
    public static Map<String, Object> getStatistics() {
        ensureInitialized();
        
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalProcedures", PROCEDURE_INSTANCES.size());
        stats.put("supportedCatalogTypes", TYPE_TO_PROCEDURES.keySet());
        
        // 按类型统计
        Map<String, Integer> typeStats = new HashMap<>();
        TYPE_TO_PROCEDURES.forEach((type, procedures) -> 
            typeStats.put(type, procedures.size()));
        stats.put("proceduresByType", typeStats);
        
        // 执行统计
        long totalExecutions = PROCEDURE_INSTANCES.values().stream()
            .mapToLong(AbstractSingletonProcedure::getExecutionCount)
            .sum();
        long totalSuccesses = PROCEDURE_INSTANCES.values().stream()
            .mapToLong(AbstractSingletonProcedure::getSuccessCount)
            .sum();
        
        stats.put("totalExecutions", totalExecutions);
        stats.put("totalSuccesses", totalSuccesses);
        stats.put("overallSuccessRate", totalExecutions == 0 ? 0.0 : 
                 (double) totalSuccesses / totalExecutions);
        
        return stats;
    }
}
```

## 3. 具体Procedure单例实现

### 3.1 IcebergSnapshotProcedure单例实现

```java
public class IcebergSnapshotProcedure extends AbstractSingletonProcedure {
    
    // 单例实例 - 线程安全的懒加载
    private static volatile IcebergSnapshotProcedure instance;
    private static final Object INSTANCE_LOCK = new Object();
    
    // 私有构造函数
    private IcebergSnapshotProcedure() {
        super("snapshot", buildParameters(), 
              "Create a snapshot of the Iceberg table",
              Sets.newHashSet("iceberg"));
    }
    
    /**
     * 获取单例实例 - 双重检查锁定
     */
    public static IcebergSnapshotProcedure getInstance() {
        if (instance == null) {
            synchronized (INSTANCE_LOCK) {
                if (instance == null) {
                    instance = new IcebergSnapshotProcedure();
                }
            }
        }
        return instance;
    }
    
    /**
     * 构建参数定义 - 静态方法，无状态
     */
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
    
    /**
     * 核心执行逻辑 - 无状态实现
     */
    @Override
    protected void executeInternal(ExecutionContext context) throws UserException {
        String operation = context.getArg(0, String.class, "append");
        String whereCondition = context.getArg(1, String.class);
        String options = context.getArg(2, String.class, "{}");
        
        try {
            // 获取Iceberg原生表对象
            org.apache.iceberg.Table icebergTable = 
                context.getTableContext().getNativeTable(org.apache.iceberg.Table.class);
            
            // 解析选项 - 每次执行都重新解析
            Map<String, Object> optionsMap = parseOptions(options);
            context.putData("parsedOptions", optionsMap);
            
            // 执行不同类型的snapshot操作
            Snapshot snapshot = executeSnapshotOperation(
                context, icebergTable, operation, whereCondition, optionsMap);
            
            // 存储执行结果
            context.putData("resultSnapshot", snapshot);
            
        } catch (Exception e) {
            throw new UserException(String.format(
                "Failed to create snapshot for table %s: %s", 
                context.getTableContext().getTable().getName(), e.getMessage()), e);
        }
    }
    
    /**
     * 解析选项 - 无状态静态方法
     */
    private static Map<String, Object> parseOptions(String options) throws UserException {
        try {
            if (Strings.isNullOrEmpty(options) || "{}".equals(options.trim())) {
                return new HashMap<>();
            }
            
            Map<String, Object> result = new HashMap<>();
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
    
    /**
     * 执行snapshot操作 - 无状态方法
     */
    private static Snapshot executeSnapshotOperation(ExecutionContext context,
                                                    org.apache.iceberg.Table table, 
                                                    String operation,
                                                    String whereCondition, 
                                                    Map<String, Object> options) 
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
    
    /**
     * 执行append snapshot - 静态方法
     */
    private static Snapshot executeAppendSnapshot(org.apache.iceberg.Table table, 
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
        
        // 这里简化处理，实际需要根据具体业务逻辑添加数据文件
        append.commit();
        return table.currentSnapshot();
    }
    
    /**
     * 执行overwrite snapshot - 静态方法
     */
    private static Snapshot executeOverwriteSnapshot(org.apache.iceberg.Table table, 
                                                    String whereCondition, 
                                                    Map<String, Object> options) {
        OverwriteFiles overwrite = table.newOverwrite();
        
        // 应用WHERE条件
        if (!Strings.isNullOrEmpty(whereCondition)) {
            // 这里需要解析WHERE条件并应用过滤器
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

### 3.2 PaimonCompactProcedure单例实现

```java
public class PaimonCompactProcedure extends AbstractSingletonProcedure {
    
    // 单例实例
    private static volatile PaimonCompactProcedure instance;
    private static final Object INSTANCE_LOCK = new Object();
    
    private PaimonCompactProcedure() {
        super("compact", buildParameters(), 
              "Compact small files for the Paimon table",
              Sets.newHashSet("paimon"));
    }
    
    public static PaimonCompactProcedure getInstance() {
        if (instance == null) {
            synchronized (INSTANCE_LOCK) {
                if (instance == null) {
                    instance = new PaimonCompactProcedure();
                }
            }
        }
        return instance;
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
    protected void executeInternal(ExecutionContext context) throws UserException {
        String partitions = context.getArg(0, String.class);
        String orderStrategy = context.getArg(1, String.class, "none");
        String orderColumns = context.getArg(2, String.class);
        String whereCondition = context.getArg(3, String.class);
        
        try {
            org.apache.paimon.table.Table paimonTable = 
                context.getTableContext().getNativeTable(org.apache.paimon.table.Table.class);
            
            // 构建压缩计划 - 无状态操作
            CompactionPlan compactionPlan = buildCompactionPlan(
                paimonTable, partitions, whereCondition);
            
            if (compactionPlan.isEmpty()) {
                context.putData("compactionResult", "No files need compaction");
                return;
            }
            
            // 执行压缩 - 无状态操作
            executeCompaction(paimonTable, compactionPlan, orderStrategy, orderColumns);
            
            // 存储结果
            context.putData("compactionResult", 
                String.format("Compacted %d files", compactionPlan.files().size()));
                
        } catch (Exception e) {
            throw new UserException(String.format(
                "Failed to compact table %s: %s", 
                context.getTableContext().getTable().getName(), e.getMessage()), e);
        }
    }
    
    // 其他静态辅助方法...
    private static CompactionPlan buildCompactionPlan(org.apache.paimon.table.Table table,
                                                      String partitions, String whereCondition) 
            throws Exception {
        // 实现细节...
        return table.store().newCompaction().plan();
    }
    
    private static void executeCompaction(org.apache.paimon.table.Table table,
                                         CompactionPlan plan, String orderStrategy, 
                                         String orderColumns) throws Exception {
        // 实现细节...
        table.store().newCompaction().compact(plan);
    }
}
```

## 4. ExternalCatalog集成

### 4.1 ExternalCatalog修改

```java
public abstract class ExternalCatalog implements CatalogIf<ExternalDatabase<? extends ExternalTable>>, 
                                               Writable, GsonPostProcessable {
    // 现有代码...
    
    // 移除实例存储，改为引用单例
    // protected Map<String, TableProcedure> registeredProcedures; // 删除这行
    
    /**
     * 获取支持的Procedure名称列表
     */
    public List<String> getSupportedProcedureNames() {
        return SingletonProcedureRegistry.getProceduresByType(getType())
            .stream()
            .map(AbstractSingletonProcedure::getProcedureName)
            .collect(Collectors.toList());
    }
    
    /**
     * 获取指定名称的Procedure
     */
    public final Optional<AbstractSingletonProcedure> getProcedure(String procedureName) {
        Optional<AbstractSingletonProcedure> procedure = 
            SingletonProcedureRegistry.getProcedure(procedureName);
        
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
        return SingletonProcedureRegistry.getProceduresByType(getType())
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
        return !getSupportedProcedureNames().isEmpty();
    }
    
    /**
     * 执行指定的Procedure
     */
    public final void executeProcedure(String procedureName, List<Expression> args, 
                                      TableIf table, ConnectContext ctx) throws UserException {
        Optional<AbstractSingletonProcedure> procedure = getProcedure(procedureName);
        if (!procedure.isPresent()) {
            throw new UserException(String.format(
                "Procedure '%s' not found in catalog '%s' of type '%s'", 
                procedureName, getName(), getType()));
        }
        
        procedure.get().execute(args, table, ctx);
    }
    
    @Override
    protected void initLocalObjectsImpl() {
        // 现有初始化逻辑...
        
        // 确保单例注册表已初始化
        SingletonProcedureRegistry.ensureInitialized();
    }
}
```

## 5. 性能优化和监控

### 5.1 ProcedureMetrics增强

```java
public class ProcedureMetrics {
    // 现有指标...
    
    // 单例相关指标
    public static final Gauge PROCEDURE_INSTANCES = 
        Gauge.build()
            .name("doris_procedure_instances_total")
            .help("Total number of procedure singleton instances")
            .register();
    
    // 并发执行指标
    public static final Gauge CONCURRENT_EXECUTIONS = 
        Gauge.build()
            .name("doris_procedure_concurrent_executions")
            .labelNames("procedure")
            .help("Number of concurrent procedure executions")
            .register();
    
    // 内存使用指标
    public static final Gauge PROCEDURE_MEMORY_USAGE = 
        Gauge.build()
            .name("doris_procedure_memory_usage_bytes")
            .help("Memory usage of procedure singleton instances")
            .register();
    
    public static void registerProcedure(String procedureName) {
        PROCEDURE_INSTANCES.inc();
        CONCURRENT_EXECUTIONS.labels(procedureName).set(0);
    }
    
    public static void recordConcurrentExecution(String procedureName, boolean start) {
        if (start) {
            CONCURRENT_EXECUTIONS.labels(procedureName).inc();
        } else {
            CONCURRENT_EXECUTIONS.labels(procedureName).dec();
        }
    }
    
    @Override
    public static void recordExecution(String catalogType, String procedure, 
                                      boolean success, double durationSeconds) {
        // 现有记录逻辑...
        
        // 记录并发执行结束
        recordConcurrentExecution(procedure, false);
    }
    
    public static Map<String, Object> getDetailedStatistics() {
        Map<String, Object> stats = new HashMap<>();
        
        // 基础统计
        stats.put("totalInstances", (int) PROCEDURE_INSTANCES.get());
        
        // 单例注册表统计
        stats.putAll(SingletonProcedureRegistry.getStatistics());
        
        // 内存使用估算
        Runtime runtime = Runtime.getRuntime();
        stats.put("totalMemory", runtime.totalMemory());
        stats.put("usedMemory", runtime.totalMemory() - runtime.freeMemory());
        
        return stats;
    }
}
```

### 5.2 ExecutionContext池化优化

```java
public class ExecutionContextPool {
    private static final int MAX_POOL_SIZE = 1000;
    private static final Queue<ExecutionContext> CONTEXT_POOL = 
        new ConcurrentLinkedQueue<>();
    private static final AtomicInteger POOL_SIZE = new AtomicInteger(0);
    
    /**
     * 获取ExecutionContext（如果可能的话从池中获取）
     */
    public static ExecutionContext acquire(long executionId, String procedureName,
                                          List<Object> processedArgs, TableContext tableContext,
                                          ConnectContext connectContext, long startTime) {
        
        ExecutionContext context = CONTEXT_POOL.poll();
        if (context != null) {
            POOL_SIZE.decrementAndGet();
            // 重置context状态
            context.reset(executionId, procedureName, processedArgs, 
                         tableContext, connectContext, startTime);
            return context;
        }
        
        // 池中没有可用对象，创建新的
        return new ExecutionContext(executionId, procedureName, processedArgs,
                                   tableContext, connectContext, startTime);
    }
    
    /**
     * 释放ExecutionContext回池中
     */
    public static void release(ExecutionContext context) {
        if (POOL_SIZE.get() < MAX_POOL_SIZE) {
            context.clear(); // 清理敏感数据
            CONTEXT_POOL.offer(context);
            POOL_SIZE.incrementAndGet();
        }
        // 如果池已满，让GC回收
    }
}
```

## 6. 单元测试和性能测试

### 6.1 单例模式测试

```java
@Test
public class SingletonProcedureTest {
    
    @Test
    public void testSingletonInstance() {
        // 验证单例模式
        IcebergSnapshotProcedure instance1 = IcebergSnapshotProcedure.getInstance();
        IcebergSnapshotProcedure instance2 = IcebergSnapshotProcedure.getInstance();
        
        assertThat(instance1).isSameAs(instance2);
    }
    
    @Test
    public void testConcurrentAccess() throws Exception {
        int threadCount = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        Set<IcebergSnapshotProcedure> instances = ConcurrentHashMap.newKeySet();
        
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
    
    @Test
    public void testConcurrentExecution() throws Exception {
        IcebergSnapshotProcedure procedure = IcebergSnapshotProcedure.getInstance();
        int executionCount = 50;
        
        ExecutorService executor = Executors.newFixedThreadPool(10);
        List<Future<Void>> futures = new ArrayList<>();
        
        for (int i = 0; i < executionCount; i++) {
            int finalI = i;
            futures.add(executor.submit(() -> {
                try {
                    // 模拟并发执行
                    List<Expression> args = Arrays.asList(
                        new StringLiteral("append"),
                        new StringLiteral(null),
                        new StringLiteral("{\"execution\":\"" + finalI + "\"}")
                    );
                    procedure.execute(args, mockTable, mockContext);
                    return null;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        
        for (Future<Void> future : futures) {
            future.get();
        }
        
        // 验证执行计数
        assertThat(procedure.getExecutionCount()).isEqualTo(executionCount);
    }
    
    @Test
    public void testMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();
        long beforeMemory = runtime.totalMemory() - runtime.freeMemory();
        
        // 获取多个procedure实例
        for (int i = 0; i < 1000; i++) {
            IcebergSnapshotProcedure.getInstance();
            PaimonCompactProcedure.getInstance();
        }
        
        long afterMemory = runtime.totalMemory() - runtime.freeMemory();
        long memoryIncrease = afterMemory - beforeMemory;
        
        // 内存增长应该很小（只有第一次创建实例时）
        assertThat(memoryIncrease).isLessThan(1024 * 1024); // < 1MB
    }
}
```

### 6.2 性能基准测试

```java
@Benchmark
public class ProcedureBenchmark {
    
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void benchmarkSingletonAccess(Blackhole bh) {
        IcebergSnapshotProcedure procedure = IcebergSnapshotProcedure.getInstance();
        bh.consume(procedure);
    }
    
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void benchmarkExecutionContextCreation(Blackhole bh) {
        ExecutionContext context = new ExecutionContext(
            1L, "snapshot", Arrays.asList("append", null, "{}"),
            mockTableContext, mockConnectContext, System.currentTimeMillis()
        );
        bh.consume(context);
    }
    
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(10)
    public void benchmarkConcurrentExecution(Blackhole bh) throws Exception {
        IcebergSnapshotProcedure procedure = IcebergSnapshotProcedure.getInstance();
        
        List<Expression> args = Arrays.asList(
            new StringLiteral("append"),
            new StringLiteral(null),
            new StringLiteral("{}")
        );
        
        procedure.execute(args, mockTable, mockContext);
        bh.consume(procedure.getExecutionCount());
    }
}
```

## 7. 部署配置

### 7.1 配置参数

```java
public class Config {
    @ConfField
    public static boolean enable_procedure_singleton_mode = true;
    
    @ConfField
    public static int procedure_execution_context_pool_size = 1000;
    
    @ConfField
    public static boolean enable_procedure_metrics_detailed = true;
    
    @ConfField
    public static int procedure_concurrent_execution_limit = 100;
    
    @ConfField
    public static boolean enable_procedure_execution_context_pooling = true;
}
```

## 8. 使用示例

### 8.1 单例模式下的使用

```sql
-- 使用方式与之前完全相同
ALTER TABLE iceberg_catalog.sales_db.orders 
EXECUTE snapshot('append');

-- 查看procedures (结果相同)
SHOW PROCEDURES FROM iceberg_catalog;

-- 性能监控查询
SELECT procedure_name, execution_count, success_rate 
FROM system.procedure_statistics 
ORDER BY execution_count DESC;
```

### 8.2 管理和监控

```sql
-- 查看单例统计信息
SHOW PROCEDURE STATISTICS;

-- 示例输出:
-- +------------------+------------------+---------------+--------------+
-- | Procedure        | Total_Executions | Success_Count | Success_Rate |
-- +------------------+------------------+---------------+--------------+
-- | snapshot         | 1520             | 1515          | 99.67%       |
-- | compact          | 890              | 885           | 99.44%       |
-- | expire_snapshots | 245              | 243           | 99.18%       |
-- +------------------+------------------+---------------+--------------+

-- 查看内存使用情况
SHOW PROCEDURE MEMORY USAGE;
```

## 9. 总结

### 9.1 单例模式优势
1. **内存优化显著**：每种Procedure只有一个实例，大幅减少内存占用
2. **性能提升明显**：避免重复创建对象的开销，提高执行效率
3. **线程安全保证**：通过无状态设计和ExecutionContext确保并发安全
4. **管理简化**：集中的单例注册表便于统一管理和监控
5. **扩展性增强**：新增Procedure类型更加简单

### 9.2 架构改进
1. **去实例化存储**：Catalog不再存储Procedure实例，只引用单例
2. **上下文分离**：执行状态通过ExecutionContext传递，保持无状态
3. **池化优化**：ExecutionContext可以池化重用，进一步优化性能
4. **监控增强**：提供详细的单例使用统计和性能指标

### 9.3 实施建议
1. **渐进迁移**：可以先实现单例基础设施，然后逐步迁移现有Procedures
2. **性能测试**：重点关注并发执行的性能和内存使用情况
3. **监控完善**：建立完整的单例使用监控和告警机制
4. **文档更新**：更新开发文档，说明新的单例模式开发规范

通过单例模式的引入，整个Procedure系统在保持功能完整性的同时，获得了显著的性能提升和资源优化，为大规模部署提供了更好的基础。