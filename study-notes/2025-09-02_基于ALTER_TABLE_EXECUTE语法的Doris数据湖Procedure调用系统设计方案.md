# 基于ALTER TABLE EXECUTE语法的Doris数据湖Procedure调用系统设计方案

## 1. 方案概述

### 1.1 设计目标
本方案基于`ALTER TABLE ${table_name} EXECUTE ${procedure_name}(args...)`语法，为Apache Doris提供与表紧密关联的数据湖Procedure调用能力。该方案更符合数据库标准SQL扩展语法，提供更直观的表级别操作体验。

### 1.2 方案优势
1. **语法直观**：直接在表上执行procedure，语义更清晰
2. **权限精确**：基于表级权限控制，粒度更细
3. **上下文丰富**：自动获取表信息，减少参数传递
4. **标准兼容**：符合SQL标准的ALTER TABLE扩展语法
5. **安全性高**：procedure与特定表绑定，避免误操作

### 1.3 适用场景
- 表级别的数据管理操作（压缩、清理、快照等）
- Schema演进相关操作（分区管理、列变更等）
- 表维护任务（统计信息更新、索引重建等）
- 数据治理操作（数据质量检查、合规性扫描等）

## 2. 语法设计

### 2.1 基础语法结构

```sql
ALTER TABLE [catalog.]database.table_name 
EXECUTE procedure_name([parameter_list])
```

### 2.2 语法示例

```sql
-- Iceberg表快照操作
ALTER TABLE iceberg_catalog.sales_db.orders 
EXECUTE snapshot('append');

-- Iceberg表回滚操作
ALTER TABLE iceberg_catalog.sales_db.orders 
EXECUTE rollback_to_snapshot(123456789);

-- Paimon表压缩操作
ALTER TABLE paimon_catalog.warehouse_db.inventory 
EXECUTE compact('dt=2024-01-01', 'zorder');

-- 表分区管理
ALTER TABLE iceberg_catalog.logs_db.access_logs 
EXECUTE expire_snapshots(timestamp('2024-01-01 00:00:00'));

-- 孤儿文件清理
ALTER TABLE iceberg_catalog.data_db.user_events 
EXECUTE remove_orphan_files();

-- 分支管理
ALTER TABLE iceberg_catalog.feature_db.experiments 
EXECUTE create_branch('feature_v2', 987654321);

-- 查看支持的Procedures
SHOW PROCEDURES;                           -- 显示当前catalog的procedures
SHOW PROCEDURES FROM iceberg_catalog;      -- 显示指定catalog的procedures
SHOW PROCEDURES LIKE 'snapshot%';          -- 模式匹配过滤
```

### 2.3 语法规则

#### ALTER TABLE EXECUTE语法：
1. **表名解析**：支持完整的三级命名空间（catalog.database.table）
2. **Procedure名称**：不区分大小写，支持下划线和驼峰命名
3. **参数类型**：支持字面量常量、表达式、子查询
4. **权限要求**：需要目标表的ALTER权限
5. **事务性**：支持事务回滚和提交

#### SHOW PROCEDURES语法：
1. **基础语法**：`SHOW PROCEDURES [FROM catalog_name] [LIKE pattern]`
2. **可选子句**：支持FROM指定catalog，LIKE模式匹配
3. **权限要求**：需要对应catalog的SHOW权限
4. **结果排序**：按procedure名称字母序排列

## 3. 系统架构设计

### 3.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                        SQL解析层                                │
├─────────────────────────────────────────────────────────────────┤
│ ALTER TABLE EXECUTE语法解析  │  SHOW PROCEDURES语法解析         │
├─────────────────────────────────────────────────────────────────┤
│    AlterTableExecuteStmt    │      ShowProceduresStmt           │
├─────────────────────────────────────────────────────────────────┤
│             语句分析器和权限检查                                 │
├─────────────────────────────────────────────────────────────────┤
│  表权限检查  │  Procedure发现  │  参数验证  │  上下文构建        │
├─────────────────────────────────────────────────────────────────┤
│       ExecuteHandler执行器    │    ShowHandler结果生成器        │
├─────────────────────────────────────────────────────────────────┤
│                    Catalog级别的Procedure管理                   │
├─────────────────────────────────────────────────────────────────┤
│       TableProcedure抽象基类（包含表上下文）                   │
├─────────────────────────────────────────────────────────────────┤
│  IcebergTableProcedure  │  PaimonTableProcedure │  Extensions   │
├─────────────────────────────────────────────────────────────────┤
│  IcebergExternalCatalog │  PaimonExternalCatalog │  其他Catalog  │
├─────────────────────────────────────────────────────────────────┤
│              ExternalCatalog抽象基类                            │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 核心组件设计

#### 3.2.1 ExternalCatalog扩展 - Procedure管理

首先，需要扩展ExternalCatalog抽象基类，添加Procedure管理功能：

```java
public abstract class ExternalCatalog implements CatalogIf<ExternalDatabase<? extends ExternalTable>>, 
                                               Writable, GsonPostProcessable {
    // 现有代码...
    
    // Procedure相关成员变量
    protected Map<String, TableProcedure> registeredProcedures;
    protected boolean proceduresInitialized = false;
    
    public ExternalCatalog(long catalogId, String name, InitCatalogLog.Type logType, String comment) {
        // 现有构造函数代码...
        this.registeredProcedures = Maps.newConcurrentMap();
    }
    
    /**
     * 初始化Catalog支持的Procedures
     * 子类需要重写此方法来注册具体的Procedures
     */
    protected void initializeProcedures() {
        // 默认实现：不注册任何procedure
        proceduresInitialized = true;
    }
    
    /**
     * 确保Procedures已经初始化
     */
    protected final void ensureProceduresInitialized() {
        if (!proceduresInitialized) {
            synchronized (this) {
                if (!proceduresInitialized) {
                    initializeProcedures();
                }
            }
        }
    }
    
    /**
     * 注册一个TableProcedure
     */
    protected final void registerProcedure(TableProcedure procedure) {
        if (!procedure.getSupportedTableTypes().contains(getType())) {
            LOG.warn("Procedure {} does not support catalog type {}, skipping registration",
                    procedure.getProcedureName(), getType());
            return;
        }
        
        registeredProcedures.put(procedure.getProcedureName().toLowerCase(), procedure);
        LOG.debug("Registered procedure: {} for catalog: {}", 
                procedure.getProcedureName(), getName());
    }
    
    /**
     * 获取指定名称的Procedure
     */
    public final Optional<TableProcedure> getProcedure(String procedureName) {
        ensureProceduresInitialized();
        return Optional.ofNullable(registeredProcedures.get(procedureName.toLowerCase()));
    }
    
    /**
     * 列出所有支持的Procedures
     */
    public final List<ProcedureInfo> listProcedures() {
        ensureProceduresInitialized();
        return registeredProcedures.values().stream()
            .map(proc -> new ProcedureInfo(
                proc.getProcedureName(),
                proc.getParameters(),
                proc.getDescription(),
                getType()
            ))
            .sorted((a, b) -> a.getName().compareTo(b.getName()))
            .collect(Collectors.toList());
    }
    
    /**
     * 检查是否支持Procedures
     */
    public boolean supportsProcedures() {
        ensureProceduresInitialized();
        return !registeredProcedures.isEmpty();
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
    
    @Override
    protected void initLocalObjectsImpl() {
        // 现有初始化逻辑...
        
        // 初始化Procedures
        ensureProceduresInitialized();
    }
}
```

#### 3.2.2 ProcedureInfo信息类

```java
public class ProcedureInfo {
    private final String name;
    private final List<ProcedureParameter> parameters;
    private final String description;
    private final String catalogType;
    
    public ProcedureInfo(String name, List<ProcedureParameter> parameters, 
                        String description, String catalogType) {
        this.name = name;
        this.parameters = parameters;
        this.description = description;
        this.catalogType = catalogType;
    }
    
    // Getters
    public String getName() { return name; }
    public List<ProcedureParameter> getParameters() { return parameters; }
    public String getDescription() { return description; }
    public String getCatalogType() { return catalogType; }
    
    public String getParameterSignature() {
        return parameters.stream()
            .map(param -> {
                String paramStr = param.getName() + " " + param.getType().toString();
                if (!param.isRequired() && param.getDefaultValue() != null) {
                    paramStr += " DEFAULT " + param.getDefaultValue();
                }
                return paramStr;
            })
            .collect(Collectors.joining(", "));
    }
}
```

#### 3.2.3 ShowProceduresStmt语句对象

```java
public class ShowProceduresStmt extends ShowStmt {
    private String catalogName;
    private String procedurePattern;
    
    public ShowProceduresStmt(String catalogName, String procedurePattern) {
        this.catalogName = catalogName;
        this.procedurePattern = procedurePattern;
    }
    
    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        super.analyze(analyzer);
        
        // 如果没有指定catalog，使用当前catalog
        if (Strings.isNullOrEmpty(catalogName)) {
            catalogName = analyzer.getDefaultCatalog();
        }
        
        // 验证catalog是否存在
        CatalogMgr catalogMgr = Env.getCurrentEnv().getCatalogMgr();
        CatalogIf catalog = catalogMgr.getCatalog(catalogName);
        if (catalog == null) {
            throw new AnalysisException("Catalog not found: " + catalogName);
        }
        
        // 检查权限
        ConnectContext ctx = analyzer.getContext();
        UserIdentity user = ctx.getCurrentUserIdentity();
        if (!Env.getCurrentEnv().getAccessManager().checkCtlPriv(
                user, catalogName, PrivPredicate.SHOW)) {
            throw new AnalysisException(String.format(
                "Access denied. User %s needs SHOW privilege on catalog %s", 
                user, catalogName));
        }
    }
    
    @Override
    public ShowResultSet toSelectStmt(Analyzer analyzer) throws AnalysisException {
        return ShowProceduresAnalyzer.analyze(this, analyzer.getContext());
    }
    
    // Getters
    public String getCatalogName() { return catalogName; }
    public String getProcedurePattern() { return procedurePattern; }
}
```

#### 3.2.4 ShowProceduresAnalyzer分析器

```java
public class ShowProceduresAnalyzer {
    private static final Logger LOG = LogManager.getLogger(ShowProceduresAnalyzer.class);
    
    public static ShowResultSet analyze(ShowProceduresStmt stmt, ConnectContext ctx) 
            throws AnalysisException {
        
        String catalogName = stmt.getCatalogName();
        String procedurePattern = stmt.getProcedurePattern();
        
        // 获取Catalog
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
        if (catalog == null) {
            throw new AnalysisException("Catalog not found: " + catalogName);
        }
        
        // 获取Procedures列表
        List<ProcedureInfo> procedures;
        if (catalog instanceof ExternalCatalog) {
            ExternalCatalog externalCatalog = (ExternalCatalog) catalog;
            procedures = externalCatalog.listProcedures();
        } else {
            // 内部Catalog暂不支持Procedures
            procedures = Lists.newArrayList();
        }
        
        // 应用过滤条件
        if (!Strings.isNullOrEmpty(procedurePattern)) {
            procedures = procedures.stream()
                .filter(proc -> matchesPattern(proc.getName(), procedurePattern))
                .collect(Collectors.toList());
        }
        
        // 构建结果集
        return buildResultSet(procedures, catalogName);
    }
    
    private static boolean matchesPattern(String name, String pattern) {
        // 支持SQL LIKE语法的模式匹配
        String regex = pattern.replace("%", ".*").replace("_", ".");
        return name.matches("(?i)" + regex);  // 不区分大小写
    }
    
    private static ShowResultSet buildResultSet(List<ProcedureInfo> procedures, String catalogName) {
        // 定义列信息
        List<Column> columns = Arrays.asList(
            new Column("Catalog", Type.VARCHAR),
            new Column("Procedure", Type.VARCHAR), 
            new Column("Parameters", Type.VARCHAR),
            new Column("Description", Type.VARCHAR),
            new Column("Type", Type.VARCHAR)
        );
        
        // 构建数据行
        List<List<String>> rows = Lists.newArrayList();
        for (ProcedureInfo proc : procedures) {
            List<String> row = Lists.newArrayList();
            row.add(catalogName);
            row.add(proc.getName());
            row.add(proc.getParameterSignature());
            row.add(Strings.nullToEmpty(proc.getDescription()));
            row.add(proc.getCatalogType().toUpperCase());
            rows.add(row);
        }
        
        return new ShowResultSet(new ShowResultSetMetaData(columns), rows);
    }
}
```

#### 3.2.5 AlterTableExecuteStmt语句对象

```java
public class AlterTableExecuteStmt extends AlterTableStmt {
    private final String procedureName;
    private final List<Expression> procedureArgs;
    private TableName tableName;
    private TableIf targetTable;
    private TableProcedure resolvedProcedure;
    
    public AlterTableExecuteStmt(TableName tableName, String procedureName, 
                                List<Expression> procedureArgs) {
        super(tableName, AlterTableType.EXECUTE_PROCEDURE);
        this.tableName = tableName;
        this.procedureName = procedureName;
        this.procedureArgs = procedureArgs != null ? procedureArgs : Lists.newArrayList();
    }
    
    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        super.analyze(analyzer);
        
        // 1. 解析和验证表名
        analyzeTableName(analyzer);
        
        // 2. 获取目标表对象
        resolveTargetTable(analyzer);
        
        // 3. 检查表级别权限
        checkTablePrivileges(analyzer);
        
        // 4. 发现和解析Procedure
        resolveProcedure(analyzer);
        
        // 5. 验证Procedure参数
        validateProcedureArguments(analyzer);
    }
    
    private void analyzeTableName(Analyzer analyzer) throws AnalysisException {
        tableName.analyze(analyzer);
        if (tableName.getDb() == null) {
            tableName.setDb(analyzer.getDefaultDb());
        }
        if (tableName.getCtl() == null) {
            tableName.setCtl(analyzer.getDefaultCatalog());
        }
    }
    
    private void resolveTargetTable(Analyzer analyzer) throws AnalysisException {
        CatalogIf catalog = analyzer.getCatalog(tableName.getCtl());
        if (catalog == null) {
            throw new AnalysisException("Catalog not found: " + tableName.getCtl());
        }
        
        DatabaseIf database = catalog.getDbNullable(tableName.getDb());
        if (database == null) {
            throw new AnalysisException(String.format(
                "Database not found: %s.%s", tableName.getCtl(), tableName.getDb()));
        }
        
        targetTable = database.getTableNullable(tableName.getTbl());
        if (targetTable == null) {
            throw new AnalysisException(String.format(
                "Table not found: %s.%s.%s", 
                tableName.getCtl(), tableName.getDb(), tableName.getTbl()));
        }
    }
    
    private void checkTablePrivileges(Analyzer analyzer) throws AnalysisException {
        ConnectContext ctx = analyzer.getContext();
        UserIdentity user = ctx.getCurrentUserIdentity();
        
        // 检查表的ALTER权限
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(
                user, tableName.getCtl(), tableName.getDb(), tableName.getTbl(), 
                PrivPredicate.ALTER)) {
            throw new AnalysisException(String.format(
                "Access denied. User %s needs ALTER privilege on table %s", 
                user, tableName));
        }
    }
    
    private void resolveProcedure(Analyzer analyzer) throws AnalysisException {
        if (!(targetTable instanceof ExternalTable)) {
            throw new AnalysisException(
                "EXECUTE procedure is only supported for external tables");
        }
        
        ExternalTable externalTable = (ExternalTable) targetTable;
        CatalogIf catalog = externalTable.getCatalog();
        
        // 检查catalog是否支持procedures
        if (!(catalog instanceof ExternalCatalog)) {
            throw new AnalysisException(String.format(
                "Catalog '%s' does not support procedures", catalog.getName()));
        }
        
        ExternalCatalog externalCatalog = (ExternalCatalog) catalog;
        if (!externalCatalog.supportsProcedures()) {
            throw new AnalysisException(String.format(
                "Catalog '%s' of type '%s' does not have any registered procedures", 
                catalog.getName(), catalog.getType()));
        }
        
        // 获取表相关的Procedure
        Optional<TableProcedure> procedure = externalCatalog.getProcedure(procedureName);
        
        if (!procedure.isPresent()) {
            throw new AnalysisException(String.format(
                "Procedure '%s' not found in catalog '%s' of type '%s'", 
                procedureName, catalog.getName(), catalog.getType()));
        }
        
        resolvedProcedure = procedure.get();
    }
    
    private void validateProcedureArguments(Analyzer analyzer) throws AnalysisException {
        try {
            resolvedProcedure.validateArguments(procedureArgs, targetTable);
        } catch (Exception e) {
            throw new AnalysisException("Procedure argument validation failed: " + e.getMessage(), e);
        }
    }
    
    // Getters
    public String getProcedureName() { return procedureName; }
    public List<Expression> getProcedureArgs() { return procedureArgs; }
    public TableIf getTargetTable() { return targetTable; }
    public TableProcedure getResolvedProcedure() { return resolvedProcedure; }
}
```

#### 3.2.2 TableProcedure抽象基类

```java
public abstract class TableProcedure {
    protected final String procedureName;
    protected final List<ProcedureParameter> parameters;
    protected final String description;
    protected final Set<String> supportedTableTypes;
    
    public TableProcedure(String procedureName, List<ProcedureParameter> parameters,
                         String description, Set<String> supportedTableTypes) {
        this.procedureName = procedureName;
        this.parameters = parameters;
        this.description = description;
        this.supportedTableTypes = supportedTableTypes;
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
        
        // 5. 执行前置检查
        preExecutionCheck(tableContext, processedArgs);
        
        // 6. 执行核心逻辑
        executeInternal(tableContext, processedArgs);
        
        // 7. 执行后置处理
        postExecutionHandler(tableContext, processedArgs);
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
        
        // 验证每个参数
        for (int i = 0; i < args.size(); i++) {
            ProcedureParameter param = parameters.get(i);
            Expression arg = args.get(i);
            param.validate(arg);
        }
        
        // 检查必需参数
        for (int i = args.size(); i < parameters.size(); i++) {
            ProcedureParameter param = parameters.get(i);
            if (param.isRequired() && param.getDefaultValue() == null) {
                throw new UserException(String.format(
                    "Required parameter %s is missing", param.getName()));
            }
        }
    }
    
    protected List<Object> processArguments(List<Expression> args, TableIf table) 
            throws UserException {
        List<Object> result = new ArrayList<>();
        
        for (int i = 0; i < parameters.size(); i++) {
            ProcedureParameter param = parameters.get(i);
            
            if (i < args.size()) {
                // 用户提供的参数
                Expression arg = args.get(i);
                result.add(param.convertValue(arg));
            } else {
                // 使用默认值或从表上下文推导
                Object defaultValue = resolveDefaultValue(param, table);
                result.add(defaultValue);
            }
        }
        
        return result;
    }
    
    protected Object resolveDefaultValue(ProcedureParameter param, TableIf table) 
            throws UserException {
        // 1. 使用参数定义的默认值
        if (param.getDefaultValue() != null) {
            return param.getDefaultValue();
        }
        
        // 2. 从表上下文推导默认值
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
    
    // Hook methods for subclasses
    protected void preExecutionCheck(TableContext context, List<Object> args) 
            throws UserException {
        // Default: no pre-execution checks
    }
    
    protected void postExecutionHandler(TableContext context, List<Object> args) 
            throws UserException {
        // Default: refresh table metadata
        context.getTable().resetToUninitialized();
    }
    
    // Abstract method for subclasses to implement
    protected abstract void executeInternal(TableContext context, List<Object> args) 
            throws UserException;
    
    // Getters
    public String getProcedureName() { return procedureName; }
    public List<ProcedureParameter> getParameters() { return parameters; }
    public String getDescription() { return description; }
    public Set<String> getSupportedTableTypes() { return supportedTableTypes; }
}
```

#### 3.2.3 TableContext表上下文

```java
public class TableContext {
    private final ExternalTable table;
    private final CatalogIf catalog;
    private final UserIdentity user;
    private final ConnectContext connectContext;
    
    // 缓存的表信息
    private Schema cachedSchema;
    private List<String> cachedPartitions;
    private Map<String, String> cachedProperties;
    
    public TableContext(ExternalTable table, CatalogIf catalog, 
                       UserIdentity user, ConnectContext connectContext) {
        this.table = table;
        this.catalog = catalog;
        this.user = user;
        this.connectContext = connectContext;
    }
    
    // 懒加载表Schema
    public Schema getTableSchema() {
        if (cachedSchema == null) {
            cachedSchema = table.getFullSchema();
        }
        return cachedSchema;
    }
    
    // 获取表分区信息
    public List<String> getPartitions() {
        if (cachedPartitions == null) {
            try {
                cachedPartitions = loadTablePartitions();
            } catch (Exception e) {
                LOG.warn("Failed to load partitions for table: {}", table.getName(), e);
                cachedPartitions = Lists.newArrayList();
            }
        }
        return cachedPartitions;
    }
    
    private List<String> loadTablePartitions() {
        // 根据不同的表类型加载分区信息
        if (isIcebergTable()) {
            return loadIcebergPartitions();
        } else if (isPaimonTable()) {
            return loadPaimonPartitions();
        }
        return Lists.newArrayList();
    }
    
    private List<String> loadIcebergPartitions() {
        try {
            IcebergExternalCatalog icebergCatalog = (IcebergExternalCatalog) catalog;
            org.apache.iceberg.Table icebergTable = icebergCatalog.getCatalog()
                .loadTable(TableIdentifier.of(table.getDbName(), table.getName()));
            
            // 获取分区信息
            return icebergTable.spec().fields().stream()
                .map(field -> field.name())
                .collect(Collectors.toList());
        } catch (Exception e) {
            LOG.warn("Failed to load Iceberg partitions", e);
            return Lists.newArrayList();
        }
    }
    
    private List<String> loadPaimonPartitions() {
        try {
            PaimonExternalCatalog paimonCatalog = (PaimonExternalCatalog) catalog;
            NameMapping nameMapping = new NameMapping(null, 
                table.getDbName(), table.getName(), table.getDbName(), table.getName());
            
            List<Partition> partitions = paimonCatalog.getPaimonPartitions(nameMapping);
            return partitions.stream()
                .map(partition -> partition.toString())
                .collect(Collectors.toList());
        } catch (Exception e) {
            LOG.warn("Failed to load Paimon partitions", e);
            return Lists.newArrayList();
        }
    }
    
    // 获取表属性
    public Map<String, String> getTableProperties() {
        if (cachedProperties == null) {
            cachedProperties = table.getProperties();
        }
        return cachedProperties;
    }
    
    // 类型判断方法
    public boolean isIcebergTable() {
        return catalog instanceof IcebergExternalCatalog;
    }
    
    public boolean isPaimonTable() {
        return catalog instanceof PaimonExternalCatalog;
    }
    
    // 获取原生表对象
    @SuppressWarnings("unchecked")
    public <T> T getNativeTable(Class<T> expectedType) throws UserException {
        if (isIcebergTable()) {
            IcebergExternalCatalog icebergCatalog = (IcebergExternalCatalog) catalog;
            try {
                org.apache.iceberg.Table icebergTable = icebergCatalog.getCatalog()
                    .loadTable(TableIdentifier.of(table.getDbName(), table.getName()));
                return expectedType.cast(icebergTable);
            } catch (Exception e) {
                throw new UserException("Failed to load Iceberg table: " + e.getMessage(), e);
            }
        } else if (isPaimonTable()) {
            PaimonExternalCatalog paimonCatalog = (PaimonExternalCatalog) catalog;
            try {
                NameMapping nameMapping = new NameMapping(null, 
                    table.getDbName(), table.getName(), table.getDbName(), table.getName());
                org.apache.paimon.table.Table paimonTable = paimonCatalog.getPaimonTable(nameMapping);
                return expectedType.cast(paimonTable);
            } catch (Exception e) {
                throw new UserException("Failed to load Paimon table: " + e.getMessage(), e);
            }
        }
        
        throw new UserException("Unsupported table type for native table access");
    }
    
    // Getters
    public ExternalTable getTable() { return table; }
    public CatalogIf getCatalog() { return catalog; }
    public UserIdentity getUser() { return user; }
    public ConnectContext getConnectContext() { return connectContext; }
}
```

## 4. Iceberg表Procedure实现

### 4.1 IcebergSnapshotTableProcedure

```java
public class IcebergSnapshotTableProcedure extends TableProcedure {
    private static final String PROCEDURE_NAME = "snapshot";
    
    public IcebergSnapshotTableProcedure() {
        super(PROCEDURE_NAME, buildParameters(), 
              "Create a snapshot of the Iceberg table",
              Sets.newHashSet("iceberg"));
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
    protected void executeInternal(TableContext context, List<Object> args) throws UserException {
        String operation = (String) args.get(0);
        String whereCondition = (String) args.get(1);
        String options = (String) args.get(2);
        
        try {
            // 获取Iceberg原生表对象
            org.apache.iceberg.Table icebergTable = 
                context.getNativeTable(org.apache.iceberg.Table.class);
            
            // 解析选项
            Map<String, Object> optionsMap = parseOptions(options);
            
            // 执行不同类型的snapshot操作
            Snapshot snapshot = executeSnapshotOperation(
                icebergTable, operation, whereCondition, optionsMap);
            
            // 记录操作结果
            LOG.info("Snapshot operation completed. Table: {}.{}.{}, Operation: {}, " +
                    "Snapshot ID: {}, Timestamp: {}", 
                    context.getCatalog().getName(),
                    context.getTable().getDbName(),
                    context.getTable().getName(),
                    operation,
                    snapshot.snapshotId(),
                    Instant.ofEpochMilli(snapshot.timestampMillis()));
                    
        } catch (Exception e) {
            throw new UserException(String.format(
                "Failed to create snapshot for table %s: %s", 
                context.getTable().getName(), e.getMessage()), e);
        }
    }
    
    private Map<String, Object> parseOptions(String options) throws UserException {
        try {
            if (Strings.isNullOrEmpty(options) || "{}".equals(options.trim())) {
                return Maps.newHashMap();
            }
            
            // 简单的JSON解析（实际项目中应使用专门的JSON库）
            Map<String, Object> result = Maps.newHashMap();
            String cleaned = options.trim().replaceAll("[{}]", "");
            
            if (!cleaned.isEmpty()) {
                String[] pairs = cleaned.split(",");
                for (String pair : pairs) {
                    String[] kv = pair.split(":");
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
                return executeMergeSnapshot(table, whereCondition, options);
                
            default:
                throw new UserException("Unsupported snapshot operation: " + operation);
        }
    }
    
    private Snapshot executeAppendSnapshot(org.apache.iceberg.Table table, 
                                          Map<String, Object> options) {
        AppendFiles append = table.newAppend();
        
        // 应用选项
        applyAppendOptions(append, options);
        
        // 这里简化处理，实际需要根据具体业务逻辑添加数据文件
        // append.appendFile(dataFile);
        
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
        applyOverwriteOptions(overwrite, options);
        
        overwrite.commit();
        return table.currentSnapshot();
    }
    
    private Snapshot executeMergeSnapshot(org.apache.iceberg.Table table, 
                                        String whereCondition, Map<String, Object> options) 
            throws UserException {
        // Iceberg的merge操作比较复杂，这里简化处理
        throw new UserException("Merge operation not yet implemented");
    }
    
    private void applyAppendOptions(AppendFiles append, Map<String, Object> options) {
        // 应用append特定的选项
        if (options.containsKey("snapshot-property")) {
            String value = (String) options.get("snapshot-property");
            String[] parts = value.split("=", 2);
            if (parts.length == 2) {
                append.set(parts[0], parts[1]);
            }
        }
    }
    
    private void applyOverwriteOptions(OverwriteFiles overwrite, Map<String, Object> options) {
        // 应用overwrite特定的选项
        if (options.containsKey("validate-from-snapshot-id")) {
            Long snapshotId = Long.parseLong((String) options.get("validate-from-snapshot-id"));
            overwrite.validateFromSnapshot(snapshotId);
        }
    }
}
```

### 4.2 IcebergExpireSnapshotsProcedure

```java
public class IcebergExpireSnapshotsProcedure extends TableProcedure {
    private static final String PROCEDURE_NAME = "expire_snapshots";
    
    public IcebergExpireSnapshotsProcedure() {
        super(PROCEDURE_NAME, buildParameters(), 
              "Expire old snapshots of the Iceberg table",
              Sets.newHashSet("iceberg"));
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
    protected void preExecutionCheck(TableContext context, List<Object> args) throws UserException {
        // 检查是否有足够的权限进行snapshot清理
        UserIdentity user = context.getUser();
        String catalogName = context.getCatalog().getName();
        
        if (!Env.getCurrentEnv().getAccessManager().checkCtlPriv(
                user, catalogName, PrivPredicate.ALTER)) {
            throw new UserException(String.format(
                "User %s needs ALTER privilege on catalog %s to expire snapshots", 
                user, catalogName));
        }
    }
    
    @Override
    protected void executeInternal(TableContext context, List<Object> args) throws UserException {
        Long olderThanMillis = (Long) args.get(0);
        Integer retainLast = (Integer) args.get(1);
        Long maxSnapshotAgeMs = (Long) args.get(2);
        
        try {
            org.apache.iceberg.Table icebergTable = 
                context.getNativeTable(org.apache.iceberg.Table.class);
            
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
            
            // 执行过期操作
            expireSnapshots.commit();
            
            LOG.info("Snapshots expired successfully for table: {}.{}.{}", 
                    context.getCatalog().getName(),
                    context.getTable().getDbName(),
                    context.getTable().getName());
                    
        } catch (Exception e) {
            throw new UserException(String.format(
                "Failed to expire snapshots for table %s: %s", 
                context.getTable().getName(), e.getMessage()), e);
        }
    }
}
```

## 5. Paimon表Procedure实现

### 5.1 PaimonCompactTableProcedure

```java
public class PaimonCompactTableProcedure extends TableProcedure {
    private static final String PROCEDURE_NAME = "compact";
    
    public PaimonCompactTableProcedure() {
        super(PROCEDURE_NAME, buildParameters(), 
              "Compact small files for the Paimon table",
              Sets.newHashSet("paimon"));
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
    protected void executeInternal(TableContext context, List<Object> args) throws UserException {
        String partitions = (String) args.get(0);
        String orderStrategy = (String) args.get(1);
        String orderColumns = (String) args.get(2);
        String whereCondition = (String) args.get(3);
        
        try {
            org.apache.paimon.table.Table paimonTable = 
                context.getNativeTable(org.apache.paimon.table.Table.class);
            
            // 构建压缩计划
            CompactionPlan compactionPlan = buildCompactionPlan(
                paimonTable, partitions, whereCondition);
            
            if (compactionPlan.isEmpty()) {
                LOG.info("No files need compaction for table: {}", 
                        context.getTable().getName());
                return;
            }
            
            // 执行压缩
            executeCompaction(paimonTable, compactionPlan, orderStrategy, orderColumns);
            
            LOG.info("Table compaction completed. Table: {}.{}.{}, " +
                    "Compacted files: {}", 
                    context.getCatalog().getName(),
                    context.getTable().getDbName(),
                    context.getTable().getName(),
                    compactionPlan.files().size());
                    
        } catch (Exception e) {
            throw new UserException(String.format(
                "Failed to compact table %s: %s", 
                context.getTable().getName(), e.getMessage()), e);
        }
    }
    
    private CompactionPlan buildCompactionPlan(org.apache.paimon.table.Table table,
                                              String partitions, String whereCondition) 
            throws Exception {
        
        CompactionPlanner planner = table.store().newCompaction();
        
        // 应用分区过滤
        if (!Strings.isNullOrEmpty(partitions)) {
            // 解析分区条件并应用
            Map<String, String> partitionFilter = parsePartitionFilter(partitions);
            if (!partitionFilter.isEmpty()) {
                // planner.withPartitionFilter(partitionFilter);
            }
        }
        
        // 应用WHERE条件过滤
        if (!Strings.isNullOrEmpty(whereCondition)) {
            // 解析WHERE条件并应用
            // planner.withRowFilter(parseRowFilter(whereCondition));
        }
        
        return planner.plan();
    }
    
    private Map<String, String> parsePartitionFilter(String partitions) {
        Map<String, String> result = Maps.newHashMap();
        
        // 简单的分区过滤解析，支持 "key=value,key2=value2" 格式
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
        
        // 应用排序策略
        if (!"none".equalsIgnoreCase(orderStrategy) && !Strings.isNullOrEmpty(orderColumns)) {
            String[] columns = orderColumns.split(",");
            List<String> orderColumnList = Arrays.stream(columns)
                .map(String::trim)
                .collect(Collectors.toList());
            
            switch (orderStrategy.toLowerCase()) {
                case "zorder":
                    // compactionTask.withZOrderColumns(orderColumnList);
                    break;
                case "hilbert":
                    // compactionTask.withHilbertColumns(orderColumnList);
                    break;
                default:
                    LOG.warn("Unsupported order strategy: {}", orderStrategy);
            }
        }
        
        // 执行压缩
        compactionTask.compact(plan);
    }
}
```

### 5.2 PaimonCreateBranchProcedure

```java
public class PaimonCreateBranchProcedure extends TableProcedure {
    private static final String PROCEDURE_NAME = "create_branch";
    
    public PaimonCreateBranchProcedure() {
        super(PROCEDURE_NAME, buildParameters(), 
              "Create a new branch for the Paimon table",
              Sets.newHashSet("paimon"));
    }
    
    private static List<ProcedureParameter> buildParameters() {
        return Arrays.asList(
            new ProcedureParameter("branch_name", Type.VARCHAR, true, 
                "Name of the branch to create", null),
            new ProcedureParameter("snapshot_id", Type.BIGINT, false, 
                "Snapshot ID to create branch from", null),
            new ProcedureParameter("tag_name", Type.VARCHAR, false, 
                "Tag name to create branch from", null)
        );
    }
    
    @Override
    protected void validateArguments(List<Expression> args, TableIf table) throws UserException {
        super.validateArguments(args, table);
        
        // 额外验证：snapshot_id和tag_name不能同时指定
        if (args.size() >= 3) {
            Expression snapshotIdArg = args.get(1);
            Expression tagNameArg = args.get(2);
            
            boolean hasSnapshotId = snapshotIdArg != null && 
                !(snapshotIdArg instanceof NullLiteral);
            boolean hasTagName = tagNameArg != null && 
                !(tagNameArg instanceof NullLiteral);
                
            if (hasSnapshotId && hasTagName) {
                throw new UserException(
                    "Cannot specify both snapshot_id and tag_name");
            }
        }
    }
    
    @Override
    protected void executeInternal(TableContext context, List<Object> args) throws UserException {
        String branchName = (String) args.get(0);
        Long snapshotId = (Long) args.get(1);
        String tagName = (String) args.get(2);
        
        try {
            PaimonExternalCatalog paimonCatalog = (PaimonExternalCatalog) context.getCatalog();
            org.apache.paimon.catalog.Catalog nativeCatalog = paimonCatalog.createCatalog();
            
            Identifier tableId = Identifier.create(
                context.getTable().getDbName(), 
                context.getTable().getName());
            
            // 创建分支
            if (snapshotId != null) {
                nativeCatalog.createBranch(tableId, branchName, snapshotId);
                LOG.info("Branch '{}' created from snapshot {} for table {}", 
                        branchName, snapshotId, tableId);
            } else if (!Strings.isNullOrEmpty(tagName)) {
                nativeCatalog.createBranch(tableId, branchName, tagName);
                LOG.info("Branch '{}' created from tag '{}' for table {}", 
                        branchName, tagName, tableId);
            } else {
                // 从当前HEAD创建分支
                nativeCatalog.createBranch(tableId, branchName);
                LOG.info("Branch '{}' created from HEAD for table {}", 
                        branchName, tableId);
            }
            
        } catch (Exception e) {
            throw new UserException(String.format(
                "Failed to create branch '%s' for table %s: %s", 
                branchName, context.getTable().getName(), e.getMessage()), e);
        }
    }
}
```

## 6. 具体Catalog的Procedure实现

### 6.1 IcebergExternalCatalog扩展

```java
public abstract class IcebergExternalCatalog extends ExternalCatalog {
    // 现有代码...
    
    @Override
    protected void initializeProcedures() {
        // 核心表管理Procedures
        registerProcedure(new IcebergSnapshotTableProcedure());
        registerProcedure(new IcebergRollbackToSnapshotProcedure());
        registerProcedure(new IcebergSetCurrentSnapshotProcedure());
        registerProcedure(new IcebergCherrypickSnapshotProcedure());
        
        // 分支和标签管理
        registerProcedure(new IcebergCreateBranchProcedure());
        registerProcedure(new IcebergDropBranchProcedure());
        registerProcedure(new IcebergCreateTagProcedure());
        registerProcedure(new IcebergDropTagProcedure());
        
        // 维护Procedures
        registerProcedure(new IcebergExpireSnapshotsProcedure());
        registerProcedure(new IcebergRemoveOrphanFilesProcedure());
        registerProcedure(new IcebergRewriteDataFilesProcedure());
        registerProcedure(new IcebergRewriteManifestsProcedure());
        
        // 元数据管理
        registerProcedure(new IcebergAddFilesProcedure());
        registerProcedure(new IcebergRemoveFilesProcedure());
        registerProcedure(new IcebergMigrateTableProcedure());
        
        // 标记初始化完成
        proceduresInitialized = true;
        
        LOG.info("Initialized {} Iceberg procedures for catalog: {}", 
                registeredProcedures.size(), getName());
    }
}
```

### 6.2 PaimonExternalCatalog扩展

```java
public class PaimonExternalCatalog extends ExternalCatalog {
    // 现有代码...
    
    @Override
    protected void initializeProcedures() {
        // 核心表管理Procedures
        registerProcedure(new PaimonCompactTableProcedure());
        
        // 分支和标签管理
        registerProcedure(new PaimonCreateBranchProcedure());
        registerProcedure(new PaimonDropBranchProcedure());
        registerProcedure(new PaimonCreateTagProcedure());
        registerProcedure(new PaimonDropTagProcedure());
        
        // 维护Procedures
        registerProcedure(new PaimonExpireSnapshotsProcedure());
        registerProcedure(new PaimonRemoveOrphanFilesProcedure());
        
        // 标记初始化完成
        proceduresInitialized = true;
        
        LOG.info("Initialized {} Paimon procedures for catalog: {}", 
                registeredProcedures.size(), getName());
    }
}
```

## 7. 执行器实现

### 7.1 AlterTableExecuteHandler

```java
public class AlterTableExecuteHandler implements AlterHandler {
    private static final Logger LOG = LogManager.getLogger(AlterTableExecuteHandler.class);
    
    @Override
    public void handle(AlterTableStmt stmt, ConnectContext ctx) throws UserException {
        if (!(stmt instanceof AlterTableExecuteStmt)) {
            throw new UserException("Invalid statement type for AlterTableExecuteHandler");
        }
        
        AlterTableExecuteStmt executeStmt = (AlterTableExecuteStmt) stmt;
        
        try {
            // 构建执行上下文
            TableProcedureExecutionContext execContext = 
                new TableProcedureExecutionContext(executeStmt, ctx);
            
            // 执行procedure
            executeProcedure(execContext);
            
            // 记录执行结果
            logExecutionResult(execContext);
            
        } catch (Exception e) {
            // 处理执行异常
            handleExecutionError(executeStmt, e);
            throw e;
        }
    }
    
    private void executeProcedure(TableProcedureExecutionContext context) throws UserException {
        AlterTableExecuteStmt stmt = context.getStatement();
        ConnectContext ctx = context.getConnectContext();
        
        // 获取resolved procedure
        TableProcedure procedure = stmt.getResolvedProcedure();
        TableIf table = stmt.getTargetTable();
        List<Expression> args = stmt.getProcedureArgs();
        
        // 记录执行开始
        long startTime = System.currentTimeMillis();
        LOG.info("Starting procedure execution: {} on table {}.{}.{}", 
                stmt.getProcedureName(),
                table.getCatalog().getName(),
                ((ExternalTable) table).getDbName(),
                table.getName());
        
        try {
            // 执行procedure
            ExternalCatalog externalCatalog = (ExternalCatalog) table.getCatalog();
            externalCatalog.executeProcedure(stmt.getProcedureName(), args, table, ctx);
            
            // 记录执行时长
            long duration = System.currentTimeMillis() - startTime;
            context.setExecutionDuration(duration);
            context.setSuccess(true);
            
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            context.setExecutionDuration(duration);
            context.setSuccess(false);
            context.setError(e);
            throw e;
        }
    }
    
    private void logExecutionResult(TableProcedureExecutionContext context) {
        AlterTableExecuteStmt stmt = context.getStatement();
        ExternalTable table = (ExternalTable) stmt.getTargetTable();
        
        if (context.isSuccess()) {
            LOG.info("Procedure execution completed successfully. " +
                    "Procedure: {}, Table: {}.{}.{}, Duration: {}ms",
                    stmt.getProcedureName(),
                    table.getCatalog().getName(),
                    table.getDbName(),
                    table.getName(),
                    context.getExecutionDuration());
        } else {
            LOG.error("Procedure execution failed. " +
                    "Procedure: {}, Table: {}.{}.{}, Duration: {}ms, Error: {}",
                    stmt.getProcedureName(),
                    table.getCatalog().getName(),
                    table.getDbName(),
                    table.getName(),
                    context.getExecutionDuration(),
                    context.getError().getMessage());
        }
    }
    
    private void handleExecutionError(AlterTableExecuteStmt stmt, Exception e) {
        // 记录详细的错误信息
        ExternalTable table = (ExternalTable) stmt.getTargetTable();
        LOG.error("Failed to execute procedure '{}' on table {}.{}.{}: {}", 
                stmt.getProcedureName(),
                table.getCatalog().getName(),
                table.getDbName(),
                table.getName(),
                e.getMessage(), e);
        
        // 更新监控指标
        updateErrorMetrics(table.getCatalog().getType(), stmt.getProcedureName());
    }
    
    private void updateErrorMetrics(String catalogType, String procedureName) {
        // 更新Prometheus监控指标
        if (ProcedureMetrics.PROCEDURE_ERROR_COUNTER != null) {
            ProcedureMetrics.PROCEDURE_ERROR_COUNTER
                .labels(catalogType, procedureName)
                .inc();
        }
    }
}
```

### 7.2 TableProcedureExecutionContext

```java
public class TableProcedureExecutionContext {
    private final AlterTableExecuteStmt statement;
    private final ConnectContext connectContext;
    
    private long executionDuration;
    private boolean success;
    private Exception error;
    private Map<String, Object> metadata;
    
    public TableProcedureExecutionContext(AlterTableExecuteStmt statement, 
                                         ConnectContext connectContext) {
        this.statement = statement;
        this.connectContext = connectContext;
        this.metadata = Maps.newHashMap();
    }
    
    // 添加执行元数据
    public void addMetadata(String key, Object value) {
        metadata.put(key, value);
    }
    
    public Object getMetadata(String key) {
        return metadata.get(key);
    }
    
    // Getters and Setters
    public AlterTableExecuteStmt getStatement() { return statement; }
    public ConnectContext getConnectContext() { return connectContext; }
    
    public long getExecutionDuration() { return executionDuration; }
    public void setExecutionDuration(long executionDuration) { 
        this.executionDuration = executionDuration; 
    }
    
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
    
    public Exception getError() { return error; }
    public void setError(Exception error) { this.error = error; }
    
    public Map<String, Object> getMetadata() { return metadata; }
}
```

## 8. 监控和指标

### 8.1 性能监控

```java
public class ProcedureMetrics {
    // 执行次数统计
    public static final Counter PROCEDURE_EXECUTION_COUNTER = 
        Counter.build()
            .name("doris_table_procedure_executions_total")
            .labelNames("catalog_type", "procedure", "status")
            .help("Total table procedure executions")
            .register();
    
    // 执行时间统计
    public static final Histogram PROCEDURE_EXECUTION_DURATION = 
        Histogram.build()
            .name("doris_table_procedure_execution_duration_seconds")
            .labelNames("catalog_type", "procedure")
            .buckets(0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0)
            .help("Table procedure execution duration in seconds")
            .register();
    
    // 错误统计
    public static final Counter PROCEDURE_ERROR_COUNTER = 
        Counter.build()
            .name("doris_table_procedure_errors_total")
            .labelNames("catalog_type", "procedure")
            .help("Total table procedure execution errors")
            .register();
    
    // 当前正在执行的procedure数量
    public static final Gauge PROCEDURE_ACTIVE_EXECUTIONS = 
        Gauge.build()
            .name("doris_table_procedure_active_executions")
            .labelNames("catalog_type")
            .help("Number of currently executing table procedures")
            .register();
    
    public static void recordExecution(String catalogType, String procedure, 
                                      boolean success, double durationSeconds) {
        String status = success ? "success" : "failure";
        
        PROCEDURE_EXECUTION_COUNTER.labels(catalogType, procedure, status).inc();
        PROCEDURE_EXECUTION_DURATION.labels(catalogType, procedure).observe(durationSeconds);
        
        if (!success) {
            PROCEDURE_ERROR_COUNTER.labels(catalogType, procedure).inc();
        }
    }
}
```

## 9. 使用示例

### 9.1 基础操作示例

```sql
-- 创建Iceberg和Paimon catalog
CREATE CATALOG iceberg_catalog PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "hms",
    "hive.metastore.uris" = "thrift://localhost:9083"
);

CREATE CATALOG paimon_catalog PROPERTIES (
    "type" = "paimon",
    "paimon.catalog.type" = "filesystem",
    "warehouse" = "hdfs://namenode:port/path/to/warehouse"
);

-- 查看支持的procedures
SHOW PROCEDURES FROM iceberg_catalog;
SHOW PROCEDURES FROM paimon_catalog;
SHOW PROCEDURES LIKE 'snapshot%';

-- 示例输出:
-- +------------------+-------------+----------------------------------------+----------------------------------+--------+
-- | Catalog          | Procedure   | Parameters                             | Description                      | Type   |
-- +------------------+-------------+----------------------------------------+----------------------------------+--------+
-- | iceberg_catalog  | snapshot    | operation VARCHAR DEFAULT 'append',   | Create a snapshot of the table   | ICEBERG|
-- |                  |             | where_condition VARCHAR DEFAULT NULL, |                                  |        |
-- |                  |             | options VARCHAR DEFAULT '{}'           |                                  |        |
-- | iceberg_catalog  | compact     | partitions VARCHAR DEFAULT NULL,      | Compact small files             | ICEBERG|
-- |                  |             | order_strategy VARCHAR DEFAULT 'none' |                                  |        |
-- +------------------+-------------+----------------------------------------+----------------------------------+--------+

-- Iceberg表操作示例
ALTER TABLE iceberg_catalog.sales_db.orders 
EXECUTE snapshot('append');

ALTER TABLE iceberg_catalog.sales_db.orders 
EXECUTE rollback_to_snapshot(123456789);

ALTER TABLE iceberg_catalog.sales_db.orders 
EXECUTE expire_snapshots(timestamp('2024-01-01 00:00:00'), 5);

ALTER TABLE iceberg_catalog.sales_db.orders 
EXECUTE create_branch('feature_branch', 123456789);

-- Paimon表操作示例
ALTER TABLE paimon_catalog.warehouse_db.inventory 
EXECUTE compact('dt=2024-01-01', 'zorder', 'product_id,category');

ALTER TABLE paimon_catalog.warehouse_db.inventory 
EXECUTE create_branch('release_v1', 987654321);

ALTER TABLE paimon_catalog.warehouse_db.inventory 
EXECUTE expire_snapshots(timestamp('2024-01-01 00:00:00'), 10);
```

### 9.2 高级操作示例

```sql
-- 复杂的快照操作
ALTER TABLE iceberg_catalog.logs_db.access_logs 
EXECUTE snapshot('overwrite', 'log_level = "ERROR"', 
    '{"snapshot-property": "cleanup.policy=delete"}');

-- 带条件的压缩操作
ALTER TABLE paimon_catalog.metrics_db.events 
EXECUTE compact(
    'year=2024,month=01', 
    'hilbert', 
    'user_id,event_type', 
    'event_time > "2024-01-01 00:00:00"'
);

-- 批量维护操作
ALTER TABLE iceberg_catalog.data_db.user_events 
EXECUTE remove_orphan_files();

ALTER TABLE iceberg_catalog.data_db.user_events 
EXECUTE rewrite_data_files('{"target-file-size-bytes": "134217728"}');

-- 元数据管理
ALTER TABLE iceberg_catalog.staging_db.temp_data 
EXECUTE add_files('/path/to/new/data/*.parquet');
```

### 9.3 事务性操作示例

```sql
-- 事务中执行多个procedure
BEGIN;

ALTER TABLE iceberg_catalog.finance_db.transactions 
EXECUTE snapshot('append');

ALTER TABLE iceberg_catalog.finance_db.transactions 
EXECUTE create_tag('monthly_snapshot_202401');

ALTER TABLE iceberg_catalog.finance_db.transactions 
EXECUTE expire_snapshots(timestamp('2023-12-01 00:00:00'), 10);

COMMIT;
```

## 10. 性能优化和最佳实践

### 10.1 性能优化建议

1. **批量操作**：对于大量文件的操作，建议使用批量参数
2. **并发控制**：合理设置procedure执行的并发度
3. **监控指标**：持续监控procedure执行时间和成功率
4. **资源管理**：为procedure执行分配合适的内存和CPU资源

### 10.2 最佳实践

1. **权限管理**：为不同用户分配适当的procedure执行权限
2. **定期维护**：定期执行清理和压缩操作
3. **备份策略**：在执行重要操作前创建快照或标签
4. **监控告警**：设置procedure执行失败的告警机制

## 11. 总结

本方案基于`ALTER TABLE EXECUTE`语法设计，通过Catalog级别的Procedure注册管理，提供了：

### 11.1 核心特性
1. **直观的语法**：`ALTER TABLE table EXECUTE procedure(args)`与表紧密关联
2. **Catalog级别管理**：每个ExternalCatalog独立管理自己的Procedures
3. **完整的SHOW支持**：`SHOW PROCEDURES [FROM catalog] [LIKE pattern]`查看可用procedures
4. **精确的权限控制**：基于表级ALTER权限和catalog级SHOW权限
5. **丰富的上下文信息**：自动获取表信息，简化参数传递
6. **完善的监控体系**：提供详细的执行指标和错误统计

### 11.2 架构优势
1. **去中心化注册**：取消了全局注册中心，改为Catalog级别管理
2. **懒加载初始化**：Procedures在首次访问时才初始化，节省资源
3. **类型安全**：完整的参数验证和类型转换机制
4. **扩展性强**：新的数据湖格式只需继承ExternalCatalog并实现initializeProcedures()

### 11.3 实现亮点
1. **统一接口设计**：通过ExternalCatalog抽象类提供统一的Procedure管理接口
2. **自动发现机制**：SHOW PROCEDURES能自动发现和展示各Catalog支持的Procedures
3. **参数自动推导**：根据表上下文自动推导默认参数值
4. **错误处理完善**：统一的异常处理和用户友好的错误信息

### 11.4 使用体验
- **开发者友好**：新增数据湖格式支持只需实现几个简单方法
- **用户直观**：语法清晰，权限控制精确，错误信息明确
- **运维便利**：完整的监控指标，易于troubleshooting

该方案更符合数据库标准，为用户提供了直观、安全、高效的数据湖表管理能力，同时为系统架构带来了更好的模块化和可扩展性。