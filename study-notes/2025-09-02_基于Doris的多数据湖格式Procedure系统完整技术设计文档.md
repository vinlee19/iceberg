# 基于Doris的多数据湖格式Procedure系统完整技术设计文档

## 1. 项目背景与需求分析

### 1.1 项目背景
Apache Doris作为现代化的MPP数据库，已经支持多种外部数据源的集成，包括Iceberg和Paimon等主流数据湖格式。然而，目前Doris缺乏对这些外部数据源Procedure（存储过程）的统一支持，限制了用户对数据湖进行高级操作的能力。

### 1.2 需求分析
1. **统一的Procedure框架**：支持Iceberg和Paimon的Procedure调用
2. **外部Catalog集成**：基于现有的ExternalCatalog架构扩展
3. **SHOW PROCEDURE支持**：能够查看和发现外部Catalog支持的Procedure
4. **参数校验和类型转换**：确保Procedure调用的安全性和正确性
5. **权限控制**：集成Doris现有的权限管理系统

### 1.3 技术调研结论

#### 1.3.1 Apache Iceberg Procedure系统特点
- 基于Spark SQL Extensions实现
- 支持20+内置Procedure（如snapshot_table、rollback_to_snapshot等）
- 采用Strategy + Factory Method + Template Method设计模式
- 完整的参数验证和类型转换机制

#### 1.3.2 Doris现有架构分析
- **CatalogIf接口**：定义Catalog的基本操作
- **ExternalCatalog抽象类**：外部数据源的统一基础
- **CallFunc框架**：已支持EXECUTE_STMT和FLUSH_AUDIT_LOG
- **CallProcedure类**：通过PlSqlOperation执行存储过程

## 2. 系统架构设计

### 2.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                        Doris SQL层                              │
├─────────────────────────────────────────────────────────────────┤
│ CALL procedure_name(args...)  │  SHOW PROCEDURES [FROM catalog] │
├─────────────────────────────────────────────────────────────────┤
│                   CallFunc Factory 扩展                         │
├─────────────────────────────────────────────────────────────────┤
│  CallExternalProcedure  │  CallShowProcedure  │  原有CallFunc   │
├─────────────────────────────────────────────────────────────────┤
│                   ProcedureRegistry 注册中心                     │
├─────────────────────────────────────────────────────────────────┤
│        BaseProcedure抽象基类 (参数验证 + 执行模板)              │
├─────────────────────────────────────────────────────────────────┤
│ IcebergProcedure  │  PaimonProcedure  │  Future Extensions...   │
├─────────────────────────────────────────────────────────────────┤
│     IcebergExternalCatalog    │    PaimonExternalCatalog        │
├─────────────────────────────────────────────────────────────────┤
│              ExternalCatalog (CatalogIf扩展)                   │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 核心组件设计

#### 2.2.1 CatalogIf接口扩展
在现有CatalogIf接口基础上添加Procedure相关方法：

```java
public interface CatalogIf<T extends DatabaseIf> {
    // 现有方法...
    
    // 新增Procedure相关方法
    default List<ProcedureInfo> listProcedures() {
        return Lists.newArrayList();
    }
    
    default Optional<BaseProcedure> getProcedure(String procedureName) {
        return Optional.empty();
    }
    
    default boolean supportsProcedures() {
        return false;
    }
    
    default void executeProcedure(String procedureName, List<Expression> args, 
                                  ConnectContext ctx) throws UserException {
        throw new UserException("Procedure execution not supported for catalog: " + getName());
    }
}
```

#### 2.2.2 BaseProcedure抽象基类

```java
public abstract class BaseProcedure {
    protected final String procedureName;
    protected final List<ProcedureParameter> parameters;
    protected final CatalogIf catalog;
    
    public BaseProcedure(String procedureName, List<ProcedureParameter> parameters, CatalogIf catalog) {
        this.procedureName = procedureName;
        this.parameters = parameters;
        this.catalog = catalog;
    }
    
    // Template Method Pattern - 定义执行流程
    public final void execute(List<Expression> args, ConnectContext ctx) throws UserException {
        validateArguments(args);
        List<Object> processedArgs = processArguments(args);
        checkPermissions(ctx);
        executeInternal(processedArgs, ctx);
    }
    
    // 参数验证 (Strategy Pattern)
    protected void validateArguments(List<Expression> args) throws UserException {
        if (args.size() != parameters.size()) {
            throw new UserException(String.format(
                "Procedure %s expects %d arguments, but got %d", 
                procedureName, parameters.size(), args.size()));
        }
        
        for (int i = 0; i < args.size(); i++) {
            ProcedureParameter param = parameters.get(i);
            Expression arg = args.get(i);
            param.validate(arg);
        }
    }
    
    // 参数处理和类型转换
    protected List<Object> processArguments(List<Expression> args) throws UserException {
        List<Object> result = new ArrayList<>();
        for (int i = 0; i < args.size(); i++) {
            ProcedureParameter param = parameters.get(i);
            Expression arg = args.get(i);
            result.add(param.convertValue(arg));
        }
        return result;
    }
    
    // 权限检查
    protected void checkPermissions(ConnectContext ctx) throws UserException {
        UserIdentity user = ctx.getCurrentUserIdentity();
        String catalogName = catalog.getName();
        
        if (!Env.getCurrentEnv().getAccessManager()
                .checkCtlPriv(user, catalogName, PrivPredicate.LOAD)) {
            throw new UserException(
                String.format("User %s has no privilege to execute procedure in catalog %s", 
                            user, catalogName));
        }
    }
    
    // 子类实现具体执行逻辑
    protected abstract void executeInternal(List<Object> args, ConnectContext ctx) throws UserException;
    
    // Getter methods
    public String getProcedureName() { return procedureName; }
    public List<ProcedureParameter> getParameters() { return parameters; }
    public String getDescription() { return ""; }
}
```

#### 2.2.3 ProcedureParameter参数定义

```java
public class ProcedureParameter {
    private final String name;
    private final Type type;
    private final boolean required;
    private final String description;
    private final Object defaultValue;
    
    public ProcedureParameter(String name, Type type, boolean required, 
                             String description, Object defaultValue) {
        this.name = name;
        this.type = type;
        this.required = required;
        this.description = description;
        this.defaultValue = defaultValue;
    }
    
    public void validate(Expression arg) throws UserException {
        if (arg == null && required) {
            throw new UserException(String.format("Required parameter %s is missing", name));
        }
        
        if (arg != null && !isCompatibleType(arg.getType(), type)) {
            throw new UserException(String.format(
                "Parameter %s expects type %s, but got %s", 
                name, type, arg.getType()));
        }
    }
    
    public Object convertValue(Expression arg) throws UserException {
        if (arg == null) {
            return defaultValue;
        }
        
        // 类型转换逻辑
        if (arg instanceof Literal) {
            Literal literal = (Literal) arg;
            return convertLiteralValue(literal, type);
        }
        
        throw new UserException(String.format(
            "Parameter %s must be a constant value", name));
    }
    
    private boolean isCompatibleType(Type argType, Type expectedType) {
        // 实现类型兼容性检查
        return Type.canCastTo(argType, expectedType);
    }
    
    private Object convertLiteralValue(Literal literal, Type targetType) throws UserException {
        // 实现具体的类型转换逻辑
        switch (targetType.getPrimitiveType()) {
            case VARCHAR:
                return literal.getStringValue();
            case BIGINT:
                return literal.getLongValue();
            case BOOLEAN:
                return literal.getBoolValue();
            default:
                throw new UserException("Unsupported parameter type: " + targetType);
        }
    }
}
```

#### 2.2.4 ProcedureRegistry注册中心

```java
public class ProcedureRegistry {
    private static final Map<String, Map<String, BaseProcedure>> catalogProcedures = Maps.newConcurrentMap();
    
    // 注册Catalog的所有Procedure
    public static void registerCatalogProcedures(String catalogName, List<BaseProcedure> procedures) {
        Map<String, BaseProcedure> procedureMap = Maps.newHashMap();
        for (BaseProcedure procedure : procedures) {
            procedureMap.put(procedure.getProcedureName().toLowerCase(), procedure);
        }
        catalogProcedures.put(catalogName, procedureMap);
    }
    
    // 获取指定Catalog的Procedure
    public static Optional<BaseProcedure> getProcedure(String catalogName, String procedureName) {
        Map<String, BaseProcedure> procedures = catalogProcedures.get(catalogName);
        if (procedures != null) {
            return Optional.ofNullable(procedures.get(procedureName.toLowerCase()));
        }
        return Optional.empty();
    }
    
    // 列出Catalog的所有Procedure
    public static List<ProcedureInfo> listCatalogProcedures(String catalogName) {
        Map<String, BaseProcedure> procedures = catalogProcedures.get(catalogName);
        if (procedures == null) {
            return Lists.newArrayList();
        }
        
        return procedures.values().stream()
            .map(proc -> new ProcedureInfo(
                proc.getProcedureName(),
                proc.getParameters(),
                proc.getDescription()
            ))
            .collect(Collectors.toList());
    }
    
    // 清理Catalog的Procedure
    public static void unregisterCatalogProcedures(String catalogName) {
        catalogProcedures.remove(catalogName);
    }
}
```

### 2.3 Iceberg Procedure实现

#### 2.3.1 IcebergExternalCatalog扩展

```java
public abstract class IcebergExternalCatalog extends ExternalCatalog {
    // 现有代码...
    
    private Map<String, BaseProcedure> icebergProcedures;
    
    @Override
    protected void initLocalObjectsImpl() {
        // 现有初始化逻辑...
        
        // 初始化Iceberg Procedures
        initIcebergProcedures();
    }
    
    private void initIcebergProcedures() {
        icebergProcedures = Maps.newHashMap();
        
        // 注册核心Iceberg Procedures
        registerProcedure(new SnapshotTableProcedure(this));
        registerProcedure(new RollbackToSnapshotProcedure(this));
        registerProcedure(new SetCurrentSnapshotProcedure(this));
        registerProcedure(new CherrypickSnapshotProcedure(this));
        registerProcedure(new CreateBranchProcedure(this));
        registerProcedure(new DropBranchProcedure(this));
        registerProcedure(new CreateTagProcedure(this));
        registerProcedure(new DropTagProcedure(this));
        registerProcedure(new ExpireSnapshotsProcedure(this));
        registerProcedure(new RemoveOrphanFilesProcedure(this));
        registerProcedure(new RewriteDataFilesProcedure(this));
        registerProcedure(new RewriteManifestsProcedure(this));
        
        // 注册到全局注册中心
        ProcedureRegistry.registerCatalogProcedures(getName(), 
            new ArrayList<>(icebergProcedures.values()));
    }
    
    private void registerProcedure(BaseProcedure procedure) {
        icebergProcedures.put(procedure.getProcedureName().toLowerCase(), procedure);
    }
    
    @Override
    public List<ProcedureInfo> listProcedures() {
        makeSureInitialized();
        return ProcedureRegistry.listCatalogProcedures(getName());
    }
    
    @Override
    public Optional<BaseProcedure> getProcedure(String procedureName) {
        makeSureInitialized();
        return ProcedureRegistry.getProcedure(getName(), procedureName);
    }
    
    @Override
    public boolean supportsProcedures() {
        return true;
    }
    
    @Override
    public void executeProcedure(String procedureName, List<Expression> args, 
                                ConnectContext ctx) throws UserException {
        Optional<BaseProcedure> procedure = getProcedure(procedureName);
        if (!procedure.isPresent()) {
            throw new UserException(String.format(
                "Procedure '%s' not found in catalog '%s'", procedureName, getName()));
        }
        
        procedure.get().execute(args, ctx);
    }
}
```

#### 2.3.2 具体Procedure实现示例

```java
public class SnapshotTableProcedure extends BaseProcedure {
    private static final String PROCEDURE_NAME = "snapshot";
    
    public SnapshotTableProcedure(IcebergExternalCatalog catalog) {
        super(PROCEDURE_NAME, buildParameters(), catalog);
    }
    
    private static List<ProcedureParameter> buildParameters() {
        return Arrays.asList(
            new ProcedureParameter("table", Type.VARCHAR, true, 
                "Table name in format 'db.table'", null),
            new ProcedureParameter("operation", Type.VARCHAR, false, 
                "Snapshot operation type", "append"),
            new ProcedureParameter("location", Type.VARCHAR, false, 
                "Data file location", null)
        );
    }
    
    @Override
    protected void executeInternal(List<Object> args, ConnectContext ctx) throws UserException {
        String tableName = (String) args.get(0);
        String operation = (String) args.get(1);
        String location = (String) args.get(2);
        
        try {
            // 解析table name
            String[] parts = tableName.split("\\.");
            if (parts.length != 2) {
                throw new UserException("Table name must be in format 'database.table'");
            }
            String dbName = parts[0];
            String tblName = parts[1];
            
            // 获取Iceberg catalog和table
            IcebergExternalCatalog icebergCatalog = (IcebergExternalCatalog) catalog;
            Catalog icebergNativeCatalog = icebergCatalog.getCatalog();
            TableIdentifier tableId = TableIdentifier.of(dbName, tblName);
            
            Table table = icebergNativeCatalog.loadTable(tableId);
            
            // 执行snapshot操作
            Snapshot snapshot;
            switch (operation.toLowerCase()) {
                case "append":
                    AppendFiles appendFiles = table.newAppend();
                    if (location != null) {
                        // 添加数据文件逻辑
                        appendFiles.appendFile(createDataFile(location));
                    }
                    appendFiles.commit();
                    snapshot = table.currentSnapshot();
                    break;
                    
                case "overwrite":
                    OverwriteFiles overwriteFiles = table.newOverwrite();
                    if (location != null) {
                        overwriteFiles.addFile(createDataFile(location));
                    }
                    overwriteFiles.commit();
                    snapshot = table.currentSnapshot();
                    break;
                    
                default:
                    throw new UserException("Unsupported operation: " + operation);
            }
            
            // 返回结果信息
            LOG.info("Snapshot created successfully. Snapshot ID: {}, Table: {}", 
                    snapshot.snapshotId(), tableName);
                    
        } catch (Exception e) {
            throw new UserException("Failed to create snapshot: " + e.getMessage(), e);
        }
    }
    
    private DataFile createDataFile(String location) {
        // 实现数据文件创建逻辑
        // 这里简化处理，实际需要根据文件格式和schema创建
        return null;
    }
    
    @Override
    public String getDescription() {
        return "Create a snapshot of the table with specified operation";
    }
}
```

### 2.4 Paimon Procedure实现

#### 2.4.1 PaimonExternalCatalog扩展

```java
public class PaimonExternalCatalog extends ExternalCatalog {
    // 现有代码...
    
    private Map<String, BaseProcedure> paimonProcedures;
    
    @Override
    protected void initLocalObjectsImpl() {
        // 现有初始化逻辑...
        
        // 初始化Paimon Procedures
        initPaimonProcedures();
    }
    
    private void initPaimonProcedures() {
        paimonProcedures = Maps.newHashMap();
        
        // 注册核心Paimon Procedures
        registerProcedure(new CompactTableProcedure(this));
        registerProcedure(new CreateBranchProcedure(this));
        registerProcedure(new DropBranchProcedure(this));
        registerProcedure(new CreateTagProcedure(this));
        registerProcedure(new DropTagProcedure(this));
        registerProcedure(new ExpireSnapshotsProcedure(this));
        registerProcedure(new RemoveOrphanFilesProcedure(this));
        
        // 注册到全局注册中心
        ProcedureRegistry.registerCatalogProcedures(getName(), 
            new ArrayList<>(paimonProcedures.values()));
    }
    
    // 实现CatalogIf接口方法 (与Iceberg类似)
    @Override
    public boolean supportsProcedures() {
        return true;
    }
    
    // 其他方法实现...
}
```

#### 2.4.2 Paimon Procedure示例

```java
public class CompactTableProcedure extends BaseProcedure {
    private static final String PROCEDURE_NAME = "compact";
    
    public CompactTableProcedure(PaimonExternalCatalog catalog) {
        super(PROCEDURE_NAME, buildParameters(), catalog);
    }
    
    private static List<ProcedureParameter> buildParameters() {
        return Arrays.asList(
            new ProcedureParameter("table", Type.VARCHAR, true, 
                "Table name in format 'db.table'", null),
            new ProcedureParameter("partitions", Type.VARCHAR, false, 
                "Partition filter expression", null),
            new ProcedureParameter("order_strategy", Type.VARCHAR, false, 
                "Order strategy for compaction", "none")
        );
    }
    
    @Override
    protected void executeInternal(List<Object> args, ConnectContext ctx) throws UserException {
        String tableName = (String) args.get(0);
        String partitions = (String) args.get(1);
        String orderStrategy = (String) args.get(2);
        
        try {
            // 解析table name
            String[] parts = tableName.split("\\.");
            if (parts.length != 2) {
                throw new UserException("Table name must be in format 'database.table'");
            }
            String dbName = parts[0];
            String tblName = parts[1];
            
            // 获取Paimon catalog和table
            PaimonExternalCatalog paimonCatalog = (PaimonExternalCatalog) catalog;
            NameMapping nameMapping = new NameMapping(null, dbName, tblName, dbName, tblName);
            org.apache.paimon.table.Table table = paimonCatalog.getPaimonTable(nameMapping);
            
            // 执行compaction操作
            CompactionPlan compactionPlan = table.store().newCompaction().plan();
            if (compactionPlan.isEmpty()) {
                LOG.info("No files need to be compacted for table: {}", tableName);
                return;
            }
            
            // 执行压缩
            table.store().newCompaction().compact(compactionPlan);
            
            LOG.info("Table compaction completed successfully. Table: {}, " +
                    "Compacted files: {}", tableName, compactionPlan.files().size());
                    
        } catch (Exception e) {
            throw new UserException("Failed to compact table: " + e.getMessage(), e);
        }
    }
    
    @Override
    public String getDescription() {
        return "Compact small files for the specified table";
    }
}
```

### 2.5 CallFunc框架扩展

#### 2.5.1 CallFunc工厂方法扩展

```java
public abstract class CallFunc {
    // 现有代码...
    
    public static CallFunc getFunc(ConnectContext ctx, UserIdentity user, 
                                  UnboundFunction unboundFunction, String originSql) {
        String funcName = unboundFunction.getName().toUpperCase();
        switch (funcName) {
            // 现有built-in functions
            case "EXECUTE_STMT":
                return CallExecuteStmtFunc.create(user, unboundFunction.getArguments());
            case "FLUSH_AUDIT_LOG":
                return CallFlushAuditLogFunc.create(user, unboundFunction.getArguments());
                
            // 新增外部Catalog Procedure支持
            default:
                // 检查是否为外部Catalog的Procedure调用
                Optional<CallFunc> externalProcedure = tryCreateExternalProcedureCall(
                    ctx, user, funcName, unboundFunction.getArguments());
                if (externalProcedure.isPresent()) {
                    return externalProcedure.get();
                }
                
                // 回退到原有的CallProcedure逻辑
                return CallProcedure.create(ctx, originSql);
        }
    }
    
    private static Optional<CallFunc> tryCreateExternalProcedureCall(
            ConnectContext ctx, UserIdentity user, String funcName, List<Expression> args) {
        
        // 遍历所有Catalog查找匹配的Procedure
        CatalogMgr catalogMgr = Env.getCurrentEnv().getCatalogMgr();
        for (CatalogIf catalog : catalogMgr.getCopyOfCatalog()) {
            if (catalog.supportsProcedures()) {
                Optional<BaseProcedure> procedure = catalog.getProcedure(funcName);
                if (procedure.isPresent()) {
                    return Optional.of(new CallExternalProcedure(
                        user, catalog.getName(), funcName, args));
                }
            }
        }
        
        return Optional.empty();
    }
}
```

#### 2.5.2 CallExternalProcedure实现

```java
public class CallExternalProcedure extends CallFunc {
    private final UserIdentity user;
    private final String catalogName;
    private final String procedureName;
    private final List<Expression> arguments;
    
    private CallExternalProcedure(UserIdentity user, String catalogName, 
                                  String procedureName, List<Expression> arguments) {
        this.user = Objects.requireNonNull(user, "user is missing");
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName is missing");
        this.procedureName = Objects.requireNonNull(procedureName, "procedureName is missing");
        this.arguments = Objects.requireNonNull(arguments, "arguments is missing");
    }
    
    public static CallFunc create(UserIdentity user, String catalogName, 
                                  String procedureName, List<Expression> args) {
        return new CallExternalProcedure(user, catalogName, procedureName, args);
    }
    
    @Override
    public void run() {
        try {
            // 获取Catalog
            CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
            if (catalog == null) {
                throw new AnalysisException("Catalog not found: " + catalogName);
            }
            
            if (!catalog.supportsProcedures()) {
                throw new AnalysisException(String.format(
                    "Catalog %s does not support procedures", catalogName));
            }
            
            // 执行Procedure
            ConnectContext ctx = ConnectContext.get();
            ctx.setCurrentUserIdentity(user);
            catalog.executeProcedure(procedureName, arguments, ctx);
            
        } catch (Exception e) {
            throw new RuntimeException(String.format(
                "Failed to execute procedure %s in catalog %s: %s", 
                procedureName, catalogName, e.getMessage()), e);
        }
    }
}
```

### 2.6 SHOW PROCEDURES指令支持

#### 2.6.1 ShowProceduresStmt语法解析

```java
public class ShowProceduresStmt extends ShowStmt {
    private String catalogName;
    private String dbName;
    private String procedurePattern;
    
    public ShowProceduresStmt(String catalogName, String dbName, String procedurePattern) {
        this.catalogName = catalogName;
        this.dbName = dbName;
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
        
        if (!catalog.supportsProcedures()) {
            throw new AnalysisException(String.format(
                "Catalog %s does not support procedures", catalogName));
        }
    }
    
    // Getter methods...
    public String getCatalogName() { return catalogName; }
    public String getDbName() { return dbName; }
    public String getProcedurePattern() { return procedurePattern; }
}
```

#### 2.6.2 ShowProceduresAnalyzer处理

```java
public class ShowProceduresAnalyzer {
    
    public static ShowResultSet analyze(ShowProceduresStmt stmt, ConnectContext ctx) 
            throws AnalysisException {
        
        String catalogName = stmt.getCatalogName();
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
        
        // 检查权限
        if (!Env.getCurrentEnv().getAccessManager().checkCtlPriv(
                ctx.getCurrentUserIdentity(), catalogName, PrivPredicate.SHOW)) {
            throw new AnalysisException(String.format(
                "Access denied. User %s needs SHOW privilege on catalog %s", 
                ctx.getCurrentUserIdentity(), catalogName));
        }
        
        // 获取Procedure列表
        List<ProcedureInfo> procedures = catalog.listProcedures();
        
        // 应用过滤条件
        if (!Strings.isNullOrEmpty(stmt.getProcedurePattern())) {
            procedures = procedures.stream()
                .filter(proc -> proc.getName().matches(stmt.getProcedurePattern()))
                .collect(Collectors.toList());
        }
        
        // 构建结果集
        return buildResultSet(procedures, catalogName);
    }
    
    private static ShowResultSet buildResultSet(List<ProcedureInfo> procedures, String catalogName) {
        List<List<String>> rows = Lists.newArrayList();
        
        for (ProcedureInfo proc : procedures) {
            List<String> row = Lists.newArrayList();
            row.add(catalogName);                    // Catalog
            row.add(proc.getName());                 // Procedure Name
            row.add(formatParameters(proc.getParameters()));  // Parameters
            row.add(proc.getDescription());          // Description
            rows.add(row);
        }
        
        // 定义列信息
        List<Column> columns = Arrays.asList(
            new Column("Catalog", Type.VARCHAR),
            new Column("Procedure", Type.VARCHAR),
            new Column("Parameters", Type.VARCHAR),
            new Column("Description", Type.VARCHAR)
        );
        
        return new ShowResultSet(new ShowResultSetMetaData(columns), rows);
    }
    
    private static String formatParameters(List<ProcedureParameter> parameters) {
        return parameters.stream()
            .map(param -> String.format("%s %s%s", 
                param.getName(), 
                param.getType().toString(),
                param.isRequired() ? "" : " DEFAULT " + param.getDefaultValue()))
            .collect(Collectors.joining(", "));
    }
}
```

## 3. 实现细节和关键技术点

### 3.1 参数验证和类型转换

#### 3.1.1 类型系统映射
建立Doris类型系统与数据湖格式类型系统的映射关系：

```java
public class TypeConverter {
    private static final Map<PrimitiveType, Set<PrimitiveType>> COMPATIBLE_TYPES = 
        ImmutableMap.<PrimitiveType, Set<PrimitiveType>>builder()
            .put(PrimitiveType.VARCHAR, Sets.newHashSet(
                PrimitiveType.VARCHAR, PrimitiveType.STRING, PrimitiveType.CHAR))
            .put(PrimitiveType.BIGINT, Sets.newHashSet(
                PrimitiveType.BIGINT, PrimitiveType.INT, PrimitiveType.SMALLINT))
            .put(PrimitiveType.BOOLEAN, Sets.newHashSet(PrimitiveType.BOOLEAN))
            .put(PrimitiveType.DOUBLE, Sets.newHashSet(
                PrimitiveType.DOUBLE, PrimitiveType.FLOAT, PrimitiveType.DECIMAL))
            .build();
    
    public static boolean isCompatible(Type from, Type to) {
        if (from.equals(to)) return true;
        
        Set<PrimitiveType> compatibleTypes = COMPATIBLE_TYPES.get(to.getPrimitiveType());
        return compatibleTypes != null && compatibleTypes.contains(from.getPrimitiveType());
    }
}
```

#### 3.1.2 表达式求值
实现常量表达式的求值和类型转换：

```java
public class ExpressionEvaluator {
    
    public static Object evaluateConstant(Expression expr, Type targetType) throws UserException {
        if (!(expr instanceof Literal)) {
            throw new UserException("Expression must be a constant literal");
        }
        
        Literal literal = (Literal) expr;
        return convertLiteralValue(literal, targetType);
    }
    
    private static Object convertLiteralValue(Literal literal, Type targetType) throws UserException {
        try {
            switch (targetType.getPrimitiveType()) {
                case VARCHAR:
                case STRING:
                    return literal.getStringValue();
                case BIGINT:
                case INT:
                    return literal.getLongValue();
                case BOOLEAN:
                    return literal.getBoolValue();
                case DOUBLE:
                case FLOAT:
                    return literal.getDoubleValue();
                case DECIMAL:
                    return literal.getDecimalValue();
                default:
                    throw new UserException("Unsupported parameter type: " + targetType);
            }
        } catch (Exception e) {
            throw new UserException(String.format(
                "Cannot convert literal %s to type %s", literal, targetType), e);
        }
    }
}
```

### 3.2 权限控制集成

#### 3.2.1 权限检查策略
```java
public class ProcedurePrivilegeChecker {
    
    public static void checkExecutePrivilege(ConnectContext ctx, String catalogName, 
                                           String procedureName) throws UserException {
        UserIdentity user = ctx.getCurrentUserIdentity();
        
        // 检查Catalog级别权限
        if (!Env.getCurrentEnv().getAccessManager().checkCtlPriv(
                user, catalogName, PrivPredicate.LOAD)) {
            throw new UserException(String.format(
                "Access denied. User %s needs LOAD privilege on catalog %s to execute procedures", 
                user, catalogName));
        }
        
        // 检查特定Procedure权限(如果需要更细粒度控制)
        checkSpecificProcedurePrivilege(user, catalogName, procedureName);
    }
    
    private static void checkSpecificProcedurePrivilege(UserIdentity user, String catalogName, 
                                                       String procedureName) throws UserException {
        // 对于敏感操作(如删除、修改schema等)进行额外权限检查
        Set<String> sensitiveProcs = Sets.newHashSet(
            "drop_table", "alter_table", "expire_snapshots", "remove_orphan_files");
        
        if (sensitiveProcs.contains(procedureName.toLowerCase())) {
            if (!Env.getCurrentEnv().getAccessManager().checkCtlPriv(
                    user, catalogName, PrivPredicate.ALTER)) {
                throw new UserException(String.format(
                    "Access denied. User %s needs ALTER privilege to execute procedure %s", 
                    user, procedureName));
            }
        }
    }
}
```

### 3.3 错误处理和日志记录

#### 3.3.1 统一异常处理
```java
public class ProcedureExceptionHandler {
    private static final Logger LOG = LogManager.getLogger(ProcedureExceptionHandler.class);
    
    public static UserException handleExecutionException(String catalogName, String procedureName, 
                                                        Exception e) {
        // 记录详细错误日志
        LOG.error("Failed to execute procedure {} in catalog {}: {}", 
                 procedureName, catalogName, e.getMessage(), e);
        
        // 根据异常类型返回不同的用户友好错误信息
        if (e instanceof TableNotFoundException) {
            return new UserException(String.format(
                "Table not found when executing procedure %s", procedureName));
        } else if (e instanceof ValidationException) {
            return new UserException(String.format(
                "Invalid parameters for procedure %s: %s", procedureName, e.getMessage()));
        } else if (e instanceof AuthenticationException) {
            return new UserException(String.format(
                "Authentication failed when executing procedure %s", procedureName));
        } else {
            return new UserException(String.format(
                "Unexpected error executing procedure %s: %s", procedureName, e.getMessage()));
        }
    }
}
```

## 4. 性能优化策略

### 4.1 Procedure缓存机制
```java
public class ProcedureCache {
    private static final LoadingCache<String, Map<String, BaseProcedure>> procedureCache = 
        CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(1, TimeUnit.HOURS)
            .build(new CacheLoader<String, Map<String, BaseProcedure>>() {
                @Override
                public Map<String, BaseProcedure> load(String catalogName) throws Exception {
                    return loadProceduresFromCatalog(catalogName);
                }
            });
    
    public static Optional<BaseProcedure> getCachedProcedure(String catalogName, String procedureName) {
        try {
            Map<String, BaseProcedure> procedures = procedureCache.get(catalogName);
            return Optional.ofNullable(procedures.get(procedureName.toLowerCase()));
        } catch (Exception e) {
            LOG.warn("Failed to load procedures from cache for catalog: {}", catalogName, e);
            return Optional.empty();
        }
    }
    
    public static void invalidateCatalog(String catalogName) {
        procedureCache.invalidate(catalogName);
    }
}
```

### 4.2 异步执行支持
```java
public class AsyncProcedureExecutor {
    private static final ThreadPoolExecutor EXECUTOR = 
        ThreadPoolManager.newDaemonFixedThreadPoolExecutor(
            Config.max_procedure_execution_threads,
            Integer.MAX_VALUE,
            "procedure-execution-pool",
            true);
    
    public static Future<Void> executeAsync(BaseProcedure procedure, 
                                          List<Expression> args, ConnectContext ctx) {
        return EXECUTOR.submit(() -> {
            try {
                procedure.execute(args, ctx);
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
```

## 5. 测试策略

### 5.1 单元测试

#### 5.1.1 参数验证测试
```java
@Test
public void testParameterValidation() throws Exception {
    // 测试必需参数缺失
    BaseProcedure procedure = new TestProcedure();
    List<Expression> args = Lists.newArrayList(); // 空参数列表
    
    UserException exception = assertThrows(UserException.class, () -> {
        procedure.execute(args, mockConnectContext);
    });
    assertThat(exception.getMessage()).contains("Required parameter");
    
    // 测试类型不匹配
    args.add(new StringLiteral("invalid_number"));
    exception = assertThrows(UserException.class, () -> {
        procedure.execute(args, mockConnectContext);
    });
    assertThat(exception.getMessage()).contains("expects type");
}
```

#### 5.1.2 权限检查测试
```java
@Test
public void testPermissionCheck() throws Exception {
    // 模拟无权限用户
    ConnectContext ctx = new ConnectContext();
    ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("testuser", "%"));
    
    BaseProcedure procedure = new TestProcedure();
    List<Expression> args = createValidArgs();
    
    UserException exception = assertThrows(UserException.class, () -> {
        procedure.execute(args, ctx);
    });
    assertThat(exception.getMessage()).contains("has no privilege");
}
```

### 5.2 集成测试

#### 5.2.1 端到端测试
```java
@Test
public void testIcebergSnapshotProcedure() throws Exception {
    // 创建测试表
    String createTableSql = "CREATE TABLE iceberg_catalog.test_db.test_table (" +
                           "id BIGINT, name VARCHAR(50)) USING ICEBERG";
    sql(createTableSql);
    
    // 执行Procedure
    String procedureCall = "CALL iceberg_catalog.snapshot('test_db.test_table', 'append')";
    QueryResult result = sql(procedureCall);
    
    // 验证结果
    assertThat(result.isSuccess()).isTrue();
    
    // 验证snapshot确实被创建
    String showSnapshots = "SELECT * FROM iceberg_catalog.test_db.test_table.snapshots";
    QueryResult snapshots = sql(showSnapshots);
    assertThat(snapshots.getRows()).hasSize(1);
}
```

### 5.3 性能测试
```java
@Test
public void testProcedureExecutionPerformance() throws Exception {
    int threadCount = 10;
    int executionsPerThread = 100;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    
    long startTime = System.currentTimeMillis();
    
    List<Future<Void>> futures = new ArrayList<>();
    for (int i = 0; i < threadCount; i++) {
        futures.add(executor.submit(() -> {
            for (int j = 0; j < executionsPerThread; j++) {
                String sql = "CALL iceberg_catalog.snapshot('test_db.test_table_" + j + "', 'append')";
                sql(sql);
            }
            return null;
        }));
    }
    
    for (Future<Void> future : futures) {
        future.get();
    }
    
    long endTime = System.currentTimeMillis();
    long totalTime = endTime - startTime;
    double avgTime = totalTime / (double)(threadCount * executionsPerThread);
    
    assertThat(avgTime).isLessThan(1000); // 平均执行时间小于1秒
}
```

## 6. 部署和运维

### 6.1 配置参数
```java
// fe.conf配置项
public class Config {
    @ConfField
    public static boolean enable_external_catalog_procedures = true;
    
    @ConfField
    public static int max_procedure_execution_threads = 10;
    
    @ConfField
    public static int procedure_execution_timeout_sec = 300; // 5分钟
    
    @ConfField
    public static boolean enable_procedure_privilege_check = true;
    
    @ConfField
    public static int procedure_cache_size = 1000;
    
    @ConfField
    public static int procedure_cache_expire_hours = 1;
}
```

### 6.2 监控指标
```java
public class ProcedureMetrics {
    // 执行次数统计
    public static final Counter PROCEDURE_EXECUTION_COUNTER = 
        Counter.build()
            .name("doris_procedure_executions_total")
            .labelNames("catalog", "procedure", "status")
            .help("Total procedure executions")
            .register();
    
    // 执行时间统计
    public static final Histogram PROCEDURE_EXECUTION_DURATION = 
        Histogram.build()
            .name("doris_procedure_execution_duration_seconds")
            .labelNames("catalog", "procedure")
            .help("Procedure execution duration in seconds")
            .register();
    
    // 错误率统计
    public static final Gauge PROCEDURE_ERROR_RATE = 
        Gauge.build()
            .name("doris_procedure_error_rate")
            .labelNames("catalog")
            .help("Procedure execution error rate")
            .register();
}
```

## 7. 使用示例

### 7.1 Iceberg Procedure使用示例

```sql
-- 创建Iceberg catalog
CREATE CATALOG iceberg_catalog PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "hms",
    "hive.metastore.uris" = "thrift://localhost:9083"
);

-- 查看支持的Procedures
SHOW PROCEDURES FROM iceberg_catalog;

-- 执行snapshot操作
CALL iceberg_catalog.snapshot('sales_db.orders', 'append', '/path/to/data');

-- 回滚到指定snapshot
CALL iceberg_catalog.rollback_to_snapshot('sales_db.orders', 123456789);

-- 创建分支
CALL iceberg_catalog.create_branch('sales_db.orders', 'feature_branch', 123456789);

-- 过期旧snapshots
CALL iceberg_catalog.expire_snapshots('sales_db.orders', timestamp('2024-01-01 00:00:00'));

-- 清理孤儿文件
CALL iceberg_catalog.remove_orphan_files('sales_db.orders');
```

### 7.2 Paimon Procedure使用示例

```sql
-- 创建Paimon catalog
CREATE CATALOG paimon_catalog PROPERTIES (
    "type" = "paimon",
    "paimon.catalog.type" = "filesystem",
    "warehouse" = "hdfs://namenode:port/path/to/warehouse"
);

-- 查看支持的Procedures
SHOW PROCEDURES FROM paimon_catalog;

-- 压缩表文件
CALL paimon_catalog.compact('warehouse_db.inventory', 'dt=2024-01-01', 'zorder');

-- 创建标签
CALL paimon_catalog.create_tag('warehouse_db.inventory', 'release_v1.0', 123456789);

-- 过期snapshots
CALL paimon_catalog.expire_snapshots('warehouse_db.inventory', timestamp('2024-01-01 00:00:00'));
```

### 7.3 混合使用场景

```sql
-- 同时管理多个数据湖格式
USE iceberg_catalog;
CALL snapshot('sales_db.orders', 'append');

USE paimon_catalog;
CALL compact('warehouse_db.inventory');

-- 在同一个查询中使用不同catalog的表和procedures
SELECT COUNT(*) FROM iceberg_catalog.sales_db.orders;
CALL paimon_catalog.compact('warehouse_db.inventory');
SELECT COUNT(*) FROM paimon_catalog.warehouse_db.inventory;
```

## 8. 总结和展望

### 8.1 项目成果
本设计文档提出了一套完整的基于Doris的多数据湖格式Procedure系统，主要成果包括：

1. **统一架构**：基于现有CatalogIf接口扩展，实现了统一的Procedure调用框架
2. **多格式支持**：同时支持Iceberg和Paimon两种主流数据湖格式
3. **完整功能**：包含参数验证、权限检查、错误处理、性能优化等完整功能
4. **可扩展性**：采用插件化设计，可轻松扩展支持更多数据湖格式

### 8.2 技术创新点
1. **设计模式融合**：综合运用Strategy、Factory Method、Template Method等设计模式
2. **类型系统桥接**：建立了Doris类型系统与数据湖格式类型系统的映射关系
3. **权限集成**：与Doris现有权限系统完美集成
4. **性能优化**：提供缓存机制和异步执行支持

### 8.3 后续扩展方向
1. **更多数据湖格式**：支持Delta Lake、Hudi等其他数据湖格式
2. **更多Procedure**：扩展更多数据管理和分析Procedure
3. **可视化管理**：提供Web UI进行Procedure管理和监控
4. **自动化运维**：支持定时执行和自动化数据管理任务

### 8.4 实施建议
1. **分阶段实施**：先实现核心框架，再逐步添加具体Procedure
2. **充分测试**：重点关注多并发场景和异常处理
3. **监控完善**：建立完整的监控和告警机制
4. **文档完整**：提供详细的用户手册和开发指南

通过本设计方案的实施，Doris将具备完整的多数据湖格式Procedure调用能力，为用户提供统一、高效的数据湖管理体验。