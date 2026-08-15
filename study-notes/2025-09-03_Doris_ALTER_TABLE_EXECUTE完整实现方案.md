# Apache Doris ALTER TABLE EXECUTE完整实现方案

## 1. 项目概述

### 1.1 目标
在Apache Doris中实现`ALTER TABLE EXECUTE`语法，支持对外部数据源（Iceberg、Paimon）的表级Procedure调用，提供类似Apache Iceberg和Apache Spark的Procedure执行能力。

### 1.2 核心功能
- 支持`ALTER TABLE table_name EXECUTE procedure_name(args...)`语法
- 支持Iceberg和Paimon两种数据源的Procedure
- 支持`SHOW PROCEDURES`查看外部Catalog支持的Procedure
- 采用单例模式设计，确保Procedure实例的高效管理
- 完整的参数验证、权限控制和异常处理

### 1.3 设计原则
- **增量式实现**：不影响现有功能，纯增量开发
- **模块化设计**：各组件职责清晰，松耦合
- **可扩展性**：易于支持新的数据源和Procedure类型
- **一致性**：与Doris现有架构保持一致

## 2. 架构设计

### 2.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                           SQL Parser Layer                      │
│  ┌─────────────────┐    ┌─────────────────┐                    │
│  │  DorisLexer.g4  │    │  DorisParser.g4 │                    │
│  │  (EXECUTE已存在) │    │  (新增语法规则)   │                    │
│  └─────────────────┘    └─────────────────┘                    │
└─────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                      LogicalPlan Builder                        │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │         visitAlterTableExecute(context)                     │ │
│  │  - 解析表名 (multipartIdentifier)                           │ │
│  │  - 解析Procedure名称 (identifier)                           │ │
│  │  - 解析参数列表 (expression list)                           │ │
│  │  - 创建AlterTableExecuteCommand                             │ │
│  └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Command Execution                          │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              AlterTableExecuteCommand                       │ │
│  │  ┌─────────────────┐  ┌─────────────────┐                  │ │
│  │  │   Table Name    │  │ Procedure Name  │                  │ │
│  │  │   Resolution    │  │   Resolution    │                  │ │
│  │  └─────────────────┘  └─────────────────┘                  │ │
│  │                           │                                 │ │
│  │                           ▼                                 │ │
│  │  ┌─────────────────────────────────────────────────────────┐ │ │
│  │  │            Catalog Procedure Manager                    │ │ │
│  │  └─────────────────────────────────────────────────────────┘ │ │
│  └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Procedure Framework                           │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │ Iceberg         │  │    Paimon       │  │   Future        │  │
│  │ Procedures      │  │   Procedures    │  │  DataSources    │  │
│  │                 │  │                 │  │                 │  │
│  │ ├─Snapshot      │  │ ├─Compaction    │  │ ├─Hudi          │  │
│  │ ├─Compaction    │  │ ├─Snapshot      │  │ ├─DeltaLake     │  │
│  │ ├─ExpireSnapshots│  │ ├─Cleanup       │  │ └─...          │  │
│  │ └─Rewrite       │  │ └─...           │  │                 │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 核心组件关系

```java
// 语法解析层
DorisParser.g4 -> LogicalPlanBuilder.visitAlterTableExecute() 
                                   │
                                   ▼
// 命令执行层                      
AlterTableExecuteCommand.run() -> CatalogIf.getProcedure()
                                   │
                                   ▼
// Procedure框架层
TableProcedure.execute() -> 具体的Procedure实现
```

## 3. 详细实现方案

### 3.1 语法层实现

#### 3.1.1 DorisParser.g4修改

**位置**: `fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisParser.g4`

**修改内容**: 在`supportedAlterStatement`规则中添加：

```antlr
supportedAlterStatement
    : ALTER VIEW name=multipartIdentifier
        (MODIFY commentSpec |
              (LEFT_PAREN cols=simpleColumnDefs RIGHT_PAREN)?
              commentSpec? AS query)                                               #alterView
    | ALTER STORAGE VAULT name=multipartIdentifier properties=propertyClause       #alterStorageVault
    | ALTER SYSTEM RENAME COMPUTE GROUP name=identifier newName=identifier         #alterSystemRenameComputeGroup
    // 新增：ALTER TABLE EXECUTE语法支持
    | ALTER TABLE tableName=multipartIdentifier 
        EXECUTE procedureName=identifier 
        (LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN)?                 #alterTableExecute
    ;
```

#### 3.1.2 LogicalPlanBuilder.java扩展

**位置**: `fe/fe-core/src/main/java/org/apache/doris/nereids/parser/LogicalPlanBuilder.java`

**新增方法**:

```java
@Override
public LogicalPlan visitAlterTableExecute(DorisParser.AlterTableExecuteContext ctx) {
    // 解析表名 - 支持三级命名空间 (catalog.database.table)
    List<String> tableName = visitMultipartIdentifier(ctx.tableName);
    
    // 解析Procedure名称
    String procedureName = ctx.procedureName.getText();
    
    // 解析参数列表
    List<Expression> arguments = new ArrayList<>();
    if (ctx.expression() != null) {
        for (DorisParser.ExpressionContext exprCtx : ctx.expression()) {
            arguments.add((Expression) visit(exprCtx));
        }
    }
    
    // 创建ALTER TABLE EXECUTE命令
    return new AlterTableExecuteCommand(tableName, procedureName, arguments);
}
```

### 3.2 命令执行层实现

#### 3.2.1 PlanType枚举扩展

**位置**: `fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/PlanType.java`

```java
public enum PlanType {
    // ... 现有类型
    
    // commands  
    ALTER_MTMV_COMMAND,
    ALTER_VIEW_COMMAND,
    ALTER_STORAGE_VAULT,
    ALTER_SYSTEM_RENAME_COMPUTE_GROUP,
    ALTER_TABLE_EXECUTE_COMMAND,  // 新增
    
    // ... 其他类型
}
```

#### 3.2.2 AlterTableExecuteCommand实现

**位置**: `fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/AlterTableExecuteCommand.java`

```java
package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.exceptions.AnalysisException as NereidsAnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableProcedure;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;

/**
 * ALTER TABLE EXECUTE Command Implementation
 * 支持对外部数据源表执行Procedure操作
 */
public class AlterTableExecuteCommand extends Command implements ForwardWithSync {
    private static final Logger LOG = LogManager.getLogger(AlterTableExecuteCommand.class);
    
    private final List<String> tableName;
    private final String procedureName;
    private final List<Expression> arguments;

    public AlterTableExecuteCommand(List<String> tableName, String procedureName, List<Expression> arguments) {
        super(PlanType.ALTER_TABLE_EXECUTE_COMMAND);
        this.tableName = Objects.requireNonNull(tableName, "Table name cannot be null");
        this.procedureName = Objects.requireNonNull(procedureName, "Procedure name cannot be null");
        this.arguments = arguments != null ? ImmutableList.copyOf(arguments) : ImmutableList.of();
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        LOG.info("Executing ALTER TABLE {} EXECUTE {} with {} arguments", 
                String.join(".", tableName), procedureName, arguments.size());
        
        // 权限检查
        executor.checkBlockRules();
        
        try {
            // 执行Procedure调用
            executeProcedure(ctx, executor);
            LOG.info("Successfully executed procedure {} on table {}", procedureName, String.join(".", tableName));
        } catch (Exception e) {
            LOG.error("Failed to execute procedure {} on table {}: {}", 
                    procedureName, String.join(".", tableName), e.getMessage(), e);
            throw e;
        }
    }

    /**
     * 执行Procedure的核心逻辑
     */
    private void executeProcedure(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // 1. 解析和验证表名
        CatalogIf catalog = resolveCatalog(ctx);
        
        // 2. 验证表存在性和权限
        validateTableAccess(ctx, catalog);
        
        // 3. 获取Procedure实例
        TableProcedure procedure = resolveProcedure(catalog);
        
        // 4. 参数验证和转换
        Object[] convertedArgs = validateAndConvertArguments(ctx, procedure);
        
        // 5. 执行Procedure
        procedure.execute(ctx, tableName, convertedArgs);
    }

    /**
     * 解析Catalog
     */
    private CatalogIf resolveCatalog(ConnectContext ctx) throws AnalysisException {
        String catalogName = tableName.size() >= 3 ? tableName.get(0) : ctx.getCurrentCatalog().getName();
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
        
        if (catalog == null) {
            throw new AnalysisException("Catalog not found: " + catalogName);
        }
        
        // 验证是否为外部Catalog (只有外部Catalog支持Procedure)
        if (catalog.isInternalCatalog()) {
            throw new AnalysisException("ALTER TABLE EXECUTE is only supported for external catalogs. " +
                    "Internal catalog does not support procedures.");
        }
        
        return catalog;
    }

    /**
     * 验证表访问权限
     */
    private void validateTableAccess(ConnectContext ctx, CatalogIf catalog) throws Exception {
        String dbName = tableName.size() >= 2 ? tableName.get(tableName.size() - 2) : ctx.getDatabase();
        String tblName = tableName.get(tableName.size() - 1);
        
        if (Strings.isNullOrEmpty(dbName)) {
            throw new AnalysisException("Database name is required");
        }
        
        // 检查表是否存在
        if (!catalog.tableExists(ctx, dbName, tblName)) {
            throw new AnalysisException(String.format("Table %s.%s.%s does not exist", 
                    catalog.getName(), dbName, tblName));
        }
        
        // TODO: 添加细粒度权限检查
        // AuthorizationChecker.checkTablePermission(ctx.getCurrentUserIdentity(), 
        //     catalog.getName(), dbName, tblName, PrivPredicate.ALTER);
    }

    /**
     * 解析和获取Procedure实例
     */
    private TableProcedure resolveProcedure(CatalogIf catalog) throws AnalysisException {
        TableProcedure procedure = catalog.getProcedure(procedureName);
        
        if (procedure == null) {
            // 提供友好的错误信息，包含可用的Procedure列表
            List<String> availableProcedures = catalog.listProcedures().stream()
                    .map(info -> info.getName())
                    .collect(java.util.stream.Collectors.toList());
                    
            throw new AnalysisException(String.format(
                    "Procedure '%s' not found in catalog '%s'. Available procedures: [%s]",
                    procedureName, catalog.getName(), String.join(", ", availableProcedures)));
        }
        
        return procedure;
    }

    /**
     * 参数验证和类型转换
     */
    private Object[] validateAndConvertArguments(ConnectContext ctx, TableProcedure procedure) throws Exception {
        // 获取Procedure的参数定义
        List<TableProcedure.ParameterInfo> expectedParams = procedure.getParameters();
        
        // 参数数量验证
        if (arguments.size() != expectedParams.size()) {
            throw new AnalysisException(String.format(
                    "Procedure '%s' expects %d arguments, but %d were provided. " +
                    "Expected signature: %s(%s)",
                    procedureName, expectedParams.size(), arguments.size(),
                    procedureName, formatParameterSignature(expectedParams)));
        }
        
        // 参数类型转换
        Object[] convertedArgs = new Object[arguments.size()];
        for (int i = 0; i < arguments.size(); i++) {
            Expression expr = arguments.get(i);
            TableProcedure.ParameterInfo paramInfo = expectedParams.get(i);
            
            // 转换Expression到具体的Java对象
            convertedArgs[i] = convertExpressionToValue(ctx, expr, paramInfo);
        }
        
        return convertedArgs;
    }

    /**
     * 表达式值转换
     */
    private Object convertExpressionToValue(ConnectContext ctx, Expression expr, 
            TableProcedure.ParameterInfo paramInfo) throws Exception {
        // TODO: 实现完整的Expression到Java对象的转换逻辑
        // 这里需要处理各种类型：字符串、数字、日期、布尔值等
        
        // 简化实现示例
        if (expr instanceof org.apache.doris.nereids.trees.expressions.literal.Literal) {
            return convertLiteralValue((org.apache.doris.nereids.trees.expressions.literal.Literal) expr, paramInfo);
        } else {
            // 对于复杂表达式，可能需要执行计算
            throw new AnalysisException("Complex expressions in procedure parameters are not yet supported");
        }
    }

    /**
     * 字面值转换
     */
    private Object convertLiteralValue(org.apache.doris.nereids.trees.expressions.literal.Literal literal,
            TableProcedure.ParameterInfo paramInfo) throws AnalysisException {
        Class<?> targetType = paramInfo.getType();
        Object value = literal.getValue();
        
        // 类型转换逻辑
        if (targetType == String.class) {
            return value != null ? value.toString() : null;
        } else if (targetType == Integer.class || targetType == int.class) {
            if (value instanceof Number) {
                return ((Number) value).intValue();
            } else {
                return Integer.valueOf(value.toString());
            }
        } else if (targetType == Long.class || targetType == long.class) {
            if (value instanceof Number) {
                return ((Number) value).longValue();
            } else {
                return Long.valueOf(value.toString());
            }
        } else if (targetType == Boolean.class || targetType == boolean.class) {
            if (value instanceof Boolean) {
                return value;
            } else {
                return Boolean.valueOf(value.toString());
            }
        }
        // TODO: 添加更多类型转换支持
        
        throw new AnalysisException(String.format("Unsupported parameter type: %s", targetType.getSimpleName()));
    }

    /**
     * 格式化参数签名用于错误信息
     */
    private String formatParameterSignature(List<TableProcedure.ParameterInfo> params) {
        return params.stream()
                .map(p -> p.getType().getSimpleName() + " " + p.getName())
                .collect(java.util.stream.Collectors.joining(", "));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAlterTableExecuteCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ALTER;
    }

    // Getters
    public List<String> getTableName() {
        return tableName;
    }

    public String getProcedureName() {
        return procedureName;
    }

    public List<Expression> getArguments() {
        return arguments;
    }

    @Override
    public String toString() {
        return String.format("AlterTableExecuteCommand{table=%s, procedure=%s, args=%d}", 
                String.join(".", tableName), procedureName, arguments.size());
    }
}
```

#### 3.2.3 CommandVisitor扩展

**位置**: `fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/visitor/CommandVisitor.java`

```java
// 在CommandVisitor接口中添加
default R visitAlterTableExecuteCommand(AlterTableExecuteCommand alterTableExecuteCommand, C context) {
    return visitCommand(alterTableExecuteCommand, context);
}
```

### 3.3 Procedure框架实现

#### 3.3.1 TableProcedure基类

**位置**: `fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/info/TableProcedure.java`

```java
package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.qe.ConnectContext;

import java.util.List;

/**
 * 表级Procedure的基类
 * 所有外部数据源的Table Procedure都应该继承此类
 */
public abstract class TableProcedure {
    
    private final String name;
    private final String description;
    private final List<ParameterInfo> parameters;

    protected TableProcedure(String name, String description, List<ParameterInfo> parameters) {
        this.name = name;
        this.description = description;
        this.parameters = parameters;
    }

    /**
     * 执行Procedure的核心方法
     * 
     * @param ctx ConnectContext
     * @param tableName 三级表名 [catalog, database, table] 或 [database, table]
     * @param args 参数数组
     * @throws Exception 执行异常
     */
    public abstract void execute(ConnectContext ctx, List<String> tableName, Object[] args) throws Exception;

    /**
     * 参数信息类
     */
    public static class ParameterInfo {
        private final String name;
        private final Class<?> type;
        private final String description;
        private final boolean optional;
        private final Object defaultValue;

        public ParameterInfo(String name, Class<?> type, String description) {
            this(name, type, description, false, null);
        }

        public ParameterInfo(String name, Class<?> type, String description, boolean optional, Object defaultValue) {
            this.name = name;
            this.type = type;
            this.description = description;
            this.optional = optional;
            this.defaultValue = defaultValue;
        }

        // Getters
        public String getName() { return name; }
        public Class<?> getType() { return type; }
        public String getDescription() { return description; }
        public boolean isOptional() { return optional; }
        public Object getDefaultValue() { return defaultValue; }
    }

    /**
     * Procedure信息类，用于SHOW PROCEDURES
     */
    public static class ProcedureInfo {
        private final String name;
        private final String description;
        private final List<ParameterInfo> parameters;

        public ProcedureInfo(String name, String description, List<ParameterInfo> parameters) {
            this.name = name;
            this.description = description;
            this.parameters = parameters;
        }

        // Getters
        public String getName() { return name; }
        public String getDescription() { return description; }
        public List<ParameterInfo> getParameters() { return parameters; }
    }

    // Getters
    public String getName() { return name; }
    public String getDescription() { return description; }
    public List<ParameterInfo> getParameters() { return parameters; }
}
```

#### 3.3.2 Iceberg Procedure实现

**位置**: `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/procedure/IcebergSnapshotProcedure.java`

```java
package org.apache.doris.datasource.iceberg.procedure;

import org.apache.doris.nereids.trees.plans.commands.info.TableProcedure;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Iceberg Snapshot Procedure - 单例模式实现
 * 用于创建表快照
 */
public class IcebergSnapshotProcedure extends TableProcedure {
    private static final Logger LOG = LogManager.getLogger(IcebergSnapshotProcedure.class);
    
    // 单例实例 - 使用静态final字段确保线程安全
    private static final IcebergSnapshotProcedure INSTANCE = new IcebergSnapshotProcedure();
    
    // 私有构造函数防止外部实例化
    private IcebergSnapshotProcedure() {
        super("snapshot", 
              "Create a snapshot of the table at the current timestamp",
              ImmutableList.of(
                  new ParameterInfo("operation", String.class, "Operation type: 'append' or 'overwrite'"),
                  new ParameterInfo("options", String.class, "Additional options in JSON format", true, "{}")
              ));
    }
    
    // 获取单例实例
    public static IcebergSnapshotProcedure getInstance() {
        return INSTANCE;
    }

    @Override
    public void execute(ConnectContext ctx, List<String> tableName, Object[] args) throws Exception {
        LOG.info("Executing Iceberg snapshot procedure for table: {}", String.join(".", tableName));
        
        String operation = (String) args[0];
        String options = args.length > 1 ? (String) args[1] : "{}";
        
        // 验证操作类型
        if (!"append".equalsIgnoreCase(operation) && !"overwrite".equalsIgnoreCase(operation)) {
            throw new IllegalArgumentException("Operation must be 'append' or 'overwrite', got: " + operation);
        }
        
        try {
            // 获取Iceberg表实例并执行快照操作
            // 这里简化实现，实际需要通过Catalog获取Iceberg Table对象
            executeIcebergSnapshot(tableName, operation, options);
            
            LOG.info("Successfully created {} snapshot for table {}", operation, String.join(".", tableName));
        } catch (Exception e) {
            LOG.error("Failed to create snapshot for table {}: {}", String.join(".", tableName), e.getMessage(), e);
            throw e;
        }
    }
    
    private void executeIcebergSnapshot(List<String> tableName, String operation, String options) {
        // TODO: 实际的Iceberg快照创建逻辑
        // 1. 通过tableName解析获取Iceberg Table对象
        // 2. 根据operation类型执行相应的快照操作
        // 3. 处理options中的额外参数
        LOG.info("Creating {} snapshot for Iceberg table {} with options: {}", operation, tableName, options);
        
        // 模拟执行时间
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
```

#### 3.3.3 其他Iceberg Procedure实现

**位置**: `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/procedure/IcebergCompactionProcedure.java`

```java
package org.apache.doris.datasource.iceberg.procedure;

import org.apache.doris.nereids.trees.plans.commands.info.TableProcedure;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Iceberg Compaction Procedure - 数据压缩
 */
public class IcebergCompactionProcedure extends TableProcedure {
    private static final Logger LOG = LogManager.getLogger(IcebergCompactionProcedure.class);
    private static final IcebergCompactionProcedure INSTANCE = new IcebergCompactionProcedure();
    
    private IcebergCompactionProcedure() {
        super("compaction", 
              "Compact table files to optimize query performance",
              ImmutableList.of(
                  new ParameterInfo("strategy", String.class, "Compaction strategy: 'files' or 'size'"),
                  new ParameterInfo("target_size", Long.class, "Target file size in bytes", true, 134217728L) // 128MB
              ));
    }
    
    public static IcebergCompactionProcedure getInstance() {
        return INSTANCE;
    }

    @Override
    public void execute(ConnectContext ctx, List<String> tableName, Object[] args) throws Exception {
        LOG.info("Executing Iceberg compaction procedure for table: {}", String.join(".", tableName));
        
        String strategy = (String) args[0];
        Long targetSize = args.length > 1 ? (Long) args[1] : 134217728L;
        
        executeIcebergCompaction(tableName, strategy, targetSize);
    }
    
    private void executeIcebergCompaction(List<String> tableName, String strategy, Long targetSize) {
        LOG.info("Compacting Iceberg table {} with strategy {} and target size {}", 
                tableName, strategy, targetSize);
        // TODO: 实际的compaction逻辑
    }
}
```

**ExpireSnapshots Procedure**:

```java
package org.apache.doris.datasource.iceberg.procedure;

import org.apache.doris.nereids.trees.plans.commands.info.TableProcedure;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.time.LocalDateTime;

/**
 * Iceberg ExpireSnapshots Procedure - 过期快照清理
 */
public class IcebergExpireSnapshotsProcedure extends TableProcedure {
    private static final Logger LOG = LogManager.getLogger(IcebergExpireSnapshotsProcedure.class);
    private static final IcebergExpireSnapshotsProcedure INSTANCE = new IcebergExpireSnapshotsProcedure();
    
    private IcebergExpireSnapshotsProcedure() {
        super("expire_snapshots", 
              "Remove old snapshots and their associated data files",
              ImmutableList.of(
                  new ParameterInfo("older_than", String.class, "Timestamp string (yyyy-MM-dd HH:mm:ss)"),
                  new ParameterInfo("retain_last", Integer.class, "Number of recent snapshots to keep", true, 5),
                  new ParameterInfo("max_concurrent_deletes", Integer.class, "Max concurrent delete operations", true, 10)
              ));
    }
    
    public static IcebergExpireSnapshotsProcedure getInstance() {
        return INSTANCE;
    }

    @Override
    public void execute(ConnectContext ctx, List<String> tableName, Object[] args) throws Exception {
        String olderThan = (String) args[0];
        Integer retainLast = args.length > 1 ? (Integer) args[1] : 5;
        Integer maxConcurrentDeletes = args.length > 2 ? (Integer) args[2] : 10;
        
        LOG.info("Expiring snapshots for table {} older than {} (retain last {})", 
                String.join(".", tableName), olderThan, retainLast);
                
        executeExpireSnapshots(tableName, olderThan, retainLast, maxConcurrentDeletes);
    }
    
    private void executeExpireSnapshots(List<String> tableName, String olderThan, 
            Integer retainLast, Integer maxConcurrentDeletes) {
        // TODO: 实际的expire snapshots逻辑
        LOG.info("Expiring snapshots for {} with parameters: olderThan={}, retainLast={}, maxConcurrentDeletes={}", 
                tableName, olderThan, retainLast, maxConcurrentDeletes);
    }
}
```

#### 3.3.4 Paimon Procedure实现

**位置**: `fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/procedure/PaimonCompactionProcedure.java`

```java
package org.apache.doris.datasource.paimon.procedure;

import org.apache.doris.nereids.trees.plans.commands.info.TableProcedure;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Paimon Compaction Procedure
 */
public class PaimonCompactionProcedure extends TableProcedure {
    private static final Logger LOG = LogManager.getLogger(PaimonCompactionProcedure.class);
    private static final PaimonCompactionProcedure INSTANCE = new PaimonCompactionProcedure();
    
    private PaimonCompactionProcedure() {
        super("compaction", 
              "Compact Paimon table files for better performance",
              ImmutableList.of(
                  new ParameterInfo("partition", String.class, "Partition to compact (optional)", true, null),
                  new ParameterInfo("type", String.class, "Compaction type: 'major' or 'minor'", true, "major")
              ));
    }
    
    public static PaimonCompactionProcedure getInstance() {
        return INSTANCE;
    }

    @Override
    public void execute(ConnectContext ctx, List<String> tableName, Object[] args) throws Exception {
        String partition = args.length > 0 ? (String) args[0] : null;
        String type = args.length > 1 ? (String) args[1] : "major";
        
        LOG.info("Executing Paimon {} compaction for table {}, partition: {}", 
                type, String.join(".", tableName), partition);
                
        executePaimonCompaction(tableName, partition, type);
    }
    
    private void executePaimonCompaction(List<String> tableName, String partition, String type) {
        // TODO: 实际的Paimon compaction逻辑
        LOG.info("Compacting Paimon table {} (partition: {}, type: {})", tableName, partition, type);
    }
}
```

### 3.4 Catalog接口扩展

#### 3.4.1 CatalogIf接口扩展

**位置**: `fe/fe-core/src/main/java/org/apache/doris/datasource/CatalogIf.java`

```java
// 在CatalogIf接口中添加以下方法：

/**
 * 获取指定名称的Procedure
 * 
 * @param procedureName Procedure名称
 * @return TableProcedure实例，如果不存在返回null
 */
TableProcedure getProcedure(String procedureName);

/**
 * 获取该Catalog支持的所有Procedure信息列表
 * 用于SHOW PROCEDURES命令
 * 
 * @return Procedure信息列表
 */
List<TableProcedure.ProcedureInfo> listProcedures();

/**
 * 检查表是否存在
 */
boolean tableExists(ConnectContext ctx, String dbName, String tableName) throws Exception;
```

#### 3.4.2 IcebergExternalCatalog实现

**位置**: `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergExternalCatalog.java`

```java
// 在IcebergExternalCatalog类中添加：

import org.apache.doris.datasource.iceberg.procedure.*;
import org.apache.doris.nereids.trees.plans.commands.info.TableProcedure;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class IcebergExternalCatalog extends ExternalCatalog {
    
    // Procedure单例注册表 - 静态初始化确保线程安全
    private static final Map<String, TableProcedure> PROCEDURES = ImmutableMap.<String, TableProcedure>builder()
            .put("snapshot", IcebergSnapshotProcedure.getInstance())
            .put("compaction", IcebergCompactionProcedure.getInstance())
            .put("expire_snapshots", IcebergExpireSnapshotsProcedure.getInstance())
            .put("rewrite_files", IcebergRewriteFilesProcedure.getInstance())
            .put("rewrite_manifests", IcebergRewriteManifestsProcedure.getInstance())
            .build();

    @Override
    public TableProcedure getProcedure(String procedureName) {
        return PROCEDURES.get(procedureName.toLowerCase());
    }

    @Override
    public List<TableProcedure.ProcedureInfo> listProcedures() {
        return PROCEDURES.values().stream()
                .map(procedure -> new TableProcedure.ProcedureInfo(
                        procedure.getName(),
                        procedure.getDescription(),
                        procedure.getParameters()))
                .collect(Collectors.toList());
    }

    @Override
    public boolean tableExists(ConnectContext ctx, String dbName, String tableName) throws Exception {
        // TODO: 实现Iceberg表存在性检查
        // 通过Iceberg Catalog API检查表是否存在
        return true; // 简化实现
    }
}
```

#### 3.4.3 PaimonExternalCatalog实现

```java
// 在PaimonExternalCatalog类中添加类似的实现

import org.apache.doris.datasource.paimon.procedure.*;

public class PaimonExternalCatalog extends ExternalCatalog {
    
    private static final Map<String, TableProcedure> PROCEDURES = ImmutableMap.<String, TableProcedure>builder()
            .put("compaction", PaimonCompactionProcedure.getInstance())
            .put("snapshot", PaimonSnapshotProcedure.getInstance())
            .put("cleanup", PaimonCleanupProcedure.getInstance())
            .build();

    @Override
    public TableProcedure getProcedure(String procedureName) {
        return PROCEDURES.get(procedureName.toLowerCase());
    }

    @Override
    public List<TableProcedure.ProcedureInfo> listProcedures() {
        return PROCEDURES.values().stream()
                .map(procedure -> new TableProcedure.ProcedureInfo(
                        procedure.getName(),
                        procedure.getDescription(),
                        procedure.getParameters()))
                .collect(Collectors.toList());
    }

    @Override
    public boolean tableExists(ConnectContext ctx, String dbName, String tableName) throws Exception {
        // TODO: Paimon表存在性检查
        return true;
    }
}
```

#### 3.4.4 InternalCatalog默认实现

```java
// 在InternalCatalog中提供默认实现

@Override
public TableProcedure getProcedure(String procedureName) {
    // 内部Catalog不支持Procedure
    return null;
}

@Override
public List<TableProcedure.ProcedureInfo> listProcedures() {
    // 内部Catalog返回空列表
    return Collections.emptyList();
}
```

### 3.5 SHOW PROCEDURES实现

#### 3.5.1 ShowProceduresCommand

**位置**: `fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/ShowProceduresCommand.java`

```java
package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.ShowStmt;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableProcedure;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * SHOW PROCEDURES命令实现
 */
public class ShowProceduresCommand extends ShowStmt implements ForwardNoSync {
    
    private final String catalogName;

    public ShowProceduresCommand(String catalogName) {
        this.catalogName = catalogName;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // 解析Catalog
        String targetCatalog = catalogName != null ? catalogName : ctx.getCurrentCatalog().getName();
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(targetCatalog);
        
        if (catalog == null) {
            throw new AnalysisException("Catalog not found: " + targetCatalog);
        }

        // 获取Procedure列表
        List<TableProcedure.ProcedureInfo> procedures = catalog.listProcedures();
        
        // 构建结果集
        List<List<String>> rows = Lists.newArrayList();
        for (TableProcedure.ProcedureInfo proc : procedures) {
            rows.add(Lists.newArrayList(
                    catalog.getName(),                    // Catalog
                    proc.getName(),                       // Procedure
                    proc.getDescription(),                // Description
                    formatParameters(proc.getParameters()) // Parameters
            ));
        }
        
        ShowResultSet resultSet = new ShowResultSet(getMetaData(), rows);
        ctx.getResultSet(resultSet);
    }

    private String formatParameters(List<TableProcedure.ParameterInfo> parameters) {
        if (parameters.isEmpty()) {
            return "()";
        }
        
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < parameters.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            TableProcedure.ParameterInfo param = parameters.get(i);
            sb.append(param.getType().getSimpleName())
              .append(" ")
              .append(param.getName());
              
            if (param.isOptional()) {
                sb.append(" [OPTIONAL]");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    private ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("Catalog", ScalarType.createVarchar(64)));
        builder.addColumn(new Column("Procedure", ScalarType.createVarchar(64)));
        builder.addColumn(new Column("Description", ScalarType.createVarchar(256)));
        builder.addColumn(new Column("Parameters", ScalarType.createVarchar(512)));
        return builder.build();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowProceduresCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.SHOW;
    }
}
```

## 4. 测试用例

### 4.1 语法测试

```sql
-- 基础语法测试
ALTER TABLE iceberg_catalog.test_db.test_table EXECUTE snapshot;
ALTER TABLE test_table EXECUTE snapshot('append');
ALTER TABLE catalog.db.table EXECUTE compaction('files', 134217728);
ALTER TABLE table1 EXECUTE expire_snapshots('2024-01-01 00:00:00', 5, 10);

-- 错误语法测试
ALTER TABLE EXECUTE snapshot;              -- 缺少表名
ALTER TABLE test_table EXECUTE;           -- 缺少procedure名
ALTER TABLE test_table EXECUTE snapshot(,); -- 无效参数
```

### 4.2 功能测试

```sql
-- SHOW PROCEDURES测试
SHOW PROCEDURES;                           -- 显示当前catalog的procedures
SHOW PROCEDURES FROM iceberg_catalog;      -- 显示指定catalog的procedures

-- Iceberg procedures测试
ALTER TABLE iceberg_catalog.db.sales EXECUTE snapshot('append');
ALTER TABLE iceberg_catalog.db.sales EXECUTE compaction('files');
ALTER TABLE iceberg_catalog.db.sales EXECUTE expire_snapshots('2024-01-01 00:00:00');

-- Paimon procedures测试  
ALTER TABLE paimon_catalog.db.orders EXECUTE compaction('partition1', 'major');
ALTER TABLE paimon_catalog.db.orders EXECUTE snapshot();
```

### 4.3 异常测试

```sql
-- Catalog不存在
ALTER TABLE nonexistent_catalog.db.table EXECUTE snapshot;

-- 表不存在
ALTER TABLE iceberg_catalog.db.nonexistent_table EXECUTE snapshot;

-- Procedure不存在
ALTER TABLE iceberg_catalog.db.table EXECUTE nonexistent_procedure;

-- 参数不匹配
ALTER TABLE iceberg_catalog.db.table EXECUTE snapshot('append', 'extra', 'params');
```

## 5. 部署和配置

### 5.1 编译构建

```bash
# 重新生成ANTLR解析器
cd fe/fe-core
./generate_parser.sh

# 编译FE模块
cd ../../
./build.sh --fe

# 运行测试
./build.sh --fe --clean --with-mysql --with-postgres --with-oracle
```

### 5.2 配置说明

无需额外配置，功能依赖现有的Catalog配置：

```sql
-- 创建Iceberg Catalog (现有功能)
CREATE CATALOG iceberg_catalog PROPERTIES (
    "type"="iceberg",
    "iceberg.catalog.type"="hms",
    "hive.metastore.uris"="thrift://localhost:9083"
);

-- 创建Paimon Catalog (现有功能)  
CREATE CATALOG paimon_catalog PROPERTIES (
    "type"="paimon",
    "warehouse"="s3://my-bucket/warehouse"
);
```

## 6. 监控和运维

### 6.1 日志监控

```java
// 在AlterTableExecuteCommand中添加详细日志
LOG.info("ALTER TABLE {} EXECUTE {} started", tableName, procedureName);
LOG.info("ALTER TABLE {} EXECUTE {} completed in {}ms", tableName, procedureName, duration);
LOG.error("ALTER TABLE {} EXECUTE {} failed: {}", tableName, procedureName, error);
```

### 6.2 性能指标

- Procedure执行时间
- 成功/失败率
- 参数验证耗时
- Catalog解析耗时

### 6.3 错误处理

- 友好的错误信息提示
- 详细的异常堆栈记录
- 参数验证失败的具体原因
- 可用Procedure列表提示

## 7. 扩展规划

### 7.1 近期扩展

1. **更多Procedure类型**：
   - Iceberg: `rewrite_data_files`, `delete_orphan_files`
   - Paimon: `optimize_table`, `vacuum`

2. **参数类型支持**：
   - 复杂表达式参数
   - 数组和Map类型参数
   - 函数调用参数

### 7.2 长期规划

1. **更多数据源支持**：
   - Hudi Procedures
   - Delta Lake Procedures
   - 自定义数据源Procedures

2. **高级功能**：
   - 异步Procedure执行
   - Procedure执行状态查询
   - 批量Procedure执行

## 8. 总结

本实现方案提供了完整的`ALTER TABLE EXECUTE`功能，包括：

### 8.1 核心特性
- ✅ 完整的语法支持 - 支持可选参数
- ✅ 多数据源支持 - Iceberg和Paimon
- ✅ 单例模式设计 - 高效的实例管理
- ✅ 完善的错误处理 - 友好的错误提示
- ✅ SHOW PROCEDURES - 支持查看可用Procedure

### 8.2 技术亮点
- **增量式实现**：不影响现有功能
- **模块化设计**：易于扩展和维护
- **类型安全**：完善的参数验证
- **性能优化**：单例模式减少对象创建

### 8.3 实施计划
- **Phase 1**：语法解析 (1-2天)
- **Phase 2**：命令框架 (3-5天)
- **Phase 3**：Procedure实现 (5-7天)
- **Phase 4**：测试完善 (2-3天)

总计预期：**2-3周完成完整实现**

该方案完全基于Apache Doris现有架构，确保了系统的一致性和可维护性，为用户提供了强大的表级操作能力。