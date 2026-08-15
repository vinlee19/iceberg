# Nereids框架ALTER TABLE EXECUTE支持所需代码修改分析

## 1. 前置分析总结

基于之前对DorisParser.g4语法文件的分析，我们已经确定了语法层面的修改需求：

### 1.1 语法文件修改（已分析）
- **DorisParser.g4**：需要在`supportedAlterStatement`中添加新语法规则
- **DorisLexer.g4**：无需修改，`EXECUTE`关键字已存在

### 1.2 新语法规则
```antlr
| ALTER TABLE tableName=multipartIdentifier 
  EXECUTE procedureName=identifier 
  (LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN)?  #alterTableExecute
```

## 2. Nereids框架核心组件分析

### 2.1 LogicalPlanBuilder.java 修改需求

**文件位置**: `/Users/xiaowenli/kevin/workspace/doris/fe/fe-core/src/main/java/org/apache/doris/nereids/parser/LogicalPlanBuilder.java`

**分析结果**:
- LogicalPlanBuilder继承DorisParserBaseVisitor，负责将ANTLR解析结果转换为逻辑计划节点
- 当前已有多个ALTER相关的visitor方法：
  - `visitAlterMTMV()` - 处理MTMV变更
  - `visitAlterView()` - 处理视图变更  
  - `visitAlterStorageVault()` - 处理存储保险库变更
  - `visitAlterSystemRenameComputeGroup()` - 处理计算组重命名

**需要新增的方法**:
```java
@Override
public LogicalPlan visitAlterTableExecute(DorisParser.AlterTableExecuteContext ctx) {
    List<String> tableName = visitMultipartIdentifier(ctx.tableName);
    String procedureName = ctx.procedureName.getText();
    
    List<Expression> arguments = new ArrayList<>();
    if (ctx.expression() != null) {
        for (DorisParser.ExpressionContext exprCtx : ctx.expression()) {
            arguments.add((Expression) visit(exprCtx));
        }
    }
    
    return new AlterTableExecuteCommand(tableName, procedureName, arguments);
}
```

### 2.2 Command类体系结构分析

**核心基类**: `Command.java`
- 位置: `/Users/xiaowenli/kevin/workspace/doris/fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/Command.java`
- 所有DDL和DML命令的基类
- 核心方法: `public abstract void run(ConnectContext ctx, StmtExecutor executor) throws Exception`

**现有ALTER相关Command类**:
1. **AlterViewCommand.java** - ALTER VIEW命令实现
2. **AlterStorageVaultCommand.java** - ALTER STORAGE VAULT命令实现  
3. **AlterSystemRenameComputeGroupCommand.java** - ALTER SYSTEM命令实现
4. **AlterMTMVCommand.java** - ALTER MTMV命令实现

**需要新增的Command类**: `AlterTableExecuteCommand.java`

### 2.3 PlanType枚举扩展

**文件位置**: `/Users/xiaowenli/kevin/workspace/doris/fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/PlanType.java`

**当前ALTER相关类型**:
```java
ALTER_MTMV_COMMAND,
ALTER_VIEW_COMMAND, 
ALTER_STORAGE_VAULT,
ALTER_SYSTEM_RENAME_COMPUTE_GROUP,
```

**需要新增**:
```java
ALTER_TABLE_EXECUTE_COMMAND,
```

### 2.4 CommandVisitor接口扩展

**文件位置**: `/Users/xiaowenli/kevin/workspace/doris/fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/visitor/CommandVisitor.java`

**当前ALTER相关visitor方法**:
- `visitAlterMTMVCommand()`
- `visitAlterViewCommand()`  

**需要新增**:
```java
default R visitAlterTableExecuteCommand(AlterTableExecuteCommand alterTableExecuteCommand, C context) {
    return visitCommand(alterTableExecuteCommand, context);
}
```

## 3. 具体实现需求

### 3.1 新增AlterTableExecuteCommand类

**文件位置**: `/Users/xiaowenli/kevin/workspace/doris/fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/AlterTableExecuteCommand.java`

**基本结构**:
```java
public class AlterTableExecuteCommand extends Command implements ForwardWithSync {
    private final List<String> tableName;
    private final String procedureName;
    private final List<Expression> arguments;
    
    public AlterTableExecuteCommand(List<String> tableName, String procedureName, List<Expression> arguments) {
        super(PlanType.ALTER_TABLE_EXECUTE_COMMAND);
        this.tableName = tableName;
        this.procedureName = procedureName;
        this.arguments = arguments;
    }
    
    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // 实现procedure调用逻辑
        executeProcedure(ctx, executor);
    }
    
    private void executeProcedure(ConnectContext ctx, StmtExecutor executor) {
        // 1. 解析表名，获取Catalog
        // 2. 从Catalog获取Procedure实例
        // 3. 验证参数
        // 4. 执行Procedure
    }
}
```

### 3.2 核心执行逻辑设计

```java
private void executeProcedure(ConnectContext ctx, StmtExecutor executor) throws Exception {
    // 1. 解析三级表名 (catalog.database.table)
    CatalogIf catalog = getCatalog(tableName);
    
    // 2. 获取Procedure实例 (基于之前的单例设计)
    TableProcedure procedure = catalog.getProcedure(procedureName);
    if (procedure == null) {
        throw new AnalysisException("Procedure not found: " + procedureName);
    }
    
    // 3. 参数验证和转换
    Object[] params = validateAndConvertArguments(arguments, procedure);
    
    // 4. 执行Procedure
    procedure.execute(ctx, tableName, params);
}
```

### 3.3 依赖的接口扩展

**CatalogIf接口需要新增方法**:
```java
// 获取指定名称的Procedure
TableProcedure getProcedure(String procedureName);

// 获取所有支持的Procedure列表 (for SHOW PROCEDURES)
List<ProcedureInfo> listProcedures();
```

## 4. 文件修改清单

### 4.1 必须修改的文件

1. **语法解析层**:
   - `DorisParser.g4` - 添加新语法规则 ✅ (已分析)
   - `LogicalPlanBuilder.java` - 添加visitAlterTableExecute方法

2. **计划节点层**:
   - `PlanType.java` - 添加ALTER_TABLE_EXECUTE_COMMAND枚举值
   - `AlterTableExecuteCommand.java` - 新增命令类 (需创建)

3. **访问者模式层**:
   - `CommandVisitor.java` - 添加visitAlterTableExecuteCommand方法

4. **数据源接口层**:
   - `CatalogIf.java` - 添加Procedure相关接口方法
   - `ExternalCatalog.java` - 实现Procedure相关接口方法

### 4.2 可能需要修改的文件

1. **异常处理**:
   - 可能需要定义新的异常类处理Procedure执行错误

2. **权限控制**:
   - 可能需要在权限系统中添加ALTER TABLE EXECUTE相关的权限检查

3. **日志和监控**:
   - 可能需要添加Procedure执行的日志记录和监控指标

## 5. 实现复杂度分析

### 5.1 核心实现复杂度

1. **语法解析** - 简单 (已分析，只需1条规则)
2. **LogicalPlanBuilder** - 简单 (参考现有ALTER方法)
3. **Command实现** - 中等 (需要实现核心执行逻辑)
4. **Visitor扩展** - 简单 (模板化代码)
5. **接口扩展** - 中等 (需要在多个Catalog实现类中添加方法)

### 5.2 关键技术难点

1. **参数类型转换**: Expression到Java对象的转换
2. **异常处理**: Procedure执行失败时的错误处理
3. **事务管理**: 确保Procedure执行的事务一致性
4. **权限验证**: 确保用户有权限执行特定Procedure

## 6. 实施建议

### 6.1 开发顺序

1. **第一阶段**: 语法解析支持
   - 修改DorisParser.g4
   - 添加LogicalPlanBuilder.visitAlterTableExecute方法
   - 重新生成parser代码并验证语法解析正确

2. **第二阶段**: 命令框架搭建
   - 新增PlanType枚举值
   - 实现AlterTableExecuteCommand基本框架
   - 添加CommandVisitor支持

3. **第三阶段**: 核心执行逻辑
   - 实现具体的Procedure调用逻辑
   - 扩展Catalog接口
   - 添加异常处理

4. **第四阶段**: 测试和完善
   - 编写测试用例
   - 完善错误处理
   - 性能优化

### 6.2 风险评估

**低风险**:
- 语法规则添加 (纯增量，不影响现有功能)
- Command类实现 (独立的新功能)

**中等风险**:
- CatalogIf接口扩展 (需要所有实现类都添加方法)
- 参数转换逻辑 (类型转换可能出错)

**需要关注**:
- ANTLR重新生成可能影响现有解析器
- 新增Command的执行路径需要确保不破坏现有流程

## 7. 总结

通过对Nereids框架的深入分析，我们确定了支持ALTER TABLE EXECUTE语法需要修改的核心组件：

### 7.1 核心修改点
1. **LogicalPlanBuilder** - 添加新的visitor方法
2. **AlterTableExecuteCommand** - 新增命令实现类 
3. **PlanType** - 添加新的计划类型枚举
4. **CommandVisitor** - 扩展访问者接口
5. **CatalogIf** - 扩展数据源接口

### 7.2 实现策略
- **增量式开发**: 所有修改都是增量的，不影响现有功能
- **模块化设计**: 新功能独立实现，减少与现有代码的耦合
- **参考现有实现**: 大量参考现有ALTER命令的实现模式

### 7.3 预期工作量
- **语法层面**: 1-2天 (简单)
- **框架集成**: 3-5天 (中等)
- **核心逻辑**: 5-7天 (复杂)
- **测试完善**: 2-3天 (中等)

总计预期工作量：**2-3周**

这个实现方案完全基于Nereids框架的现有架构，确保了与现有系统的兼容性，同时为ALTER TABLE EXECUTE功能提供了完整的技术实现路径。