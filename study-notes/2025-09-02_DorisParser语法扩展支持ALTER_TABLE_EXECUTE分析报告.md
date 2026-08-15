# DorisParser语法扩展支持ALTER TABLE EXECUTE分析报告

## 1. 语法文件分析结果

### 1.1 当前语法结构分析

通过分析 `/Users/xiaowenli/kevin/workspace/doris/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisParser.g4` 文件，发现以下关键信息：

#### 1.1.1 现有的ALTER TABLE语法位置
当前ALTER TABLE相关的语法规则位于 `unsupportedAlterStatement` 中：

```antlr
unsupportedAlterStatement
    : ALTER TABLE tableName=multipartIdentifier
        alterTableClause (COMMA alterTableClause)*                                  #alterTable
    | ALTER TABLE tableName=multipartIdentifier ADD ROLLUP
        addRollupClause (COMMA addRollupClause)*                                    #alterTableAddRollup
    | ALTER TABLE tableName=multipartIdentifier DROP ROLLUP
        dropRollupClause (COMMA dropRollupClause)*                                  #alterTableDropRollup
    // ... 其他ALTER TABLE变体
```

#### 1.1.2 EXECUTE关键字已存在
在DorisLexer.g4中已经定义了EXECUTE关键字：
```antlr
EXECUTE: 'EXECUTE';
```

#### 1.1.3 支持的ALTER语句结构
`supportedAlterStatement` 目前只包含：
```antlr
supportedAlterStatement
    : ALTER VIEW name=multipartIdentifier ...    #alterView
    | ALTER STORAGE VAULT ...                     #alterStorageVault  
    | ALTER SYSTEM RENAME COMPUTE GROUP ...       #alterSystemRenameComputeGroup
```

## 2. 语法扩展方案

### 2.1 需要修改的文件

1. **DorisParser.g4** - 添加新的语法规则
2. 不需要修改DorisLexer.g4 - EXECUTE关键字已存在

### 2.2 具体语法规则修改

#### 2.2.1 在supportedAlterStatement中添加新规则

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

#### 2.2.2 语法规则说明

新增的语法规则 `#alterTableExecute` 支持以下语法格式：

1. **基本格式**：
   ```sql
   ALTER TABLE table_name EXECUTE procedure_name
   ```

2. **带参数格式**：
   ```sql  
   ALTER TABLE table_name EXECUTE procedure_name(arg1, arg2, ...)
   ```

3. **无参数但有括号格式**：
   ```sql
   ALTER TABLE table_name EXECUTE procedure_name()
   ```

### 2.3 语法规则解析

- `tableName=multipartIdentifier`：支持三级命名空间的表名（catalog.database.table）
- `procedureName=identifier`：Procedure名称
- `(LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN)?`：可选的参数列表
  - 外层`?`表示整个括号部分是可选的
  - `(expression (COMMA expression)*)?`表示参数列表可以为空
  - 支持任意数量的表达式参数，以逗号分隔

## 3. 与现有语法的兼容性分析

### 3.1 关键字冲突检查

- `ALTER` - 已存在，无冲突
- `TABLE` - 已存在，无冲突  
- `EXECUTE` - 已存在于lexer中，无冲突
- `LEFT_PAREN`, `RIGHT_PAREN`, `COMMA` - 标准符号，无冲突

### 3.2 语法优先级分析

新语法规则添加在`supportedAlterStatement`中，与现有的unsupported规则分离，不会产生语法冲突。

### 3.3 解析器生成影响

添加新规则后，ANTLR会生成新的解析方法：
- `AlterTableExecuteContext` 上下文类
- 对应的visitor/listener方法

## 4. 实现建议

### 4.1 语法文件修改步骤

1. **修改DorisParser.g4**：
   ```antlr
   // 在supportedAlterStatement规则末尾添加
   | ALTER TABLE tableName=multipartIdentifier 
     EXECUTE procedureName=identifier 
     (LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN)?  #alterTableExecute
   ```

2. **重新生成解析器**：
   ```bash
   # 在doris项目根目录执行
   ./build.sh --fe
   ```

### 4.2 对应的Java代码实现

生成的解析器会创建`AlterTableExecuteContext`，需要在相应的visitor中处理：

```java
// DorisParserBaseVisitor的实现中需要重写：
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

### 4.3 后续实现需要的类

1. **AlterTableExecuteCommand** - 逻辑计划节点
2. **AlterTableExecuteAnalyzer** - 语义分析器  
3. **AlterTableExecuteExecutor** - 执行器
4. **相关的异常处理类**

## 5. 测试用例设计

### 5.1 正确语法测试

```sql
-- 基本语法
ALTER TABLE test_table EXECUTE snapshot;

-- 带参数语法
ALTER TABLE catalog.db.table EXECUTE snapshot('append', 'condition');

-- 空参数语法
ALTER TABLE table_name EXECUTE compact();

-- 复杂参数语法
ALTER TABLE iceberg.sales.orders EXECUTE expire_snapshots(
    timestamp('2024-01-01 00:00:00'), 
    5, 
    86400000
);
```

### 5.2 错误语法测试

```sql
-- 缺少表名
ALTER TABLE EXECUTE snapshot;  -- 应该报错

-- 缺少procedure名称  
ALTER TABLE test_table EXECUTE;  -- 应该报错

-- 无效的参数语法
ALTER TABLE test_table EXECUTE snapshot(,);  -- 应该报错
```

## 6. 语法扩展的影响范围

### 6.1 编译影响

- 需要重新编译FE模块
- ANTLR生成的解析器代码会更新
- 可能影响现有的单元测试

### 6.2 向后兼容性

- 新语法是纯增量的，不会影响现有SQL的解析
- 现有ALTER TABLE语句保持不变
- 只是将新语法从unsupported移动到supported

### 6.3 性能影响

- 解析器性能基本无影响（增加一个可选规则）
- 内存使用略微增加（新增context对象）

## 7. 实施计划

### 7.1 第一阶段：语法支持

1. 修改DorisParser.g4文件
2. 重新生成解析器代码
3. 验证基本语法解析正确性

### 7.2 第二阶段：语义分析

1. 实现AlterTableExecuteCommand逻辑计划节点
2. 实现对应的分析器和校验逻辑
3. 集成到现有的分析流程中

### 7.3 第三阶段：执行实现

1. 实现具体的执行逻辑
2. 集成Procedure调用框架
3. 完善错误处理和日志

### 7.4 第四阶段：测试和优化

1. 编写完整的测试用例
2. 性能测试和优化
3. 文档更新

## 8. 总结

通过分析DorisParser.g4语法文件，发现：

### 8.1 修改需求明确

- **只需修改DorisParser.g4**：添加一条新的语法规则
- **EXECUTE关键字已存在**：无需修改lexer
- **修改位置明确**：在supportedAlterStatement中添加

### 8.2 实现复杂度评估

- **语法扩展**：简单（1条规则）
- **解析器变更**：自动生成
- **兼容性风险**：极低
- **实现工作量**：中等（主要在语义分析和执行层面）

### 8.3 推荐的语法规则

```antlr  
| ALTER TABLE tableName=multipartIdentifier 
  EXECUTE procedureName=identifier 
  (LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN)?  #alterTableExecute
```

这个语法规则简洁、清晰，完全支持目标语法格式，与现有语法兼容，是理想的实现方案。