# Apache Iceberg Catalog架构深度源码分析报告（第一部分）

> **文档版本**: v1.0
> **分析版本**: Apache Iceberg 1.10.x
> **生成时间**: 2025-11-02
> **分析深度**: 源码级完整分析
> **文档说明**: 本文档深入分析Apache Iceberg的四种Catalog实现（HadoopCatalog、HiveCatalog、RESTCatalog、NessieCatalog），包含完整的源码剖析、架构设计、物理结构图和实现流程

---

## 目录（第一部分）

1. [概述与架构总览](#1-概述与架构总览)
2. [核心接口与基础架构](#2-核心接口与基础架构)
3. [HadoopCatalog深度源码分析](#3-hadoopcatalog深度源码分析)
4. [HiveCatalog深度源码分析](#4-hivecatalog深度源码分析)

---

## 1. 概述与架构总览

### 1.1 Catalog在Iceberg中的定位

Apache Iceberg是一个开放的表格式（Table Format），而Catalog是Iceberg架构中负责**表元数据管理**的核心组件。Catalog的主要职责包括：

1. **表的CRUD操作**：创建、读取、更新、删除表
2. **命名空间管理**：管理数据库/Schema层级结构
3. **表元数据存储**：持久化表的Schema、分区规范、快照等元数据
4. **并发控制**：保证多个写入者对表元数据的原子性修改
5. **表发现与枚举**：列出命名空间下的所有表

### 1.2 四种Catalog实现对比

Apache Iceberg官方提供了四种主要的Catalog实现，每种适用于不同的使用场景：

| Catalog类型 | 存储机制 | 适用场景 | 并发控制 | 优点 | 缺点 |
|------------|---------|---------|---------|------|------|
| **HadoopCatalog** | 文件系统目录结构 | 测试、小规模部署 | 文件系统原子重命名 + LockManager | 简单、无外部依赖 | 性能较差、不支持rename |
| **HiveCatalog** | Hive Metastore (RDBMS) | 企业级生产环境 | HMS事务 + 文件锁 | 成熟稳定、支持视图 | 依赖HMS、配置复杂 |
| **RESTCatalog** | HTTP REST API | 云原生、多租户 | 服务端控制 | 灵活、适合云环境 | 需要REST服务器 |
| **NessieCatalog** | Git-like版本控制 | 数据湖多版本管理 | Nessie事务 | 版本控制、分支管理 | 相对新、GC需手动 |

### 1.3 Catalog架构UML类图

```
                           ┌─────────────────┐
                           │   <<interface>> │
                           │     Catalog     │
                           │                 │
                           │ + initialize()  │
                           │ + listTables()  │
                           │ + loadTable()   │
                           │ + createTable() │
                           │ + dropTable()   │
                           │ + renameTable() │
                           └────────▲────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │                               │
         ┌──────────┴──────────┐       ┌──────────┴──────────┐
         │  <<abstract>>       │       │                     │
         │ BaseMetastoreCatalog│       │   RESTCatalog       │
         │                     │       │   (委托模式)         │
         │ + loadTable()       │       │                     │
         │ + newTableOps()     │       └─────────────────────┘
         │ + defaultLocation() │
         └──────────▲──────────┘
                    │
        ┌───────────┼───────────┐
        │           │           │
   ┌────┴────┐ ┌───┴─────┐ ┌──┴─────┐
   │ Hadoop  │ │  Hive   │ │ Nessie │
   │ Catalog │ │ Catalog │ │Catalog │
   └─────────┘ └─────────┘ └────────┘
```

**关键设计模式**：
- **模板方法模式**：BaseMetastoreCatalog定义了loadTable()的骨架，子类实现newTableOps()
- **委托模式**：RESTCatalog将所有操作委托给RESTSessionCatalog
- **策略模式**：不同的Catalog使用不同的存储策略

### 1.4 TableOperations层架构

每个Catalog都有对应的TableOperations实现，负责底层元数据的原子操作：

```
                      ┌──────────────────────┐
                      │   <<interface>>      │
                      │  TableOperations     │
                      │                      │
                      │ + current()          │
                      │ + refresh()          │
                      │ + commit(base, new)  │
                      └──────────▲───────────┘
                                 │
                  ┌──────────────┴──────────────┐
                  │                             │
    ┌─────────────┴────────────┐   ┌──────────┴─────────┐
    │  <<abstract>>            │   │                    │
    │ BaseMetastoreTable       │   │  HadoopTable       │
    │ Operations               │   │  Operations        │
    │                          │   │  (文件系统)         │
    │ + doRefresh()            │   └────────────────────┘
    │ + doCommit()             │
    └─────────────▲────────────┘
                  │
        ┌─────────┴─────────┐
        │                   │
   ┌────┴─────┐      ┌─────┴──────┐
   │  Hive    │      │  Nessie    │
   │  Table   │      │  Table     │
   │  Ops     │      │  Ops       │
   └──────────┘      └────────────┘
```

---

## 2. 核心接口与基础架构

### 2.1 Catalog接口定义

**源码位置**: `api/src/main/java/org/apache/iceberg/catalog/Catalog.java`

```java
package org.apache.iceberg.catalog;

/**
 * Catalog接口是所有Iceberg catalog实现的基础契约
 * 定义了表和命名空间管理的核心操作
 */
public interface Catalog {

  /**
   * 初始化catalog，传入配置参数
   * @param name catalog名称
   * @param properties 配置属性Map
   */
  default void initialize(String name, Map<String, String> properties) {}

  /**
   * 返回catalog的名称
   */
  String name();

  /**
   * 列出命名空间下的所有表
   * @param namespace 命名空间（数据库/schema）
   * @return 表标识符列表
   */
  List<TableIdentifier> listTables(Namespace namespace);

  /**
   * 加载表，返回Table对象
   * @param identifier 表标识符
   * @return Table对象，包含当前元数据
   * @throws NoSuchTableException 表不存在时抛出
   */
  Table loadTable(TableIdentifier identifier);

  /**
   * 删除表
   * @param identifier 表标识符
   * @param purge 是否彻底删除数据文件
   * @return 是否成功删除
   */
  boolean dropTable(TableIdentifier identifier, boolean purge);

  /**
   * 重命名表
   * @param from 原表标识符
   * @param to 新表标识符
   * @throws NoSuchTableException 原表不存在时抛出
   */
  void renameTable(TableIdentifier from, TableIdentifier to);

  /**
   * 构建器模式创建表
   * @param identifier 表标识符
   * @param schema 表Schema
   * @return TableBuilder构建器
   */
  default TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new TableBuilder() {
      // 建造者模式实现
    };
  }

  /**
   * TableBuilder内部接口：用于流式构建表
   */
  interface TableBuilder {
    TableBuilder withPartitionSpec(PartitionSpec spec);
    TableBuilder withSortOrder(SortOrder sortOrder);
    TableBuilder withLocation(String location);
    TableBuilder withProperties(Map<String, String> properties);
    TableBuilder withProperty(String key, String value);

    Table create();                         // 创建新表
    Transaction createTransaction();        // 创建表事务
    Transaction replaceTransaction();       // 替换表事务
    Transaction createOrReplaceTransaction(); // 创建或替换表事务
  }
}
```

**关键设计点**：

1. **命名空间层级结构**：支持多级命名空间（如`namespace1.namespace2`）
2. **TableIdentifier**：由Namespace + 表名组成的唯一标识
3. **建造者模式**：通过TableBuilder流式构建复杂表结构
4. **事务支持**：通过Transaction接口支持多操作原子提交

### 2.2 BaseMetastoreCatalog抽象基类

**源码位置**: `core/src/main/java/org/apache/iceberg/BaseMetastoreCatalog.java`

这是HadoopCatalog、HiveCatalog、NessieCatalog的共同父类，提供了**模板方法模式**的核心实现。

```java
public abstract class BaseMetastoreCatalog implements Catalog, Closeable {

  private String name;
  private Map<String, String> properties;

  /**
   * 模板方法：加载表的标准流程
   */
  @Override
  public Table loadTable(TableIdentifier identifier) {
    Table result;

    // 1. 验证表标识符是否有效
    if (isValidIdentifier(identifier)) {
      // 2. 创建对应的TableOperations（由子类实现）
      TableOperations ops = newTableOps(identifier);

      // 3. 检查是否为元数据表（如snapshots表、files表）
      if (ops.current() == null) {
        if (isValidMetadataIdentifier(identifier)) {
          result = loadMetadataTable(identifier);
        } else {
          throw new NoSuchTableException("Table does not exist: %s", identifier);
        }
      } else {
        // 4. 创建BaseTable包装TableOperations
        result = new BaseTable(ops, fullTableName(name(), identifier), metricsReporter());
      }
    } else {
      throw new NoSuchTableException("Invalid identifier: %s", identifier);
    }

    return result;
  }

  /**
   * 抽象方法：由子类实现具体的TableOperations创建逻辑
   */
  protected abstract TableOperations newTableOps(TableIdentifier tableIdentifier);

  /**
   * 抽象方法：由子类实现默认的warehouse位置计算
   */
  protected abstract String defaultWarehouseLocation(TableIdentifier tableIdentifier);

  /**
   * 创建表的通用实现
   */
  @Override
  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties) {

    // 1. 检查表是否已存在
    if (tableExists(identifier)) {
      throw new AlreadyExistsException("Table already exists: %s", identifier);
    }

    // 2. 确定表的存储位置
    String tableLocation = location != null ? location : defaultWarehouseLocation(identifier);

    // 3. 创建TableOperations
    TableOperations ops = newTableOps(identifier);

    // 4. 检查当前元数据是否为null（确保是新表）
    if (ops.current() != null) {
      throw new AlreadyExistsException("Table already exists: %s", identifier);
    }

    // 5. 构建初始TableMetadata
    TableMetadata metadata = TableMetadata.newTableMetadata(
        schema, spec, sortOrder, tableLocation, properties);

    // 6. 提交初始元数据（创建第一个版本）
    try {
      ops.commit(null, metadata);
    } catch (CommitFailedException e) {
      throw new RuntimeException("Failed to create table: " + identifier, e);
    }

    // 7. 返回Table对象
    return new BaseTable(ops, fullTableName(name(), identifier), metricsReporter());
  }

  /**
   * 注册表：假设表已存在，只是在catalog中注册
   */
  public Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
    TableOperations ops = newTableOps(identifier);
    TableMetadata metadata = TableMetadataParser.read(ops.io(), metadataFileLocation);
    ops.commit(null, metadata);
    return new BaseTable(ops, fullTableName(name(), identifier), metricsReporter());
  }
}
```

**核心设计模式分析**：

1. **模板方法模式**：
   - `loadTable()`定义了加载表的标准流程
   - `newTableOps()`和`defaultWarehouseLocation()`留给子类实现
   - 保证了所有catalog的行为一致性

2. **策略模式**：
   - 通过不同的TableOperations实现不同的存储策略
   - HadoopTableOperations：文件系统策略
   - HiveTableOperations：HMS + 文件系统混合策略
   - NessieTableOperations：版本控制策略

3. **工厂方法模式**：
   - `newTableOps()`是工厂方法，由子类决定创建哪种TableOperations

### 2.3 TableOperations接口

**源码位置**: `api/src/main/java/org/apache/iceberg/TableOperations.java`

```java
/**
 * TableOperations接口定义了对表元数据的底层原子操作
 * 每个Catalog实现都有对应的TableOperations实现
 */
public interface TableOperations {

  /**
   * 返回当前的TableMetadata
   * @return 当前表元数据，如果表不存在则返回null
   */
  TableMetadata current();

  /**
   * 刷新元数据，从存储中重新加载最新版本
   * @return 刷新后的TableMetadata
   */
  TableMetadata refresh();

  /**
   * 原子提交元数据更新
   * 这是Iceberg并发控制的核心方法
   *
   * @param base 期望的当前版本（乐观锁基础）
   * @param metadata 新的元数据版本
   * @throws CommitFailedException 如果base不是当前版本（并发冲突）
   */
  void commit(TableMetadata base, TableMetadata metadata);

  /**
   * 返回FileIO接口，用于读写元数据和数据文件
   */
  FileIO io();

  /**
   * 返回加密管理器（可选）
   */
  EncryptionManager encryption();

  /**
   * 返回表的元数据文件位置
   */
  String metadataFileLocation(String fileName);

  /**
   * 创建新的元数据文件位置
   */
  LocationProvider locationProvider();

  /**
   * 临时提交状态（用于快速失败）
   */
  default TableMetadata commitStatus() {
    return current();
  }
}
```

**关键方法：commit()的原子性保证**：

```
commit()方法是Iceberg并发控制的核心：

1. 调用者传入base（期望的当前版本）和metadata（新版本）
2. TableOperations检查当前版本是否等于base
3. 如果不等于，说明有其他人已经提交了新版本 → 抛出CommitFailedException
4. 如果等于，执行原子操作：
   - HadoopTableOperations: 使用文件系统的原子rename
   - HiveTableOperations: 使用HMS的事务 + 文件锁
   - NessieTableOperations: 使用Nessie的版本控制事务
```

### 2.4 TableMetadata版本化机制

Iceberg的元数据是不可变的（Immutable），每次修改都会生成新版本：

```
表目录结构（HadoopCatalog为例）：
warehouse/
└── db1/
    └── table1/
        ├── data/
        │   ├── 00000-0-xxx.parquet
        │   ├── 00001-0-xxx.parquet
        │   └── ...
        └── metadata/
            ├── version-hint.text         # 当前版本号提示
            ├── v1.metadata.json          # 版本1元数据
            ├── v2.metadata.json          # 版本2元数据
            ├── v3.metadata.json          # 版本3元数据（当前）
            ├── snap-xxx.avro             # 快照清单文件
            └── xxx-m0.avro               # Manifest文件
```

**元数据版本演进示例**：

```json
// v1.metadata.json (初始创建)
{
  "format-version": 2,
  "table-uuid": "12345-67890",
  "location": "s3://bucket/warehouse/db1/table1",
  "last-sequence-number": 0,
  "last-updated-ms": 1698764321000,
  "last-column-id": 3,
  "schema": {...},
  "partition-spec": [...],
  "default-spec-id": 0,
  "last-partition-id": 1000,
  "properties": {},
  "current-snapshot-id": -1,
  "snapshots": [],
  "snapshot-log": [],
  "metadata-log": []
}

// v2.metadata.json (第一次写入数据后)
{
  "format-version": 2,
  "table-uuid": "12345-67890",
  "location": "s3://bucket/warehouse/db1/table1",
  "last-sequence-number": 1,
  "last-updated-ms": 1698764421000,
  "last-column-id": 3,
  "schema": {...},
  "partition-spec": [...],
  "default-spec-id": 0,
  "last-partition-id": 1000,
  "properties": {},
  "current-snapshot-id": 987654321,  // 新增快照
  "snapshots": [
    {
      "snapshot-id": 987654321,
      "timestamp-ms": 1698764421000,
      "summary": {
        "operation": "append",
        "added-data-files": "5",
        "added-records": "10000"
      },
      "manifest-list": "s3://.../snap-987654321-1-xxx.avro"
    }
  ],
  "snapshot-log": [...],
  "metadata-log": [
    {
      "timestamp-ms": 1698764321000,
      "metadata-file": "s3://.../v1.metadata.json"
    }
  ]
}
```

---

## 3. HadoopCatalog深度源码分析

### 3.1 HadoopCatalog概述

**核心特点**：
- 最简单的Catalog实现
- 使用**文件系统目录结构**存储元数据
- 无需外部服务依赖（如HMS）
- 通过**原子文件重命名**保证并发安全
- 适用于测试、开发和小规模生产环境

**限制**：
- 不支持表的rename操作（需要移动整个目录）
- 性能受文件系统限制（list操作可能很慢）
- 依赖文件系统的原子rename语义（HDFS、S3等）

### 3.2 HadoopCatalog源码完整分析

**源码位置**: `core/src/main/java/org/apache/iceberg/hadoop/HadoopCatalog.java`

```java
package org.apache.iceberg.hadoop;

/**
 * HadoopCatalog: 基于文件系统的Catalog实现
 *
 * 物理存储结构：
 * $warehouse/
 *   $namespace1/
 *     $namespace2/
 *       $tableName/
 *         metadata/
 *           version-hint.text
 *           v1.metadata.json
 *           v2.metadata.json
 *           ...
 *         data/
 *           00000-0-xxx.parquet
 *           ...
 */
public class HadoopCatalog extends BaseMetastoreCatalog
    implements SupportsNamespaces, Configurable<Configuration> {

  private String catalogName;
  private Configuration conf;
  private String warehouseLocation;
  private FileSystem fs;
  private FileIO fileIO;
  private LockManager lockManager;
  private Closeable closeableGroup;

  // 配置项
  private static final String WAREHOUSE_LOCATION_PROP = "warehouse";
  private static final String LOCK_IMPL = "lock-impl";
  private static final String LOCK_IMPL_DEFAULT =
      "org.apache.iceberg.hadoop.HadoopLockManager";

  /**
   * 无参构造函数（Hadoop可配置接口要求）
   */
  public HadoopCatalog() {}

  /**
   * 带Configuration的构造函数
   */
  public HadoopCatalog(Configuration conf, String warehouseLocation) {
    this.conf = conf;
    this.warehouseLocation = LocationUtil.stripTrailingSlash(warehouseLocation);
  }

  /**
   * 初始化Catalog
   *
   * properties包含的配置：
   * - warehouse: warehouse根目录
   * - lock-impl: 锁管理器实现类
   * - io-impl: FileIO实现类
   */
  @Override
  public void initialize(String name, Map<String, String> properties) {
    this.catalogName = name;

    // 1. 解析warehouse位置
    String inputWarehouseLocation = properties.get(WAREHOUSE_LOCATION_PROP);
    Preconditions.checkArgument(
        inputWarehouseLocation != null,
        "Cannot initialize HadoopCatalog without warehouse location");
    this.warehouseLocation = LocationUtil.stripTrailingSlash(inputWarehouseLocation);

    // 2. 初始化Hadoop配置
    if (conf == null) {
      this.conf = new Configuration();
    }

    // 3. 获取FileSystem
    try {
      this.fs = Util.getFs(new Path(warehouseLocation), conf);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to get FileSystem for: " + warehouseLocation, e);
    }

    // 4. 初始化FileIO（文件读写抽象）
    String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    this.fileIO = CatalogUtil.loadFileIO(fileIOImpl, properties, conf);

    // 5. 初始化LockManager（并发控制）
    this.lockManager = LockManagers.from(properties);

    // 6. 组装Closeable资源组
    this.closeableGroup = new Closeable() {
      @Override
      public void close() throws IOException {
        if (lockManager instanceof Closeable) {
          ((Closeable) lockManager).close();
        }
        fileIO.close();
      }
    };
  }

  /**
   * 核心方法：创建TableOperations
   * 这是模板方法模式中子类必须实现的工厂方法
   */
  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new HadoopTableOperations(
        new Path(defaultWarehouseLocation(tableIdentifier)),
        fileIO,
        conf,
        lockManager);
  }

  /**
   * 计算表的默认存储位置
   *
   * 规则：$warehouse/$namespace1/$namespace2/.../$tableName
   *
   * 示例：
   * - warehouse: hdfs://namenode:8020/user/hive/warehouse
   * - namespace: db1.db2
   * - table: orders
   * - 结果: hdfs://namenode:8020/user/hive/warehouse/db1/db2/orders
   */
  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    StringBuilder sb = new StringBuilder();
    sb.append(warehouseLocation).append('/');

    // 添加命名空间层级
    for (String level : tableIdentifier.namespace().levels()) {
      sb.append(level).append('/');
    }

    // 添加表名
    sb.append(tableIdentifier.name());

    return sb.toString();
  }

  /**
   * 列出命名空间下的所有表
   *
   * 实现：遍历目录，找出包含metadata/的子目录
   */
  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    Path namespacePath = pathForNamespace(namespace);

    // 检查命名空间目录是否存在
    try {
      if (!fs.exists(namespacePath) || !fs.isDirectory(namespacePath)) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to list namespace: " + namespace, e);
    }

    List<TableIdentifier> tableIdentifiers = Lists.newArrayList();

    try {
      // 列出命名空间目录下的所有子目录
      FileStatus[] fileStatuses = fs.listStatus(namespacePath);
      for (FileStatus fileStatus : fileStatuses) {
        if (fileStatus.isDirectory()) {
          Path tablePath = fileStatus.getPath();
          Path metadataPath = new Path(tablePath, "metadata");

          // 检查是否包含metadata/目录（判断是否为Iceberg表）
          if (fs.exists(metadataPath) && fs.isDirectory(metadataPath)) {
            String tableName = tablePath.getName();
            tableIdentifiers.add(TableIdentifier.of(namespace, tableName));
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to list tables in namespace: " + namespace, e);
    }

    return tableIdentifiers;
  }

  /**
   * 删除表
   *
   * @param purge 如果为true，删除表目录和所有数据文件
   */
  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    if (!tableExists(identifier)) {
      return false;
    }

    Path tablePath = new Path(defaultWarehouseLocation(identifier));

    try {
      if (purge) {
        // 彻底删除整个表目录
        fs.delete(tablePath, true /* recursive */);
      } else {
        // 只删除metadata/目录，保留数据文件
        Path metadataPath = new Path(tablePath, "metadata");
        fs.delete(metadataPath, true);
      }
      return true;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to drop table: " + identifier, e);
    }
  }

  /**
   * 重命名表（不支持）
   *
   * 原因：需要移动整个目录，在分布式文件系统中代价很高且不原子
   */
  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    throw new UnsupportedOperationException(
        "HadoopCatalog does not support table rename");
  }

  // ==================== SupportsNamespaces接口实现 ====================

  /**
   * 创建命名空间（创建目录）
   */
  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    Path namespacePath = pathForNamespace(namespace);

    try {
      if (fs.exists(namespacePath)) {
        throw new AlreadyExistsException("Namespace already exists: %s", namespace);
      }
      fs.mkdirs(namespacePath);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create namespace: " + namespace, e);
    }
  }

  /**
   * 列出所有命名空间
   */
  @Override
  public List<Namespace> listNamespaces() {
    return listNamespaces(Namespace.empty());
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) {
    Path namespacePath = namespace.isEmpty()
        ? new Path(warehouseLocation)
        : pathForNamespace(namespace);

    List<Namespace> namespaces = Lists.newArrayList();

    try {
      if (!fs.exists(namespacePath)) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
      }

      FileStatus[] fileStatuses = fs.listStatus(namespacePath);
      for (FileStatus fileStatus : fileStatuses) {
        if (fileStatus.isDirectory()) {
          String dirName = fileStatus.getPath().getName();
          Namespace childNamespace = namespace.isEmpty()
              ? Namespace.of(dirName)
              : Namespace.of(
                  ArrayUtil.add(namespace.levels(), dirName));
          namespaces.add(childNamespace);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to list namespaces under: " + namespace, e);
    }

    return namespaces;
  }

  /**
   * 删除命名空间（删除目录）
   */
  @Override
  public boolean dropNamespace(Namespace namespace) {
    Path namespacePath = pathForNamespace(namespace);

    try {
      if (!fs.exists(namespacePath)) {
        return false;
      }

      // 检查命名空间是否为空
      FileStatus[] fileStatuses = fs.listStatus(namespacePath);
      if (fileStatuses.length > 0) {
        throw new NamespaceNotEmptyException(
            "Namespace %s is not empty", namespace);
      }

      return fs.delete(namespacePath, false);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to drop namespace: " + namespace, e);
    }
  }

  /**
   * 工具方法：将Namespace转换为Path
   */
  private Path pathForNamespace(Namespace namespace) {
    if (namespace.isEmpty()) {
      return new Path(warehouseLocation);
    }

    StringBuilder sb = new StringBuilder();
    sb.append(warehouseLocation);
    for (String level : namespace.levels()) {
      sb.append('/').append(level);
    }
    return new Path(sb.toString());
  }

  @Override
  public void close() throws IOException {
    if (closeableGroup != null) {
      closeableGroup.close();
    }
  }
}
```

### 3.3 HadoopTableOperations深度源码分析

**源码位置**: `core/src/main/java/org/apache/iceberg/hadoop/HadoopTableOperations.java`

```java
package org.apache.iceberg.hadoop;

/**
 * HadoopTableOperations: 基于文件系统的TableOperations实现
 *
 * 核心机制：
 * 1. 元数据版本化：v1.metadata.json, v2.metadata.json, ...
 * 2. 原子提交：使用FileSystem.rename()的原子性
 * 3. 并发控制：通过LockManager + version检查
 * 4. version-hint.text：快速定位当前版本号
 */
public class HadoopTableOperations implements TableOperations, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopTableOperations.class);

  private final Configuration conf;
  private final Path location;           // 表的根目录
  private final FileIO fileIO;
  private final LockManager lockManager;

  // 缓存的当前元数据和版本号
  private volatile TableMetadata currentMetadata = null;
  private volatile Integer version = null;
  private volatile boolean shouldRefresh = true;

  public HadoopTableOperations(
      Path location,
      FileIO fileIO,
      Configuration conf,
      LockManager lockManager) {
    this.location = location;
    this.fileIO = fileIO;
    this.conf = conf;
    this.lockManager = lockManager;
  }

  /**
   * 返回当前缓存的元数据
   */
  @Override
  public TableMetadata current() {
    if (shouldRefresh) {
      return refresh();
    }
    return currentMetadata;
  }

  /**
   * 刷新元数据：从文件系统重新加载最新版本
   *
   * 步骤：
   * 1. 从version-hint.text读取提示版本号
   * 2. 从该版本号开始，递增查找最新版本
   * 3. 加载最新版本的metadata.json
   * 4. 更新缓存
   */
  @Override
  public TableMetadata refresh() {
    int ver = version != null ? version : findVersion();

    try {
      // 从当前版本号开始，找到最新版本
      Path metadataFile = metadataPath(ver);
      while (fileIO.newInputFile(metadataFile.toString()).exists()) {
        ver += 1;
        metadataFile = metadataPath(ver);
      }

      // 回退到最后一个存在的版本
      ver -= 1;
      metadataFile = metadataPath(ver);

      // 读取元数据文件
      if (ver >= 0) {
        LOG.debug("Loading metadata from version {}: {}", ver, metadataFile);
        this.currentMetadata = TableMetadataParser.read(fileIO, metadataFile.toString());
        this.version = ver;
        this.shouldRefresh = false;
      } else {
        // 表不存在
        this.currentMetadata = null;
        this.version = null;
      }

    } catch (IOException e) {
      throw new UncheckedIOException("Failed to refresh table metadata", e);
    }

    return currentMetadata;
  }

  /**
   * 从version-hint.text读取版本号提示
   *
   * version-hint.text内容示例：3
   * 表示当前可能是版本3，但实际版本可能更高（需要递增查找）
   */
  private int findVersion() {
    Path versionHintFile = new Path(metadataRoot(), "version-hint.text");

    try {
      if (fileIO.newInputFile(versionHintFile.toString()).exists()) {
        String hint = Util.readAll(fileIO.newInputFile(versionHintFile.toString()));
        return Integer.parseInt(hint.trim());
      }
    } catch (IOException | NumberFormatException e) {
      LOG.warn("Failed to read version hint, starting from 0", e);
    }

    return 0;
  }

  /**
   * 原子提交元数据更新
   *
   * 这是Iceberg并发控制的核心方法，使用乐观锁机制：
   *
   * 1. 检查base是否等于current（乐观锁检查）
   * 2. 写入新的metadata文件到临时位置
   * 3. 原子重命名到最终位置（vN+1.metadata.json）
   * 4. 更新version-hint.text
   *
   * @param base 期望的当前版本（乐观锁基础）
   * @param metadata 新的元数据版本
   * @throws CommitFailedException 如果并发冲突
   */
  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    // 1. 刷新获取最新版本
    refresh();

    // 2. 乐观锁检查：base必须等于current
    if (base != null && currentMetadata != null &&
        base.uuid().equals(currentMetadata.uuid()) == false) {
      throw new CommitFailedException(
          "Cannot commit: stale table metadata. " +
          "Table UUID %s != expected UUID %s",
          currentMetadata.uuid(), base.uuid());
    }

    if (base == null && currentMetadata != null) {
      throw new CommitFailedException(
          "Cannot commit: table already exists");
    }

    // 3. 确定新版本号
    int newVersion = (version != null ? version : 0) + 1;

    // 4. 确定元数据压缩格式
    String codecName = metadata.property(
        TableProperties.METADATA_COMPRESSION,
        TableProperties.METADATA_COMPRESSION_DEFAULT);
    String fileExtension = TableMetadataParser.getFileExtension(codecName);

    // 5. 写入新元数据到临时文件
    String tempMetadataFileName = UUID.randomUUID().toString() + fileExtension;
    Path tempMetadataFile = new Path(metadataRoot(), tempMetadataFileName);

    LOG.debug("Writing new metadata to temp file: {}", tempMetadataFile);
    TableMetadataParser.write(metadata, fileIO.newOutputFile(tempMetadataFile.toString()));

    // 6. 准备最终文件名
    String finalMetadataFileName = formatMetadataFileName(newVersion, codecName);
    Path finalMetadataFile = new Path(metadataRoot(), finalMetadataFileName);

    // 7. 获取锁（可选，取决于LockManager配置）
    LockManager.LockId lockId = lockManager.acquire(location.toString());

    try {
      // 8. 再次检查版本（双重检查锁定）
      refresh();
      if (base != null && currentMetadata != null &&
          !base.uuid().equals(currentMetadata.uuid())) {
        throw new CommitFailedException(
            "Cannot commit: concurrent update detected");
      }

      // 9. 原子重命名（这是并发安全的关键）
      FileSystem fs = Util.getFs(finalMetadataFile, conf);
      if (!fs.rename(tempMetadataFile, finalMetadataFile)) {
        throw new CommitFailedException(
            "Failed to rename metadata file from %s to %s",
            tempMetadataFile, finalMetadataFile);
      }

      LOG.info("Committed metadata file: {}", finalMetadataFile);

      // 10. 更新version-hint.text
      writeVersionHint(newVersion);

      // 11. 更新缓存
      this.currentMetadata = metadata;
      this.version = newVersion;
      this.shouldRefresh = false;

    } finally {
      // 12. 释放锁
      lockManager.release(lockId);

      // 13. 删除临时文件（如果重命名失败）
      try {
        FileSystem fs = Util.getFs(tempMetadataFile, conf);
        if (fs.exists(tempMetadataFile)) {
          fs.delete(tempMetadataFile, false);
        }
      } catch (IOException e) {
        LOG.warn("Failed to delete temp metadata file: {}", tempMetadataFile, e);
      }
    }
  }

  /**
   * 写入version-hint.text文件
   *
   * 内容：当前版本号的字符串表示
   * 作用：加速下次refresh()的版本查找
   */
  private void writeVersionHint(int version) {
    Path versionHintFile = new Path(metadataRoot(), "version-hint.text");
    try {
      OutputFile output = fileIO.newOutputFile(versionHintFile.toString());
      try (OutputStream out = output.create()) {
        out.write(String.valueOf(version).getBytes(StandardCharsets.UTF_8));
      }
    } catch (IOException e) {
      LOG.warn("Failed to write version hint", e);
      // 非致命错误，不抛出异常
    }
  }

  /**
   * 返回metadata/目录的Path
   */
  private Path metadataRoot() {
    return new Path(location, "metadata");
  }

  /**
   * 构造元数据文件路径：metadata/v{N}.metadata.json
   */
  private Path metadataPath(int version) {
    return new Path(metadataRoot(), formatMetadataFileName(version, "none"));
  }

  /**
   * 格式化元数据文件名
   *
   * 格式：v{N}.metadata.json 或 v{N}.gz.metadata.json
   */
  private String formatMetadataFileName(int version, String codec) {
    String extension = TableMetadataParser.getFileExtension(codec);
    return String.format("v%d%s", version, extension);
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  public String metadataFileLocation(String fileName) {
    return new Path(metadataRoot(), fileName).toString();
  }

  @Override
  public LocationProvider locationProvider() {
    return LocationProviders.locationsFor(location.toString(), currentMetadata.properties());
  }
}
```

### 3.4 HadoopCatalog物理结构图

```
HadoopCatalog物理存储结构：

文件系统根目录（warehouse）
│
├── warehouse/                              # warehouse根目录
│   ├── namespace1/                         # 命名空间1（数据库）
│   │   ├── table_a/                        # 表A
│   │   │   ├── metadata/                   # 元数据目录
│   │   │   │   ├── version-hint.text       # 当前版本号提示
│   │   │   │   │   内容: "3"
│   │   │   │   ├── v1.metadata.json        # 版本1元数据
│   │   │   │   ├── v2.metadata.json        # 版本2元数据
│   │   │   │   ├── v3.metadata.json        # 版本3元数据（当前）
│   │   │   │   ├── snap-123-1-abc.avro     # 快照清单文件
│   │   │   │   └── def-m0.avro             # Manifest文件
│   │   │   └── data/                       # 数据目录
│   │   │       ├── partition1=val1/        # 分区目录
│   │   │       │   ├── 00000-0-xxx.parquet
│   │   │       │   └── 00001-0-yyy.parquet
│   │   │       └── partition1=val2/
│   │   │           └── 00000-0-zzz.parquet
│   │   └── table_b/
│   │       ├── metadata/
│   │       └── data/
│   └── namespace2/
│       └── namespace3/                     # 嵌套命名空间
│           └── table_c/
│               ├── metadata/
│               └── data/

元数据文件内部结构（v3.metadata.json）：
{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": "hdfs://namenode/warehouse/namespace1/table_a",
  "last-sequence-number": 3,
  "last-updated-ms": 1698764500000,
  "last-column-id": 5,
  "schema": {
    "type": "struct",
    "schema-id": 0,
    "fields": [...]
  },
  "partition-spec": [...],
  "current-snapshot-id": 987654321,
  "snapshots": [
    {
      "snapshot-id": 987654321,
      "timestamp-ms": 1698764500000,
      "summary": {
        "operation": "append",
        "added-data-files": "3",
        "added-records": "5000"
      },
      "manifest-list": "hdfs://.../snap-987654321-1-abc.avro"
    }
  ],
  "snapshot-log": [...],
  "metadata-log": [
    {"timestamp-ms": 1698764321000, "metadata-file": ".../v1.metadata.json"},
    {"timestamp-ms": 1698764400000, "metadata-file": ".../v2.metadata.json"}
  ]
}
```

### 3.5 HadoopCatalog实现流程分析

#### 3.5.1 创建表流程

```
用户代码：
  catalog.createTable(
    TableIdentifier.of("db1", "orders"),
    schema,
    partitionSpec,
    properties)

调用栈：
1. HadoopCatalog.createTable()
   ├─ 检查表是否已存在
   ├─ 计算表位置: warehouse/db1/orders
   ├─ 创建HadoopTableOperations
   │  └─ 构造函数传入: location, fileIO, conf, lockManager
   └─ 调用BaseMetastoreCatalog.createTable()

2. BaseMetastoreCatalog.createTable()
   ├─ 构建初始TableMetadata
   │  ├─ schema
   │  ├─ partition-spec
   │  ├─ sort-order
   │  ├─ location
   │  ├─ properties
   │  └─ uuid (生成新UUID)
   └─ 调用TableOperations.commit(null, metadata)

3. HadoopTableOperations.commit(null, metadata)
   ├─ 确定版本号: newVersion = 1
   ├─ 生成临时文件名: {uuid}.metadata.json
   ├─ 写入临时文件: metadata/{uuid}.metadata.json
   │  └─ TableMetadataParser.write(metadata, outputFile)
   ├─ 获取锁: lockManager.acquire()
   ├─ 原子重命名: {uuid}.metadata.json → v1.metadata.json
   │  └─ FileSystem.rename() [原子操作]
   ├─ 写入version-hint.text: "1"
   ├─ 更新缓存: currentMetadata = metadata, version = 1
   └─ 释放锁: lockManager.release()

文件系统变化：
Before:
  warehouse/
    db1/
      (不存在)

After:
  warehouse/
    db1/
      orders/
        metadata/
          version-hint.text    (内容: "1")
          v1.metadata.json     (初始元数据)
        data/
          (空目录)
```

#### 3.5.2 加载表流程

```
用户代码：
  Table table = catalog.loadTable(TableIdentifier.of("db1", "orders"))

调用栈：
1. HadoopCatalog.loadTable("db1.orders")
   └─ 继承自BaseMetastoreCatalog.loadTable()

2. BaseMetastoreCatalog.loadTable()
   ├─ 验证表标识符
   ├─ 调用newTableOps() → HadoopTableOperations
   │  └─ 构造函数: location = warehouse/db1/orders
   ├─ 调用ops.current() → 触发refresh()
   └─ 创建BaseTable包装ops

3. HadoopTableOperations.refresh()
   ├─ 读取version-hint.text → version = 3
   ├─ 递增查找最新版本:
   │  ├─ 检查v3.metadata.json (存在)
   │  ├─ 检查v4.metadata.json (存在)
   │  ├─ 检查v5.metadata.json (不存在)
   │  └─ 最新版本 = 4
   ├─ 读取v4.metadata.json
   │  └─ TableMetadataParser.read(fileIO, "metadata/v4.metadata.json")
   └─ 更新缓存: currentMetadata, version = 4

4. BaseTable构造
   ├─ 持有TableOperations引用
   ├─ 当前元数据: v4.metadata.json
   └─ 返回给用户

用户获得的Table对象：
  - table.schema() → 返回当前schema
  - table.spec() → 返回当前partition spec
  - table.currentSnapshot() → 返回当前快照
  - table.newAppend() → 创建追加操作
```

#### 3.5.3 数据写入流程（Append操作）

```
用户代码：
  table.newAppend()
    .appendFile(dataFile1)
    .appendFile(dataFile2)
    .commit()

调用栈：
1. BaseTable.newAppend()
   └─ 创建SnapshotProducer (MergeAppend)

2. MergeAppend.appendFile(dataFile1)
   └─ 添加到内部列表: dataFilesToAdd

3. MergeAppend.commit()
   ├─ 调用TableOperations.refresh() 获取最新元数据
   │  └─ 当前版本: v4, snapshot-id: 100
   ├─ 创建新的Manifest文件
   │  ├─ 写入新增的dataFile1, dataFile2
   │  └─ 文件位置: metadata/abc-m0.avro
   ├─ 创建新的Snapshot清单
   │  ├─ snapshot-id: 101 (新生成)
   │  ├─ parent-snapshot-id: 100
   │  ├─ manifest-list: metadata/snap-101-1-def.avro
   │  └─ summary: {operation: append, added-files: 2, added-records: 1000}
   ├─ 构建新的TableMetadata
   │  ├─ 基于current metadata
   │  ├─ 添加新snapshot: 101
   │  ├─ 更新current-snapshot-id: 101
   │  └─ 递增last-sequence-number: 5
   └─ 调用TableOperations.commit(base=v4, new=v5)

4. HadoopTableOperations.commit(base, new)
   ├─ 乐观锁检查: base.uuid == current.uuid ✓
   ├─ 新版本号: 5
   ├─ 写入临时文件: metadata/{uuid}.metadata.json
   ├─ 获取lockManager锁
   ├─ 再次refresh检查版本（双重检查）
   ├─ 原子重命名: {uuid}.metadata.json → v5.metadata.json
   ├─ 更新version-hint.text: "5"
   └─ 释放锁

并发场景（两个写入者）：
Writer A                          Writer B
─────────────────────────────────────────────────────────
refresh() → v4                    refresh() → v4
appendFile(file1)                 appendFile(file2)
创建snapshot-id=101               创建snapshot-id=102
commit(base=v4, new=v5)
  ├─ 写temp文件
  ├─ 获取锁
  ├─ 检查版本 ✓
  ├─ rename → v5 ✓
  └─ 释放锁
                                  commit(base=v4, new=v5)
                                    ├─ 写temp文件
                                    ├─ 获取锁
                                    ├─ refresh() → v5 (已变化!)
                                    ├─ 检查版本 ✗ (base=v4 != current=v5)
                                    └─ 抛出CommitFailedException

Writer B处理冲突：
  - 捕获CommitFailedException
  - 重新refresh()获取v5
  - 基于v5重新构建snapshot
  - 重新提交commit(base=v5, new=v6)
```

### 3.6 HadoopCatalog并发控制机制

Hadoop Catalog使用**乐观锁 + 文件系统原子操作**来保证并发安全：

```
并发控制三层保障：

1. 版本号检查（乐观锁）
   - commit()时检查base.uuid是否等于current.uuid
   - 如果不等，说明有并发写入 → 抛出异常

2. LockManager（可选）
   - 在commit的critical section加锁
   - 防止多个写入者同时执行rename操作
   - 实现类：
     * HadoopLockManager（本地文件锁）
     * HiveLockManager（HMS锁）
     * NoLockManager（无锁，仅依赖原子rename）

3. FileSystem原子rename
   - rename()操作在HDFS/S3等分布式文件系统中是原子的
   - 保证v5.metadata.json不会被覆盖
   - 如果rename失败，整个commit失败

时序图：
Writer A          LockManager       FileSystem        Writer B
   │                  │                 │                 │
   ├─ refresh() ──────┼─────────────────┤                 │
   │  (version=4)     │                 │                 │
   │                  │                 │                 │
   ├─ commit()        │                 │                 │
   ├─ acquire() ──────>                 │                 │
   │  <──── lockId    │                 │                 │
   ├─ write temp ─────┼─────────────────>                 │
   │                  │  temp写入完成    │                 │
   ├─ rename() ───────┼─────────────────>                 │
   │                  │  temp→v5(原子)   │                 │
   ├─ release() ──────>                 │                 │
   │                  │                 │                 │
   │                  │                 │  <─ refresh()   │
   │                  │                 │  (version=5)    │
   │                  │                 │  commit()     ──┤
   │                  │  <─ acquire() ───────────────────┤
   │                  │  lockId ─────────────────────────>
   │                  │                 │  write temp  ───┤
   │                  │                 │  <─ rename() ───┤
   │                  │  v6(原子) ───────────────────────>
   │                  │  <─ release() ───────────────────┤
```

---

## 4. HiveCatalog深度源码分析

### 4.1 HiveCatalog概述

**核心特点**：
- 使用**Hive Metastore (HMS)**存储表的元数据指针
- 元数据文件仍然存储在文件系统中
- HMS存储：表名、location、表类型、Iceberg元数据文件位置
- 适用于企业级生产环境，与Hive生态无缝集成
- 支持表和视图（View）

**架构模式**：混合存储
```
Hive Metastore (RDBMS)              文件系统
─────────────────────────           ───────────────────────
表名: db1.orders                    warehouse/db1/orders/
表类型: ICEBERG                       metadata/
location: hdfs://.../orders           v1.metadata.json
metadata_location:                    v2.metadata.json
  hdfs://.../v2.metadata.json         v3.metadata.json ← 当前
properties: {...}                     snap-xxx.avro
                                      data/
     ↓                                  00000-0-xxx.parquet
  指向实际元数据文件
```

### 4.2 HiveCatalog源码完整分析

**源码位置**: `hive-metastore/src/main/java/org/apache/iceberg/hive/HiveCatalog.java`

```java
package org.apache.iceberg.hive;

/**
 * HiveCatalog: 基于Hive Metastore的Catalog实现
 *
 * 核心组件：
 * 1. ClientPool: HMS客户端连接池
 * 2. FileIO: 文件读写
 * 3. HiveTableOperations: 表元数据操作
 * 4. HiveConf: Hive配置
 */
public class HiveCatalog extends BaseMetastoreViewCatalog
    implements SupportsNamespaces, Configurable<Configuration> {

  private static final Logger LOG = LoggerFactory.getLogger(HiveCatalog.class);

  private String catalogName;
  private Configuration conf;
  private FileIO fileIO;
  private ClientPool<IMetaStoreClient, TException> clients;
  private Closeable closeable;
  private boolean listAllTables = false;

  // HMS表属性键
  private static final String ICEBERG_TABLE_TYPE = "table_type";
  private static final String ICEBERG_TABLE_TYPE_VALUE = "ICEBERG";
  private static final String METADATA_LOCATION_PROP = "metadata_location";

  public HiveCatalog() {}

  /**
   * 初始化HiveCatalog
   *
   * properties配置项：
   * - uri: Hive Metastore URI (thrift://host:port)
   * - warehouse: warehouse根目录
   * - io-impl: FileIO实现类
   * - clients: HMS客户端池大小
   * - list-all-tables: 是否列出所有表（包括非Iceberg表）
   */
  @Override
  public void initialize(String name, Map<String, String> properties) {
    this.catalogName = name;

    // 1. 初始化Hadoop配置
    if (conf == null) {
      this.conf = new Configuration();
    }

    // 2. 配置HMS URI
    if (properties.containsKey(CatalogProperties.URI)) {
      String hiveMetastoreUris = properties.get(CatalogProperties.URI);
      conf.set(HiveConf.ConfVars.METASTOREURIS.varname, hiveMetastoreUris);
      LOG.info("Hive Metastore URI: {}", hiveMetastoreUris);
    }

    // 3. 配置warehouse位置
    if (properties.containsKey(CatalogProperties.WAREHOUSE_LOCATION)) {
      String warehouseLocation = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
      conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehouseLocation);
    }

    // 4. 初始化FileIO
    String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    this.fileIO = fileIOImpl != null
        ? CatalogUtil.loadFileIO(fileIOImpl, properties, conf)
        : new HadoopFileIO(conf);

    // 5. 初始化HMS客户端池
    this.clients = new CachedClientPool(conf, properties);

    // 6. 配置选项
    this.listAllTables = PropertyUtil.propertyAsBoolean(
        properties,
        CatalogProperties.ENGINE_HIVE_ENABLED,
        CatalogProperties.ENGINE_HIVE_ENABLED_DEFAULT);

    // 7. 资源管理
    this.closeable = () -> {
      clients.close();
      if (fileIO instanceof Closeable) {
        ((Closeable) fileIO).close();
      }
    };
  }

  /**
   * 核心方法：创建HiveTableOperations
   */
  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    String dbName = tableIdentifier.namespace().level(0);
    String tableName = tableIdentifier.name();

    return new HiveTableOperations(
        conf,
        clients,
        fileIO,
        catalogName,
        dbName,
        tableName);
  }

  /**
   * 计算表的默认存储位置
   *
   * 规则：
   * 1. 如果namespace有location属性，使用该location
   * 2. 否则使用：warehouse/dbName/tableName
   */
  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    String dbName = tableIdentifier.namespace().level(0);
    String tableName = tableIdentifier.name();

    // 尝试从数据库获取location
    String databaseLocation = null;
    try {
      Database database = clients.run(client -> client.getDatabase(dbName));
      databaseLocation = database.getLocationUri();
    } catch (TException | InterruptedException e) {
      throw new RuntimeException("Failed to get database location for: " + dbName, e);
    }

    if (databaseLocation != null && !databaseLocation.isEmpty()) {
      return String.format("%s/%s", databaseLocation, tableName);
    } else {
      // 回退到默认warehouse
      String warehouseLocation = conf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname);
      return String.format("%s/%s.db/%s", warehouseLocation, dbName, tableName);
    }
  }

  /**
   * 列出命名空间下的所有表
   *
   * 实现：调用HMS的getAllTables()，过滤出Iceberg表
   */
  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    Preconditions.checkArgument(
        namespace.levels().length == 1,
        "HiveCatalog only supports single-level namespaces, got: %s", namespace);

    String dbName = namespace.level(0);

    try {
      // 1. 从HMS获取所有表名
      List<String> tableNames = clients.run(client -> client.getAllTables(dbName));

      // 2. 过滤出Iceberg表
      List<TableIdentifier> tableIdentifiers = Lists.newArrayListWithCapacity(tableNames.size());

      for (String tableName : tableNames) {
        if (listAllTables || isIcebergTable(dbName, tableName)) {
          tableIdentifiers.add(TableIdentifier.of(namespace, tableName));
        }
      }

      return tableIdentifiers;

    } catch (TException | InterruptedException e) {
      throw new RuntimeException("Failed to list tables in namespace: " + namespace, e);
    }
  }

  /**
   * 判断HMS中的表是否为Iceberg表
   */
  private boolean isIcebergTable(String dbName, String tableName) {
    try {
      Table hmsTable = clients.run(client -> client.getTable(dbName, tableName));
      String tableType = hmsTable.getParameters().get(ICEBERG_TABLE_TYPE);
      return ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(tableType);
    } catch (TException | InterruptedException e) {
      LOG.warn("Failed to check if table is Iceberg: {}.{}", dbName, tableName, e);
      return false;
    }
  }

  /**
   * 删除表
   *
   * 实现：
   * 1. 从HMS删除表元数据
   * 2. 如果purge=true，删除表的数据文件和元数据文件
   */
  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    if (!isValidIdentifier(identifier)) {
      return false;
    }

    String dbName = identifier.namespace().level(0);
    String tableName = identifier.name();

    try {
      // 1. 加载表以获取location（用于purge）
      String tableLocation = null;
      if (purge) {
        Table table = loadTable(identifier);
        tableLocation = table.location();
      }

      // 2. 从HMS删除表
      clients.run(client -> {
        client.dropTable(
            dbName,
            tableName,
            true /* deleteData - 由HMS管理的数据 */,
            false /* ignoreUnknownTab */);
        return null;
      });

      LOG.info("Dropped table from HMS: {}.{}", dbName, tableName);

      // 3. 如果purge=true，彻底删除表目录
      if (purge && tableLocation != null) {
        try {
          Path tablePath = new Path(tableLocation);
          FileSystem fs = tablePath.getFileSystem(conf);
          fs.delete(tablePath, true /* recursive */);
          LOG.info("Purged table location: {}", tableLocation);
        } catch (IOException e) {
          LOG.warn("Failed to purge table location: {}", tableLocation, e);
        }
      }

      return true;

    } catch (NoSuchTableException e) {
      return false;
    } catch (TException | InterruptedException e) {
      throw new RuntimeException("Failed to drop table: " + identifier, e);
    }
  }

  /**
   * 重命名表
   *
   * 实现：调用HMS的alter_table()
   * 注意：只修改HMS中的表名，不移动表的物理位置
   */
  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    Preconditions.checkArgument(
        from.namespace().equals(to.namespace()),
        "Cannot rename table across databases: %s -> %s", from, to);

    String dbName = from.namespace().level(0);
    String fromTableName = from.name();
    String toTableName = to.name();

    try {
      // 1. 获取原表的HMS Table对象
      Table hmsTable = clients.run(client -> client.getTable(dbName, fromTableName));

      // 2. 修改表名
      hmsTable.setTableName(toTableName);

      // 3. 调用HMS的alter_table
      clients.run(client -> {
        client.alter_table(dbName, fromTableName, hmsTable);
        return null;
      });

      LOG.info("Renamed table in HMS: {}.{} -> {}.{}",
               dbName, fromTableName, dbName, toTableName);

    } catch (TException | InterruptedException e) {
      throw new RuntimeException("Failed to rename table: " + from + " -> " + to, e);
    }
  }

  // ==================== SupportsNamespaces接口实现 ====================

  /**
   * 创建命名空间（创建数据库）
   */
  @Override
  public void createNamespace(Namespace namespace, Map<String, String> meta) {
    Preconditions.checkArgument(
        namespace.levels().length == 1,
        "HiveCatalog only supports single-level namespaces, got: %s", namespace);

    String dbName = namespace.level(0);

    Database database = new Database();
    database.setName(dbName);

    // 设置数据库location
    if (meta.containsKey("location")) {
      database.setLocationUri(meta.get("location"));
    }

    // 设置数据库描述
    if (meta.containsKey("comment")) {
      database.setDescription(meta.get("comment"));
    }

    // 设置数据库properties
    database.setParameters(convertToHMSProperties(meta));

    try {
      clients.run(client -> {
        client.createDatabase(database);
        return null;
      });
      LOG.info("Created database in HMS: {}", dbName);
    } catch (TException | InterruptedException e) {
      throw new RuntimeException("Failed to create namespace: " + namespace, e);
    }
  }

  /**
   * 列出所有命名空间（数据库）
   */
  @Override
  public List<Namespace> listNamespaces() {
    try {
      List<String> databases = clients.run(client -> client.getAllDatabases());
      return databases.stream()
          .map(Namespace::of)
          .collect(Collectors.toList());
    } catch (TException | InterruptedException e) {
      throw new RuntimeException("Failed to list namespaces", e);
    }
  }

  /**
   * 删除命名空间（删除数据库）
   */
  @Override
  public boolean dropNamespace(Namespace namespace) {
    String dbName = namespace.level(0);

    try {
      clients.run(client -> {
        client.dropDatabase(
            dbName,
            true /* deleteData */,
            false /* ignoreUnknownDb */,
            false /* cascade */);
        return null;
      });
      LOG.info("Dropped database from HMS: {}", dbName);
      return true;
    } catch (TException | InterruptedException e) {
      if (e.getMessage().contains("NoSuchObjectException")) {
        return false;
      }
      throw new RuntimeException("Failed to drop namespace: " + namespace, e);
    }
  }

  /**
   * 加载命名空间元数据
   */
  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
    String dbName = namespace.level(0);

    try {
      Database database = clients.run(client -> client.getDatabase(dbName));

      Map<String, String> metadata = Maps.newHashMap();

      if (database.getLocationUri() != null) {
        metadata.put("location", database.getLocationUri());
      }

      if (database.getDescription() != null) {
        metadata.put("comment", database.getDescription());
      }

      if (database.getParameters() != null) {
        metadata.putAll(database.getParameters());
      }

      return metadata;

    } catch (TException | InterruptedException e) {
      throw new RuntimeException("Failed to load namespace metadata: " + namespace, e);
    }
  }

  @Override
  public void close() throws IOException {
    if (closeable != null) {
      closeable.close();
    }
  }
}
```

### 4.3 CachedClientPool实现

HiveCatalog使用连接池管理HMS客户端连接：

```java
/**
 * CachedClientPool: HMS客户端连接池
 *
 * 功能：
 * 1. 连接复用，避免频繁创建Thrift连接
 * 2. 连接池大小可配置
 * 3. 连接健康检查和重连
 */
class CachedClientPool implements ClientPool<IMetaStoreClient, TException> {

  private final HiveConf hiveConf;
  private final int poolSize;
  private final LinkedBlockingQueue<IMetaStoreClient> clients;
  private final AtomicInteger clientCount = new AtomicInteger(0);

  CachedClientPool(Configuration conf, Map<String, String> properties) {
    this.hiveConf = new HiveConf(conf, HiveConf.class);
    this.poolSize = PropertyUtil.propertyAsInt(
        properties,
        CatalogProperties.CLIENT_POOL_SIZE,
        CatalogProperties.CLIENT_POOL_SIZE_DEFAULT);
    this.clients = new LinkedBlockingQueue<>(poolSize);
  }

  /**
   * 执行HMS操作，自动管理连接获取和释放
   */
  @Override
  public <R> R run(Action<R, IMetaStoreClient, TException> action)
      throws TException, InterruptedException {

    IMetaStoreClient client = get();
    try {
      return action.run(client);
    } finally {
      release(client);
    }
  }

  /**
   * 从连接池获取客户端
   */
  private IMetaStoreClient get() throws TException, InterruptedException {
    IMetaStoreClient client = clients.poll();

    if (client == null) {
      if (clientCount.get() < poolSize) {
        // 创建新连接
        client = createClient();
        clientCount.incrementAndGet();
      } else {
        // 等待可用连接
        client = clients.take();
      }
    }

    // 健康检查
    if (!isClientHealthy(client)) {
      client = reconnect(client);
    }

    return client;
  }

  /**
   * 释放客户端回连接池
   */
  private void release(IMetaStoreClient client) {
    clients.offer(client);
  }

  /**
   * 创建新的HMS客户端
   */
  private IMetaStoreClient createClient() throws TException {
    try {
      return RetryingMetaStoreClient.getProxy(
          hiveConf,
          tbl -> null,
          HiveMetaStoreClient.class.getName());
    } catch (MetaException e) {
      throw new TException("Failed to create Hive Metastore client", e);
    }
  }

  /**
   * 检查客户端连接是否健康
   */
  private boolean isClientHealthy(IMetaStoreClient client) {
    try {
      client.getAllDatabases(); // 简单的ping操作
      return true;
    } catch (TException e) {
      return false;
    }
  }

  /**
   * 重新连接
   */
  private IMetaStoreClient reconnect(IMetaStoreClient client) throws TException {
    try {
      client.close();
    } catch (Exception e) {
      // ignore
    }
    return createClient();
  }

  @Override
  public void close() {
    for (IMetaStoreClient client : clients) {
      try {
        client.close();
      } catch (Exception e) {
        // ignore
      }
    }
    clients.clear();
  }
}
```

### 4.4 HiveTableOperations深度源码分析

**源码位置**: `hive-metastore/src/main/java/org/apache/iceberg/hive/HiveTableOperations.java`

```java
package org.apache.iceberg.hive;

/**
 * HiveTableOperations: 基于Hive Metastore的TableOperations实现
 *
 * 核心机制：
 * 1. HMS存储表的元数据指针（metadata_location属性）
 * 2. 实际元数据文件存储在文件系统中
 * 3. commit时原子更新HMS中的metadata_location
 * 4. 使用HMS的事务机制保证并发安全
 */
public class HiveTableOperations extends BaseMetastoreTableOperations
    implements HiveOperationsBase {

  private static final Logger LOG = LoggerFactory.getLogger(HiveTableOperations.class);

  private final String catalogName;
  private final ClientPool<IMetaStoreClient, TException> metaClients;
  private final String database;
  private final String tableName;
  private final Configuration conf;
  private final FileIO fileIO;

  // HMS表属性键
  private static final String METADATA_LOCATION_PROP = "metadata_location";
  private static final String PREVIOUS_METADATA_LOCATION_PROP = "previous_metadata_location";
  private static final String TABLE_TYPE_PROP = "table_type";
  private static final String ICEBERG_TABLE_TYPE_VALUE = "ICEBERG";

  public HiveTableOperations(
      Configuration conf,
      ClientPool<IMetaStoreClient, TException> metaClients,
      FileIO fileIO,
      String catalogName,
      String database,
      String tableName) {
    this.conf = conf;
    this.metaClients = metaClients;
    this.fileIO = fileIO;
    this.catalogName = catalogName;
    this.database = database;
    this.tableName = tableName;
  }

  /**
   * 刷新元数据：从HMS读取metadata_location，然后加载元数据文件
   */
  @Override
  protected void doRefresh() {
    String metadataLocation = null;

    try {
      // 1. 从HMS获取表对象
      Table hmsTable = metaClients.run(client -> client.getTable(database, tableName));

      // 2. 验证是否为Iceberg表
      HiveOperationsBase.validateTableIsIceberg(hmsTable, fullName());

      // 3. 读取metadata_location属性
      metadataLocation = hmsTable.getParameters().get(METADATA_LOCATION_PROP);

      if (metadataLocation == null) {
        throw new IllegalStateException(
            String.format("Cannot find %s in HMS table properties for %s",
                          METADATA_LOCATION_PROP, fullName()));
      }

    } catch (NoSuchObjectException e) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException("Table does not exist: %s.%s", database, tableName);
      }
    } catch (TException | InterruptedException e) {
      throw new RuntimeException("Failed to refresh table from HMS: " + fullName(), e);
    }

    // 4. 从文件系统加载元数据
    refreshFromMetadataLocation(metadataLocation, metadataRefreshMaxRetries());
  }

  /**
   * 提交元数据更新
   *
   * 步骤：
   * 1. 写入新的元数据文件到文件系统
   * 2. 获取HMS表锁
   * 3. 读取HMS中的当前metadata_location
   * 4. 检查是否与base一致（乐观锁）
   * 5. 更新HMS表属性中的metadata_location
   * 6. 调用HMS的alter_table（事务性更新）
   * 7. 释放锁
   */
  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    boolean newTable = base == null;
    String newMetadataLocation = newTable
        ? writeNewMetadata(metadata, 0)
        : writeNewMetadataIfRequired(metadata);

    boolean hiveEngineEnabled = PropertyUtil.propertyAsBoolean(
        metadata.properties(),
        TableProperties.ENGINE_HIVE_ENABLED,
        TableProperties.ENGINE_HIVE_ENABLED_DEFAULT);

    boolean keepHiveStats = hiveEngineEnabled;

    try {
      // 1. 获取HMS表锁（基于HMS的锁机制）
      HiveLock lock = lockObject();
      lock.lock();

      try {
        // 2. 从HMS加载当前表对象
        Table hmsTable;
        try {
          hmsTable = metaClients.run(client -> client.getTable(database, tableName));
        } catch (NoSuchObjectException e) {
          if (newTable) {
            hmsTable = newHMSTable();
          } else {
            throw new NoSuchTableException("Table does not exist: %s.%s", database, tableName);
          }
        }

        // 3. 乐观锁检查：比较HMS中的metadata_location与base
        if (!newTable) {
          String currentMetadataLocation = hmsTable.getParameters().get(METADATA_LOCATION_PROP);
          String baseMetadataLocation = base != null ? currentMetadataLocation() : null;

          if (!Objects.equals(currentMetadataLocation, baseMetadataLocation)) {
            throw new CommitFailedException(
                "Cannot commit: metadata location in HMS has changed. " +
                "Expected %s but found %s",
                baseMetadataLocation, currentMetadataLocation);
          }
        }

        // 4. 更新HMS表属性
        Map<String, String> parameters = hmsTable.getParameters();
        if (parameters == null) {
          parameters = Maps.newHashMap();
        }

        // 设置新的metadata_location
        parameters.put(METADATA_LOCATION_PROP, newMetadataLocation);

        // 保存previous_metadata_location（用于回滚）
        if (!newTable && currentMetadataLocation() != null) {
          parameters.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation());
        }

        // 设置Iceberg表类型
        parameters.put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE);

        // 5. 更新HMS表的Schema（同步Iceberg Schema）
        if (hiveEngineEnabled) {
          List<FieldSchema> columns = HiveSchemaUtil.convert(metadata.schema());
          hmsTable.getSd().setCols(columns);

          // 更新分区列
          List<FieldSchema> partColumns = HiveSchemaUtil.convertPartitionColumns(
              metadata.schema(), metadata.spec());
          hmsTable.setPartitionKeys(partColumns);

          // 设置InputFormat和OutputFormat
          hmsTable.getSd().setInputFormat("org.apache.iceberg.mr.hive.HiveIcebergInputFormat");
          hmsTable.getSd().setOutputFormat("org.apache.iceberg.mr.hive.HiveIcebergOutputFormat");
          hmsTable.getSd().setSerdeInfo(new SerDeInfo(
              "iceberg",
              "org.apache.iceberg.mr.hive.HiveIcebergSerDe",
              Maps.newHashMap()));
        }

        // 6. 设置表的location
        hmsTable.getSd().setLocation(metadata.location());

        // 7. 更新快照统计信息（如果启用Hive引擎）
        if (hiveEngineEnabled && metadata.currentSnapshot() != null) {
          Map<String, String> summary = metadata.currentSnapshot().summary();
          parameters.put("numFiles", summary.get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
          parameters.put("numRows", summary.get(SnapshotSummary.TOTAL_RECORDS_PROP));
          parameters.put("totalSize", summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP));
        }

        // 8. 调用HMS API提交更新
        if (newTable) {
          metaClients.run(client -> {
            client.createTable(hmsTable);
            return null;
          });
          LOG.info("Created table in HMS: {}.{}", database, tableName);
        } else {
          metaClients.run(client -> {
            client.alter_table(database, tableName, hmsTable, null);
            return null;
          });
          LOG.info("Updated table in HMS: {}.{}", database, tableName);
        }

      } finally {
        // 9. 释放锁
        lock.unlock();
      }

    } catch (TException | InterruptedException e) {
      throw new RuntimeException("Failed to commit table to HMS: " + fullName(), e);
    } catch (CommitFailedException e) {
      // 删除新写入的元数据文件
      if (!newMetadataLocation.equals(currentMetadataLocation())) {
        try {
          fileIO.deleteFile(newMetadataLocation);
        } catch (Exception deleteException) {
          LOG.warn("Failed to delete metadata file after commit failure: {}",
                   newMetadataLocation, deleteException);
        }
      }
      throw e;
    }

    // 10. 更新本地缓存
    refreshFromMetadataLocation(newMetadataLocation, 2);
  }

  /**
   * 创建新的HMS表对象
   */
  private Table newHMSTable() {
    Table hmsTable = new Table();
    hmsTable.setDbName(database);
    hmsTable.setTableName(tableName);
    hmsTable.setTableType("EXTERNAL_TABLE");
    hmsTable.setParameters(Maps.newHashMap());

    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("");
    sd.setCols(Lists.newArrayList());
    sd.setSerdeInfo(new SerDeInfo());
    hmsTable.setSd(sd);

    return hmsTable;
  }

  /**
   * HMS表锁实现（基于HMS的事务机制）
   */
  private HiveLock lockObject() {
    return new HiveLock() {
      private LockResponse lockResponse = null;

      @Override
      public void lock() {
        try {
          LockRequest lockRequest = new LockRequest();
          LockComponent lockComponent = new LockComponent();
          lockComponent.setDbname(database);
          lockComponent.setTablename(tableName);
          lockComponent.setType(LockType.EXCLUSIVE);
          lockComponent.setLevel(LockLevel.TABLE);
          lockRequest.setComponent(Lists.newArrayList(lockComponent));

          lockResponse = metaClients.run(client -> client.lock(lockRequest));

          // 等待锁获取
          if (lockResponse.getState() == LockState.WAITING) {
            lockResponse = metaClients.run(client ->
                client.checkLock(lockResponse.getLockid()));
          }

          if (lockResponse.getState() != LockState.ACQUIRED) {
            throw new CommitFailedException(
                "Failed to acquire HMS lock for table: %s.%s",
                database, tableName);
          }

        } catch (TException | InterruptedException e) {
          throw new RuntimeException("Failed to acquire HMS lock", e);
        }
      }

      @Override
      public void unlock() {
        if (lockResponse != null) {
          try {
            metaClients.run(client -> {
              client.unlock(lockResponse.getLockid());
              return null;
            });
          } catch (TException | InterruptedException e) {
            LOG.warn("Failed to release HMS lock", e);
          }
        }
      }
    };
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  private String fullName() {
    return String.format("%s.%s.%s", catalogName, database, tableName);
  }
}
```

### 4.5 HiveCatalog物理结构图

```
HiveCatalog混合存储架构：

┌─────────────────────────────────────────────────────────────┐
│                  Hive Metastore (RDBMS)                     │
│                                                             │
│  TBLS Table (主表):                                          │
│  ┌──────────┬────────────┬─────────────┬──────────────┐    │
│  │ TBL_ID   │ DB_ID      │ TBL_NAME    │ TBL_TYPE     │    │
│  ├──────────┼────────────┼─────────────┼──────────────┤    │
│  │ 1001     │ 1          │ orders      │ EXTERNAL_TBL │    │
│  │ 1002     │ 1          │ customers   │ EXTERNAL_TBL │    │
│  └──────────┴────────────┴─────────────┴──────────────┘    │
│                                                             │
│  TABLE_PARAMS Table (表属性):                                │
│  ┌──────────┬────────────────┬──────────────────────────┐  │
│  │ TBL_ID   │ PARAM_KEY      │ PARAM_VALUE              │  │
│  ├──────────┼────────────────┼──────────────────────────┤  │
│  │ 1001     │ table_type     │ ICEBERG                  │  │
│  │ 1001     │ metadata_loc.. │ hdfs://.../v5.metadata...│←─┼─┐
│  │ 1001     │ numFiles       │ 120                      │  │ │
│  │ 1001     │ numRows        │ 1000000                  │  │ │
│  │ 1001     │ totalSize      │ 52428800                 │  │ │
│  └──────────┴────────────────┴──────────────────────────┘  │ │
│                                                             │ │
│  SDS Table (StorageDescriptor):                             │ │
│  ┌──────────┬─────────┬──────────────────────────────────┐ │ │
│  │ SD_ID    │ CD_ID   │ LOCATION                         │ │ │
│  ├──────────┼─────────┼──────────────────────────────────┤ │ │
│  │ 2001     │ 3001    │ hdfs://namenode/warehouse/orders │ │ │
│  └──────────┴─────────┴──────────────────────────────────┘ │ │
└─────────────────────────────────────────────────────────────┘ │
                                                                │
                                          指向实际元数据文件 ────┘
                                                                │
┌─────────────────────────────────────────────────────────────┐ │
│                    文件系统 (HDFS/S3/...)                     │ │
│                                                             │ │
│  hdfs://namenode/warehouse/db1/orders/                      │ │
│  ├── metadata/                                              │ │
│  │   ├── version-hint.text      (内容: "5")                 │ │
│  │   ├── v1.metadata.json                                   │ │
│  │   ├── v2.metadata.json                                   │ │
│  │   ├── v3.metadata.json                                   │ │
│  │   ├── v4.metadata.json                                   │ │
│  │   ├── v5.metadata.json  ←───────────────────────────────┘
│  │   │   {
│  │   │     "format-version": 2,
│  │   │     "table-uuid": "uuid-xxx",
│  │   │     "location": "hdfs://.../orders",
│  │   │     "last-sequence-number": 5,
│  │   │     "schema": {...},
│  │   │     "current-snapshot-id": 987654321,
│  │   │     "snapshots": [...]
│  │   │   }
│  │   ├── snap-987654321-1-abc.avro   # Snapshot清单
│  │   ├── def-m0.avro                 # Manifest文件
│  │   └── ghi-m1.avro
│  └── data/
│      ├── 00000-0-data-file1.parquet
│      ├── 00001-0-data-file2.parquet
│      └── ...
└─────────────────────────────────────────────────────────────┘

数据流向：
1. 用户查询 → HiveCatalog.loadTable()
2. HiveTableOperations.refresh()
3. 从HMS读取metadata_location → "hdfs://.../v5.metadata.json"
4. 从HDFS加载v5.metadata.json
5. 解析得到TableMetadata对象
6. 返回BaseTable包装TableOperations
```

### 4.6 HiveCatalog实现流程分析

#### 4.6.1 创建表流程

```
用户代码：
  catalog.createTable(
    TableIdentifier.of("db1", "orders"),
    schema,
    partitionSpec,
    properties)

调用栈：
1. HiveCatalog.createTable()
   └─ 继承自BaseMetastoreCatalog.createTable()

2. BaseMetastoreCatalog.createTable()
   ├─ 检查表是否已存在（调用HMS）
   ├─ 计算表location: warehouse/db1.db/orders
   ├─ 创建HiveTableOperations
   │  └─ 构造函数传入: conf, clients, fileIO, dbName, tableName
   ├─ 构建初始TableMetadata
   └─ 调用TableOperations.commit(null, metadata)

3. HiveTableOperations.doCommit(null, metadata)
   ├─ 写入v1.metadata.json到文件系统
   │  └─ 位置: warehouse/db1.db/orders/metadata/v1.metadata.json
   ├─ 获取HMS锁: lockObject().lock()
   │  └─ 调用HMS API: client.lock()
   ├─ 创建HMS Table对象:
   │  ├─ dbName = "db1"
   │  ├─ tableName = "orders"
   │  ├─ tableType = "EXTERNAL_TABLE"
   │  ├─ parameters:
   │  │  ├─ table_type = "ICEBERG"
   │  │  └─ metadata_location = "hdfs://.../v1.metadata.json"
   │  └─ sd.location = "hdfs://namenode/warehouse/db1.db/orders"
   ├─ 调用HMS API: client.createTable(hmsTable)
   └─ 释放锁: lockObject().unlock()

HMS中的变化：
Before:
  TBLS table: (无records)
  TABLE_PARAMS table: (无records)

After:
  TBLS table:
    TBL_ID=1001, DB_ID=1, TBL_NAME="orders", TBL_TYPE="EXTERNAL_TABLE"

  TABLE_PARAMS table:
    TBL_ID=1001, PARAM_KEY="table_type", PARAM_VALUE="ICEBERG"
    TBL_ID=1001, PARAM_KEY="metadata_location", PARAM_VALUE="hdfs://.../v1.metadata.json"

  SDS table:
    SD_ID=2001, LOCATION="hdfs://namenode/warehouse/db1.db/orders"

文件系统中的变化：
  warehouse/db1.db/orders/
    metadata/
      v1.metadata.json  (新创建)
    data/
      (空目录)
```

#### 4.6.2 加载表流程

```
用户代码：
  Table table = catalog.loadTable(TableIdentifier.of("db1", "orders"))

调用栈：
1. HiveCatalog.loadTable("db1.orders")
   └─ 继承自BaseMetastoreCatalog.loadTable()

2. BaseMetastoreCatalog.loadTable()
   ├─ 验证表标识符
   ├─ 调用newTableOps() → HiveTableOperations
   ├─ 调用ops.current() → 触发doRefresh()
   └─ 创建BaseTable包装ops

3. HiveTableOperations.doRefresh()
   ├─ 从HMS客户端池获取连接: clients.run()
   ├─ 调用HMS API: client.getTable("db1", "orders")
   ├─ 读取HMS Table对象:
   │  └─ parameters.get("metadata_location") → "hdfs://.../v5.metadata.json"
   ├─ 验证是否为Iceberg表:
   │  └─ parameters.get("table_type") == "ICEBERG" ✓
   ├─ 从文件系统加载元数据:
   │  └─ TableMetadataParser.read(fileIO, "hdfs://.../v5.metadata.json")
   └─ 更新缓存: currentMetadata

4. BaseTable构造
   ├─ 持有HiveTableOperations引用
   └─ 当前元数据来自v5.metadata.json

HMS查询SQL（内部）：
SELECT t.TBL_NAME, t.TBL_TYPE, sd.LOCATION, p.PARAM_KEY, p.PARAM_VALUE
FROM TBLS t
JOIN SDS sd ON t.SD_ID = sd.SD_ID
JOIN TABLE_PARAMS p ON t.TBL_ID = p.TBL_ID
WHERE t.DB_ID = (SELECT DB_ID FROM DBS WHERE NAME = 'db1')
  AND t.TBL_NAME = 'orders'
  AND p.PARAM_KEY IN ('table_type', 'metadata_location')
```

#### 4.6.3 并发写入流程（HMS锁机制）

```
场景：两个Spark作业同时向同一张表追加数据

Writer A                               HMS                               Writer B
──────────────────────────────────────────────────────────────────────────────────
loadTable("db1.orders")
  └─ refresh() → v5.metadata.json
appendFile(file1)
commit()
  ├─ writeNewMetadata() → v6.metadata.json
  ├─ lockObject().lock()
  │  └─ HMS API: lock(db1, orders, EXCLUSIVE)
  │        └─→ HMS分配lockId=100001
  │            插入HIVE_LOCKS表:
  │              LOCK_ID=100001
  │              DB=db1
  │              TABLE=orders
  │              STATE=ACQUIRED
  │                                                       loadTable("db1.orders")
  │                                                         └─ refresh() → v5
  │                                                       appendFile(file2)
  │                                                       commit()
  │                                                         ├─ writeNewMetadata() → v6.metadata.json
  │                                                         ├─ lockObject().lock()
  │                                                         │  └─ HMS API: lock(db1, orders, EXCLUSIVE)
  │                                                         │        └─→ HMS发现已有锁
  │                                                         │            插入HIVE_LOCKS表:
  │                                                         │              LOCK_ID=100002
  │                                                         │              DB=db1
  │                                                         │              TABLE=orders
  │                                                         │              STATE=WAITING ← 等待
  ├─ 从HMS读取当前metadata_location
  │  └─ SELECT PARAM_VALUE FROM TABLE_PARAMS
  │     WHERE PARAM_KEY='metadata_location'
  │     → "hdfs://.../v5.metadata.json"
  ├─ 检查base == current ✓ (v5 == v5)
  ├─ 更新HMS:
  │  └─ UPDATE TABLE_PARAMS
  │     SET PARAM_VALUE='hdfs://.../v6.metadata.json'
  │     WHERE PARAM_KEY='metadata_location'
  ├─ HMS API: alter_table(db1, orders, hmsTable)
  │  └─→ HMS提交事务，v6.metadata.json生效
  └─ lockObject().unlock()
     └─ HMS API: unlock(100001)
           └─→ HMS删除HIVE_LOCKS中的记录
               更新LOCK_ID=100002的STATE=ACQUIRED
                                                                                   锁被唤醒
                                                                                   ├─ 从HMS读取当前metadata_location
                                                                                   │  └─ "hdfs://.../v6.metadata.json" (已变化!)
                                                                                   ├─ 检查base == current ✗ (v5 != v6)
                                                                                   └─ 抛出CommitFailedException

Writer B处理冲突：
  - 捕获CommitFailedException
  - 删除已写入的v6.metadata.json文件
  - 重新refresh()获取最新元数据 → v6
  - 基于v6重新构建snapshot
  - 重新写入v7.metadata.json
  - 重新提交commit(base=v6, new=v7)

最终HMS中的metadata_location演进：
v5 (初始) → v6 (Writer A成功) → v7 (Writer B重试成功)
```

---

## 小结（第一部分）

本文档第一部分深入分析了Apache Iceberg Catalog架构的核心设计和两种基于文件系统/Metastore的实现：

### 核心设计模式总结

1. **模板方法模式**：BaseMetastoreCatalog定义了表操作的骨架流程
2. **工厂方法模式**：newTableOps()由子类实现，创建不同的TableOperations
3. **策略模式**：不同的Catalog采用不同的存储和并发控制策略
4. **委托模式**：Catalog委托TableOperations处理底层元数据操作

### HadoopCatalog vs HiveCatalog对比

| 维度 | HadoopCatalog | HiveCatalog |
|------|---------------|-------------|
| **元数据存储** | 文件系统目录结构 | HMS (RDBMS) + 文件系统 |
| **并发控制** | 文件系统原子rename + LockManager | HMS事务 + HMS锁 |
| **表发现** | 遍历目录 (较慢) | HMS查询 (快速) |
| **rename支持** | ❌ 不支持 | ✅ 支持 |
| **外部依赖** | 无 | Hive Metastore |
| **适用场景** | 测试、小规模 | 企业级生产环境 |
| **性能** | 一般 | 优秀 |
| **视图支持** | ❌ 不支持 | ✅ 支持 |

### 元数据版本化机制

两种Catalog都使用**不可变元数据 + 版本号**的设计：
- v1.metadata.json → v2.metadata.json → v3.metadata.json → ...
- 每次修改都生成新版本，旧版本保留用于time travel
- commit()通过检查base版本号实现乐观锁

### 并发安全保证

**HadoopCatalog**:
```
乐观锁检查 → LockManager加锁 → 原子rename → 释放锁
```

**HiveCatalog**:
```
HMS锁获取 → 乐观锁检查 → HMS事务更新 → 释放锁
```

---

**下一部分预告**：
- RESTCatalog：云原生HTTP架构
- NessieCatalog：Git-like版本控制
- 四种Catalog的全面对比分析
- 选型指南和最佳实践

---

**文档未完待续，请查看第二部分...**
