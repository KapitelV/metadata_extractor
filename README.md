# SQL元数据提取与数据血缘管理系统

## 📋 项目概述

SQL元数据提取与数据血缘管理系统是一个功能完整的数据仓库元数据管理工具，能够：

- ✅ 自动解析DDL和DML语句，提取表和字段元数据
- ✅ 智能处理数据冲突（DDL vs DDL, DML vs DDL, DDL vs DML, DML vs DML）
- ✅ 构建依赖关系图，自动识别目标表和来源表
- ✅ 追踪数据血缘关系，支持多目标表场景
- ✅ 管理SQL脚本信息，支持脚本与表的多对多关系
- ✅ 提供数据血缘可视化工具（静态图+交互式图）

**支持30+种SQL方言**，包括MySQL、PostgreSQL、Oracle、Teradata、Hive、Spark SQL等。

---

## 🚀 快速开始

### 1. 初始化数据库

```bash
python init_sqlite.py --force-reset
```

### 2. 处理单个SQL文件

```bash
python sql_file_processor.py my_etl.sql teradata
```

### 3. 批量处理目录

```python
from sql_file_processor import process_sql_directory

result = process_sql_directory(
    directory_path='./sql_scripts',
    dialect='teradata',
    mode='clear',  # 'clear' 或 'insert'
    db_path='dw_metadata.db',
    lineage_json_path='datalineage.json',
    log_file='sql_extractor.log'
)

print(f"成功: {result['success']}")
print(f"错误: {result['errors']}")
```

### 4. 生成血缘可视化

```bash
# 交互式HTML（推荐）
python lineage_viz_interactive.py datalineage.json

# 静态图（SVG/PNG/PDF）
python lineage_viz.py datalineage.json -f png
```

---

## 📦 核心模块

| 模块 | 说明 |
|------|------|
| `metadata_extractor.py` | 元数据提取核心模块（DDL/DML解析） |
| `sql_file_processor.py` | SQL文件处理和血缘分析主模块 |
| `init_sqlite.py` | 数据库初始化和验证脚本 |
| `sqlite_schema.sql` | 数据库Schema定义 |
| `lineage_visualizer/` | 数据血缘可视化工具包 |

---

## 🔧 核心功能

### 1. 元数据提取

#### DDL语句支持

**CREATE TABLE**
```sql
CREATE TABLE db_schema.users (
    user_id INT NOT NULL AUTO_INCREMENT COMMENT '用户ID',
    username VARCHAR(50) NOT NULL COMMENT '用户名',
    email VARCHAR(100) COMMENT '电子邮件',
    PRIMARY KEY (user_id)
) COMMENT='用户表';
```

**CREATE VIEW**
```sql
CREATE VIEW db_schema.user_summary AS
SELECT user_id, username FROM users;
```

**CREATE TEMPORARY TABLE**
```sql
CREATE TEMPORARY TABLE temp_data AS
SELECT * FROM source_table;
```

**CREATE VOLATILE TABLE (Teradata)**
```sql
CREATE MULTISET VOLATILE TABLE VT_2_65536, NO LOG (
    CAMP_ID VARCHAR(60) NOT NULL CASESPECIFIC /* 营销活动编号 */
) PRIMARY INDEX (CAMP_ID) ON COMMIT PRESERVE ROWS;
```

#### DML语句支持

**INSERT INTO ... SELECT**
```sql
INSERT INTO target_db.customer_dim (
    customer_id,  /* 客户ID */
    customer_name,  /* 客户名称 */
    email  /* 邮箱地址 */
)
SELECT 
    c.id,
    c.name,
    c.email
FROM source_db.customers c
WHERE c.status = 'active';
```

**UPDATE ... SET**
```sql
UPDATE employees
SET 
    salary = salary * 1.1,  /* 工资 */
    updated_at = CURRENT_TIMESTAMP  /* 更新时间 */
WHERE department_id = 100;
```

**MERGE INTO**
```sql
MERGE INTO target_customers t
USING source_customers s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN
    UPDATE SET 
        name = s.name,  /* 客户名称 */
        email = s.email  /* 电子邮件 */
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email)
    VALUES (s.customer_id, s.name, s.email);
```

### 2. 表类型识别

| SQL语句 | 有schema | 无schema | 结果 |
|---------|----------|----------|------|
| `CREATE TABLE` | ✅ | ✅ | `TABLE` |
| `CREATE VIEW` | ✅ | ✅ | `VIEW` |
| `CREATE TEMPORARY TABLE` | ✅ | ✅ | `TMP_TABLE` |
| `CREATE VOLATILE TABLE` | ✅ | ✅ | `TMP_TABLE` |
| `INSERT/UPDATE/MERGE` | ✅ | ❌ | `TABLE` |
| `INSERT/UPDATE/MERGE` | ❌ | ✅ | `TMP_TABLE` |

### 3. 数据冲突处理策略

| 数据库 | 新数据 | 处理策略 |
|--------|--------|----------|
| DDL | DDL | ❌ 报错：不允许重复定义 |
| DML | DDL | ✅ DDL覆盖，保留DML的col_cn_nm |
| DDL | DML | ✅ DDL保持，补充col_cn_nm，新字段报错 |
| DML | DML | ✅ 去重合并，冲突报错，有值覆盖无值 |

### 4. ID生成规则

**tables表**
- 实体表：`{schema_name}__{table_name}__`
- 临时表：`{schema_name}__{table_name}__{script_id}`（无schema时为`__{table_name}__{script_id}`）

**columns表**
- 实体表：`{schema_name}__{table_name}____{column_name}`
- 临时表：`{schema_name}__{table_name}__{script_id}__{column_name}`

**sql_scripts表**
- `id = script_name`（不含扩展名）

**data_lineage表**
- `id = {target_table_id}__{source_table_id}__{script_id}`

---

## 🌐 数据血缘追踪

### 目标表识别逻辑（三级优先级）

1. **优先级1：入度>0的非临时表**
   - 条件：`in_degree > 0` AND `is_temp_table == False`
   - 含义：有数据流入的实体表（真正的ETL目标）

2. **优先级2：出度=0的表**
   - 条件：`out_degree == 0`
   - 触发：优先级1未找到任何表
   - 含义：依赖图的最终节点

3. **优先级3：空集合**
   - 如果前两个策略都未找到，返回空集合，触发"未能识别到目标表"错误

### 来源表识别

- **规则：** 入度为0的表
- **处理：** 自动创建外部表记录（如果来源表不在数据库中）

### 多目标表支持

系统支持一个SQL脚本操作多个目标表的场景：
- 每个脚本只有一条`sql_scripts`记录
- 脚本与目标表通过`data_lineage`表关联（多对多关系）
- 为每个目标表和来源表的组合创建血缘记录

### 依赖图构建

- **节点：** 表（完整名称：schema.table或table）
- **边：** 来源表 → 目标表
- **输出：** `{文件名}_graph.json`（NetworkX格式）

### 全局血缘图维护

**文件：** `datalineage.json`（NetworkX node-link格式）

**功能：**
- 维护整个系统的表血缘关系图
- 累积更新，不覆盖
- 边属性包含`script_paths`列表（记录所有相关脚本路径）

**边属性格式：**
```json
{
  "source": "source_table",
  "target": "target_table",
  "script_paths": [
    "path/to/script1.sql",
    "path/to/script2.sql"
  ]
}
```

---

## 📊 数据库架构

### 核心表结构

#### 1. databases - 数据库/Schema信息
```sql
CREATE TABLE databases (
    id TEXT PRIMARY KEY,  -- 与schema_name一致
    name TEXT NOT NULL,
    description TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

#### 2. tables - 表元数据
```sql
CREATE TABLE tables (
    id TEXT PRIMARY KEY,  -- 'SCHEMA_NAME'__'TABLE_NAME'__'SCRIPT_ID'
    database_id TEXT REFERENCES databases(id),
    schema_name TEXT,
    script_id TEXT,  -- 临时表的脚本ID，实体表为空字符串
    table_name TEXT NOT NULL,
    table_type TEXT,  -- TABLE, VIEW, TMP_TABLE
    description TEXT,
    business_purpose TEXT,
    data_source TEXT,  -- DDL, DML, EXTERNAL
    refresh_frequency TEXT,  -- REALTIME, HOURLY, DAILY, WEEKLY
    row_count INTEGER,
    data_size_mb REAL,
    last_updated DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(database_id, schema_name, table_name, script_id)
);
```

#### 3. columns - 字段元数据
```sql
CREATE TABLE columns (
    id TEXT PRIMARY KEY,  -- 'SCHEMA_NAME'__'TABLE_NAME'__'SCRIPT_ID'__'COLUMN_NAME'
    table_id TEXT REFERENCES tables(id),
    column_name TEXT NOT NULL,
    data_type TEXT,
    max_length INTEGER,
    is_nullable INTEGER,  -- 0 = false, 1 = true
    default_value TEXT,
    is_primary_key INTEGER,
    is_foreign_key INTEGER,
    description TEXT,
    ordinal_position INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

#### 4. sql_scripts - SQL脚本信息
```sql
CREATE TABLE sql_scripts (
    id TEXT PRIMARY KEY,  -- 脚本名称（不含扩展名）
    script_name TEXT,
    script_content TEXT NOT NULL,
    script_type TEXT,
    script_purpose TEXT,
    author TEXT,
    description TEXT,
    execution_frequency TEXT,  -- REALTIME, HOURLY, DAILY, WEEKLY
    execution_order INTEGER,
    is_active INTEGER DEFAULT 1,
    last_executed DATETIME,
    avg_execution_time_seconds INTEGER,
    performance_stats_json TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

#### 5. data_lineage - 数据血缘关系
```sql
CREATE TABLE data_lineage (
    id TEXT PRIMARY KEY,  -- 'TARGET_TABLE_ID'__'SOURCE_TABLE_ID'__'SCRIPT_ID'
    target_table_id TEXT REFERENCES tables(id),
    source_table_id TEXT REFERENCES tables(id),
    script_id TEXT REFERENCES sql_scripts(id),
    lineage_type TEXT,
    transformation_logic TEXT,
    columns_mapping_json TEXT,
    filter_conditions TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

#### 6. script_dependencies - 脚本依赖关系
```sql
CREATE TABLE script_dependencies (
    id TEXT PRIMARY KEY,  -- 'SOURCE_TABLE_ID'__'SCRIPT_ID'
    script_id TEXT REFERENCES sql_scripts(id),
    source_table_id TEXT REFERENCES tables(id),
    dependency_type TEXT,
    usage_pattern TEXT,
    columns_used_json TEXT,
    join_conditions TEXT,
    filter_conditions TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

### 视图

#### v_table_complete_info
表的完整信息（包括列数和脚本数）
```sql
SELECT 
    t.id as table_id,
    d.name as database_name,
    t.schema_name,
    t.table_name,
    t.table_type,
    COUNT(DISTINCT c.id) as column_count,
    COUNT(DISTINCT dl.script_id) as script_count
FROM tables t
LEFT JOIN databases d ON t.database_id = d.id
LEFT JOIN columns c ON t.id = c.table_id
LEFT JOIN data_lineage dl ON t.id = dl.target_table_id
GROUP BY t.id, ...
```

#### v_script_dependencies_detail
脚本依赖关系详情
```sql
SELECT 
    s.id as script_id,
    s.script_name,
    source_t.schema_name as source_schema,
    source_t.table_name as source_table
FROM sql_scripts s
JOIN script_dependencies sd ON s.id = sd.script_id
JOIN tables source_t ON sd.source_table_id = source_t.id;
```

#### v_data_lineage_detail
数据血缘关系详情
```sql
SELECT 
    dl.id as lineage_id,
    source_t.schema_name || '.' || source_t.table_name as source_table,
    target_t.schema_name || '.' || target_t.table_name as target_table,
    s.script_name
FROM data_lineage dl
JOIN tables source_t ON dl.source_table_id = source_t.id
JOIN tables target_t ON dl.target_table_id = target_t.id
LEFT JOIN sql_scripts s ON dl.script_id = s.id;
```

---

## 🎯 使用示例

### Python代码示例

```python
from sql_file_processor import process_sql_file

# 处理SQL文件
success, error_msg = process_sql_file(
    sql_file_path='my_etl.sql',
    dialect='teradata',
    db_path='dw_metadata.db'
)

if success:
    print("✅ 处理成功")
    # 生成的文件：
    # - my_etl_graph.json (依赖图)
    # - datalineage.json (全局血缘图)
else:
    print(f"❌ 失败: {error_msg}")
```

### 查询脚本的所有目标表

```sql
SELECT 
    s.script_name,
    t.schema_name,
    t.table_name,
    t.table_type
FROM sql_scripts s
JOIN data_lineage dl ON s.id = dl.script_id
JOIN tables t ON dl.target_table_id = t.id
WHERE s.id = 'your_script_name'
GROUP BY t.id;
```

### 查询脚本的所有来源表

```sql
SELECT 
    s.script_name,
    t.schema_name,
    t.table_name
FROM sql_scripts s
JOIN data_lineage dl ON s.id = dl.script_id
JOIN tables t ON dl.source_table_id = t.id
WHERE s.id = 'your_script_name'
GROUP BY t.id;
```

### 查询表的完整血缘关系

```sql
SELECT 
    s.script_name,
    source_t.schema_name || '.' || source_t.table_name as source_table,
    target_t.schema_name || '.' || target_t.table_name as target_table,
    dl.lineage_type
FROM data_lineage dl
JOIN sql_scripts s ON dl.script_id = s.id
JOIN tables source_t ON dl.source_table_id = source_t.id
JOIN tables target_t ON dl.target_table_id = target_t.id
WHERE s.id = 'your_script_name'
ORDER BY target_table, source_table;
```

---

## 📊 数据血缘可视化

### 快速使用

```bash
# 交互式HTML（推荐，支持拖拽、缩放、搜索）
python lineage_viz_interactive.py datalineage.json

# 静态图（SVG/PNG/PDF）
python lineage_viz.py datalineage.json -f png

# 查看统计信息
python lineage_viz.py datalineage.json --stats-only
```

### 主要功能

- ✅ **两种可视化方式**：静态图（Graphviz）和交互式图（Pyvis）
- ✅ **多种输出格式**：SVG, PNG, PDF, JPG, HTML
- ✅ **自动着色**：按Schema分组和着色
- ✅ **聚焦模式**：显示指定节点的上下游关系
- ✅ **过滤功能**：按Schema、表名模式过滤
- ✅ **血缘追溯**：上游追溯和下游影响分析

### 常用命令

```bash
# 聚焦某个表（显示上下游2层）
python lineage_viz.py datalineage.json \
  --focus "MDB_AL.TABLE_NAME" \
  --upstream 2 \
  --downstream 2 \
  -f png

# 追溯数据来源（只看上游3层）
python lineage_viz.py datalineage.json \
  --focus "MDB_AL.TABLE_NAME" \
  --upstream 3 \
  --downstream 0 \
  -o upstream_trace

# 分析影响范围（只看下游3层）
python lineage_viz.py datalineage.json \
  --focus "CDBVIEW.TABLE_NAME" \
  --upstream 0 \
  --downstream 3 \
  -o downstream_impact

# 按业务域查看
python lineage_viz.py datalineage.json \
  --schemas MDB_AL CDBVIEW \
  -o business_domain
```

更多详细信息请参考 `lineage_visualizer/README.md`

---

## 📖 常用命令

### 数据库操作

```bash
# 重置数据库
python init_sqlite.py --force-reset

# 验证数据库结构
python init_sqlite.py
```

### SQL文件处理

```bash
# MySQL
python sql_file_processor.py script.sql mysql

# PostgreSQL
python sql_file_processor.py script.sql postgres

# Teradata
python sql_file_processor.py script.sql teradata

# Oracle
python sql_file_processor.py script.sql oracle
```

### 数据库查询

```sql
-- 查看所有表
SELECT * FROM v_table_complete_info;

-- 查看血缘关系
SELECT * FROM v_data_lineage_detail;

-- 查看脚本依赖
SELECT * FROM v_script_dependencies_detail;

-- 查询特定表的血缘
SELECT 
    source_t.table_name as source_table,
    target_t.table_name as target_table,
    s.script_name
FROM data_lineage dl
JOIN tables source_t ON dl.source_table_id = source_t.id
JOIN tables target_t ON dl.target_table_id = target_t.id
JOIN sql_scripts s ON dl.script_id = s.id;
```

---

## ⚠️ 注意事项

### 1. 空文件处理

如果SQL文件为空，系统会返回成功（`True, ''`），不会报错。

### 2. 外部表自动创建

当来源表不在数据库中时，系统会自动创建外部表记录（`data_source='EXTERNAL'`），确保血缘关系不丢失。如果后续有实际定义，会自动覆盖外部表记录。

### 3. 临时表处理

临时表通过`script_id`字段区分，即使同名也不会冲突。临时表的ID格式：`{schema_name}__{table_name}__{script_id}`

### 4. SQL方言支持

通过sqlglot库支持30+种SQL方言：
- MySQL / MariaDB
- PostgreSQL
- Oracle
- SQL Server (T-SQL)
- Teradata
- Hive
- Spark SQL
- Snowflake
- BigQuery
- Redshift
- ... 更多

### 5. 性能特点

- ✅ 高效解析：使用sqlglot进行语法分析
- ✅ 事务保证：使用数据库事务，失败自动回滚
- ✅ 增量更新：支持冲突检测和合并，不覆盖已有数据
- ✅ 批量处理：支持目录批量处理，带日志记录

---

## 🔍 故障排查

### 问题1: "未能识别到目标表"

**原因：** 所有表都有入边（被其他表依赖），或依赖图分析失败

**解决：** 检查SQL逻辑，确保有最终的输出表（实体表且有数据流入）

### 问题2: SQL解析失败

**原因：** SQL语法错误或使用了不支持的语法

**解决：** 
- 检查SQL语法是否正确
- 确认SQL方言参数是否正确
- 某些复杂语法可能不被sqlglot支持（这是库的限制）

### 问题3: networkx警告

```
FutureWarning: The default value will be edges="edges" in NetworkX 3.6
```

**说明：** 这是networkx版本兼容性警告，不影响功能

**解决：** 可忽略，或升级到最新版networkx

---

## 🛠️ 技术特点

### 1. 智能冲突处理
- 4种冲突场景的精确处理（DDL vs DDL, DML vs DDL, DDL vs DML, DML vs DML）
- 保护数据完整性，避免数据丢失

### 2. 依赖图分析
- 基于图论的智能识别（NetworkX）
- 三级优先级策略识别目标表
- 自动识别来源表并创建外部表记录

### 3. 多目标表支持
- 一个脚本可以操作多个目标表
- 通过`data_lineage`表实现多对多关系
- 灵活的查询支持

### 4. 累积式血缘
- 支持多脚本的血缘合并
- 全局血缘图累积更新
- 边属性记录所有相关脚本路径

### 5. 完整的类型提示
- 所有函数都有完整的类型注解
- 提高代码可维护性

---

## 📈 项目统计

- **核心代码：** ~3000行
- **支持的SQL类型：** DDL (CREATE TABLE/VIEW/TEMPORARY), DML (INSERT/UPDATE/MERGE)
- **数据库表：** 7张核心表 + 3个视图
- **SQL方言支持：** 30+种

---

## 🎉 项目成果

### ✅ 已完成功能

1. ✅ 完整的元数据提取系统（DDL和DML）
2. ✅ 智能冲突处理机制（4种场景）
3. ✅ 依赖图自动构建（NetworkX）
4. ✅ 数据血缘追踪（支持多目标表）
5. ✅ 全局血缘图维护（累积更新）
6. ✅ 外部表自动创建（确保血缘完整性）
7. ✅ 批量处理支持（目录扫描+日志记录）
8. ✅ 数据血缘可视化工具（静态图+交互式图）

### 🏆 核心能力

- **元数据提取：** 支持DDL和DML的完整元数据提取，包括字段注释
- **冲突处理：** 智能处理4种数据冲突场景
- **血缘追踪：** 自动构建和维护数据血缘关系，支持多目标表
- **依赖分析：** 基于图论的依赖关系分析，三级优先级策略
- **脚本管理：** 完整的SQL脚本信息管理，支持多对多关系
- **可视化：** 强大的血缘关系可视化工具

---

## 📚 相关文件

- `sqlite_schema.sql` - 数据库架构定义
- `init_sqlite.py` - 数据库初始化和验证
- `sql_file_processor.py` - SQL文件处理核心逻辑
- `metadata_extractor.py` - SQL元数据提取
- `lineage_visualizer/` - 数据血缘可视化工具包
- `datalineage.json` - 全局血缘图（NetworkX格式）
- `dw_metadata.db` - SQLite数据库文件

---

**项目状态：** ✅ 完成并可投入使用  
**版本：** v2.0  
**最后更新：** 2025-10-31

---

**开始使用：** 运行 `python sql_file_processor.py your_script.sql teradata` 即可开始！
