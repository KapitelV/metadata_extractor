# SQL元数据提取与数据血缘管理系统

## 📋 项目概述

SQL元数据提取与数据血缘管理系统是一个功能完整的数据仓库元数据管理工具，提供：

- ✅ 自动解析DDL和DML语句，提取表和字段元数据
- ✅ 智能处理数据冲突（DDL vs DDL, DML vs DDL, DDL vs DML, DML vs DML）
- ✅ **双层血缘管理**：Detail（语句级，含临时表）+ Summary（脚本级，仅实体表）
- ✅ **增量更新支持**：自动清理旧数据，保证数据一致性
- ✅ **JSON导出功能**：标准NetworkX格式，支持可视化和分析
- ✅ 构建依赖关系图，自动识别目标表和来源表
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

处理完成后自动生成：
- 元数据存入SQLite数据库
- Detail和Summary血缘关系
- JSON格式的血缘图（`./datalineage/scripts/`）

### 3. 批量处理目录

```python
from sql_file_processor import process_sql_directory

result = process_sql_directory(
    directory_path='./sql_scripts',
    dialect='teradata',
    mode='clear',  # 'clear' 或 'insert'
    db_path='dw_metadata.db'
)

print(f"成功: {result['success']}")
print(f"错误: {result['errors']}")
```

### 4. 导出全局血缘

```bash
# 导出所有脚本的合并血缘
python export_all_lineage.py
```

输出文件：
- `./datalineage/all_lineage_detail.json` - 详细血缘（含临时表）
- `./datalineage/all_lineage_summary.json` - 汇总血缘（仅实体表）

### 5. 生成血缘可视化

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
| `lineage_graph_manager.py` | 血缘图管理（Detail→Summary推导，JSON导出） |
| `export_all_lineage.py` | 全局血缘导出工具 |
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

### 2. 数据冲突处理策略

| 数据库 | 新数据 | 处理策略 |
|--------|--------|----------|
| DDL | DDL | ❌ 报错：不允许重复定义 |
| DML | DDL | ✅ DDL覆盖，保留DML的col_cn_nm |
| DDL | DML | ✅ DDL保持，补充col_cn_nm，新字段报错 |
| DML | DML | ✅ 去重合并，冲突报错，有值覆盖无值 |

### 3. 表类型识别

| SQL语句 | 有schema | 无schema | 结果 |
|---------|----------|----------|------|
| `CREATE TABLE` | ✅ | ✅ | `TABLE` |
| `CREATE VIEW` | ✅ | ✅ | `VIEW` |
| `CREATE TEMPORARY TABLE` | ✅ | ✅ | `TMP_TABLE` |
| `CREATE VOLATILE TABLE` | ✅ | ✅ | `TMP_TABLE` |
| `INSERT/UPDATE/MERGE` | ✅ | ❌ | `TABLE` |

---

## 📊 数据库架构

### 核心表结构

#### 1. databases - 数据库/Schema信息
```sql
CREATE TABLE databases (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

#### 2. tables - 表元数据
```sql
CREATE TABLE tables (
    id TEXT PRIMARY KEY,
    database_id TEXT REFERENCES databases(id),
    schema_name TEXT,
    script_id TEXT,  -- 临时表的脚本ID，实体表为空
    table_name TEXT NOT NULL,
    table_type TEXT,  -- TABLE, VIEW, TMP_TABLE
    description TEXT,
    data_source TEXT,  -- DDL, DML, EXTERNAL
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

#### 3. columns - 字段元数据
```sql
CREATE TABLE columns (
    id TEXT PRIMARY KEY,
    table_id TEXT REFERENCES tables(id),
    column_name TEXT NOT NULL,
    data_type TEXT,
    is_nullable INTEGER,
    is_primary_key INTEGER,
    description TEXT,
    ordinal_position INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

#### 4. sql_scripts - SQL脚本信息
```sql
CREATE TABLE sql_scripts (
    id TEXT PRIMARY KEY,
    script_name TEXT,
    script_content TEXT NOT NULL,
    script_type TEXT,
    is_active INTEGER DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

#### 5. script_statements - 脚本语句（分段）
```sql
CREATE TABLE script_statements (
    id TEXT PRIMARY KEY,
    script_id TEXT REFERENCES sql_scripts(id),
    statement_index INTEGER NOT NULL,
    statement_type TEXT,
    statement_content TEXT NOT NULL,
    target_table_id TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

#### 6. data_lineage_detail - 详细血缘（语句级）
```sql
CREATE TABLE data_lineage_detail (
    id TEXT PRIMARY KEY,
    target_table_id TEXT REFERENCES tables(id),
    source_table_id TEXT REFERENCES tables(id),
    script_id TEXT REFERENCES sql_scripts(id),
    statement_id TEXT REFERENCES script_statements(id),
    transformation_logic TEXT,
    filter_conditions TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

#### 7. data_lineage_summary - 汇总血缘（脚本级）
```sql
CREATE TABLE data_lineage_summary (
    id TEXT PRIMARY KEY,
    target_table_id TEXT REFERENCES tables(id),
    source_table_id TEXT REFERENCES tables(id),
    script_id TEXT REFERENCES sql_scripts(id),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

### 重要视图

- `v_data_lineage` - 汇总血缘关系详情
- `v_data_lineage_statements` - 详细血缘关系（按语句）
- `v_column_lineage` - 字段级血缘
- `v_temp_table_lifecycle` - 临时表生命周期
- `v_script_execution_flow` - 脚本执行流程
- `v_table_complete_info` - 表的完整信息

---

## 🎯 双层血缘设计

### Detail层（详细血缘）

**特点**：
- ✅ 语句级粒度（每条SQL语句一条记录）
- ✅ 包含所有表（实体表 + 临时表）
- ✅ 记录完整的数据加工链路

**用途**：
- 详细的数据流转分析
- 调试和问题排查
- 理解复杂的ETL逻辑

**示例**：
```
TBL_SOURCE_A → VT_TEMP1 (语句1)
VT_TEMP1 → VT_TEMP2 (语句2)
VT_TEMP2 → TBL_TARGET (语句3)
```

### Summary层（汇总血缘）

**特点**：
- ✅ 脚本级粒度（每个脚本一条记录）
- ✅ 仅包含实体表（跳过临时表）
- ✅ 自动从Detail层推导生成

**用途**：
- 高层次的血缘关系查看
- 业务理解和沟通
- 影响分析和追溯

**示例**：
```
TBL_SOURCE_A → TBL_TARGET (脚本级)
```

### 自动推导

系统使用NetworkX图算法自动从Detail推导Summary：
1. 构建Detail层有向图
2. 识别实体表节点
3. 查找实体表之间的所有路径
4. 生成Summary层边记录

---

## 🔄 增量更新支持

### 功能说明

系统完全支持增量更新，当重新处理脚本时：

✅ **自动清理旧数据**
- 删除该脚本的旧Summary记录
- 删除该脚本的旧Detail记录
- 删除该脚本的旧语句记录
- 更新脚本信息

✅ **插入新数据**
- 插入新的语句记录
- 插入新的血缘记录
- 自动生成新的Summary

✅ **事务保护**
- 所有操作在事务内执行
- 失败自动回滚
- 保证数据一致性

### 使用示例

```python
# 第一次处理
process_sql_file('my_script.sql', dialect='hive')

# 修改脚本后，第二次处理
process_sql_file('my_script.sql', dialect='hive')
# 自动清理旧数据，插入新数据，保证一致性
```

---

## 📁 JSON导出功能

### 自动导出（单个脚本）

处理每个脚本时自动导出JSON：

```python
process_sql_file('my_script.sql', dialect='hive')
```

输出文件：
```
./datalineage/scripts/
├── my_script_detail.json    # 详细血缘（含临时表）
└── my_script_summary.json   # 汇总血缘（仅实体表）
```

### 手动导出（全局血缘）

导出所有脚本的合并血缘：

```python
from export_all_lineage import export_all_lineage_json
export_all_lineage_json()
```

或命令行：
```bash
python export_all_lineage.py
```

输出文件：
```
./datalineage/
├── all_lineage_detail.json   # 所有脚本的详细血缘
└── all_lineage_summary.json  # 所有脚本的汇总血缘
```

### JSON格式

使用NetworkX标准的`node_link`格式：

```json
{
  "directed": true,
  "multigraph": false,
  "graph": {},
  "nodes": [
    {
      "id": "DW__TBL_SOURCE__",
      "schema_name": "DW",
      "table_name": "TBL_SOURCE",
      "node_type": "TABLE",
      "is_entity": true
    },
    {
      "id": "__VT_TEMP__my_script",
      "schema_name": "",
      "table_name": "VT_TEMP",
      "node_type": "TMP_TABLE",
      "is_entity": false
    }
  ],
  "links": [
    {
      "source": "DW__TBL_SOURCE__",
      "target": "__VT_TEMP__my_script",
      "edge_type": "statement",
      "script_id": "my_script",
      "statement_id": "my_script__STMT_001",
      "statement_index": 1,
      "statement_type": "CREATE"
    }
  ]
}
```

**节点属性说明**：
- `id`: table_id（唯一标识，格式：`{schema}__{table}__{script_id}`）
- `schema_name`: schema名称（可能为空字符串）
- `table_name`: 表名
- `node_type`: 表类型（TABLE/VIEW/TMP_TABLE）
- `is_entity`: 是否为实体表（实体表为true，临时表为false）

**边属性说明**：
- `source`: 源表的table_id（非表名）
- `target`: 目标表的table_id（非表名）
- `edge_type`: 边类型（statement/script）
- `script_id`: 脚本ID

### 读取和使用

```python
import json
import networkx as nx
from networkx.readwrite import json_graph

# 读取JSON文件
with open('./datalineage/all_lineage_summary.json', 'r') as f:
    data = json.load(f)

# 转换为NetworkX图
graph = json_graph.node_link_graph(data)

# 访问节点属性
for node_id, attrs in graph.nodes(data=True):
    print(f"表ID: {node_id}")
    print(f"  Schema: {attrs['schema_name']}")
    print(f"  Table: {attrs['table_name']}")
    print(f"  Type: {attrs['node_type']}")

# 查找上游表（使用table_id）
target_table_id = 'DW__TBL_TARGET__'
if graph.has_node(target_table_id):
    upstream = nx.ancestors(graph, target_table_id)
    print(f"\n上游表:")
    for table_id in upstream:
        attrs = graph.nodes[table_id]
        print(f"  - {attrs['schema_name']}.{attrs['table_name']}")

# 查找下游表（使用table_id）
source_table_id = 'STG__TBL_SOURCE__'
if graph.has_node(source_table_id):
    downstream = nx.descendants(graph, source_table_id)
    print(f"\n下游表:")
    for table_id in downstream:
        attrs = graph.nodes[table_id]
        print(f"  - {attrs['schema_name']}.{attrs['table_name']}")
```

### 应用场景

1. **可视化** - 使用D3.js、Cytoscape.js等工具
2. **分析** - 使用NetworkX进行图分析
3. **查询** - 快速查找上下游依赖
4. **导入** - 导入到其他系统
5. **备份** - 作为血缘数据的备份格式

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
```

更多详细信息请参考 `lineage_visualizer/README.md`

---

## 🎯 使用示例

### 基本使用

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
else:
    print(f"❌ 失败: {error_msg}")
```

### 查询血缘关系

```sql
-- 查看汇总血缘
SELECT * FROM v_data_lineage;

-- 查看详细血缘（按语句）
SELECT * FROM v_data_lineage_statements;

-- 查询特定表的上游
SELECT DISTINCT
    source_schema || '.' || source_table as upstream
FROM v_data_lineage
WHERE target_schema || '.' || target_table = 'MY_SCHEMA.MY_TABLE';

-- 查询特定表的下游
SELECT DISTINCT
    target_schema || '.' || target_table as downstream
FROM v_data_lineage
WHERE source_schema || '.' || source_table = 'MY_SCHEMA.MY_TABLE';
```

### 批量处理和导出

```python
from sql_file_processor import process_sql_directory
from export_all_lineage import export_all_lineage_json

# 1. 批量处理SQL文件
result = process_sql_directory(
    directory_path='./sql_scripts',
    dialect='hive'
)

print(f"成功: {result['success']}, 失败: {len(result['errors'])}")

# 2. 导出全局血缘JSON
export_all_lineage_json()

print("✅ 所有脚本处理完成，血缘已导出！")
```

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

# Hive
python sql_file_processor.py script.sql hive
```

### 导出操作

```bash
# 导出全局血缘JSON
python export_all_lineage.py

# 生成血缘可视化
python lineage_viz_interactive.py datalineage.json
```

---

## ⚠️ 注意事项

### 1. 空文件处理

如果SQL文件为空，系统会返回成功，不会报错。

### 2. 外部表自动创建

当来源表不在数据库中时，系统会自动创建外部表记录（`data_source='EXTERNAL'`），确保血缘关系不丢失。

### 3. 临时表处理

临时表通过`script_id`字段区分，即使同名也不会冲突。ID格式：`{schema_name}__{table_name}__{script_id}`

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
- ✅ 增量更新：自动清理旧数据，保证一致性
- ✅ 批量处理：支持目录批量处理，带日志记录

---

## 🔍 故障排查

### 问题1: SQL解析失败

**原因**：SQL语法错误或使用了不支持的语法

**解决**：
- 检查SQL语法是否正确
- 确认SQL方言参数是否正确
- 某些复杂语法可能不被sqlglot支持

### 问题2: JSON文件未生成

**检查**：
```python
import sqlite3
conn = sqlite3.connect('dw_metadata.db')
cursor = conn.cursor()

# 检查是否有数据
cursor.execute("SELECT COUNT(*) FROM data_lineage_detail")
print(f"Detail记录数: {cursor.fetchone()[0]}")

cursor.execute("SELECT COUNT(*) FROM data_lineage_summary")
print(f"Summary记录数: {cursor.fetchone()[0]}")

conn.close()
```

### 问题3: 血缘关系不正确

**检查**：
```sql
-- 查看Detail层血缘
SELECT * FROM v_data_lineage_statements
WHERE script_name = 'your_script';

-- 查看Summary层血缘
SELECT * FROM v_data_lineage
WHERE script_name = 'your_script';

-- 查看临时表
SELECT * FROM tables
WHERE table_type = 'TMP_TABLE' AND script_id = 'your_script';
```

---

## 🛠️ 技术特点

### 1. 双层血缘管理
- Detail层：语句级，包含临时表，完整链路
- Summary层：脚本级，仅实体表，自动推导

### 2. 智能冲突处理
- 4种冲突场景的精确处理
- 保护数据完整性，避免数据丢失

### 3. 增量更新支持
- 自动清理旧数据
- 事务保护，失败回滚
- 保证数据一致性

### 4. 标准JSON导出
- NetworkX node_link格式
- 支持可视化和分析
- 易于集成其他工具

### 5. 完整的类型提示
- 所有函数都有完整的类型注解
- 提高代码可维护性

---

## 🎉 项目成果

### ✅ 已完成功能

1. ✅ 完整的元数据提取系统（DDL和DML）
2. ✅ 智能冲突处理机制（4种场景）
3. ✅ 双层血缘管理（Detail + Summary）
4. ✅ 自动血缘推导（基于NetworkX图算法）
5. ✅ 增量更新支持（自动清理旧数据）
6. ✅ JSON导出功能（标准格式）
7. ✅ 数据血缘可视化工具（静态图+交互式图）
8. ✅ 批量处理支持（目录扫描+日志记录）

### 🏆 核心能力

- **元数据提取**：支持DDL和DML的完整元数据提取
- **冲突处理**：智能处理4种数据冲突场景
- **血缘追踪**：双层血缘设计，满足不同层次需求
- **增量更新**：自动维护数据一致性
- **JSON导出**：标准格式，易于集成和分析
- **可视化**：强大的血缘关系可视化工具

---

## 📚 相关文件

- `sqlite_schema.sql` - 数据库架构定义
- `init_sqlite.py` - 数据库初始化和验证
- `sql_file_processor.py` - SQL文件处理核心逻辑
- `metadata_extractor.py` - SQL元数据提取
- `lineage_graph_manager.py` - 血缘图管理和JSON导出
- `export_all_lineage.py` - 全局血缘导出工具
- `lineage_visualizer/` - 数据血缘可视化工具包
- `dw_metadata.db` - SQLite数据库文件

---

## 📁 目录结构

```
metadata_extractor/
├── sql_file_processor.py          # 主处理模块
├── metadata_extractor.py          # 元数据提取
├── lineage_graph_manager.py       # 血缘图管理
├── export_all_lineage.py          # 全局导出工具
├── init_sqlite.py                 # 数据库初始化
├── sqlite_schema.sql              # Schema定义
├── dw_metadata.db                 # SQLite数据库
├── datalineage.json               # 旧版全局血缘
├── datalineage/                   # JSON导出目录
│   ├── scripts/                   # 单个脚本血缘
│   │   ├── script1_detail.json
│   │   └── script1_summary.json
│   ├── all_lineage_detail.json    # 全局详细血缘
│   └── all_lineage_summary.json   # 全局汇总血缘
├── lineage_visualizer/            # 可视化工具
│   ├── lineage_visualizer.py      # 静态图生成
│   └── lineage_visualizer_interactive.py  # 交互式图
└── sqlglot/                       # SQL解析器
```

---

**项目状态：** ✅ 完成并可投入使用  
**版本：** v3.0  
**最后更新：** 2025-11-06

---

**开始使用：** 运行 `python sql_file_processor.py your_script.sql teradata` 即可开始！
