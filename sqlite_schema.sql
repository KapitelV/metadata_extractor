-- ==================================================
-- 数仓SQL开发智能辅助工具 - SQLite数据库设计（优化版）
-- 设计目标：支持LLM获取表元数据和SQL脚本信息
-- 数据库：SQLite（关系型数据存储）
-- 
-- 主要优化：
-- 1. 新增脚本语句分段表（script_statements）
-- 2. 血缘关系分为详细和汇总两层（detail/summary）
-- 3. 支持字段级血缘追踪（column_lineage_detail/summary）
-- 4. 完整的临时表支持和生命周期追踪
-- 5. 移除冗余的 script_dependencies 表
-- ==================================================

-- ==================================================
-- 核心元数据表
-- ==================================================

-- 数据库/模式信息
CREATE TABLE databases (
    id TEXT PRIMARY KEY, -- 与SCHEMA_NAME一致
    name TEXT NOT NULL,
    description TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP  --默认为2025-01-01 00:00:00
);

-- 表信息（增强版）
CREATE TABLE tables (
    id TEXT PRIMARY KEY,-- 'SCHEMA_NAME'__'TABLE_NAME'__'SCRIPT_ID' 若为实体表则SCRIPT_ID为空字符串
    database_id TEXT REFERENCES databases(id),
    schema_name TEXT, -- 若为临时表则schema_name为空字符串
    script_id TEXT, -- 若为实体表则SCRIPT_ID为空字符串；若为临时表则为所属脚本ID
    table_name TEXT NOT NULL,
    table_type TEXT, -- TABLE, VIEW, TMP_TABLE
    description TEXT,
    business_purpose TEXT, -- 业务用途描述，无则赋空值
    data_source TEXT, -- DDL, DML
    refresh_frequency TEXT, -- REALTIME, HOURLY, DAILY, WEEKLY，默认DAILY
    row_count INTEGER, --默认为NULL
    data_size_mb REAL, --默认为NULL
    last_updated DATETIME, --默认为NULL
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,  --默认为2025-01-01 00:00:00
    UNIQUE(database_id, schema_name, table_name, script_id)
);

-- 字段信息（包含实体表和临时表的字段）
CREATE TABLE columns (
    id TEXT PRIMARY KEY,-- 'SCHEMA_NAME'__'TABLE_NAME'__'SCRIPT_ID'__'COLUMN_NAME' 若为实体表则SCRIPT_ID为空字符串
    table_id TEXT REFERENCES tables(id),
    column_name TEXT NOT NULL,
    data_type TEXT,
    max_length INTEGER,  --默认为NULL
    is_nullable INTEGER, -- 0 = false, 1 = true,  默认为1
    default_value TEXT,  --默认为NULL
    is_primary_key INTEGER, -- 0 = false, 1 = true,  默认为0
    is_foreign_key INTEGER, -- 0 = false, 1 = true,  默认为0
    description TEXT,
    ordinal_position INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP  --默认为2025-01-01 00:00:00
);

-- 外键关系表（预留，暂不使用）
CREATE TABLE foreign_keys (
    id TEXT PRIMARY KEY,
    fk_column_id TEXT REFERENCES columns(id),
    referenced_table_id TEXT REFERENCES tables(id),
    referenced_column_id TEXT REFERENCES columns(id),
    constraint_name TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ==================================================
-- SQL脚本相关表
-- ==================================================

-- SQL脚本信息（增强版）
CREATE TABLE sql_scripts (
    id TEXT PRIMARY KEY,-- 脚本名称（不含扩展名），如 'script_name'
    script_name TEXT, 
    script_content TEXT NOT NULL,
    script_type TEXT, -- CREATE_TABLE, INSERT_ETL, UPDATE_DIM, AGGREGATE, VIEW_DEF
    script_purpose TEXT, -- 脚本的业务目的，无则赋空值
    author TEXT, -- 作者，无则赋空值
    description TEXT,
    execution_frequency TEXT, -- REALTIME, HOURLY, DAILY, WEEKLY，默认DAILY
    execution_order INTEGER, -- 同一表的多个脚本的执行顺序，默认为NULL
    is_active INTEGER DEFAULT 1, -- 0 = false, 1 = true
    last_executed DATETIME,  --默认为2025-01-31 00:00:00
    avg_execution_time_seconds INTEGER,  --默认为NULL
    performance_stats_json TEXT, -- JSON格式的性能统计，默认为NULL
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,  --默认为2025-01-01 00:00:00
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP  --默认为2025-01-01 00:00:00
);

-- SQL脚本语句表（脚本的分段）
CREATE TABLE script_statements (
    id TEXT PRIMARY KEY,  -- 'SCRIPT_ID'__'STMT_001'
    script_id TEXT REFERENCES sql_scripts(id),
    statement_index INTEGER NOT NULL,  -- 语句在脚本中的序号（从1开始）
    statement_type TEXT,  -- CREATE_TABLE, CREATE_TEMP_TABLE, INSERT, UPDATE, DELETE, MERGE, SELECT
    statement_content TEXT NOT NULL,  -- 该语句的完整SQL
    target_table_id TEXT REFERENCES tables(id),  -- 该语句主要操作的目标表（如果有）
    description TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(script_id, statement_index)
);

-- ==================================================
-- 数据血缘关系表（双层设计）
-- ==================================================

-- 表级血缘 - 详细层（语句级别，包含临时表）
CREATE TABLE data_lineage_detail (
    id TEXT PRIMARY KEY,-- 'TARGET_TABLE_ID'__'SOURCE_TABLE_ID'__'STATEMENT_ID'
    target_table_id TEXT REFERENCES tables(id),  -- 可以是实体表或临时表
    source_table_id TEXT REFERENCES tables(id),  -- 可以是实体表或临时表
    script_id TEXT REFERENCES sql_scripts(id),
    statement_id TEXT REFERENCES script_statements(id),  -- 必须关联到具体语句
    transformation_logic TEXT, -- 转换逻辑的简要描述
    filter_conditions TEXT, -- 过滤条件
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 表级血缘 - 汇总层（脚本级别，仅实体表）
CREATE TABLE data_lineage_summary (
    id TEXT PRIMARY KEY,-- 'TARGET_TABLE_ID'__'SOURCE_TABLE_ID'__'SCRIPT_ID'
    target_table_id TEXT REFERENCES tables(id),  -- 必须是实体表（TABLE或VIEW）
    source_table_id TEXT REFERENCES tables(id),  -- 必须是实体表（TABLE或VIEW）
    script_id TEXT REFERENCES sql_scripts(id),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(target_table_id, source_table_id, script_id)
);

-- 字段级血缘 - 详细层（语句级别，包含临时表字段）
CREATE TABLE column_lineage_detail (
    id TEXT PRIMARY KEY,
    target_column_id TEXT REFERENCES columns(id),  -- 可以是实体表或临时表的字段
    source_column_id TEXT REFERENCES columns(id),  -- 可以是实体表或临时表的字段
    script_id TEXT REFERENCES sql_scripts(id),
    statement_id TEXT REFERENCES script_statements(id),  -- 必须关联到具体语句
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 字段级血缘 - 汇总层（脚本级别，仅实体表字段）
CREATE TABLE column_lineage_summary (
    id TEXT PRIMARY KEY,
    target_column_id TEXT REFERENCES columns(id),  -- 必须是实体表字段
    source_column_id TEXT REFERENCES columns(id),  -- 必须是实体表字段
    script_id TEXT REFERENCES sql_scripts(id),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(target_column_id, source_column_id, script_id)
);

-- ==================================================
-- 向量数据映射表
-- ==================================================

-- 向量数据映射表（用于关联Milvus中的向量数据）
CREATE TABLE vector_mappings (
    id TEXT PRIMARY KEY,
    object_type TEXT NOT NULL, -- TABLE, COLUMN, SCRIPT, PROCESS
    object_id TEXT NOT NULL, -- 对应的业务对象ID
    collection_name TEXT NOT NULL, -- Milvus集合名称
    vector_id TEXT NOT NULL, -- Milvus中的向量ID
    description TEXT, -- 向量化的文本描述
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,  --默认为2025-01-01 00:00:00
    UNIQUE(object_type, object_id, collection_name)
);

-- ==================================================
-- 索引定义（优化查询性能）
-- ==================================================

-- 元数据查询优化
CREATE INDEX idx_tables_name ON tables(database_id, schema_name, table_name);
CREATE INDEX idx_tables_type ON tables(table_type);
CREATE INDEX idx_tables_script ON tables(script_id);  -- 用于查询临时表所属脚本
CREATE INDEX idx_columns_table_name ON columns(table_id, column_name);
CREATE INDEX idx_columns_data_type ON columns(data_type);

-- 脚本查询优化
CREATE INDEX idx_scripts_type ON sql_scripts(script_type);
CREATE INDEX idx_scripts_active ON sql_scripts(is_active);
CREATE INDEX idx_script_statements_script ON script_statements(script_id);
CREATE INDEX idx_script_statements_type ON script_statements(statement_type);
CREATE INDEX idx_script_statements_target ON script_statements(target_table_id);

-- 表级血缘查询优化
CREATE INDEX idx_lineage_detail_target ON data_lineage_detail(target_table_id);
CREATE INDEX idx_lineage_detail_source ON data_lineage_detail(source_table_id);
CREATE INDEX idx_lineage_detail_script ON data_lineage_detail(script_id);
CREATE INDEX idx_lineage_detail_statement ON data_lineage_detail(statement_id);

CREATE INDEX idx_lineage_summary_target ON data_lineage_summary(target_table_id);
CREATE INDEX idx_lineage_summary_source ON data_lineage_summary(source_table_id);
CREATE INDEX idx_lineage_summary_script ON data_lineage_summary(script_id);

-- 字段级血缘查询优化
CREATE INDEX idx_col_lineage_detail_target ON column_lineage_detail(target_column_id);
CREATE INDEX idx_col_lineage_detail_source ON column_lineage_detail(source_column_id);
CREATE INDEX idx_col_lineage_detail_script ON column_lineage_detail(script_id);
CREATE INDEX idx_col_lineage_detail_statement ON column_lineage_detail(statement_id);

CREATE INDEX idx_col_lineage_summary_target ON column_lineage_summary(target_column_id);
CREATE INDEX idx_col_lineage_summary_source ON column_lineage_summary(source_column_id);
CREATE INDEX idx_col_lineage_summary_script ON column_lineage_summary(script_id);

-- 外键关系查询优化
CREATE INDEX idx_foreign_keys_fk_column ON foreign_keys(fk_column_id);
CREATE INDEX idx_foreign_keys_referenced_table ON foreign_keys(referenced_table_id);
CREATE INDEX idx_foreign_keys_referenced_column ON foreign_keys(referenced_column_id);

-- 向量映射查询优化
CREATE INDEX idx_vector_mappings_object ON vector_mappings(object_type, object_id);
CREATE INDEX idx_vector_mappings_collection ON vector_mappings(collection_name);
CREATE INDEX idx_vector_mappings_vector_id ON vector_mappings(vector_id);

-- ==================================================
-- 视图定义（简化常用查询）
-- ==================================================

-- ===== 表元数据视图 =====

-- 表的完整信息视图
CREATE VIEW v_table_complete_info AS
SELECT 
    t.id as table_id,
    d.name as database_name,
    t.schema_name,
    t.table_name,
    t.table_type,
    t.description,
    t.business_purpose,
    t.data_source,
    t.refresh_frequency,
    t.row_count,
    t.data_size_mb,
    t.last_updated,
    COUNT(DISTINCT c.id) as column_count,
    -- 统计作为目标表的脚本数（通过汇总表）
    COUNT(DISTINCT dls.script_id) as source_script_count
FROM tables t
LEFT JOIN databases d ON t.database_id = d.id
LEFT JOIN columns c ON t.id = c.table_id
LEFT JOIN data_lineage_summary dls ON t.id = dls.target_table_id
GROUP BY t.id, d.name, t.schema_name, t.table_name, t.table_type, 
         t.description, t.business_purpose, t.data_source, t.refresh_frequency,
         t.row_count, t.data_size_mb, t.last_updated;

-- ===== 表级血缘视图（主视图，80%场景使用）=====

-- 表级血缘汇总视图（仅实体表，脚本级别）
CREATE VIEW v_data_lineage AS
SELECT 
    source_t.database_id as source_database_id,
    source_t.schema_name as source_schema,
    source_t.table_name as source_table,
    target_t.database_id as target_database_id,
    target_t.schema_name as target_schema,
    target_t.table_name as target_table,
    s.script_name,
    s.script_type,
    s.execution_frequency,
    dls.created_at as lineage_created_at
FROM data_lineage_summary dls
JOIN tables source_t ON dls.source_table_id = source_t.id
JOIN tables target_t ON dls.target_table_id = target_t.id
LEFT JOIN sql_scripts s ON dls.script_id = s.id;

-- 表级血缘详细视图（包含临时表和语句信息，调试使用）
CREATE VIEW v_data_lineage_statements AS
SELECT 
    source_t.schema_name as source_schema,
    source_t.table_name as source_table,
    source_t.table_type as source_type,
    target_t.schema_name as target_schema,
    target_t.table_name as target_table,
    target_t.table_type as target_type,
    s.script_name,
    st.statement_index as step,
    st.statement_type,
    st.statement_content as sql_statement
FROM data_lineage_detail dld
JOIN tables source_t ON dld.source_table_id = source_t.id
JOIN tables target_t ON dld.target_table_id = target_t.id
LEFT JOIN sql_scripts s ON dld.script_id = s.id
LEFT JOIN script_statements st ON dld.statement_id = st.id
ORDER BY s.script_name, st.statement_index;

-- 逻辑血缘视图（递归查询，跳过临时表显示实体表间的完整路径）
CREATE VIEW v_data_lineage_with_path AS
WITH RECURSIVE lineage_path AS (
    -- 基础：从实体表开始
    SELECT 
        dld.source_table_id,
        dld.target_table_id,
        dld.script_id,
        source_t.table_name as path,
        source_t.table_type as source_type,
        target_t.table_type as target_type,
        1 as level
    FROM data_lineage_detail dld
    JOIN tables source_t ON dld.source_table_id = source_t.id
    JOIN tables target_t ON dld.target_table_id = target_t.id
    WHERE source_t.table_type IN ('TABLE', 'VIEW')  -- 从实体表开始
    
    UNION ALL
    
    -- 递归：继续追踪
    SELECT 
        lp.source_table_id,
        dld.target_table_id,
        lp.script_id,
        lp.path || ' -> ' || target_t.table_name,
        lp.source_type,
        target_t.table_type,
        lp.level + 1
    FROM lineage_path lp
    JOIN data_lineage_detail dld ON lp.target_table_id = dld.source_table_id
    JOIN tables target_t ON dld.target_table_id = target_t.id
    WHERE lp.level < 10  -- 防止无限递归
)
SELECT 
    source_t.schema_name as source_schema,
    source_t.table_name as source_table,
    target_t.schema_name as target_schema,
    target_t.table_name as target_table,
    lp.path || ' -> ' || target_t.table_name as full_path,
    s.script_name,
    lp.level as chain_length
FROM lineage_path lp
JOIN tables source_t ON lp.source_table_id = source_t.id
JOIN tables target_t ON lp.target_table_id = target_t.id
LEFT JOIN sql_scripts s ON lp.script_id = s.id
WHERE target_t.table_type IN ('TABLE', 'VIEW')  -- 终点是实体表
ORDER BY source_t.table_name, target_t.table_name, lp.level;

-- ===== 字段级血缘视图 =====

-- 字段级血缘汇总视图（仅实体表字段，脚本级别）
CREATE VIEW v_column_lineage AS
SELECT 
    source_t.schema_name || '.' || source_t.table_name || '.' || source_c.column_name as source_column,
    target_t.schema_name || '.' || target_t.table_name || '.' || target_c.column_name as target_column,
    source_c.data_type as source_data_type,
    target_c.data_type as target_data_type,
    s.script_name
FROM column_lineage_summary cls
JOIN columns source_c ON cls.source_column_id = source_c.id
JOIN columns target_c ON cls.target_column_id = target_c.id
JOIN tables source_t ON source_c.table_id = source_t.id
JOIN tables target_t ON target_c.table_id = target_t.id
LEFT JOIN sql_scripts s ON cls.script_id = s.id;

-- 字段级血缘详细视图（包含临时表字段和语句信息）
CREATE VIEW v_column_lineage_statements AS
SELECT 
    source_t.schema_name || '.' || source_t.table_name as source_table,
    source_c.column_name as source_column,
    source_t.table_type as source_table_type,
    target_t.schema_name || '.' || target_t.table_name as target_table,
    target_c.column_name as target_column,
    target_t.table_type as target_table_type,
    s.script_name,
    st.statement_index as step,
    st.statement_type
FROM column_lineage_detail cld
JOIN columns source_c ON cld.source_column_id = source_c.id
JOIN columns target_c ON cld.target_column_id = target_c.id
JOIN tables source_t ON source_c.table_id = source_t.id
JOIN tables target_t ON target_c.table_id = target_t.id
LEFT JOIN sql_scripts s ON cld.script_id = s.id
LEFT JOIN script_statements st ON cld.statement_id = st.id
ORDER BY s.script_name, st.statement_index;

-- ===== 临时表专项视图 =====

-- 临时表生命周期视图
CREATE VIEW v_temp_table_lifecycle AS
SELECT 
    t.table_name as temp_table,
    t.script_id,
    s.script_name,
    -- 创建信息
    create_st.statement_index as created_at_step,
    create_st.statement_type as create_type,
    SUBSTR(create_st.statement_content, 1, 100) as creation_sql_preview,
    -- 使用信息
    COUNT(DISTINCT use_dld.id) as usage_count,
    GROUP_CONCAT(DISTINCT use_st.statement_index ORDER BY use_st.statement_index) as used_in_steps,
    -- 影响的目标表
    GROUP_CONCAT(DISTINCT target_t.table_name) as affects_tables
FROM tables t
JOIN sql_scripts s ON t.script_id = s.id
LEFT JOIN script_statements create_st ON t.id = create_st.target_table_id
LEFT JOIN data_lineage_detail use_dld ON t.id = use_dld.source_table_id
LEFT JOIN script_statements use_st ON use_dld.statement_id = use_st.id
LEFT JOIN data_lineage_detail target_dld ON t.id = target_dld.source_table_id
LEFT JOIN tables target_t ON target_dld.target_table_id = target_t.id 
    AND target_t.table_type IN ('TABLE', 'VIEW')
WHERE t.table_type = 'TMP_TABLE'
GROUP BY t.table_name, t.script_id, s.script_name, create_st.statement_index, 
         create_st.statement_type, create_st.statement_content;

-- ===== 脚本分析视图 =====

-- 脚本执行流程视图（按语句顺序显示脚本的执行逻辑）
CREATE VIEW v_script_execution_flow AS
SELECT 
    s.script_name,
    st.statement_index as step,
    st.statement_type,
    target_t.table_name as target_table,
    target_t.table_type as target_table_type,
    -- 该语句涉及的源表
    GROUP_CONCAT(DISTINCT source_t.table_name) as source_tables,
    SUBSTR(st.statement_content, 1, 100) as sql_preview
FROM script_statements st
JOIN sql_scripts s ON st.script_id = s.id
LEFT JOIN tables target_t ON st.target_table_id = target_t.id
LEFT JOIN data_lineage_detail dld ON st.id = dld.statement_id
LEFT JOIN tables source_t ON dld.source_table_id = source_t.id
GROUP BY s.script_name, st.statement_index, st.statement_type, 
         target_t.table_name, target_t.table_type, st.statement_content
ORDER BY s.script_name, st.statement_index;
