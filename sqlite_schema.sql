-- ==================================================
-- 数仓SQL开发智能辅助工具 - SQLite数据库设计
-- 设计目标：支持LLM获取表元数据和SQL脚本信息
-- 数据库：SQLite（关系型数据存储）
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
    id TEXT PRIMARY KEY,-- 'SCHEMA_NAME'__'TABLE_NAME'__'SCRIPT_ID' 若为实体表则SCRIPT_ID为空值
    database_id TEXT REFERENCES databases(id),
    schema_name TEXT, -- 若为临时表则schema_name为空字符串
    script_id TEXT, -- 若为实体表则SCRIPT_ID为空字符串
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

-- 字段信息
CREATE TABLE columns (
    id TEXT PRIMARY KEY,-- 'SCHEMA_NAME'__'TABLE_NAME'__'SCRIPT_ID'__'COLUMN_NAME'若为实体表则SCRIPT_ID为空值
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


-- SQL脚本信息（增强版）
-- 一个脚本可以操作多个目标表，通过data_lineage表关联目标表
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

-- 表的数据血缘关系
CREATE TABLE data_lineage (
    id TEXT PRIMARY KEY,-- 'TARGET_TABLE_ID'__'SOURCE_TABLE_ID'__'SCRIPT_ID'
    target_table_id TEXT REFERENCES tables(id),
    source_table_id TEXT REFERENCES tables(id),
    script_id TEXT REFERENCES sql_scripts(id), -- 关联的处理脚本
    lineage_type TEXT, -- DIRECT_COPY, TRANSFORM, AGGREGATE, JOIN, UNION，直接赋空值
    transformation_logic TEXT, -- 转换逻辑的简要描述，直接赋空值
    columns_mapping_json TEXT, -- JSON格式的字段映射关系，直接赋空值
    filter_conditions TEXT, -- 过滤条件，直接赋空值
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP  --默认为2025-01-01 00:00:00
);

-- SQL脚本的详细依赖关系
CREATE TABLE script_dependencies (
    id TEXT PRIMARY KEY,-- 'SOURCE_TABLE_ID'__'SCRIPT_ID'
    script_id TEXT REFERENCES sql_scripts(id),
    source_table_id TEXT REFERENCES tables(id),
    dependency_type TEXT, -- READ, WRITE, UPDATE, TEMP_CREATE，直接赋空值
    usage_pattern TEXT, -- FULL_TABLE, FILTERED, JOINED, AGGREGATED，直接赋空值
    columns_used_json TEXT, -- JSON格式的使用字段列表，直接赋空值
    join_conditions TEXT, -- JOIN条件（如果适用），直接赋空值
    filter_conditions TEXT, -- WHERE条件（如果适用），直接赋空值
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP  --默认为2025-01-01 00:00:00
);



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
-- 索引建议（用于优化查询性能）
-- ==================================================

-- 元数据查询优化
CREATE INDEX idx_tables_name ON tables(database_id, schema_name, table_name);
CREATE INDEX idx_tables_type ON tables(table_type);
CREATE INDEX idx_columns_table_name ON columns(table_id, column_name);
CREATE INDEX idx_columns_data_type ON columns(data_type);

-- 脚本查询优化
CREATE INDEX idx_scripts_type ON sql_scripts(script_type);
CREATE INDEX idx_scripts_active ON sql_scripts(is_active);
CREATE INDEX idx_script_deps_script ON script_dependencies(script_id);
CREATE INDEX idx_script_deps_source ON script_dependencies(source_table_id);

-- 血缘关系查询优化
CREATE INDEX idx_lineage_target ON data_lineage(target_table_id);
CREATE INDEX idx_lineage_source ON data_lineage(source_table_id);
CREATE INDEX idx_lineage_script ON data_lineage(script_id);

-- 向量映射查询优化
CREATE INDEX idx_vector_mappings_object ON vector_mappings(object_type, object_id);
CREATE INDEX idx_vector_mappings_collection ON vector_mappings(collection_name);
CREATE INDEX idx_vector_mappings_vector_id ON vector_mappings(vector_id);

-- ==================================================
-- 视图定义（简化常用查询）
-- ==================================================

-- 表的完整信息视图
-- 通过data_lineage表关联脚本（一个表可能是多个脚本的目标表）
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
    COUNT(DISTINCT dl.script_id) as script_count
FROM tables t
LEFT JOIN databases d ON t.database_id = d.id
LEFT JOIN columns c ON t.id = c.table_id
LEFT JOIN data_lineage dl ON t.id = dl.target_table_id
GROUP BY t.id, d.name, t.schema_name, t.table_name, t.table_type, 
         t.description, t.business_purpose, t.data_source, t.refresh_frequency,
         t.row_count, t.data_size_mb, t.last_updated;

-- 脚本依赖关系视图
-- 显示脚本的所有依赖表（不再区分单一目标表，因为一个脚本可能有多个目标表）
CREATE VIEW v_script_dependencies_detail AS
SELECT 
    s.id as script_id,
    s.script_name,
    s.script_type,
    source_t.schema_name as source_schema,
    source_t.table_name as source_table,
    sd.dependency_type,
    sd.usage_pattern,
    sd.columns_used_json,
    sd.join_conditions,
    sd.filter_conditions
FROM sql_scripts s
JOIN script_dependencies sd ON s.id = sd.script_id
JOIN tables source_t ON sd.source_table_id = source_t.id;

-- 数据血缘关系视图
CREATE VIEW v_data_lineage_detail AS
SELECT 
    dl.id as lineage_id,
    source_t.table_name as source_table,
    target_t.table_name as target_table,
    dl.lineage_type,
    dl.transformation_logic,
    dl.columns_mapping_json,
    dl.filter_conditions,
    s.script_name,
    s.script_type,
    s.author
FROM data_lineage dl
JOIN tables source_t ON dl.source_table_id = source_t.id
JOIN tables target_t ON dl.target_table_id = target_t.id
LEFT JOIN sql_scripts s ON dl.script_id = s.id; 