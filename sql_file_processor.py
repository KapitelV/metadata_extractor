"""
SQL文件处理器 - 从SQL文件中提取元数据并存储到SQLite数据库

功能：
1. 读取SQL文件内容
2. 解析SQL语句（DDL和DML）
3. 提取表和字段元数据
4. 处理与现有数据的冲突
5. 存储到dw_metadata.db数据库

作者：AI Assistant
创建时间：2025-01-23
"""

import sqlite3
import os
import json
import subprocess
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Set
import sqlglot
from sqlglot import exp
import networkx as nx
from metadata_extractor import extract_ddl_metadata, extract_sql_metadata, _classify_statement_type


def process_sql_file(
    sql_file_path: str,
    dialect: str = None,
    db_path: str = 'dw_metadata.db'
) -> Tuple[bool, str]:
    """
    处理SQL文件并存储元数据到数据库
    
    Args:
        sql_file_path: SQL文件路径
        dialect: SQL方言（如'mysql', 'teradata', 'postgres'等）
        db_path: SQLite数据库路径，默认为'dw_metadata.db'
    
    Returns:
        (True, '') - 成功
        (False, '错误原因') - 失败
    """
    try:
        # 1. 读取SQL文件
        print(f"📖 正在读取SQL文件: {sql_file_path}")
        if not os.path.exists(sql_file_path):
            return False, f"文件不存在: {sql_file_path}"
        
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # 若文件为空，则直接返回成功
        if not sql_content.strip():
            return True, ""
        
        # 2. 解析SQL语句
        print(f"🔍 正在解析SQL语句...")
        try:
            parsed_statements = sqlglot.parse(sql_content, dialect=dialect)
        except Exception as e:
            return False, f"SQL解析失败: {str(e)}"
        
        if not parsed_statements:
            return False, "未能解析出任何SQL语句"
        
        print(f"✅ 成功解析 {len(parsed_statements)} 条SQL语句")
        
        # 3. 提取元数据
        print(f"📊 正在提取元数据...")
        extracted_data = []
        
        for idx, parsed_sql in enumerate(parsed_statements, 1):
            if parsed_sql is None:
                continue
                
            try:
                # 使用_classify_statement_type获取细粒度类型
                statement_type = _classify_statement_type(parsed_sql)
                
                # 根据类型判断是DDL还是DML
                ddl_types = {'CREATE_TABLE', 'CREATE_TABLE_AS', 'CREATE_VIEW'}
                dml_types = {'INSERT_SELECT', 'INSERT_VALUES', 'UPDATE', 'MERGE'}
                
                if statement_type in ddl_types:
                    print(f"  [{idx}] DDL语句 ({statement_type}) - {type(parsed_sql).__name__}")
                    metadata = extract_ddl_metadata(parsed_sql.sql(dialect=dialect), dialect=dialect)
                    metadata['statement_type'] = statement_type
                    metadata['_type'] = 'DDL'
                    metadata['_ast'] = parsed_sql
                    extracted_data.append(metadata)
                    
                elif statement_type in dml_types:
                    print(f"  [{idx}] DML语句 ({statement_type}) - {type(parsed_sql).__name__}")
                    metadata = extract_sql_metadata(parsed_sql.sql(dialect=dialect), dialect=dialect)
                    metadata['statement_type'] = statement_type
                    metadata['_type'] = 'DML'
                    metadata['_ast'] = parsed_sql
                    extracted_data.append(metadata)
                    
                else:
                    print(f"  [{idx}] 跳过语句 ({statement_type}) - {type(parsed_sql).__name__} (不支持的类型)")
                    
            except Exception as e:
                return False, f"提取第{idx}条SQL元数据失败: {str(e)}"
        
        if not extracted_data:
            return False, "未能提取到任何有效的元数据"
        
        print(f"✅ 成功提取 {len(extracted_data)} 条元数据")
        
        # 4. 整合数据（按表分组）
        print(f"🔄 正在整合数据...")
        tables_data = _consolidate_metadata(extracted_data)
        print(f"✅ 整合后共 {len(tables_data)} 个表")
        
        # 5. 构建依赖图并识别目标表（需要在处理表数据前生成script_id）
        print(f"\n📊 正在构建依赖图...")
        try:
            dependency_graph = _build_dependency_graph(extracted_data)
            print(f"✅ 依赖图构建完成: {len(dependency_graph.nodes())} 个节点, {len(dependency_graph.edges())} 条边")
            
            # 6. 保存依赖图到文件
            graph_file_path = _save_dependency_graph(sql_file_path, dependency_graph)
            print(f"✅ 依赖图已保存到: {graph_file_path}")
            
            # 7. 识别目标表和来源表
            target_tables, source_tables = _identify_target_and_source_tables(dependency_graph, extracted_data)
            print(f"✅ 识别到目标表: {target_tables}")
            print(f"✅ 识别到来源表: {source_tables}")
            
            # 检查目标表数量
            if len(target_tables) == 0:
                return False, "未能识别到目标表"
            
            # 8. 生成script_id（只使用脚本名，不含扩展名）
            script_name = os.path.splitext(os.path.basename(sql_file_path))[0]
            script_id = script_name
            
            print(f"✅ 生成脚本ID: {script_id}")
            print(f"   脚本操作 {len(target_tables)} 个目标表")
            
        except Exception as e:
            return False, f"构建依赖图失败: {str(e)}"
        
        # 9. 连接数据库并处理冲突
        print(f"\n💾 正在连接数据库: {db_path}")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # 使结果可以通过列名访问
        
        try:
            # 使用事务
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION")
            
            # 10. 处理每个表的数据（传入script_id用于临时表）
            for table_key, table_data in tables_data.items():
                print(f"\n📋 处理表: {table_key}")
                try:
                    _process_table_data(cursor, table_data, script_id)
                except Exception as e:
                    conn.rollback()
                    return False, f"处理表 {table_key} 时出错: {str(e)}"
            
            # 11. 填充sql_scripts、script_statements、data_lineage_detail表
            print(f"\n📝 正在填充脚本信息...")
            _populate_script_tables(
                cursor, 
                sql_file_path, 
                sql_content,
                target_tables,  # 传入目标表集合（可能多个）
                source_tables,
                extracted_data,
                dependency_graph,
                parsed_statements,  # 新增：传递解析后的语句
                script_id
            )
            print(f"✅ 脚本信息已保存")
            
            # 12. 更新全局血缘图
            print(f"\n🌐 正在更新全局血缘图...")
            _update_global_lineage(
                sql_file_path,
                target_tables,  # 传入目标表集合（可能多个）
                source_tables
            )
            print(f"✅ 全局血缘图已更新")
            
            # 提交事务
            conn.commit()
            print(f"\n✅ 所有数据已成功保存到数据库")
            
        finally:
            conn.close()
        
        return True, ""
        
    except Exception as e:
        return False, f"处理过程中发生未预期的错误: {str(e)}"


def _identify_statement_type(parsed_sql: exp.Expression) -> str:
    """
    获取更精细的语句类型

    Args:
        parsed_sql: sqlglot解析后的AST

    Returns:
        'CREATE_TABLE' - CREATE TABLE语句（完整字段定义）
        'INSERT_EXPLICIT' - INSERT(col1,col2)语句（显式指定列名）
        'INSERT_VALUES' - INSERT VALUES语句（未显式指定列名）
        'UPDATE' - UPDATE语句（部分字段更新）
        'MERGE' - MERGE语句（复杂操作）
        'OTHER' - 其他类型
    """
    if isinstance(parsed_sql, exp.Create):
        return 'CREATE_TABLE'
    elif isinstance(parsed_sql, exp.Insert):
        # 检查是否显式指定了列名
        schema = parsed_sql.find(exp.Schema)
        if schema and schema.expressions:
            return 'INSERT_EXPLICIT'
        else:
            return 'INSERT_VALUES'
    elif isinstance(parsed_sql, exp.Update):
        return 'UPDATE'
    elif isinstance(parsed_sql, exp.Merge):
        return 'MERGE'
    else:
        return 'OTHER'


def _consolidate_metadata(extracted_data: List[Dict]) -> Dict[str, Dict]:
    """
    整合元数据（按表分组）
    
    处理目标表和来源表，确保所有引用的表都被记录
    
    Args:
        extracted_data: 提取的元数据列表
    
    Returns:
        按表分组的数据字典，key为(schema_name, table_name)
    """
    tables_data = {}
    
    for metadata in extracted_data:
        target_table = metadata['target_table']

        # 获取语句类型
        stmt_type = metadata.get('statement_type', _classify_statement_type(metadata['_ast']))

        # 1. 处理目标表
        schema_name = target_table.get('schema_nm', '') or ''
        table_name = target_table.get('tbl_en_nm', '')

        if table_name:
            # 使用(schema_name, table_name)作为key
            table_key = (schema_name, table_name)

            if table_key not in tables_data:
                # 确定表类型
                table_type = _determine_table_type(metadata['_ast'], schema_name)
                tables_data[table_key] = {
                    'schema_name': schema_name,
                    'table_name': table_name,
                    'table_cn_name': target_table.get('tbl_cn_nm', ''),
                    'table_type': table_type,
                    'data_source': stmt_type,
                    'columns': [],
                    'ast': metadata['_ast']
                }

            # 更新data_source为更具体的类型
            tables_data[table_key]['data_source'] = stmt_type
            
            # 整合字段信息
            if 'target_columns' in metadata and metadata['target_columns']:
                for col in metadata['target_columns']:
                    # 检查字段是否已存在
                    existing_col = None
                    for existing in tables_data[table_key]['columns']:
                        if existing['col_en_nm'] == col.get('col_en_nm'):
                            existing_col = existing
                            break
                    
                    if existing_col:
                        # 合并字段信息（优先保留建表语句信息）
                        ddl_types = {'CREATE_TABLE', 'CREATE_TABLE_AS', 'CREATE_VIEW'}
                        if stmt_type in ddl_types:
                            # 建表语句优先，覆盖原有信息
                            for key, value in col.items():
                                if value or key in ['is_null', 'is_pri_key', 'is_foreign_key']:
                                    existing_col[key] = value
                        else:
                            # DML补充信息
                            for key, value in col.items():
                                if value and not existing_col.get(key):
                                    existing_col[key] = value
                    else:
                        # 新字段
                        tables_data[table_key]['columns'].append(col)
        
        # 2. 处理来源表（仅当是DML或有source_tables时）
        if 'source_tables' in metadata and metadata['source_tables']:
            for source_table in metadata['source_tables']:
                src_schema = source_table.get('schema_nm', '') or ''
                src_table = source_table.get('tbl_en_nm', '')
                
                if not src_table:
                    continue
                
                src_key = (src_schema, src_table)
                
                # 如果来源表还未记录，添加为外部表
                if src_key not in tables_data:
                    tables_data[src_key] = {
                        'schema_name': src_schema,
                        'table_name': src_table,
                        'table_cn_name': '',
                        'table_type': 'TABLE',  # 默认为TABLE类型
                        'data_source': 'EXTERNAL',  # 标记为外部表
                        'columns': [],
                        'ast': None
                    }
    
    return tables_data


def get_conflict_strategy(existing_type: str, new_type: str) -> str:
    """
    获取冲突处理策略

    Args:
        existing_type: 数据库中现有的语句类型
        new_type: 新的语句类型

    Returns:
        冲突处理策略: 'ERROR', 'KEEP_CREATE_TABLE', 'SUPPLEMENT_CHINESE_NAMES', 'MERGE_INFO'
    """
    # 建表语句优先级最高（包括CREATE_TABLE, CREATE_TABLE_AS, CREATE_VIEW）
    ddl_types = {'CREATE_TABLE', 'CREATE_TABLE_AS', 'CREATE_VIEW'}
    
    if existing_type in ddl_types or new_type in ddl_types:
        # 如果两个都是建表语句，不允许重复
        if existing_type in ddl_types and new_type in ddl_types:
            if existing_type == new_type:
                return 'ERROR'  # 不允许重复建表
            else:
                # 不同类型的建表语句，CREATE_TABLE优先
                if existing_type == 'CREATE_TABLE' or new_type == 'CREATE_TABLE':
                    return 'KEEP_CREATE_TABLE'
                # 其他情况，保留现有的
                return 'KEEP_CREATE_TABLE'

        # 建表语句总是优先
        return 'KEEP_CREATE_TABLE'

    # 其他语句的合并策略
    merge_strategies = {
        # DDL vs 其他
        ('CREATE_TABLE_AS', 'INSERT_SELECT'): 'SUPPLEMENT_CHINESE_NAMES',
        ('CREATE_TABLE_AS', 'INSERT_VALUES'): 'SUPPLEMENT_CHINESE_NAMES',
        ('CREATE_TABLE_AS', 'UPDATE'): 'SUPPLEMENT_CHINESE_NAMES',
        ('CREATE_TABLE_AS', 'MERGE'): 'SUPPLEMENT_CHINESE_NAMES',

        # CREATE VIEW vs 其他
        ('CREATE_VIEW', 'INSERT_SELECT'): 'SUPPLEMENT_CHINESE_NAMES',
        ('CREATE_VIEW', 'INSERT_VALUES'): 'SUPPLEMENT_CHINESE_NAMES',
        ('CREATE_VIEW', 'UPDATE'): 'SUPPLEMENT_CHINESE_NAMES',
        ('CREATE_VIEW', 'MERGE'): 'SUPPLEMENT_CHINESE_NAMES',

        # 非建表语句之间的合并
        ('INSERT_SELECT', 'INSERT_SELECT'): 'MERGE_INFO',
        ('INSERT_SELECT', 'INSERT_VALUES'): 'MERGE_INFO',
        ('INSERT_SELECT', 'UPDATE'): 'MERGE_INFO',
        ('INSERT_SELECT', 'MERGE'): 'MERGE_INFO',

        ('INSERT_VALUES', 'INSERT_VALUES'): 'MERGE_INFO',
        ('INSERT_VALUES', 'UPDATE'): 'MERGE_INFO',
        ('INSERT_VALUES', 'MERGE'): 'MERGE_INFO',

        ('UPDATE', 'UPDATE'): 'MERGE_INFO',
        ('UPDATE', 'MERGE'): 'MERGE_INFO',

        ('MERGE', 'MERGE'): 'MERGE_INFO',
    }

    return merge_strategies.get((existing_type, new_type), 'MERGE_INFO')


def _process_table_data(cursor: sqlite3.Cursor, table_data: Dict, script_id: str = None):
    """
    处理单个表的数据（包括冲突检测和合并）
    
    Args:
        cursor: 数据库游标
        table_data: 表数据字典
    
    Raises:
        Exception: 当检测到不允许的冲突时
    """
    schema_name = table_data['schema_name']
    table_name = table_data['table_name']
    table_cn_name = table_data['table_cn_name']
    data_source = table_data['data_source']
    table_type = table_data['table_type']
    
    # 判断是否为临时表
    is_tmp_table = (table_type == 'TMP_TABLE')
    # script_id: 实体表为None，临时表为实际值（用于ID生成和逻辑判断）
    current_script_id = script_id if is_tmp_table else None

    # 生成表ID（临时表需要传入script_id）
    table_id = _generate_table_id(schema_name, table_name, current_script_id)

    # 查询数据库中是否已存在该表
    cursor.execute("""
        SELECT id, schema_name, table_name, table_type, description,
               data_source, refresh_frequency, row_count, data_size_mb,
               last_updated, created_at, script_id
        FROM tables
        WHERE id = ?
    """, (table_id,))

    existing_table = cursor.fetchone()
    
    if existing_table:
        print(f"  ⚠️  表已存在，检测冲突...")
        existing_data_source = existing_table['data_source']

        # 如果新数据是EXTERNAL，跳过（已有任何定义都优先）
        if data_source == 'EXTERNAL':
            print(f"  ⏭️  表已存在，跳过外部表创建")
            return

        # 如果已存在的是EXTERNAL，用新数据覆盖
        if existing_data_source == 'EXTERNAL':
            print(f"  🔄 用实际定义覆盖外部表记录")
            _update_table_with_statement(cursor, table_id, table_data, current_script_id, data_source)
            return

        # 获取冲突处理策略
        strategy = get_conflict_strategy(existing_data_source, data_source)
        print(f"  🔄 冲突策略: {strategy}")

        if strategy == 'ERROR':
            raise Exception(f"表 {schema_name}.{table_name} 冲突不允许: {existing_data_source} vs {data_source}")

        elif strategy == 'KEEP_CREATE_TABLE':
            # 建表语句优先，覆盖其他定义
            print(f"  🔄 建表语句优先覆盖")
            _update_table_with_statement(cursor, table_id, table_data, current_script_id, data_source)

        elif strategy == 'SUPPLEMENT_CHINESE_NAMES':
            # 只补充中文名，不检查字段存在性
            print(f"  ➕ 只补充中文名信息")
            _supplement_chinese_names_only(cursor, table_id, table_data, current_script_id)

        elif strategy == 'MERGE_INFO':
            # 正常合并信息
            print(f"  🔀 合并字段信息")
            _merge_statement_info(cursor, table_id, table_data, current_script_id, existing_data_source, data_source)

    else:
        print(f"  ✨ 新建表记录")
        _insert_new_table(cursor, table_data, current_script_id)


def _generate_table_id(schema_name: str, table_name: str, script_id: str = None) -> str:
    """
    生成表ID
    规则：
    - 有schema且有script_id（临时表）: {SCHEMA_NAME}__{TABLE_NAME}__{SCRIPT_ID}
    - 有schema无script_id（实体表）: {SCHEMA_NAME}__{TABLE_NAME}__
    - 无schema有script_id（临时表）: __{TABLE_NAME}__{SCRIPT_ID}
    - 无schema无script_id（临时表，无脚本）: __{TABLE_NAME}__
    """
    if schema_name:
        if script_id:
            return f"{schema_name}__{table_name}__{script_id}"
        else:
            return f"{schema_name}__{table_name}__"
    else:
        if script_id:
            return f"__{table_name}__{script_id}"
        else:
            return f"__{table_name}__"


def _generate_column_id(schema_name: str, table_name: str, column_name: str, script_id: str = None) -> str:
    """
    生成字段ID
    规则：
    - 有schema且有script_id（临时表）: {SCHEMA_NAME}__{TABLE_NAME}__{SCRIPT_ID}__{COLUMN_NAME}
    - 有schema无script_id（实体表）: {SCHEMA_NAME}__{TABLE_NAME}____{COLUMN_NAME}
    - 无schema有script_id（临时表）: __{TABLE_NAME}__{SCRIPT_ID}__{COLUMN_NAME}
    - 无schema无script_id（临时表，无脚本）: __{TABLE_NAME}____{COLUMN_NAME}
    """
    if schema_name:
        if script_id:
            return f"{schema_name}__{table_name}__{script_id}__{column_name}"
        else:
            return f"{schema_name}__{table_name}____{column_name}"
    else:
        if script_id:
            return f"__{table_name}__{script_id}__{column_name}"
        else:
            return f"__{table_name}____{column_name}"


def _determine_table_type(ast: exp.Expression, schema_name: str) -> str:
    """
    确定表类型
    
    Args:
        ast: SQL语句的AST
        schema_name: schema名称
    
    Returns:
        'TABLE', 'VIEW', 'TMP_TABLE'
    """
    if isinstance(ast, exp.Create):
        # 检查是否是VIEW
        if hasattr(ast, 'kind') and ast.kind and 'VIEW' in str(ast.kind).upper():
            return 'VIEW'
        
        # 检查是否是临时表
        if hasattr(ast, 'args'):
            # TEMPORARY TABLE
            if ast.args.get('temporary'):
                return 'TMP_TABLE'
            
            # VOLATILE TABLE (Teradata) - 检查args中的volatile标志
            if ast.args.get('volatile'):
                return 'TMP_TABLE'
            
            # 检查properties中是否包含VOLATILE或TEMPORARY
            if 'properties' in ast.args and ast.args['properties']:
                properties = ast.args['properties']
                if hasattr(properties, 'expressions'):
                    for prop in properties.expressions:
                        prop_str = str(type(prop).__name__).upper()
                        prop_value = str(prop).upper()
                        # 检查StabilityProperty: VOLATILE 或包含TEMPORARY的属性
                        if 'VOLATILE' in prop_str or 'VOLATILE' in prop_value:
                            return 'TMP_TABLE'
                        if 'TEMPORARY' in prop_str or 'TEMPORARY' in prop_value:
                            return 'TMP_TABLE'
            
            # 检查kind中是否包含VOLATILE或TEMPORARY关键词
            if hasattr(ast, 'kind') and ast.kind:
                kind_str = str(ast.kind).upper()
                if 'VOLATILE' in kind_str or 'TEMPORARY' in kind_str:
                    return 'TMP_TABLE'
        
        return 'TABLE'
    
    elif isinstance(ast, (exp.Insert, exp.Update, exp.Merge)):
        # DML语句：有schema_name则为TABLE，否则为TMP_TABLE
        if schema_name:
            return 'TABLE'
        else:
            return 'TMP_TABLE'
    
    return 'TABLE'


def _update_table_with_statement(cursor: sqlite3.Cursor, table_id: str, table_data: Dict, script_id: str = None, new_data_source: str = None):
    """
    用新语句完全覆盖表信息

    Args:
        cursor: 数据库游标
        table_id: 表ID
        table_data: 新的表数据
        script_id: 脚本ID
        new_data_source: 新的数据源类型
    """
    schema_name = table_data['schema_name']
    table_name = table_data['table_name']
    table_cn_name = table_data['table_cn_name']
    table_type = table_data['table_type']

    # 判断是否为临时表
    is_tmp_table = (table_type == 'TMP_TABLE')
    current_script_id = script_id if is_tmp_table else ''

    # 更新表基本信息
    cursor.execute("""
        UPDATE tables
        SET database_id = ?, schema_name = ?, table_name = ?, table_type = ?,
            description = ?, data_source = ?, script_id = ?
        WHERE id = ?
    """, (
        schema_name if schema_name else '',
        schema_name,
        table_name,
        table_type,
        table_cn_name,
        new_data_source or table_data.get('data_source', ''),
        current_script_id,
        table_id
    ))

    # 删除现有字段，重新插入
    cursor.execute("DELETE FROM columns WHERE table_id = ?", (table_id,))

    # 插入新字段
    _insert_columns(cursor, table_id, schema_name, table_name, table_data['columns'], script_id)


def _supplement_chinese_names_only(cursor: sqlite3.Cursor, table_id: str, table_data: Dict, script_id: str = None):
    """
    只补充中文名信息，不检查字段存在性

    Args:
        cursor: 数据库游标
        table_id: 表ID
        table_data: 新的表数据
        script_id: 脚本ID
    """
    # 读取现有字段
    cursor.execute("""
        SELECT column_name, description
        FROM columns
        WHERE table_id = ?
    """, (table_id,))

    existing_columns = {row['column_name']: row['description'] for row in cursor.fetchall()}

    # 只补充中文名
    for col in table_data['columns']:
        col_en_nm = col.get('col_en_nm')
        col_cn_nm = col.get('col_cn_nm')

        if col_en_nm in existing_columns and col_cn_nm and not existing_columns[col_en_nm]:
            cursor.execute("""
                UPDATE columns
                SET description = ?
                WHERE table_id = ? AND column_name = ?
            """, (col_cn_nm, table_id, col_en_nm))
            print(f"    ➕ 补充字段中文名: {col_en_nm} -> {col_cn_nm}")


def _merge_statement_info(cursor: sqlite3.Cursor, table_id: str, table_data: Dict, script_id: str = None,
                         existing_data_source: str = None, new_data_source: str = None):
    """
    合并语句信息，允许新增字段

    Args:
        cursor: 数据库游标
        table_id: 表ID
        table_data: 新的表数据
        script_id: 脚本ID
        existing_data_source: 现有数据源类型
        new_data_source: 新数据源类型
    """
    schema_name = table_data['schema_name']
    table_name = table_data['table_name']

    # 读取现有字段
    cursor.execute("""
        SELECT column_name, data_type, is_nullable, default_value,
               is_primary_key, is_foreign_key, description, ordinal_position
        FROM columns
        WHERE table_id = ?
    """, (table_id,))

    existing_columns = {row['column_name']: dict(row) for row in cursor.fetchall()}

    # 处理新字段
    for col in table_data['columns']:
        col_en_nm = col.get('col_en_nm')

        if col_en_nm in existing_columns:
            # 字段已存在，合并信息
            existing = existing_columns[col_en_nm]
            col_cn_nm = col.get('col_cn_nm')

            # 检查中文名冲突
            if col_cn_nm and existing['description'] and col_cn_nm != existing['description']:
                print(f"    ⚠️ 字段中文名冲突: {schema_name}.{table_name}.{col_en_nm}")
                print(f"       现有: '{existing['description']}', 新: '{col_cn_nm}'")
                # 保留现有中文名（按时间优先）

            # 补充缺失的中文名
            elif col_cn_nm and not existing['description']:
                cursor.execute("""
                    UPDATE columns
                    SET description = ?
                    WHERE table_id = ? AND column_name = ?
                """, (col_cn_nm, table_id, col_en_nm))
                print(f"    ➕ 补充字段中文名: {col_en_nm} -> {col_cn_nm}")

        else:
            # 新字段，添加它
            print(f"    ➕ 新增字段: {col_en_nm}")
            _insert_single_column(cursor, table_id, schema_name, table_name, col, script_id)


def _insert_single_column(cursor: sqlite3.Cursor, table_id: str, schema_name: str, table_name: str,
                         col: Dict, script_id: str = None):
    """插入单个字段"""
    col_no = col.get('col_no', 1)
    col_en_nm = col.get('col_en_nm', '')
    col_cn_nm = col.get('col_cn_nm', '')
    data_type = col.get('data_type', '')
    is_null = col.get('is_null', True)
    default_value = col.get('default_value', '')
    is_pri_key = col.get('is_pri_key', False)
    is_foreign_key = col.get('is_foreign_key', False)

    # 生成字段ID
    column_id = _generate_column_id(schema_name, table_name, col_en_nm, script_id if script_id else None)

    cursor.execute("""
        INSERT INTO columns (
            id, table_id, column_name, data_type, max_length, is_nullable,
            default_value, is_primary_key, is_foreign_key, description, ordinal_position
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        column_id,
        table_id,
        col_en_nm,
        data_type,
        None,  # max_length
        is_null,
        default_value,
        is_pri_key,
        is_foreign_key,
        col_cn_nm,
        col_no
    ))


def _insert_new_table(cursor: sqlite3.Cursor, table_data: Dict, script_id: str = None):
    """插入新表"""
    schema_name = table_data['schema_name']
    table_name = table_data['table_name']
    table_cn_name = table_data['table_cn_name']
    data_source = table_data['data_source']
    table_type = table_data['table_type']
    
    # 判断是否为临时表
    is_tmp_table = (table_type == 'TMP_TABLE')
    current_script_id = script_id if is_tmp_table else None

    # 生成表ID（临时表需要传入script_id）
    table_id = _generate_table_id(schema_name, table_name, current_script_id)
    
    # 处理database_id
    database_id = ''
    if schema_name:
        # 确保database记录存在
        cursor.execute("SELECT id FROM databases WHERE id = ?", (schema_name,))
        if not cursor.fetchone():
            cursor.execute("""
                INSERT INTO databases (id, name, description)
                VALUES (?, ?, '')
            """, (schema_name, schema_name))
            print(f"  📂 创建数据库记录: {schema_name}")
        database_id = schema_name
    
    # 插入表记录（文本字段使用空字符串代替NULL，数值/日期字段使用NULL）
    cursor.execute("""
        INSERT INTO tables (
            id, database_id, schema_name, table_name, table_type,
            description, business_purpose, data_source, refresh_frequency,
            row_count, data_size_mb, last_updated, script_id
        ) VALUES (?, ?, ?, ?, ?, ?, '', ?, 'DAILY', NULL, NULL, NULL, ?)
    """, (
        table_id,
        database_id,
        schema_name or '',
        table_name,
        table_type,
        table_cn_name or '',
        data_source,
        current_script_id or ''  # 将None转换为空字符串
    ))
    
    print(f"  ✅ 插入表: {table_id} (类型: {table_type})")
    
    # 插入字段记录
    _insert_columns(cursor, table_id, schema_name, table_name, table_data['columns'], current_script_id)


def _update_table_with_ddl(cursor: sqlite3.Cursor, table_id: str, table_data: Dict, script_id: str = None):
    """用DDL覆盖DML表"""
    schema_name = table_data['schema_name']
    table_name = table_data['table_name']
    table_cn_name = table_data['table_cn_name']
    table_type = table_data['table_type']
    
    # 判断是否为临时表
    is_tmp_table = (table_type == 'TMP_TABLE')
    current_script_id = script_id if is_tmp_table else None
    
    # 读取现有字段的中文名（DML可能有）
    cursor.execute("""
        SELECT column_name, description
        FROM columns
        WHERE table_id = ? AND description IS NOT NULL AND description != ''
    """, (table_id,))
    
    existing_col_descriptions = {row['column_name']: row['description'] for row in cursor.fetchall()}
    
    # 获取实际的data_source（从table_data中）
    actual_data_source = table_data.get('data_source', 'CREATE_TABLE')
    
    # 更新表信息（文本字段使用空字符串）
    cursor.execute("""
        UPDATE tables
        SET table_type = ?,
            description = ?,
            data_source = ?
        WHERE id = ?
    """, (table_type, table_cn_name or '', actual_data_source, table_id))
    
    print(f"  ✅ 更新表信息，data_source={actual_data_source}")
    
    # 删除旧字段
    cursor.execute("DELETE FROM columns WHERE table_id = ?", (table_id,))
    
    # 插入DDL字段，补充DML的中文名
    for col in table_data['columns']:
        col_en_nm = col.get('col_en_nm')
        if col_en_nm in existing_col_descriptions:
            # 如果DDL没有中文名，使用DML的
            if not col.get('col_cn_nm'):
                col['col_cn_nm'] = existing_col_descriptions[col_en_nm]
    
    _insert_columns(cursor, table_id, schema_name, table_name, table_data['columns'], current_script_id)


def _supplement_ddl_with_dml(cursor: sqlite3.Cursor, table_id: str, table_data: Dict, script_id: str = None):
    """补充DML信息到DDL表"""
    schema_name = table_data['schema_name']
    table_name = table_data['table_name']
    
    # 读取现有字段
    cursor.execute("""
        SELECT column_name, description
        FROM columns
        WHERE table_id = ?
    """, (table_id,))
    
    existing_columns = {row['column_name']: row['description'] for row in cursor.fetchall()}
    
    # 检查DML的字段
    for col in table_data['columns']:
        col_en_nm = col.get('col_en_nm')
        
        if col_en_nm not in existing_columns:
            # DML有新字段，DDL没有 - 报错
            raise Exception(
                f"DML语句引用了DDL中不存在的字段: "
                f"{schema_name}.{table_name}.{col_en_nm}"
            )
        
        # 补充中文名
        col_cn_nm = col.get('col_cn_nm')
        if col_cn_nm and not existing_columns[col_en_nm]:
            cursor.execute("""
                UPDATE columns
                SET description = ?
                WHERE table_id = ? AND column_name = ?
            """, (col_cn_nm, table_id, col_en_nm))
            print(f"    ➕ 补充字段中文名: {col_en_nm} -> {col_cn_nm}")


def _merge_dml_with_dml(cursor: sqlite3.Cursor, table_id: str, table_data: Dict, script_id: str = None):
    """合并DML与DML"""
    schema_name = table_data['schema_name']
    table_name = table_data['table_name']
    table_type = table_data['table_type']
    
    # 判断是否为临时表
    is_tmp_table = (table_type == 'TMP_TABLE')
    current_script_id = script_id if is_tmp_table else None
    
    # 读取现有字段
    cursor.execute("""
        SELECT column_name, data_type, is_nullable, default_value,
               is_primary_key, is_foreign_key, description, ordinal_position
        FROM columns
        WHERE table_id = ?
    """, (table_id,))
    
    existing_columns = {row['column_name']: dict(row) for row in cursor.fetchall()}
    
    # 处理新字段
    for col in table_data['columns']:
        col_en_nm = col.get('col_en_nm')
        
        if col_en_nm in existing_columns:
            # 字段已存在，检查冲突
            existing = existing_columns[col_en_nm]
            col_cn_nm = col.get('col_cn_nm')
            
            # 检查冲突：如果两个都有值且不同，则报错
            if col_cn_nm and existing['description'] and col_cn_nm != existing['description']:
                raise Exception(
                    f"字段中文名冲突: {schema_name}.{table_name}.{col_en_nm} "
                    f"现有: '{existing['description']}', 新: '{col_cn_nm}'"
                )
            
            # 用有值覆盖无值
            if col_cn_nm and not existing['description']:
                cursor.execute("""
                    UPDATE columns
                    SET description = ?
                    WHERE table_id = ? AND column_name = ?
                """, (col_cn_nm, table_id, col_en_nm))
                print(f"    🔄 更新字段中文名: {col_en_nm} -> {col_cn_nm}")
        
        else:
            # 新字段，直接添加（需要传入script_id以支持临时表）
            col_id = _generate_column_id(schema_name, table_name, col_en_nm, current_script_id)
            cursor.execute("""
                INSERT INTO columns (
                    id, table_id, column_name, data_type, max_length,
                    is_nullable, default_value, is_primary_key, is_foreign_key,
                    description, ordinal_position
                ) VALUES (?, ?, ?, '', NULL, 1, '', 0, 0, ?, NULL)
            """, (
                col_id,
                table_id,
                col_en_nm,
                col.get('col_cn_nm') or ''
            ))
            print(f"    ✨ 添加新字段: {col_en_nm}")


def _insert_columns(cursor: sqlite3.Cursor, table_id: str, schema_name: str,
                   table_name: str, columns: List[Dict], script_id: str = None):
    """插入字段记录"""
    for col in columns:
        col_en_nm = col.get('col_en_nm')
        if not col_en_nm:
            continue

        col_id = _generate_column_id(schema_name, table_name, col_en_nm, script_id)
        
        # 从col字典中获取值，设置默认值（文本字段使用空字符串，数值字段使用None）
        data_type = col.get('data_type') or ''
        max_length = None  # 暂不处理，数值字段
        is_nullable = 0 if col.get('is_null') == False else 1
        default_value = col.get('default_value') or ''
        is_primary_key = 1 if col.get('is_pri_key') else 0
        is_foreign_key = 1 if col.get('is_foreign_key') else 0
        description = col.get('col_cn_nm') or ''
        ordinal_position = col.get('col_no') or None  # 数值字段
        
        cursor.execute("""
            INSERT INTO columns (
                id, table_id, column_name, data_type, max_length,
                is_nullable, default_value, is_primary_key, is_foreign_key,
                description, ordinal_position
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            col_id, table_id, col_en_nm, data_type, max_length,
            is_nullable, default_value, is_primary_key, is_foreign_key,
            description, ordinal_position
        ))
    
    print(f"  ✅ 插入 {len(columns)} 个字段")


def _build_dependency_graph(extracted_data: List[Dict]) -> nx.DiGraph:
    """
    构建SQL依赖图
    
    Args:
        extracted_data: 提取的元数据列表
    
    Returns:
        NetworkX有向图
    """
    graph = nx.DiGraph()
    
    for metadata in extracted_data:
        target_table = metadata['target_table']
        target_schema = target_table.get('schema_nm', '') or ''
        target_name = target_table.get('tbl_en_nm', '')
        
        if not target_name:
            continue
        
        # 构造完整的表标识
        target_full_name = f"{target_schema}.{target_name}" if target_schema else target_name
        
        # 添加目标表节点
        if target_full_name not in graph:
            graph.add_node(target_full_name, schema=target_schema, table=target_name)
        
        # 添加来源表和边（DML或CREATE AS）
        # CREATE AS语句也有来源表依赖
        if 'source_tables' in metadata and metadata['source_tables']:
            for source_table in metadata['source_tables']:
                source_schema = source_table.get('schema_nm', '') or ''
                source_name = source_table.get('tbl_en_nm', '')
                
                if not source_name:
                    continue
                
                # 构造完整的来源表标识
                source_full_name = f"{source_schema}.{source_name}" if source_schema else source_name
                
                # 添加来源表节点
                if source_full_name not in graph:
                    graph.add_node(source_full_name, schema=source_schema, table=source_name)
                
                # 添加边（来源表 -> 目标表），避免重复
                if not graph.has_edge(source_full_name, target_full_name):
                    graph.add_edge(source_full_name, target_full_name)
    
    return graph


def _save_dependency_graph(sql_file_path: str, graph: nx.DiGraph) -> str:
    """
    保存依赖图到JSON文件
    
    Args:
        sql_file_path: SQL文件路径
        graph: 依赖图
    
    Returns:
        JSON文件路径
    """
    # 生成JSON文件名
    base_name = os.path.splitext(os.path.basename(sql_file_path))[0]
    dir_name = os.path.dirname(sql_file_path)
    if not dir_name:
        dir_name = '.'
    json_file_path = os.path.join(dir_name, f"{base_name}_graph.json")
    
    # 使用networkx的node-link格式导出
    graph_data = nx.node_link_data(graph)
    
    # 保存到文件
    with open(json_file_path, 'w', encoding='utf-8') as f:
        json.dump(graph_data, f, ensure_ascii=False, indent=2)
    
    return json_file_path


def _identify_target_and_source_tables(
    graph: nx.DiGraph,
    extracted_data: List[Dict] = None
) -> Tuple[Set[str], Set[str]]:
    """
    识别目标表和来源表
    
    目标表识别逻辑（按优先级）：
    1. 入度>0的非临时表（有数据流入的实体表）
    2. 如果1未找到，则找出度=0的表（最终节点）
    3. 如果仍未找到，返回空集合
    
    来源表: 入度为0的表
    
    Args:
        graph: 依赖图
        extracted_data: 提取的元数据列表
    
    Returns:
        (目标表集合, 来源表集合)
    """
    target_tables = set()
    source_tables = set()
    
    # 辅助函数：判断是否为临时表
    def is_temp_table(table_name: str) -> bool:
        """
        临时表判断规则：
        1. 没有schema（不包含'.'）
        2. 或者以常见临时表前缀开头（VT_、TMP_、TEMP_等）
        """
        if '.' not in table_name:
            # 没有schema的表，可能是临时表
            # 进一步检查表名特征
            table_only = table_name.upper()
            temp_prefixes = ['VT_', 'TMP_', 'TEMP_', 'VOLATILE_', '#']
            return any(table_only.startswith(prefix) for prefix in temp_prefixes)
        return False
    
    # 收集所有节点的度数信息
    nodes_info = []
    for node in graph.nodes():
        out_degree = graph.out_degree(node)
        in_degree = graph.in_degree(node)
        is_temp = is_temp_table(node)
        nodes_info.append({
            'node': node,
            'in_degree': in_degree,
            'out_degree': out_degree,
            'is_temp': is_temp
        })
    
    # 策略1：找入度>0的非临时表
    for info in nodes_info:
        if info['in_degree'] > 0 and not info['is_temp']:
            target_tables.add(info['node'])
    
    # 策略2：如果策略1未找到，找出度=0的表
    if not target_tables:
        for info in nodes_info:
            if info['out_degree'] == 0:
                target_tables.add(info['node'])
    
    # 识别来源表（入度=0）
    for info in nodes_info:
        if info['in_degree'] == 0:
            source_tables.add(info['node'])
    
    return target_tables, source_tables


def _create_external_table_record(cursor: sqlite3.Cursor, schema_name: str, table_name: str):
    """
    创建外部表的基础记录
    
    外部表是指在当前脚本中被引用但未定义的表（通常是其他系统的表）
    
    Args:
        cursor: 数据库游标
        schema_name: Schema名称
        table_name: 表名
    """
    table_id = _generate_table_id(schema_name, table_name, None)
    
    # 检查是否已存在（避免重复创建）
    cursor.execute("SELECT id FROM tables WHERE id = ?", (table_id,))
    if cursor.fetchone():
        return  # 已存在，不重复创建
    
    # 获取或创建database_id
    database_id = schema_name if schema_name else ''
    if schema_name:
        cursor.execute("SELECT id FROM databases WHERE id = ?", (database_id,))
        if not cursor.fetchone():
            cursor.execute("""
                INSERT INTO databases (id, name, description) 
                VALUES (?, ?, '')
            """, (database_id, schema_name))
    
    # 插入外部表记录
    cursor.execute("""
        INSERT INTO tables (
            id, database_id, schema_name, table_name, table_type,
            description, business_purpose, data_source,
            refresh_frequency, row_count, data_size_mb, script_id
        ) VALUES (?, ?, ?, ?, ?, '', '外部表（自动创建）', '', 'EXTERNAL', '', NULL, NULL, '')
    """, (
        table_id,
        database_id,
        schema_name,
        table_name,
        'TABLE'  # 外部表默认为TABLE类型
    ))


def _cleanup_script_data(cursor: sqlite3.Cursor, script_id: str) -> None:
    """
    清理脚本的旧数据（为增量更新做准备）
    
    在重新处理脚本前，删除该脚本的所有相关数据，确保数据一致性。
    
    Args:
        cursor: 数据库游标
        script_id: 脚本ID
    """
    # 检查脚本是否存在
    cursor.execute("SELECT id FROM sql_scripts WHERE id = ?", (script_id,))
    if not cursor.fetchone():
        # 脚本不存在，无需清理
        return
    
    deleted_counts = {}
    
    # 1. 删除summary（必须先删除，因为依赖detail）
    cursor.execute("DELETE FROM data_lineage_summary WHERE script_id = ?", (script_id,))
    deleted_counts['summary'] = cursor.rowcount
    
    # 2. 删除detail
    cursor.execute("DELETE FROM data_lineage_detail WHERE script_id = ?", (script_id,))
    deleted_counts['detail'] = cursor.rowcount
    
    # 3. 删除statements
    cursor.execute("DELETE FROM script_statements WHERE script_id = ?", (script_id,))
    deleted_counts['statements'] = cursor.rowcount
    
    # 注意：不删除临时表，因为：
    # 1. 临时表可能被其他脚本引用（虽然不常见）
    # 2. 临时表会在下次处理时自动更新
    # 3. 如果需要清理，可以手动处理
    
    if any(deleted_counts.values()):
        print(f"  🧹 清理旧数据: summary={deleted_counts['summary']}, "
              f"detail={deleted_counts['detail']}, "
              f"statements={deleted_counts['statements']}")


def _populate_script_tables(
    cursor: sqlite3.Cursor,
    sql_file_path: str,
    sql_content: str,
    target_tables: Set[str],
    source_tables: Set[str],
    extracted_data: List[Dict],
    dependency_graph: nx.DiGraph,
    parsed_statements: List[exp.Expression],
    script_id: str
):
    """
    填充sql_scripts、script_statements、data_lineage_detail表
    
    Args:
        cursor: 数据库游标
        sql_file_path: SQL文件路径
        sql_content: SQL文件内容
        target_tables: 目标表集合（完整名称，如schema.table）
        source_tables: 来源表集合
        extracted_data: 提取的元数据（每个元素对应一条语句）
        dependency_graph: 依赖图
        parsed_statements: 解析后的SQL语句列表
        script_id: 脚本ID
    """
    # 生成script_name（只使用脚本名，不含扩展名）
    script_name = os.path.splitext(os.path.basename(sql_file_path))[0]
    
    # 0. 清理旧数据（如果脚本已存在，支持增量更新）
    _cleanup_script_data(cursor, script_id)
    
    # 1. 插入sql_scripts表（一个脚本只有一条记录）
    cursor.execute("""
        INSERT OR REPLACE INTO sql_scripts (
            id, script_name, script_content,
            script_type, script_purpose, author, description,
            execution_frequency, execution_order, is_active,
            last_executed, avg_execution_time_seconds, performance_stats_json
        ) VALUES (?, ?, ?, '', '', '', '', 'DAILY', NULL, 1, NULL, NULL, NULL)
    """, (
        script_id,
        script_name,
        sql_content
    ))
    
    # 2. 填充script_statements表（按语句）
    print(f"  📝 填充script_statements表...")
    for idx, parsed_sql in enumerate(parsed_statements, 1):
        if parsed_sql is None:
            continue
        
        statement_id = f"{script_id}__STMT_{idx:03d}"
        statement_type = _classify_statement_type(parsed_sql)
        statement_content = parsed_sql.sql()
        
        # 提取该语句的目标表
        target_table_id = None
        if idx <= len(extracted_data):
            metadata = extracted_data[idx - 1]
            target_table = metadata.get('target_table', {})
            target_schema = target_table.get('schema_nm', '') or ''
            target_name = target_table.get('tbl_en_nm', '')
            
            if target_name:
                # 尝试实体表
                target_table_id = _generate_table_id(target_schema, target_name, None)
                cursor.execute("SELECT id FROM tables WHERE id = ?", (target_table_id,))
                if not cursor.fetchone():
                    # 尝试临时表
                    target_table_id = _generate_table_id(target_schema, target_name, script_id)
                    cursor.execute("SELECT id FROM tables WHERE id = ?", (target_table_id,))
                    if not cursor.fetchone():
                        target_table_id = None
        
        # 插入statement记录
        cursor.execute("""
            INSERT OR REPLACE INTO script_statements (
                id, script_id, statement_index, statement_type,
                statement_content, target_table_id, description
            ) VALUES (?, ?, ?, ?, ?, ?, '')
        """, (
            statement_id,
            script_id,
            idx,
            statement_type,
            statement_content,
            target_table_id
        ))
    
    print(f"  ✅ 已填充 {len([s for s in parsed_statements if s is not None])} 条语句记录")
    
    # 3. 填充data_lineage_detail表（按语句）
    print(f"  📊 填充data_lineage_detail表...")
    lineage_count = 0
    
    for idx, metadata in enumerate(extracted_data, 1):
        statement_id = f"{script_id}__STMT_{idx:03d}"
        
        # 获取该语句的目标表
        target_table = metadata.get('target_table', {})
        target_schema = target_table.get('schema_nm', '') or ''
        target_name = target_table.get('tbl_en_nm', '')
        
        if not target_name:
            continue
        
        # 查找目标表ID（先实体表，再临时表）
        target_table_id = _generate_table_id(target_schema, target_name, None)
        cursor.execute("SELECT id FROM tables WHERE id = ?", (target_table_id,))
        if not cursor.fetchone():
            target_table_id = _generate_table_id(target_schema, target_name, script_id)
            cursor.execute("SELECT id FROM tables WHERE id = ?", (target_table_id,))
            if not cursor.fetchone():
                print(f"  ⚠️  语句{idx}的目标表 {target_schema}.{target_name} 不在数据库中，跳过")
                continue
        
        # 获取该语句的来源表
        source_tables_in_stmt = metadata.get('source_tables', [])
        
        for source_table in source_tables_in_stmt:
            source_schema = source_table.get('schema_nm', '') or ''
            source_name = source_table.get('tbl_en_nm', '')
            
            if not source_name:
                continue
            
            # 查找来源表ID（先实体表，再临时表）
            source_table_id = _generate_table_id(source_schema, source_name, None)
            cursor.execute("SELECT id FROM tables WHERE id = ?", (source_table_id,))
            if not cursor.fetchone():
                source_table_id = _generate_table_id(source_schema, source_name, script_id)
                cursor.execute("SELECT id FROM tables WHERE id = ?", (source_table_id,))
                if not cursor.fetchone():
                    # 来源表不存在，自动创建外部表记录
                    print(f"  📥 自动创建外部表记录: {source_schema}.{source_name}")
                    _create_external_table_record(cursor, source_schema, source_name)
                    source_table_id = _generate_table_id(source_schema, source_name, None)
            
            # 生成lineage_id
            lineage_id = f"{target_table_id}__{source_table_id}__{statement_id}"
            
            # 插入data_lineage_detail
            cursor.execute("""
                INSERT OR REPLACE INTO data_lineage_detail (
                    id, target_table_id, source_table_id, script_id, statement_id,
                    transformation_logic, filter_conditions
                ) VALUES (?, ?, ?, ?, ?, '', '')
            """, (
                lineage_id,
                target_table_id,
                source_table_id,
                script_id,
                statement_id
            ))
            lineage_count += 1
    
    print(f"  ✅ 已填充 {lineage_count} 条血缘记录")
    
    # 4. 生成data_lineage_summary（从detail推导）
    print(f"  🔄 正在生成summary...")
    try:
        from lineage_graph_manager import generate_lineage_summary
        generate_lineage_summary(cursor, script_id)
    except Exception as e:
        print(f"  ⚠️  Summary生成失败: {e}")
        # 不影响主流程


def _update_global_lineage(
    sql_file_path: str,
    target_tables: Set[str],
    source_tables: Set[str],
    lineage_json_path: str = 'datalineage.json'
):
    """
    更新全局血缘图（datalineage.json）
    
    Args:
        sql_file_path: SQL文件路径
        target_tables: 目标表集合（完整名称）
        source_tables: 来源表集合
        lineage_json_path: 血缘图JSON文件路径
    """
    # 读取现有血缘图（如果存在）
    if os.path.exists(lineage_json_path):
        with open(lineage_json_path, 'r', encoding='utf-8') as f:
            graph_data = json.load(f)
        global_graph = nx.node_link_graph(graph_data, directed=True)
    else:
        global_graph = nx.DiGraph()
    
    # 为每个目标表处理血缘关系
    for target_table in target_tables:
        # 添加目标表节点（如果不存在）
        if target_table not in global_graph:
            target_schema, target_name = _parse_full_table_name(target_table)
            global_graph.add_node(target_table, schema=target_schema, table=target_name)
        
        # 添加来源表节点和边
        for source_table in source_tables:
            # 添加来源表节点（如果不存在）
            if source_table not in global_graph:
                source_schema, source_name = _parse_full_table_name(source_table)
                global_graph.add_node(source_table, schema=source_schema, table=source_name)
            
            # 添加或更新边
            if global_graph.has_edge(source_table, target_table):
                # 边已存在，更新script_paths属性
                edge_data = global_graph[source_table][target_table]
                script_paths = edge_data.get('script_paths', [])
                if sql_file_path not in script_paths:
                    script_paths.append(sql_file_path)
                    global_graph[source_table][target_table]['script_paths'] = script_paths
            else:
                # 边不存在，创建新边
                global_graph.add_edge(source_table, target_table, script_paths=[sql_file_path])
    
    # 保存到文件
    graph_data = nx.node_link_data(global_graph)
    with open(lineage_json_path, 'w', encoding='utf-8') as f:
        json.dump(graph_data, f, ensure_ascii=False, indent=2)


def _parse_full_table_name(full_name: str) -> Tuple[str, str]:
    """
    解析完整表名为(schema, table)
    
    Args:
        full_name: 完整表名，如"schema.table"或"table"
    
    Returns:
        (schema_name, table_name)
    """
    if '.' in full_name:
        parts = full_name.split('.')
        return parts[0], parts[1]
    else:
        return '', full_name


def process_sql_directory(
    directory_path: str,
    dialect: str = 'teradata',
    mode: str = 'insert',
    db_path: str = 'dw_metadata.db',
    lineage_json_path: str = 'datalineage.json',
    log_file: str = 'batch_process_log.txt'
) -> Dict:
    """
    批量处理目录下所有SQL文件
    
    Args:
        directory_path: SQL文件目录路径
        dialect: SQL方言（如'mysql', 'teradata', 'postgres'等）
        mode: 处理模式
            - 'clear': 清洗数据库后处理
            - 'insert': 在当前数据基础上追加
        db_path: SQLite数据库路径
        lineage_json_path: 全局血缘图JSON文件路径
        log_file: 日志文件路径
    
    Returns:
        {
            'success': True/False,
            'errors': [
                {'file': 'xxx.sql', 'error': 'xxx'}
            ]
        }
    """
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='w', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("="*70)
        logger.info("开始批量处理SQL文件")
        logger.info(f"目录: {directory_path}")
        logger.info(f"方言: {dialect}")
        logger.info(f"模式: {mode}")
        logger.info("="*70)
        
        # 1. 检查目录是否存在
        if not os.path.exists(directory_path):
            error_msg = f"目录不存在: {directory_path}"
            logger.error(error_msg)
            return {'success': False, 'errors': [{'file': directory_path, 'error': error_msg}]}
        
        # 2. 如果是clear模式，重新初始化数据库
        if mode == 'clear':
            logger.info("\n🔄 模式: CLEAR - 正在重新初始化数据库...")
            
            # 删除旧的血缘图文件
            if os.path.exists(lineage_json_path):
                os.remove(lineage_json_path)
                logger.info(f"  已删除旧的血缘图文件: {lineage_json_path}")
            
            # 调用init_sqlite.py重新初始化数据库
            try:
                result = subprocess.run(
                    ['python', 'init_sqlite.py', '--force-reset'],
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    timeout=60
                )
                if result.returncode != 0:
                    error_msg = f"数据库初始化失败: {result.stderr}"
                    logger.error(error_msg)
                    return {'success': False, 'errors': [{'file': 'init_sqlite.py', 'error': error_msg}]}
                logger.info("  ✅ 数据库已重新初始化")
            except subprocess.TimeoutExpired:
                error_msg = "数据库初始化超时"
                logger.error(error_msg)
                return {'success': False, 'errors': [{'file': 'init_sqlite.py', 'error': error_msg}]}
            except Exception as e:
                error_msg = f"数据库初始化异常: {str(e)}"
                logger.error(error_msg)
                return {'success': False, 'errors': [{'file': 'init_sqlite.py', 'error': error_msg}]}
        
        # 3. 递归扫描目录，获取所有SQL文件
        logger.info("\n📂 正在扫描SQL文件...")
        sql_files = []
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                if file.lower().endswith('.sql'):
                    sql_files.append(os.path.join(root, file))
        
        if not sql_files:
            logger.warning("  ⚠️  未找到任何SQL文件")
            return {'success': True, 'errors': []}
        
        logger.info(f"  ✅ 找到 {len(sql_files)} 个SQL文件")
        
        # 4. 逐个处理SQL文件
        logger.info("\n📊 开始处理SQL文件...")
        errors = []
        
        for idx, sql_file in enumerate(sql_files, 1):
            relative_path = os.path.relpath(sql_file, directory_path)
            logger.info(f"\n[{idx}/{len(sql_files)}] 处理: {relative_path}")
            
            try:
                success, error_msg = process_sql_file(
                    sql_file_path=sql_file,
                    dialect=dialect,
                    db_path=db_path
                )
                
                if success:
                    logger.info(f"  ✅ 成功")
                else:
                    logger.error(f"  ❌ 失败: {error_msg}")
                    errors.append({
                        'file': relative_path,
                        'error': error_msg
                    })
                    # 立即停止模式：遇到错误直接返回
                    logger.error("\n⛔ 遇到错误，停止批处理")
                    return {'success': False, 'errors': errors}
                    
            except Exception as e:
                error_msg = f"处理异常: {str(e)}"
                logger.error(f"  ❌ {error_msg}")
                errors.append({
                    'file': relative_path,
                    'error': error_msg
                })
                # 立即停止模式：遇到错误直接返回
                logger.error("\n⛔ 遇到错误，停止批处理")
                return {'success': False, 'errors': errors}
        
        # 5. 汇总结果
        logger.info("\n" + "="*70)
        logger.info("批处理完成")
        logger.info(f"总文件数: {len(sql_files)}")
        logger.info(f"成功: {len(sql_files) - len(errors)}")
        logger.info(f"失败: {len(errors)}")
        logger.info("="*70)
        
        if errors:
            logger.info("\n失败文件列表:")
            for err in errors:
                logger.info(f"  ❌ {err['file']}")
                logger.info(f"     错误: {err['error']}")
        
        return {
            'success': len(errors) == 0,
            'errors': errors
        }
        
    except Exception as e:
        error_msg = f"批处理过程发生未预期的错误: {str(e)}"
        logger.error(error_msg)
        return {'success': False, 'errors': [{'file': 'batch_process', 'error': error_msg}]}


# 主程序入口（用于测试）
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        # print("用法: python sql_file_processor.py <sql_file_path> [dialect]")
        # print("示例: python sql_file_processor.py test.sql mysql")
        # sys.exit(1)
        
        sql_file = "C:\\pyworks\\Datasets\\SQLs\\DML\\Teradata\\minsheng\\MDB_TD\\sqls\\dm88_op_cnt_camp_ac_cs_ex_situ_mdm_10200.pl.1609.sql"
        sql_file = "C:\\pyworks\\Datasets\\SQLs\\DML\\Teradata\\minsheng\\MDB_TD\\sqls\\dm88_op_cnt_camp_ac_stat_trace_mdm_10200.pl.1615.sql"
        dialect = "teradata"

    else:
        sql_file = sys.argv[1]
        dialect = sys.argv[2] if len(sys.argv) > 2 else None
    
    success, error_msg = process_sql_file(sql_file, dialect=dialect)
    
    if success:
        print("\n🎉 处理成功！")
        sys.exit(0)
    else:
        print(f"\n❌ 处理失败: {error_msg}")
        sys.exit(1)

