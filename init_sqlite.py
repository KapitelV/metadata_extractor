import sqlite3
import json
import os

def verify_sqlite_structure():
    """
    验证SQLite数据库结构是否正确创建
    """
    print("=" * 60)
    print("开始验证SQLite数据库结构...")
    print("=" * 60)
    
    # 定义预期的表结构
    expected_tables = {
        "databases": {
            "columns": ["id", "name", "description", "created_at"],
            "primary_key": "id",
            "foreign_keys": []
        },
        "tables": {
            "columns": ["id", "database_id", "schema_name", "script_id", "table_name", "table_type", "description", 
                       "business_purpose", "data_source", "refresh_frequency", "row_count", "data_size_mb", 
                       "last_updated", "created_at"],
            "primary_key": "id",
            "foreign_keys": ["database_id"]
        },
        "columns": {
            "columns": ["id", "table_id", "column_name", "data_type", "max_length", "is_nullable", 
                       "default_value", "is_primary_key", "is_foreign_key", "description", "ordinal_position", "created_at"],
            "primary_key": "id",
            "foreign_keys": ["table_id"]
        },
        "foreign_keys": {
            "columns": ["id", "fk_column_id", "referenced_table_id", "referenced_column_id", "constraint_name", "created_at"],
            "primary_key": "id",
            "foreign_keys": ["fk_column_id", "referenced_table_id", "referenced_column_id"]
        },
        "sql_scripts": {
            "columns": ["id", "script_name", "script_content", "script_type", 
                       "script_purpose", "author", "description", "execution_frequency", "execution_order", 
                       "is_active", "last_executed", "avg_execution_time_seconds", "performance_stats_json", 
                       "created_at", "updated_at"],
            "primary_key": "id",
            "foreign_keys": []
        },
        "script_statements": {
            "columns": ["id", "script_id", "statement_index", "statement_type", "statement_content", 
                       "target_table_id", "description", "created_at"],
            "primary_key": "id",
            "foreign_keys": ["script_id", "target_table_id"]
        },
        "data_lineage_detail": {
            "columns": ["id", "target_table_id", "source_table_id", "script_id", "statement_id", 
                       "transformation_logic", "filter_conditions", "created_at"],
            "primary_key": "id",
            "foreign_keys": ["target_table_id", "source_table_id", "script_id", "statement_id"]
        },
        "data_lineage_summary": {
            "columns": ["id", "target_table_id", "source_table_id", "script_id", "created_at"],
            "primary_key": "id",
            "foreign_keys": ["target_table_id", "source_table_id", "script_id"]
        },
        "column_lineage_detail": {
            "columns": ["id", "target_column_id", "source_column_id", "script_id", "statement_id", "created_at"],
            "primary_key": "id",
            "foreign_keys": ["target_column_id", "source_column_id", "script_id", "statement_id"]
        },
        "column_lineage_summary": {
            "columns": ["id", "target_column_id", "source_column_id", "script_id", "created_at"],
            "primary_key": "id",
            "foreign_keys": ["target_column_id", "source_column_id", "script_id"]
        },
        "vector_mappings": {
            "columns": ["id", "object_type", "object_id", "collection_name", "vector_id", "description", "created_at"],
            "primary_key": "id",
            "foreign_keys": []
        }
    }
    
    # 定义预期的视图
    expected_views = [
        "v_table_complete_info",
        "v_data_lineage",
        "v_data_lineage_statements",
        "v_data_lineage_with_path",
        "v_column_lineage",
        "v_column_lineage_statements",
        "v_temp_table_lifecycle",
        "v_script_execution_flow"
    ]
    
    # 定义预期的索引
    expected_indexes = [
        # 元数据索引
        "idx_tables_name",
        "idx_tables_type",
        "idx_tables_script",
        "idx_columns_table_name",
        "idx_columns_data_type",
        # 脚本索引
        "idx_scripts_type",
        "idx_scripts_active",
        "idx_script_statements_script",
        "idx_script_statements_type",
        "idx_script_statements_target",
        # 表级血缘索引
        "idx_lineage_detail_target",
        "idx_lineage_detail_source",
        "idx_lineage_detail_script",
        "idx_lineage_detail_statement",
        "idx_lineage_summary_target",
        "idx_lineage_summary_source",
        "idx_lineage_summary_script",
        # 字段级血缘索引
        "idx_col_lineage_detail_target",
        "idx_col_lineage_detail_source",
        "idx_col_lineage_detail_script",
        "idx_col_lineage_detail_statement",
        "idx_col_lineage_summary_target",
        "idx_col_lineage_summary_source",
        "idx_col_lineage_summary_script",
        # 外键索引
        "idx_foreign_keys_fk_column",
        "idx_foreign_keys_referenced_table",
        "idx_foreign_keys_referenced_column",
        # 向量映射索引
        "idx_vector_mappings_object",
        "idx_vector_mappings_collection",
        "idx_vector_mappings_vector_id"
    ]
    
    verification_results = {}
    
    try:
        # 连接数据库
        conn = sqlite3.connect('dw_metadata.db')
        cursor = conn.cursor()
        
        # 验证表结构
        print("\n�� 验证表结构...")
        print("-" * 40)
        
        for table_name, expected_config in expected_tables.items():
            print(f"\n📋 验证表: {table_name}")
            
            result = {
                "exists": False,
                "columns_correct": False,
                "primary_key_correct": False,
                "foreign_keys_correct": False,
                "errors": []
            }
            
            try:
                # 检查表是否存在
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                if cursor.fetchone():
                    result["exists"] = True
                    print(f"✅ 表存在")
                    
                    # 获取表结构
                    cursor.execute(f"PRAGMA table_info({table_name})")
                    columns_info = cursor.fetchall()
                    actual_columns = [col[1] for col in columns_info]  # col[1]是列名
                    
                    # 检查列定义
                    expected_columns = expected_config["columns"]
                    if set(actual_columns) == set(expected_columns):
                        result["columns_correct"] = True
                        print(f"✅ 列定义正确 ({len(actual_columns)}个列)")
                    else:
                        missing_columns = set(expected_columns) - set(actual_columns)
                        extra_columns = set(actual_columns) - set(expected_columns)
                        if missing_columns:
                            result["errors"].append(f"缺少列: {missing_columns}")
                        if extra_columns:
                            result["errors"].append(f"多余列: {extra_columns}")
                        print(f"❌ 列定义不匹配")
                        print(f"   期望: {expected_columns}")
                        print(f"   实际: {actual_columns}")
                    
                    # 检查主键
                    primary_key_cols = [col[1] for col in columns_info if col[5] == 1]  # col[5]是pk标志
                    if expected_config["primary_key"] in primary_key_cols:
                        result["primary_key_correct"] = True
                        print(f"✅ 主键正确: {expected_config['primary_key']}")
                    else:
                        result["errors"].append(f"主键错误: 期望{expected_config['primary_key']}, 实际{primary_key_cols}")
                        print(f"❌ 主键错误")
                    
                    # 检查外键 - 修复逻辑
                    cursor.execute(f"PRAGMA foreign_key_list({table_name})")
                    fk_info = cursor.fetchall()
                    # fk_info结构: (id, seq, table, from, to, on_update, on_delete, match)
                    # from是外键列名，to是被引用的列名，table是被引用的表名
                    actual_fk_columns = [fk[3] for fk in fk_info] if fk_info else []  # fk[3]是外键列名
                    
                    expected_fks = expected_config["foreign_keys"]
                    if set(actual_fk_columns) == set(expected_fks):
                        result["foreign_keys_correct"] = True
                        print(f"✅ 外键正确: {expected_fks}")
                    else:
                        missing_fks = set(expected_fks) - set(actual_fk_columns)
                        extra_fks = set(actual_fk_columns) - set(expected_fks)
                        if missing_fks:
                            result["errors"].append(f"缺少外键: {missing_fks}")
                        if extra_fks:
                            result["errors"].append(f"多余外键: {extra_fks}")
                        print(f"❌ 外键不匹配")
                        print(f"   期望: {expected_fks}")
                        print(f"   实际: {actual_fk_columns}")
                    
                else:
                    result["errors"].append("表不存在")
                    print(f"❌ 表不存在")
                    
            except Exception as e:
                result["errors"].append(f"验证过程出错: {str(e)}")
                print(f"❌ 验证过程出错: {e}")
            
            verification_results[table_name] = result
        
        # 验证视图
        print("\n🔍 验证视图...")
        print("-" * 40)
        
        view_results = {}
        for view_name in expected_views:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='view' AND name=?", (view_name,))
            if cursor.fetchone():
                print(f"✅ 视图 {view_name} 存在")
                view_results[view_name] = True
            else:
                print(f"❌ 视图 {view_name} 不存在")
                view_results[view_name] = False
        
        # 验证索引
        print("\n🔍 验证索引...")
        print("-" * 40)
        
        index_results = {}
        for index_name in expected_indexes:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name=?", (index_name,))
            if cursor.fetchone():
                print(f"✅ 索引 {index_name} 存在")
                index_results[index_name] = True
            else:
                print(f"❌ 索引 {index_name} 不存在")
                index_results[index_name] = False
        
        # 获取数据库统计信息
        print("\n📊 数据库统计信息...")
        print("-" * 40)
        
        # 获取表数量
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
        table_count = cursor.fetchone()[0]
        print(f"📋 表数量: {table_count}")
        
        # 获取视图数量
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='view'")
        view_count = cursor.fetchone()[0]
        print(f"👁️  视图数量: {view_count}")
        
        # 获取索引数量
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='index'")
        index_count = cursor.fetchone()[0]
        print(f"🔍 索引数量: {index_count}")
        
        # 获取数据库文件大小
        if os.path.exists('dw_metadata.db'):
            db_size = os.path.getsize('dw_metadata.db')
            print(f"💾 数据库文件大小: {db_size / 1024:.2f} KB")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        return False, {}
    
    # 输出验证总结
    print("\n" + "=" * 60)
    print("验证结果总结")
    print("=" * 60)
    
    all_tables_passed = True
    for table_name, result in verification_results.items():
        status = "✅ 通过" if all([
            result["exists"],
            result["columns_correct"],
            result["primary_key_correct"],
            result["foreign_keys_correct"]
        ]) else "❌ 失败"
        
        print(f"{table_name}: {status}")
        
        if result["errors"]:
            all_tables_passed = False
            for error in result["errors"]:
                print(f"  - {error}")
    
    # 视图验证总结
    views_passed = all(view_results.values())
    print(f"\n视图验证: {'✅ 通过' if views_passed else '❌ 失败'}")
    for view_name, exists in view_results.items():
        if not exists:
            print(f"  - 缺少视图: {view_name}")
    
    # 索引验证总结
    indexes_passed = all(index_results.values())
    print(f"\n索引验证: {'✅ 通过' if indexes_passed else '❌ 失败'}")
    for index_name, exists in index_results.items():
        if not exists:
            print(f"  - 缺少索引: {index_name}")
    
    all_passed = all_tables_passed and views_passed and indexes_passed
    print(f"\n总体状态: {'✅ 所有结构验证通过' if all_passed else '❌ 部分结构验证失败'}")
    
    return all_passed, verification_results

def drop_all_tables(conn):
    """删除所有表和视图"""
    print("🔥 删除所有现有表和视图...")
    
    cursor = conn.cursor()
    
    # 删除所有视图
    cursor.execute("SELECT name FROM sqlite_master WHERE type='view'")
    views = cursor.fetchall()
    for view in views:
        view_name = view[0]
        print(f"  🗑️ 删除视图: {view_name}")
        cursor.execute(f"DROP VIEW IF EXISTS {view_name}")
    
    # 删除所有表
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    for table in tables:
        table_name = table[0]
        if table_name != 'sqlite_sequence':  # 跳过系统表
            print(f"  🗑️ 删除表: {table_name}")
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    
    conn.commit()
    print("✅ 所有表和视图已删除")


def check_tables_exist(conn):
    """检查是否有表已存在"""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence'")
    tables = cursor.fetchall()
    return len(tables) > 0


def main(auto_reset=False, force_reset=False):
    """主函数"""
    import sys
    import io
    # 确保stdout使用UTF-8编码
    if hasattr(sys.stdout, 'buffer'):
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    print("🚀 开始初始化SQLite数据库...")
    
    try:
        # 检查schema文件是否存在
        schema_file = os.path.join(os.path.dirname(__file__), 'sqlite_schema.sql')
        if not os.path.exists(schema_file):
            print("❌ sqlite_schema.sql 文件不存在")
            return False
        
        print("✅ 找到sqlite_schema.sql文件")
        
        # 连接数据库
        conn = sqlite3.connect('dw_metadata.db')
        
        # 检查是否需要重置
        if force_reset:
            print("🔥 强制重置模式：删除所有现有表...")
            drop_all_tables(conn)
        elif check_tables_exist(conn):
            if auto_reset:
                print("⚠️ 发现已存在的表，自动重置模式：删除现有表...")
                drop_all_tables(conn)
            else:
                print("⚠️ 发现已存在的表")
                response = input("是否要删除现有表并重新创建？(y/N): ")
                if response.lower() in ['y', 'yes']:
                    drop_all_tables(conn)
                else:
                    print("❌ 用户取消操作")
                    conn.close()
                    return False
        
        # 执行SQL创建表结构
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema_sql = f.read()
        
        conn.executescript(schema_sql)
        conn.close()
        
        print("✅ 数据库初始化完成")
        
        # 验证数据库结构
        all_passed, verification_results = verify_sqlite_structure()
        
        if all_passed:
            print("\n🎉 SQLite数据库初始化成功！所有表结构都已正确创建。")
        else:
            print("\n⚠️  SQLite数据库初始化完成，但部分结构存在问题，请检查上述错误信息。")
            
    except Exception as e:
        print(f"❌ 初始化失败: {e}")
        return False
    
    return all_passed

if __name__ == "__main__":
    # 解析命令行参数
    import argparse
    parser = argparse.ArgumentParser(description='初始化SQLite数据库')
    parser.add_argument('--auto-reset', action='store_true', 
                       help='自动重置冲突的表，无需用户确认')
    parser.add_argument('--force-reset', action='store_true',
                       help='强制重置所有表，无需检查冲突')
    args = parser.parse_args()
    
    success = main(auto_reset=args.auto_reset, force_reset=args.force_reset)
    exit(0 if success else 1)
