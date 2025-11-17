"""
导出所有脚本的合并血缘到JSON文件

用法：
    python export_all_lineage.py
    
    或者在代码中：
    from export_all_lineage import export_all_lineage_json
    export_all_lineage_json()
"""
import sqlite3
from lineage_graph_manager import export_all_lineage


def export_all_lineage_json(db_path: str = 'dw_metadata.db', output_dir: str = './datalineage'):
    """
    导出所有脚本的合并血缘到JSON文件
    
    Args:
        db_path: 数据库文件路径
        output_dir: 输出目录
    """
    print("=" * 60)
    print("导出所有脚本的合并血缘到JSON")
    print("=" * 60)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 检查是否有数据
        cursor.execute("SELECT COUNT(*) FROM data_lineage_detail")
        detail_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM data_lineage_summary")
        summary_count = cursor.fetchone()[0]
        
        print(f"\n数据库统计:")
        print(f"  Detail记录: {detail_count}")
        print(f"  Summary记录: {summary_count}")
        
        if detail_count == 0:
            print("\n⚠️  数据库中没有血缘数据，无需导出")
            return False
        
        # 导出全局血缘
        print(f"\n开始导出...")
        detail_file, summary_file = export_all_lineage(cursor, output_dir)
        
        print(f"\n✅ 导出完成!")
        print(f"   Detail文件: {detail_file}")
        print(f"   Summary文件: {summary_file}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ 导出失败: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        conn.close()


if __name__ == "__main__":
    export_all_lineage_json()

