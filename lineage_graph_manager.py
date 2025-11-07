"""
数据血缘图管理器
使用networkx从detail层推导summary层
"""
import sqlite3
import networkx as nx
from typing import List, Tuple, Dict, Set
import json
import os
from networkx.readwrite import json_graph


class LineageGraphManager:
    """
    血缘关系图管理器
    
    功能：
    1. 从data_lineage_detail构建detail层图（包含临时表）
    2. 推导summary层图（仅实体表，跳过临时表）
    3. 将summary保存到data_lineage_summary表
    """
    
    def __init__(self):
        self.detail_graph = nx.DiGraph()  # detail层图（语句级，含临时表）
        self.summary_graph = nx.DiGraph()  # summary层图（脚本级，仅实体表）
        self.script_id = None
    
    def build_detail_graph(self, cursor: sqlite3.Cursor, script_id: str) -> None:
        """
        从data_lineage_detail表构建detail层图
        
        Args:
            cursor: 数据库游标
            script_id: 脚本ID
        """
        self.script_id = script_id
        self.detail_graph.clear()
        
        # 查询所有血缘记录
        cursor.execute("""
            SELECT 
                source_t.id as source_id,
                source_t.schema_name as source_schema,
                source_t.table_name as source_name,
                source_t.table_type as source_type,
                source_t.script_id as src_script_id,
                target_t.id as target_id,
                target_t.schema_name as target_schema,
                target_t.table_name as target_name,
                target_t.table_type as target_type,
                target_t.script_id as tgt_script_id,
                dld.statement_id,
                st.statement_index,
                st.statement_type
            FROM data_lineage_detail dld
            JOIN tables source_t ON dld.source_table_id = source_t.id
            JOIN tables target_t ON dld.target_table_id = target_t.id
            JOIN script_statements st ON dld.statement_id = st.id
            WHERE dld.script_id = ?
            ORDER BY st.statement_index
        """, (script_id,))
        
        edges_added = 0
        for row in cursor.fetchall():
            src_id, src_schema, src_name, src_type, src_script, \
            tgt_id, tgt_schema, tgt_name, tgt_type, tgt_script, \
            stmt_id, stmt_idx, stmt_type = row
            
            # 添加源节点（使用table_id作为节点ID）
            if not self.detail_graph.has_node(src_id):
                self.detail_graph.add_node(
                    src_id,
                    schema_name=src_schema or '',
                    table_name=src_name,
                    node_type=src_type,
                    table_script_id=src_script or '',
                    is_entity=(src_type in ['TABLE', 'VIEW'])
                )
            
            # 添加目标节点（使用table_id作为节点ID）
            if not self.detail_graph.has_node(tgt_id):
                self.detail_graph.add_node(
                    tgt_id,
                    schema_name=tgt_schema or '',
                    table_name=tgt_name,
                    node_type=tgt_type,
                    table_script_id=tgt_script or '',
                    is_entity=(tgt_type in ['TABLE', 'VIEW'])
                )
            
            # 添加边（语句级）
            self.detail_graph.add_edge(
                src_id,
                tgt_id,
                edge_type='STATEMENT',
                script_id=script_id,
                statement_id=stmt_id,
                statement_index=stmt_idx,
                statement_type=stmt_type
            )
            edges_added += 1
        
        print(f"    Detail图: {self.detail_graph.number_of_nodes()} 个节点, {edges_added} 条边")
    
    def generate_summary_graph(self, max_path_length: int = 20) -> Dict[str, int]:
        """
        从detail图推导summary图
        
        策略：
        1. 找到所有实体表节点
        2. 对每对实体表，找到所有可能的路径（允许临时表间有环）
        3. 记录所有路径到summary图
        
        Args:
            max_path_length: 最大路径长度（防止无限循环）
        
        Returns:
            统计信息字典
        """
        self.summary_graph.clear()
        
        # 1. 找到所有实体表节点
        entity_nodes = [
            n for n, attr in self.detail_graph.nodes(data=True)
            if attr.get('is_entity', False)
        ]
        
        print(f"    实体表节点: {len(entity_nodes)} 个")
        
        if len(entity_nodes) == 0:
            print("    ⚠️  没有实体表节点，无法生成summary")
            return {'entity_count': 0, 'path_count': 0, 'summary_edge_count': 0}
        
        # 2. 找到所有实体表之间的路径
        path_count = 0
        summary_edge_count = 0
        paths_by_pair = {}  # 记录每对表之间的所有路径
        
        for source in entity_nodes:
            for target in entity_nodes:
                if source == target:
                    continue
                
                # 使用all_simple_paths找路径（不包含环）
                # 注意：这会自动避免节点重复，所以临时表的环会被跳过
                try:
                    paths = list(nx.all_simple_paths(
                        self.detail_graph,
                        source,
                        target,
                        cutoff=max_path_length
                    ))
                    
                    if paths:
                        paths_by_pair[(source, target)] = paths
                        path_count += len(paths)
                        
                        # 添加summary边（所有路径共享一条边，但记录路径信息）
                        # 将所有路径信息合并
                        all_paths_str = ' | '.join([' -> '.join(p) for p in paths])
                        min_hops = min(len(p) - 1 for p in paths)
                        max_hops = max(len(p) - 1 for p in paths)
                        
                        # 添加节点（从detail图继承属性）
                        if not self.summary_graph.has_node(source):
                            src_attrs = self.detail_graph.nodes[source]
                            self.summary_graph.add_node(
                                source,
                                schema_name=src_attrs.get('schema_name', ''),
                                table_name=src_attrs.get('table_name', ''),
                                node_type=src_attrs.get('node_type', 'TABLE'),
                                is_entity=True
                            )
                        if not self.summary_graph.has_node(target):
                            tgt_attrs = self.detail_graph.nodes[target]
                            self.summary_graph.add_node(
                                target,
                                schema_name=tgt_attrs.get('schema_name', ''),
                                table_name=tgt_attrs.get('table_name', ''),
                                node_type=tgt_attrs.get('node_type', 'TABLE'),
                                is_entity=True
                            )
                        
                        # 添加边（脚本级）
                        self.summary_graph.add_edge(
                            source,
                            target,
                            edge_type='SCRIPT',
                            script_id=self.script_id,
                            path_count=len(paths),
                            min_hop_count=min_hops,
                            max_hop_count=max_hops,
                            all_paths=all_paths_str[:500]  # 限制长度避免过长
                        )
                        summary_edge_count += 1
                        
                except nx.NetworkXNoPath:
                    # 没有路径，跳过
                    continue
                except nx.NodeNotFound:
                    # 节点不存在，跳过
                    continue
        
        stats = {
            'entity_count': len(entity_nodes),
            'path_count': path_count,
            'summary_edge_count': summary_edge_count
        }
        
        print(f"    Summary图: 找到 {path_count} 条路径, 生成 {summary_edge_count} 条边")
        
        return stats
    
    def save_summary_to_db(self, cursor: sqlite3.Cursor) -> int:
        """
        将summary图保存到data_lineage_summary表
        
        Args:
            cursor: 数据库游标
        
        Returns:
            保存的记录数
        """
        saved_count = 0
        
        for source_id, target_id, edge_data in self.summary_graph.edges(data=True):
            # 节点ID就是table_id，直接使用
            # 生成lineage_id
            lineage_id = f"{target_id}__{source_id}__{self.script_id}"
            
            # 插入数据库
            cursor.execute("""
                INSERT OR REPLACE INTO data_lineage_summary (
                    id, target_table_id, source_table_id, script_id
                ) VALUES (?, ?, ?, ?)
            """, (lineage_id, target_id, source_id, self.script_id))
            
            saved_count += 1
        
        return saved_count
    
    def _get_table_id(self, cursor: sqlite3.Cursor, table_name: str) -> str:
        """
        根据表名获取表ID
        
        优先查找实体表，找不到再查找临时表
        
        Args:
            cursor: 数据库游标
            table_name: 表名
        
        Returns:
            表ID，如果找不到返回None
        """
        # 先尝试实体表（script_id为空）
        cursor.execute("""
            SELECT id FROM tables 
            WHERE table_name = ? AND (script_id = '' OR script_id IS NULL)
            LIMIT 1
        """, (table_name,))
        
        row = cursor.fetchone()
        if row:
            return row[0]
        
        # 再尝试临时表（属于当前脚本）
        cursor.execute("""
            SELECT id FROM tables 
            WHERE table_name = ? AND script_id = ?
            LIMIT 1
        """, (table_name, self.script_id))
        
        row = cursor.fetchone()
        if row:
            return row[0]
        
        return None
    
    def detect_cycles(self) -> List[List[str]]:
        """
        检测detail图中的环路
        
        Returns:
            环路列表，每个环路是一个节点列表
        """
        try:
            cycles = list(nx.simple_cycles(self.detail_graph))
            return cycles
        except:
            return []
    
    def get_statistics(self) -> Dict:
        """
        获取图的统计信息
        
        Returns:
            统计信息字典
        """
        entity_nodes = [
            n for n, attr in self.detail_graph.nodes(data=True)
            if attr.get('is_entity', False)
        ]
        temp_nodes = [
            n for n, attr in self.detail_graph.nodes(data=True)
            if not attr.get('is_entity', False)
        ]
        
        cycles = self.detect_cycles()
        
        return {
            'detail_nodes': self.detail_graph.number_of_nodes(),
            'detail_edges': self.detail_graph.number_of_edges(),
            'entity_nodes': len(entity_nodes),
            'temp_nodes': len(temp_nodes),
            'cycles': len(cycles),
            'summary_nodes': self.summary_graph.number_of_nodes(),
            'summary_edges': self.summary_graph.number_of_edges()
        }
    
    def export_to_json(self, script_name: str, output_dir: str = './datalineage/scripts') -> Tuple[str, str]:
        """
        导出detail和summary图为JSON文件
        
        使用networkx的标准node_link格式导出
        
        Args:
            script_name: 脚本名称（不含扩展名）
            output_dir: 输出目录
        
        Returns:
            (detail文件路径, summary文件路径)
        """
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        # 导出detail图
        detail_file = os.path.join(output_dir, f"{script_name}_detail.json")
        detail_data = json_graph.node_link_data(self.detail_graph)
        with open(detail_file, 'w', encoding='utf-8') as f:
            json.dump(detail_data, f, indent=2, ensure_ascii=False)
        
        # 导出summary图
        summary_file = os.path.join(output_dir, f"{script_name}_summary.json")
        summary_data = json_graph.node_link_data(self.summary_graph)
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary_data, f, indent=2, ensure_ascii=False)
        
        return detail_file, summary_file


def generate_lineage_summary(cursor: sqlite3.Cursor, script_id: str, export_json: bool = True) -> bool:
    """
    为指定脚本生成血缘summary
    
    这是一个便捷函数，封装了完整的流程
    
    Args:
        cursor: 数据库游标
        script_id: 脚本ID
        export_json: 是否导出JSON文件
    
    Returns:
        是否成功
    """
    try:
        print(f"  🔄 正在生成 {script_id} 的summary...")
        
        # 1. 构建manager
        manager = LineageGraphManager()
        
        # 2. 构建detail图
        manager.build_detail_graph(cursor, script_id)
        
        # 3. 检测环路（可选，仅用于报告）
        cycles = manager.detect_cycles()
        if cycles:
            print(f"    ℹ️  检测到 {len(cycles)} 个环路（临时表间）")
            for i, cycle in enumerate(cycles[:3], 1):  # 只显示前3个
                print(f"       环路{i}: {' -> '.join(cycle + [cycle[0]])}")
        
        # 4. 生成summary图
        stats = manager.generate_summary_graph()
        
        # 5. 保存到数据库
        if stats['summary_edge_count'] > 0:
            saved_count = manager.save_summary_to_db(cursor)
            print(f"  ✅ Summary生成完成: {saved_count} 条记录")
        else:
            print(f"  ℹ️  没有实体表间的血缘关系，无需生成summary")
        
        # 6. 导出JSON（如果需要）
        if export_json:
            try:
                detail_file, summary_file = manager.export_to_json(script_id)
                print(f"  📁 JSON已导出:")
                print(f"     Detail:  {detail_file}")
                print(f"     Summary: {summary_file}")
            except Exception as e:
                print(f"  ⚠️  JSON导出失败: {e}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Summary生成失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def export_all_lineage(cursor: sqlite3.Cursor, output_dir: str = './datalineage') -> Tuple[str, str]:
    """
    导出所有脚本的合并血缘到单个JSON文件
    
    Args:
        cursor: 数据库游标
        output_dir: 输出目录
    
    Returns:
        (all_detail文件路径, all_summary文件路径)
    """
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. 构建所有脚本的detail图
    all_detail_graph = nx.DiGraph()
    
    cursor.execute("""
        SELECT 
            t1.id as source_id,
            t1.schema_name as source_schema,
            t1.table_name as source_name,
            t1.table_type as source_type,
            t2.id as target_id,
            t2.schema_name as target_schema,
            t2.table_name as target_name,
            t2.table_type as target_type,
            d.script_id,
            d.statement_id,
            ss.statement_index,
            ss.statement_type
        FROM data_lineage_detail d
        JOIN tables t1 ON d.source_table_id = t1.id
        JOIN tables t2 ON d.target_table_id = t2.id
        LEFT JOIN script_statements ss ON d.statement_id = ss.id
    """)
    
    for row in cursor.fetchall():
        source_id, source_schema, source_name, source_type, \
        target_id, target_schema, target_name, target_type, \
        script_id, statement_id, stmt_idx, stmt_type = row
        
        # 添加节点（使用table_id作为节点ID）
        if not all_detail_graph.has_node(source_id):
            all_detail_graph.add_node(
                source_id,
                schema_name=source_schema or '',
                table_name=source_name,
                node_type=source_type,
                is_entity=(source_type in ['TABLE', 'VIEW'])
            )
        
        if not all_detail_graph.has_node(target_id):
            all_detail_graph.add_node(
                target_id,
                schema_name=target_schema or '',
                table_name=target_name,
                node_type=target_type,
                is_entity=(target_type in ['TABLE', 'VIEW'])
            )
        
        # 添加边
        all_detail_graph.add_edge(
            source_id, target_id,
            edge_type='statement',
            script_id=script_id,
            statement_id=statement_id or '',
            statement_index=stmt_idx or 0,
            statement_type=stmt_type or ''
        )
    
    # 2. 构建所有脚本的summary图
    all_summary_graph = nx.DiGraph()
    
    cursor.execute("""
        SELECT 
            t1.id as source_id,
            t1.schema_name as source_schema,
            t1.table_name as source_name,
            t1.table_type as source_type,
            t2.id as target_id,
            t2.schema_name as target_schema,
            t2.table_name as target_name,
            t2.table_type as target_type,
            s.script_id
        FROM data_lineage_summary s
        JOIN tables t1 ON s.source_table_id = t1.id
        JOIN tables t2 ON s.target_table_id = t2.id
    """)
    
    for row in cursor.fetchall():
        source_id, source_schema, source_name, source_type, \
        target_id, target_schema, target_name, target_type, \
        script_id = row
        
        # 添加节点（使用table_id作为节点ID）
        if not all_summary_graph.has_node(source_id):
            all_summary_graph.add_node(
                source_id,
                schema_name=source_schema or '',
                table_name=source_name,
                node_type=source_type,
                is_entity=True
            )
        
        if not all_summary_graph.has_node(target_id):
            all_summary_graph.add_node(
                target_id,
                schema_name=target_schema or '',
                table_name=target_name,
                node_type=target_type,
                is_entity=True
            )
        
        # 添加边
        all_summary_graph.add_edge(
            source_id, target_id,
            edge_type='script',
            script_id=script_id
        )
    
    # 3. 导出detail图
    detail_file = os.path.join(output_dir, 'all_lineage_detail.json')
    detail_data = json_graph.node_link_data(all_detail_graph)
    with open(detail_file, 'w', encoding='utf-8') as f:
        json.dump(detail_data, f, indent=2, ensure_ascii=False)
    
    # 4. 导出summary图
    summary_file = os.path.join(output_dir, 'all_lineage_summary.json')
    summary_data = json_graph.node_link_data(all_summary_graph)
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary_data, f, indent=2, ensure_ascii=False)
    
    print(f"  📁 全局JSON已导出:")
    print(f"     All Detail:  {detail_file} ({all_detail_graph.number_of_nodes()} nodes, {all_detail_graph.number_of_edges()} edges)")
    print(f"     All Summary: {summary_file} ({all_summary_graph.number_of_nodes()} nodes, {all_summary_graph.number_of_edges()} edges)")
    
    return detail_file, summary_file


if __name__ == "__main__":
    # 测试代码
    import sqlite3
    
    conn = sqlite3.connect('dw_metadata.db')
    cursor = conn.cursor()
    
    # 获取所有脚本
    cursor.execute("SELECT DISTINCT script_id FROM data_lineage_detail")
    script_ids = [row[0] for row in cursor.fetchall()]
    
    print(f"找到 {len(script_ids)} 个脚本")
    
    for script_id in script_ids:
        print(f"\n处理脚本: {script_id}")
        generate_lineage_summary(cursor, script_id)
    
    # 导出全局血缘
    print("\n导出全局血缘...")
    export_all_lineage(cursor)
    
    conn.commit()
    conn.close()
    
    print("\n✅ 所有脚本的summary已生成")

