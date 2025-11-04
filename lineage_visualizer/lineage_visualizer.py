"""
数据血缘可视化工具
基于 Graphviz 生成数据血缘关系图
支持节点过滤、层级展示、颜色分组等功能
"""

import json
import argparse
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple
from collections import defaultdict
import graphviz


class LineageVisualizer:
    """数据血缘可视化类"""
    
    # 预定义的配色方案
    SCHEMA_COLORS = {
        'MDB_AL': '#FFE6E6',      # 浅红色
        'CDBVIEW': '#E6F3FF',     # 浅蓝色
        'ODBVIEW': '#E6FFE6',     # 浅绿色
        'PDBVIEW': '#FFF9E6',     # 浅黄色
        'default': '#F0F0F0'      # 浅灰色
    }
    
    def __init__(self, json_file: str):
        """
        初始化可视化工具
        
        Args:
            json_file: networkx JSON 格式的数据血缘文件路径
        """
        self.json_file = json_file
        self.nodes = []
        self.links = []
        self.graph_data = {}
        self.load_data()
        
    def load_data(self):
        """加载 JSON 数据"""
        with open(self.json_file, 'r', encoding='utf-8') as f:
            self.graph_data = json.load(f)
        
        self.nodes = self.graph_data.get('nodes', [])
        self.links = self.graph_data.get('links', [])
        
        print(f"✓ 加载完成: {len(self.nodes)} 个节点, {len(self.links)} 条边")
    
    def get_schema_color(self, schema: str) -> str:
        """获取 schema 对应的颜色"""
        return self.SCHEMA_COLORS.get(schema, self.SCHEMA_COLORS['default'])
    
    def filter_nodes_by_schema(self, schemas: List[str]) -> Set[str]:
        """根据 schema 过滤节点"""
        filtered_ids = set()
        for node in self.nodes:
            if node.get('schema') in schemas:
                filtered_ids.add(node.get('id'))
        return filtered_ids
    
    def filter_nodes_by_pattern(self, pattern: str) -> Set[str]:
        """根据表名模式过滤节点（支持通配符）"""
        import re
        filtered_ids = set()
        regex_pattern = pattern.replace('*', '.*').replace('?', '.')
        regex = re.compile(regex_pattern, re.IGNORECASE)
        
        for node in self.nodes:
            table_name = node.get('table', '')
            node_id = node.get('id', '')
            if regex.search(table_name) or regex.search(node_id):
                filtered_ids.add(node_id)
        return filtered_ids
    
    def get_upstream_nodes(self, node_id: str, max_depth: int = -1) -> Set[str]:
        """获取指定节点的上游节点（递归）"""
        upstream = set()
        visited = set()
        
        def dfs(current_id: str, depth: int):
            if max_depth != -1 and depth > max_depth:
                return
            if current_id in visited:
                return
            visited.add(current_id)
            
            for link in self.links:
                if link['target'] == current_id:
                    source_id = link['source']
                    upstream.add(source_id)
                    dfs(source_id, depth + 1)
        
        dfs(node_id, 0)
        return upstream
    
    def get_downstream_nodes(self, node_id: str, max_depth: int = -1) -> Set[str]:
        """获取指定节点的下游节点（递归）"""
        downstream = set()
        visited = set()
        
        def dfs(current_id: str, depth: int):
            if max_depth != -1 and depth > max_depth:
                return
            if current_id in visited:
                return
            visited.add(current_id)
            
            for link in self.links:
                if link['source'] == current_id:
                    target_id = link['target']
                    downstream.add(target_id)
                    dfs(target_id, depth + 1)
        
        dfs(node_id, 0)
        return downstream
    
    def get_subgraph(self, focus_node: str, upstream_depth: int = -1, 
                     downstream_depth: int = -1) -> Tuple[Set[str], Set[str]]:
        """
        获取以某个节点为中心的子图
        
        Returns:
            (nodes_to_include, edges_to_include)
        """
        nodes_to_include = {focus_node}
        
        if upstream_depth != 0:
            upstream = self.get_upstream_nodes(focus_node, upstream_depth)
            nodes_to_include.update(upstream)
        
        if downstream_depth != 0:
            downstream = self.get_downstream_nodes(focus_node, downstream_depth)
            nodes_to_include.update(downstream)
        
        # 过滤边
        edges_to_include = set()
        for link in self.links:
            if link['source'] in nodes_to_include and link['target'] in nodes_to_include:
                edges_to_include.add((link['source'], link['target']))
        
        return nodes_to_include, edges_to_include
    
    def create_graph(self, 
                    output_file: str = 'lineage',
                    format: str = 'svg',
                    layout: str = 'dot',
                    filter_schemas: Optional[List[str]] = None,
                    filter_pattern: Optional[str] = None,
                    focus_node: Optional[str] = None,
                    upstream_depth: int = -1,
                    downstream_depth: int = -1,
                    show_schema_labels: bool = True,
                    rankdir: str = 'LR',
                    node_style: str = 'rounded',
                    edge_labels: bool = False) -> str:
        """
        创建数据血缘可视化图
        
        Args:
            output_file: 输出文件名（不含扩展名）
            format: 输出格式 (svg, png, pdf, etc.)
            layout: 布局引擎 (dot, neato, fdp, sfdp, circo, twopi)
            filter_schemas: 只显示指定 schema 的表
            filter_pattern: 表名过滤模式（支持通配符）
            focus_node: 聚焦节点（显示其上下游）
            upstream_depth: 上游深度（-1 表示无限）
            downstream_depth: 下游深度（-1 表示无限）
            show_schema_labels: 是否在节点中显示 schema
            rankdir: 图的方向 (LR: 左到右, TB: 上到下, RL: 右到左, BT: 下到上)
            node_style: 节点样式 (rounded, box, ellipse, etc.)
            edge_labels: 是否显示边标签
            
        Returns:
            生成的文件路径
        """
        # 创建有向图
        dot = graphviz.Digraph(
            name='DataLineage',
            engine=layout,
            format=format
        )
        
        # 设置图属性
        dot.attr(rankdir=rankdir)
        dot.attr('graph', 
                 bgcolor='white',
                 splines='ortho',  # 正交线条
                 nodesep='0.5',
                 ranksep='1.0')
        
        # 设置默认节点属性
        dot.attr('node',
                 shape=node_style,
                 style='filled',
                 fontname='Microsoft YaHei',
                 fontsize='10')
        
        # 设置默认边属性
        dot.attr('edge',
                 fontname='Microsoft YaHei',
                 fontsize='8',
                 color='#666666')
        
        # 确定要包含的节点
        nodes_to_include = set(node['id'] for node in self.nodes)
        edges_to_include = None
        
        # 应用过滤
        if focus_node:
            nodes_to_include, edges_to_include = self.get_subgraph(
                focus_node, upstream_depth, downstream_depth
            )
            print(f"✓ 聚焦节点: {focus_node}")
            print(f"  包含节点数: {len(nodes_to_include)}")
        
        if filter_schemas:
            filtered = self.filter_nodes_by_schema(filter_schemas)
            nodes_to_include = nodes_to_include.intersection(filtered)
            print(f"✓ 按 schema 过滤: {filter_schemas}")
            print(f"  剩余节点数: {len(nodes_to_include)}")
        
        if filter_pattern:
            filtered = self.filter_nodes_by_pattern(filter_pattern)
            nodes_to_include = nodes_to_include.intersection(filtered)
            print(f"✓ 按模式过滤: {filter_pattern}")
            print(f"  剩余节点数: {len(nodes_to_include)}")
        
        # 按 schema 分组创建子图（用于更好的布局）
        schema_groups = defaultdict(list)
        for node in self.nodes:
            if node['id'] in nodes_to_include:
                schema = node.get('schema', 'unknown')
                schema_groups[schema].append(node)
        
        # 添加节点
        for schema, nodes_in_schema in schema_groups.items():
            # 为每个 schema 创建一个子图（用于视觉分组）
            with dot.subgraph(name=f'cluster_{schema}') as subgraph:
                subgraph.attr(label=schema, 
                            style='filled',
                            color='lightgrey',
                            fontname='Microsoft YaHei',
                            fontsize='12')
                
                for node in nodes_in_schema:
                    node_id = node['id']
                    table = node.get('table', '')
                    
                    # 构建节点标签
                    if show_schema_labels:
                        label = f"{schema}\n{table}"
                    else:
                        label = table
                    
                    # 如果是聚焦节点，使用特殊样式
                    if node_id == focus_node:
                        subgraph.node(node_id, 
                                    label=label,
                                    fillcolor='#FFD700',  # 金色
                                    color='#FF6347',      # 红色边框
                                    penwidth='3.0')
                    else:
                        subgraph.node(node_id,
                                    label=label,
                                    fillcolor=self.get_schema_color(schema))
        
        # 添加边
        edge_count = 0
        for link in self.links:
            source = link['source']
            target = link['target']
            
            # 检查是否在要包含的节点中
            if source not in nodes_to_include or target not in nodes_to_include:
                continue
            
            # 如果有边过滤
            if edges_to_include is not None:
                if (source, target) not in edges_to_include:
                    continue
            
            # 添加边标签（可选）
            if edge_labels and 'script_paths' in link and link['script_paths']:
                # 只显示文件名，不显示完整路径
                script_path = link['script_paths'][0]
                script_name = Path(script_path).name
                dot.edge(source, target, label=script_name, tooltip=script_path)
            else:
                dot.edge(source, target)
            
            edge_count += 1
        
        print(f"✓ 图生成完成: {len(nodes_to_include)} 个节点, {edge_count} 条边")
        
        # 渲染输出
        output_path = dot.render(output_file, cleanup=True)
        print(f"✓ 文件已保存: {output_path}")
        
        return output_path
    
    def get_statistics(self) -> Dict:
        """获取数据血缘统计信息"""
        stats = {
            'total_nodes': len(self.nodes),
            'total_edges': len(self.links),
            'schemas': defaultdict(int),
            'in_degree': defaultdict(int),
            'out_degree': defaultdict(int)
        }
        
        # 统计每个 schema 的表数量
        for node in self.nodes:
            schema = node.get('schema', 'unknown')
            stats['schemas'][schema] += 1
        
        # 统计入度和出度
        for link in self.links:
            stats['out_degree'][link['source']] += 1
            stats['in_degree'][link['target']] += 1
        
        return stats
    
    def print_statistics(self):
        """打印统计信息"""
        stats = self.get_statistics()
        
        print("\n" + "="*60)
        print("数据血缘统计信息")
        print("="*60)
        print(f"总节点数: {stats['total_nodes']}")
        print(f"总边数: {stats['total_edges']}")
        print(f"\nSchema 分布:")
        for schema, count in sorted(stats['schemas'].items(), key=lambda x: x[1], reverse=True):
            print(f"  {schema}: {count} 个表")
        
        # 找出入度和出度最高的节点
        if stats['in_degree']:
            top_in = sorted(stats['in_degree'].items(), key=lambda x: x[1], reverse=True)[:5]
            print(f"\n入度最高的节点（被依赖最多）:")
            for node_id, degree in top_in:
                print(f"  {node_id}: {degree}")
        
        if stats['out_degree']:
            top_out = sorted(stats['out_degree'].items(), key=lambda x: x[1], reverse=True)[:5]
            print(f"\n出度最高的节点（依赖其他表最多）:")
            for node_id, degree in top_out:
                print(f"  {node_id}: {degree}")
        
        print("="*60 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='数据血缘可视化工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 基础用法：生成完整的数据血缘图
  python lineage_visualizer.py datalineage.json
  
  # 指定输出格式和文件名
  python lineage_visualizer.py datalineage.json -o output/lineage -f png
  
  # 只显示特定 schema 的表
  python lineage_visualizer.py datalineage.json --schemas MDB_AL CDBVIEW
  
  # 根据表名模式过滤
  python lineage_visualizer.py datalineage.json --pattern "*LOAN*"
  
  # 聚焦某个表，显示其上下游（各2层）
  python lineage_visualizer.py datalineage.json --focus "MDB_AL.AL88_CORE_LIAB_TRL_CLC_AGT_SUM" --upstream 2 --downstream 2
  
  # 使用不同布局引擎
  python lineage_visualizer.py datalineage.json --layout neato
  
  # 显示统计信息
  python lineage_visualizer.py datalineage.json --stats-only
        """
    )
    
    parser.add_argument('json_file', 
                       help='networkx JSON 格式的数据血缘文件')
    parser.add_argument('-o', '--output', 
                       default='lineage',
                       help='输出文件名（不含扩展名，默认: lineage）')
    parser.add_argument('-f', '--format', 
                       default='svg',
                       choices=['svg', 'png', 'pdf', 'jpg'],
                       help='输出格式（默认: svg）')
    parser.add_argument('-l', '--layout',
                       default='dot',
                       choices=['dot', 'neato', 'fdp', 'sfdp', 'circo', 'twopi'],
                       help='布局引擎（默认: dot）')
    parser.add_argument('--schemas',
                       nargs='+',
                       help='只显示指定 schema 的表')
    parser.add_argument('--pattern',
                       help='表名过滤模式（支持 * 和 ? 通配符）')
    parser.add_argument('--focus',
                       help='聚焦节点 ID（显示其上下游）')
    parser.add_argument('--upstream',
                       type=int,
                       default=-1,
                       help='上游深度（-1 表示无限，默认: -1）')
    parser.add_argument('--downstream',
                       type=int,
                       default=-1,
                       help='下游深度（-1 表示无限，默认: -1）')
    parser.add_argument('--rankdir',
                       default='LR',
                       choices=['LR', 'TB', 'RL', 'BT'],
                       help='图的方向（LR:左到右, TB:上到下, 默认: LR）')
    parser.add_argument('--no-schema-labels',
                       action='store_true',
                       help='不在节点中显示 schema')
    parser.add_argument('--edge-labels',
                       action='store_true',
                       help='显示边标签（脚本文件名）')
    parser.add_argument('--node-style',
                       default='rounded',
                       help='节点样式（默认: rounded）')
    parser.add_argument('--stats-only',
                       action='store_true',
                       help='只显示统计信息，不生成图')
    
    args = parser.parse_args()
    
    # 创建可视化工具实例
    visualizer = LineageVisualizer(args.json_file)
    
    # 显示统计信息
    if args.stats_only:
        visualizer.print_statistics()
        return
    
    # 生成图
    visualizer.create_graph(
        output_file=args.output,
        format=args.format,
        layout=args.layout,
        filter_schemas=args.schemas,
        filter_pattern=args.pattern,
        focus_node=args.focus,
        upstream_depth=args.upstream,
        downstream_depth=args.downstream,
        show_schema_labels=not args.no_schema_labels,
        rankdir=args.rankdir,
        node_style=args.node_style,
        edge_labels=args.edge_labels
    )
    
    # 显示统计信息
    visualizer.print_statistics()


if __name__ == '__main__':
    main()

