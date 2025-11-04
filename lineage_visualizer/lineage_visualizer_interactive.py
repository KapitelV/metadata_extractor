"""
交互式数据血缘可视化工具
基于 pyvis 生成可交互的 HTML 页面
支持拖拽、缩放、搜索等功能
"""

import json
import argparse
from pathlib import Path
from typing import Dict, List, Set, Optional
from collections import defaultdict

try:
    from pyvis.network import Network
    PYVIS_AVAILABLE = True
except ImportError:
    PYVIS_AVAILABLE = False
    print("⚠️  pyvis 未安装。要使用交互式可视化，请运行: pip install pyvis")


class InteractiveLineageVisualizer:
    """交互式数据血缘可视化类"""
    
    # 预定义的配色方案
    SCHEMA_COLORS = {
        'MDB_AL': '#ff6b6b',      # 红色
        'CDBVIEW': '#4ecdc4',     # 青色
        'ODBVIEW': '#95e1d3',     # 浅绿色
        'PDBVIEW': '#f9ca24',     # 黄色
        'default': '#95a5a6'      # 灰色
    }
    
    def __init__(self, json_file: str):
        """
        初始化交互式可视化工具
        
        Args:
            json_file: networkx JSON 格式的数据血缘文件路径
        """
        if not PYVIS_AVAILABLE:
            raise ImportError("请先安装 pyvis: pip install pyvis")
        
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
        """根据表名模式过滤节点"""
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
        """获取指定节点的上游节点"""
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
        """获取指定节点的下游节点"""
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
    
    def create_interactive_graph(self,
                                output_file: str = 'lineage_interactive.html',
                                filter_schemas: Optional[List[str]] = None,
                                filter_pattern: Optional[str] = None,
                                focus_node: Optional[str] = None,
                                upstream_depth: int = -1,
                                downstream_depth: int = -1,
                                height: str = '900px',
                                width: str = '100%',
                                physics_enabled: bool = True,
                                show_buttons: bool = True) -> str:
        """
        创建交互式数据血缘图
        
        Args:
            output_file: 输出 HTML 文件名
            filter_schemas: 只显示指定 schema 的表
            filter_pattern: 表名过滤模式
            focus_node: 聚焦节点
            upstream_depth: 上游深度
            downstream_depth: 下游深度
            height: 图的高度
            width: 图的宽度
            physics_enabled: 是否启用物理引擎
            show_buttons: 是否显示控制按钮
            
        Returns:
            生成的文件路径
        """
        # 创建网络图
        net = Network(
            height=height,
            width=width,
            directed=True,
            notebook=False,
            bgcolor='#ffffff',
            font_color='#000000'
        )
        
        # 先确定要包含的节点（用于配置物理引擎）
        nodes_to_include = set(node['id'] for node in self.nodes)
        
        # 应用过滤（临时，用于确定节点数量）
        if focus_node:
            temp_nodes = {focus_node}
            if upstream_depth != 0:
                upstream = self.get_upstream_nodes(focus_node, upstream_depth)
                temp_nodes.update(upstream)
            if downstream_depth != 0:
                downstream = self.get_downstream_nodes(focus_node, downstream_depth)
                temp_nodes.update(downstream)
            nodes_to_include = temp_nodes
        
        if filter_schemas:
            filtered = self.filter_nodes_by_schema(filter_schemas)
            nodes_to_include = nodes_to_include.intersection(filtered)
        
        if filter_pattern:
            filtered = self.filter_nodes_by_pattern(filter_pattern)
            nodes_to_include = nodes_to_include.intersection(filtered)
        
        # 根据最终节点数量配置物理引擎
        node_count = len(nodes_to_include)
        
        if physics_enabled:
            # 对于大型图，使用更适合的物理引擎设置
            if node_count > 500:
                # 大型图：使用更快的布局算法
                physics_config = """
            {
              "physics": {
                "enabled": true,
                "solver": "barnesHut",
                "barnesHut": {
                  "gravitationalConstant": -8000,
                  "centralGravity": 0.1,
                  "springLength": 100,
                  "springConstant": 0.04,
                  "damping": 0.09,
                  "avoidOverlap": 0.5
                },
                "maxVelocity": 50,
                "minVelocity": 0.1,
                "stabilization": {
                  "enabled": true,
                  "iterations": 200,
                  "updateInterval": 10,
                  "onlyDynamicEdges": false,
                  "fit": true
                }
              },
              "interaction": {
                "hover": true,
                "tooltipDelay": 100,
                "zoomView": true,
                "dragView": true,
                "zoomSpeed": 1.0
              },
              "layout": {
                "improvedLayout": true,
                "hierarchical": {
                  "enabled": false
                }
              },
              "nodes": {
                "font": {
                  "size": 10,
                  "face": "Microsoft YaHei"
                },
                "scaling": {
                  "min": 10,
                  "max": 30
                }
              },
              "edges": {
                "arrows": {
                  "to": {
                    "enabled": true,
                    "scaleFactor": 0.5
                  }
                },
                "smooth": {
                  "enabled": true,
                  "type": "continuous",
                  "roundness": 0.5
                },
                "width": 1
              }
            }
            """
            else:
                # 中小型图：使用 forceAtlas2Based
                physics_config = """
            {
              "physics": {
                "enabled": true,
                "solver": "forceAtlas2Based",
                "forceAtlas2Based": {
                  "gravitationalConstant": -50,
                  "centralGravity": 0.01,
                  "springLength": 200,
                  "springConstant": 0.08,
                  "damping": 0.4,
                  "avoidOverlap": 0.5
                },
                "maxVelocity": 50,
                "minVelocity": 0.1,
                "stabilization": {
                  "enabled": true,
                  "iterations": 1000,
                  "updateInterval": 25,
                  "fit": true
                }
              },
              "interaction": {
                "hover": true,
                "tooltipDelay": 100,
                "zoomView": true,
                "dragView": true
              },
              "nodes": {
                "font": {
                  "size": 12,
                  "face": "Microsoft YaHei"
                }
              },
              "edges": {
                "arrows": {
                  "to": {
                    "enabled": true,
                    "scaleFactor": 0.5
                  }
                },
                "smooth": {
                  "enabled": true,
                  "type": "dynamic"
                }
              }
            }
            """
            net.set_options(physics_config)
        else:
            # 禁用物理引擎时，使用分层布局
            net.set_options("""
            {
              "physics": {
                "enabled": false
              },
              "layout": {
                "improvedLayout": true,
                "hierarchical": {
                  "enabled": true,
                  "direction": "UD",
                  "sortMethod": "directed"
                }
              },
              "interaction": {
                "hover": true,
                "zoomView": true,
                "dragView": true
              }
            }
            """)
        
        # 节点过滤已经在上面完成了，这里打印信息
        if focus_node:
            print(f"✓ 聚焦节点: {focus_node}")
        if filter_schemas:
            print(f"✓ 按 schema 过滤: {filter_schemas}")
        if filter_pattern:
            print(f"✓ 按模式过滤: {filter_pattern}")
        print(f"  最终包含节点数: {len(nodes_to_include)}")
        
        # 添加节点
        for node in self.nodes:
            if node['id'] not in nodes_to_include:
                continue
            
            node_id = node['id']
            schema = node.get('schema', 'unknown')
            table = node.get('table', '')
            
            # 构建节点标签和悬停提示
            label = table
            title = f"<b>{schema}.{table}</b><br>ID: {node_id}"
            
            # 获取颜色
            color = self.get_schema_color(schema)
            
            # 如果是聚焦节点，使用特殊样式
            if node_id == focus_node:
                net.add_node(
                    node_id,
                    label=label,
                    title=title,
                    color={'background': '#FFD700', 'border': '#FF6347'},
                    borderWidth=3,
                    size=30,
                    font={'size': 14, 'face': 'Microsoft YaHei', 'bold': True}
                )
            else:
                net.add_node(
                    node_id,
                    label=label,
                    title=title,
                    color=color,
                    size=20
                )
        
        # 添加边
        edge_count = 0
        for link in self.links:
            source = link['source']
            target = link['target']
            
            if source not in nodes_to_include or target not in nodes_to_include:
                continue
            
            # 构建边的悬停提示
            title = ""
            if 'script_paths' in link and link['script_paths']:
                script_path = link['script_paths'][0]
                script_name = Path(script_path).name
                title = f"脚本: {script_name}"
            
            net.add_edge(source, target, title=title)
            edge_count += 1
        
        print(f"✓ 图生成完成: {len(nodes_to_include)} 个节点, {edge_count} 条边")
        
        # 如果节点数量很多，提示用户
        if len(nodes_to_include) > 500:
            print(f"  ⚠️  节点数量较多（{len(nodes_to_include)} 个），渲染可能需要一些时间")
            print(f"     如果显示有问题，可以尝试使用 --no-physics 参数")
        
        # 显示控制按钮（如果可用）
        if show_buttons:
            try:
                # 尝试使用新版本 API
                net.show_buttons(filter_=['physics', 'interaction', 'manipulation'])
            except (AttributeError, TypeError):
                # 如果 API 不可用，尝试其他方式或不显示按钮
                try:
                    # 尝试旧版本 API
                    net.show_buttons()
                except (AttributeError, TypeError):
                    # 如果都不可用，跳过（不影响主要功能）
                    print("  ⚠️  控制按钮功能不可用，但图仍可正常使用")
        
        # 保存为 HTML
        net.save_graph(output_file)
        print(f"✓ 文件已保存: {output_file}")
        print(f"  请在浏览器中打开该文件查看交互式可视化")
        
        return output_file


def main():
    if not PYVIS_AVAILABLE:
        print("错误: pyvis 未安装")
        print("请运行: pip install pyvis")
        return
    
    parser = argparse.ArgumentParser(
        description='交互式数据血缘可视化工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 基础用法：生成交互式血缘图
  python lineage_visualizer_interactive.py ../datalineage.json
  
  # 聚焦某个表及其上下游
  python lineage_visualizer_interactive.py ../datalineage.json \\
    --focus "MDB_AL.AL88_CORE_LIAB_TRL_CLC_AGT_SUM" \\
    --upstream 2 --downstream 2
  
  # 只显示特定 schema
  python lineage_visualizer_interactive.py ../datalineage.json \\
    --schemas MDB_AL CDBVIEW
  
  # 禁用物理引擎（大图时更快）
  python lineage_visualizer_interactive.py ../datalineage.json --no-physics
        """
    )
    
    parser.add_argument('json_file',
                       help='networkx JSON 格式的数据血缘文件')
    parser.add_argument('-o', '--output',
                       default='lineage_interactive.html',
                       help='输出 HTML 文件名（默认: lineage_interactive.html）')
    parser.add_argument('--schemas',
                       nargs='+',
                       help='只显示指定 schema 的表')
    parser.add_argument('--pattern',
                       help='表名过滤模式（支持 * 和 ? 通配符）')
    parser.add_argument('--focus',
                       help='聚焦节点 ID')
    parser.add_argument('--upstream',
                       type=int,
                       default=-1,
                       help='上游深度（-1 表示无限）')
    parser.add_argument('--downstream',
                       type=int,
                       default=-1,
                       help='下游深度（-1 表示无限）')
    parser.add_argument('--height',
                       default='900px',
                       help='图的高度（默认: 900px）')
    parser.add_argument('--width',
                       default='100%',
                       help='图的宽度（默认: 100%）')
    parser.add_argument('--no-physics',
                       action='store_true',
                       help='禁用物理引擎（静态布局）')
    parser.add_argument('--no-buttons',
                       action='store_true',
                       help='不显示控制按钮')
    
    args = parser.parse_args()
    
    # 创建可视化工具实例
    visualizer = InteractiveLineageVisualizer(args.json_file)
    
    # 生成交互式图
    visualizer.create_interactive_graph(
        output_file=args.output,
        filter_schemas=args.schemas,
        filter_pattern=args.pattern,
        focus_node=args.focus,
        upstream_depth=args.upstream,
        downstream_depth=args.downstream,
        height=args.height,
        width=args.width,
        physics_enabled=not args.no_physics,
        show_buttons=not args.no_buttons
    )


if __name__ == '__main__':
    main()

