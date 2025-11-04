"""
数据血缘可视化工具使用示例
演示各种常见的使用场景
"""

from .lineage_visualizer import LineageVisualizer
from pathlib import Path


def example_1_full_lineage():
    """示例1: 生成完整的数据血缘图"""
    print("\n" + "="*60)
    print("示例 1: 生成完整的数据血缘图")
    print("="*60)
    
    viz = LineageVisualizer('../datalineage.json')
    viz.create_graph(
        output_file='output/example1_full_lineage',
        format='svg',
        layout='dot',
        rankdir='LR'
    )


def example_2_focus_node():
    """示例2: 聚焦某个节点，显示其上下游"""
    print("\n" + "="*60)
    print("示例 2: 聚焦节点及其上下游关系")
    print("="*60)
    
    viz = LineageVisualizer('../datalineage.json')
    
    # 选择一个节点进行聚焦
    focus_table = "MDB_AL.AL88_CORE_LIAB_TRL_CLC_AGT_SUM"
    
    viz.create_graph(
        output_file='output/example2_focused',
        format='png',
        focus_node=focus_table,
        upstream_depth=2,
        downstream_depth=2,
        edge_labels=True
    )


def example_3_schema_filter():
    """示例3: 按 Schema 过滤"""
    print("\n" + "="*60)
    print("示例 3: 只显示特定 Schema 的表")
    print("="*60)
    
    viz = LineageVisualizer('../datalineage.json')
    viz.create_graph(
        output_file='output/example3_schema_filter',
        format='svg',
        filter_schemas=['MDB_AL', 'CDBVIEW']
    )


def example_4_pattern_filter():
    """示例4: 按表名模式过滤"""
    print("\n" + "="*60)
    print("示例 4: 查找包含特定关键字的表")
    print("="*60)
    
    viz = LineageVisualizer('../datalineage.json')
    viz.create_graph(
        output_file='output/example4_pattern_filter',
        format='png',
        filter_pattern='*LOAN*'
    )


def example_5_upstream_only():
    """示例5: 只显示上游数据来源"""
    print("\n" + "="*60)
    print("示例 5: 追溯数据来源（只显示上游）")
    print("="*60)
    
    viz = LineageVisualizer('../datalineage.json')
    
    # 找一个有上游的节点
    stats = viz.get_statistics()
    if stats['in_degree']:
        # 选择入度最高的节点之一
        focus_table = list(stats['in_degree'].keys())[0]
        
        viz.create_graph(
            output_file='output/example5_upstream_only',
            format='svg',
            focus_node=focus_table,
            upstream_depth=3,
            downstream_depth=0,
            rankdir='RL'  # 右到左，更符合追溯的感觉
        )


def example_6_downstream_only():
    """示例6: 只显示下游影响"""
    print("\n" + "="*60)
    print("示例 6: 分析影响范围（只显示下游）")
    print("="*60)
    
    viz = LineageVisualizer('../datalineage.json')
    
    # 找一个有下游的节点
    stats = viz.get_statistics()
    if stats['out_degree']:
        # 选择出度最高的节点之一
        focus_table = list(stats['out_degree'].keys())[0]
        
        viz.create_graph(
            output_file='output/example6_downstream_only',
            format='svg',
            focus_node=focus_table,
            upstream_depth=0,
            downstream_depth=3
        )


def example_7_different_layouts():
    """示例7: 尝试不同的布局引擎"""
    print("\n" + "="*60)
    print("示例 7: 使用不同的布局引擎")
    print("="*60)
    
    viz = LineageVisualizer('../datalineage.json')
    
    # 获取一个子图用于演示
    stats = viz.get_statistics()
    if stats['in_degree']:
        focus_table = list(stats['in_degree'].keys())[0]
        
        layouts = ['dot', 'neato', 'fdp', 'circo']
        
        for layout in layouts:
            print(f"\n生成 {layout} 布局...")
            viz.create_graph(
                output_file=f'output/example7_layout_{layout}',
                format='png',
                layout=layout,
                focus_node=focus_table,
                upstream_depth=1,
                downstream_depth=1
            )


def example_8_statistics():
    """示例8: 查看统计信息"""
    print("\n" + "="*60)
    print("示例 8: 数据血缘统计分析")
    print("="*60)
    
    viz = LineageVisualizer('../datalineage.json')
    viz.print_statistics()
    
    # 获取统计数据进行进一步分析
    stats = viz.get_statistics()
    
    # 找出孤立节点（既无上游也无下游）
    isolated_nodes = []
    for node in viz.nodes:
        node_id = node['id']
        if (node_id not in stats['in_degree'] and 
            node_id not in stats['out_degree']):
            isolated_nodes.append(node_id)
    
    if isolated_nodes:
        print(f"\n孤立节点（无上下游关系）: {len(isolated_nodes)} 个")
        for node_id in isolated_nodes[:5]:  # 只显示前5个
            print(f"  - {node_id}")
        if len(isolated_nodes) > 5:
            print(f"  ... 还有 {len(isolated_nodes) - 5} 个")


def example_9_combined_filters():
    """示例9: 组合多个过滤条件"""
    print("\n" + "="*60)
    print("示例 9: 组合使用多个过滤条件")
    print("="*60)
    
    viz = LineageVisualizer('../datalineage.json')
    viz.create_graph(
        output_file='output/example9_combined',
        format='svg',
        filter_schemas=['MDB_AL', 'CDBVIEW'],
        filter_pattern='*SUM*',
        edge_labels=True
    )


def example_10_high_quality_export():
    """示例10: 生成高质量的文档图片"""
    print("\n" + "="*60)
    print("示例 10: 导出高质量图片用于文档")
    print("="*60)
    
    viz = LineageVisualizer('../datalineage.json')
    
    stats = viz.get_statistics()
    if stats['in_degree']:
        focus_table = list(stats['in_degree'].keys())[0]
        
        # 生成多种格式
        for fmt in ['pdf', 'svg', 'png']:
            print(f"\n导出 {fmt.upper()} 格式...")
            viz.create_graph(
                output_file=f'output/example10_documentation',
                format=fmt,
                focus_node=focus_table,
                upstream_depth=2,
                downstream_depth=2,
                edge_labels=True,
                rankdir='TB'
            )


def run_all_examples():
    """运行所有示例"""
    # 创建输出目录
    Path('output').mkdir(exist_ok=True)
    
    examples = [
        example_1_full_lineage,
        example_2_focus_node,
        example_3_schema_filter,
        example_4_pattern_filter,
        example_5_upstream_only,
        example_6_downstream_only,
        example_7_different_layouts,
        example_8_statistics,
        example_9_combined_filters,
        example_10_high_quality_export,
    ]
    
    print("\n" + "🚀 "*30)
    print("数据血缘可视化工具 - 示例集合")
    print("🚀 "*30)
    
    for i, example_func in enumerate(examples, 1):
        try:
            example_func()
        except Exception as e:
            print(f"\n❌ 示例 {i} 执行失败: {e}")
            continue
    
    print("\n" + "✅ "*30)
    print("所有示例执行完成！")
    print("生成的文件保存在 output/ 目录")
    print("✅ "*30 + "\n")


def run_quick_demo():
    """运行快速演示（只运行几个关键示例）"""
    Path('output').mkdir(exist_ok=True)
    
    print("\n" + "🚀 "*30)
    print("数据血缘可视化工具 - 快速演示")
    print("🚀 "*30)
    
    # 只运行统计和一个可视化示例
    example_8_statistics()
    example_2_focus_node()
    
    print("\n" + "✅ "*30)
    print("快速演示完成！")
    print("✅ "*30 + "\n")


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--quick':
            run_quick_demo()
        elif sys.argv[1] == '--all':
            run_all_examples()
        else:
            print("用法:")
            print("  python examples_lineage_viz.py           # 运行快速演示")
            print("  python examples_lineage_viz.py --quick   # 运行快速演示")
            print("  python examples_lineage_viz.py --all     # 运行所有示例")
    else:
        run_quick_demo()

