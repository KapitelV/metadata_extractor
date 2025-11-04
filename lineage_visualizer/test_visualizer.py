"""
测试数据血缘可视化工具
快速验证工具是否正常工作
"""

import sys
from pathlib import Path


def test_basic_import():
    """测试基础导入"""
    print("测试 1: 基础导入...")
    try:
        # 支持从项目根目录或 lineage_visualizer 目录运行
        try:
            from .lineage_visualizer import LineageVisualizer
        except ImportError:
            from lineage_visualizer import LineageVisualizer
        print("  ✓ lineage_visualizer 导入成功")
        return True
    except Exception as e:
        print(f"  ✗ 导入失败: {e}")
        return False


def test_load_data():
    """测试数据加载"""
    print("\n测试 2: 数据加载...")
    try:
        # 支持从项目根目录或 lineage_visualizer 目录运行
        try:
            from .lineage_visualizer import LineageVisualizer
        except ImportError:
            from lineage_visualizer import LineageVisualizer
        
        # 尝试多个可能的路径
        json_file = None
        for path in ['../datalineage.json', 'datalineage.json']:
            if Path(path).exists():
                json_file = path
                break
        
        if not json_file or not Path(json_file).exists():
            print("  ⚠️  datalineage.json 文件不存在，跳过此测试")
            return True
        
        viz = LineageVisualizer(json_file)
        print(f"  ✓ 数据加载成功: {len(viz.nodes)} 个节点, {len(viz.links)} 条边")
        return True
    except Exception as e:
        print(f"  ✗ 数据加载失败: {e}")
        return False


def test_statistics():
    """测试统计功能"""
    print("\n测试 3: 统计功能...")
    try:
        try:
            from .lineage_visualizer import LineageVisualizer
        except ImportError:
            from lineage_visualizer import LineageVisualizer
        
        json_file = None
        for path in ['../datalineage.json', 'datalineage.json']:
            if Path(path).exists():
                json_file = path
                break
        
        if not json_file or not Path(json_file).exists():
            print("  ⚠️  datalineage.json 文件不存在，跳过此测试")
            return True
        
        viz = LineageVisualizer(json_file)
        stats = viz.get_statistics()
        
        print(f"  ✓ 统计信息获取成功")
        print(f"    - 总节点数: {stats['total_nodes']}")
        print(f"    - 总边数: {stats['total_edges']}")
        print(f"    - Schema 数量: {len(stats['schemas'])}")
        return True
    except Exception as e:
        print(f"  ✗ 统计失败: {e}")
        return False


def test_filter_functions():
    """测试过滤功能"""
    print("\n测试 4: 过滤功能...")
    try:
        try:
            from .lineage_visualizer import LineageVisualizer
        except ImportError:
            from lineage_visualizer import LineageVisualizer
        
        json_file = None
        for path in ['../datalineage.json', 'datalineage.json']:
            if Path(path).exists():
                json_file = path
                break
        
        if not json_file or not Path(json_file).exists():
            print("  ⚠️  datalineage.json 文件不存在，跳过此测试")
            return True
        
        viz = LineageVisualizer(json_file)
        
        # 测试 schema 过滤
        if viz.nodes:
            first_schema = viz.nodes[0].get('schema')
            filtered = viz.filter_nodes_by_schema([first_schema])
            print(f"  ✓ Schema 过滤: {len(filtered)} 个节点匹配 '{first_schema}'")
        
        # 测试模式过滤
        filtered = viz.filter_nodes_by_pattern('*')
        print(f"  ✓ 模式过滤: {len(filtered)} 个节点匹配 '*'")
        
        return True
    except Exception as e:
        print(f"  ✗ 过滤测试失败: {e}")
        return False


def test_graph_traversal():
    """测试图遍历功能"""
    print("\n测试 5: 图遍历功能...")
    try:
        try:
            from .lineage_visualizer import LineageVisualizer
        except ImportError:
            from lineage_visualizer import LineageVisualizer
        
        json_file = None
        for path in ['../datalineage.json', 'datalineage.json']:
            if Path(path).exists():
                json_file = path
                break
        
        if not json_file or not Path(json_file).exists():
            print("  ⚠️  datalineage.json 文件不存在，跳过此测试")
            return True
        
        viz = LineageVisualizer(json_file)
        
        if viz.nodes:
            test_node = viz.nodes[0]['id']
            
            # 测试上游查询
            upstream = viz.get_upstream_nodes(test_node, max_depth=2)
            print(f"  ✓ 上游查询: 节点 '{test_node}' 有 {len(upstream)} 个上游节点（2层）")
            
            # 测试下游查询
            downstream = viz.get_downstream_nodes(test_node, max_depth=2)
            print(f"  ✓ 下游查询: 节点 '{test_node}' 有 {len(downstream)} 个下游节点（2层）")
        
        return True
    except Exception as e:
        print(f"  ✗ 图遍历测试失败: {e}")
        return False


def test_graphviz():
    """测试 Graphviz 是否可用"""
    print("\n测试 6: Graphviz 可用性...")
    try:
        import graphviz
        
        # 尝试创建一个简单的图
        dot = graphviz.Digraph()
        dot.node('A', 'Node A')
        dot.node('B', 'Node B')
        dot.edge('A', 'B')
        
        print("  ✓ Graphviz Python 包可用")
        
        # 尝试渲染（但不保存文件）
        try:
            dot.render('test_graph', format='svg', cleanup=True)
            Path('test_graph.svg').unlink()  # 删除测试文件
            print("  ✓ Graphviz 可执行文件可用")
        except Exception as e:
            print(f"  ⚠️  Graphviz 可执行文件可能未正确安装: {e}")
            print("     请确保 Graphviz 已安装并添加到 PATH")
        
        return True
    except ImportError:
        print("  ✗ Graphviz Python 包未安装")
        print("     请运行: pip install graphviz")
        return False
    except Exception as e:
        print(f"  ✗ Graphviz 测试失败: {e}")
        return False


def test_pyvis():
    """测试 Pyvis 是否可用（可选）"""
    print("\n测试 7: Pyvis 可用性（可选）...")
    try:
        import pyvis
        print("  ✓ Pyvis 已安装，可以使用交互式可视化")
        return True
    except ImportError:
        print("  ⚠️  Pyvis 未安装（可选）")
        print("     如需交互式可视化，请运行: pip install pyvis")
        return True  # 不算作失败


def test_graph_generation():
    """测试图生成功能"""
    print("\n测试 8: 图生成功能...")
    try:
        try:
            from .lineage_visualizer import LineageVisualizer
        except ImportError:
            from lineage_visualizer import LineageVisualizer
        
        json_file = None
        for path in ['../datalineage.json', 'datalineage.json']:
            if Path(path).exists():
                json_file = path
                break
        
        if not json_file or not Path(json_file).exists():
            print("  ⚠️  datalineage.json 文件不存在，跳过此测试")
            return True
        
        # 创建测试输出目录
        test_output_dir = Path('test_output')
        test_output_dir.mkdir(exist_ok=True)
        
        viz = LineageVisualizer(json_file)
        
        # 找一个有上下游的节点
        test_node = None
        for node in viz.nodes[:10]:  # 只检查前10个
            node_id = node['id']
            upstream = viz.get_upstream_nodes(node_id, max_depth=1)
            downstream = viz.get_downstream_nodes(node_id, max_depth=1)
            if upstream or downstream:
                test_node = node_id
                break
        
        if not test_node:
            print("  ⚠️  找不到合适的测试节点，使用第一个节点")
            test_node = viz.nodes[0]['id']
        
        # 生成一个小的测试图
        output_file = test_output_dir / 'test_lineage'
        viz.create_graph(
            output_file=str(output_file),
            format='svg',
            focus_node=test_node,
            upstream_depth=1,
            downstream_depth=1
        )
        
        if Path(f"{output_file}.svg").exists():
            print(f"  ✓ 图生成成功: {output_file}.svg")
            # 清理测试文件
            Path(f"{output_file}.svg").unlink()
            return True
        else:
            print("  ✗ 图文件未生成")
            return False
        
    except Exception as e:
        print(f"  ✗ 图生成失败: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # 清理测试目录
        try:
            test_output_dir = Path('test_output')
            if test_output_dir.exists() and not any(test_output_dir.iterdir()):
                test_output_dir.rmdir()
        except:
            pass


def run_all_tests():
    """运行所有测试"""
    print("="*70)
    print("数据血缘可视化工具 - 功能测试")
    print("="*70)
    
    tests = [
        test_basic_import,
        test_load_data,
        test_statistics,
        test_filter_functions,
        test_graph_traversal,
        test_graphviz,
        test_pyvis,
        test_graph_generation,
    ]
    
    results = []
    for test in tests:
        result = test()
        results.append(result)
    
    print("\n" + "="*70)
    print("测试总结")
    print("="*70)
    
    passed = sum(results)
    total = len(results)
    
    print(f"通过: {passed}/{total}")
    
    if passed == total:
        print("\n✅ 所有测试通过！工具已就绪。")
        return 0
    else:
        print(f"\n⚠️  {total - passed} 个测试失败，请检查错误信息。")
        return 1


if __name__ == '__main__':
    sys.exit(run_all_tests())

