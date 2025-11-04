#!/usr/bin/env python
"""
交互式数据血缘可视化工具 - 命令行入口
从项目根目录运行: python lineage_viz_interactive.py [参数]
"""

import sys
from pathlib import Path

# 添加 lineage_visualizer 目录到路径
sys.path.insert(0, str(Path(__file__).parent))

from lineage_visualizer.lineage_visualizer_interactive import main

if __name__ == '__main__':
    main()

