# 数据血缘可视化工具

<div align="center">

**基于 Graphviz 和 Pyvis 的强大数据血缘关系可视化工具**

支持静态图生成 | 交互式 HTML | 多种过滤选项 | 血缘追溯

</div>

---

## 📖 项目简介

这是一个专为数据工程师和分析师设计的数据血缘可视化工具，能够将存储在 NetworkX JSON 格式中的数据血缘关系转换为直观、美观的可视化图表。

### 支持两种可视化方式

1. **Graphviz 静态图** - 适合文档和报告
   - 输出格式：SVG, PNG, PDF, JPG
   - 布局精美，适合打印和演示
   
2. **Pyvis 交互式图** - 适合探索和分析
   - 输出格式：HTML
   - 支持拖拽、缩放、搜索节点
   - 适合大型复杂图的探索

---

## ⚡ 快速入门

### 1. 安装依赖

**安装 Graphviz（必需）**

Windows:
```bash
choco install graphviz
```

Linux:
```bash
sudo apt-get install graphviz
```

macOS:
```bash
brew install graphviz
```

**安装 Python 包**

```bash
pip install -r lineage_visualizer/requirements.txt
```

或者单独安装：
```bash
# 基础版本（Graphviz 静态图）
pip install graphviz

# 交互式版本（推荐，支持拖拽、缩放）
pip install graphviz pyvis
```

### 2. 快速生成可视化

**方式 1：使用命令行入口（推荐）**

从**项目根目录**运行：

```bash
# 生成静态图
python lineage_viz.py datalineage.json -f png

# 生成交互式 HTML（推荐）
python lineage_viz_interactive.py datalineage.json

# 查看统计信息
python lineage_viz.py datalineage.json --stats-only
```

**方式 2：使用快速启动脚本**

Windows:
```cmd
cd lineage_visualizer
quick_start.bat
```

Linux/macOS:
```bash
cd lineage_visualizer
chmod +x quick_start.sh
./quick_start.sh
```

**方式 3：直接使用模块**

```bash
# 从项目根目录
python -m lineage_visualizer.lineage_visualizer datalineage.json --stats-only
python -m lineage_visualizer.lineage_visualizer_interactive datalineage.json
```

---

## ✨ 功能特性

### 🎨 可视化功能
- ✅ 两种可视化方式：静态图（Graphviz）和交互式图（Pyvis）
- ✅ 多种输出格式：SVG, PNG, PDF, JPG, HTML
- ✅ 自动按 Schema 分组和着色
- ✅ 聚焦节点高亮显示
- ✅ 支持显示 SQL 脚本文件名

### 🔍 过滤功能
- ✅ 按 Schema 过滤
- ✅ 按表名模式过滤（支持通配符）
- ✅ 聚焦模式（显示指定节点的上下游）
- ✅ 可配置的上下游深度

### 📊 分析功能
- ✅ 数据血缘统计信息
- ✅ 入度/出度分析（找出关键节点）
- ✅ 上游追溯
- ✅ 下游影响分析

### ⚙️ 布局选项
- ✅ 6 种布局引擎（dot, neato, fdp, sfdp, circo, twopi）
- ✅ 4 种图方向（左到右、上到下、右到左、下到上）
- ✅ 可自定义节点样式和颜色

---

## 📁 文件结构

```
metadata_exctractor/
├── lineage_visualizer/          # 可视化工具包
│   ├── __init__.py              # 包初始化
│   ├── lineage_visualizer.py    # 主程序（Graphviz版本）
│   ├── lineage_visualizer_interactive.py  # 交互式版本
│   ├── examples_lineage_viz.py  # 使用示例
│   ├── test_visualizer.py       # 测试脚本
│   ├── requirements.txt         # 依赖列表
│   ├── README.md                # 本文档
│   ├── quick_start.bat          # Windows快速启动
│   ├── quick_start.sh           # Linux/macOS快速启动
│   └── generate_examples.bat    # 批量生成示例
├── lineage_viz.py              # 命令行入口（从根目录运行）
├── lineage_viz_interactive.py  # 交互式命令行入口
└── datalineage.json            # 数据血缘文件（在项目根目录）
```

---

## 🎯 使用示例

### 示例 1：查看完整血缘图

```bash
# 生成交互式 HTML（可拖拽、缩放）
python lineage_viz_interactive.py datalineage.json

# 生成静态 SVG（适合文档）
python lineage_viz.py datalineage.json -f svg
```

### 示例 2：聚焦某个表

```bash
# 查看某个表的上下游（各2层）
python lineage_viz.py datalineage.json \
  --focus "MDB_AL.AL88_CORE_LIAB_TRL_CLC_AGT_SUM" \
  --upstream 2 \
  --downstream 2 \
  -f png
```

### 示例 3：追溯数据来源

```bash
# 只显示上游 3 层（追溯数据来源）
python lineage_viz.py datalineage.json \
  --focus "MDB_AL.AL88_CORE_LIAB_TRL_CLC_AGT_SUM" \
  --upstream 3 \
  --downstream 0 \
  -o data_source \
  -f png
```

### 示例 4：分析影响范围

```bash
# 只显示下游 3 层（影响分析）
python lineage_viz.py datalineage.json \
  --focus "CDBVIEW.T88_CORE_LIAB_TRL_CLC_AGT_SUM" \
  --upstream 0 \
  --downstream 3 \
  -o impact_analysis \
  -f png
```

### 示例 5：按业务域查看

```bash
# 只看特定 Schema 的表
python lineage_viz.py datalineage.json \
  --schemas MDB_AL CDBVIEW \
  -o business_domain \
  -f svg
```

### 示例 6：搜索特定表

```bash
# 查找所有贷款相关的表
python lineage_viz.py datalineage.json \
  --pattern "*LOAN*" \
  -o loan_lineage \
  -f png
```

### 示例 7：查看统计信息

```bash
# 显示统计信息（找出关键节点）
python lineage_viz.py datalineage.json --stats-only
```

输出示例：
```
============================================================
数据血缘统计信息
============================================================
总节点数: 1500
总边数: 3200

Schema 分布:
  CDBVIEW: 600 个表
  ODBVIEW: 450 个表
  MDB_AL: 300 个表

入度最高的节点（被依赖最多）:
  CDBVIEW.T88_CORE_LIAB_TRL_CLC_AGT_SUM: 25
  ODBVIEW.MAN_CALN: 20
  ...

出度最高的节点（依赖其他表最多）:
  MDB_AL.AL88_ASS_SL_P_COR_LB_M_TRC_SUM: 15
  ...
============================================================
```

---

## 🛠️ 命令行参数详解

### 基础参数

| 参数 | 简写 | 说明 | 默认值 |
|------|------|------|--------|
| `json_file` | - | networkx JSON 格式的数据血缘文件（必需） | - |
| `--output` | `-o` | 输出文件名（不含扩展名） | `lineage` |
| `--format` | `-f` | 输出格式（svg/png/pdf/jpg） | `svg` |
| `--layout` | `-l` | 布局引擎（dot/neato/fdp/sfdp/circo/twopi） | `dot` |

### 过滤参数

| 参数 | 说明 |
|------|------|
| `--schemas` | 只显示指定 schema 的表 |
| `--pattern` | 表名过滤模式（支持 * 和 ? 通配符） |
| `--focus` | 聚焦节点 ID（显示其上下游） |
| `--upstream` | 上游深度（-1 表示无限） |
| `--downstream` | 下游深度（-1 表示无限） |

### 样式参数

| 参数 | 说明 |
|------|------|
| `--rankdir` | 图的方向（LR:左到右, TB:上到下, RL:右到左, BT:下到上） |
| `--edge-labels` | 显示边标签（脚本文件名） |
| `--no-schema-labels` | 不在节点中显示 schema |
| `--node-style` | 节点样式（rounded/box/ellipse 等） |

### 其他参数

| 参数 | 说明 |
|------|------|
| `--stats-only` | 只显示统计信息，不生成图 |
| `-h, --help` | 显示帮助信息 |

### 交互式版本特有参数

| 参数 | 说明 |
|------|------|
| `--height` | 图的高度（默认: 900px） |
| `--width` | 图的宽度（默认: 100%） |
| `--no-physics` | 禁用物理引擎（静态布局，适合大图） |
| `--no-buttons` | 不显示控制按钮 |

---

## 📊 布局引擎选择

| 引擎 | 适用场景 | 特点 |
|------|---------|------|
| `dot` | **数据血缘图（推荐）** | 层次化布局，适合有向图 |
| `neato` | 小型图 | 力导向布局，较美观 |
| `fdp` | 大型图 | 基于力的布局 |
| `sfdp` | 超大型图 | 多尺度布局 |
| `circo` | 环形关系 | 圆形布局 |
| `twopi` | 中心辐射 | 放射状布局 |

---

## 🎨 颜色方案

| Schema | 颜色 |
|--------|------|
| MDB_AL | 🔴 浅红色 (#FFE6E6) |
| CDBVIEW | 🔵 浅蓝色 (#E6F3FF) |
| ODBVIEW | 🟢 浅绿色 (#E6FFE6) |
| PDBVIEW | 🟡 浅黄色 (#FFF9E6) |
| 聚焦节点 | 🟡 金色 (#FFD700) + 🔴 红色边框 |

---

## 🐍 Python API 使用

```python
from lineage_visualizer import LineageVisualizer

# 加载数据
viz = LineageVisualizer('datalineage.json')

# 查看统计
viz.print_statistics()

# 生成完整图
viz.create_graph(output_file='full', format='svg')

# 生成聚焦图
viz.create_graph(
    output_file='focused',
    format='png',
    focus_node='MDB_AL.AL88_CORE_LIAB_TRL_CLC_AGT_SUM',
    upstream_depth=2,
    downstream_depth=2,
    edge_labels=True
)

# 获取某个表的所有上游表
upstream = viz.get_upstream_nodes('MDB_AL.AL88_CORE_LIAB_TRL_CLC_AGT_SUM')
print(f"上游表数量: {len(upstream)}")
for table in list(upstream)[:5]:
    print(f"  - {table}")

# 获取统计数据
stats = viz.get_statistics()
print(f"总节点数: {stats['total_nodes']}")
print(f"总边数: {stats['total_edges']}")
```

### 交互式可视化 API

```python
from lineage_visualizer.lineage_visualizer_interactive import InteractiveLineageVisualizer

# 创建交互式可视化
viz = InteractiveLineageVisualizer('datalineage.json')

# 生成交互式 HTML
viz.create_interactive_graph(
    output_file='interactive.html',
    focus_node='MDB_AL.AL88_CORE_LIAB_TRL_CLC_AGT_SUM',
    upstream_depth=2,
    downstream_depth=2,
    physics_enabled=True
)
```

---

## 🎯 常见使用场景

### 场景 1: 追溯数据来源

```bash
# 查看某个表的数据从哪来（只看上游 3 层）
python lineage_viz.py datalineage.json \
  --focus "MDB_AL.AL88_CORE_LIAB_TRL_CLC_AGT_SUM" \
  --upstream 3 \
  --downstream 0 \
  -o upstream_trace \
  -f png
```

### 场景 2: 分析影响范围

```bash
# 如果修改某个表，会影响哪些下游表？（只看下游 3 层）
python lineage_viz.py datalineage.json \
  --focus "CDBVIEW.T88_CORE_LIAB_TRL_CLC_AGT_SUM" \
  --upstream 0 \
  --downstream 3 \
  -o downstream_impact \
  -f png
```

### 场景 3: 查看特定业务域

```bash
# 只看 MDB_AL schema 的表之间的关系
python lineage_viz.py datalineage.json \
  --schemas MDB_AL \
  -o mdb_lineage \
  -f svg
```

### 场景 4: 搜索相关表

```bash
# 找出所有包含 "LOAN" 的表及其关系
python lineage_viz.py datalineage.json \
  --pattern "*LOAN*" \
  -o loan_tables \
  -f png
```

---

## 🎨 高级技巧

### 技巧 1: 改变图的方向

```bash
# 从上到下（适合层次结构）
python lineage_viz.py datalineage.json --rankdir TB

# 从左到右（默认，适合宽屏）
python lineage_viz.py datalineage.json --rankdir LR
```

### 技巧 2: 显示脚本文件信息

```bash
# 在边上显示 SQL 脚本文件名
python lineage_viz.py datalineage.json --edge-labels
```

### 技巧 3: 尝试不同布局

```bash
# 对于复杂的图，尝试不同的布局引擎
python lineage_viz.py datalineage.json --layout neato  # 力导向
python lineage_viz.py datalineage.json --layout fdp    # 适合大图
python lineage_viz.py datalineage.json --layout circo  # 圆形布局
```

### 技巧 4: 大型图优化

对于包含大量节点的图（>500个节点），交互式版本会自动优化：
- 使用更快的布局算法（barnesHut）
- 减少稳定化迭代次数
- 如果仍然很慢，可以使用 `--no-physics` 禁用物理引擎

---

## 📝 数据格式说明

输入的 JSON 文件应遵循 NetworkX 的 node-link 格式：

```json
{
  "directed": true,
  "multigraph": false,
  "nodes": [
    {
      "schema": "MDB_AL",
      "table": "TABLE_NAME",
      "id": "MDB_AL.TABLE_NAME"
    }
  ],
  "links": [
    {
      "source": "SCHEMA1.TABLE1",
      "target": "SCHEMA2.TABLE2",
      "script_paths": ["path/to/script.sql"]
    }
  ]
}
```

**必需字段**：
- `nodes`: 节点数组，每个节点需包含 `schema`, `table`, `id`
- `links`: 边数组，每条边需包含 `source`, `target`

**可选字段**：
- `script_paths`: SQL 脚本路径数组（用于显示在边上）

---

## ❓ 常见问题

### Q: 图太大，看不清怎么办？

A: 三个办法：
1. 使用 `--focus` 聚焦到关心的节点
2. 使用 `--schemas` 或 `--pattern` 过滤
3. 使用交互式 HTML 版本，可以缩放和拖拽

### Q: 如何找到关键的表？

A: 运行 `python lineage_viz.py datalineage.json --stats-only`，查看入度和出度最高的节点。

### Q: Graphviz 安装后还是报错？

A: 确保 Graphviz 的 bin 目录已添加到系统 PATH 环境变量。

### Q: 生成的图布局不美观？

A: 尝试不同的布局引擎（`--layout neato`）和方向（`--rankdir TB`）。

### Q: 交互式 HTML 看不到节点？

A: 
1. 对于大型图（>500节点），等待物理引擎稳定可能需要一些时间
2. 尝试使用 `--no-physics` 参数禁用物理引擎
3. 在浏览器中尝试缩放（鼠标滚轮或 Ctrl+滚轮）
4. 检查浏览器控制台是否有 JavaScript 错误

### Q: 支持哪些数据格式？

A: 目前支持 NetworkX JSON node-link 格式。节点需包含 `schema`, `table`, `id` 字段，边需包含 `source`, `target` 字段。

---

## 🧪 测试

运行测试脚本验证工具是否正常工作：

```bash
# 从项目根目录运行
python lineage_visualizer/test_visualizer.py
```

测试脚本会检查：
- 依赖是否正确安装
- 数据是否能正常加载
- 核心功能是否正常
- 图生成是否成功

---

## 💡 推荐工作流

1. **了解全貌**
   ```bash
   python lineage_viz.py datalineage.json --stats-only
   ```

2. **探索数据**
   ```bash
   python lineage_viz_interactive.py datalineage.json
   ```

3. **聚焦分析**
   ```bash
   python lineage_viz.py datalineage.json --focus "表名" --upstream 2 --downstream 2
   ```

4. **导出文档**
   ```bash
   python lineage_viz.py datalineage.json --focus "表名" -f pdf
   ```

---

## 📦 输出文件

所有生成的可视化文件默认保存在：
- 命令行直接运行：当前目录
- 使用快速启动脚本：`lineage_visualizer/output/` 目录
- 示例脚本：`lineage_visualizer/output/` 或 `lineage_visualizer/examples/` 目录

---

## 🤝 贡献

欢迎提交问题和改进建议！

---

## 📄 许可证

MIT License

---

<div align="center">

Made with ❤️ for Data Engineers

</div>
