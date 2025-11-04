@echo off
REM Windows 批处理脚本 - 生成示例可视化
REM 根据常见场景生成多个示例图
REM 使用方法: 在 lineage_visualizer 目录下运行

echo ======================================================================
echo 生成示例可视化
echo ======================================================================
echo.

if not exist ..\datalineage.json (
    echo [错误] 找不到 datalineage.json 文件
    echo 请确保该文件在父目录下
    pause
    exit /b 1
)

REM 创建输出目录
if not exist examples mkdir examples

echo [示例 1] 生成完整血缘图 (SVG)...
cd ..
python lineage_viz.py datalineage.json -o lineage_visualizer\examples\full_lineage -f svg
cd lineage_visualizer

echo.
echo [示例 2] 生成交互式版本...
cd ..
python lineage_viz_interactive.py datalineage.json -o lineage_visualizer\examples\interactive_lineage.html
cd lineage_visualizer

echo.
echo [示例 3] 按 Schema 分别生成...
cd ..
python lineage_viz.py datalineage.json --schemas MDB_AL -o lineage_visualizer\examples\mdb_al -f png
python lineage_viz.py datalineage.json --schemas CDBVIEW -o lineage_visualizer\examples\cdbview -f png
python lineage_viz.py datalineage.json --schemas ODBVIEW -o lineage_visualizer\examples\odbview -f png
cd lineage_visualizer

echo.
echo [示例 4] 查找贷款相关表...
cd ..
python lineage_viz.py datalineage.json --pattern "*LOAN*" -o lineage_visualizer\examples\loan_tables -f png
cd lineage_visualizer

echo.
echo [示例 5] 查找汇总表...
cd ..
python lineage_viz.py datalineage.json --pattern "*SUM*" -o lineage_visualizer\examples\sum_tables -f png
cd lineage_visualizer

echo.
echo ======================================================================
echo 完成！所有示例保存在 examples/ 目录
echo ======================================================================
pause

