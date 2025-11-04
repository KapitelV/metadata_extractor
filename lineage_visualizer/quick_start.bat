@echo off
REM Windows 批处理脚本 - 数据血缘可视化快速入门
REM 使用方法: 在 lineage_visualizer 目录下双击运行或在命令行执行 quick_start.bat

echo ======================================================================
echo 数据血缘可视化工具 - 快速入门
echo ======================================================================
echo.

REM 检查 datalineage.json 是否存在（在父目录）
if not exist ..\datalineage.json (
    echo [错误] 找不到 datalineage.json 文件
    echo 请确保该文件在父目录下
    pause
    exit /b 1
)

REM 创建输出目录
if not exist output mkdir output

echo [1/4] 运行测试...
cd ..
python lineage_visualizer\test_visualizer.py
cd lineage_visualizer
if errorlevel 1 (
    echo.
    echo [警告] 某些测试未通过，但可以继续...
    echo.
)

echo.
echo [2/4] 查看统计信息...
cd ..
python lineage_viz.py ..\datalineage.json --stats-only
cd lineage_visualizer

echo.
echo [3/4] 生成交互式可视化（推荐）...
cd ..
python lineage_viz_interactive.py ..\datalineage.json -o lineage_visualizer\output\lineage_interactive.html
cd lineage_visualizer
if errorlevel 1 (
    echo [注意] 交互式版本生成失败，可能需要安装 pyvis
    echo 运行: pip install pyvis
    echo.
)

echo.
echo [4/4] 生成静态 SVG 图...
cd ..
python lineage_viz.py ..\datalineage.json -o lineage_visualizer\output\lineage_static -f svg
cd lineage_visualizer

echo.
echo ======================================================================
echo 完成！生成的文件在 output/ 目录：
echo   - lineage_interactive.html (交互式，推荐用浏览器打开)
echo   - lineage_static.svg       (静态图)
echo ======================================================================
echo.

REM 询问是否打开结果
set /p open_file="是否打开交互式可视化？(Y/N): "
if /i "%open_file%"=="Y" (
    if exist output\lineage_interactive.html (
        start output\lineage_interactive.html
    )
)

pause

