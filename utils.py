import logging
import sys
import json
from pathlib import Path
import os
from typing import List, Dict


def getLogger(name, file_name, use_formatter=True):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s    %(message)s')
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)
    if file_name:
        handler = logging.FileHandler(file_name, encoding='utf8')
        handler.setLevel(logging.INFO)
        if use_formatter:
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
            handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

def file_reader(file_path):
    encodings = ['utf-8-sig', 'utf-8', 'gbk', 'shift-jis', 'iso-8859-1']
    content = None
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                content = f.read()
            print(f"成功使用 {encoding} 编码读取文件")
            break
        except UnicodeDecodeError:
            continue
    return content

def save_res(file_path, sqls):
    with open(file_path, 'w', encoding='utf-8') as w:
        w.write(json.dumps(sqls, ensure_ascii=False, indent=4))  


def read_json_file(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

def collect_json_file(filepath='DDL/failed/'):
    # 获取 datasets 目录下的所有 .json 文件
    datasets_dir = Path(filepath)
    if not datasets_dir.exists():
        print("错误: datasets 目录不存在")
        return
    files =[]
    default_files = list(datasets_dir.glob('*.sql'))
    if not default_files:
        print("未找到文件")
    for file in default_files:
        files.append(os.path.basename(file))
    return files


def read_sql_file(file_path: str) -> str:
    """
    读取 SQL 文件内容
    :param file_path: SQL 文件路径
    :return: SQL 文件内容字符串
    :raises: FileNotFoundError, IOError
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"SQL 文件不存在: {file_path}")
    except UnicodeDecodeError:
        with open(file_path, 'r', encoding='GBK') as f:  # 尝试 GBK 编码
            return f.read()



def save_results(results: List[Dict], output_path: str) -> None:
    """
    保存解析结果到 JSON 文件
    """
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"结果已保存至: {output_path}")
    except Exception as e:
        print(f"保存结果失败: {str(e)}")






