import sqlite3
import pandas as pd


def query_to_dataframe(db_path, query, n=100):
    """
    执行 SQL 查询并返回 pandas DataFrame，同时打印前 n 行（含表头）。

    参数:
        db_path (str): SQLite 数据库文件路径
        query (str): 要执行的 SQL 查询语句
        n (int): 要打印的行数，默认为 5

    返回:
        pd.DataFrame: 查询结果的 DataFrame
    """
    # 连接数据库
    conn = sqlite3.connect(db_path)
    
    try:
        # 执行查询并直接读取到 DataFrame（自动包含列名）
        df = pd.read_sql_query(query, conn)
        
        # 打印前 n 行（含表头）
        print(f"前 {n} 行结果（共 {len(df)} 行）：")
        print(df.head(n).to_string(index=False))
        
        return df
    
    finally:
        # 确保关闭连接
        conn.close()


# ==================== 使用示例 ====================
if __name__ == "__main__":
    # 假设有一个数据库文件 'example.db'
    DB_PATH = 'dw_metadata.db'
    
    # 示例查询语句
    SQL_QUERY = "SELECT * FROM tables LIMIT 10;"  # 替换为你的实际表和查询
    
    # 调用函数，设置 n=3
    result_df = query_to_dataframe(DB_PATH, SQL_QUERY, n=100)

