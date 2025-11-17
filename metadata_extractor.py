import sqlglot
from sqlglot import exp
from typing import Dict, List, Set, Optional


def get_statement_type(ast: exp.Expression) -> str:
    """
    获取更精细的语句类型，用于冲突处理策略

    Args:
        ast: sqlglot解析后的AST

    Returns:
        语句类型字符串
    """
    if isinstance(ast, exp.Create):
        return 'CREATE_TABLE'  # 完整字段定义

    elif isinstance(ast, exp.Insert):
        # 检查是否显式指定了列名
        schema = ast.find(exp.Schema)
        if schema and schema.expressions:
            return 'INSERT_EXPLICIT'  # INSERT(col1,col2) - 字段可能不完整
        else:
            return 'INSERT_VALUES'   # INSERT VALUES - 字段可能不完整

    elif isinstance(ast, exp.Update):
        return 'UPDATE'  # 部分字段更新

    elif isinstance(ast, exp.Merge):
        return 'MERGE'  # 复杂操作

    else:
        return 'OTHER'


def _classify_statement_type(parsed_sql: exp.Expression) -> str:
    """
    分类SQL语句类型，用于字段信息优先级判断

    Returns:
        - 'CREATE_TABLE': CREATE TABLE (col1, col2, col3)
        - 'CREATE_TABLE_AS': CREATE TABLE AS SELECT
        - 'CREATE_VIEW': CREATE VIEW
        - 'INSERT_SELECT': INSERT INTO ... SELECT
        - 'INSERT_VALUES': INSERT INTO ... VALUES
        - 'UPDATE': UPDATE ... SET
        - 'MERGE': MERGE INTO
        - 'UNKNOWN': 未知类型
    """
    if isinstance(parsed_sql, exp.Create):
        if parsed_sql.kind and parsed_sql.kind.upper() == 'TABLE':
            if parsed_sql.expression:
                return 'CREATE_TABLE_AS'
            else:
                return 'CREATE_TABLE'
        elif parsed_sql.kind and parsed_sql.kind.upper() == 'VIEW':
            return 'CREATE_VIEW'
        else:
            return 'UNKNOWN'
    elif isinstance(parsed_sql, exp.Insert):
        if parsed_sql.expression and isinstance(parsed_sql.expression, exp.Select):
            return 'INSERT_SELECT'
        else:
            return 'INSERT_VALUES'
    elif isinstance(parsed_sql, exp.Update):
        return 'UPDATE'
    elif isinstance(parsed_sql, exp.Merge):
        return 'MERGE'
    else:
        return 'UNKNOWN'


def extract_sql_metadata(sql: str, dialect: str = None) -> Dict:
    """
    从SQL语句中提取元数据信息
    支持的语句类型：INSERT, UPDATE, MERGE INTO, CREATE TABLE AS
    
    Args:
        sql: SQL语句字符串
        dialect: SQL方言（如 'mysql', 'postgres', 'hive' 等），默认为None自动检测
    
    Returns:
        包含以下键的字典：
        - target_table: {"schema_nm": "schema名称", "tbl_en_nm": "表名"}
        - target_columns: [{"col_no": 序号, "col_en_nm": "字段英文名称", "col_cn_nm": "字段中文名称", "is_target_field": True/False}, ...]
        - source_tables: [{"schema_nm": "schema名称", "tbl_en_nm": "表名"}, ...]
        - statement_type: 语句类型分类
    """
    try:
        # 解析SQL为AST
        parsed = sqlglot.parse_one(sql, dialect=dialect)
        
        # 初始化结果
        result = {
            "target_table": {"schema_nm": "", "tbl_en_nm": ""},
            "target_columns": [],
            "source_tables": [],
            "statement_type": _classify_statement_type(parsed)
        }
        
        # 提取目标表信息
        target_table = _extract_target_table(parsed)
        result["target_table"] = target_table
        
        # 提取目标字段信息（区分目标字段和源字段注释）
        target_columns = _extract_target_columns_with_priority(parsed, result["statement_type"])
        result["target_columns"] = target_columns
        
        # 提取来源表信息
        source_tables = _extract_source_tables(parsed)
        result["source_tables"] = source_tables
        
        return result
        
    except Exception as e:
        raise Exception(f"解析SQL失败: {str(e)}")


def extract_insert_metadata(sql: str, dialect: str = None) -> Dict:
    """
    从INSERT语句中提取元数据信息（向后兼容的包装函数）
    
    Args:
        sql: INSERT SQL语句字符串
        dialect: SQL方言（如 'mysql', 'postgres', 'hive' 等），默认为None自动检测
    
    Returns:
        包含以下键的字典：
        - target_table: {"schema_nm": "schema名称", "tbl_en_nm": "表名"}
        - target_columns: [{"col_en_nm": "字段英文名称", "col_cn_nm": "字段中文名称"}, ...]
        - source_tables: [{"schema_nm": "schema名称", "tbl_en_nm": "表名"}, ...]
    """
    return extract_sql_metadata(sql, dialect)


def _resolve_update_table_alias(update_sql: exp.Update, table_expr: exp.Table) -> exp.Table:
    """
    解析UPDATE语句中的别名映射，将别名映射到真实表名
    
    在某些数据库（如SQL Server、Teradata）中，UPDATE语句可以使用如下语法：
    UPDATE alias FROM table_name alias SET ...
    此时UPDATE后跟的是别名而不是真正的表名，需要从FROM子句中找到真实表名。
    
    Args:
        update_sql: UPDATE语句的AST
        table_expr: UPDATE语句的this（可能是别名）
    
    Returns:
        真实的表对象，如果无法解析则返回原table_expr
    """
    # 如果没有FROM子句，直接返回原表达式
    from_clause = update_sql.args.get('from')
    if not from_clause:
        return table_expr
    
    # 如果table_expr不是Table类型，直接返回
    if not isinstance(table_expr, exp.Table):
        return table_expr
    
    # 提取UPDATE后的表名（可能是别名）
    possible_alias = None
    if hasattr(table_expr.this, 'this'):
        possible_alias = table_expr.this.this
    else:
        possible_alias = str(table_expr.this)
    
    # 检查UPDATE后的表是否有schema，如果有schema则很可能不是别名
    has_schema = False
    if table_expr.db:
        has_schema = True
    
    # 如果已经有schema，且UPDATE的this就是一个完整的表引用，可能不需要映射
    # 但仍然检查FROM子句中是否有匹配的别名
    
    # 在FROM子句中查找表及其别名
    from_tables = []
    for table in from_clause.find_all(exp.Table):
        from_tables.append(table)
    
    # 遍历FROM子句中的表，查找别名匹配
    for from_table in from_tables:
        # 获取FROM表的别名
        table_alias = None
        if hasattr(from_table, 'alias'):
            table_alias = from_table.alias
            if isinstance(table_alias, exp.TableAlias):
                # 从TableAlias中提取别名名称
                if hasattr(table_alias, 'this'):
                    if hasattr(table_alias.this, 'this'):
                        table_alias = table_alias.this.this
                    else:
                        table_alias = str(table_alias.this)
            elif isinstance(table_alias, str):
                pass  # 已经是字符串
            elif hasattr(table_alias, 'this'):
                table_alias = table_alias.this
            else:
                table_alias = str(table_alias)
        
        # 如果找到别名匹配，返回FROM子句中的真实表
        if table_alias and table_alias == possible_alias:
            return from_table
    
    # 如果没有找到别名匹配，检查是否有单个表的情况
    # 在某些情况下，UPDATE alias FROM table_name，但FROM中的表没有明确写别名
    # 这时如果FROM中只有一个表且UPDATE后没有schema，可能UPDATE后的就是别名
    if not has_schema and len(from_tables) == 1:
        # 获取FROM中唯一表的名称
        from_table = from_tables[0]
        from_table_name = None
        if hasattr(from_table.this, 'this'):
            from_table_name = from_table.this.this
        else:
            from_table_name = str(from_table.this)
        
        # 如果UPDATE后的名称与FROM表名称不同，说明UPDATE后的是别名
        if from_table_name != possible_alias:
            return from_table
    
    # 无法确定映射关系，返回原表达式
    return table_expr


def _extract_target_table(parsed_sql: exp.Expression) -> Dict[str, str]:
    """
    提取目标表信息（支持INSERT/UPDATE/MERGE/CREATE TABLE AS）
    
    Args:
        parsed_sql: sqlglot解析后的AST
    
    Returns:
        {"schema_nm": "schema名称", "tbl_en_nm": "表名"}
    """
    result = {"schema_nm": "", "tbl_en_nm": ""}
    table_expr = None
    
    # INSERT语句
    if isinstance(parsed_sql, exp.Insert):
        table_expr = parsed_sql.this
        # 如果是Schema对象，需要从Schema.this获取Table
        if isinstance(table_expr, exp.Schema):
            table_expr = table_expr.this
    
    # UPDATE语句 - 需要处理别名映射
    elif isinstance(parsed_sql, exp.Update):
        table_expr = parsed_sql.this
        # 调用辅助函数处理UPDATE语句的别名映射
        table_expr = _resolve_update_table_alias(parsed_sql, table_expr)
    
    # MERGE语句
    elif isinstance(parsed_sql, exp.Merge):
        table_expr = parsed_sql.this
    
    # CREATE TABLE AS语句
    elif isinstance(parsed_sql, exp.Create):
        table_expr = parsed_sql.this
    
    # 提取表信息
    if table_expr and isinstance(table_expr, exp.Table):
        # 提取表名 - 注意Table.this是Identifier对象
        if hasattr(table_expr.this, 'this'):
            result["tbl_en_nm"] = table_expr.this.this
        else:
            result["tbl_en_nm"] = str(table_expr.this)
        
        # 提取schema名（如果存在） - Table.db也是Identifier对象
        if table_expr.db:
            if hasattr(table_expr.db, 'this'):
                result["schema_nm"] = table_expr.db.this
            else:
                result["schema_nm"] = str(table_expr.db)
                
    return result


def _extract_target_columns_with_priority(parsed_sql: exp.Expression, statement_type: str) -> List[Dict]:
    """
    提取目标字段信息，区分目标字段和源字段注释，并标记字段来源类型

    Args:
        parsed_sql: sqlglot解析后的AST
        statement_type: 语句类型分类

    Returns:
        [{"col_no": 序号, "col_en_nm": "字段英文名称", "col_cn_nm": "字段中文名称", "is_target_field": True/False, "source_type": "语句类型"}, ...]
    """
    columns = []

    # INSERT语句 - 目标字段优先级最高
    if isinstance(parsed_sql, exp.Insert):
        if parsed_sql.expression:
            schema = parsed_sql.find(exp.Schema)
            if schema:
                # 首先从Schema获取目标字段名和注释（优先级最高）
                for expr in schema.expressions:
                    col_info = _extract_column_info(expr)
                    if col_info:
                        col_info["is_target_field"] = True
                        col_info["source_type"] = statement_type
                        columns.append(col_info)

                # 然后从SELECT的投影列中获取源字段注释（优先级较低）
                select_expr = parsed_sql.expression
                if isinstance(select_expr, exp.Select) and select_expr.expressions:
                    # 如果字段数量匹配，添加源字段注释
                    if len(select_expr.expressions) == len(columns):
                        for i, projection in enumerate(select_expr.expressions):
                            if hasattr(projection, 'comments') and projection.comments:
                                source_comment = " ".join(projection.comments).strip()
                                # 如果目标字段没有注释，使用源字段注释
                                if not columns[i]["col_cn_nm"]:
                                    columns[i]["col_cn_nm"] = source_comment
                                    columns[i]["is_target_field"] = False  # 标记为源字段注释

    # UPDATE语句 - 目标字段（被更新的字段）
    elif isinstance(parsed_sql, exp.Update):
        if parsed_sql.expressions:
            for set_item in parsed_sql.expressions:
                if isinstance(set_item, exp.EQ) and set_item.this:
                    if isinstance(set_item.this, exp.Column):
                        col_info = _extract_column_info(set_item.this)
                        if col_info:
                            col_info["is_target_field"] = True
                            col_info["source_type"] = statement_type
                            # 注释可能在EQ节点上（逗号后的注释）或右边的表达式上（行尾注释）
                            if hasattr(set_item, 'comments') and set_item.comments:
                                col_info["col_cn_nm"] = " ".join(set_item.comments).strip()
                            elif hasattr(set_item.expression, 'comments') and set_item.expression.comments:
                                col_info["col_cn_nm"] = " ".join(set_item.expression.comments).strip()
                            columns.append(col_info)

    # MERGE语句 - 从WHEN MATCHED/NOT MATCHED子句中提取列
    elif isinstance(parsed_sql, exp.Merge):
        # 从WHEN MATCHED THEN UPDATE中提取（目标字段）
        for when_matched in parsed_sql.find_all(exp.When):
            for set_item in when_matched.find_all(exp.EQ):
                if set_item.this and isinstance(set_item.this, exp.Column):
                    col_info = _extract_column_info(set_item.this)
                    if col_info:
                        col_info["is_target_field"] = True
                        col_info["source_type"] = statement_type
                        # 注释可能在EQ节点上（逗号后的注释）或右边的表达式上（行尾注释）
                        if hasattr(set_item, 'comments') and set_item.comments:
                            col_info["col_cn_nm"] = " ".join(set_item.comments).strip()
                        elif hasattr(set_item.expression, 'comments') and set_item.expression.comments:
                            col_info["col_cn_nm"] = " ".join(set_item.expression.comments).strip()
                        if col_info not in columns:
                            columns.append(col_info)

        # 从WHEN NOT MATCHED THEN INSERT中提取（目标字段）
        for schema in parsed_sql.find_all(exp.Schema):
            for expr in schema.expressions:
                col_info = _extract_column_info(expr)
                if col_info:
                    col_info["is_target_field"] = True
                    col_info["source_type"] = statement_type
                    if col_info not in columns:
                        columns.append(col_info)

    # CREATE TABLE AS语句 - 从SELECT的投影列中提取目标字段
    elif isinstance(parsed_sql, exp.Create):
        # 首先尝试从显式的Schema定义中提取（如果有）
        schema = parsed_sql.find(exp.Schema)
        if schema and schema.expressions:
            for expr in schema.expressions:
                col_info = _extract_column_info(expr)
                if col_info:
                    col_info["is_target_field"] = True
                    col_info["source_type"] = statement_type
                    columns.append(col_info)
        # 如果没有显式Schema，从SELECT的投影列中提取
        elif parsed_sql.expression:
            select_expr = parsed_sql.expression
            if isinstance(select_expr, exp.Select):
                columns = _extract_select_columns_with_priority(select_expr, statement_type)
            elif hasattr(select_expr, 'this') and isinstance(select_expr.this, exp.Select):
                columns = _extract_select_columns_with_priority(select_expr.this, statement_type)

    # 为所有字段添加序号，并调整字段顺序
    result_columns = []
    for i, col in enumerate(columns, start=1):
        result_columns.append({
            "col_no": i,
            "col_en_nm": col["col_en_nm"],
            "col_cn_nm": col["col_cn_nm"],
            "is_target_field": col.get("is_target_field", False),
            "source_type": col.get("source_type", statement_type)
        })

    return result_columns


def _extract_select_columns_with_priority(select_expr: exp.Select, statement_type: str) -> List[Dict]:
    """从SELECT语句的投影列中提取字段信息（用于CREATE TABLE AS）"""
    columns = []
    if select_expr.expressions:
        for expr in select_expr.expressions:
            col_info = _extract_column_info(expr)
            if col_info:
                col_info["is_target_field"] = True  # CREATE TABLE AS的目标字段
                col_info["source_type"] = statement_type
                columns.append(col_info)
    return columns


def _extract_target_columns(parsed_sql: exp.Expression) -> List[Dict[str, str]]:
    """
    提取目标字段信息（支持INSERT/UPDATE/MERGE/CREATE TABLE AS）
    
    Args:
        parsed_sql: sqlglot解析后的AST
    
    Returns:
        [{"col_no": 序号, "col_en_nm": "字段英文名称", "col_cn_nm": "字段中文名称"}, ...]
    """
    columns = []
    
    # INSERT语句 - 从Schema中提取列名，从SELECT中提取注释
    if isinstance(parsed_sql, exp.Insert):
        if parsed_sql.expression:
            schema = parsed_sql.find(exp.Schema)
            if schema:
                # 首先从Schema获取字段名列表
                for expr in schema.expressions:
                    col_info = _extract_column_info(expr)
                    if col_info:
                        columns.append(col_info)
                
                # 然后尝试从SELECT的投影列中获取注释
                select_expr = parsed_sql.expression
                if isinstance(select_expr, exp.Select) and select_expr.expressions:
                    # 如果字段数量匹配，将SELECT中的注释匹配到对应字段
                    if len(select_expr.expressions) == len(columns):
                        for i, projection in enumerate(select_expr.expressions):
                            if hasattr(projection, 'comments') and projection.comments:
                                columns[i]["col_cn_nm"] = " ".join(projection.comments).strip()
    
    # UPDATE语句 - 从SET子句中提取被更新的列
    elif isinstance(parsed_sql, exp.Update):
        # 只从SET子句（expressions）中提取，不包含WHERE等其他子句
        if parsed_sql.expressions:
            for set_item in parsed_sql.expressions:
                # SET子句中的赋值操作，左边是列名
                if isinstance(set_item, exp.EQ) and set_item.this:
                    if isinstance(set_item.this, exp.Column):
                        col_info = _extract_column_info(set_item.this)
                        if col_info:
                            # 注释可能在EQ节点上（逗号后的注释）或右边的表达式上（行尾注释）
                            if hasattr(set_item, 'comments') and set_item.comments:
                                col_info["col_cn_nm"] = " ".join(set_item.comments).strip()
                            elif hasattr(set_item.expression, 'comments') and set_item.expression.comments:
                                col_info["col_cn_nm"] = " ".join(set_item.expression.comments).strip()
                            columns.append(col_info)
    
    # MERGE语句 - 从WHEN MATCHED/NOT MATCHED子句中提取列
    elif isinstance(parsed_sql, exp.Merge):
        # 从WHEN MATCHED THEN UPDATE中提取
        for when_matched in parsed_sql.find_all(exp.When):
            for set_item in when_matched.find_all(exp.EQ):
                if set_item.this and isinstance(set_item.this, exp.Column):
                    col_info = _extract_column_info(set_item.this)
                    if col_info:
                        # 注释可能在EQ节点上（逗号后的注释）或右边的表达式上（行尾注释）
                        if hasattr(set_item, 'comments') and set_item.comments:
                            col_info["col_cn_nm"] = " ".join(set_item.comments).strip()
                        elif hasattr(set_item.expression, 'comments') and set_item.expression.comments:
                            col_info["col_cn_nm"] = " ".join(set_item.expression.comments).strip()
                        if col_info not in columns:
                            columns.append(col_info)
        
        # 从WHEN NOT MATCHED THEN INSERT中提取
        # 这部分类似于INSERT，寻找Schema
        for schema in parsed_sql.find_all(exp.Schema):
            for expr in schema.expressions:
                col_info = _extract_column_info(expr)
                if col_info and col_info not in columns:
                    columns.append(col_info)
    
    # CREATE TABLE AS语句 - 从SELECT的投影列中提取目标字段
    elif isinstance(parsed_sql, exp.Create):
        # 首先尝试从显式的Schema定义中提取（如果有）
        schema = parsed_sql.find(exp.Schema)
        if schema and schema.expressions:
            for expr in schema.expressions:
                col_info = _extract_column_info(expr)
                if col_info:
                    columns.append(col_info)
        # 如果没有显式Schema，从SELECT的投影列中提取
        elif parsed_sql.expression:
            select_expr = parsed_sql.expression
            # 查找SELECT语句
            if isinstance(select_expr, exp.Select):
                columns = _extract_select_columns(select_expr)
            # 可能包含在子查询中
            elif hasattr(select_expr, 'this') and isinstance(select_expr.this, exp.Select):
                columns = _extract_select_columns(select_expr.this)
    
    # 为所有字段添加序号，并调整字段顺序为：col_no, col_en_nm, col_cn_nm
    result_columns = []
    for i, col in enumerate(columns, start=1):
        result_columns.append({
            "col_no": i,
            "col_en_nm": col["col_en_nm"],
            "col_cn_nm": col["col_cn_nm"]
        })
    
    return result_columns


def _extract_select_columns(select_expr: exp.Select) -> List[Dict[str, str]]:
    """
    从SELECT语句的投影列中提取字段信息
    
    Args:
        select_expr: SELECT表达式
    
    Returns:
        [{"col_en_nm": "字段英文名称", "col_cn_nm": "字段中文名称"}, ...]
    """
    columns = []
    
    if not select_expr.expressions:
        return columns
    
    for projection in select_expr.expressions:
        col_info = {"col_en_nm": "", "col_cn_nm": ""}
        
        # 处理别名（Alias）
        if isinstance(projection, exp.Alias):
            # 使用别名作为字段名
            alias = projection.alias
            if isinstance(alias, str):
                col_info["col_en_nm"] = alias
            elif hasattr(alias, 'this'):
                col_info["col_en_nm"] = alias.this
            else:
                col_info["col_en_nm"] = str(alias)
            
            # 尝试提取注释
            if hasattr(projection, 'comments') and projection.comments:
                col_info["col_cn_nm"] = " ".join(projection.comments).strip()
        
        # 处理列（Column）
        elif isinstance(projection, exp.Column):
            col_info["col_en_nm"] = projection.name
            if hasattr(projection, 'comments') and projection.comments:
                col_info["col_cn_nm"] = " ".join(projection.comments).strip()
        
        # 处理标识符（Identifier）
        elif isinstance(projection, exp.Identifier):
            col_info["col_en_nm"] = projection.name
            if hasattr(projection, 'comments') and projection.comments:
                col_info["col_cn_nm"] = " ".join(projection.comments).strip()
        
        # 处理函数调用等其他表达式
        else:
            # 尝试获取表达式的字符串表示
            if hasattr(projection, 'name'):
                col_info["col_en_nm"] = projection.name
            elif hasattr(projection, 'sql_name'):
                col_info["col_en_nm"] = projection.sql_name()
            else:
                # 使用表达式的字符串形式（作为最后的手段）
                col_str = str(projection)
                # 简化过长的表达式
                if len(col_str) > 50:
                    col_str = col_str[:47] + "..."
                col_info["col_en_nm"] = col_str
            
            if hasattr(projection, 'comments') and projection.comments:
                col_info["col_cn_nm"] = " ".join(projection.comments).strip()
        
        if col_info["col_en_nm"]:
            columns.append(col_info)
    
    return columns


def _extract_column_info(expr: exp.Expression) -> Optional[Dict[str, str]]:
    """
    从表达式中提取列信息的辅助函数
    
    Args:
        expr: sqlglot表达式对象
    
    Returns:
        {"col_en_nm": "字段英文名称", "col_cn_nm": "字段中文名称"} 或 None
    """
    col_info = {"col_en_nm": "", "col_cn_nm": ""}
    
    if isinstance(expr, exp.Column):
        col_info["col_en_nm"] = expr.name
    elif isinstance(expr, exp.Identifier):
        col_info["col_en_nm"] = expr.name
    else:
        # 处理其他类型的表达式
        col_name = expr.name if hasattr(expr, 'name') else str(expr)
        col_info["col_en_nm"] = col_name
    
    # 提取注释作为中文名称
    if hasattr(expr, 'comments') and expr.comments:
        # 合并所有注释
        col_info["col_cn_nm"] = " ".join(expr.comments).strip()
    
    return col_info if col_info["col_en_nm"] else None


def _extract_source_tables(parsed_sql: exp.Expression) -> List[Dict[str, str]]:
    """
    提取来源表信息（支持INSERT/UPDATE/MERGE/CREATE TABLE AS），排除CTE和子查询的别名
    
    Args:
        parsed_sql: sqlglot解析后的AST
    
    Returns:
        [{"schema_nm": "schema名称", "tbl_en_nm": "表名"}, ...]
    """
    source_tables = []
    seen_tables = set()  # 用于去重
    
    # 确定要搜索的表达式范围
    search_expressions = []
    
    # INSERT语句 - 从expression部分（SELECT语句）提取
    if isinstance(parsed_sql, exp.Insert):
        if parsed_sql.expression:
            search_expressions.append(parsed_sql.expression)
    
    # UPDATE语句 - 从FROM子句、SET子句和WHERE等子句提取（但排除目标表）
    elif isinstance(parsed_sql, exp.Update):
        # UPDATE的FROM子句
        from_clause = parsed_sql.args.get('from')
        if from_clause:
            search_expressions.append(from_clause)
        # SET子句中可能包含子查询
        if parsed_sql.expressions:
            for set_expr in parsed_sql.expressions:
                search_expressions.append(set_expr)
        # WHERE子句可能包含子查询
        where_clause = parsed_sql.args.get('where')
        if where_clause:
            search_expressions.append(where_clause)
    
    # MERGE语句 - 从USING子句提取
    elif isinstance(parsed_sql, exp.Merge):
        using_clause = parsed_sql.args.get('using')
        if using_clause:
            search_expressions.append(using_clause)
        # WHEN子句中可能也有子查询
        for when_clause in parsed_sql.find_all(exp.When):
            search_expressions.append(when_clause)
    
    # CREATE TABLE AS语句 - 从AS后的SELECT语句提取
    elif isinstance(parsed_sql, exp.Create):
        # 查找CREATE TABLE AS中的SELECT语句
        select_expr = parsed_sql.expression
        if select_expr:
            search_expressions.append(select_expr)
    
    # 如果没有要搜索的表达式，返回空列表
    if not search_expressions:
        return source_tables
    
    # 收集所有CTE（公共表表达式）的别名和其内部的表
    # 注意：CTE定义在整个语句级别，需要从整个parsed_sql中查找
    cte_aliases = set()
    cte_tables = []  # 存储CTE内部的表
    
    for cte in parsed_sql.find_all(exp.CTE):
        if cte.alias:
            # alias可能是字符串或Identifier对象
            alias_name = cte.alias if isinstance(cte.alias, str) else str(cte.alias)
            cte_aliases.add(alias_name)
        
        # 提取CTE内部的表（这些也是来源表）
        for table in cte.find_all(exp.Table):
            cte_tables.append(table)
    
    # 收集所有子查询的别名
    subquery_aliases = set()
    for search_expr in search_expressions:
        for subquery in search_expr.find_all(exp.Subquery):
            if subquery.alias:
                # alias可能是字符串或Identifier对象
                alias_name = subquery.alias if isinstance(subquery.alias, str) else str(subquery.alias)
                subquery_aliases.add(alias_name)
    
    # 收集目标表，避免把它当作来源表（特别是UPDATE语句）
    target_table_key = None
    target_table_info = _extract_target_table(parsed_sql)
    if target_table_info["tbl_en_nm"]:
        target_table_key = (
            f"{target_table_info['schema_nm']}.{target_table_info['tbl_en_nm']}" 
            if target_table_info['schema_nm'] 
            else target_table_info['tbl_en_nm']
        )
    
    # 遍历搜索表达式中的Table节点 + CTE内部的表
    all_tables = cte_tables.copy()
    for search_expr in search_expressions:
        all_tables.extend(search_expr.find_all(exp.Table))
    
    for table in all_tables:
        # 提取表名
        if hasattr(table.this, 'this'):
            table_name = table.this.this
        else:
            table_name = table.name
        
        # 提取schema名
        schema_name = ""
        if table.db:
            if hasattr(table.db, 'this'):
                schema_name = table.db.this
            else:
                schema_name = str(table.db)
        
        # 跳过CTE和子查询的别名
        if table_name in cte_aliases or table_name in subquery_aliases:
            continue
        
        # 构建唯一标识（用于去重）
        table_key = f"{schema_name}.{table_name}" if schema_name else table_name
        
        # 对于UPDATE/MERGE语句，避免将目标表作为来源表
        # 但如果是自引用（真的需要读取目标表），则保留
        if isinstance(parsed_sql, (exp.Update, exp.Merge)):
            # 检查这个表是否就是目标表在语句开头的定义
            # 如果表在FROM或USING子句中，说明是真正的来源表
            is_target_in_from = False
            if isinstance(parsed_sql, exp.Update):
                # UPDATE语句中，目标表在this中，FROM子句中的才是来源
                is_target_in_from = parsed_sql.args.get('from') and table in parsed_sql.args.get('from').find_all(exp.Table)
            elif isinstance(parsed_sql, exp.Merge):
                # MERGE语句中，目标表在this中，USING中的才是来源
                is_target_in_from = parsed_sql.args.get('using') and table in parsed_sql.args.get('using').find_all(exp.Table)
            
            # 如果表在FROM/USING中，或者表键与目标表不同，则认为是来源表
            if not is_target_in_from and table_key == target_table_key and table == parsed_sql.this:
                continue
        
        if table_key not in seen_tables:
            seen_tables.add(table_key)
            source_tables.append({
                "schema_nm": schema_name,
                "tbl_en_nm": table_name
            })
    
    return source_tables


def extract_ddl_metadata(sql: str, dialect: str = None) -> Dict:
    """
    从DDL的CREATE语句中提取元数据信息
    
    Args:
        sql: CREATE SQL语句字符串
        dialect: SQL方言（如 'mysql', 'postgres', 'hive', 'teradata' 等），默认为None自动检测
    
    Returns:
        包含以下键的字典：
        - target_table: {
            "schema_nm": "schema名称（若有，否则为空字符串）", 
            "tbl_en_nm": "表英文名", 
            "tbl_cn_nm": "表中文名"
          }
        - target_columns: [{
            "col_no": 字段序号,
            "col_en_nm": "字段英文名称",
            "col_cn_nm": "字段中文名称",
            "data_type": "数据类型",
            "is_null": True或False,
            "default_value": "默认值",
            "is_pri_key": True或False,
            "is_foreign_key": True或False
          }, ...]
    """
    try:
        # 解析SQL为AST
        parsed = sqlglot.parse_one(sql, dialect=dialect)
        
        # 初始化结果
        result = {
            "target_table": {"schema_nm": "", "tbl_en_nm": "", "tbl_cn_nm": ""},
            "target_columns": [],
            "source_tables": []
        }
        
        # 确保是CREATE语句
        if not isinstance(parsed, exp.Create):
            raise Exception("不是有效的CREATE语句")
        
        # 提取目标表信息
        target_table = _extract_ddl_target_table(parsed)
        result["target_table"] = target_table
        
        # 提取目标字段信息
        target_columns = _extract_ddl_columns(parsed)
        result["target_columns"] = target_columns
        
        # 提取来源表信息（CREATE TABLE AS SELECT的情况）
        source_tables = _extract_source_tables(parsed)
        result["source_tables"] = source_tables
        
        return result
        
    except Exception as e:
        raise Exception(f"解析DDL语句失败: {str(e)}")


def _extract_ddl_target_table(parsed_sql: exp.Create) -> Dict[str, str]:
    """
    从CREATE语句中提取目标表信息
    
    Args:
        parsed_sql: sqlglot解析后的CREATE语句AST
    
    Returns:
        {"schema_nm": "schema名称", "tbl_en_nm": "表名", "tbl_cn_nm": "表中文名"}
    """
    result = {"schema_nm": "", "tbl_en_nm": "", "tbl_cn_nm": ""}
    
    # 获取表对象
    # parsed.this 可能是 Schema 或 Table
    table_expr = None
    
    if isinstance(parsed_sql.this, exp.Schema):
        # 如果是Schema，表对象在Schema.this中
        table_expr = parsed_sql.this.this
    elif isinstance(parsed_sql.this, exp.Table):
        # 直接是Table
        table_expr = parsed_sql.this
    
    if table_expr and isinstance(table_expr, exp.Table):
        # 提取表名
        if hasattr(table_expr.this, 'this'):
            result["tbl_en_nm"] = table_expr.this.this
        else:
            result["tbl_en_nm"] = str(table_expr.this)
        
        # 提取schema名（如果存在）
        if table_expr.db:
            if hasattr(table_expr.db, 'this'):
                result["schema_nm"] = table_expr.db.this
            else:
                result["schema_nm"] = str(table_expr.db)
    
    # 提取表注释（COMMENT）作为中文名
    # 注释可能在表级别的properties中
    if hasattr(parsed_sql, 'args') and 'properties' in parsed_sql.args:
        properties = parsed_sql.args['properties']
        if properties:
            # properties可能是Property节点的列表
            if isinstance(properties, exp.Properties):
                for prop in properties.expressions:
                    # 查找SchemaCommentProperty
                    if isinstance(prop, exp.SchemaCommentProperty):
                        if prop.this:
                            comment = str(prop.this)
                            # 移除引号
                            result["tbl_cn_nm"] = comment.strip("'\"")
                    # 检查属性名称中是否包含COMMENT
                    elif hasattr(prop, 'name') and 'COMMENT' in str(prop.name).upper():
                        if hasattr(prop, 'this'):
                            comment = str(prop.this)
                            result["tbl_cn_nm"] = comment.strip("'\"")
    
    # 如果没有找到COMMENT，也检查parsed_sql本身的comments属性
    if not result["tbl_cn_nm"] and hasattr(parsed_sql, 'comments') and parsed_sql.comments:
        result["tbl_cn_nm"] = " ".join(parsed_sql.comments).strip()
    
    return result


def _extract_ddl_columns(parsed_sql: exp.Create) -> List[Dict]:
    """
    从CREATE语句中提取字段信息
    
    Args:
        parsed_sql: sqlglot解析后的CREATE语句AST
    
    Returns:
        字段信息列表，每个元素包含：col_no, col_en_nm, col_cn_nm, data_type, 
        is_null, default_value, is_pri_key, is_foreign_key
    """
    columns = []
    primary_keys = set()  # 存储主键字段名
    foreign_keys = set()  # 存储外键字段名
    
    # 首先收集主键信息
    primary_keys = _extract_primary_keys(parsed_sql)
    
    # 收集外键信息
    foreign_keys = _extract_foreign_keys(parsed_sql)
    
    # 查找Schema节点（字段定义）
    schema = parsed_sql.find(exp.Schema)
    
    if not schema or not schema.expressions:
        return columns
    
    col_no = 1
    for expr in schema.expressions:
        # 只处理ColumnDef节点（字段定义）
        if isinstance(expr, exp.ColumnDef):
            col_info = _extract_column_def_info(expr, col_no, primary_keys, foreign_keys)
            if col_info:
                columns.append(col_info)
                col_no += 1
    
    return columns


def _extract_column_def_info(column_def: exp.ColumnDef, col_no: int, 
                             primary_keys: Set[str], foreign_keys: Set[str]) -> Dict:
    """
    从ColumnDef节点中提取字段详细信息
    
    Args:
        column_def: ColumnDef表达式节点
        col_no: 字段序号
        primary_keys: 主键字段名集合
        foreign_keys: 外键字段名集合
    
    Returns:
        字段信息字典
    """
    col_info = {
        "col_no": col_no,
        "col_en_nm": "",
        "col_cn_nm": "",
        "data_type": "",
        "is_null": True,  # 默认允许为空
        "default_value": "",
        "is_pri_key": False,
        "is_foreign_key": False
    }
    
    # 1. 提取字段名
    if column_def.this:
        if hasattr(column_def.this, 'this'):
            col_info["col_en_nm"] = column_def.this.this
        else:
            col_info["col_en_nm"] = str(column_def.this)
    
    # 2. 提取数据类型
    if column_def.kind:
        # 将数据类型节点转换为SQL文本
        col_info["data_type"] = column_def.kind.sql()
    
    # 3. 检查约束条件
    if hasattr(column_def, 'constraints') and column_def.constraints:
        for constraint in column_def.constraints:
            # 约束通常包装在ColumnConstraint中，真正的约束类型在kind属性中
            actual_constraint = constraint
            if hasattr(constraint, 'kind') and constraint.kind:
                actual_constraint = constraint.kind
            
            # NOT NULL约束
            if isinstance(actual_constraint, exp.NotNullColumnConstraint):
                col_info["is_null"] = False
            
            # DEFAULT约束
            elif isinstance(actual_constraint, exp.DefaultColumnConstraint):
                if actual_constraint.this:
                    col_info["default_value"] = str(actual_constraint.this)
            
            # PRIMARY KEY约束（字段级别）
            elif hasattr(exp, 'PrimaryKeyColumnConstraint') and isinstance(actual_constraint, exp.PrimaryKeyColumnConstraint):
                col_info["is_pri_key"] = True
                col_info["is_null"] = False  # 主键不允许为空
            
            # COMMENT约束
            elif isinstance(actual_constraint, exp.CommentColumnConstraint):
                if actual_constraint.this:
                    comment = str(actual_constraint.this)
                    # 移除引号
                    col_info["col_cn_nm"] = comment.strip("'\"")
    
    # 4. 如果字段名在主键集合中，标记为主键
    if col_info["col_en_nm"] in primary_keys:
        col_info["is_pri_key"] = True
        col_info["is_null"] = False
    
    # 5. 如果字段名在外键集合中，标记为外键
    if col_info["col_en_nm"] in foreign_keys:
        col_info["is_foreign_key"] = True
    
    # 6. 提取注释（如果COMMENT约束未找到）
    if not col_info["col_cn_nm"]:
        # 尝试从约束的comments属性中获取（如Teradata的CASESPECIFIC等约束后的注释）
        if hasattr(column_def, 'constraints') and column_def.constraints:
            for constraint in column_def.constraints:
                # 检查约束的kind属性中的comments
                if hasattr(constraint, 'kind'):
                    kind = constraint.kind
                    
                    # 首先检查kind本身的comments
                    if hasattr(kind, 'comments') and kind.comments:
                        col_info["col_cn_nm"] = " ".join(str(c).strip() for c in kind.comments if c).strip()
                        break
                    
                    # 然后检查kind.this的comments（如DateFormatColumnConstraint）
                    if hasattr(kind, 'this') and kind.this and hasattr(kind.this, 'comments'):
                        comments = kind.this.comments
                        if comments:
                            col_info["col_cn_nm"] = " ".join(str(c).strip() for c in comments if c).strip()
                            break
                
                # 或者直接检查约束的comments属性
                elif hasattr(constraint, 'comments') and constraint.comments:
                    col_info["col_cn_nm"] = " ".join(str(c).strip() for c in constraint.comments if c).strip()
                    break
        
        # 尝试从节点的comments属性获取（代码注释）
        if not col_info["col_cn_nm"] and hasattr(column_def, 'comments') and column_def.comments:
            col_info["col_cn_nm"] = " ".join(str(c).strip() for c in column_def.comments if c).strip()
    
    return col_info


def _extract_primary_keys(parsed_sql: exp.Create) -> Set[str]:
    """
    提取主键字段名集合
    
    Args:
        parsed_sql: CREATE语句的AST
    
    Returns:
        主键字段名集合
    """
    primary_keys = set()
    
    # 查找所有PrimaryKey约束（表级别）
    for pk_constraint in parsed_sql.find_all(exp.PrimaryKey):
        # PrimaryKey的expressions包含主键字段
        if hasattr(pk_constraint, 'expressions') and pk_constraint.expressions:
            for col_expr in pk_constraint.expressions:
                # 可能是Column或Identifier
                if isinstance(col_expr, exp.Column):
                    primary_keys.add(col_expr.name)
                elif isinstance(col_expr, exp.Identifier):
                    primary_keys.add(col_expr.name)
                elif hasattr(col_expr, 'this'):
                    # 处理其他可能的表达式类型
                    if isinstance(col_expr.this, str):
                        primary_keys.add(col_expr.this)
                    elif hasattr(col_expr.this, 'this'):
                        primary_keys.add(col_expr.this.this)
    
    # 查找indexes列表中的PRIMARY INDEX（某些数据库如Teradata）
    if hasattr(parsed_sql, 'args') and 'indexes' in parsed_sql.args:
        indexes = parsed_sql.args['indexes']
        if indexes:
            for index in indexes:
                # primary属性存储在index.args中
                is_primary = False
                if isinstance(index, exp.Index):
                    if hasattr(index, 'args') and 'primary' in index.args:
                        is_primary = index.args['primary']
                
                if is_primary:
                    # 这是主索引，提取字段
                    # 字段可能在index.args['params']中
                    params = None
                    if hasattr(index, 'args') and 'params' in index.args:
                        params = index.args['params']
                    
                    if params:
                        # columns可能在params.args['columns']中
                        columns = None
                        if hasattr(params, 'args') and 'columns' in params.args:
                            columns = params.args['columns']
                        
                        if columns:
                            for col_item in columns:
                                # 可能是Ordered对象，需要获取其this
                                col_expr = col_item.this if hasattr(col_item, 'this') else col_item
                                
                                if isinstance(col_expr, exp.Column):
                                    primary_keys.add(col_expr.name)
                                elif isinstance(col_expr, exp.Identifier):
                                    primary_keys.add(col_expr.name)
                                elif hasattr(col_expr, 'this'):
                                    if isinstance(col_expr.this, str):
                                        primary_keys.add(col_expr.this)
                                    elif hasattr(col_expr.this, 'this'):
                                        primary_keys.add(col_expr.this.this)
                    
                    # 字段也可能在index.expressions中
                    if hasattr(index, 'expressions') and index.expressions:
                        for col_expr in index.expressions:
                            if isinstance(col_expr, exp.Column):
                                primary_keys.add(col_expr.name)
                            elif isinstance(col_expr, exp.Identifier):
                                primary_keys.add(col_expr.name)
                            elif hasattr(col_expr, 'this'):
                                if isinstance(col_expr.this, str):
                                    primary_keys.add(col_expr.this)
                                elif hasattr(col_expr.this, 'this'):
                                    primary_keys.add(col_expr.this.this)
    
    # 查找PrimaryKeyColumnConstraint（字段级别，已在_extract_column_def_info中处理）
    # 这里不需要重复处理
    
    return primary_keys


def _extract_foreign_keys(parsed_sql: exp.Create) -> Set[str]:
    """
    提取外键字段名集合
    
    Args:
        parsed_sql: CREATE语句的AST
    
    Returns:
        外键字段名集合
    """
    foreign_keys = set()
    
    # 查找所有ForeignKey约束
    for fk_constraint in parsed_sql.find_all(exp.ForeignKey):
        # ForeignKey的expressions包含外键字段
        if hasattr(fk_constraint, 'expressions') and fk_constraint.expressions:
            for col_expr in fk_constraint.expressions:
                if isinstance(col_expr, exp.Column):
                    foreign_keys.add(col_expr.name)
                elif isinstance(col_expr, exp.Identifier):
                    foreign_keys.add(col_expr.name)
                elif hasattr(col_expr, 'this'):
                    if isinstance(col_expr.this, str):
                        foreign_keys.add(col_expr.this)
                    elif hasattr(col_expr.this, 'this'):
                        foreign_keys.add(col_expr.this.this)
    
    return foreign_keys


def format_metadata_output(metadata: Dict) -> str:
    """
    格式化输出元数据信息（可选的辅助函数，用于友好展示）
    
    Args:
        metadata: extract_sql_metadata返回的元数据字典
    
    Returns:
        格式化的字符串
    """
    import json
    
    output = []
    output.append("=" * 60)
    output.append("SQL语句元数据提取结果")
    output.append("=" * 60)
    
    output.append("\n【目标表】")
    output.append(json.dumps(metadata["target_table"], ensure_ascii=False, indent=2))
    
    output.append("\n【目标表字段】")
    output.append(json.dumps(metadata["target_columns"], ensure_ascii=False, indent=2))
    
    if "source_tables" in metadata:
        output.append("\n【来源表清单】")
        output.append(json.dumps(metadata["source_tables"], ensure_ascii=False, indent=2))
    
    output.append("=" * 60)
    
    return "\n".join(output)


# 示例使用
if __name__ == "__main__":
    import json
    
    # 测试用例1：INSERT语句
    print("=" * 70)
    print("示例1：INSERT语句")
    print("=" * 70)
    insert_sql = """
    INSERT INTO schema1.target_table (
        id,          /* 编号 */
        name,        /* 名称 */
        age          /* 年龄 */
    )
    SELECT 
        t1.id,
        t1.name,
        t2.age
    FROM source_table1 t1
    INNER JOIN source_table2 t2 ON t1.id = t2.id
    WHERE t1.status = 'active'
    """
    
    try:
        result = extract_sql_metadata(insert_sql)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except Exception as e:
        print(f"错误: {e}")
    
    # 测试用例2：UPDATE语句（传统语法）
    print("\n" + "=" * 70)
    print("示例2：UPDATE语句（传统语法）")
    print("=" * 70)
    update_sql = """
    UPDATE employees
    SET 
        salary = salary * 1.1,  /* 工资 */
        updated_at = CURRENT_TIMESTAMP  /* 更新时间 */
    WHERE department_id = 100
    """
    
    try:
        result = extract_sql_metadata(update_sql)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except Exception as e:
        print(f"错误: {e}")
    
    # 测试用例2b：UPDATE语句（带别名映射）
    print("\n" + "=" * 70)
    print("示例2b：UPDATE语句（带别名映射 - SQL Server/Teradata风格）")
    print("=" * 70)
    update_sql_alias = """
    UPDATE t
    FROM schema1.employees t
    SET 
        t.salary = t.salary * 1.1,  /* 工资 */
        t.updated_at = CURRENT_TIMESTAMP  /* 更新时间 */
    WHERE t.department_id = 100
    """
    
    try:
        result = extract_sql_metadata(update_sql_alias, dialect='tsql')
        print(json.dumps(result, ensure_ascii=False, indent=2))
        print("注：成功将别名't'映射到真实表名'schema1.employees'")
    except Exception as e:
        print(f"错误: {e}")
    
    # 测试用例3：MERGE语句
    print("\n" + "=" * 70)
    print("示例3：MERGE语句")
    print("=" * 70)
    merge_sql = """
    MERGE INTO target_customers t
    USING source_customers s
    ON t.customer_id = s.customer_id
    WHEN MATCHED THEN
        UPDATE SET 
            name = s.name,  /* 客户名称 */
            email = s.email  /* 电子邮件 */
    WHEN NOT MATCHED THEN
        INSERT (customer_id, name, email)
        VALUES (s.customer_id, s.name, s.email)
    """
    
    try:
        result = extract_sql_metadata(merge_sql)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except Exception as e:
        print(f"错误: {e}")
    
    # 测试用例4：CREATE TABLE AS语句
    print("\n" + "=" * 70)
    print("示例4：CREATE TABLE AS语句")
    print("=" * 70)
    create_sql = """
    CREATE TABLE summary_table AS
    SELECT 
        dept_id,  /* 部门ID */
        COUNT(*) as emp_count,  /* 员工数量 */
        AVG(salary) as avg_salary  /* 平均工资 */
    FROM employees
    GROUP BY dept_id
    """
    
    try:
        result = extract_sql_metadata(create_sql)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except Exception as e:
        print(f"错误: {e}")
    
    # 测试用例5：DDL CREATE TABLE语句（MySQL示例）
    print("\n" + "=" * 70)
    print("示例5：DDL CREATE TABLE语句（MySQL）")
    print("=" * 70)
    ddl_sql_mysql = """
    CREATE TABLE db_schema.users (
        user_id INT NOT NULL AUTO_INCREMENT COMMENT '用户ID',
        username VARCHAR(50) NOT NULL COMMENT '用户名',
        email VARCHAR(100) COMMENT '电子邮件',
        age INT DEFAULT 0 COMMENT '年龄',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        PRIMARY KEY (user_id)
    ) COMMENT='用户表';
    """
    
    try:
        result = extract_ddl_metadata(ddl_sql_mysql, dialect='mysql')
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except Exception as e:
        print(f"错误: {e}")
    
    # 测试用例6：DDL CREATE TABLE语句（Teradata示例）
    print("\n" + "=" * 70)
    print("示例6：DDL CREATE TABLE语句（Teradata）")
    print("=" * 70)
    ddl_sql_teradata = """
    CREATE MULTISET VOLATILE TABLE VT_2_65536, NO LOG (
        CAMP_ID VARCHAR(60) NOT NULL  CASESPECIFIC          /* 营销活动编号 */
       ,ACTIV_INST_NUM VARCHAR(200) NOT NULL  CASESPECIFIC          /* 活动实例编号 */
       ,STEP_NUM VARCHAR(60) NOT NULL  CASESPECIFIC          /* 步骤编号 */
       ,QLFY_NUM VARCHAR(30) NOT NULL  CASESPECIFIC          /* 达标编号 */
       ,HOST_CUST_ID VARCHAR(30) NOT NULL  CASESPECIFIC          /* 核心客户号 */
       ,START_DATE DATE NOT NULL  FORMAT'YYYYMMDD'          /* 商机开始日期 */
    )
    PRIMARY INDEX
    (
    CAMP_ID, ACTIV_INST_NUM, STEP_NUM, QLFY_NUM, HOST_CUST_ID
    ) ON COMMIT PRESERVE ROWS;
    """
    
    try:
        result = extract_ddl_metadata(ddl_sql_teradata, dialect='teradata')
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except Exception as e:
        print(f"错误: {e}")
    
    # 测试用例7：DDL CREATE TABLE语句（带外键）
    print("\n" + "=" * 70)
    print("示例7：DDL CREATE TABLE语句（带外键）")
    print("=" * 70)
    ddl_sql_fk = """
    CREATE TABLE orders (
        order_id INT PRIMARY KEY COMMENT '订单ID',
        user_id INT NOT NULL COMMENT '用户ID',
        product_name VARCHAR(100) COMMENT '产品名称',
        quantity INT DEFAULT 1 COMMENT '数量',
        total_price DECIMAL(10,2) COMMENT '总价',
        FOREIGN KEY (user_id) REFERENCES users(user_id)
    ) COMMENT='订单表';
    """
    
    try:
        result = extract_ddl_metadata(ddl_sql_fk, dialect='mysql')
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except Exception as e:
        print(f"错误: {e}")

