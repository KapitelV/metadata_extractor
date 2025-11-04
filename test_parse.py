from sqlglot import parse,parse_one
from utils import read_sql_file
import os,json

dialect="teradata"

# # 只处理badcases
# with open('bad_cases.json', 'r', encoding='utf-8') as f:
#     sql_files = json.load(f)
# unfixed_bad_cases=[]
# for sql_file in sql_files:
#     with open(sql_file, 'r', encoding='utf-8') as f:
#         sql_content = f.read()
#     try:
#         parsed_statements = parse(sql_content, dialect=dialect)
#     except Exception as e:
#         # if "XMLAGG" in sql_content:
#         #     pass
#         # else:
#         unfixed_bad_cases.append(sql_file)    
# with open("unfixed_bad_cases.json", 'w', encoding='utf-8') as f:
#     f.write(json.dumps(unfixed_bad_cases, ensure_ascii=False))      

# 处理目录下全部文件
directory_path = "C:\\pyworks\\Datasets\\SQLs\\DML\\Teradata\\minsheng\\MDB_TD\\sqls"
# sql_path='.'

sql_files = []
for root, dirs, files in os.walk(directory_path):
    for file in files:
        if file.lower().endswith('.sql'):
            sql_files.append(os.path.join(root, file))

if not sql_files:
    print("  ⚠️  未找到任何SQL文件")


bad_cases=[]
for idx, sql_file in enumerate(sql_files, 1):
    with open(sql_file, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    try:
        parsed_statements = parse(sql_content, dialect=dialect)
    except Exception as e:
        bad_cases.append(sql_file)

with open("bad_cases.json", 'w', encoding='utf-8') as f:
    f.write(json.dumps(bad_cases, ensure_ascii=False))       
    
sql= read_sql_file('test.sql')
parsed = parse(sql,dialect='teradata')
print(1)