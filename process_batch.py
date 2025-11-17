from sql_file_processor import process_sql_directory
from utils import save_results

res_all = []

sql_path = "/data/czt/dataset/ODB_TD/sqls"

res=process_sql_directory(
    directory_path = sql_path,
    dialect = 'teradata',
    mode = 'clear',
    db_path = 'dw_metadata.db',
    lineage_json_path = 'datalineage.json',
    log_file = 'batch_process_log.txt'
)
res_all.append(res)

sql_path = "/data/czt/dataset/CDB_TD/sqls"

res=process_sql_directory(
    directory_path = sql_path,
    dialect = 'teradata',
    mode = 'insert',
    db_path = 'dw_metadata.db',
    lineage_json_path = 'datalineage.json',
    log_file = 'batch_process_log.txt'
)
res_all.append(res)
sql_path = "/data/czt/dataset/PDB_TD/sqls"
res=process_sql_directory(
    directory_path = sql_path,
    dialect = 'teradata',
    mode = 'insert',
    db_path = 'dw_metadata.db',
    lineage_json_path = 'datalineage.json',
    log_file = 'batch_process_log.txt'
)
res_all.append(res)
sql_path = "/data/czt/dataset/MDB_TD/sqls"
res=process_sql_directory(
    directory_path = sql_path,
    dialect = 'teradata',
    mode = 'insert',
    db_path = 'dw_metadata.db',
    lineage_json_path = 'datalineage.json',
    log_file = 'batch_process_log.txt'
)
res_all.append(res)

save_results(res_all, 'batch_process_results.json')