from sql_file_processor import process_sql_directory

sql_path = "C:\\pyworks\\Datasets\\SQLs\\DML\\Teradata\\minsheng\\PDB_TD\\sqls"

res=process_sql_directory(
    directory_path = sql_path,
    dialect = 'teradata',
    mode = 'clear',
    db_path = 'dw_metadata.db',
    lineage_json_path = 'datalineage.json',
    log_file = 'batch_process_log.txt'
)

