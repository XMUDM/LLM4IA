import os
import json
from PostgreSQL import PGHypo as PG

if __name__ == '__main__':
    # load config file
    config_db = json.load(open(os.getcwd() + "/config_db.json"))
    db_connector = PG(config_db)
    print("connect successful!")
    tables = db_connector.get_tables()
    table_num = len(tables)
    column_num = 0
    tables_rows = dict()
    table_columns = dict()
    for table in tables:
        tables_rows[table] = db_connector.get_table_rows(table)
        columns = db_connector.get_columns(table)
        table_columns[table] = columns
        column_num += len(columns)
    print(tables_rows)
    print(table_columns)
    print(f"table number is {table_num}")
    print(f"column number is {column_num}")