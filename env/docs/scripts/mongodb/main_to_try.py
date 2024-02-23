import sqlite3
sqlite_conn = sqlite3.connect('database.db')
sqlite_cursor = sqlite_conn.cursor()
table_name="movies"


query = "SELECT name FROM sqlite_master WHERE type = 'table';"
sqlite_cursor.execute(query)
data = sqlite_cursor.fetchall()

tables=[]
for row in data:
    tables.append(row[0])

for table_name in tables:
    query = f"PRAGMA table_info('{table_name}')"
    sqlite_cursor.execute(query)
    data = sqlite_cursor.fetchall()
    column_names=[]
    for row in data:
        column_names.append(row[1])
    transfer_data_sqlite_to_mongo(table_name, mongo_db[table_name], column_names)



