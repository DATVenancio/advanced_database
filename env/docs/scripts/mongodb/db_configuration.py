import sqlite3
from pymongo import MongoClient

# sqlite
sqlite_conn = sqlite3.connect('database.db')
sqlite_cursor = sqlite_conn.cursor()

# mongo
mongo_client = MongoClient('localhost', 27017)
mongo_db = mongo_client['advanced_database']



def transfer_data_sqlite_to_mongo(sqlite_table_name, mongo_collection, column_names):
    #creating mongo collections
    sqlite_cursor.execute(f"SELECT * FROM {sqlite_table_name}")
    data = sqlite_cursor.fetchall()
    for row in data:
        doc = {}
        for i, col_name in enumerate(column_names):
            doc[col_name] = row[i]
        mongo_collection.insert_one(doc)
    print(f"Collection {sqlite_table_name} créée dans MongoDB")

#create a table with the names of sqlite tables
query = "SELECT name FROM sqlite_master WHERE type = 'table';"
sqlite_cursor.execute(query)
data = sqlite_cursor.fetchall()
tables=[]
for row in data:
    tables.append(row[0])

#for each table name, get the columns names and call transfer_data_sqlite_to_mongo
for table_name in tables:
    query = f"PRAGMA table_info('{table_name}')"
    sqlite_cursor.execute(query)
    data = sqlite_cursor.fetchall()
    column_names=[]
    for row in data:
        column_names.append(row[1])
    transfer_data_sqlite_to_mongo(table_name, mongo_db[table_name], column_names)




#nouvelle collection 
new_collection = mongo_db['nouvelle_collection']

