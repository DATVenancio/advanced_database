import sqlite3
db = sqlite3.connect('database.db')
db_cursor = db.cursor()
result = db_cursor.execute("SELECT * FROM movies")

for line in result:
    print(line)