import sqlite3
from pymongo import MongoClient

# Connexion à SQLite
sqlite_conn = sqlite3.connect('database.db')
sqlite_cursor = sqlite_conn.cursor()

# Connexion à MongoDB
client = MongoClient('localhost', 27017)
mongo_db = client['advanced_database']

# Fonction pour transférer les données d'une table SQLite vers une collection MongoDB
def transfer_data_sqlite_to_mongo(sqlite_table_name, mongo_collection):
    # Récupération des noms de colonnes de la table
    sqlite_cursor.execute(f"PRAGMA table_info({sqlite_table_name})")
    columns_info = sqlite_cursor.fetchall()
    column_names = [col[1] for col in columns_info]

    # Récupération des données de la table
    sqlite_cursor.execute(f"SELECT * FROM {sqlite_table_name}")
    data = sqlite_cursor.fetchall()

    # Insertion des données dans MongoDB
    for row in data:
        doc = dict(zip(column_names, row))
        mongo_collection.insert_one(doc)

    print(f"Données de la table {sqlite_table_name} transférées avec succès vers MongoDB")

# Récupération des noms de tables dans la base de données SQLite
sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [table[0] for table in sqlite_cursor.fetchall()]

# Transfert des données de chaque table vers MongoDB
for table_name in tables:
    transfer_data_sqlite_to_mongo(table_name, mongo_db[table_name])

# Création d'une nouvelle collection MongoDB
new_collection = mongo_db['nouvelle_collection']
print("Collection 'nouvelle_collection' créée dans MongoDB")
