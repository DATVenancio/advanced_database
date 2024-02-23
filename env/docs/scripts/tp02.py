import sqlite3
from pymongo import MongoClient

# Connexion à SQLite
sqlite_conn = sqlite3.connect('database.db')
sqlite_cursor = sqlite_conn.cursor()

# Connexion à MongoDB
mongo_client = MongoClient('localhost', 27017)
mongo_db = mongo_client['advanced_database']

# Création des collections dans MongoDB
def transfer_data_sqlite_to_mongo(sqlite_table_name, mongo_collection, column_names):

    # Récupération des données de SQLite
    sqlite_cursor.execute(f"SELECT * FROM {sqlite_table_name}")

    data = sqlite_cursor.fetchall()

    # Insertion des données dans MongoDB
    for row in data:
        
        doc = {}
        for i, col_name in enumerate(column_names):
            doc[col_name] = row[i]

        mongo_collection.insert_one(doc)


    
    print(f"Collection {sqlite_table_name} créée dans MongoDB")


#création des collections dans MongoDB


#movies
movies_columns = ['mid', 'titleType', 'primaryTitle', 'originalTitle', 'isAdult', 'startYear', 'endYear', 'runtimeMinutes']

transfer_data_sqlite_to_mongo('movies', mongo_db['movies'], movies_columns)

#genres
genres_columns = ['mid', 'genre']

transfer_data_sqlite_to_mongo('genres', mongo_db['genres'], genres_columns)

#knownformovies
knownformovies_columns = ['pid', 'mid']

transfer_data_sqlite_to_mongo('knownformovies', mongo_db['knownformovies'], knownformovies_columns)

#principals
principals_columns = ['mid', 'ordering', 'pid', 'category', 'job']

transfer_data_sqlite_to_mongo('principals', mongo_db['principals'], principals_columns)

#professions
professions_columns = ['pid', 'jobName']

transfer_data_sqlite_to_mongo('professions', mongo_db['professions'], professions_columns)

#ratings
ratings_columns = ['mid', 'averageRating', 'numVotes']

transfer_data_sqlite_to_mongo('ratings', mongo_db['ratings'], ratings_columns)

#titles
titles_columns = ['mid', 'ordering', 'title', 'region', 'language', 'types', 'attributes', 'isOriginalTitle']

transfer_data_sqlite_to_mongo('titles', mongo_db['titles'], titles_columns)

#writers
writers_columns = ['mid', 'pid']

transfer_data_sqlite_to_mongo('writers', mongo_db['writers'], writers_columns)

#characters
characters_columns = ['mid', 'pid', 'name']

transfer_data_sqlite_to_mongo('characters', mongo_db['characters'], characters_columns)



"""directors,persons"""



#nouvelle collection 
new_collection = mongo_db['nouvelle_collection']

