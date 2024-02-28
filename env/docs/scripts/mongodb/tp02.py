from pymongo import MongoClient
import time

"""
# Se connecter à MongoDB
client = MongoClient('localhost', 27017)
db = client['advanced_database']  
#setup
persons_collection = db['persons']
characters_collection = db['characters']
movies_collection = db['movies']
genres_collection = db['genres']

# q01 - Dans quels films a joué Jean Reno ?
start_time= time.time()
mids=[]
movies=[]

pid = persons_collection.find_one({"primaryName":"Jean Reno"})['pid']
characters = characters_collection.find({"pid":pid})
for character in  characters:
    mids.append(character["mid"])
for mid in mids:
    movies.append(movies_collection.find_one({"mid":mid})["primaryTitle"])
end_time=time.time()

print("QUESTION 01 :")
for movie in movies:
    print(movie)
print("Time: ", end_time-start_time)

"""
# Se connecter à MongoDB
client = MongoClient('localhost', 27017)
db = client['advanced_database']  
#setup
persons_collection = db['persons']
characters_collection = db['characters']
movies_collection = db['movies']
genres_collection = db['genres']


# q01 - Dans quels films a joué Jean Reno ?
# Étape 1 : Extraire le pid de Jean Reno de la table persons
jean_reno = persons_collection.find_one({"primaryName": "Jean Reno"}, {"pid": 1})
jean_reno_pid = jean_reno['pid']
print(jean_reno_pid)

# Étape 2 : Trouver les mids correspondants dans la table characters
jean_reno_characters = characters_collection.find({"pid": jean_reno_pid}, {"mid": 1})
jean_reno_mids = [character['mid'] for character in jean_reno_characters]
print(jean_reno_mids)

# Étape 3 : Chercher les noms des films correspondant à ces mids dans la table movies
movies_collection = db['movies']
jean_reno_films = movies_collection.find({"mid": {"$in": jean_reno_mids}}, {"primaryTitle": 1})

# Afficher les noms des films
print("Les films dans lesquels Jean Reno a joué :")
for film in jean_reno_films:
    print(film['primaryTitle'])



#q02 - Quels sont les trois meilleurs films d’horreur des années 2000 au sens de la note moyenne par les utilisateurs ?

start_time=time.time()

# Étape 1 : Récupérer les mids des films d'horreur
genres_collection = db['genres']
horror_movies = genres_collection.find({"genre": "Horror"}, {"mid": 1})

# Liste des mids
horror_mids = [movie['mid'] for movie in horror_movies]

# Étape 2 : Récupérer les primaryTitle, startYear et endYear correspondant à ces mids
movies_collection = db['movies']
horror_movies_info = movies_collection.find({"mid": {"$in": horror_mids}}, {"primaryTitle": 1, "startYear": 1, "endYear": 1})

# Étape 3 : Récupérer les 3 meilleurs films selon averageRatings
ratings_collection = db['ratings']
top_horror_movies = ratings_collection.find({"mid": {"$in": horror_mids}}).sort("averageRating", -1).limit(3)

# Afficher les titres des trois meilleurs films
movie_data=[]
for movie in top_horror_movies:
    movie_data.append(movies_collection.find_one({"mid": movie['mid']})["primaryTitle"])
end_time=time.time()

print("Les trois meilleurs films d'horreur des années 2000 selon la note moyenne des utilisateurs :")
for movie in movie_data:
    print(movie)

print("TIME: ",end_time-start_time)






""""
#q03 - Quels sont les scénaristes qui ont écrit des films jamais diffusés en Espagne (région ES dans la table titles) ?

# Étape 1 : Récupérer les mid des films jamais diffusés en Espagne
titles_collection = db['titles']
unreleased_movies = titles_collection.find({"region": {"$ne": "ES"}}, {"mid": 1})

# Liste des mids
unreleased_mids = [movie['mid'] for movie in unreleased_movies]

# Étape 2 : Récupérer les pid des scénaristes correspondant à ces mids
writers_collection = db['writers']
writers = writers_collection.find({"mid": {"$in": unreleased_mids}}, {"pid": 1})

# Liste des pids
writer_pids = [writer['pid'] for writer in writers]

# Étape 3 : Récupérer les noms des scénaristes
persons_collection = db['persons']
writers_names = persons_collection.find({"pid": {"$in": writer_pids}}, {"primaryName": 1})

# Afficher les noms des scénaristes
print("Les scénaristes qui ont écrit des films jamais diffusés en Espagne :")
for writer in writers_names:
    print(writer['primaryName'])


#q04 - Quels acteurs ont joué le plus de rôles différents dans un même film ?


#je suis pas sur de la requete !!!!!!




# Étape 1 : Trouver les acteurs qui ont joué plus d'un rôle dans un film
knownformovies_collection = db['knownformovies']
actors_with_multiple_roles = knownformovies_collection.aggregate([
    {"$group": {"_id": "$pid", "roles_count": {"$sum": 1}}},
    {"$match": {"roles_count": {"$gt": 1}}}
])

# Liste des pids des acteurs avec plusieurs rôles
actors_with_multiple_roles_pids = [actor['_id'] for actor in actors_with_multiple_roles]

# Étape 2 : Récupérer les noms des acteurs
persons_collection = db['persons']
print("Les acteurs ayant joué plus d'un rôle dans un film :")
for actor_pid in actors_with_multiple_roles_pids:
    actor_info = persons_collection.find_one({"pid": actor_pid}, {"primaryName": 1})
    print(actor_info['primaryName'])

"""

