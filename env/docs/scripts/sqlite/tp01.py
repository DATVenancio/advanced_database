import sqlite3
import time
db = sqlite3.connect('database.db')
db_cursor = db.cursor()

#q01 - Dans quels films a joué Jean Reno ?

start_time=time.time()
name="Jean Reno"
query="""
SELECT DISTINCT primaryTitle FROM movies
JOIN characters ON movies.mid = characters.mid
JOIN persons ON characters.pid = persons.pid
WHERE primaryName = 'Jean Reno'
"""
db_cursor.execute(query)
result = db_cursor.fetchall()
end_time=time.time()
print("QUESTION 01 :")
for response in result:
    print(response[0])

print("Time: ", end_time-start_time)
print("_________________________")





#q02 - Quels sont les trois meilleurs films d’horreur des années 2000 au sens de la note moyenne par les utilisateurs ?
print("QUESTION 02 :")
start_time=time.time()
query="""
SELECT primaryTitle FROM movies
JOIN ratings ON movies.mid = ratings.mid
JOIN genres on movies.mid = genres.mid
WHERE  genre == 'Horror'AND startYear  BETWEEN 2000 AND 2009
ORDER BY  averageRating DESC LIMIT 3
"""
db_cursor.execute(query)
result = db_cursor.fetchall()
end_time=time.time()
for response in result:
    print(response[0])
print("TIME: ",end_time-start_time)

print("_________________________")

#q03 - Quels sont les scénaristes qui ont écrit des films jamais diffusés en Espagne (région ES dans la table titles) ?
print("QUESTION 03 :")

query="""
SELECT DISTINCT primaryName FROM persons
JOIN writers ON writers.pid = persons.pid
JOIN titles ON titles.mid = writers.mid
WHERE titles.mid NOT in(
    SELECT mid
    FROM titles
    WHERE region = 'ES'
)
"""
db_cursor.execute(query)
result = db_cursor.fetchall()
for response in result:
    print(response[0])


print("_________________________")

#q04 - Quels acteurs ont joué le plus de rôles différents dans un même film ?
print("QUESTION 04 :")

query="""
SELECT persons.primaryName, COUNT(DISTINCT characters.name) AS amount_roles
FROM characters
JOIN persons ON characters.pid = persons.pid
GROUP BY characters.pid, characters.mid
ORDER BY amount_roles DESC
LIMIT 3;
"""

db_cursor.execute(query)
result = db_cursor.fetchall()
for response in result:
    print(response[0])