import csv
import sqlite3
import os

comando = "pwd"

saida = os.popen(comando).read()

print(saida)

# Connect / create db
db = sqlite3.connect('database.db')
db_cursor = db.cursor()



def create_tables():
    db_cursor.execute("DROP TABLE IF EXISTS movies;")

    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS movies(
        mid VARCHAR PRIMARY KEY,
        titleType VARCHAR,
        primaryTitle VARCHAR,
        originalTitle VARCHAR,
        isAdult INTEGER CHECK (isAdult IN (0, 1)),
        startYear VARCHAR,
        endYear VARCHAR,
        runtimeMinutes REAL
    );
    ''')
    db_cursor.execute("DROP TABLE IF EXISTS genres;")
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS genres(
        mid VARCHAR SECONDARY KEY,
        genre VARCHAR,
        FOREIGN KEY (mid) REFERENCES movies(mid)
    );
    ''')

    db_cursor.execute("DROP TABLE IF EXISTS persons;")
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS persons(
        pid VARCHAR PRIMARY KEY,
        primaryName VARCHAR,
        birthYear VARCHAR,
        deathYear VARCHAR
    );
    ''')

    db_cursor.execute("DROP TABLE IF EXISTS knownformovies;")
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS knownformovies   (
        pid VARCHAR SECONDARY KEY,
        mid VARCHAR SECONDARY KEY,
        FOREIGN KEY (pid) REFERENCES movies(pid),
        FOREIGN KEY (mid) REFERENCES persons(mid)
    );
    ''')
    
    db_cursor.execute("DROP TABLE IF EXISTS principals;")
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS principals   (
        mid VARCHAR SECONDARY KEY,
        ordering INTEGER,
        pid VARCHAR SECONDARY KEY,
        category VARCHAR,
        job VARCHAR,
        FOREIGN KEY (pid) REFERENCES persons(pid),
        FOREIGN KEY (mid) REFERENCES movies(mid)
    );
    ''')


    db_cursor.execute("DROP TABLE IF EXISTS professions;")
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS professions   (
        pid VARCHAR SECONDARY KEY,
        jobName VARCHAR,
        FOREIGN KEY (pid) REFERENCES persons(pid)
    );
    ''')

    db_cursor.execute("DROP TABLE IF EXISTS professions;")
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS professions   (
        pid VARCHAR SECONDARY KEY,
        jobName VARCHAR,
        FOREIGN KEY (pid) REFERENCES persons(pid)
    );
    ''')

    db_cursor.execute("DROP TABLE IF EXISTS ratings;")
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS ratings   (
        mid VARCHAR SECONDARY KEY,
        averageRating REAL,
        numVotes INTEGER,
        FOREIGN KEY (mid) REFERENCES movies(mid)
    );
    ''')

    db_cursor.execute("DROP TABLE IF EXISTS ratings;")
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS ratings   (
        mid VARCHAR SECONDARY KEY,
        averageRating REAL,
        numVotes INTEGER,
        FOREIGN KEY (mid) REFERENCES movies(mid)
    );
    ''')

    db_cursor.execute("DROP TABLE IF EXISTS titles;")
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS titles   (
        mid VARCHAR SECONDARY KEY,
        ordering INTEGER,
        title VARCHAR,
        region VARCHAR,
        language VARCHAR,
        types VARCHAR,
        attributes VARCHAR,
        isOriginalTitle INTEGER CHECK (isOriginalTitle IN (0, 1)),           
        FOREIGN KEY (mid) REFERENCES movies(mid)
    );
    ''')

    db_cursor.execute("DROP TABLE IF EXISTS writers;")
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS writers   (
        mid VARCHAR SECONDARY KEY,
        pid VARCHAR SECONDARY KEY,          
        FOREIGN KEY (mid) REFERENCES movies(mid)
        FOREIGN KEY (pid) REFERENCES persons(pid)
        
    );
    ''')

    db_cursor.execute("DROP TABLE IF EXISTS characters;")
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS characters   (
        mid VARCHAR SECONDARY KEY,
        pid VARCHAR SECONDARY KEY,
        name VARCHAR,          
        FOREIGN KEY (mid) REFERENCES movies(mid),
        FOREIGN KEY (pid) REFERENCES persons(pid),
        PRIMARY KEY(mid,pid,name)
        
    );
    ''')
  




def insert_data():
    #movies
    with open("../imdb-tiny/movies.csv", 'r', newline='') as file:
        reader_csv = csv.reader(file)
        next(reader_csv)  #jump first line
        for line in reader_csv:
            #print(line)
            mid,titleType,primaryTitle,originalTitle,isAdult,startYear,endYear,runtimeMinutes= line
            db_cursor.execute('''INSERT OR REPLACE INTO movies (mid, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes) 
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', 
                           (mid,titleType,primaryTitle,originalTitle,isAdult,startYear,endYear,runtimeMinutes))
    
    #persons
    with open("../imdb-tiny/persons.csv", 'r', newline='') as file:
        reader_csv = csv.reader(file)
        next(reader_csv)  #jump first line
        for line in reader_csv:
            #print(line)
            pid,primaryName,birthYear,deathYear = line
            db_cursor.execute('''INSERT OR REPLACE INTO persons (pid,primaryName,birthYear,deathYear) 
                              VALUES (?, ?, ?, ?)''', 
                           (pid,primaryName,birthYear,deathYear))
    #genre
    with open("../imdb-tiny/genres.csv", 'r', newline='') as file:
        reader_csv = csv.reader(file)
        next(reader_csv)  #jump first line
        for line in reader_csv:
            #print(line)
            mid,genre = line
            db_cursor.execute('''INSERT OR REPLACE INTO genres (mid,genre) 
                              VALUES (?, ?)''', 
                           (mid,genre))
    
    #knownfor
    with open("../imdb-tiny/knownformovies.csv", 'r', newline='') as file:
        reader_csv = csv.reader(file)
        next(reader_csv)  #jump first line
        for line in reader_csv:
            #print(line)
            pid,mid = line
            db_cursor.execute('''INSERT OR REPLACE INTO knownformovies (pid,mid) 
                              VALUES (?, ?)''', 
                           (pid,mid))
    
    #principals
    with open("../imdb-tiny/principals.csv", 'r', newline='') as file:
        reader_csv = csv.reader(file)
        next(reader_csv)  #jump first line
        for line in reader_csv:
            #print(line)
            mid,ordering,pid,category,job = line
            db_cursor.execute('''INSERT OR REPLACE INTO principals (mid,ordering,pid,category,job) 
                              VALUES (?, ?, ?, ?, ?)''', 
                           (mid,ordering,pid,category,job))
            
    #professions
    with open("../imdb-tiny/professions.csv", 'r', newline='') as file:
        reader_csv = csv.reader(file)
        next(reader_csv)  #jump first line
        for line in reader_csv:
            #print(line)
            pid,jobName = line
            db_cursor.execute('''INSERT OR REPLACE INTO professions (pid,jobName) 
                              VALUES (?, ?)''', 
                           (pid,jobName))
    
    #ratings
    with open("../imdb-tiny/ratings.csv", 'r', newline='') as file:
        reader_csv = csv.reader(file)
        next(reader_csv)  #jump first line
        for line in reader_csv:
            #print(line)
            mid,averageRating,numVotes = line
            db_cursor.execute('''INSERT OR REPLACE INTO ratings (mid,averageRating,numVotes) 
                              VALUES (?, ?,?)''', 
                           (mid,averageRating,numVotes))
    #titles
    with open("../imdb-tiny/titles.csv", 'r', newline='') as file:
        reader_csv = csv.reader(file)
        next(reader_csv)  #jump first line
        for line in reader_csv:
            #print(line)
            mid,ordering,title,region,language,types,attributes,isOriginalTitle= line
            db_cursor.execute('''INSERT OR REPLACE INTO titles (mid,ordering,title,region,language,types,attributes,isOriginalTitle) 
                              VALUES (?, ?,?,?,?,?,?,?)''', 
                           (mid,ordering,title,region,language,types,attributes,isOriginalTitle))
    
    #writers
    with open("../imdb-tiny/writers.csv", 'r', newline='') as file:
        reader_csv = csv.reader(file)
        next(reader_csv)  #jump first line
        for line in reader_csv:
            #print(line)
            mid,pid= line
            db_cursor.execute('''INSERT OR REPLACE INTO writers (mid,pid) 
                              VALUES (?, ?)''', 
                           (mid,pid))
    
    #characters
    with open("../imdb-tiny/characters.csv", 'r', newline='') as file:
        reader_csv = csv.reader(file)
        next(reader_csv)  #jump first line
        for line in reader_csv:
            #print(line)
            mid,pid,name= line
            db_cursor.execute('''INSERT OR REPLACE INTO characters (mid,pid,name) 
                              VALUES (?, ?,?)''', 
                           (mid,pid,name))
            
table_names=["movies"]
def insert_data_auto():
    for name in table_names:
        with open("../imdb-tiny/"+name+".csv", 'r', newline='') as file:
            reader_csv = csv.reader(file)
            first_line=next(reader_csv)[0]  #jump first line
            print(first_line)
            """
            for line in reader_csv:
                #print(line)
                mid,titleType,primaryTitle,originalTitle,isAdult,startYear,endYear,runtimeMinutes= line
                #db_cursor.execute("INSERT INTO movies (originalTitle) VALUES (?)",("897"))


                db_cursor.execute('''INSERT OR REPLACE INTO movies (mid, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', 
                            (mid,titleType,primaryTitle,originalTitle,isAdult,startYear,endYear,runtimeMinutes))
            """
            
create_tables()
insert_data()

db.commit()
db.close()
