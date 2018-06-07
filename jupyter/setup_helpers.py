'''
NOTE: These helper functions should only be used once to get everything initialized.
'''
import os
from urllib.request import urlretrieve
from zipfile import ZipFile
from neo4j.v1 import GraphDatabase

netflix_data_url = 'https://www.kaggle.com/netflix-inc/netflix-prize-data/downloads/netflix-prize-data.zip/1'
data_dir = 'data'
n4j_driver = GraphDatabase('bolt://neo4j:7687', auth=('neo4j', os.environ['NEO4J_AUTH']))

def download_and_unzip_netflix_data():
    print('downloading netflix data ...')
    path = '{}/netflix-prize-data.zip'.format(data_dir)
    urlretrieve(netflix_data_url, filename=path)
    with ZipFile(path, 'r') as zf:
        print('unzipping ...')
        zf.extractall(data_dir)
    print('done.')

def init_db():
    print('creating constraints ...')
    with n4j_driver.session() as session:
        session.run('CREATE CONSTRAINT ON (m:Movie) ASSERT m.movie_id IS UNIQUE')
        session.run('CREATE CONSTRAINT ON (ru:RealUser) ASSERT ru.user_id IS UNIQUE')
        session.run('CREATE CONSTRAINT ON (su:SyntheticUser) ASSERT su.user_id IS UNIQUE')
    print('done.')

def create_movies():
    def create_movie(tx, movie_id, year, title):
        tx.run(
            'CREATE (m:Movie {movie_id: $movie_id, year: $year, title: $title}) ',
            movie_id=movie_id, year=year, title=title
        )
    # It is necessary to manually parse the csv as it is malformed
    with open('data/movie_titles.csv', 'r', encoding='ISO-8859-1') as file:
        with n4j_driver.session() as session:
            tx = None
            for line in file:
                values = line.strip().split(',')
                movie_id = int(values[0])
                year = None if values[1] == 'NULL' else int(values[1])
                title = ','.join(values[2:])
                if (movie_id % 1000) == 0:
                    tx.commit()
                    tx = None
                if tx is None:
                    tx = session.begin_transaction()
                create_movie(tx, movie_id, year, title)
            tx.commit()

def create_ratings_and_real_users():
    def add_ratings(session, file):
        # This is not the most efficient way to import the data.
        # Room for improvement here.
        for line in file:
            line = line.strip()
            if line[-1] == ':':
                movie_id = int(line[:-1])
            else:
                values = line.split(',')
                user_id = int(values[0])
                rating = int(values[1])
                date = values[2]
                session.run(
                    'MATCH (movie:Movie) WHERE movie.movie_id = $movie_id '
                    'MERGE (user:RealUser {user_id: $user_id}) '
                    'MERGE (user)-[:rated {rating: $rating, date: $date}]->(movie) ',
                    movie_id=movie_id, user_id=user_id, rating=rating, date=date
                )
    rating_files = ['combined_data_{}.txt'.format(i) for i in range(1, 5)]
    with driver.session() as session:
        for file_path in rating_files:
            with open(file_path, 'r') as file:
                add_ratings(session, file)
