'''
NOTE: These helper functions should only be used once to get everything initialized.
'''
import os
from urllib.request import urlretrieve
from zipfile import ZipFile
from neo4j.v1 import GraphDatabase

netflix_data_url = 'https://www.kaggle.com/netflix-inc/netflix-prize-data/downloads/netflix-prize-data.zip/1'
data_dir = 'data'
rating_files = ['{}/combined_data_{}.txt'.format(data_dir, i) for i in range(1, 5)]
n4j_driver = GraphDatabase.driver('bolt://neo4j:7687', auth=('neo4j', os.environ['NEO4J_AUTH'].split('/')[1]))

# NOTE Cannot download from kaggle without logging in.
# This step must be done manually through a browser.
# def download_netflix_data():
#     path = '{}/netflix-prize-data.zip'.format(data_dir)
#     print('downloading netflix data ...')
#     urlretrieve(netflix_data_url, filename=path)
#     print('done.')

def unzip_netflix_data():
    path = '{}/netflix-prize-data.zip'.format(data_dir)
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
    with open('{}/movie_titles.csv'.format(data_dir), 'r', encoding='ISO-8859-1') as file:
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
                    print('processing movies {} to {}'.format(movie_id, movie_id + 999), end='\r')
                    tx = session.begin_transaction()
                create_movie(tx, movie_id, year, title)
            tx.commit()
    print('\ndone.')

def create_real_users():
    created_users = set()
    def create_real_user(tx, user_id):
        if user_id in created_users:
            return False
        else:
            tx.run(
                'CREATE (u:RealUser {user_id: $user_id}) ',
                user_id=user_id
            )
            created_users.add(user_id)
            return True
    with n4j_driver.session() as session:
        i = 1
        tx = None
        for file_path in rating_files:
            with open(file_path, 'r') as file:
                for line in file:
                    line = line.strip()
                    if line[-1] == ':':
                        pass
                    else:
                        values = line.split(',')
                        user_id = int(values[0])
                        if (i % 1000) == 0:
                            tx.commit()
                            tx = None
                        if tx is None:
                            print('processing users {} to {}'.format(i, i + 999), end='\r')
                            tx = session.begin_transaction()
                        if create_real_user(tx, user_id):
                            i += 1
        tx.commit()
    print('\ndone.')

def create_ratings(starting_movie_id=1):
    def add_ratings(session, file):
        # This is not the most efficient way to import the data.
        # Room for improvement here.
        tx = None
        tx_size = 0
        for line in file:
            line = line.strip()
            if line[-1] == ':':
                movie_id = int(line[:-1])
                if movie_id >= starting_movie_id:
                    print('processing movie {}'.format(movie_id), end='\r')
                    if tx: tx.commit()
                    tx = session.begin_transaction()
            elif movie_id >= starting_movie_id:
                values = line.split(',')
                user_id = int(values[0])
                rating = int(values[1])
                date = values[2]
                if tx_size > 999:
                    tx.commit()
                    tx = session.begin_transaction()
                    tx_size = 0
                tx.run(
                    'MATCH (movie:Movie) WHERE movie.movie_id = $movie_id '
                    'MATCH (user:RealUser) WHERE user.user_id = $user_id '
                    'CREATE (user)-[:rated {rating: $rating, date: $date}]->(movie) ',
                    movie_id=movie_id, user_id=user_id, rating=rating, date=date
                )
                tx_size += 1
        if tx: tx.commit()
    with n4j_driver.session() as session:
        for file_path in rating_files:
            with open(file_path, 'r') as file:
                add_ratings(session, file)
    print('\ndone.')
