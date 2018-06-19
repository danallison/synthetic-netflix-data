import os
import logging
from project_helpers import n4j_driver as n4j
try:
    from dask import delayed
except ImportError:
    # dask is only needed for the parallelized functions,
    # so we allow the use of the other functions without dask.
    dask = None

def find_neighbors(session, user_id, k=10):
    '''
    '''
    neighbors = session.run(
        '''
        MATCH (u:RealUser {user_id: $user_id})-[r1:rated]->(m:Movie),
              (otherUsers:RealUser)-[r2:rated]->(m)
        RETURN otherUsers.user_id AS user_id,
               sqrt(sum((r1.rating - r2.rating)^2)) AS distance,
               count(r2) AS common_movies
        ORDER BY common_movies DESC, distance
        LIMIT $k
        ''',
        user_id=user_id, k=k
    )
    return neighbors

def find_and_process_neighbors(user_ids, file_path, logger=None):
    '''
    '''
    with n4j.session() as session:
        user_id_count = len(user_ids)
        for i, user_id in enumerate(user_ids):
            if logger:
                logger.info('processing user {} of {}. id = {}'.format(i + 1, user_id_count, user_id))
            neighbors = find_neighbors(session, user_id)
            with open(file_path, 'a') as file:
                file.write(''.join([
                    '{},{},{},{}\n'.format(
                        user_id,
                        n['user_id'],
                        n['common_movies'],
                        n['distance']
                    ) for n in neighbors
                ]))
        if logger:
            logger.info('completed processing neighbors for {} users'.format(user_id_count))

def process_neighbors_in_parallel(user_ids, file_dir='.', index_offset=0, logger=None):
    # Assuming we're running on Linux
    cpu_cores = os.sysconf("SC_NPROCESSORS_ONLN")
    chunk_count = min(cpu_cores, len(user_ids))
    chunk_size = len(user_ids) // chunk_count
    processes = []
    for i in range(chunk_count):
        start = i * chunk_size
        if i == (chunk_count - 1):
            # Make sure we get all ids at the end
            end = len(user_ids)
        else:
            end = (i + 1) * chunk_size
        chunk = user_ids[start:end]
        file_path = '{}/neighbors_{}-{}.csv'.format(file_dir, index_offset + start, index_offset + end)
        if logger:
            process_logger = logging.getLogger('find_nearest_neighbors.py:{}-{}'.format(index_offset + start, index_offset + end))
        processes.append(
            delayed(find_and_process_neighbors)(chunk, file_path, process_logger)
        )
    if logger:
        logger.info('starting {} parallel processes.'.format(chunk_count))
    delayed(lambda x: x)(processes).compute()
