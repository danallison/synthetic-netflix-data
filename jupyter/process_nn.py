import os
import logging
from time import sleep, time

logging.basicConfig(filename='process_nn.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('process_nn.py')

# NOTE This is a hack to avoid connection errors.
logger.info('sleeping while neo4j warms up ...')
sleep(10)

from project_helpers import n4j_driver as n4j
from find_nearest_neighbors import process_neighbors_in_parallel

def process_nn():
    start = int(os.environ['START_INDEX'])
    stop = int(os.environ['STOP_INDEX'])
    limit = stop - start
    with n4j.session() as session:
        user_ids = session.run(
            '''
            MATCH (u:RealUser)
            WITH u.user_id AS user_id
            ORDER BY user_id
            SKIP $start LIMIT $limit
            RETURN collect(user_id)
            ''',
            start=start, limit=limit
        ).single().value()
    start_time = time()
    logger.info('starting process for {} users from index {} to {}.'.format(len(user_ids), start, stop))
    fnn_logger = logging.getLogger('find_nearest_neighbors.py')
    # ===================================================================== #
    process_neighbors_in_parallel(user_ids, './neighbors', start, fnn_logger)
    # ===================================================================== #
    total_seconds = round(time() - start_time)
    total_minutes = total_seconds // 60
    hours = total_minutes // 60
    seconds = total_seconds % 60
    minutes = total_minutes % 60
    logger.info('done after {} hours, {} minutes, and {} seconds.'.format(hours, minutes, seconds))

if __name__ == '__main__':
    process_nn()
