import unittest
from setup_helpers import *

class TestSetupFunctions(unittest.TestCase):
    def test_functions_exist(self):
        # NOTE These functions modify the database, so we are only
        # testing that the functions exist, without actually calling them.
        self.assertTrue(callable(unzip_netflix_data))
        self.assertTrue(callable(init_db))
        self.assertTrue(callable(create_movies))
        self.assertTrue(callable(create_real_users))
        self.assertTrue(callable(create_ratings))
        self.assertTrue(callable(create_ratings_csv))

if __name__ == '__main__':
    unittest.main()
