import unittest
from project_helpers import n4j_driver

class TestN4jDriver(unittest.TestCase):
    def test_creates_session(self):
        with n4j_driver.session() as session:
            result = session.run('MATCH (n) RETURN count(n)').single()[0]
            self.assertEqual(type(result), int)

if __name__ == '__main__':
    unittest.main()
