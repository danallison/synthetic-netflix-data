import os
from neo4j.v1 import GraphDatabase

n4j_driver = GraphDatabase.driver('bolt://neo4j:7687', auth=('neo4j', os.environ['NEO4J_AUTH'].split('/')[1]))
