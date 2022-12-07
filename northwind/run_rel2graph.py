import os
import time

NEO4J_CMD = "/Users/julian/Library/Application Support/Neo4j Desktop/Application/relate-data/dbmss/dbms-bb6ae047-728a-4147-a14f-62a1af6fbd54/bin/cypher-shell"


RUNS = 10 # number of times to run the query
total_seconds = 0
for i in range(RUNS):
    print("Deleting database")
    os.system(f'"{NEO4J_CMD}" -u "neo4j" -p "password" "MATCH (n) DETACH DELETE n"')
    print("Importing data")
    start = time.time()
    os.system(f'python northwind/import_rel2graph.py > /dev/null')
    end = time.time()
    print(f"Import took {end - start} seconds")
    total_seconds += end - start

print(f"Average import time: {total_seconds / RUNS} seconds")