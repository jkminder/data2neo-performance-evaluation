import os
import time
import shutil

NEO4J_HOME = "/Users/julian/Library/Application Support/Neo4j Desktop/Application/relate-data/dbmss/dbms-bb6ae047-728a-4147-a14f-62a1af6fbd54/"

files = ["categories", "employees", "orders", "products", "suppliers"]
print("Copying files")
for file in files:
    shutil.copy(f"data/northwind/{file}.csv", f"{NEO4J_HOME}/import/{file}.csv")


RUNS = 10 # number of times to run the query
total_seconds = 0
for i in range(RUNS):
    print("Deleting database")
    os.system(f'"{NEO4J_HOME}bin/cypher-shell" -u "neo4j" -p "password" "MATCH (n) DETACH DELETE n"')
    print("Importing data")
    start = time.time()
    os.system(f'"{NEO4J_HOME}bin/cypher-shell" --file "/Users/julian/Repositories/rel2graph-performance-evaluation/northwind/import_csv.cypher" -u "neo4j" -p "password" > /dev/null')
    end = time.time()
    print(f"Import took {end - start} seconds")
    total_seconds += end - start

print(f"Average import time: {total_seconds / RUNS} seconds")