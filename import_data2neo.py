from tqdm import tqdm
import sqlite3
import logging
from multiprocessing import freeze_support
import time
import os
import neo4j
import argparse

from data2neo import IteratorIterator, Converter, Attribute, register_attribute_postprocessor, register_subgraph_preprocessor, register_attribute_preprocessor
import data2neo.common_modules
from data2neo.utils import load_file

from data2neo.relational_modules.sqlite import SQLiteIterator

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO)
# log to console
console = logging.StreamHandler()
console.setLevel(logging.INFO)
# add process name to log
formatter = logging.Formatter('%(asctime)s - %(name)s - %(processName)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)


@register_subgraph_preprocessor
def IF_RENAME(resource):
    if resource["old_path"] != resource["new_path"] and not resource["new_path"] is None and resource["old_path"] is not None and resource["new_path"] != "" and resource["old_path"] != "":
        return resource
    return None

@register_attribute_postprocessor
def CAPS(attribute):
    return Attribute(attribute.key, attribute.value.upper())

@register_attribute_preprocessor
def SELECT_PATH(resource):
    if resource["new_path"] is None and resource["new_path"] != "":
        resource["path"] = resource["old_path"]
    else:
        resource["path"] = resource["new_path"]
    return resource

@register_subgraph_preprocessor
def IF_NOT_ONLY_RENAMED(resource):
    if resource["old_path"] != resource["new_path"] and not resource["new_path"] is None and resource["edit_type"] != "file_renaming":
        return resource
    return None

from functools import partial

if __name__ == '__main__':
    freeze_support()

    parser = argparse.ArgumentParser()
    parser.add_argument("--database", help="Path to the sqlite database")
    parser.add_argument("--workers", help="Number of workers", type=int, default=32)
    parser.add_argument("--name", help="Name of the output file", default=time.time())
    args = parser.parse_args()
    print("Deleting database")
    os.system("sh delete_restart_neo4j.sh")

    start = time.time()
    uri = "bolt://localhost:7687"

    auth = neo4j.basic_auth("neo4j", "password")

    # Unique constraints
    driver = neo4j.GraphDatabase.driver(uri, auth=auth)
    with driver.session() as session:
        session.run("CREATE CONSTRAINT Commit_hash_unique IF NOT EXISTS FOR (n:Commit) REQUIRE n.hash IS UNIQUE")
        session.run("CREATE CONSTRAINT File_path_unique IF NOT EXISTS FOR (n:File) REQUIRE n.path IS UNIQUE")
    
    # Create Iterator
    tables = ["commits", "edits"]
    conn = sqlite3.connect(args.database, check_same_thread=False)
    iterator = SQLiteIterator(conn, tables, primary_keys={"commits": ["hash"], "productivity": ["commit_hash", "new_path"], "edits": ["commit_hash", "new_path", "pre_starting_line_no"]})

    # Create converter instance with schema, the final iterator and the graph
    converter = Converter(load_file("data2neo_schema.yaml"), iterator, neo4j_auth=auth, neo4j_uri=uri, num_workers=args.workers, batch_size=20000)
    # Start the conversion
    converter(tqdm)

    end = time.time()
    
    # generate output dir
    output_dir = "measurements"
    os.makedirs(output_dir, exist_ok=True)

    # write time to file
    with open(f"{output_dir}/{args.name}.txt", "w") as f:
        f.write(str(end - start))

    print(f"Time: {end - start}")