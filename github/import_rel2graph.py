from typing import Any, List
import pandas as pd 
import numpy as np
from tqdm import tqdm
from threading import Lock
import sqlite3
import logging
from collections import defaultdict
from multiprocessing import Process, freeze_support, Event

from rel2graph import Resource, ResourceIterator
from rel2graph.relational_modules.pandas import PandasDataframeIterator 
from rel2graph import IteratorIterator, Converter, Attribute, register_attribute_postprocessor, register_subgraph_preprocessor
from rel2graph.common_modules import DATETIME
from rel2graph.utils import load_file
from py2neo import Graph

logger = logging.getLogger(__name__)

class SQLiteResource(Resource):
    __supply_store = defaultdict(dict)

    def __init__(self, data, cols, pks, table) -> None:
        super().__init__()
        self._data = list(data)
        self._type = table
        # convert to dict for fast access
        self._cols = dict((col, i) for i,col in enumerate(cols))
        self._pks = tuple(pks)

    @property
    def supplies(self):
        key = tuple([self.type]+[self[pk] for pk in self._pks])
        return SQLiteResource.__supply_store[key]

    def clear_supplies(self) -> None:
        SQLiteResource.__supply_store[tuple([self[pk] for pk in self._pks])] = {}
    
    @staticmethod
    def tmp():
        return SQLiteResource.__supply_store
    
    @property
    def type(self) -> str:
        return self._type
    
    @property
    def pks(self) -> List[Any]:
        return dict((pk, self[pk]) for pk in self._pks)
    
    def __getitem__(self, key: str) -> str:
        return self._data[self._cols[key]]

    def __setitem__(self, key: str, value: Any) -> None:
        if key in self._cols.keys():
            self._data[self._cols[key]] = value
        else:
            self._cols[key] = len(self._data)
            self._data.append(value)
        
    def __repr__(self) -> str:
        repr = f"{super().__repr__()} ("
        for pk in self._pks:
            repr += f"{pk}={self[pk]},"
        return repr + ")"

class SQLiteIterator(ResourceIterator):
    def __init__(self, database, filter=None, primary_keys = None) -> None:
        super().__init__()

        # save because next is always synchronised
        self._con = sqlite3.connect(database, check_same_thread=False)
        self._con_lock = Lock()
        
        all_tables = [table[0] for table in self._con.execute(
            'SELECT name from sqlite_master where type= "table"'
            ).fetchall()]
        self._tables = all_tables
        if filter is not None:
            self._tables = filter
        logger.info(f"Iterating over tables: {self._tables}")

        self._cols = {}
        self._pks = {}
        for table in self._tables:
            cols = self._con.execute(f"PRAGMA table_info('{table}');").fetchall()
            self._cols[table] = [col[1] for col in cols]
            if primary_keys is not None and table in primary_keys.keys():
                self._pks[table] = primary_keys[table]
            else:
                self._pks[table] = [col[1] for col in cols if col[5]]
            if len(self._pks[table]) == 0:
                raise Exception(f"Table '{table}' has no primary key, which is required for the conversion. Please add a primary key to the table or specify it manually when instatiating the Iterator.")
        self._cursors = {}

        self._len = None

    def _init_cursors(self):
        for table in self._tables:
            with self._con_lock:
                self._cursors[table] = self._con.execute(f"SELECT * FROM {table}")      
            
    def __iter__(self):
        self._init_cursors()
        while True:
            for key in self._cursors.keys():
                with self._con_lock:
                    row = self._cursors[key].fetchone()
                if row is not None:
                    yield SQLiteResource(row, self._cols[key], self._pks[key], key)
                else:
                    # Remove cursor from dict
                    del self._cursors[key]
                    if len(self._cursors) == 0:
                        return
                    break

    def request_resources(self, table, filters):
        query = f"SELECT * FROM {table} WHERE "
        were = []
        for key, value in filters.items():
            if isinstance(value, str):
                quotes = "'"
            else:
                quotes = ""
            were.append(f"{table}.{key} == {quotes}{value}{quotes}")
        query = query + " AND ".join(were)
        with self._con_lock:
            cursor = self._con.execute(query)
            rows = cursor.fetchall()
        return [SQLiteResource(row, self._cols[table], self._pks[table], table) for row in rows]
        
    def __len__(self) -> None:
        if self._len is None:
            self._len = 0
            for table in self._tables:
                self._len += self._con.execute(f"SELECT Count(*) FROM {table}").fetchone()[0]
        return self._len
    

# registerfloat processing function
@register_attribute_postprocessor
def FLOAT(attribute):
    return Attribute(attribute.key, float(attribute.value))

@register_attribute_postprocessor
# This is required because the pandas dataframe iterator will convert all values 
# to int64 which is not supported by neo4j
def INT(attribute):
    if np.isnan(attribute.value):
        return Attribute(attribute.key, -1) # handle null values
    return Attribute(attribute.key, int(attribute.value))

database = "/Users/julian/Repositories/rel2graph-performance-evaluation/data/github/ansible__ansible__2020-06-11_06-06-22.db"
# database = "/Users/julian/Repositories/rel2graph-performance-evaluation/data/github/airbnb__enzyme__2020-06-11_01-31-34.db"

def wait_for_other(events, pos):
    events[pos].set()
    for i in range(len(events)):
        if i != pos:
            events[i].wait()

def run(table, pos, status_events):
    graph = Graph(scheme="bolt", host="localhost", port=7687,  auth=('neo4j', 'password')) 

    # Create Iterator
    iterator = SQLiteIterator(database, [table], primary_keys={"commits": ["hash"], "productivity": ["commit_hash", "new_path"]})

    # Create converter instance with schema, the final iterator and the graph
    converter = Converter(load_file("github/schema.yaml"), iterator, graph, num_workers=10)
    
    # Start the conversion
    converter(lambda total: tqdm(total=total, desc=table, position=pos), relations_wait_function=lambda: wait_for_other(status_events, pos))





# # Create Iterator
# tables = ["commits", "productivity"]
# iterator = SQLiteIterator("/Users/julian/Repositories/rel2graph-performance-evaluation/data/github/airbnb__enzyme__2020-06-11_01-31-34.db", tables, primary_keys={"commits": ["hash"], "productivity": ["commit_hash", "new_path"]})

# # Create converter instance with schema, the final iterator and the graph
# converter = Converter(load_file("github/schema.yaml"), iterator, graph, num_workers=10)
# # Start the conversion
# converter(tqdm)

from functools import partial

if __name__ == '__main__':
    freeze_support()
    
    # Create a connection to the neo4j graph with the py2neo Graph object
    graph = Graph(scheme="bolt", host="localhost", port=7687,  auth=('neo4j', 'password')) 
    graph.delete_all()

    status_events = [Event() for _ in range(2)]


    # Create a new process for each table as deamon processes
    processes = []
    for i, table in enumerate(["commits", "productivity"]):
        p = Process(target=run, args=(table, i, status_events), daemon=True)
        p.start()
        processes.append(p)

    # Wait for all processes to finish
    for p in processes:
        p.join()