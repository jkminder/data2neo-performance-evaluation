from py2neo import Graph
import pandas as pd 
import numpy as np
from rel2graph.relational_modules.pandas import PandasDataframeIterator 
from rel2graph import IteratorIterator, Converter, Attribute, register_attribute_postprocessor
from rel2graph.utils import load_file
from tqdm import tqdm

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
    
# Create a connection to the neo4j graph with the py2neo Graph object
graph = Graph(scheme="bolt", host="localhost", port=7687,  auth=('neo4j', 'password')) 
graph.delete_all()

# Create IteratorIterator
files = ["categories", "employees", "orders", "products", "suppliers"]
iterators = []
for file in files:
    df = pd.read_csv(f"data/northwind/{file}.csv")
    iterators.append(PandasDataframeIterator(df, file))

iterator = IteratorIterator(iterators)

# Create converter instance with schema, the final iterator and the graph
converter = Converter(load_file("northwind/schema.yaml"), iterator, graph, num_workers = 5)
# Start the conversion
converter(tqdm)