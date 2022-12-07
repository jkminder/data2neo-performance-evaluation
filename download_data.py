import os
import wget

# create a folder to store the data
if not os.path.isdir('data/northwind'):
    os.mkdir('data/northwind')

northwind_tables =  ['categories', 'customers', 'employees', 'orders', 'products', 'suppliers']

for table in northwind_tables:
    url = f'https://raw.githubusercontent.com/neo4j-documentation/developer-resources/gh-pages/data/northwind/{table}.csv'
    wget.download(url, out=f'data/northwind/{table}.csv')