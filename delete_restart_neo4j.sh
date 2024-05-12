#!/bin/bash
# Script for deleting the currently running knowledge graph and restarting it

neo4j stop
rm -rf /var/lib/neo4j/data/databases/neo4j /var/lib/neo4j/data/transactions/neo4j
neo4j start

# While we can't connect wait
while ! curl -s http://localhost:7474 > /dev/null
do
    sleep 1
done