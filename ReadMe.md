# Performance measurements for Data2Neo

This repository holds the code for the performance evaluation of the Data2Neo library. As baseline we measure import time for direct import with neo4j's CYPHER and LOAD CSV.

# Reproducability Instructions

Start by downloading the input data.
```python
python download_data.py --data PATH_TO_DATA_STORE
```

Adapt the `run.sh` file by changing the paths to your desired locations. For neo4j to be able to import the data, we copy it to the default neo4j import folder `/var/lib/neo4j/import`. If your neo4j installation has a different install location, please adapt this path in `import_csv.py` as well as `delete_restart_neo4j.sh`. Also to copy files to the import folder we need sudo access. Make sure that the executing user has sudo access, or adapt the script/permissions accordingly.

To run the benchmark:
```bash
bash run.sh
```

This will repeat each experiment 5 times and create a folder `measurements` with one file per run. Each file contains the seconds that the configuration needed for the conversion. 