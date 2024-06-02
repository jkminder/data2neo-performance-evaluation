import os
import time
import shutil
import pandas as pd
import sqlite3
import csv 
import neo4j
import argparse

def export_csv(outfile, table, database):
    inpsql3 = sqlite3.connect(database)
    sql3_cursor = inpsql3.cursor()
    sql3_cursor.execute(f'SELECT * FROM {table}')
    i = 0
    with open(outfile,'w') as out_csv_file:
        csv_out = csv.writer(out_csv_file, quoting=csv.QUOTE_ALL,  quotechar='"', escapechar='\\')
        # write header                        
        csv_out.writerow([d[0] for d in sql3_cursor.description])
        # write data 
        for result in sql3_cursor:
            i += 1
            csv_out.writerow(result)
        inpsql3.close()
        print(f"Exported {i} rows")

    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", help="Path to the sqlite database")
    parser.add_argument("--name", help="Name of the output file", default=time.time())
    args = parser.parse_args()
    print("Deleting database")
    os.system("sh delete_restart_neo4j.sh")



    start = time.time()

    database_folder = "/".join(args.database.split("/")[:-1])

    # Export csvs
    conn = sqlite3.connect(args.database, isolation_level=None,
                        detect_types=sqlite3.PARSE_COLNAMES)
    
    for table in ["commits", "edits"]:
       print(f"Exporting {table} table")
       export_csv(f"{database_folder}/{table}.csv", table, args.database)
       # copy csv to import folder 
       # THIS NEEDS SUDO PERMISSIONS! adapt to your needs
       os.system(f"sudo cp {database_folder}/{table}.csv /var/lib/neo4j/import")

    # Replace placeholders in the cypher script
    start_import = time.time()
    os.system(f'"cypher-shell" --file "./import_csv.cypher" -u "neo4j" -p "password"')
    end = time.time()
    
    # generate output dir
    output_dir = "measurements"
    os.makedirs(output_dir, exist_ok=True)

    # write time to file
    with open(f"{output_dir}/{args.name}.txt", "w") as f:
        f.write(str(end - start))
        f.write("\n")
        f.write(str(end-start_import))

    print(f"Time: {end - start}")