CREATE CONSTRAINT Commit_hash_unique IF NOT EXISTS FOR (n:Commit) REQUIRE n.hash IS UNIQUE;
CREATE CONSTRAINT File_path_unique IF NOT EXISTS FOR (n:File) REQUIRE n.path IS UNIQUE;

LOAD CSV WITH HEADERS FROM 'file:///commits.csv' AS row
CALL {
  WITH row
  CREATE (commit:Commit {hash: toString(row.hash), no_of_modifications: row.no_of_modifications, commit_message: row.commit_message, date : datetime(row.date)})
  MERGE (author:Author {id: row.author_id})
  MERGE (author)-[:AUTHORED]->(commit)
} IN TRANSACTIONS OF 20000 ROWS;

LOAD CSV WITH HEADERS FROM 'file:///commits.csv' AS row
CALL {
  WITH row
  MATCH (commit:Commit {hash: toString(row.hash)})
  MATCH (parent:Commit {hash: toString(row.parents)})
  MERGE (parent)-[:CHILD]->(commit)
} IN TRANSACTIONS OF 20000 ROWS;

LOAD CSV WITH HEADERS FROM 'file:///edits.csv' AS row
CALL {
  WITH row
  CALL apoc.do.when(
    row.new_path IS NOT NULL AND row.new_path <> '',
    'MATCH (commit:Commit {hash: toString($row.commit_hash)}) MERGE (file:File {path: row.new_path}) WITH file, commit CALL apoc.create.relationship(commit, toUpper($row.edit_type), {pre_starting_line_no: $row.pre_starting_line_no,
      levenshtein_dist: $row.levenshtein_dist,
      post_starting_line_no: $row.post_starting_line_no,
      pre_starting_line_no: $row.pre_starting_line_no,
      post_starting_line_no: $row.post_starting_line_no,
      pre_text : $row.pre_text,
      post_text : $row.post_text}, file) 
  YIELD rel RETURN rel',
    'MATCH (commit:Commit {hash: toString($row.commit_hash)}) MERGE (file:File {path: row.new_path}) WITH file, commit CALL apoc.create.relationship(commit, toUpper($row.edit_type), {pre_starting_line_no: $row.pre_starting_line_no,
      levenshtein_dist: $row.levenshtein_dist,
      post_starting_line_no: $row.post_starting_line_no,
      pre_starting_line_no: $row.pre_starting_line_no,
      post_starting_line_no: $row.post_starting_line_no,
      pre_text : $row.pre_text,
      post_text : $row.post_text}, file) 
  YIELD rel RETURN rel',
    {row:row})
  YIELD value
} IN TRANSACTIONS OF 20000 ROWS;


// This needs to be a seperate query because we need to make sure that old_path is already in the graph
LOAD CSV WITH HEADERS FROM 'file:///edits.csv' AS row
CALL {
  WITH row
  CALL apoc.do.when(
    row.new_path <> row.old_path AND row.new_path IS NOT NULL  AND row.new_path <> '' AND row.old_path IS NOT NULL AND row.old_path <> '',
    'MATCH  (new:File {path: row.new_path}) MATCH (old:File {path: row.old_path}) MERGE (new)<-[:RENAMED_TO {commit: $row.commit_hash}]-(old) RETURN new',
    '',
    {row:row})
  YIELD value
} IN TRANSACTIONS OF 20000 ROWS;
