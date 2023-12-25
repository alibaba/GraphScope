Match (message:COMMENT|POST)-[:HASCREATOR]->(person:PERSON), 
      (message:COMMENT|POST)-[:HASTAG]->(tag:TAG), 
      (person:PERSON) -[:HASINTEREST]->(tag:TAG)
Return count(person);