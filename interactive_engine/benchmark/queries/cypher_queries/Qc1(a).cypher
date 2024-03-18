Match (message:Message)-[:HASCREATOR]->(person:PERSON),
(message:Message)-[:HASTAG]->(tag:TAG),
(person:PERSON)-[:HASINTEREST]->(tag:TAG)
Return count(person);