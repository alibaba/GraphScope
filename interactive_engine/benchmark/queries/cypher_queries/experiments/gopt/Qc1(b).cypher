Match (message:PERSON|FORUM)-[:KNOWS|HASMODERATOR]->(person:PERSON),
(message)-[]->(tag:TAG),
(person)-[]->(tag)
Return count(person);