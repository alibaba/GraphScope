Match (person:PERSON)-[:LIKES]->(message:POST),
(message:POST)<-[:CONTAINEROF]-(forum:FORUM),
(person:PERSON)-[:KNOWS|HASINTEREST]->(p:PERSON|TAG),
(forum:FORUM)-[:HASMODERATOR|HASTAG]->(p:PERSON|TAG)
Return count(person);