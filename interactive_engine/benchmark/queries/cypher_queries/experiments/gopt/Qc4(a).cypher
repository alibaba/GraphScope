Match (forum:FORUM)-[:CONTAINEROF]->(post:POST),
(forum:FORUM)-[:HASMEMBER]->(person1:PERSON),
(forum:FORUM)-[:HASMEMBER]->(person2:PERSON),
(person1:PERSON)-[:KNOWS]->(person2:PERSON),
(person1:PERSON)-[:LIKES]->(post:POST),
(person2:PERSON)-[:LIKES]->(post:POST)
Return count(person1);