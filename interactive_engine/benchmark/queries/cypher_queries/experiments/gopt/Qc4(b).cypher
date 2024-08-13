Match (forum:FORUM)-[:HASTAG]->(post:TAG),
(forum:FORUM)-[:HASMODERATOR]->(person1:PERSON),
(forum:FORUM)-[:HASMODERATOR|CONTAINEROF]->(person2:PERSON|POST),
(person1:PERSON)-[:KNOWS|LIKES]->(person2:PERSON|POST),
(person1:PERSON)-[:HASINTEREST]->(post:TAG),
(person2:PERSON|POST)-[:HASINTEREST|HASTAG]->(post:TAG)
Return count(person1);