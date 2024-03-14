Match (person1:PERSON)-[:LIKES]->(message:MESSAGE),
(message:MESSAGE)-[:HASCREATOR]->(person2:PERSON),
(person1:PERSON)<-[:HASMODERATOR]-(forum:FORUM),
(person2:PERSON)<-[:HASMODERATOR]-(forum:FORUM)
Return count(person1);