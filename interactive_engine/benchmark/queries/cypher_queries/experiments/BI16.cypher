MATCH (person1:PERSON)<-[:HASCREATOR]-(message1)-[:HASTAG]->(tag:TAG {name: $tagName})
WHERE message1.creationDate > $date
OPTIONAL MATCH (person1)-[:KNOWS]-(person2:PERSON)<-[:HASCREATOR]-(message2)-[:HASTAG]->(tag)
WHERE message2.creationDate = $date
WITH person1, count(DISTINCT message1) AS cm, count(DISTINCT person2) AS cp2
WHERE cp2 <= 4
// return count
RETURN person1, cm;