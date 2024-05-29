MATCH (person3)-[:hasInterest]->(tag),
(person1)-[:knows]-(person2),
(person2)-[:knows]-(person3)
WHERE person1<>person3
RETURN COUNT(person3)