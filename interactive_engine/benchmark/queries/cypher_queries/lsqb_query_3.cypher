MATCH (person1)-[:knows]-(person2),
(person1)-[:knows]-(person3),
(person2)-[:knows]-(person3),
(person1)-[:isLocatedIn]->(city1),
(city1)-[:isPartOf]->(country),
(person2)-[:isLocatedIn]->(city2),
(city2)-[:isPartOf]->(country),
(person3)-[:isLocatedIn]->(city3),
(city3)-[:isPartOf]->(country)
RETURN COUNT(person1)