MATCH  (p1:PERSON {id:$personId})-[:PERSON_KNOWS_PERSON]-(p2:PERSON)<-[:COMMENT_HASCREATOR_PERSON]-(c1:COMMENT)-[:COMMENT_ISLOCATEDIN_PLACE]->(pl1:PLACE {name:'$countryXName'}), 
(p2)<-[:COMMENT_HASCREATOR_PERSON]-(c2:COMMENT)-[:COMMENT_ISLOCATEDIN_PLACE]->(pl2:PLACE {name:'$countryYName'})
WHERE  c1.creationDate >= $startDate AND c1.creationDate < $endDate
      	AND c2.creationDate >= $startDate AND c2.creationDate < $endDate
RETURN COUNT(p1)