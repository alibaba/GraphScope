MATCH  (:PERSON)-[:PERSON_KNOWS_PERSON]-(p1:PERSON {id:$personId})-[:PERSON_KNOWS_PERSON]-(p2:PERSON)<-[:POST_HASCREATOR_PERSON]-(ps:POST)-[:POST_HASTAG_TAG]->(t:TAG)
WHERE  ps.creationDate >= $startDate AND ps.creationDate < $endDate
RETURN COUNT(p1)