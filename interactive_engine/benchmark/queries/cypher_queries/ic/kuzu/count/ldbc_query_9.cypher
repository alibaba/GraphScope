MATCH (p1:PERSON {id: $personId})-[:PERSON_KNOWS_PERSON]-(p2:PERSON)<-[:COMMENT_HASCREATOR_PERSON]-(c:COMMENT)
WHERE  c.creationDate < $maxDate
RETURN COUNT(p1)