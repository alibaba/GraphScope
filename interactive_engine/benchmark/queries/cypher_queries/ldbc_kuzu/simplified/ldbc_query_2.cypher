MATCH  (p1:PERSON {id:$personId})-[:PERSON_KNOWS_PERSON]-(p2:PERSON)<-[:COMMENT_HASCREATOR_PERSON]-(c:COMMENT)
WHERE  c.creationDate < $maxDate
RETURN p2.id, p2.firstName, p2.lastName, c.id, c.content, 
				c.creationDate;