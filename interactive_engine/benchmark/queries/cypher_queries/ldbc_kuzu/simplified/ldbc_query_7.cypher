MATCH  (p1:PERSON {id:$personId})<-[:COMMENT_HASCREATOR_PERSON]-(c:COMMENT)<-[:PERSON_LIKES_COMMENT]-(p2:PERSON),
      	(p1)-[:PERSON_KNOWS_PERSON]-(p2)
RETURN p2.id, p2.firstName, p2.lastName, c.content;