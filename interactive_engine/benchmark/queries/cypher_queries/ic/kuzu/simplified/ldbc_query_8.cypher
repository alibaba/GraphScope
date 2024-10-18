MATCH (p1:PERSON {id: $personId})<-[:POST_HASCREATOR_PERSON]-(ps:POST)<-[:COMMENT_REPLYOF_POST]-(c:COMMENT)-[:COMMENT_HASCREATOR_PERSON]->(p2:PERSON)
RETURN p2.id, p2.firstName, p2.lastName, 
				c.creationDate, c.id, c.content;