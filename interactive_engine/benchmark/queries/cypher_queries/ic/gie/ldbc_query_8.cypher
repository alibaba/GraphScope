MATCH (person:PERSON {id: $personId})<-[:HASCREATOR]-(message)<-[:REPLYOF]-(comment:COMMENT)-[:HASCREATOR]->(author:PERSON)
RETURN 
	author.id,
	author.firstName,
	author.lastName,
	comment.creationDate as commentDate,
	comment.id as commentId,
	comment.content
ORDER BY
	commentDate desc,
	commentId asc
LIMIT 20