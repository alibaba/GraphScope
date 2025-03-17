MATCH (p :PERSON {id: 26388279068220L})<-[:HASCREATOR]-(message: POST | COMMENT)
WITH
 message,
 message.id AS messageId,
 message.creationDate AS messageCreationDate
ORDER BY messageCreationDate DESC, messageId ASC
LIMIT 10
MATCH (message: POST | COMMENT)-[:REPLYOF*0..*]->(post:POST)-[:HASCREATOR]->(person:PERSON)
RETURN
 messageId,
 CASE WHEN message.content = "" THEN message.imageFile 
 ELSE message.content END as messageContent,
 messageCreationDate,
 post.id AS originalPostId,
 person.id AS originalPostAuthorId,
 person.firstName AS originalPostAuthorFirstName,
 person.lastName AS originalPostAuthorLastName
ORDER BY messageCreationDate DESC, messageId ASC