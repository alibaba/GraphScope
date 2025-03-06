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
 messageCreationDate,
 message.content as messageContent,
 message.imageFile as messageImageFile,
 post.id AS postId,
 person.id AS personId,
 person.firstName AS personFirstName,
 person.lastName AS personLastName
ORDER BY messageCreationDate DESC, messageId ASC