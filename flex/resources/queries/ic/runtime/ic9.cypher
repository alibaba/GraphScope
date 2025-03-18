MATCH (p:PERSON {id: 19791209300317L})-[:KNOWS*1..3]-(friend:PERSON)
WITH distinct friend
where friend.id <>  19791209300317L
MATCH  (message:POST|COMMENT)-[e:HASCREATOR]->(friend)
where e.creationDate < 1352160000000L
WITH friend, message

RETURN 
    friend.id AS personId, 
    friend.firstName AS personFirstName, 
    friend.lastName AS personLastName, 
    message.id AS messageId, 
    CASE WHEN message.content = "" THEN message.imageFile
    ELSE message.content END as messageContent,
    message.creationDate AS messageCreationDate
ORDER BY 
   messageCreationDate DESC, 
    messageId ASC 
LIMIT 20