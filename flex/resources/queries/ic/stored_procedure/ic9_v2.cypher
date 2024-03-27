MATCH (p:PERSON {id: $personId})-[:KNOWS*1..3]-(friend:PERSON)
WHERE p <> friend
WITH distinct friend

MATCH (friend:PERSON)<-[:HASCREATOR]-(message : POST | COMMENT) 
WHERE
    AND message.creationDate < $maxDate
with
    friend,message 
ORDER BY
    message.creationDate DESC, 
    message.id ASC 
LIMIT 20
RETURN
    friend.id AS personId, 
    friend.firstName AS personFirstName, 
    friend.lastName AS personLastName, 
    message.id AS commentOrPostId, 
    message.content AS messageContent, 
    message.imageFile AS messageImageFile, 
    message.creationDate AS commentOrPostCreationDate 