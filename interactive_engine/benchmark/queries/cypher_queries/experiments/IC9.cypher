MATCH (p:PERSON {id: $personId})-[:KNOWS*1..3]-(friend:PERSON)
MATCH  (message)-[:HASCREATOR]->(friend:PERSON)
where message.creationDate < $maxDate
      AND friend.id <> $personId

WITH DISTINCT friend, message

RETURN 
    friend.id AS personId, 
    friend.firstName AS personFirstName, 
    friend.lastName AS personLastName, 
    message.id AS commentOrPostId, 
    message.content AS messageContent, 
    message.imageFile AS messageImageFile, 
    message.creationDate AS commentOrPostCreationDate
ORDER BY 
    commentOrPostCreationDate DESC, 
    commentOrPostId ASC 
LIMIT 20;