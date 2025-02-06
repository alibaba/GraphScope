MATCH (p:PERSON {id: 32985348834013})-[:KNOWS*1..3]-(friend:PERSON)
WITH distinct friend
where friend.id <> 32985348834013
MATCH  (message:POST|COMMENT)-[e:HASCREATOR]->(friend)
where e.creationDate < 1346112000000
WITH friend, message

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
LIMIT 20