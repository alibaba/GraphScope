MATCH (p :PERSON {id: 32985348834326L})-[:KNOWS]-(friend:PERSON)<-[:HASCREATOR]-(message : POST | COMMENT) 
WHERE 
    message.creationDate <= 1334102400000L
WITH 
    friend, 
    message 
ORDER BY 
    message.creationDate DESC, 
    message.id ASC LIMIT 20 
return 
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName, 
    message.id AS messageId,
    CASE WHEN message.content = "" THEN message.imageFile
    ELSE message.content END as messageContent,
    message.creationDate AS messageCreationDate;