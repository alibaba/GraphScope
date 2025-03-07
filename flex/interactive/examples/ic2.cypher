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
    message.id AS postOrCommentId,
    message.content AS content,
    message.imageFile AS imageFile,
    message.creationDate AS postOrCommentCreationDate;