MATCH (p:PERSON {id: 2199023323088})-[:KNOWS*1..3]-(friend:PERSON)<-[:HASCREATOR]-(message : POST | COMMENT) 
WHERE friend <> p and message.creationDate < 1333670400000 with friend,message ORDER BY message.creationDate DESC, message.id ASC LIMIT 20 
RETURN friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, message.id AS commentOrPostId, 
message.content AS messageContent, message.imageFile AS messageImageFile, message.creationDate AS commentOrPostCreationDate 