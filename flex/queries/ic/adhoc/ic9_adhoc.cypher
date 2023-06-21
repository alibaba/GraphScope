MATCH (p:person {id: 2199023323088})-[:knows*1..3]-(friend:person)<-[:hasCreator]-(message : post | comment) 
WHERE friend <> p and message.creationDate < 1333670400000 with friend,message ORDER BY message.creationDate DESC, message.id ASC LIMIT 20 
RETURN friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, message.id AS commentOrPostId, 
message.content AS messageContent, message.imageFile AS messageImageFile, message.creationDate AS commentOrPostCreationDate 