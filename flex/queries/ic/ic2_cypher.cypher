:submit 
MATCH (p :person {id: $personId })-[:knows]-(friend:person)<-[:hasCreator]-(message : post | comment) WHERE message.creationDate < $maxDate  WITH friend, message ORDER BY message.creationDate DESC, message.id ASC LIMIT 20 return friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, message.id AS postOrCommentId, message.content AS content,message.imageFile AS imageFile,message.creationDate AS postOrCommentCreationDate 


CALL apoc.custom.asProcedure('query_ic2',
  'MATCH (n:PERSON {name:$name})-[:KNOWS]->(friend) RETURN friend','read',
  [['friend','NODE']],[['name','STRING']], 'get friends of a person');



// adhoc query sf10
MATCH (p :person {id: 32985348864506})-[:knows]-(friend:person)<-[:hasCreator]-(message : post | comment) 
WHERE message.creationDate < 1348012800000 WITH friend, message ORDER BY message.creationDate DESC, message.id ASC LIMIT 20 
return friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, message.id AS postOrCommentId,
message.content AS content,message.imageFile AS imageFile,message.creationDate AS postOrCommentCreationDate;

// adhoc query sf0.1
MATCH (p :person {id: 19791209300143})-[:knows]-(friend:person)<-[:hasCreator]-(message : post | comment) 
WHERE message.creationDate < 1354060800000 WITH friend, message ORDER BY message.creationDate DESC, message.id ASC LIMIT 20 
return friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, message.id AS postOrCommentId,
message.content AS content,message.imageFile AS imageFile,message.creationDate AS postOrCommentCreationDate;

MATCH (p :person {id: 26388279107154 }) return p;