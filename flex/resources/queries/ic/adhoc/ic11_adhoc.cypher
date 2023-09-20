MATCH (p:PERSON {id: 6597069812321})-[:KNOWS*1..3]-(friend:PERSON)-[wa:WORKAT]->(com:ORGANISATION)-[:ISLOCATEDIN]->(:PLACE {name: "Papua_New_Guinea"}) 
WHERE p <> friend and wa.workFrom < 2011
with distinct friend as friend, com AS com, wa.workFrom as organizationWorkFromYear 
ORDER BY organizationWorkFromYear ASC, friend.id ASC, com.name DESC LIMIT 10 
return friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, com.name as organizationName, organizationWorkFromYear as organizationWorkFromYear;