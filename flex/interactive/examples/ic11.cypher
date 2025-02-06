MATCH (p:PERSON {id: 24189255811707})-[:KNOWS*1..3]-(friend:PERSON)
WITH distinct friend
WHERE 
     friend.id <> 24189255811707     

MATCH (friend:PERSON)-[wa:WORKAT]->(com:ORGANISATION)-[:ISLOCATEDIN]->(:PLACE {name: "Switzerland"}) 
WHERE wa.workFrom < 2006
WITH
    friend as friend, 
    com AS com, 
    wa.workFrom as organizationWorkFromYear 
ORDER BY 
    organizationWorkFromYear ASC, 
    friend.id ASC, com.name DESC 
LIMIT 10 
return 
    friend.id AS personId, 
    friend.firstName AS personFirstName, 
    friend.lastName AS personLastName, 
    com.name as organizationName, 
    organizationWorkFromYear as organizationWorkFromYear;