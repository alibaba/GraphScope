:submit MATCH (p:person {id: 24189255811707})-[:knows*1..3]-(friend:person)-[wa:workAt]->(com:organisation)-[:isLocatedIn]->(:place {name: "Switzerland"}) WHERE p <> friend and wa.workFrom < 2006 return friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName ,com.name AS organizationName,wa.workFrom AS organizationWorkFromYear  ORDER BY organizationWorkFromYear ASC, personId ASC, organizationName DESC LIMIT 10


// v1
:submit MATCH (p:person {id: 24189255811707})-[:knows*1..3]-(friend:person)-[wa:workAt]->(com:organisation)-[:isLocatedIn]->(:place {name: "Switzerland"}) WHERE p <> friend and wa.workFrom < 2006 with distinct friend as friend, com.name AS organizationName, wa.workFrom AS organizationWorkFromYear return friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, organizationName, organizationWorkFromYear ORDER BY organizationWorkFromYear ASC, personId ASC, organizationName DESC LIMIT 10

:submit MATCH (p:person {id: 24189255811707})-[:knows*1..3]-(friend:person)-[wa:workAt]->(com:organisation)-[:isLocatedIn]->(:place {name: "Switzerland"}) WHERE p <> friend and wa.workFrom < 2006 with distinct friend as friend, com AS com, wa.workFrom as organizationWorkFromYear ORDER BY organizationWorkFromYear ASC, friend.id ASC, com.name DESC LIMIT 10 return friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, com.name as organizationName, organizationWorkFromYear as organizationWorkFromYear
//wa.workFrom AS organizationWorkFromYear

//dyn
:submit MATCH (p:person {id: $personId})-[:knows*1..3]-(friend:person)-[wa:workAt]->(com:organisation)-[:isLocatedIn]->(:place {name: $countryName}) WHERE p <> friend and wa.workFrom < $workFromYear with distinct friend as friend, com AS com, wa.workFrom as organizationWorkFromYear ORDER BY organizationWorkFromYear ASC, friend.id ASC, com.name DESC LIMIT 10 return friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, com.name as organizationName, organizationWorkFromYear as organizationWorkFromYear

//adhoc
MATCH (p:person {id: 6597069812321})-[:knows*1..3]-(friend:person)-[wa:workAt]->(com:organisation)-[:isLocatedIn]->(:place {name: "Papua_New_Guinea"}) 
WHERE p <> friend and wa.workFrom < 2011
with distinct friend as friend, com AS com, wa.workFrom as organizationWorkFromYear 
ORDER BY organizationWorkFromYear ASC, friend.id ASC, com.name DESC LIMIT 10 
return friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, com.name as organizationName, organizationWorkFromYear as organizationWorkFromYear



MATCH (p:PERSON {id: 6597069812321})-[:KNOWS*1..3]-(friend:PERSON)-[wa:WORKAT]->(com:ORGANISATION)-[:ISLOCATEDIN]->(:COUNTRY {name: "Papua_New_Guinea"}) 
WHERE p <> friend and wa.workFrom < 2011
with distinct friend as friend, com AS com, wa.workFrom as organizationWorkFromYear 
ORDER BY organizationWorkFromYear ASC, friend.id ASC, com.name DESC LIMIT 10 
return friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, com.name as organizationName, organizationWorkFromYear as organizationWorkFromYear