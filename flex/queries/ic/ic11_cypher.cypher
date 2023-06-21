MATCH (p:person {id: $personId})-[:knows*1..3]-(friend:person)-[wa:workAt]->(com:organisation)-[:isLocatedIn]->(:place {name: $countryName}) WHERE p <> friend and wa.workFrom < $workFromYear with distinct friend as friend, com AS com, wa.workFrom as organizationWorkFromYear ORDER BY organizationWorkFromYear ASC, friend.id ASC, com.name DESC LIMIT 10 return friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, com.name as organizationName, organizationWorkFromYear as organizationWorkFromYear