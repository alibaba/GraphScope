MATCH 
    (unused:PERSON {id: 10995116278647L })-[:KNOWS]-(friend:PERSON)<-[:HASCREATOR]-(comments:COMMENT)-[:REPLYOF]->(:POST)-[:HASTAG]->(tags:TAG)
WITH friend, tags, comments
MATCH (tags:TAG)-[:HASTYPE]->(:TAGCLASS)-[:ISSUBCLASSOF*0..*]->(:TAGCLASS {name: "Chancellor"})
WITH 
    friend AS friend, 
    collect(DISTINCT tags.name) AS tagNames, 
    count(DISTINCT comments) AS replyCount 
ORDER BY 
    replyCount DESC, 
    friend.id ASC 
LIMIT 20 
RETURN 
    friend.id AS personId, 
    friend.firstName AS personFirstName, 
    friend.lastName AS personLastName, 
    tagNames, 
    replyCount