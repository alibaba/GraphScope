MATCH (p:PERSON {id: $personId})-[:KNOWS]-(friend:PERSON)<-[:HASCREATOR]-(comment:COMMENT)-[:REPLYOF]->(:POST)-[:HASTAG]->(tag:TAG)-[:HASTYPE]->(:TAGCLASS)-[:ISSUBCLASSOF*0..5]->(baseTagClass:TAGCLASS {name: '$tagClassName'})
RETURN
  friend.id AS personId,
  friend.firstName AS personFirstName,
  friend.lastName AS personLastName,
  collect(DISTINCT tag.name) AS tagNames,
  count(DISTINCT comment) AS replyCount
ORDER BY
  replyCount DESC,
  personId ASC
LIMIT 20