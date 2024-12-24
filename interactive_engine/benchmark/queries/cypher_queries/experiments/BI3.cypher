MATCH
  (country:PLACE {name: $country})<-[:ISPARTOF]-()<-[:ISLOCATEDIN]-
  (person:PERSON)<-[:HASMODERATOR]-(forum:FORUM)-[:CONTAINEROF]->(post:POST)<-[:REPLYOF*0..30]-(message)-[:HASTAG]->(:TAG)-[:HASTYPE]->(:TAGCLASS {name: $tagClass})
RETURN
  forum.id as id,
  forum.title,
  forum.creationDate,
  person.id as personId,
  count(DISTINCT message) AS messageCount
  ORDER BY
  messageCount DESC,
  id ASC
  LIMIT 20;