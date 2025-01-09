MATCH
  (tag:TAG {name: $tag})<-[:HASTAG]-(message:COMMENT),
  (message)<-[:REPLYOF]-(comment:COMMENT),
  (comment:COMMENT)-[:HASTAG]->(relatedTag:TAG)
WHERE NOT (comment:COMMENT)-[:HASTAG]->(tag:TAG {name: $tag})
RETURN
  relatedTag.name as name,
  count(DISTINCT comment) AS count
ORDER BY
  count DESC,
  name ASC
LIMIT 100;