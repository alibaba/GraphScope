Match (tag:TAG {name: $tag})<-[:HASTAG]-(message)
OPTIONAL MATCH (message)<-[:LIKES]-(liker:PERSON)
OPTIONAL MATCH (message)<-[:REPLYOF]-(comment:COMMENT)
MATCH (message)-[:HASCREATOR]->(person:PERSON)
WITH message, person, count(distinct liker) as likeCount, count(distinct comment) as replyCount
WITH
  person.id AS id,
  sum(replyCount) as replyCount,
  sum(likeCount) as likeCount,
  count(message) as messageCount
RETURN
  id,
  replyCount,
  likeCount,
  messageCount,
  1*messageCount + 2*replyCount + 10*likeCount AS score
ORDER BY
  score DESC,
  id ASC
LIMIT 100;