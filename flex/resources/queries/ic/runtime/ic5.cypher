MATCH (person:PERSON { id: 4398046511592L })-[:KNOWS*1..3]-(friend)
WITH DISTINCT friend
WHERE friend.id <> 4398046511592L
CALL {
  WITH friend
  MATCH (friend)<-[membership:HASMEMBER]-(forum)
  WHERE membership.joinDate > 1346544000000L
  WITH distinct forum
  ORDER BY forum.id ASC
  LIMIT 20
  RETURN forum, 0L AS postCount
}
UNION
CALL {
  WITH friend
  MATCH (friend)<-[membership:HASMEMBER]-(forum)
  WHERE membership.joinDate > 1346544000000L
  WITH friend, collect(distinct forum) AS forums
  MATCH (friend)<-[:HASCREATOR]-(post)<-[:CONTAINEROF]-(forum)
  WHERE forum IN forums
  WITH forum, count(post) AS postCount
  RETURN forum, postCount
  ORDER BY postCount DESC, forum.id ASC
  LIMIT 20
}
WITH forum, max(postCount) AS postCount
ORDER BY postCount DESC, forum.id ASC
LIMIT 20

RETURN forum.title as forumTitle, postCount;