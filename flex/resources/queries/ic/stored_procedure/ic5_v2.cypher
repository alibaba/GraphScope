MATCH (person:PERSON { id: $personId })-[:KNOWS*1..2]-(friend)
MATCH (friend)<-[membership:HASMEMBER]-(forum)
WHERE membership.joinDate > $minDate
OPTIONAL MATCH (friend)<-[:HASCREATOR]-(post)<-[:CONTAINEROF]-(forum)
WHERE
  NOT person=friend
WITH
  forum,
  count(distinct post) AS postCount
ORDER BY
  postCount DESC,
  forum.id ASC
LIMIT 20
RETURN
  forum.title AS forumName,
  postCount;