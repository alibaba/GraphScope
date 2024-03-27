MATCH (person:PERSON { id: $personId })-[:KNOWS*1..2]-(friend)
WHERE
    NOT person=friend
WITH DISTINCT friend
MATCH (friend)<-[membership:HASMEMBER]-(forum)
WHERE
    membership.joinDate > $minDate
WITH 
    friend AS friend, 
    forum AS forum
OPTIONAL MATCH (friend)<-[:HASCREATOR]-(post)<-[:CONTAINEROF]-(forum)
WITH
    forum,
    count(post) AS postCount
ORDER BY
    postCount DESC,
    forum.id ASC
LIMIT 20
RETURN
    forum.title AS forumName,
    postCount