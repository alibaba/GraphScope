MATCH (person:PERSON { id: $personId })-[:KNOWS*1..3]-(friend),
      (friend)<-[membership:HASMEMBER]-(forum)
OPTIONAL MATCH (friend)<-[:HASCREATOR]-(post)<-[:CONTAINEROF]-(forum)
WHERE
    NOT friend.id = $personId
    AND membership.joinDate > $minDate 
WITH
    DISTINCT
    friend AS friend, 
    forum AS forum,
    post as post
      
WITH
    forum,
    count(post) AS postCount
ORDER BY
    postCount DESC,
    forum.id ASC
LIMIT 20
RETURN
    forum.title AS forumName,
    postCount;