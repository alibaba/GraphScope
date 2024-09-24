MATCH (person:PERSON {id: $personId})-[:KNOWS]-(friend:PERSON),
      (friend)<-[:HASCREATOR]-(post:POST)-[:HASTAG]->(tag)
WITH DISTINCT tag, post
WITH tag,
     CASE
       WHEN post.creationDate < $endDate AND post.creationDate >= $startDate THEN 1
       ELSE 0
     END AS valid,
     CASE
       WHEN post.creationDate < $startDate THEN 1
       ELSE 0
     END AS inValid
WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
WHERE postCount>0 AND inValidPostCount=0
RETURN tag.name AS tagName, postCount
ORDER BY postCount DESC, tagName ASC
LIMIT 10