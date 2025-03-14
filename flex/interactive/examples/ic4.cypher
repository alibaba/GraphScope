MATCH (person:PERSON {id: 10995116278874L})-[:KNOWS]-(friend:PERSON)<-[:HASCREATOR]-(post:POST)-[:HASTAG]->(tag: TAG)
WITH tag,
     CASE
       WHEN  post.creationDate >= 1338508800000L AND post.creationDate < 1340928000000L THEN 1
       ELSE 0
     END AS valid,
     CASE
       WHEN post.creationDate < 1338508800000L  THEN 1
       ELSE 0
     END AS inValid
WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
WHERE postCount>0 AND inValidPostCount=0

RETURN tag.name AS tagName, postCount
ORDER BY postCount DESC, tagName ASC
LIMIT 10;