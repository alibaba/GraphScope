MATCH (p_:PERSON {id: $personId})-[:KNOWS*1..3]-(other:PERSON),
      (other)<-[:HASCREATOR]-(p:POST)-[:HASTAG]->(t:TAG {name: $tagName}),
      (p:POST)-[:HASTAG]->(otherTag:TAG)

WHERE other.id <> $personId AND otherTag <> t

WITH DISTINCT
      otherTag,
      p

RETURN
    otherTag.name as name,
    count(p) as postCnt 
ORDER BY 
    postCnt desc, 
    name asc 
LIMIT 10;