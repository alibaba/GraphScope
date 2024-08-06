MATCH (person:PERSON {id:$personId})-[:KNOWS*1..3]-(other)<-[:HASCREATOR]-(post:POST)-[:HASTAG]->(tag:TAG {name:'$tagName'}),
      (post)-[:HASTAG]->(otherTag:TAG)
WHERE otherTag <> tag
RETURN otherTag.name as name, count(distinct post) as postCnt
ORDER BY postCnt desc, name asc
LIMIT 10