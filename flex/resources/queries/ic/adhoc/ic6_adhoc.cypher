MATCH (p_:PERSON {id: 6597069812321})-[:KNOWS*1..3]-(other:PERSON)<-[:HASCREATOR]-(p:POST)-[:HASTAG]->(t:TAG {name: "William_Wordsworth"}),
(p:POST)-[:HASTAG]->(otherTag:TAG) WHERE otherTag <> t RETURN otherTag.name as name, count(distinct p) as postCnt 
ORDER BY postCnt desc, name asc LIMIT 10