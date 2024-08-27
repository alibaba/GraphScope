MATCH 
    (p_:PERSON {id: $personId})-[:KNOWS*1..3]-(other:PERSON)<-[:HASCREATOR]-(p:POST)-[:HASTAG]->(t:TAG {name: $tagName}),
    (p:POST)-[:HASTAG]->(otherTag:TAG)
WHERE 
    p_ <> other AND
    otherTag <> t 
RETURN 
    otherTag.name as name, 
    count(distinct p) as postCnt 
ORDER BY 
    postCnt desc, 
    name asc 
LIMIT 10;