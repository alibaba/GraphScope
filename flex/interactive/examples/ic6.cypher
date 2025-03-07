MATCH (p_:PERSON {id: 19791209300317L})-[:KNOWS*1..3]-(other:PERSON)
WITH distinct other
WHERE  other.id <> 19791209300317L

MATCH (other)<-[:HASCREATOR]-(p:POST)-[:HASTAG]->(t:TAG {name: "Nat_King_Cole"})
MATCH    (p:POST)-[:HASTAG]->(otherTag:TAG)

WITH otherTag, t,count(distinct p) as postCnt
WHERE 
    otherTag <> t 
RETURN
    otherTag.name as name,
    postCnt 
ORDER BY 
    postCnt desc, 
    name asc 
LIMIT 10;