MATCH (p_:PERSON {id: 19791209300317L})-[:KNOWS*1..3]-(other:PERSON)
WITH distinct other
WHERE  other.id <> 19791209300317L

MATCH (other)<-[:HASCREATOR]-(p:POST)-[:HASTAG]->(t:TAG {name: "Nat_King_Cole"})
MATCH    (p:POST)-[:HASTAG]->(otherTag:TAG)

WITH otherTag, t,count(distinct p) as postCount
WHERE 
    otherTag <> t 
RETURN
    otherTag.name as tagName,
    postCount
ORDER BY 
    postCount desc, 
    tagName asc 
LIMIT 10;