MATCH  (p1:PERSON {id:$personId})-[:PERSON_KNOWS_PERSON]-(p2:PERSON)<-[:POST_HASCREATOR_PERSON]-(m:POST)-[:POST_HASTAG_TAG]->(t1:TAG {name:'$tagName'}),
        (m)-[:POST_HASTAG_TAG]->(t2:TAG)
WHERE t2.name <> '$tagName'
RETURN COUNT(p1)