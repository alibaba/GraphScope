MATCH (u: User)-[:REVIEW]->(m: Movie)<-[:ACTED_IN]-(actor: Person),
              (u)-[:FOLLOWS]->(actor)
WITH DISTINCT u, COUNT(m) as cnt1
MATCH (u: User)-[r:REVIEW]->(likeM: Movie)<-[:ACTED_IN]-(actor: Person)
MATCH (u:User)-[:FOLLOWS]->(actor)
WHERE r.rating > $rateThresh
WITH DISTINCT u, cnt1, COUNT(likeM) as cnt2
RETURN u.name, cnt2 / cnt1;