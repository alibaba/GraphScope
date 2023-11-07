MATCH (u: User)-[:REVIEW]->(m: Movie)
WITH u, COUNT(m) as cnt1
MATCH (u)-[r:REVIEW]->(likeM: Movie)
WHERE r.rating > $rateThresh
WITH u, cnt1, COUNT(likeM) as cnt2
RETURN u.name, cnt2 / cnt1;