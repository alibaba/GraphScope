MATCH (t: Topic)<-[:Belong]-(a:Task),
    (a)<-[:WorkOn]-(p:Paper)-[:Use]->(s:Solution),
    (s)-[:ApplyOn]->(ch:Challenge)
WHERE t.category = $topic_name
RETURN t.category, ch.challenge, COUNT(p);