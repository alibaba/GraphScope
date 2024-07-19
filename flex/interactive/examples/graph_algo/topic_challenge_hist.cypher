MATCH (t: Topic)<-[:Belong]-(a:Task),
    (a)<-[:WorkOn]-(p:Paper)-[:Use]->(s:Solution),
    (s)-[:ApplyOn]->(ch:Challenge)
WHERE t.topic = $topic_name
RETURN t.topic, ch.challenge, COUNT(p);