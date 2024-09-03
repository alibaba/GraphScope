MATCH (t:Topic)<-[:Belong]-(ta:Task),
       (ta)<-[:WorkOn]-(p:Paper)-[:Use]->(s:Solution),
       (s)-[:ApplyOn]->(ch:Challenge)
WHERE t.topic = $topic
WITH t, ch, count(p) AS num
RETURN t.topic AS topic, ch.challenge AS challenge, num
ORDER BY num DESC;
