MATCH (ca:Category)<-[:Belong]-(a:Algorithm),
       (a)<-[:WorkOn]-(p:Paper)-[:Use]->(s:Strategy),
       (s)-[:ApplyOn]->(ch:Challenge)
WITH ca, ch, count(p) AS num
RETURN ca.category AS category, ch.challenge AS challenge, num
ORDER BY num DESC LIMIT 5;