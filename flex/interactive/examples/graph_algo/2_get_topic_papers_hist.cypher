MATCH (p:Paper)-[:WorkOn]->(:Task)-[:Belong]->(t:Topic)
WITH DISTINCT t, COUNT(p) AS paperCount
RETURN t.topic AS topic, paperCount
ORDER BY paperCount DESC;
