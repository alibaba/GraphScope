MATCH (p:Paper)-[:WorkOn]->(:Algorithm)-[:Belong]->(ca:Category)
WITH distinct ca, count(p) as paperCount
RETURN ca.category, paperCount
ORDER BY paperCount desc;