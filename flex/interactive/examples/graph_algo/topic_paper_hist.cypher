MATCH (p:Paper)-[:WorkOn]->(a:Task),(a)-[:Belong]->(t: Topic)
RETURN t.category,COUNT(p);