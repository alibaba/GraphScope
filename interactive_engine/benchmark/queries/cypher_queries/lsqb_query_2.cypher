MATCH (p1)-[:knows]-(p2),
(p1)<-[:hasCreator]-(c),
(p2)<-[:hasCreator]-(p),
(c)-[:replyOf]->(p:post)
RETURN COUNT(p1) 