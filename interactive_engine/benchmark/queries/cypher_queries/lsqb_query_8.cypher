MATCH (message)-[:hasTag]->(tag1),
(comment)-[:hasTag]->(tag2),
(comment)-[:replyOf]->(message)
WHERE not (comment)-[:hasTag]->(tag1) and tag1<>tag2
RETURN COUNT(message)