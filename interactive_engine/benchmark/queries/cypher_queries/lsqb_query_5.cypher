MATCH (message)-[:hasTag]->(tag1),
(comment)-[:hasTag]->(tag2),
(comment)-[:replyOf]->(message)
WHERE tag1<>tag2
RETURN COUNT(message)