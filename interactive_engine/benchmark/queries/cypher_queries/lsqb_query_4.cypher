MATCH (message)-[:hasTag]->(tag),
(message)-[:hasCreator]->(creator),
(liker)-[:likes]->(message),
(comment)-[:replyOf]->(message)
RETURN COUNT(message) 