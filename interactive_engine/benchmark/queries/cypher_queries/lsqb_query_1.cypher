MATCH (forum)-[:hasMember]->(person),
(person)-[:isLocatedIn]->(city),
(city)-[:isPartOf]->(country),
(forum)-[:containerOf]->(post),
(post)<-[:replyOf]-(comment),
(comment)-[:hasTag]->(tag),
(tag)-[:hasType]->(tagClass)
RETURN COUNT(forum) 