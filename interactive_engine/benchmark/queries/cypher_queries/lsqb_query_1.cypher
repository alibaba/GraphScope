MATCH (forum)-[:hasMember]->(person),
(person)-[:isLocatedIn]->(city),
(city)-[:isPartOf]->(country),
(forum)-[:containerOf]->(post),
(comment)-[:hasTag]->(tag),
(tag)-[:hasType]->(tagClass)
RETURN COUNT(forum) 