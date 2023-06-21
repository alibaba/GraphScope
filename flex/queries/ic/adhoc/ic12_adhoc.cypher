MATCH (unused:person {id: 8796093037034})-[:knows]-(friend:person)<-[:hasCreator]-(comments:comment)-
[:replyOf]->(:post)-[:hasTag]->(tags:tag)-[:hasType]->(:tagClass)-[:isSubclassOf*0..10]->(:tagClass {name: "MartialArtist"})
 with friend AS friend, collect(DISTINCT tags.name) AS tagNames, count(DISTINCT comments) AS replyCount 
 ORDER BY replyCount DESC, friend.id ASC LIMIT 20 return friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, tagNames, replyCount