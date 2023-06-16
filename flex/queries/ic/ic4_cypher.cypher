// Since where not is not supported now, we only look at right side

MATCH (unused:person {id:10995116278874})-[:knows]-(friend:person)<-[:hasCreator]-(post2:post)-[:hasTag]->(t: tag)
WHERE 1338508800000 <= post1.creationDate AND post1.creationDate < 1340928000000 and post2.creationDate < 1338508800000 RETURN t.name as name, count(post1) as postCnt ORDER BY postCnt desc, name asc LIMIT 10

//supported when (where not exists) is supported
MATCH (unused:person {id:10995116278874})-[:knows]-(friend:person)<-[:hasCreator]-(post1:post)-[:hasTag]->(t: tag),WHERE 1275350400000 <= post1.creationDate < 1277856000000 AND
NOT EXISTS {
	MATCH (unused)-[:knows]-(friend2:person)<-[:hasCreator]-(post2:post)-[:hasTag]->(t)
	WHERE post2.creationDate < 1275350400000
}
RETURN tag.name as name, count(post1) as postCnt
ORDER BY postCnt desc, name asc
LIMIT 10


:submit MATCH (unused:person {id:10995116278874})-[:knows]-(friend:person)<-[:hasCreator]-(post2:post)-[:hasTag]->(t: tag) WHERE 1338508800000 <= post1.creationDate AND post1.creationDate < 1340928000000 and post2.creationDate < 1338508800000 RETURN t.name as name, count(post1) as postCnt ORDER BY postCnt desc, name asc LIMIT 10

//adhoc
//todo: support antijoin