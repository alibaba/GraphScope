:submit MATCH (p_:person {id:30786325579101})-[:knows*1..3]-(other:person)<-[:hasCreator]-(p:post)-[:hasTag]->(t:tag {name:'Shakira'}),(p:post)-[:hasTag]->(otherTag:tag) WHERE otherTag <> t RETURN otherTag.name as name, count(distinct p) as postCnt ORDER BY postCnt desc, name asc LIMIT 10

//dyn
:submit MATCH (p_:person {id:$personId})-[:knows*1..3]-(other:person)<-[:hasCreator]-(p:post)-[:hasTag]->(t:tag {name:$tagName}),(p:post)-[:hasTag]->(otherTag:tag) WHERE otherTag <> t RETURN otherTag.name as name, count(distinct p) as postCnt ORDER BY postCnt desc, name asc LIMIT 10


//adhoc
MATCH (p_:person {id: 6597069812321})-[:knows*1..3]-(other:person)<-[:hasCreator]-(p:post)-[:hasTag]->(t:tag {name: "William_Wordsworth"}),(p:post)-[:hasTag]->(otherTag:tag) WHERE otherTag <> t RETURN otherTag.name as name, count(distinct p) as postCnt ORDER BY postCnt desc, name asc LIMIT 10