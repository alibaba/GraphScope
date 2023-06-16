:submit MATCH (p:person {id:15393162790207})-[k:knows*1..3]-(other:person)<-[hasMem:hasMember]-(f:forum), (p:person {id:15393162790207})-[k:knows*1..3]-(other:person)<-[:hasCreator]-(po:post)<-[:containerOf]-(f:forum) WHERE hasMem.joinDate > 1344643200000 RETURN f.title as title, f.id as id, count(distinct po) AS postCount ORDER BY postCount DESC, id ASC LIMIT 20
:submit MATCH (p:person {id:15393162790207})-[k:knows*1..3]-(other:person)<-[hasMem:hasMember]-(f:forum)-[:containerOf]->(po:post), (po:post)-[:hasCreator]->(other:person) WHERE hasMem.joinDate > 1344643200000 WITH f as f, count(distinct po) AS postCount ORDER BY postCount DESC, f.id ASC LIMIT 20  RETURN f.title as title, postCount 

:submit MATCH (p:person {id:15393162790207})-[k:knows*1..3]-(other:person)<-[hasMem:hasMember]-(f:forum), (f:forum)-[:containerOf]->(po:post)-[:hasCreator]->(other:person) WHERE hasMem.joinDate > 1344643200000 WITH f as f, count(distinct po) AS postCount ORDER BY postCount DESC, f.id ASC LIMIT 20  RETURN f.title as title, postCount 

//dyn param
:submit MATCH (p:person {id: $personId})-[k:knows*1..3]-(other:person)<-[hasMem:hasMember]-(f:forum), (f:forum)-[:containerOf]->(po:post)-[:hasCreator]->(other:person) WHERE hasMem.joinDate > $minDate WITH f as f, count(distinct po) AS postCount ORDER BY postCount DESC, f.id ASC LIMIT 20  RETURN f.title as title, postCount 


//adhoc, inner join
MATCH (p:person {id: 40741})-[k:knows*1..3]-(other:person)<-[hasMem:hasMember]-(f:forum), (f:forum)-[:containerOf]->(po:post)-[:hasCreator]->(other:person) WHERE hasMem.joinDate > 1346889600000 WITH f as f, count(distinct po) AS postCount ORDER BY postCount DESC, f.id ASC LIMIT 20  RETURN f.title as title, postCount 