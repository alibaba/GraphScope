MATCH (p:person {id: 40741})-[k:knows*1..3]-(other:person)<-[hasMem:hasMember]-(f:forum), 
(f:forum)-[:containerOf]->(po:post)-[:hasCreator]->(other:person) WHERE hasMem.joinDate > 1346889600000 
WITH f as f, count(distinct po) AS postCount ORDER BY postCount DESC, f.id ASC LIMIT 20  RETURN f.title as title, postCount 