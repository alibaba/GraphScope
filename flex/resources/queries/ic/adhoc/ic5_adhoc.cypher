MATCH (p:PERSON {id: 15393162790207})-[k:KNOWS*1..3]-(other:PERSON)<-[hasMem:HASMEMBER]-(f:FORUM), 
(f:FORUM)-[:CONTAINEROF]->(po:POST)-[:HASCREATOR]->(other:PERSON) WHERE hasMem.joinDate > 1344643200000 
WITH f as f, count(distinct po) AS postCount ORDER BY postCount DESC, f.id ASC LIMIT 20  RETURN f.title as title, postCount;