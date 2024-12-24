MATCH
  (comment)-[:HASTAG]->(tag:TAG {name: $tag}),
  (comment)-[:REPLYOF]->(message2),
  (message2)-[:HASTAG]->(tag),
  (message1)-[:HASTAG]->(tag:TAG {name: $tag}),
  (message1)-[:REPLYOF*0..10]->(post1:POST)<-[:CONTAINEROF]-(forum1:FORUM),
  (forum1)-[:HASMEMBER]->(person3:PERSON)<-[:HASCREATOR]-(message2)
RETURN count(*);