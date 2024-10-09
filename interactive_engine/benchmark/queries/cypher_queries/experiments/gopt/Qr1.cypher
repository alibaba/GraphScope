Match (person1:PERSON)<-[:HASCREATOR]-(comment:COMMENT), 
	    (comment:COMMENT)-[:REPLYOF]->(post:POST),
	    (post:POST)<-[:CONTAINEROF]-(forum:FORUM),
	    (forum:FORUM)-[:HASMEMBER]->(person2:PERSON)
Return count(forum.title);