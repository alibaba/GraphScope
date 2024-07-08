Match (p1:PERSON)-[:KNOWS]->(p2:PERSON)-[:LIKES]->(comment:COMMENT)
Where p1.id = $id1 and p2.id = $id2 and comment.length > $len 
Return count(p1);