Match (p1: PERSON {id:1243})-[:KNOWS*3..4]->(:PERSON) 
UNION (p1: PERSON {id:1243})-[:KNOWS*4..5]->(:PERSON),
Return count(p1);