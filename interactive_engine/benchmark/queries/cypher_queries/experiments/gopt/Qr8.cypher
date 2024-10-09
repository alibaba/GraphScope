Match (p1: PERSON {id:1243})-[:KNOWS*4..5]->(:PERSON) 
UNION (p1: PERSON {id:1243})-[:KNOWS*5..6]->(:PERSON),
Return count(p1);