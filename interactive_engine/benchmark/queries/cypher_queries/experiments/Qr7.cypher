Match (p1: PERSON {id:$id})-[:KNOWS*3..4]->(:PERSON) 
UNION (p1: PERSON {id:$id})-[:KNOWS*4..5]->(:PERSON),
Return count(p1);