Match (p1:PERSON)-[:KNOWS]->(p2:PERSON)
Where p1.id = $id1 and p2.id = $id2
Return count(p1);