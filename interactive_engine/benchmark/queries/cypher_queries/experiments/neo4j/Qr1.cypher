Match (p1:Person)-[:KNOWS]->(p2:Person)
Where p1.id < 933
Return count(p1);