Match (p1:PERSON)-[k:KNOWS]->(p2:PERSON)
Where k.creationDate > $date1 and k.creationDate < $date2
Return count(p2);