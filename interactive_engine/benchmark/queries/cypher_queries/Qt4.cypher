Match (p1)<-[]-(p2:POST),
(p1)<-[:HASMODERATOR]-()-[]->(p2)
Return count(p1);