Match (c:PLACE {name: $name})<-[:ISLOCATEDIN]-(p1:PERSON),
      (c)<-[:ISLOCATEDIN]-(p2:PERSON),
      (p1)<-[:HASCREATOR]-(m1:COMMENT)<-[:LIKES]->(p2:PERSON),
       (c:PLACE {name: $name})<-[:ISLOCATEDIN]-(p3:PERSON),
       (c)<-[:ISLOCATEDIN]-(p4:PERSON),
       (p3)<-[:HASCREATOR]-(m2:COMMENT)<-[:LIKES]->(p4:PERSON)
RETURN count(c)