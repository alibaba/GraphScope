Match (p:COMMENT)-[]->(p2:PERSON)-[]->(c:CITY),
    	(p)<-[]-(message),
      (message)-[]->(tag:TAG)
Return count(c);