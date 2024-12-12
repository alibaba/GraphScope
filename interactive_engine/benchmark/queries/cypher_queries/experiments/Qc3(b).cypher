Match (p:COMMENT)-[]->(:PERSON)-[]->(:CITY),
(p)<-[]-(message),
(message)-[]->(tag:TAG)
Return count(p);