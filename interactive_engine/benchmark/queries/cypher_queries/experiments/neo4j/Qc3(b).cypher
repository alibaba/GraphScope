Match (p:Comment)-[]->(:Person)-[]->(:Place),
      (p)<-[]-(message),
      (message)-[]->(tag:Tag)
Return count(p);