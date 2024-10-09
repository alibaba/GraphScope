Match (p1)<-[]-(p2:Post),
      (p1)<-[:HAS_MODERATOR]-()-[]->(p2)
Return count(p1);
