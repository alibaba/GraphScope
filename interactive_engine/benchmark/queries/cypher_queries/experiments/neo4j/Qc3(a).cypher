Match (person1)<-[:HAS_CREATOR]-(comment:Comment),
      (comment:Comment)-[:REPLY_OF]->(post:Post),
      (post:Post)<-[:CONTAINER_OF]-(forum),
      (forum)-[:HAS_MEMBER]->(person2)
Return count(person1);