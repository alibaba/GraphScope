Match (person1:Person)-[:LIKES]->(message:Post),
      (message:Post)<-[:CONTAINER_OF]-(person2:Forum),
      (person1:Person)-[:KNOWS]->(place),
      (person2:Forum)-[:HAS_MODERATOR]->(place)
Return count(person1);