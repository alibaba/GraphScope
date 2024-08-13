Match (forum)-[:CONTAINER_OF]->(post:Post),
      (forum)-[:HAS_MEMBER]->(person1:Person),
      (forum)-[:HAS_MEMBER]->(person2:Person),
      (person1:Person)-[:KNOWS]->(person2:Person),
      (person1:Person)-[:LIKES]->(post:Post),
      (person2:Person)-[:LIKES]->(post:Post)
Return count(person1);