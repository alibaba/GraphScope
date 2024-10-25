Match (forum)-[:HAS_TAG]->(post:Tag),
      (forum)-[:HAS_MODERATOR]->(person1),
      (forum)-[:HAS_MODERATOR|CONTAINER_OF]->(person2),
      (person1)-[:KNOWS|LIKES]->(person2),
      (person1)-[:HAS_INTEREST]->(post:Tag),
      (person2)-[:HAS_INTEREST|HAS_TAG]->(post:Tag)
Return count(person1);