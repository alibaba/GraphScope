Match (message:Post)-[:HAS_CREATOR]->(person),
      (message:Post)-[:HAS_TAG]->(tag:Tag),
      (person)-[:HAS_INTEREST]->(tag:Tag)
Return count(person);
