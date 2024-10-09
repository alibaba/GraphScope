Match (message)-[:KNOWS|HAS_MODERATOR]->(person:Person),
      (message)-[]->(tag:Tag),
      (person)-[]->(tag)
Return count(person);
