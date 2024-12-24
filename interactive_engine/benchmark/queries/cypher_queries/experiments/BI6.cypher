MATCH (tag:TAG {name: $tag})<-[:HASTAG]-(message1)-[:HASCREATOR]->(person1:PERSON),
      (message1)<-[:LIKES]-(person2:PERSON),
      (person2)<-[:HASCREATOR]-(message2)<-[like:LIKES]-(person3:PERSON)
RETURN
  person1.id,
  // Using 'DISTINCT like' here ensures that each person2's popularity score is only added once for each person1
  count(DISTINCT like) AS authorityScore
ORDER BY
  authorityScore DESC,
  person1.id ASC
LIMIT 100;