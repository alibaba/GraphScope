MATCH (tom:Person {name: 'Tom Hanks'})-[:ACTED_IN]->(movie1:Movie)<-[:ACTED_IN]-(coActor:Person)-[:ACTED_IN]->(movie2:Movie)<-[:ACTED_IN]-(cruise:Person {name: 'Tom Cruise'})
WHERE NOT (tom)-[:ACTED_IN]->(:Movie)<-[:ACTED_IN]-(cruise)
RETURN tom.name AS actorName, movie1.title AS movie1Title, coActor.name AS coActorName, movie2.title AS movie2Title, cruise.name AS coCoActorName
ORDER BY movie1Title ASC, movie2Title ASC LIMIT 10;