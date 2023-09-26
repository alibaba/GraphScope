MATCH (tom:Person {name:$actorName})-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(coActors)
RETURN m.title AS movieTitle, m.released AS releasedYear, coActors.name AS coActorName
ORDER BY releasedYear DESC, movieTitle ASC LIMIT 10;