MATCH (actor:Person {name: $actorName})-[:ACTED_IN]->(movie1:Movie)<-[:ACTED_IN]-
	  (coActor:Person)-[:ACTED_IN]->(movie2:Movie)<-[:ACTED_IN]-(coCoActor:Person)
WHERE actor <> coCoActor
AND NOT (actor)-[:ACTED_IN]->(:Movie)<-[:ACTED_IN]-(coCoActor)
RETURN DISTINCT coCoActor.name LIMIT 10;