MATCH (tom:Person {name: 'Tom Hanks'})-[:ACTED_IN]->(:Movie)<-[:ACTED_IN]-(coActor:Person)
WITH DISTINCT coActor.name AS coActorName ORDER BY coActorName ASC LIMIT 10 return coActorName;