MATCH (actor:Person {name: $actorName})-[:ACTED_IN]->(movie:Movie)
RETURN actor.name, movie.title, movie.tagline, movie.released;