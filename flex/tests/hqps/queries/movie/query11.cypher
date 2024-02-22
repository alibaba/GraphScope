MATCH (tom:Person {name: 'Tom Hanks'})-[r:ACTED_IN]->(movie:Movie)
WITH movie.title as movieTitle, movie.released as movieReleased
ORDER BY movieReleased DESC, movieTitle ASC LIMIT 10
return movieTitle, movieReleased;