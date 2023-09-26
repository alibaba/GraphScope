MATCH (tom:Person {name: "Tom Hanks"})-[:ACTED_IN]->(tomHanksMovies)
RETURN tom.born AS bornYear,
    tomHanksMovies.tagline AS movieTagline, 
    tomHanksMovies.title AS movieTitle,
    tomHanksMovies.released AS releaseYear
ORDER BY releaseYear DESC, movieTitle ASC LIMIT 10;