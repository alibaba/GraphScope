MATCH (cloudAtlas:Movie {title: "Cloud Atlas"})<-[:DIRECTED]-(directors)
RETURN directors.name AS directorsName ORDER BY directorsName ASC LIMIT 10;