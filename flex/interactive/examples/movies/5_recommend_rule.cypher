MATCH (u: User)-[:FOLLOWS]->(a: Person)-[:ACTED_IN]->(m: Movie)
WHERE NOT (u : User)-[:REVIEW]->(m)
RETURN u.name, m.title LIMIT 5;