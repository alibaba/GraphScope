MATCH (p: Paper)-[:WorkOn]->(t:Task),
      (p)-[:Has]->(c:Challenge)-[:SolvedBy]->(s:Solution),
      (p)-[:Use]->(s:Solution)
WHERE t.name = "Distributed Subgraph Matching Efficiency"
WITH t, c, count(p) AS num
RETURN t.name, c.name, num