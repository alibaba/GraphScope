MATCH (p1:Paper)<-[:Cite]-(p2:Paper)<-[:Cite]-(p3:Paper),
      (p1)-[:Has]->(c)-[:SolvedBy]->(s1:Solution)
WHERE c.name = "Optimizing Load Balance in Distributed Systems" and p1.title = "Parallel Subgraph Listing in a Large-Scale Graph"
WITH p1.title AS paper1, p2, p3, c, collect(s1.description) AS solutions1
MATCH (p2)-[:Has]->(c)-[:SolvedBy]->(s2:Solution),
      (p2)-[:Use]->(s2)
WHERE p2.title = "Scalable distributed subgraph enumeration"
WITH paper1, p2.title AS paper2, p3, c, solutions1, collect(s2.description) AS solutions2
MATCH (p3)-[:Has]->(c)-[:SolvedBy]->(s3:Solution),
      (p3)-[:Use]->(s3)
WHERE p3.title = "HUGE: An Efficient and Scalable Subgraph Enumeration System"
WITH paper1, paper2, p3.title as paper3, c.name as challenge, solutions1, solutions2, collect(s3.description) as solutions3
RETURN paper1, paper2, paper3, challenge, solutions1, solutions2, solutions3