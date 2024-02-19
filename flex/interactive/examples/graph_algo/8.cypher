MATCH (p:Paper)-[:HasField]->(f1:CCFField),
              (p1:Paper)-[:HasField]->(f2:CCFField),
               (p)-[c:Citation]->(p1)
WHERE f1.id = 0 AND f2.id <> f1.id
RETURN distinct p, p1;