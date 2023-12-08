MATCH (p:Paper)-[:HasField]->(c:CCFField),
              (p1:Paper)-[:HasField]->(c),
               (p)-[c:Citation]-(p1)
RETURN distinct p, p1;