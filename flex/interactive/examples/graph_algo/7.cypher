MATCH (p:Paper)-[:HasField]->(c:CCFField),
              (p1:Paper)-[:HasField]->(c),
               (p)-[b:Citation]-(p1)
WITH distinct p, p1
RETURN p.CCFField, p1.CCFField;