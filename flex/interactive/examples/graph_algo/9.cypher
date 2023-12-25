MATCH (p:Paper)-[:Resolve]->(ch:Challenge),
              (p1:Paper)-[:Resolve]->(ch),
               (p)-[c:Citation]-(p1)
RETURN COUNT(distinct p, p1);