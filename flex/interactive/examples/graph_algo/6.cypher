MATCH (p:Paper)-[:Resolve]->(ch:Challenge),
              (p1:Paper)-[:Resolve]->(ch),
               (p)-[c:Citation]->(p1)
WITH DISTINCT p, c, p1
WITH p.id As paperId, p1.id As citationId
RETURN paperId, citationId ORDER BY paperId ASC, citationId ASC;