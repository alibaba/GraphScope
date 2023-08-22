MATCH(a)-[b: CONTAINEROF]->(c) return c.id AS postId ORDER BY postId ASC LIMIT 10;
