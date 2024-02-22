MATCH(a:PERSON)-[b: STUDYAT]->(c) RETURN b.classYear AS classYear ORDER BY classYear DESC LIMIT 10;
