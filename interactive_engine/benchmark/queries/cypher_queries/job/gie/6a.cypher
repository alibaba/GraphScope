MATCH
          (t:TITLE)<-[ci_rel:CAST_INFO_TITLE]-(ci:CAST_INFO)-[:CAST_INFO_NAME]->(n:NAME),
          (t)-[mk:MOVIE_KEYWORD]->(k:KEYWORD)
        WHERE k.keyword = 'marvel-cinematic-universe'
          AND n.name CONTAINS 'Downey' AND n.name CONTAINS 'Robert'
          AND t.production_year > 2010
        RETURN
          MIN(k.keyword) AS movie_keyword,
          MIN(n.name) AS actor_name,
          MIN(t.title) AS marvel_movie;