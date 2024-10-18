MATCH
          (t:TITLE)-[mi:MOVIE_INFO]->(:INFO_TYPE),
          (t)-[mk:MOVIE_KEYWORD]->(k:KEYWORD)
        WHERE k.keyword CONTAINS 'sequel'
          AND mi.info IN ['Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German']
          AND t.production_year > 2005
        RETURN MIN(t.title) AS movie_title;