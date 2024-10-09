MATCH
        (t:TITLE)-[mi:MOVIE_INFO]->(it1:INFO_TYPE),
        (t)-[mi_idx:MOVIE_INFO_IDX]->(it2:INFO_TYPE),
        (t)-[:MOVIE_KEYWORD]->(k:KEYWORD),
        (t)<-[:KIND_TYPE_TITLE]-(kt:KIND_TYPE)
        WHERE it1.info = 'countries'
          AND it2.info = 'rating'
          AND k.keyword IN ['murder', 'murder-in-title', 'blood', 'violence']
          AND kt.kind = 'movie'
          AND mi.info IN ['Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German', 'USA', 'American']
          AND mi_idx.info < '8.5'
          AND t.production_year > 2010
        RETURN
          MIN(mi_idx.info) AS rating,
          MIN(t.title) AS northern_dark_movie;