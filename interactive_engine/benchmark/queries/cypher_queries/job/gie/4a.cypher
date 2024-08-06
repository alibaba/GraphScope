MATCH
          (t:TITLE)-[mi_idx:MOVIE_INFO_IDX]->(it:INFO_TYPE),
          (t)-[mk:MOVIE_KEYWORD]->(k:KEYWORD)
        WHERE it.info = 'rating'
          AND k.keyword CONTAINS 'sequel'
          AND mi_idx.info > '5.0'
          AND t.production_year > 2005
        RETURN MIN(mi_idx.info) AS rating,
               MIN(t.title) AS movie_title;