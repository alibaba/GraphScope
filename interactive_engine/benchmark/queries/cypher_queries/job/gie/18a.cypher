MATCH
        (t:TITLE)-[mi:MOVIE_INFO]->(it1:INFO_TYPE),
        (t)-[mi_idx:MOVIE_INFO_IDX]->(it2:INFO_TYPE),
        (t)<-[:CAST_INFO_TITLE]-(ci:CAST_INFO)-[:CAST_INFO_NAME]->(n:NAME)
        WHERE ci.note IN ['(producer)', '(executive producer)']
          AND it1.info = 'budget'
          AND it2.info = 'votes'
          AND n.gender = 'm'
          AND n.name CONTAINS 'Tim'
        RETURN
          MIN(mi.info) AS movie_budget,
          MIN(mi_idx.info) AS movie_votes,
          MIN(t.title) AS movie_title;