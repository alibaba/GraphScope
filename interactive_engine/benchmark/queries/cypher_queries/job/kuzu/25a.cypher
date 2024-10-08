MATCH
        (t:TITLE)-[mi:MOVIE_INFO]->(it1:INFO_TYPE),
        (t)-[mi_idx:MOVIE_INFO_IDX]->(it2:INFO_TYPE),
        (t)<-[:CAST_INFO_TITLE]-(ci:CAST_INFO)-[:CAST_INFO_NAME]->(n:NAME),
        (t)-[mk:MOVIE_KEYWORD]->(k:KEYWORD)
        WHERE ci.note IN ['(writer)', '(head writer)', '(written by)', '(story)', '(story editor)']
          AND it1.info = 'genres'
          AND it2.info = 'votes'
          AND k.keyword IN ['murder', 'blood', 'gore', 'death', 'female-nudity']
          AND mi.info = 'Horror'
          AND n.gender = 'm'
        RETURN
          MIN(mi.info) AS movie_budget,
          MIN(mi_idx.info) AS movie_votes,
          MIN(n.name) AS male_writer,
          MIN(t.title) AS violent_movie_title;