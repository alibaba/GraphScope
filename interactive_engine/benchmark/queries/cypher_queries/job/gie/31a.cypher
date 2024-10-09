MATCH
        (t:TITLE)<-[:CAST_INFO_TITLE]-(ci:CAST_INFO)-[:CAST_INFO_NAME]->(n:NAME),
        (t)-[mi:MOVIE_INFO]->(it1:INFO_TYPE),
        (t)-[mi_idx:MOVIE_INFO_IDX]->(it2:INFO_TYPE),
        (t)-[mk:MOVIE_KEYWORD]->(k:KEYWORD),
        (t)<-[:MOVIE_COMPANIES_TITLE]-(mc:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_COMPANY_NAME]->(cn:COMPANY_NAME)
        WHERE ci.note IN ['(writer)', '(head writer)', '(written by)', '(story)', '(story editor)']
          AND cn.name STARTS WITH 'Lionsgate'
          AND it1.info = 'genres'
          AND it2.info = 'votes'
          AND k.keyword IN ['murder', 'violence', 'blood', 'gore', 'death', 'female-nudity', 'hospital']
          AND mi.info IN ['Horror', 'Thriller']
          AND n.gender = 'm'
        RETURN
          MIN(mi.info) AS movie_budget,
          MIN(mi_idx.info) AS movie_votes,
          MIN(n.name) AS writer,
          MIN(t.title) AS violent_liongate_movie;