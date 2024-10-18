MATCH
                     (t:TITLE)<-[:COMPLETE_CAST_TITLE]-(cc:COMPLETE_CAST)-[:COMPLETE_CAST_SUBJECT]->(cct1:COMP_CAST_TYPE),
                     (cc)-[:COMPLETE_CAST_STATUS]->(cct2:COMP_CAST_TYPE),
                     (t)-[mi:MOVIE_INFO]->(it1:INFO_TYPE),
                     (t)-[mi_idx:MOVIE_INFO_IDX]->(it2:INFO_TYPE),
                     (t)<-[:CAST_INFO_TITLE]-(ci:CAST_INFO)-[:CAST_INFO_NAME]->(n:NAME),
                     (t)-[mk:MOVIE_KEYWORD]->(k:KEYWORD)
                     WHERE cct1.kind IN ['cast', 'crew']
                       AND cct2.kind = 'complete+verified'
                       AND ci.note IN ['(writer)', '(head writer)', '(written by)', '(story)', '(story editor)']
                       AND it1.info = 'genres'
                       AND it2.info = 'votes'
                       AND k.keyword IN ['murder', 'violence', 'blood', 'gore', 'death', 'female-nudity', 'hospital']
                       AND mi.info IN ['Horror', 'Thriller']
                       AND n.gender = 'm'
                       AND t.production_year > 2000
                     RETURN
                       MIN(mi.info) AS movie_budget,
                       MIN(mi_idx.info) AS movie_votes,
                       MIN(n.name) AS writer,
                       MIN(t.title) AS complete_violent_movie;