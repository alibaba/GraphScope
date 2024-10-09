MATCH
                (cct2:COMP_CAST_TYPE)<-[:COMPLETE_CAST_STATUS]-(cc:COMPLETE_CAST)-[:COMPLETE_CAST_TITLE]->(t:TITLE),
                (cc)-[:COMPLETE_CAST_SUBJECT]->(cct1:COMP_CAST_TYPE),
                (t)-[:MOVIE_KEYWORD]->(k:KEYWORD),
                (t)<-[:KIND_TYPE_TITLE]-(kt:KIND_TYPE),
                (t)<-[:CAST_INFO_TITLE]-(ci:CAST_INFO)-[:CAST_INFO_CHAR]->(chn:CHAR_NAME),
                (ci)-[:CAST_INFO_NAME]->(n:NAME)
                WHERE cct1.kind = 'cast'
                  AND cct2.kind CONTAINS 'complete'
                  AND NOT chn.name CONTAINS 'Sherlock'
                  AND (chn.name CONTAINS 'Tony Stark' OR chn.name CONTAINS 'Iron Man')
                  AND k.keyword IN ['superhero', 'sequel', 'second-part', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence']
                  AND kt.kind = 'movie'
                  AND t.production_year > 1950
                RETURN
                  MIN(t.title) AS complete_downey_ironman_movie;