MATCH
                (t1:TITLE)-[mk:MOVIE_KEYWORD]->(k:KEYWORD),
                (t1)<-[:MOVIE_LINK_TITLE]-(ml:MOVIE_LINK)-[:MOVIE_LINK_LINKED_TYPE]->(lt:LINK_TYPE),
                (ml)-[:MOVIE_LINK_LINKED_TITLE]->(t2:TITLE)
                WHERE k.keyword = 'character-name-in-title'
                RETURN
                  MIN(lt.link) AS link_type,
                  MIN(t1.title) AS first_movie,
                  MIN(t2.title) AS second_movie;