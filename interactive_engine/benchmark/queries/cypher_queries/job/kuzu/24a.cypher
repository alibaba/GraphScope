MATCH
                (n:NAME)<-[:CAST_INFO_NAME]-(ci:CAST_INFO)-[:CAST_INFO_TITLE]->(t:TITLE)-[:MOVIE_KEYWORD]->(k:KEYWORD),
                (t)<-[:MOVIE_COMPANIES_TITLE]-(mc:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_COMPANY_NAME]->(cn:COMPANY_NAME),
                (t)-[mi:MOVIE_INFO]->(it:INFO_TYPE),
                (ci)-[:CAST_INFO_ROLE]->(rt:ROLE_TYPE),
                (ci)-[:CAST_INFO_CHAR]->(chn:CHAR_NAME),
                (an:AKA_NAME)-[:ALSO_KNOWN_AS_NAME]->(n)
                WHERE ci.note IN ['(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)']
                  AND cn.country_code = '[us]'
                  AND it.info = 'release dates'
                  AND k.keyword IN ['hero', 'martial-arts', 'hand-to-hand-combat']
                  AND mi.info IS NOT NULL
                  AND (mi.info =~ 'Japan:.*201.*' OR mi.info =~ 'USA:.*201.*')
                  AND n.gender = 'f'
                  AND n.name =~ '.*An.*'
                  AND rt.role = 'actress'
                  AND t.production_year > 2010
                RETURN
                  MIN(chn.name) AS voiced_char_name,
                  MIN(n.name) AS voicing_actress_name,
                  MIN(t.title) AS voiced_action_movie_jap_eng;