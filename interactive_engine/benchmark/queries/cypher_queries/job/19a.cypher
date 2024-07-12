MATCH
                (n:NAME)<-[:CAST_INFO_NAME]-(ci:CAST_INFO)-[:CAST_INFO_TITLE]->(t:TITLE),
                (t)<-[:MOVIE_COMPANIES_TITLE]-(mc:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_COMPANY_NAME]->(cn:COMPANY_NAME),
                (t)-[mi:MOVIE_INFO]->(it:INFO_TYPE),
                (ci)-[:CAST_INFO_ROLE]->(rt:ROLE_TYPE),
                (ci)-[:CAST_INFO_CHAR]->(chn:CHAR_NAME),
                (an:AKA_NAME)-[:ALSO_KNOWN_AS_NAME]->(n)
                WHERE ci.note IN ['(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)']
                  AND cn.country_code = '[us]'
                  AND it.info = 'release dates'
                  AND mc.note IS NOT NULL
                  AND (mc.note CONTAINS '(USA)' OR mc.note CONTAINS '(worldwide)')
                  AND mi.info IS NOT NULL
                  AND (mi.info CONTAINS 'Japan:.*200.*' OR mi.info CONTAINS 'USA:.*200.*')
                  AND n.gender = 'f'
                  AND n.name CONTAINS 'Ang'
                  AND rt.role = 'actress'
                  AND t.production_year >= 2005
                  AND t.production_year <= 2009
                RETURN
                  MIN(n.name) AS voicing_actress,
                  MIN(t.title) AS voiced_movie;