 MATCH
                  (ct:COMPANY_TYPE)<-[:MOVIE_COMPANIES_TYPE]-(mc:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_TITLE]->(t:TITLE)<-[:KIND_TYPE_TITLE]-(kt:KIND_TYPE),
                  (mc)-[:MOVIE_COMPANIES_COMPANY_NAME]->(cn:COMPANY_NAME),
                  (t)-[mi:MOVIE_INFO]->(it2:INFO_TYPE),
                  (t)-[miidx:MOVIE_INFO_IDX]->(it:INFO_TYPE)
                WHERE cn.country_code = '[de]'
                  AND ct.kind = 'production companies'
                  AND it.info = 'rating'
                  AND it2.info = 'release dates'
                  AND kt.kind = 'movie'
                RETURN
                  MIN(mi.info) AS release_date,
                  MIN(miidx.info) AS rating,
                  MIN(t.title) AS german_movie;