MATCH
                (mc:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_TITLE]->(t:TITLE)-[:MOVIE_KEYWORD]->(k:KEYWORD),
                (mc)-[:MOVIE_COMPANIES_COMPANY_NAME]->(cn:COMPANY_NAME),
                (mc)-[:MOVIE_COMPANIES_TYPE]->(ct:COMPANY_TYPE),
                (t)-[mi:MOVIE_INFO]->(it1:INFO_TYPE),
                (t)-[mi_idx:MOVIE_INFO_IDX]->(it2:INFO_TYPE),
                (t)<-[:KIND_TYPE_TITLE]-(kt:KIND_TYPE)
                WHERE cn.country_code <> '[us]'
                  AND it1.info = 'countries'
                  AND it2.info = 'rating'
                  AND k.keyword IN ['murder', 'murder-in-title', 'blood', 'violence']
                  AND kt.kind IN ['movie', 'episode']
                  AND NOT mc.note CONTAINS '(USA)'
                  AND mc.note CONTAINS '(200.*)'
                  AND mi.info IN ['Germany', 'German', 'USA', 'American']
                  AND mi_idx.info < '7.0'
                  AND t.production_year > 2008
                RETURN
                  MIN(cn.name) AS movie_company,
                  MIN(mi_idx.info) AS rating,
                  MIN(t.title) AS western_violent_movie;