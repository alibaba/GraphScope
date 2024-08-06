MATCH
                  (t:TITLE)-[mi:MOVIE_INFO]->(:INFO_TYPE),
                  (t)<-[mct:MOVIE_COMPANIES_TITLE]-(mc:MOVIE_COMPANIES)-[mcc:MOVIE_COMPANIES_TYPE]->(ct:COMPANY_TYPE)
                WHERE ct.kind = 'production companies'
                  AND NOT mc.note CONTAINS '(TV)'
                  AND mc.note CONTAINS '(USA)'
                  AND mi.info IN ['Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Danish', 'Norwegian', 'German', 'USA','American']
                  AND t.production_year > 1990
                RETURN MIN(t.title) AS typical_european_movie;