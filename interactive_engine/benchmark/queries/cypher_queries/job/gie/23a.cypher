MATCH
                     (cct1:COMP_CAST_TYPE)<-[:COMPLETE_CAST_STATUS]-(cc:COMPLETE_CAST)-[:COMPLETE_CAST_TITLE]->(t:TITLE)-[:MOVIE_KEYWORD]->(k:KEYWORD),
                     (t)<-[:MOVIE_COMPANIES_TITLE]-(mc:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_COMPANY_NAME]->(cn:COMPANY_NAME),
                     (mc)-[:MOVIE_COMPANIES_TYPE]->(ct:COMPANY_TYPE),
                     (t)-[mi:MOVIE_INFO]->(it1:INFO_TYPE),
                     (t)<-[:KIND_TYPE_TITLE]-(kt:KIND_TYPE)
                     WHERE cct1.kind = 'complete+verified'
                       AND cn.country_code = '[us]'
                       AND it1.info = 'release dates'
                       AND kt.kind IN ['movie']
                       AND mi.note CONTAINS 'internet'
                       AND mi.info IS NOT NULL
                       AND (mi.info CONTAINS 'USA:.* 199.*' OR mi.info CONTAINS 'USA:.* 200.*')
                       AND t.production_year > 2000
                     RETURN
                       MIN(kt.kind) AS movie_kind,
                       MIN(t.title) AS complete_us_internet_movie;