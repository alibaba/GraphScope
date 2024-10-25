MATCH
                     (mc:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_TITLE]->(t:TITLE)-[:MOVIE_KEYWORD]->(k:KEYWORD),
                     (mc)-[:MOVIE_COMPANIES_COMPANY_NAME]->(cn:COMPANY_NAME),
                     (mc)-[:MOVIE_COMPANIES_TYPE]->(ct:COMPANY_TYPE),
                     (t)<-[:MOVIE_LINK_TITLE]-(ml:MOVIE_LINK)-[:MOVIE_LINK_LINKED_TYPE]->(lt:LINK_TYPE),
                     (t)-[mi:MOVIE_INFO]->(:INFO_TYPE)
                     WHERE cn.country_code <> '[pl]'
                       AND (cn.name CONTAINS 'Film' OR cn.name CONTAINS 'Warner')
                       AND ct.kind = 'production companies'
                       AND k.keyword = 'sequel'
                       AND lt.link CONTAINS 'follow'
                       AND mc.note IS NULL
                       AND mi.info IN ['Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German']
                       AND t.production_year >= 1950
                       AND t.production_year <= 2000
                     RETURN
                       MIN(cn.name) AS company_name,
                       MIN(lt.link) AS link_type,
                       MIN(t.title) AS western_follow_up;