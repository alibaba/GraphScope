 MATCH
                     (t:TITLE)<-[:COMPLETE_CAST_TITLE]-(cc:COMPLETE_CAST)-[:COMPLETE_CAST_SUBJECT]->(cct1:COMP_CAST_TYPE),
                     (cc)-[:COMPLETE_CAST_STATUS]->(cct2:COMP_CAST_TYPE),
                     (t)-[mk:MOVIE_KEYWORD]->(k:KEYWORD),
                     (t)<-[:MOVIE_LINK_TITLE]-(ml:MOVIE_LINK)-[:MOVIE_LINK_LINKED_TYPE]->(lt:LINK_TYPE),
                     (t)<-[:MOVIE_COMPANIES_TITLE]-(mc:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_COMPANY_NAME]->(cn:COMPANY_NAME),
                     (mc)-[:MOVIE_COMPANIES_TYPE]->(ct:COMPANY_TYPE),
                     (t)-[mi:MOVIE_INFO]->(:INFO_TYPE)
                     WHERE cct1.kind IN ['cast', 'crew']
                       AND cct2.kind = 'complete'
                       AND cn.country_code <> '[pl]'
                       AND (cn.name CONTAINS 'Film' OR cn.name CONTAINS 'Warner')
                       AND ct.kind = 'production companies'
                       AND k.keyword = 'sequel'
                       AND lt.link CONTAINS 'follow'
                       AND mc.note IS NULL
                       AND mi.info IN ['Sweden', 'Germany', 'Swedish', 'German']
                       AND t.production_year >= 1950
                       AND t.production_year <= 2000
                     RETURN
                       MIN(cn.name) AS producing_company,
                       MIN(lt.link) AS link_type,
                       MIN(t.title) AS complete_western_sequel;