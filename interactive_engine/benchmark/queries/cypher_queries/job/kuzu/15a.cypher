MATCH
        (:AKA_TITLE)-[:ALSO_KNOWN_AS_TITLE]->(t:TITLE)-[:MOVIE_KEYWORD]->(:KEYWORD),
        (t)<-[:MOVIE_COMPANIES_TITLE]-(mc:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_COMPANY_NAME]->(cn:COMPANY_NAME),
        (mc)-[:MOVIE_COMPANIES_TYPE]->(ct:COMPANY_TYPE),
        (t)-[mi:MOVIE_INFO]->(it1:INFO_TYPE)
        WHERE cn.country_code = '[us]'
          AND it1.info = 'release dates'
          AND mc.note =~ '.*(200.*).*'
          AND mc.note =~ '.*(worldwide).*'
          AND mi.note =~ '.*internet.*'
          AND mi.info =~ 'USA:.* 200.*'
          AND t.production_year > 2000
        RETURN
          MIN(mi.info) AS release_date,
          MIN(t.title) AS internet_movie;
