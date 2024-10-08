MATCH
        (ci:CAST_INFO)-[:CAST_INFO_NAME]->(n:NAME),
        (ci)-[:CAST_INFO_TITLE]->(t:TITLE)-[:MOVIE_KEYWORD]->(k:KEYWORD),
        (t)<-[:MOVIE_COMPANIES_TITLE]-(mc:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_COMPANY_NAME]->(cn:COMPANY_NAME)
        WHERE cn.country_code = '[us]'
          AND k.keyword = 'character-name-in-title'
          AND n.name STARTS WITH 'B'
        RETURN
          MIN(n.name) AS member_in_charnamed_american_movie,
          MIN(n.name) AS a1;