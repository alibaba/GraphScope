MATCH
          (cn:COMPANY_NAME)<-[:MOVIE_COMPANIES_COMPANY_NAME]-(mc:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_TITLE]->(t:TITLE)-[:MOVIE_KEYWORD]->(k:KEYWORD)
        WHERE cn.country_code = '[de]' AND k.keyword = 'character-name-in-title'
        RETURN MIN(t.title) AS movie_title;