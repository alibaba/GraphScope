MATCH
          (an:AKA_NAME)-[:ALSO_KNOWN_AS_NAME]->(n:NAME)-[:CAST_INFO_NAME]->(ci:CAST_INFO)-[:CAST_INFO_TITLE]->(t:TITLE)-[:MOVIE_KEYWORD]->(mk:MOVIE_KEYWORD)-[:MOVIE_KEYWORD_KEYWORD]->(k:KEYWORD),
          (t)-[:MOVIE_COMPANIES_TITLE]->(mc:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_COMPANY_NAME]->(cn:COMPANY_NAME)
        WHERE
          cn.country_code = '[us]'
          AND k.keyword = 'character-name-in-title'
          AND t.episode_nr >= 50
          AND t.episode_nr < 100
        RETURN
          MIN(an.name) AS cool_actor_pseudonym,
          MIN(t.title) AS series_named_after_char;