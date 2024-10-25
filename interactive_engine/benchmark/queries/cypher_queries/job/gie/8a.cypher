MATCH
          (n1:NAME)<-[an1_rel:ALSO_KNOWN_AS_NAME]-(an1:AKA_NAME),
          (n1)<-[ci_rel:CAST_INFO_NAME]-(ci:CAST_INFO)-[:CAST_INFO_TITLE]->(t:TITLE),
          (t)<-[mc_rel:MOVIE_COMPANIES_TITLE]-(mc:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_COMPANY_NAME]->(cn:COMPANY_NAME),
          (ci)-[:CAST_INFO_ROLE]->(rt:ROLE_TYPE)
        WHERE ci.note = '(voice: English version)'
          AND cn.country_code = '[jp]'
          AND mc.note CONTAINS '(Japan)'
          AND NOT mc.note CONTAINS '(USA)'
          AND n1.name CONTAINS 'Yo'
          AND NOT n1.name CONTAINS 'Yu'
          AND rt.role = 'actress'
        RETURN
          MIN(an1.name) AS actress_pseudonym,
          MIN(t.title) AS japanese_movie_dubbed;