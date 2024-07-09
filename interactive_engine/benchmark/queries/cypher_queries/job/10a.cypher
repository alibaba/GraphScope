MATCH
          (t:TITLE)<-[ci_rel:CAST_INFO_TITLE]-(ci:CAST_INFO)-[:CAST_INFO_CHAR]->(chn:CHAR_NAME),
          (ci)-[:CAST_INFO_ROLE]->(rt:ROLE_TYPE),
          (t)<-[mc_rel:MOVIE_COMPANIES_TITLE]-(mc:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_COMPANY_NAME]->(cn:COMPANY_NAME),
          (mc)-[:MOVIE_COMPANIES_TYPE]->(ct:COMPANY_TYPE)
        WHERE ci.note CONTAINS '(voice)'
          AND ci.note CONTAINS '(uncredited)'
          AND cn.country_code = '[ru]'
          AND rt.role = 'actor'
          AND t.production_year > 2005
        RETURN
          MIN(chn.name) AS uncredited_voiced_character,
          MIN(t.title) AS russian_movie;