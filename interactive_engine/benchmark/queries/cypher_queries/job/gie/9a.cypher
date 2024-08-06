 MATCH
          (t:TITLE)<-[ci_rel:CAST_INFO_TITLE]-(ci:CAST_INFO)-[:CAST_INFO_CHAR]->(chn:CHAR_NAME),
          (ci)-[:CAST_INFO_ROLE]->(rt:ROLE_TYPE),
          (ci)-[:CAST_INFO_NAME]->(n:NAME)<-[an_rel:ALSO_KNOWN_AS_NAME]-(an:AKA_NAME),
          (t)<-[mc_rel:MOVIE_COMPANIES_TITLE]-(mc:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_COMPANY_NAME]->(cn:COMPANY_NAME)
        WHERE ci.note IN ['(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)']
          AND cn.country_code = '[us]'
          AND mc.note IS NOT NULL
          AND (mc.note CONTAINS '(USA)' OR mc.note CONTAINS '(worldwide)')
          AND n.gender = 'f'
          AND n.name CONTAINS 'Ang'
          AND rt.role = 'actress'
          AND t.production_year >= 2005 AND t.production_year <= 2015
        RETURN
          MIN(an.name) AS alternative_name,
          MIN(chn.name) AS character_name,
          MIN(t.title) AS movie;