MATCH
          (ct:COMPANY_TYPE)<-[:MOVIE_COMPANIES_TYPE]-(mc:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_TITLE]->(t:TITLE)-[:MOVIE_INFO_IDX]->(it:INFO_TYPE)
        WHERE ct.kind = 'production companies'
          AND it.info = 'top 250 rank'
          AND NOT mc.note CONTAINS '(as Metro-Goldwyn-Mayer Pictures)'
          AND (mc.note CONTAINS '(co-production)'
               OR mc.note CONTAINS '(presents)')
        RETURN
          MIN(mc.note) AS production_note,
          MIN(t.title) AS movie_title,
          MIN(t.production_year) AS movie_year;