MATCH
          (t:TITLE)-[mi_rel:MOVIE_INFO]->(it1:INFO_TYPE),
          (t)-[mi_idx_rel:MOVIE_INFO_IDX]->(it2:INFO_TYPE),
          (t)<-[mc_rel:MOVIE_COMPANIES_TITLE]-(mc:MOVIE_COMPANIES)-[mc_type_rel:MOVIE_COMPANIES_TYPE]->(ct:COMPANY_TYPE),
          (mc)-[mc_name_rel:MOVIE_COMPANIES_COMPANY_NAME]->(cn:COMPANY_NAME)
        WHERE cn.country_code = '[us]'
          AND ct.kind = 'production companies'
          AND it1.info = 'genres'
          AND it2.info = 'rating'
          AND mi_rel.info IN ['Drama', 'Horror']
          AND mi_idx_rel.info > '8.0'
          AND t.production_year >= 2005 AND t.production_year <= 2008
        RETURN
          MIN(cn.name) AS movie_company,
          MIN(mi_idx_rel.info) AS rating,
          MIN(t.title) AS drama_horror_movie;