MATCH
(t:TITLE)<-[:COMPLETE_CAST_TITLE]-(cc:COMPLETE_CAST)-[:COMPLETE_CAST_SUBJECT]->(cct1:COMP_CAST_TYPE),
(cc)-[:COMPLETE_CAST_STATUS]->(cct2:COMP_CAST_TYPE),
(t)-[mk:MOVIE_KEYWORD]->(k:KEYWORD),
(t)<-[:MOVIE_COMPANIES_TITLE]-(mc:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_COMPANY_NAME]->(cn:COMPANY_NAME),
(mc)-[:MOVIE_COMPANIES_TYPE]->(ct:COMPANY_TYPE),
(t)-[mi:MOVIE_INFO]->(it1:INFO_TYPE),
(t)-[mi_idx:MOVIE_INFO_IDX]->(it2:INFO_TYPE),
(t)<-[:KIND_TYPE_TITLE]-(kt:KIND_TYPE)
WHERE cct1.kind = 'crew'
  AND cct2.kind <> 'complete+verified'
  AND cn.country_code <> '[us]'
  AND it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ['murder', 'murder-in-title', 'blood', 'violence']
  AND kt.kind IN ['movie', 'episode']
  AND NOT mc.note =~ '.*(USA).*'
  AND mc.note =~ '.*(200).*'
  AND mi.info IN ['Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Danish', 'Norwegian', 'German', 'USA', 'American']
  AND mi_idx.info < '8.5'
  AND t.production_year > 2000
RETURN
  MIN(cn.name) AS movie_company,
  MIN(mi_idx.info) AS rating,
  MIN(t.title) AS complete_euro_dark_movie;