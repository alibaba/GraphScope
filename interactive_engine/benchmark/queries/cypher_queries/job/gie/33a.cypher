MATCH
(t1:TITLE)<-[:MOVIE_LINK_TITLE]-(ml:MOVIE_LINK)-[:MOVIE_LINK_LINKED_TYPE]->(lt:LINK_TYPE),
(ml)-[:MOVIE_LINK_LINKED_TITLE]->(t2:TITLE),
(t1)-[mi_idx1:MOVIE_INFO_IDX]->(it1:INFO_TYPE),
(t2)-[mi_idx2:MOVIE_INFO_IDX]->(it2:INFO_TYPE),
(t1)<-[:KIND_TYPE_TITLE]-(kt1:KIND_TYPE),
(t2)<-[:KIND_TYPE_TITLE]-(kt2:KIND_TYPE),
(t1)<-[:MOVIE_COMPANIES_TITLE]-(mc1:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_COMPANY_NAME]->(cn1:COMPANY_NAME),
(t2)<-[:MOVIE_COMPANIES_TITLE]-(mc2:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_COMPANY_NAME]->(cn2:COMPANY_NAME)
WHERE cn1.country_code = '[us]'
  AND it1.info = 'rating'
  AND it2.info = 'rating'
  AND kt1.kind = 'tv series'
  AND kt2.kind = 'tv series'
  AND lt.link IN ['sequel', 'follows', 'followed by']
  AND mi_idx2.info < '3.0'
  AND t2.production_year >= 2005
  AND t2.production_year <= 2008
RETURN
  MIN(cn1.name) AS first_company,
  MIN(cn2.name) AS second_company,
  MIN(mi_idx1.info) AS first_rating,
  MIN(mi_idx2.info) AS second_rating,
  MIN(t1.title) AS first_movie,
  MIN(t2.title) AS second_movie;
