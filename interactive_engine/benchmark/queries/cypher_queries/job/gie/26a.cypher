MATCH
(t:TITLE)<-[:COMPLETE_CAST_TITLE]-(cc:COMPLETE_CAST)-[:COMPLETE_CAST_SUBJECT]->(cct1:COMP_CAST_TYPE),
(cc)-[:COMPLETE_CAST_STATUS]->(cct2:COMP_CAST_TYPE),
(t)-[mk:MOVIE_KEYWORD]->(k:KEYWORD),
(t)<-[:KIND_TYPE_TITLE]-(kt:KIND_TYPE),
(t)-[mi_idx:MOVIE_INFO_IDX]->(it2:INFO_TYPE),
(t)<-[:CAST_INFO_TITLE]-(ci:CAST_INFO)-[:CAST_INFO_CHAR]->(chn:CHAR_NAME),
(ci)-[:CAST_INFO_NAME]->(n:NAME)
WHERE cct1.kind = 'cast'
  AND cct2.kind CONTAINS 'complete'
  AND (chn.name CONTAINS 'man' OR chn.name CONTAINS 'Man')
  AND it2.info = 'rating'
  AND k.keyword IN ['superhero', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence', 'magnet', 'web', 'claw', 'laser']
  AND kt.kind = 'movie'
  AND mi_idx.info > '7.0'
  AND t.production_year > 2000
RETURN
  MIN(chn.name) AS character_name,
  MIN(mi_idx.info) AS rating,
  MIN(n.name) AS playing_actor,
  MIN(t.title) AS complete_hero_movie;