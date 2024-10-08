MATCH
(t:TITLE)<-[:COMPLETE_CAST_TITLE]-(cc:COMPLETE_CAST)-[:COMPLETE_CAST_SUBJECT]->(cct1:COMP_CAST_TYPE),
(cc)-[:COMPLETE_CAST_STATUS]->(cct2:COMP_CAST_TYPE),
(t)-[mk:MOVIE_KEYWORD]->(k:KEYWORD),
(t)<-[:MOVIE_COMPANIES_TITLE]-(mc:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_COMPANY_NAME]->(cn:COMPANY_NAME),
(t)-[mi:MOVIE_INFO]->(it:INFO_TYPE),
(t)<-[:CAST_INFO_TITLE]-(ci:CAST_INFO)-[:CAST_INFO_CHAR]->(chn:CHAR_NAME),
(ci)-[:CAST_INFO_NAME]->(n:NAME),
(ci)-[:CAST_INFO_ROLE]->(rt:ROLE_TYPE),
(n)<-[:ALSO_KNOWN_AS_NAME]-(an:AKA_NAME),
(n)-[pi:PERSON_INFO]->(it3:INFO_TYPE)
WHERE cct1.kind = 'cast'
  AND cct2.kind = 'complete+verified'
  AND chn.name = 'Queen'
  AND ci.note IN ['(voice)', '(voice) (uncredited)', '(voice: English version)']
  AND cn.country_code = '[us]'
  AND it.info = 'release dates'
  AND it3.info = 'trivia'
  AND k.keyword = 'computer-animation'
  AND mi.info IS NOT NULL
  AND (mi.info =~ 'Japan:.*200.*' OR mi.info =~ 'USA:.*200.*')
  AND n.gender = 'f'
  AND n.name =~ '.*An.*'
  AND rt.role = 'actress'
  AND t.title = 'Shrek 2'
  AND t.production_year >= 2000
  AND t.production_year <= 2010
RETURN
  MIN(chn.name) AS voiced_char,
  MIN(n.name) AS voicing_actress,
  MIN(t.title) AS voiced_animation;