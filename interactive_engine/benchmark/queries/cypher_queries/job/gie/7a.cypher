MATCH
(an:AKA_NAME)-[:ALSO_KNOWN_AS_NAME]->(n:NAME)-[pi:PERSON_INFO]->(:INFO_TYPE),
(n)<-[:CAST_INFO_NAME]-(ci:CAST_INFO)-[:CAST_INFO_TITLE]->(t:TITLE)<-[:MOVIE_LINK_LINKED_TITLE]-(ml:MOVIE_LINK)-[:MOVIE_LINK_LINKED_TYPE]->(lt:LINK_TYPE)
WHERE an.name CONTAINS 'a'
AND pi.note = 'Volker Boehm'
AND lt.link = 'features'
AND n.name_pcode_cf >= 'A' AND n.name_pcode_cf <= 'F'
AND (n.gender = 'm' OR (n.gender = 'f' AND n.name STARTS WITH 'B'))
AND pi.note = 'Volker Boehm'
AND t.production_year >= 1980 AND t.production_year <= 1995
RETURN 
  MIN(n.name) AS of_person,
  MIN(t.title) AS biography_movie