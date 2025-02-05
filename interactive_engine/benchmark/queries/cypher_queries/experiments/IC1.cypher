MATCH k = shortestPath((p: PERSON{id: $personId})-[:KNOWS*1..4]-(f: PERSON {firstName: $firstName}))
MATCH (f:PERSON)-[:ISLOCATEDIN]->(locationCity:PLACE)

WHERE 
  p <> f

OPTIONAL MATCH (f: PERSON)-[workAt:WORKAT]->(company:ORGANISATION)-[:ISLOCATEDIN]->(country:PLACE)
// append one new column <companies>
WITH 
  f, k, locationCity,
  CASE
    WHEN company is null Then null
    ELSE [company.name, workAt.workFrom, country.name]
  END as companies

WITH f, k, locationCity, collect(companies) as company_info

OPTIONAL MATCH (f: PERSON)-[studyAt:STUDYAT]->(university)-[:ISLOCATEDIN]->(universityCity:PLACE)
// append one new column <universities>
WITH f, k, locationCity, company_info,
  CASE 
    WHEN university is null Then null
    ELSE [university.name, studyAt.classYear, universityCity.name]
  END as universities

WITH f, k, locationCity, company_info, collect(universities) as university_info

// apend one new column <distance>
WITH 
  f,
  k,
  locationCity, 
  company_info, 
  university_info, 
  length(k) as distance

ORDER BY distance ASC, f.lastName ASC, f.id ASC
LIMIT 20

return f.id AS friendId,
  f.lastName AS friendLastName,
  distance AS distanceFromPerson,
  f.birthday AS friendBirthday,
  f.creationDate AS friendCreationDate,
  f.gender AS friendGender,
  f.browserUsed AS friendBrowserUsed,
  f.locationIP AS friendLocationIp,
  f.email AS friendEmail,
  f.language AS friendLanguage,
  locationCity.name AS friendCityName,
  university_info AS friendUniversities,
  company_info AS friendCompanies;