MATCH (p: PERSON{id: $personId}) -[k:KNOWS*1..4]-(f: PERSON {firstName: $firstName})
OPTIONAL MATCH (f: PERSON)-[workAt:WORKAT]->(company:ORGANISATION)-[:ISLOCATEDIN]->(country:PLACE)
OPTIONAL MATCH (f: PERSON)-[studyAt:STUDYAT]->(university)-[:ISLOCATEDIN]->(universityCity:PLACE)
MATCH (f:PERSON)-[:ISLOCATEDIN]->(locationCity:PLACE)
WHERE 
  p <> f
with 
  f AS f, 
  company, 
  university, 
  workAt, 
  country, 
  studyAt, 
  universityCity, 
  locationCity, 
  length(k) as len
with f AS f, company, university, workAt, country, studyAt, universityCity, locationCity, min(len) as distance
ORDER  BY distance ASC, f.lastName ASC, f.id ASC
LIMIT 20

WITH 
  f, distance, locationCity,
CASE
  WHEN company is null Then null
  ELSE [company.name, workAt.workFrom, country.name]
END as companies,
CASE 
  WHEN university is null Then null
  ELSE [university.name, studyAt.classYear, universityCity.name]
END as universities
WITH f, distance, locationCity, collect(companies) as company_info, collect(universities) as university_info

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