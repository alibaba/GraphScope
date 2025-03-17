MATCH (p: PERSON{id: 30786325579101}) -[k:KNOWS*1..4]-(f: PERSON) where f.firstName = "Ian" AND f.id <> 30786325579101
with f AS f, length(k) as len
with f AS f, min(len) as distance
ORDER  BY distance ASC, f.lastName ASC, f.id ASC
LIMIT 20


OPTIONAL MATCH (f: PERSON)-[workAt:WORKAT]->(company:ORGANISATION)-[:ISLOCATEDIN]->(country:PLACE)
WITH 
    f, distance,
    CASE
        WHEN company is null Then null
        ELSE [company.name, workAt.workFrom, country.name]
    END as companies
WITH f, collect(companies) as company_info, distance

OPTIONAL MATCH (f: PERSON)-[studyAt:STUDYAT]->(university)-[:ISLOCATEDIN]->(universityCity:PLACE)
WITH
  f, company_info, distance,
	CASE 
		WHEN university is null Then null
		ELSE [university.name, studyAt.classYear, universityCity.name]
	END as universities
WITH f, collect(universities) as university_info , company_info, distance

MATCH (f:PERSON)-[:ISLOCATEDIN]->(locationCity:PLACE)

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