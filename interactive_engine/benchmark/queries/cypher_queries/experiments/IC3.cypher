MATCH 
    (p:PERSON {id: $personId})-[:KNOWS*1..3]-(otherP:PERSON)
MATCH (country:PLACE)<-[:ISLOCATEDIN]-(message)-[:HASCREATOR]->(otherP:PERSON)-[ISLOCATEDIN]->(city:PLACE)-[:ISPARTOF]-> (country2:PLACE)
WHERE 
     otherP.id<> $personId
     AND (country.name = $countryXName OR country.name = $countryYName)
     AND (country2.name <> $countryXName AND country2.name <> $countryYName)
      AND message.creationDate >= $startDate
      AND message.creationDate < $endDate
WITH 
    DISTINCT
    message,
    otherP, 
    country
    
WITH otherP,
     CASE WHEN country.name=$countryXName THEN 1 ELSE 0 END AS messageX,
     CASE WHEN country.name=$countryYName THEN 1 ELSE 0 END AS messageY
WITH otherP, sum(messageX) AS xCount, sum(messageY) AS yCount
WHERE xCount > 0 AND yCount > 0
RETURN
    otherP.id as id,
    otherP.firstName as firstName,
    otherP.lastName as lastName,
    xCount,
    yCount,
    xCount + yCount as total 
ORDER BY total DESC, id ASC LIMIT 20;