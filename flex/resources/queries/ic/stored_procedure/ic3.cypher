MATCH 
    (countryX: PLACE {name: $countryXName })<-[:ISLOCATEDIN]-(messageX : POST | COMMENT)-[:HASCREATOR]->(otherP:PERSON),
    (countryY: PLACE {name: $countryYName })<-[:ISLOCATEDIN]-(messageY : POST | COMMENT)-[:HASCREATOR]->(otherP:PERSON),
    (otherP:PERSON)-[:ISLOCATEDIN]->(city:PLACE)-[:ISPARTOF]->(countryCity:PLACE),
    (p:PERSON {id: $personId})-[:KNOWS*1..3]-(otherP:PERSON)
WHERE 
    otherP <> p
    AND messageX.creationDate >= $startDate
    AND messageX.creationDate < $endDate 
    AND messageY.creationDate >= $startDate
    AND messageY.creationDate < $endDate 
    AND countryCity.name <> $countryXName 
    AND countryCity.name <> $countryYName
WITH 
    otherP, 
    count(messageX) as xCount, 
    count(messageY) as yCount 
RETURN
    otherP.id as id,
    otherP.firstName as firstName,
    otherP.lastName as lastName,
    xCount,
    yCount,
    xCount + yCount as total 
ORDER BY total DESC, id ASC LIMIT 20;