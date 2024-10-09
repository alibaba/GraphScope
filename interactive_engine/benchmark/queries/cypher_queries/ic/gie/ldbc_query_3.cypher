MATCH (countryX:PLACE {name: '$countryXName'})<-[:ISLOCATEDIN]-(messageX)-[:HASCREATOR]->(otherP:PERSON),
    	(countryY:PLACE {name: '$countryYName'})<-[:ISLOCATEDIN]-(messageY)-[:HASCREATOR]->(otherP:PERSON),
    	(otherP)-[:ISLOCATEDIN]->(city)-[:ISPARTOF]->(countryCity),
    	(person:PERSON {id:$personId})-[:KNOWS*1..3]-(otherP)
WHERE messageX.creationDate >= $startDate and messageX.creationDate < $endDate
  AND messageY.creationDate >= $startDate and messageY.creationDate < $endDate
	AND countryCity.name <> '$countryXName' AND countryCity.name <> '$countryYName'
WITH otherP, count(messageX) as xCount, count(messageY) as yCount
RETURN otherP.id as id,
			 otherP.firstName as firstName,
			 otherP.lastName as lastName,
			 xCount,
			 yCount,
			 xCount + yCount as total
ORDER BY total DESC, id ASC
Limit 20