MATCH (countryX:PLACE {name: "Papua_New_Guinea"})<-[:ISLOCATEDIN]-(messageX : POST | COMMENT)-[:HASCREATOR]->(otherP:PERSON),
(countryY:PLACE {name: "Switzerland"})<-[:ISLOCATEDIN]-(messageY: POST | COMMENT)-[:HASCREATOR]->(otherP:PERSON),
(otherP:PERSON)-[:ISLOCATEDIN]->(city:PLACE)-[:ISPARTOF]->(countryCity:PLACE),
(p:PERSON {id:27493})-[:KNOWS*1..3]-(otherP:PERSON) 
WHERE messageX.creationDate >= 1298937600000 and messageX.creationDate < 1301702400000 AND messageY.creationDate >= 1298937600000
and messageY.creationDate < 1301702400000 AND  countryCity.name <> "Papua_New_Guinea" and countryCity.name <> "Switzerland" 
WITH otherP, count(messageX) as xCount, count(messageY) as yCount RETURN otherP.id as id,otherP.firstName as firstName, otherP.lastName as lastName, 
xCount, yCount, xCount + yCount as total 
ORDER BY total DESC, id ASC LIMIT 20;