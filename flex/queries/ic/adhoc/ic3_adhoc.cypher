MATCH (countryX:place {name: "Papua_New_Guinea"})<-[:isLocatedIn]-(messageX : post | comment)-[:hasCreator]->(otherP:person),
(countryY:place {name: "Switzerland"})<-[:isLocatedIn]-(messageY: post | comment)-[:hasCreator]->(otherP:person),
(otherP:person)-[:isLocatedIn]->(city:place)-[:isPartOf]->(countryCity:place),
(p:person {id:27493})-[:knows*1..3]-(otherP:person) 
WHERE messageX.creationDate >= 1298937600000 and messageX.creationDate < 1301702400000 AND messageY.creationDate >= 1298937600000
and messageY.creationDate < 1301702400000 AND  countryCity.name <> "Papua_New_Guinea" and countryCity.name <> "Switzerland" 
WITH otherP, count(messageX) as xCount, count(messageY) as yCount RETURN otherP.id as id,otherP.firstName as firstName, otherP.lastName as lastName, 
xCount, yCount, xCount + yCount as total 
ORDER BY total DESC, id ASC LIMIT 20;