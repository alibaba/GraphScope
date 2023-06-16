:submit MATCH (countryX:place {name: 'Puerto_Rico'})<-[:isLocatedIn]-(messageX : post | comment)-[:hasCreator]->(otherP:person),
    	(countryY:place {name: 'Republic_of_Macedonia'})<-[:isLocatedIn]-(messageY: post | comment)-[:hasCreator]->(otherP),
    	(otherP)-[:isLocatedIn]->(city:place)-[:isPartOf]->(countryCity),
    	(person:PERSON {id:15393162790207})-[:KNOWS*1..3]-(otherP)
WHERE messageX.creationDate >= 1291161600000 and messageX.creationDate < 1293753600000 AND messageY.creationDate >= 1291161600000 and messageY.creationDate < 1293753600000 AND NOT countryCity IN ['Puerto_Rico', 'Republic_of_Macedonia'] WITH otherP, count(messageX) as xCount, count(messageY) as yCount RETURN otherP.id as id,otherP.firstName as firstName, otherP.lastName as lastName, xCount, yCount, xCount + yCount as total ORDER BY total DESC, id ASC

// NOT
:submit MATCH (countryX:place {name: 'Puerto_Rico'})<-[:isLocatedIn]-(messageX : post | comment)-[:hasCreator]->(otherP:person),(countryY:place {name: 'Republic_of_Macedonia'})<-[:isLocatedIn]-(messageY: post | comment)-[:hasCreator]->(otherP:person),(otherP:person)-[:isLocatedIn]->(city:place)-[:isPartOf]->(countryCity:place),(p:person {id:15393162790207})-[:knows*1..3]-(otherP:person) WHERE messageX.creationDate >= 1291161600000 and messageX.creationDate < 1293753600000 AND messageY.creationDate >= 1291161600000 and messageY.creationDate < 1293753600000 AND  countryCity.name <> 'Puerto_Rico' and countryCity.name <> 'Republic_of_Macedonia' WITH otherP, count(messageX) as xCount, count(messageY) as yCount RETURN otherP.id as id,otherP.firstName as firstName, otherP.lastName as lastName, xCount, yCount, xCount + yCount as total ORDER BY total DESC, id ASC LIMIT 20

// dynamic params
:submit MATCH (countryX:place {name: $countryXName})<-[:isLocatedIn]-(messageX : post | comment)-[:hasCreator]->(otherP:person),(countryY:place {name: $countryYName})<-[:isLocatedIn]-(messageY: post | comment)-[:hasCreator]->(otherP:person),(otherP:person)-[:isLocatedIn]->(city:place)-[:isPartOf]->(countryCity:place),(p:person {id:$personId})-[:knows*1..3]-(otherP:person) WHERE messageX.creationDate >= $startDate and messageX.creationDate < $endDate AND messageY.creationDate >= $startDate and messageY.creationDate < $endDate AND  countryCity.name <> $countryXName and countryCity.name <> $countryYName WITH otherP, count(messageX) as xCount, count(messageY) as yCount RETURN otherP.id as id,otherP.firstName as firstName, otherP.lastName as lastName, xCount, yCount, xCount + yCount as total ORDER BY total DESC, id ASC LIMIT 20


//adhoc query
//"Papua_New_Guinea", "Switzerland",27493,1298937600000, 1301702400000
MATCH (countryX:place {name: "Papua_New_Guinea"})<-[:isLocatedIn]-(messageX : post | comment)-[:hasCreator]->(otherP:person),
(countryY:place {name: "Switzerland"})<-[:isLocatedIn]-(messageY: post | comment)-[:hasCreator]->(otherP:person),
(otherP:person)-[:isLocatedIn]->(city:place)-[:isPartOf]->(countryCity:place),
(p:person {id:27493})-[:knows*1..3]-(otherP:person) 
WHERE messageX.creationDate >= 1298937600000 and messageX.creationDate < 1301702400000 AND messageY.creationDate >= 1298937600000
and messageY.creationDate < 1301702400000 AND  countryCity.name <> "Papua_New_Guinea" and countryCity.name <> "Switzerland" 
WITH otherP, count(messageX) as xCount, count(messageY) as yCount RETURN otherP.id as id,otherP.firstName as firstName, otherP.lastName as lastName, 
xCount, yCount, xCount + yCount as total 
ORDER BY total DESC, id ASC LIMIT 20