MATCH (p1:PERSON {id:$personId})-[:PERSON_KNOWS_PERSON]-(p2:PERSON)-[pc:PERSON_WORKAT_ORGANISATION]->(o:ORGANISATION)-[:ORGANISATION_ISLOCATEDIN_PLACE]->(pl:PLACE)
WHERE pc.workFrom < $workFromYear AND pl.name = '$countryName'
RETURN p2.id, p2.firstName, p2.lastName, o.name, pc.workFrom;