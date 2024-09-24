MATCH (person:PERSON {id: $personId})-[:KNOWS*1..3]-(friend:PERSON)-[workAt:WORKAT]->(company:ORGANISATION)-[:ISLOCATEDIN]->(:PLACE {name: '$countryName'})
WHERE person <> friend
	and workAt.workFrom < $workFromYear
RETURN DISTINCT
  friend.id AS personId,
  friend.firstName AS personFirstName,
  friend.lastName AS personLastName,
  company.name AS organizationName,
  workAt.workFrom AS organizationWorkFromYear
ORDER BY
  organizationWorkFromYear ASC,
  personId ASC,
  organizationName DESC
LIMIT 10