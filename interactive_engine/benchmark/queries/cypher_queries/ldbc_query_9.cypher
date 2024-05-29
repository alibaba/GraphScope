MATCH (person:PERSON {id: $personId})-[:KNOWS*1..3]-(friend:PERSON)<-[:HASCREATOR]-(message)
WHERE friend <> person
	and message.creationDate < $maxDate
RETURN
  friend.id AS personId,
  friend.firstName AS personFirstName,
  friend.lastName AS personLastName,
  message.id AS commentOrPostId,
  message.creationDate AS commentOrPostCreationDate
ORDER BY
  commentOrPostCreationDate DESC,
  commentOrPostId ASC
LIMIT 20