MATCH (country:PLACE)<-[:ISPARTOF]-(:PLACE)<-[:ISLOCATEDIN]-(person:PERSON)<-[:HASMEMBER]-(forum:FORUM)
WHERE forum.creationDate > $date
WITH country, forum, count(person) AS numberOfMembers
ORDER BY numberOfMembers DESC, forum.id ASC, country.id
WITH DISTINCT forum AS topForum
LIMIT 100

WITH collect(topForum) AS topForums

UNWIND topForums AS topForum2
MATCH (topForum1)-[:CONTAINEROF]->(post:POST)<-[:REPLYOF*0..30]-(message)-[:HASCREATOR]->(person:PERSON)<-[:HASMEMBER]-(topForum2:FORUM)
WITH person, message, topForum1
WHERE topForum1 IN topForums
WITH person, count(DISTINCT message) AS messageCount

RETURN
  person.id AS personId,
  person.firstName AS personFirstName,
  person.lastName AS personLastName,
  person.creationDate AS personCreationDate,
  sum(messageCount) AS messageCount
ORDER BY
  messageCount DESC,
  person.id ASC
LIMIT 100;