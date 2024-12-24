MATCH
  (person:PERSON)<-[:HASCREATOR]-(post:POST)<-[:REPLYOF*0..7]-(message)
WHERE
  post.creationDate >= $startDate AND post.creationDate <= $endDate AND
  message.creationDate >= $startDate AND message.creationDate <= $endDate
WITH
  person,
  count(distinct post) as threadCnt,
  count(message) as msgCnt
RETURN
  person.id as id,
  person.firstName,
  person.lastName,
  threadCnt,
  msgCnt
  ORDER BY
  msgCnt DESC,
  id ASC
  LIMIT 100;