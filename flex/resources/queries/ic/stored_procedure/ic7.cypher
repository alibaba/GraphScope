MATCH (person:PERSON {id: $personId})<-[:HASCREATOR]-(message: POST | COMMENT)<-[like:LIKES]-(liker:PERSON)
OPTIONAL MATCH (liker: PERSON)-[k:KNOWS]-(person: PERSON {id: $personId})
WITH liker, message, like.creationDate AS likeTime, person,
  CASE
      WHEN k is null THEN true
      ELSE false
     END AS isNew
ORDER BY likeTime DESC, message.id ASC
WITH liker, person, head(collect(message)) as message, head(collect(likeTime)) AS likeTime, isNew
RETURN
    liker.id AS personId,
    liker.firstName AS personFirstName,
    liker.lastName AS personLastName,
    likeTime AS likeCreationDate,
    message.id AS commentOrPostId,
    message.content AS messageContent,
    message.imageFile AS messageImageFile,
    (likeTime - message.creationDate)/1000/60 AS minutesLatency,
  	isNew
ORDER BY
    likeCreationDate DESC,
    personId ASC
LIMIT 20;