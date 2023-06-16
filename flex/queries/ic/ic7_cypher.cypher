MATCH (p:person {id: 26388279067534})<-[:hasCreator]-(message:post | comment)<-[like:likes]-(liker:person)
    WITH liker, message, like.creationDate AS likeTime, person
    ORDER BY likeTime DESC, toInteger(message.id) ASC
    WITH liker, head(collect({msg: message, likeTime: likeTime})) AS latestLike, person
RETURN
    liker.id AS personId,
    liker.firstName AS personFirstName,
    liker.lastName AS personLastName,
    latestLike.likeTime AS likeCreationDate,
    latestLike.msg.id AS commentOrPostId,
    coalesce(latestLike.msg.content, latestLike.msg.imageFile) AS commentOrPostContent,
    toInteger(floor(toFloat(latestLike.likeTime - latestLike.msg.creationDate)/1000.0)/60.0) AS minutesLatency,
    not((liker)-[:KNOWS]-(person)) AS isNew
ORDER BY
    likeCreationDate DESC,
    toInteger(personId) ASC
LIMIT 20

:submit MATCH (p:person {id: 26388279067534})<-[:hasCreator]-(message:post | comment)<-[like:likes]-(liker:person) WITH liker, message, like.creationDate AS likeTime, p ORDER BY likeTime DESC, message.id ASC  return liker.id as personId, liker.firstName AS personFirstName, liker.lastName AS personLastName