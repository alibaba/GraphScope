MATCH (n:PERSON {id: 19791209300143L })-[r:KNOWS]-(friend:PERSON)
RETURN
    friend.id AS personId,
    r.creationDate AS friendshipCreationDate,
    friend.firstName AS firstName,
    friend.lastName AS lastName
ORDER BY
    friendshipCreationDate DESC,
    personId ASC