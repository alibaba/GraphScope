MATCH (n:PERSON {id: 19791209300143L })-[r:KNOWS]-(friend:PERSON)
RETURN
    friend.id AS personId,
    friend.firstName AS firstName,
    friend.lastName AS lastName,
    r.creationDate AS friendshipCreationDate
ORDER BY
    friendshipCreationDate DESC,
    personId ASC