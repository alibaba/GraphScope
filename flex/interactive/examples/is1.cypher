MATCH (n:PERSON {id: 26388279068220L})-[:ISLOCATEDIN]->(p:PLACE)
RETURN
    n.firstName AS firstName,
    n.lastName AS lastName,
    n.locationIP AS locationIP,
    n.gender AS gender,
    n.creationDate AS creationDate,
    n.birthday AS birthday,
    n.browserUsed AS browserUsed,
    p.id AS cityId