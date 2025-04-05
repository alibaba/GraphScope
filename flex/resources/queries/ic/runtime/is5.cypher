MATCH (m:POST | COMMENT {id: 1030792332314L  })-[:HASCREATOR]->(p:PERSON)
RETURN
    p.id AS personId,
    p.firstName AS firstName,
    p.lastName AS lastName