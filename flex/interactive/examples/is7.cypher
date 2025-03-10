MATCH (a)<-[:HASCREATOR]-(m:POST|COMMENT {id: 1030792257093L })<-[:REPLYOF]-(c:COMMENT)-[:HASCREATOR]->(p:PERSON)
    OPTIONAL MATCH (a)-[r:KNOWS]-(p)
    RETURN c.id AS commentId,
        c.creationDate AS commentCreationDate,
        c.content AS commentContent,
        p.id AS replyAuthorId,
        p.firstName AS replyAuthorFirstName,
        p.lastName AS replyAuthorLastName,
        CASE 
            WHEN r is null THEN false
            ELSE true
        END AS replyAuthorKnowsOriginalMessageAuthor
    ORDER BY commentCreationDate DESC, replyAuthorId