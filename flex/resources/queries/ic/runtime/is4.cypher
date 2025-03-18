MATCH (m:POST|COMMENT {id: 1030792332314L})
RETURN
    CASE WHEN m.content = "" THEN m.imageFile
    ELSE m.content END as messageContent,
    m.creationDate as messageCreationDate