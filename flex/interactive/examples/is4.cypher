MATCH (m:POST|COMMENT {id: 1030792332314L})
RETURN
    m.creationDate as messageCreationDate,
    m.content as messageContent,
    m.imageFile as messageImageFile;