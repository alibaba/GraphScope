:submit MATCH (m: post | comment {id: $messageId }) RETURN m.creationDate as messageCreationDate, m.content AS messageContent, m.imageFile as messageImageFile