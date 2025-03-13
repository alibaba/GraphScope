MATCH(p:PERSON {id: 4398046512194L}) <-[:HASCREATOR] -(msg : POST | COMMENT) <- [:REPLYOF] - (cmt: COMMENT) - [:HASCREATOR] -> (author : PERSON)
WITH
    p, msg, cmt, author 
ORDER BY 
    cmt.creationDate DESC, 
    cmt.id ASC 
LIMIT 20 
RETURN
    author.id as personId, 
    author.firstName as personFirstName, 
    author.lastName as personLastName, 
    cmt.creationDate as commentCreationDate, 
    cmt.id as commentId, 
    cmt.content as commentContent