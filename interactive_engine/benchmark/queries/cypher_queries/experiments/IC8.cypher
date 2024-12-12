MATCH(p:PERSON {id: $personId}) <-[:HASCREATOR] -(msg) <- [:REPLYOF] - (cmt: COMMENT) - [:HASCREATOR] -> (author : PERSON)
WITH
    p, msg, cmt, author 
ORDER BY 
    cmt.creationDate DESC, 
    cmt.id ASC 
LIMIT 20 
RETURN
    author.id, 
    author.firstName, 
    author.lastName, 
    cmt.creationDate, 
    cmt.id, 
    cmt.content;