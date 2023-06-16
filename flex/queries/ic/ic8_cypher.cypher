:submit MATCH(p:person {id: 2199023256816}) <-[:hasCreator] -(msg : post | comment) <- [:replyOf] - (cmt: comment) - [:hasCreator] -> (author : person) with p, msg, cmt, author ORDER BY cmt.creationDate DESC, cmt.id ASC limit 20 return author.id, author.firstName, author.lastName, cmt.creationDate, cmt.id, cmt.content

//dyn
:submit MATCH(p:person {id: $personId}) <-[:hasCreator] -(msg : post | comment) <- [:replyOf] - (cmt: comment) - [:hasCreator] -> (author : person) with p, msg, cmt, author ORDER BY cmt.creationDate DESC, cmt.id ASC limit 20 return author.id, author.firstName, author.lastName, cmt.creationDate, cmt.id, cmt.content


//adhoc
:submit MATCH(p:person {id: 15393162801011}) <-[:hasCreator] -(msg : post | comment) <- [:replyOf] - (cmt: comment) - [:hasCreator] -> (author : person) with p, msg, cmt, author ORDER BY cmt.creationDate DESC, cmt.id ASC limit 20 return author.id, author.firstName, author.lastName, cmt.creationDate, cmt.id, cmt.content