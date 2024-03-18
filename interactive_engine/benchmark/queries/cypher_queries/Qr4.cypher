Match (author:PERSON)<-[:HASCREATOR]-(msg1:POST|COMMENT)<-[:REPLYOF]-(msg2:POST|COMMENT)
Return count(author);