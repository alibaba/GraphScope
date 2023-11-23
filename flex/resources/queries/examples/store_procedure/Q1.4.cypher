Match (author:PERSON)<-[:HASCREATOR]-(msg1:POST|COMMENT)<-[:REPLYOF]-(msg2:POST|COMMENT)
where msg2.length > $len
Return count(author);