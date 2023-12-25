Match (author:PERSON)<-[:HASCREATOR]-(msg1:POST|COMMENT)
where msg1.length > $len
Return count(author);