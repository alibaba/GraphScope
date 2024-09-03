Match (author:PERSON)<-[:HASCREATOR]-(msg1:POST|COMMENT)
Return count(author);