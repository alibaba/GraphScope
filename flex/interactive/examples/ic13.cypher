
MATCH  (person1: PERSON {id: 26388279067108L})
with person1
OPTIONAL MATCH 
    shortestPath((person1: PERSON{id:26388279067108L})-[k:KNOWS*0..*]-(person2: PERSON {id: 26388279066795L}))
WITH
    CASE 
        WHEN k is null THEN -1
        ELSE length(k)
    END as len
RETURN len