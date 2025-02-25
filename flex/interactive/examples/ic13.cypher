
MATCH  (person1: PERSON {id: 32985348833679})
with person1
OPTIONAL MATCH 
    shortestPath((person1: PERSON{id:32985348833679})-[k:KNOWS*0..32]-(person2: PERSON {id: 26388279067108}))
WITH
    CASE 
        WHEN k is null THEN -1
        ELSE length(k)
    END as len
RETURN len