MATCH (person:PERSON {id: $personId})-[:KNOWS*2..3]-(friend: PERSON)-[:ISLOCATEDIN]->(city:PLACE)
OPTIONAL MATCH (friend : PERSON)<-[:HASCREATOR]-(post:POST)
OPTIONAL MATCH (friend)<-[:HASCREATOR]-(post1:POST)-[:HASTAG]->(tag:TAG)<-[:HASINTEREST]-(person: PERSON)
WHERE NOT friend=person 
        AND NOT (friend:PERSON)-[:KNOWS]-(person :PERSON {id: $personId})
WITH 
        person, city, friend, post, post1, friend.birthday as birthday

WHERE  (birthday.month=$month AND birthday.day>=21) OR
        (birthday.month=($month + 1) AND birthday.day<22)

WITH 
        friend, 
        city, 
        count(distinct post) as postCount,
        count(distinct post1) as commonPostCount

RETURN 
        friend.id AS personId,
        friend.firstName AS personFirstName,
        friend.lastName AS personLastName,
        commonPostCount - (postCount - commonPostCount) AS commonInterestScore,
        friend.gender AS personGender,
        city.name AS personCityName
ORDER BY commonInterestScore DESC, personId ASC
LIMIT 10;