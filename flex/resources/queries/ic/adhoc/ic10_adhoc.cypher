MATCH (person:PERSON {id: 30786325579101})-[:KNOWS*2..3]-(friend: PERSON)-[:ISLOCATEDIN]->(city:PLACE)
WHERE NOT friend=person AND NOT (friend:PERSON)-[:KNOWS]-(person :PERSON {id: 30786325579101})
WITH person, city, friend, friend.birthday as birthday
WHERE  (birthday.month=7 AND birthday.day>=21) OR
        (birthday.month=8 AND birthday.day<22)
WITH DISTINCT friend, city, person

OPTIONAL MATCH (friend : PERSON)<-[:HASCREATOR]-(post:POST)
WITH friend, city, person, count(post) as postCount

OPTIONAL MATCH (friend)<-[:HASCREATOR]-(post1:POST)-[:HASTAG]->(tag:TAG)<-[:HASINTEREST]-(person: PERSON {id: 30786325579101})
WITH friend, city, postCount, count(post1) as commonPostCount

RETURN friend.id AS personId,
       friend.firstName AS personFirstName,
       friend.lastName AS personLastName,
       commonPostCount - (postCount - commonPostCount) AS commonInterestScore,
       friend.gender AS personGender,
       city.name AS personCityName
ORDER BY commonInterestScore DESC, personId ASC
LIMIT 10;