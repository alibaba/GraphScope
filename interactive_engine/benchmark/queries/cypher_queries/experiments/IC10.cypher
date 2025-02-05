MATCH (person:PERSON {id: $personId})-[:KNOWS*2..3]-(friend: PERSON)
OPTIONAL MATCH (friend : PERSON)<-[:HASCREATOR]-(post:POST)
OPTIONAL MATCH (friend)<-[:HASCREATOR]-(post1:POST)-[:HASTAG]->(tag:TAG)<-[:HASINTEREST]-(person: PERSON {id: $personId})

// Anti-Pattern
WHERE 
      NOT friend=person 
      AND NOT (friend:PERSON)-[:KNOWS]-(person :PERSON {id: $personId})

WITH 
      person,  
      friend, 
      post,
      post1,
      date(datetime({epochMillis: friend.birthday})) as birthday

// datetime(friend.birthday) as birthday

WHERE  (birthday.month=$month AND birthday.day>=21) OR
        (birthday.month=($month%12)+1 AND birthday.day<22)

// Aggregate
WITH friend, count(distinct post) as postCount, count(distinct post1) as commonPostCount

WITH friend, commonPostCount - (postCount - commonPostCount) AS commonInterestScore
ORDER BY commonInterestScore DESC, friend.id ASC
LIMIT 10

MATCH (friend:PERSON)-[:ISLOCATEDIN]->(city:PLACE)

RETURN friend.id AS personId,
       friend.firstName AS personFirstName,
       friend.lastName AS personLastName,
       commonInterestScore,
       friend.gender AS personGender,
       city.name AS personCityName;