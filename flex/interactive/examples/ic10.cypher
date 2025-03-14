MATCH (person:PERSON {id: 2199023256097L})-[:KNOWS*2..3]-(friend: PERSON)
WHERE 
        NOT friend=person 
        AND NOT (friend:PERSON)-[:KNOWS]-(person :PERSON {id: 2199023256097L})
WITH distinct
        friend
WITH friend,        friend.birthday as birthday
WHERE  (birthday.month=10 AND birthday.day>=21) OR (birthday.month=(10%12) + 1 AND birthday.day<22)

OPTIONAL MATCH (friend : PERSON)<-[:HASCREATOR]-(post:POST)
WITH friend,  count(post) as postCount

CALL {
        WITH friend, postCount
        MATCH (friend: PERSON) <- [:HASCREATOR]- (post: POST) - [:HASTAG] -> (tag:TAG)
        WITH friend, postCount, post, tag
        MATCH (tag:TAG)<-[:HASINTEREST]-(person: PERSON {id: 2199023256097L})
        WITH friend, postCount, count(distinct post) as commonPostCount
        return friend, commonPostCount - (postCount - commonPostCount) AS commonInterestScore, friend.id as friendId
        ORDER BY commonInterestScore DESC, friendId ASC
        LIMIT 10
}
UNION 
CALL {
        return friend, (0L-postCount) AS commonInterestScore, friend.id as friendId
        ORDER BY commonInterestScore DESC, friendId ASC
        LIMIT 10
}
WITH friend, max(commonInterestScore) AS score
ORDER BY score DESC, friend.id ASC
LIMIT 10
MATCH (friend: PERSON)-[:ISLOCATEDIN]->(city:PLACE)
RETURN friend.id AS personId,
       friend.firstName AS personFirstName,
       friend.lastName AS personLastName,
       score AS commonInterestScore,
       friend.gender AS personGender,
       city.name AS personCityName