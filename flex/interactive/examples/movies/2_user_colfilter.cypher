MATCH (u1: User) -[r1:REVIEW]->(movie: Movie)<-[r2: REVIEW]- (u2: User)
WHERE u1.id > u2.id
             AND r1.rating > $rateThresh
             AND r2.rating > $rateThresh
Return u1.name, u2.name, movie.title LIMIT 10;