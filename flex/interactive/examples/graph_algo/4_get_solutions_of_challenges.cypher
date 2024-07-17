MATCH (s: Solution)-[:ApplyOn]->(c: Challenge)
WHERE c.challenge = $challenge
RETURN c.challenge AS challenge, s.solution AS solution;
