MATCH (nineties:Movie) WHERE nineties.released >= 1990 AND nineties.released < 2000
RETURN nineties.title AS ninetiesTitle ORDER BY ninetiesTitle DESC LIMIT 10;