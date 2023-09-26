MATCH (actor:Person {name: $actorName})
RETURN actor.id, actor.born, actor.name;