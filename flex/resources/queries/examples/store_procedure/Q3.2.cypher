Match (person1:PERSON)-[:LIKES]->(message:COMMENT|POST), 
	  (message:COMMENT|POST)-[:HASCREATOR]->(person2:PERSON), 
	  (person1:PERSON)-[:ISLOCATEDIN]->(place:PLACE), 
      (person2:PERSON) -[:ISLOCATEDIN]->(place:PLACE)
Return count(person1);