:submit MATCH( msg : post | comment  {id: $messageId })- [:replyOf*0..3] -> (po : post) <- [:containerOf] - (f : forum) - [:hasModerator] -> (mod : person) return f.id as forumId, f.title as forumTitle, mod.id as moderatorId, mod.firstName as moderatorFirstName, mod.lastName as moderatorLastName