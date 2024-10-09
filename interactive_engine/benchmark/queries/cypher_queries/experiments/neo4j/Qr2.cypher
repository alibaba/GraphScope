Match (p1:Person)<-[:HAS_MODERATOR]-(forum:Forum),
      (p1:Person)<-[:HAS_CREATOR]-(post:Post),
      (post)<-[:CONTAINER_OF]-(forum)
Where p1.id < 933
Return count(p1);