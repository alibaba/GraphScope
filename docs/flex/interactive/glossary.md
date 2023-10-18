# Glossary

### Stored Procedure.
A stored procedure (also termed proc, storp, sproc, StoPro, StoredProc, StoreProc, sp, or SP) is a subroutine available to applications that access a relational database management system (RDBMS). Such procedures are stored in the database data dictionary(from wikipedia). 

In GraphScope Interactive, a cypher query with parameters (denote with $ prefix) or a piece of c++ code can be registered as a stored procedure.


### Cypher.

Cypher is a popular graph query language, used by [neo4j](https://neo4j.com/developer/cypher/).

### Compile.

Compile refers to the process of compiling a stored procedure into a dynamic lib that the compute engine can load execute.