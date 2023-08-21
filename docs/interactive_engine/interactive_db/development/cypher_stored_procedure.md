# Cypher Stored Procedure

Cypher queries can also be registered as stored procedures in the database. Users need to denote the running time parameters with $PARAM_NAME in the query.


## Cypher Stored Procedure
We require each cypher stored procedure is stored in a file, and write down the cypher query. A file should only contain one Cypher query, with the possibility of using line breaks.

For example, if you want to get the count of nodes with the label "person" and personId equals to a runtime specified value, you can write the following query, and i.e. put it in `query1.cypher`

```
MATCH (p :PERSON {id: $personId }) return p.firstName, p.lastName
```

## Register stored procedure to database.

You can use `db_admin.sh` to compiler and register the cypher query.

```bash
./bin/db_admin.sh compile -g ldbc -i ../resources/queries/ic/stored_procedure/ic2.cypher
```
Note that a stored procedure is bound to a specific graph.


## Call the Cypher Stored Procedure.
After the stored procedure is registerd, you should restart/start the database service.
```bash
./bin/db_admin.sh restart
# or
./bin/db_admin.sh start -n ${graph_name} -b ${bulk_load-yaml}
```

Then from cypher-shell,
```cypher
CALL query1(1234) YIELD *;
```
 