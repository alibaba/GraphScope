# Stored Procedures

Stored procedures in GraphScope Interactive offer a powerful way to encapsulate and reuse complex graph operations. This document provides a guide on how to compile, enable, and manage these procedures. We will take movies graph for example.

Note:
- For the following command, If `-g {graph_name}` is not given, the default modern graph will be used.

## Compiling a Stored Procedure
To compile a stored procedure, use the following command:
```bash
bin/gs_interactive procedure compile \
	-g {graph_name} \  # the graph **must** be specified
	-n stored_procedure_1 \  # name of the stored procedure
	-d "Description of the stored procedure's functionality" \
	-i stored_procedure_1.cypher  # input file containing the Cypher code
```

Note:
- The `-g` flag is indicates the graph associated with the stored procedure. If this option is not specified, the built-in modern graph will be used.
- While the `-n` flag, which names the stored procedure, is optional, omitting it will default the procedure's name to that of the input file.
- With the `-i` flag, you can either input a Cypher code file, or a C++ code file, or an entire directory. For comprehensive guidelines on crafting stored procedures in GraphScope Interactive using both Cypher and C++, refer to the Cypher procedure and C++ procedure documentation.
- If you opt to use the `-i` flag for specifying an entire directory, all files within that directory will be compiled. In such scenarios, the `-n` and `-d` flags become redundant, and the resulting stored procedures will adopt the names of their respective files.
- When compiling from Cypher code, the optimization rules defined under [`compiler.planner`](./configuration) will be taken into account to generate a more efficient program.
- The compilation process described above will automatically **enable** the compiled stored procedures (the subsequent section will instruct how to enable stored procedures). If you wish to compile without enabling them, use the `--compile_only` flag.


For example, You can compile the `actor` procedure on the builtin movies graph with the following command.
```bash
bin/gs_interactive procedure compile -g movies -n actor -i ./examples/movies/actor.cypher
```


Restart the service is **necessary** to activate the stored procedures:

```bash
bin/gs_interactive service restart
```

## Enabling Stored Procedures

To enable single stored procedure, simple using:

```bash
bin/gs_interactive procedure enable -g {graph_name} -n {procedure_name}
```

If you want to enable multiple procedures at once, just using the following command:

```bash
# enable all procedures listed in stored_procedures.yaml
bin/gs_interactive procedure enable -g {graph_name} -c stored_procedures.yaml
# or use comma to separate procedure names for -n option
bin/gs_interactive procedure enable -g {graph_name} -n '{proc1},{proc2}'
```
The above `stored_procedures.yaml` file configures which stored procedures are enabled for the current interactive service. Here's an example:

```yaml
actor
```

Note:
- The `enable` functionality preserves the status of currently enabled stored procedures. In other words, a previously enabled stored procedure will remain enabled after executing the enabling command, regardless of whether it appears in the provided list or not.

Remember to **restart** the service for the enabling to take effect.

## Disabling Stored Procedures

To disable a single stored procedures, simply using:

```bash
bin/gs_interactive procedure disable -g {graph_name} -n {procedure_name}
```

If you want to disable multiple procedures at once, just using the following command:


```bash
# disable all procedures listed in stored_procedures.yaml
bin/gs_interactive procedure disable -g movies -c stored_procedures.yaml
# or use comma to separate procedure names for -n option
bin/gs_interactive procedure disable -g {graph_name} -n '{proc1},{proc2}'
```
For example, you can disable `actor` procedure via the following `YAML` file.

```yaml
actor
```


The following command makes it easy to disable all stored procedures:
```bash
# `-a` will yield any given list via `-c`
bin/gs_interactive procedure disable -g {graph_name} -a
```

Remember to **restart** the service for the disabling to take effect.

## Viewing Enabled Stored Procedures

To view the currently enabled stored procedures of the given graph after starting the service, execute:

```bash
# show the procedures for a specified graph
bin/gs_interactive procedure show -g {graph_name}
# show the procedures for the builtin graph
bin/gs_interactive procedure show
```


This command provides a list of all active (enabled) stored procedures with their metadata including names and descriptions in your GraphScope Interactive instance.

Or, in the Cypher shell, run:
```bash
@neo4j> Show Procedures;
```

## Querying Stored Procedures
Once you've enabled a stored procedure, you can easily invoke it using its designated name and required parameters within the Cypher shell. For instance:
```bash
@neo4j> CALL actor(1L);
# The capitalized "L" ensures that the vertex_id is passed to the engine as a long type.
```

Ensure that the name and data type of the parameters align with what was specified during the procedure's compilation, whether in the Cypher or C++ file.