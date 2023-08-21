# The Administrative tool

We provide the administrative tool `gs interactive` as to manage the GraphScope Interacttive. In this article, we will introduce the functions provided by this script.

### gs interactive init
Init the database. You can choose the database version with `--version` option.The "version" option actually specifies the version of the Docker image.
```bash
    gs interactive init --version <image_version(can skip to use default)>
```


### gs interactive start 
Start the database on specified graph. GraphScope follows the following convention: for each graph, we place the relevant files related to that graph under `data/${GRAPH_NAME}`. The structure of this directory will be as follows:
```bash
data/
└── ldbc
    ├── graph.yaml
    ├── indices
    └── plugins
```

- graph.yaml: describes the schema information of the graph.
- indices: contains the index files created by the database for this graph.
- plugins: contains the all the stored procedure compiled for this graph.


In addition to specifying the schema for the graph, you need to specify the configuration for graph loading when starting the database. This configuration file should specify the raw data file paths for each type of vertices and edges. If the file paths specified in bulk_load.yaml are relative paths, you need to provide an additional parameter to indicate the root path.


```bash
    gs interactive start -n [--name] <graph_name> 
					  -b [--bulk-load] <bulk load yaml file> 
					  -r[--root-data-dir] <path to root data dir>
```

### gs interactive stop

Stop the current running database.

### gs interactive restart
Restart the database using the previously specified parameters.

### gs interactive compile

Compile the specified cypher query or c++ app to a stored procedure. The generated stored procedure is a dynamic library, and will be loaded into memory when database in started on this graph. Then the stored prcoedure will be callable via cypher shell.

```bash
    gs interactive compile -g[--graph] <graph_name> -i <sourcefile[.cc, .cypher]
                compile cypher/.cc to dynamic library, according to the schema of graph. The output library will be placed at ./data/{graph_name}/lib.
```