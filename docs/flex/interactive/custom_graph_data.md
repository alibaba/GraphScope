# Using Custom Graph

This guide walks you through the process of using custom graph data in GraphScope Interactive. The process comprises three main steps: 
- Creating a new graph,
- Importing graph data, and
- Starting the service with the new graph.

We'll use the [`movies`](https://github.com/neo4j-graph-examples/movies/) graph as an example, with the necessary sample files located in `{INTERACTIVE_HOME}/examples/movies/`.

## Step 1: Create a New Graph

Before starting, please make sure you are in the initial environment. If not, please destroy the currently running database first.
```bash
bin/gs_interactive service stop
bin/gs_interactive destroy
```

To create a new graph, you will need the original data of the graph. We currently support files in CSV format. Fortunately, we have prepared it for you, and you can find it in the directory `{INTERACTIVE_HOME}/examples/movies/`. 

To begin, ensure you've adjusted the settings in the `{INTERACTIVE_HOME}/conf/interactive.yaml` file. By utilizing Docker's volume mount feature, you can map an external folder containing the CSV files of `movies` to the internal directory at `/home/graphscope/movies`. It's crucial that the internal data path starts with `/home/graphscope` and concludes with `movies`, reflecting the name of your graph. If you're looking to import custom data, you can do volume mapping in a similar way.

```yaml
version: v0.0.3
volume:
  # replace INTERACTIVE_HOME with actual path.
  - {INTERACTIVE_HOME}/examples/movies:/home/graphscope/movies 
```

Now init the database.
```bash
./bin/gs_interactive init -c ./conf/interactive.yaml
```

To create a new graph "movies", execute the following command:
```bash
bin/gs_interactive database create -g movies -c ./examples/movies/graph.yaml
```

The `./examples/movies/graph.yaml` file defines the graph schema. In this file:

- For each vertex type, specify its name, allowed properties, primary keys (if any), and other relevant details.
- For each edge type, define the source/destination vertex types and their associated properties.

Ensure that each graph in the file has a unique name and an associated schema. Here's a sample schema for the "movies" graph:
```yaml
name: movies
schema:
  vertex_types:
    - type_name: Movie
      properties:
        - property_name: id
          property_type:
            primitive_type: DT_SIGNED_INT64
        - property_name: released
          property_type:
            primitive_type: DT_SIGNED_INT32
        - property_name: tagline
          property_type:
            primitive_type: DT_STRING
        - property_name: title
          property_type:
            primitive_type: DT_STRING
      primary_keys:
        - id
    - type_name: Person
      properties:
        - property_name: id
          property_type:
            primitive_type: DT_SIGNED_INT64
        - property_name: born
          property_type:
            primitive_type: DT_SIGNED_INT32
        - property_name: name
          property_type:
            primitive_type: DT_STRING
      primary_keys:
        - id
  edge_types:
    - type_name: ACTED_IN
      vertex_type_pair_relations:
        - source_vertex: Person
          destination_vertex: Movie
          relation: MANY_TO_MANY
    - type_name: DIRECTED
      vertex_type_pair_relations:
        - source_vertex: Person
          destination_vertex: Movie
          relation: MANY_TO_MANY
    - type_name: REVIEW
      vertex_type_pair_relations:
        - source_vertex: Person
          destination_vertex: Movie
          relation: MANY_TO_MANY
      properties:
        - property_name: rating
          property_type:
            primitive_type: DT_SIGNED_INT32
    - type_name: FOLLOWS
      vertex_type_pair_relations:
        - source_vertex: Person
          destination_vertex: Person
          relation: MANY_TO_MANY
    - type_name: WROTE
      vertex_type_pair_relations:
        - source_vertex: Person
          destination_vertex: Movie
          relation: MANY_TO_MANY
    - type_name: PRODUCED
      vertex_type_pair_relations:
        - source_vertex: Person
          destination_vertex: Movie
          relation: MANY_TO_MANY
```

Supported primitive data types for properties include:
- DT_SIGNED_INT32
- DT_UNSIGNED_INT32
- DT_SIGNED_INT64
- DT_UNSIGNED_INT64
- DT_BOOL
- DT_FLOAT
- DT_DOUBLE
- DT_STRING
- DT_DATE32

For a comprehensive list of supported types, please refer to the [data model](./data_model) page.

## Step 2: Import Graph Data
To import your data, utilize the `import` functionality of the administrative tool:
```bash
bin/gs_interactive database import -g movies -c examples/movies/import.yaml
```

The `import.yaml` file maps raw data fields to the schema of the "modern" graph created in Step 1. Here's an illustrative example:

```yaml
graph: movies
loading_config:
  data_source:
    scheme: file  # only file and odps is supported now
    location: /home/graphscope/movies/
  import_option: init # append, overwrite, only init is supported now
  format:
    type: csv
    metadata:
      delimiter: "|"  # other loading configuration places here
vertex_mappings:
  - type_name: Person  # must align with the schema
    inputs:
      - Person.csv
  - type_name: Movie
    inputs:
      - Movie.csv
edge_mappings:
  - type_triplet:
      edge: ACTED_IN
      source_vertex:  Person
      destination_vertex:  Movie
    inputs:
      - ACTED_IN.csv
  - type_triplet:
      edge: DIRECTED
      source_vertex:  Person
      destination_vertex:  Movie
    inputs:
      - DIRECTED.csv
  - type_triplet:
      edge: FOLLOWS
      source_vertex:  Person
      destination_vertex:  Person
    inputs:
      - FOLLOWS.csv
  - type_triplet:
      edge: PRODUCED
      source_vertex:  Person
      destination_vertex:  Movie
    inputs:
      - PRODUCED.csv
  - type_triplet:
      edge: REVIEW
      source_vertex:  Person
      destination_vertex:  Movie
    column_mappings:
      - column:
          index: 3
          name: rating
        property: rating
    inputs:
      - REVIEWED.csv
  - type_triplet:
      edge: WROTE
      source_vertex:  Person
      destination_vertex:  Movie
    inputs:
      - WROTE.csv
```

Note: The provided yaml file above offers a basic configuration for data importing. For a comprehensive understanding of data import configurations, please consult the [data import](./data_import) page.

## Step 3: Start the Service with the New Graph
To start the service using the new graph, run:
```bash
bin/gs_interactive service start -g movies
```

Note: Stopping a prior service is necessary to start a new service with an alternative graph.


Now you can move to [Stored Procedure](./stored_procedures) to explore querying via stored procedures.


## Try other graphs

In addition to `movies` graph, we have also prepared the `graph_algo` graph. You can find the raw CSV files, graph.yaml, and import.yaml in the `./examples/graph_algo/` directory. You can import the `graph_algo` graph just like importing the `movies` graph. There are also some sample cypher queries, you can find them at [GraphScope/flex/interactive/examples/graph_algo](https://github.com/alibaba/GraphScope/tree/main/flex/interactive/examples/graph_algo).