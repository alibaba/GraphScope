# Using Custom Graph

This guide walks you through the process of using custom graph data in GraphScope Interactive. The process comprises three main steps: 
- Creating a new graph,
- Importing graph data, and
- Starting the service with the new graph.

We'll use the [`movies`](https://github.com/neo4j-graph-examples/movies/) graph as an example, with the necessary sample files located in `{INTERACTIVE_HOME}/examples/movies/`.

## Step 1: Create a New Graph

Before starting, please make sure you are in the GLOBAL context.
```bash
gsctl use GLOBAL
```

To create a new graph, you will need the original data of the graph. We currently support files in CSV format. Fortunately, we have prepared it for you, and you can find it here [movie-graph](https://github.com/alibaba/GraphScope/tree/main/flex/interactive/examples/movies). 

The schema of movie-graph is defined in `movie_graph.yaml`.
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
            string:
              long_text: ""
        - property_name: title
          property_type:
            string:
              long_text: ""
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
            string:
              long_text: ""
      primary_keys:
        - id
    - type_name: User
      properties:
        - property_name: id
          property_type:
            primitive_type: DT_SIGNED_INT64
        - property_name: born
          property_type:
            primitive_type: DT_SIGNED_INT32
        - property_name: name
          property_type:
            string:
              long_text: ""
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
        - source_vertex: User
          destination_vertex: Movie
          relation: MANY_TO_MANY
      properties:
        - property_name: rating
          property_type:
            primitive_type: DT_SIGNED_INT32
    - type_name: FOLLOWS
      vertex_type_pair_relations:
        - source_vertex: User
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

In this file:

- For each vertex type, specify its name, allowed properties, primary keys (if any), and other relevant details.
- For each edge type, define the source/destination vertex types and their associated properties.


To create a new graph "movies", execute the following command:
```bash
gsctl create graph -f ./movie_graph.yaml
```

For a comprehensive list of supported types, please refer to the [data model](./data_model) page.

## Step 2: Import Graph Data

To import your data, you need to two configuration files, `movie_import.yaml` and `job_config.yaml`

#### Bind Data Source

The `import.yaml` file maps raw data fields to the schema of the "modern" graph created in Step 1. Here's an illustrative example `movie_import.yaml`:

```yaml
vertex_mappings:
  - type_name: Person  # must align with the schema
    inputs:
      - "@/path/to/Person.csv"
  - type_name: Movie
    inputs:
      - "@/path/to/Movie.csv"
edge_mappings:
  - type_triplet:
      edge: ACTED_IN
      source_vertex:  Person
      destination_vertex:  Movie
    inputs:
      - "@/path/to/ACTED_IN.csv"
  - type_triplet:
      edge: DIRECTED
      source_vertex:  Person
      destination_vertex:  Movie
    inputs:
      - "@/path/to/DIRECTED.csv"
  - type_triplet:
      edge: FOLLOWS
      source_vertex:  Person
      destination_vertex:  Person
    inputs:
      - "@/path/to/FOLLOWS.csv"
  - type_triplet:
      edge: PRODUCED
      source_vertex:  Person
      destination_vertex:  Movie
    inputs:
      - "@/path/to/PRODUCED.csv"
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
      - "@/path/to/REVIEWED.csv"
  - type_triplet:
      edge: WROTE
      source_vertex:  Person
      destination_vertex:  Movie
    inputs:
      - "@/path/to/WROTE.csv"
```

Note: The provided yaml file above offers a basic configuration for data importing. For a comprehensive understanding of data import configurations, please consult the [data import](./data_import) page. Remember to replace `@/path/to/xxx.csv` with the actual path to files.

Now bind the datasource to `movie_graph` with following command

```bash
gsctl create datasource -f ./movie_import.yaml -g movies
```

#### Create Data Loading Job

A `job_config.yaml` is  also needed to specify the configuration for bulk loading.

```yaml
loading_config:
  import_option: overwrite
  format:
    type: csv
    metadata:
      delimiter: "|"
      header_row: "true"

vertices:
  - type_name: Person
  - type_name: Movie

edges:
  - type_name: ACTED_IN
    source_vertex:  Person
    destination_vertex:  Movie
  - type_name: DIRECTED
    source_vertex:  Person
    destination_vertex:  Movie
  - type_name: FOLLOWS
    source_vertex:  Person
    destination_vertex:  Person
  - type_name: PRODUCED
    source_vertex:  Person
    destination_vertex:  Movie
  - type_name: REVIEW
    source_vertex:  Person
    destination_vertex:  Movie
  - type_name: WROTE
    source_vertex:  Person
    destination_vertex:  Movie
```

Now create a bulk loading job  with following command

```bash
gsctl create loaderjob -f ./job_config.yaml -g movies
```

a message like `Create job xxx successfully` will be printed.

Wait the job to finish by checking the job status with following command

```bash
gsctl desc job <job_id>
```

## Step 3: Start the Service with the New Graph

To start the service using the new graph, run:

```bash
gsctl use GRAPH movies
```

Now you can move to [Stored Procedure](./stored_procedures) to explore querying via stored procedures.


## Try other graphs

In addition to `movies` graph, we have also prepared the `graph_algo` graph. You can find the raw CSV files, graph.yaml, and import.yaml in the `./examples/graph_algo/` directory. You can import the `graph_algo` graph just like importing the `movies` graph. There are also some sample cypher queries, you can find them at [GraphScope/flex/interactive/examples/graph_algo](https://github.com/alibaba/GraphScope/tree/main/flex/interactive/examples/graph_algo).