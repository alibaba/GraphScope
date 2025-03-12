# Using Custom Graph

This guide walks you through the process of using custom graph data in GraphScope Interactive. The process comprises three main steps: 
- Creating a new graph,
- Importing graph data, and
- Starting the service with the new graph.

We use a simple graph, which contains only one kind vertices with label `person` and one kind edges with label `knows`, to demonstrate creating a new graph in Interactive.

## Step 1: Create a New Graph

Before starting, please make sure you are in the GLOBAL context.
```bash
gsctl use GLOBAL
```

First you need to define the vertex types and edge types of your graph, i.e. here is a sample definition of a `test_graph`. Save the file to disk with name `test_graph.yaml`. 

```yaml
name: test_graph
description: "This is a test graph"
schema:
  vertex_types:
    - type_name: person
      properties:
        - property_name: id
          property_type:
            primitive_type: DT_SIGNED_INT64
        - property_name: name
          property_type:
            string:
              long_text: ""
        - property_name: age
          property_type:
            primitive_type: DT_SIGNED_INT32
      primary_keys:
        - id
  edge_types:
    - type_name: knows
      vertex_type_pair_relations:
        - source_vertex: person
          destination_vertex: person
          relation: MANY_TO_MANY
      properties:
        - property_name: weight
          property_type:
            primitive_type: DT_DOUBLE
```

In this file:

- For each vertex type, specify its name, allowed properties, primary keys (if any), and other relevant details.
- For each edge type, define the source/destination vertex types and their associated properties.


To create a new graph `test_graph`, execute the following command:
```bash
gsctl create graph -f ./test_graph.yaml
```

For a comprehensive list of supported types, please refer to the [data model](./data_model) page.

## Step 2: Import Graph Data

To import your data, you need to first bind the data source and then submit a bulk loading job.

#### Bind Data Source

To create a new graph, you will need the original data of the graph. We currently support files in CSV format. Fortunately, we have prepared it for you, and you can find it here [modern-graph](https://github.com/alibaba/GraphScope/tree/main/flex/interactive/examples/modern_graph). 
The `import.yaml` file maps raw data fields to the schema of the "modern" graph created in Step 1. Here's an illustrative example `import.yaml`, 
note that each vertex/edge type need at least one input for bulk loading. 
In the following example, we will import data to the new graph from local file
`person.csv` and `person_knows_person.csv`. 
You can download the files from [GitHub](https://github.com/alibaba/GraphScope/tree/main/flex/interactive/examples/modern_graph), with following commands.

```bash
wget https://raw.githubusercontent.com/alibaba/GraphScope/main/flex/interactive/examples/modern_graph/person.csv
wget https://raw.githubusercontent.com/alibaba/GraphScope/main/flex/interactive/examples/modern_graph/person_knows_person.csv
```
 
After successfully downloading them, remember to replace `@/path/to/person.csv` and `@/path/to/person_knows_person.csv` with the actual path to files.

```{note}
`@` means the file is a local file and need to be uploaded.
```

```yaml
vertex_mappings:
  - type_name: person
    inputs:
      - "@/path/to/person.csv"
    column_mappings:
      - column:
          index: 0
          name: id
        property: id
      - column:
          index: 1
          name: name
        property: name
      - column:
          index: 2
          name: age
        property: age
edge_mappings:
  - type_triplet:
      edge: knows
      source_vertex: person
      destination_vertex: person
    inputs:
      - "@/path/to/person_knows_person.csv"
    source_vertex_mappings:
      - column:
          index: 0
          name: person.id
        property: id
    destination_vertex_mappings:
      - column:
          index: 1
          name: person.id
        property: id
    column_mappings:
      - column:
          index: 2
          name: weight
        property: weight
```


Note: The provided yaml file above offers a basic configuration for data importing. For a comprehensive understanding of data import configurations, please consult the [data import](./data_import) page.

Now bind the datasource to `test_graph` with following command

```bash
gsctl create datasource -f ./import.yaml -g test_graph
```

#### Create Data Loading Job

So far, we have only created the dataource, a job config `job_config.yaml` is also needed to data import.
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
  - type_name: person

edges:
  - type_name: knows
    source_vertex: person
    destination_vertex: person
```

Now create a bulk loading job  with following command

```bash
gsctl create loaderjob -f ./job_config.yaml -g test_graph
```

a message like `Create job xxx successfully` will be printed.

Wait the job to finish by checking the job status with following command

```bash
gsctl desc job <job_id>
```

## Step 3: Start the Service with the New Graph

After you have obtained a successful status with `gsctl desc job <job_id>`, you can now switch to the context of the `test_graph` graph.


```bash
gsctl use GRAPH test_graph
```

## Step 4: A More Complicated Movies Graph(optional)

The above graph is very simple, which only contains one kind vertices and one kind edges. 
For a more complicated example, We'll use the [`movies`](https://github.com/neo4j-graph-examples/movies/) graph as an example, you can download the files from our [Github Repo](https://github.com/alibaba/GraphScope/tree/main/flex/interactive/examples/movies).

```bash
wget https://interactive-release.oss-cn-hangzhou.aliyuncs.com/dataset/movies/movies.zip
```

Try to use the following configuration files to create `movie_graph`!

### movie_graph.yaml

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


### import.yaml

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

### job_config.yaml

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

## Try other graphs

In addition to `movies` graph, we have also prepared the `graph_algo` graph. You can find the raw CSV files, `graph.yaml`, and `import.yaml` in the `./examples/graph_algo/` directory. You can import the `graph_algo` graph just like importing the `movies` graph. There are also some sample cypher queries, you can find them at [GraphScope/flex/interactive/examples/graph_algo](https://github.com/alibaba/GraphScope/tree/main/flex/interactive/examples/graph_algo).

If you are seeking for a more complex and larger graph, then you may try `IMDB` graph. The schema definition and import configuration are available in the `./examples/imdb/` directory. 