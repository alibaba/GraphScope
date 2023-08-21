# Data import

This guide will walk you through the process of loading your own data using a custom graph schema. Different from  graph databases such as [Neo4j]((https://neo4j.com/)), we embrace a schema-strict approach rather than a schema-free one. This decision stems from the demands of real-world applications. A schema-strict methodology offers greater consistency and fewer issues in managing business scenarios.

For your reference, we have provided the example data and configuration files for loading [modern graph](https://tinkerpop.apache.org/docs/current/tutorials/getting-started/) in `$GS_INTERACTIVE_HOME/examples`. In addition, for more details of the schema configuration, including the explanation of each configurable section, as well as the comprehensive list of the supported data types, please refer to the page of [data model](./data_type.md).


## Define Your Custom Graph Schema
To use your own data, you need to define a custom graph schema in the `gs_interactive_image.yml` file during the [initialization](./getting_started.md) of the Interactive service. This schema will define each vertex type’s name, properties, primary keys, and each edge type’s type name, source/destination vertex type names, and properties.

Here's an example schema:

```yaml
...
graphs:
    - name: modern
      schema:
          vertex_types:
          - type_name: person
            properties:
            - property_name: id
              property_data_type:
                  primitive_type: DT_SIGNED_INT64
            - property_name: name
              property_data_type:
                  primitive_type: DT_STRING
            primary_keys: # must present in the properties
              - id  
            # ... other vertex types
          edge_types:
          - type_name: knows
            vertex_type_pair_relations:
            - source_vertex: person
              destination_vertex: person
              relation: MANY_TO_MANY
            properties:
            - property_name: weight
              property_data_type:
                  primitive_type: DT_DOUBLE
            # ... other edge types

```

The supported primitive data types for properties are:

- DT_SIGNED_INT32
- DT_UNSIGNED_INT32
- DT_SIGNED_INT64
- DT_UNSIGNED_INT64
- DT_BOOL
- DT_STRING

## Importing Your Data
To import your data, use the `-b` or `--bulk_load` option with a yaml file while starting the Interactive service:

```bash
$GS_INTERACTIVE_HOME/bin/db_admin.sh start -n modern -b ./examples/modern_graph.yaml
```

The `bulk_loading.yaml` file provides the mapping of raw data fields to the schema you've defined. Here's an example:

```yaml
graph: modern
loadingConfig:
  dataLocation: /path/to/raw/data
  metaData:
    delimiter: "|"
vertexMappings:
   - typeName: person
      inputs:
        - person.csv
      columnMappings:
        - column:
            index: 0
            name: id # useful while loading from sql-like tables
          property: id
        - column:
            index: 2 # the index 1 after row splitting will be skipped
            name: name
          property: name
# ... other vertex and edge types

```

NOTE:

- Not all data fields in the raw data are required. You can specify which columns to load using the columnMappings field.
- The current data import process will overwrite all existing data. We're working on providing an `append` option in future versions.
- At present, we only support loading data from local CSV files. Support for other file systems like S3, HDFS, and OSS is in the pipeline.

With this guide, you should be able to define your custom graph schema and import your data into GraphScope Interactive. 