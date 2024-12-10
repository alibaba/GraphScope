# Data Import Configuration

In our guide on [using custom graph data](./custom_graph_data.md), we introduced the basics of importing graph data using a simple YAML configuration. This section delves deeper, providing a thorough exploration of the extensive configuration options available for data import.

## Supported data source

Currently we only support import data to graph from local `csv` files or `odps` table. See configuration `loading_config.data_source.scheme`.

## Column mapping

When importing vertex and edge data into a graph, users must define how the raw data maps to the graph's schema. 
This can be done using a YAML configuration, as shown:

```yaml
- column:
    index: 0  # Column index in the data source
    name: col_name # If a column name is present
  property: property_name  # The mapped property name
```

The column mapping requirements differ based on the data source:

#### Import from CSV 

You can provide either `index`, `name`, or both. If both `index` and `name` are specified, we will check whether they matches.

#### Import from ODPS Table

You just need to specify the name of the `column`, since the name is guaranteed to be unique in a odps table. The `index` is disregarded.

## Sample Configuration for loading "Modern" Graph from csv files

To illustrate, let's examine the `modern_graph_import.yaml` file. This configuration is designed for importing the "modern" graph and showcases the full range of configuration possibilities. We'll dissect each configuration item in the sections that follow.

```{note}
Note that a `@` is prepend to the location, which means all files will be uploaded from local.
```

``` yaml
loading_config: 
  x_csr_params:
    parallelism: 1
    build_csr_in_mem: true
    use_mmap_vector: true
  data_source:
    scheme: file
    location: "@/home/modern_graph/"
  format: 
    metadata: 
      delimiter: "|"  
      header_row: true
      quoting: false
      quote_char: '"'
      double_quote: true
      escape_char: '\'
      block_size: 4MB
vertex_mappings: 
  - type_name: person  
    inputs: 
      - person.csv  
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
  - type_name: software
    inputs:
      - software.csv
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
          name: lang
        property: lang
edge_mappings: 
  - type_triplet:
      edge: knows  
      source_vertex:  person 
      destination_vertex:  person  
    inputs: 
      - person_knows_person.csv 
    source_vertex_mappings:
      - column:  
          index: 0  
          name: src_id
        property: id
    destination_vertex_mappings:
      - column:  
          index: 1  
          name: dst_id
        property: id
    column_mappings:
      - column:
          index: 2
          name: weight
        property: weight
  - type_triplet:
      edge: created
      source_vertex: person
      destination_vertex: software
    inputs:
      -  person_created_software.csv
    source_vertex_mappings:
      - column:
          index: 0
          name: id
    destination_vertex_mappings:
      - column:
          index: 1
          name: id
    column_mappings:
      - column:
          index: 2
          name: weight
        property: weight
```

## Sample configuration for loading "Modern Graph" from odps tables

```yaml
graph: dd_graph
loading_config:
  data_source:
    scheme: odps  # file, odps
  import_option: init # init, overwrite
  format:
    type: arrow
vertex_mappings:
  - type_name: person  # must align with the schema
    inputs:
      - your_proj_name/table_name/partition_col_name=partition_name
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
  - type_name: software
    inputs:
      - your_proj_name/table_name/partition_col_name=partition_name
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
          name: lang
        property: lang
edge_mappings:
  - type_triplet:
      edge: knows
      source_vertex: person
      destination_vertex: person
    inputs:
      - your_proj_name/table_name/partition_col_name=partition_name
    source_vertex_mappings:
      - column:
          index: 0
          name: src_user_id
        property: id
    destination_vertex_mappings:
      - column:
          index: 1
          name: dst_user_id
        property: id
    column_mappings:
      - column:
          index: 2
          name: weight
        property: weight
  - type_triplet:
      edge: created
      source_vertex: person
      destination_vertex: software
    inputs:
      - your_proj_name/table_name/partition_col_name=partition_name
    source_vertex_mappings:
      - column:
          index: 0
          name: id
    destination_vertex_mappings:
      - column:
          index: 1
          name: id
    column_mappings:
      - column:
          index: 2
          name: weight
        property: weight
```

## Configuration Breakdown
The table below offers a detailed breakdown of each configuration item. In this table, we use the "." notation to represent the hierarchy within the YAML structure.

| **Key** | **Default** | **Descriptions** | **Mandatory** |
| -------- | -------- | -------- |-------- |
| **loading_config**    | N/A     | Loading configurations     |  Yes   |
| loading_config.data_source    | N/A     | Place that maintains the raw data     |  Yes   |
| loading_config.data_source.location |	N/A | Path to the data source in the container, which must be mapped from the host machine while initializing the service |	Yes
| loading_config.scheme | file | The source of input data. Currently only `file` and `odps` are supported | No |
| loading_config.format    | N/A     | The format of the raw data in CSV    |  Yes   |
| loading_config.format.metadata    | N/A    | Mainly for configuring the options for reading CSV   |  Yes   |
| loading_config.format.metadata.delimiter | '|' | Delimiter used to split a row of data, escaped char are also supported, i.e. '\t' | Yes | 
| loading_config.format.metadata.header_row | true | Indicate if the first row should be used as the header | No | 
| loading_config.format.metadata.quoting | false | Whether quoting is used | No |
| loading_config.format.metadata.quote_char | '\"' | Quoting character (if `quoting` is true) | No |
| loading_config.format.metadata.double_quote | true |  Whether a quote inside a value is double-quoted | No |
| loading_config.format.metadata.escaping | false | Whether escaping is used | No |
| loading_config.format.metadata.escape_char | '\\' | Escaping character (if `escaping` is true) | No |
| loading_config.format.metadata.null_values | [] | Recognized spellings for null values | No |
| loading_config.format.metadata.batch_size | 4MB | The size of batch for reading from files | No |
| loading_config.x_csr_params.parallelism | 1 | Number of threads used for bulk loading | No |
| loading_config.x_csr_params.build_csr_in_mem | false | Whether to build csr fully in memory | No |
| loading_config.x_csr_params.use_mmap_vector | false | Whether to use mmap_vector rather than mmap_array for building | No |
| |  |  |  |
| **vertex_mappings** | N/A | Define how to map the raw data into a graph vertex in the schema | Yes |
| vertex_mappings.type_name |	N/A |	Name of the vertex type |	Yes |
| vertex_mappings.inputs |	N/A |	List of files containing raw data for the vertex type, relative to `loading_config.data_source.location` |	Yes |
| vertex_mappings.column_mappings | The columns in the raw data will be automatically mapped to the properties, in given order | Define which column of the vertex data maps to a given property  |	No |
| vertex_mappings.column_mappings.column.index |	N/A |	Index of the column in the raw data |	No |
| vertex_mappings.column_mappings.column.name |	N/A |	Name of the column in the raw data |	No |
| vertex_mappings.column_mappings.property |	N/A |	Property in the schema to which the column maps. |	No  |
|  |  |  |  |
| **edge_mappings** | N/A | Define how to map the raw data into a graph edge in the schema | Yes |
| edge_mappings.type_triplet |	N/A |	A triplet that uniquely defines an edge |	Yes |
| edge_mappings.type_triplet.edge |	N/A |	The type of the edge |	Yes |
| edge_mappings.type_triplet.source_vertex |	N/A |	Type of the source vertex for the edge |	Yes |
| edge_mappings.type_triplet.destination_vertex |	N/A |	Type of the destination vertex for the edge |	Yes |
| edge_mappings.inputs |	N/A |	List of files containing raw data for the edge type, relative to `loading_config.data_source.location` |	Yes |
| edge_mappings.source_vertex_mappings | The first column of the edge data |	Define which columns of the edge data map to its source vertex.  |	No |
| edge_mappings.source_vertex_mappings.column.index |	N/A |	Index of the column in the raw data representing the source vertex. |	No |
| edge_mappings.source_vertex_mappings.column.name |	N/A |	Name of the column in the raw data representing the source vertex. |	No |
| edge_mappings.destination_vertex_mappings | The second column of the edge data |	Define which columns of the edge data map to its source vertex.  |	No |
| edge_mappings.destination_vertex_mappings.column.index |	N/A |	Index of the column in the raw data representing the destination vertex. |	No |
| edge_mappings.destination_vertex_mappings.column.name |	N/A |	Name of the column in the raw data representing the destination vertex. |	No |
| edge_mappings.column_mappings | The columns in the raw data will be automatically mapped to the properties, in given order | Define which column of the edge data maps to a given property.  |	No |
| edge_mappings.column_mappings.column.index |	N/A |	Index of the column in the raw data |	No |
| edge_mappings.column_mappings.column.name |	N/A |	Name of the column in the raw data |	No |
| edge_mappings.column_mappings.property |	N/A |	Property in the schema to which the column maps. |	No (but must present if column presents) |

### Loading Configurations
The **loading_config** section defines the primary settings for data loading. This includes specifying the data source, its format, and other metadata.

- loading_config.data_source.location: This is the path to the data source within the container. It's essential to ensure this location is mapped from the host machine when initializing the service.
- loading_config.format.metadata: Mainly for configuring the options for reading CSV
	- loading_config.format.metadata.delimiter: Defines the delimiter used to split rows of data.
	- loading_config.format.metadata.header_row: Indicate if the first row should be used as the header.
	- loading_config.format.metadata.quoting: Whether quoting is used
	- loading_config.format.metadata.quote_char: Quoting character (if `quoting` is true)
	- loading_config.format.metadata.double_quote:  Whether a quote inside a value is double-quoted
	- loading_config.format.metadata.escaping: Whether escaping is used
	- loading_config.format.metadata.escape_char: Escaping character (if `escaping` is true)
	- loading_config.format.metadata.block_size: The size of batch for reading from files
	- loading_config.format.metadata.null_values: Recognized spellings for null values

### Vertex Mappings
The **vertex_mappings** section outlines how raw data maps to graph vertices based on the defined schema. 
- vertex_mappings.type_name: Specifies the name of the vertex type.
- vertex_mappings.inputs: Lists the files containing raw data for the vertex type. These paths are relative to `loading_config.data_source.location`.
- vertex_mappings.column_mappings.column**: Define which column of the vertex data maps to a given property. If not give, the columns in the raw data will be automatically mapped to the properties, in given order. 
	- vertex_mappings.column_mappings.column.index: Index of the column in the raw data.
	- vertex_mappings.column_mappings.column.name: Name of the column in the raw data.
- vertex_mappings.column_mappings.property: Property in the schema to which the column maps.

### Edge Mappings
The **edge_mappings** section details how raw data maps to graph edges as per the schema.
- edge_mappings.type_triplet: A triplet that uniquely defines an edge.
	- edge_mappings.type_triplet.edge: Indicates the type of the edge.
 	- edge_mappings.type_triplet.source_vertex: Indicates the type of the source vertex.
  	- edge_mappings.type_triplet.destination_vertex: Indicates the type of the destination vertex.		
- edge_mappings.inputs: Lists the files containing raw data for the edge type. These paths are relative to `loading_config.data_source.location`.
- edge_mappings.source_vertex_mappings: Define which columns (can be multiple) of the edge map to its source vertex. If not given, the **first** column of the edge data will be used by default.
	- edge_mappings.source_vertex_mappings.column.index: Index of the column in the raw data representing the source vertex.
 	- edge_mappings.source_vertex_mappings.column.name: Name of the column in the raw data representing the source vertex.
- edge_mappings.destination_vertex_mappings: Define which columns (can be multiple) of the edge map to its destination vertex. If not given, the **second** column of the edge data will be used by default.
	- edge_mappings.destination_vertex_mappings.column.index: Index of the column in the raw data representing the destination vertex.
 	- edge_mappings.destination_vertex_mappings.column.name: Name of the column in the raw data representing the destination vertex.
- edge_mappings.column_mappings.column: Define which column of the edge data maps to a given property. If not give, the columns in the raw data will be automatically mapped to the properties, in given order. 
	- edge_mappings.column_mappings.column.index: Index of the column in the raw data.
	- edge_mappings.column_mappings.column.name: Name of the column in the raw data.
- edge_mappings.column_mappings.property: Property in the schema to which the column maps.	


By understanding and appropriately setting these configurations, users can seamlessly import their graph data into GraphScope, ensuring data integrity and optimal performance.