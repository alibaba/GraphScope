# Data Model

When working with GraphScope Interactive, the data model must be defined while [using custom graph data](./custom_graph_data). This data model consists of two primary components: graph data and entity data.

## Graph Data
The graph data encompasses two fundamental elements:

- **Vertex (Vertices)**: A vertex represents an entity in the graph. In the context of the property graph model, vertices are often referred to as nodes. Each vertex has a unique identifier and can have zero or more properties associated with it. For example, in a social network graph, a vertex could represent a person, with properties like name, age, and location.

- **Edge (Edges)**: Edges define the relationships between vertices. Each **directed** edge has a source vertex, a destination vertex, and a type that describes the nature of the relationship. Additionally, edges can also have properties. For instance, in a social network graph, an edge might represent a friendship between two people and could have properties like "since_date" indicating when the friendship started.

Note: This graph model aligns with the property graph model, which offers a detailed explanation [here](https://subscription.packtpub.com/book/data/9781784393441/1/ch01lvl1sec09/the-property-graph-model). However, it's essential to note our terminology: we use "vertex" instead of "node" and "edge" instead of "relationship", and we only support **directed** edge instead of both directed and undirected edge. 

Within the `graph.yaml` file, vertices are delineated under the `vertex_types` section. Each vertex type is structured with mandatory fields: `type_name`, `properties`, and `primary_keys`. For instance:

```yaml
- type_name: person
  properties:
    - property_name: id
      property_type:
        primitive_type: DT_SIGNED_INT64
    - property_name: name
      property_type:
        primitive_type: DT_STRING
  primary_keys: # these must also be listed in the properties
    - id  
```

Note:
- In the current version, only one single primary key can be specified, but we plan to support multiple primary keys in the future. 
- The data type of primary key column must be one of `DT_SIGNED_INT32`, `DT_UNSIGNED_INT32`, `DT_SIGNED_INT64` or `DT_UNSIGNED_INT64`.

Edges are defined within the `edge_types` section, characterized by the mandatory fields: `type_name`, `vertex_type_pair_relations`, and `properties`. The type_name and properties fields function similarly to those in vertices. However, the vertex_type_pair_relations field is exclusive to edges, specifying the permissible source and destination vertex types, as well as the relationship detailing how many source and destination vertices can be linked by this edge. Here's an illustrative example:
```yaml
type_name: knows
vertex_type_pair_relations:
  - source_vertex: person
    destination_vertex: person
    # the "knows" edge can link multiple source and destination vertices
    relation: MANY_TO_MANY  
```

Note: 
- A single edge type can have multiple `vertex_type_pair_relations`. For instance, a "knows" edge might connect one person to another, symbolizing their friendship. Alternatively, it could associate a person with a skill, indicating their proficiency in that skill.
- The permissible relations include: `ONE_TO_ONE`, `ONE_TO_MANY`, `MANY_TO_ONE`, and `MANY_TO_MANY`. These relations can be utilized by the optimizer to generate more efficient execution plans.
- Currently we only support at most one property for each edge triplet.
- All implementation related configuration are put under `x_csr_params`.
  - `max_vertex_num` limit the number of vertices of this type:
    - The limit number is used to `mmap` memory, so it only takes virtual memory before vertices are actually inserted.
    - If `max_vertex_num` is not set, a default large number (e.g.: 2^48) will be used.
  - `edge_storage_strategy` specifies the storing strategy of the incoming or outgoing edges of this type, there are 3 kinds of strategies
    - `ONLY_IN`: Only incoming edges are stored.
    - `ONLY_OUT`: Only outgoing edges are stored.
    - `BOTH_OUT_IN`(default): Both direction of edges are stored.
 

## Entity Data

Entity data pertains to the properties associated with vertices and edges. In GraphScope Interactive, we support a diverse range of data types for these properties:

### Primitive Types
- DT_SIGNED_INT32
- DT_UNSIGNED_INT32
- DT_SIGNED_INT64
- DT_UNSIGNED_INT64
- DT_BOOL
- DT_FLOAT
- DT_DOUBLE
- DT_STRING
- DT_DATE32

In the `graph.yaml`, a primitive type, such as `DT_STRING`, can be written as:
```yaml
property_type:
  primitive_type: DT_STRING
```

### Array Types

Array types are currently not supported, but are planned to be supported in the near future.
Once supported, albeit requiring that every element within the array adheres to one of the previously mentioned primitive types. 
It's crucial that all elements within a single array share the same type. In `graph.yaml`, user can describe designating a property as an array of the `DT_STRING` type as:

```yaml
property_type:
  array:
    component_type: 
      primitive_type: DT_UNSIGNED_INT64
      max_length: 10  # overflowed elements will be truncated
```
