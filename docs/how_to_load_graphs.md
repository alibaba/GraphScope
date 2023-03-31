# How to Load Graphs

## Define Graph Schema

## Load Graphs

## Advanced Topics

### Tips for reduce memory consumption of graphs

- Tune the parameter of graph constructor `sess.g()`
    - Set `directed=False` to use an undirected graph if you do not require edge directionality. Undirected graphs require less memory than directed graphs for the same data size, as we do not need to store edge directions.
    - Set `generate_eid=False` if you do not require edge ids for interactive engine (GIE) operations.
    - Set `retain_oid=False` if you do not require the ID column as a property for interactive engine (GIE) operations.
    - Set `oid_type='int32_t'` when the ID does not exceed `2^31 - 1`.
    - Providing a complete schema that specifies the data type of each property instead of allowing GraphScope to infer it from data could benefit most cases.

- Filter out super vertices according to the requirements of the business scenario. For certain business scenarios or algorithms, high precision may not be necessary, especially when dealing with very large graph data.



