We explain the organization of the yaml files.

## settings.yaml
This is the configuration file of the GIE dbms, defining the version of the dbms,
some directories (dbms, logging). While it may include some necessary configurations
of each engine, including pegasus, hiactor, groot, gart, etc.

From now, we write $WORK_DIR as the directory configured in settings.yaml.

## graph.yaml
Note that GIE dbms may contain multiple graphs (although we support only one graph, but
do not prevent potential extensions in the future). Each graph has an individual folder
under the $WORK_DIR, given by $WORK_DIR/graph_name.

This file contains configuration to one graph in GIE dbms, including but not limited to:
- name: this must aligned with the folder name
- schema: configure the schema's yaml file, relative to this graph's folder, namely $WORK_DIR/graph_name
- stored_procedures: configure the stored procedures preinstalled to this graph, relative to this graph's folder
- log_dir: the directory for recording this graph-related logs
- data_dir: the directory for maintaining any on-disk data from the graph

## schema.yaml
This file configures the schema of the current graph.
Schema configures, for each type of vertex and edge, all properties' names and data types.
Note that schema may associate with a snapshot, indicating since when this schema is valid.
By default, the schema.yaml under $WORK_DIR refers to the latest version, while historical
schema may be maintained under the folder of history, given by `<snapshot_id>.yaml`

## stored_procedures.yaml
This file configures the pre-installed stored procedure to the graph, where each stored procedure
records the information of:
- name: the unique name of the stored procedure,
- description: the description of the stored procedure, by default the name
- read_only: whether the stored procedure is read_only, by default true
- directory: the directory whether the "so" file of this stored procedure can be found. Note that
          this is not the directory where the stored procedure will be loaded to.

The stored procedures, for simplicity, are designed to intentionally **not** aligned with
the schema (and snapshot). We consider:
  - it is okay to return error if the stored procedure uses some non-existed type of vertex/edge;
  - store procedures are often used for performance critical tasks, and considering schema dependency may introduce unnecessary overhead;
  - most existing stored procedures are not schema-dependent.
  - it is much easier to manage the stored procedures if we do not need to handle such schema dependency.

## data_loading.yaml
This file configures how to load raw data from the other data source into the dbms of certain graph.
We specially design this data_loading.yaml to consider:
- the data_loading.yaml can be anywhere, without the need of having to place under $WORK_DIR. Therefore,
  the directory of the schema is an **absolute** path.
- multiple possible data sources: local file system, oss, s3, hdfs, odps, we thus use "data_location" instead
  of "directory", "data_source_name" instead of "file_name"
- the mappings between raw data filed and property:
  -  raw data filed may be identified by splitting files, original table
  -  it's not necessary to load all raw data fields into the graph, but one has to guaranteed that the
      loaded fields are equal to the number of properties.
  -  the data_fields in mappings can be omitted, meaning that the data fields, in its index, are aligned
     with the properties perfectly
  -  for loading vertices, we can set special property "TYPE" (must be capital) to indicate that this
      field refers to a type of the vertex, e.g. "CITY" in "place.csv"
  -  for loading edges, we can set special property "SOURCE_VERTEX_ID" and "DESTINATION_VERTEX_ID" to
     indicate that this field refers to the source and destination vertices of the graph.


## The final version for PoC
Changes:
- officially using the name: gs.interactive.dbms, for GIE's db-like production
- naming convention:
  - file name: xx_xx.yaml
  - key name: camelLikeKeyName
- gs_interactive.yaml: configure items for gs.interactive.dbms
  - all graphs will be maintained under the directory of: data/graph_name
  - can specify the default graph to launch via configuring default_graph
  - configure multiple connectors, including
    - http_connector
    - bolt_connector
- graph.yaml: configure all graph-related metadata, including schema
  - there are configurations specific to a certain store, e.g. csr
    - xCsrParams: configure graph/vertex/edge-specific parameters
    - xCsrType: configure store-specific data types
- data_loading.yaml: configure all items related to loading raw tabular data into the graph database
- example_all.yaml, ldbc_ic1.yaml: Configure the parameters of stored procedure corresponding to the compiled library
