Data Source
===========

This document is used to explain the data format supported by GraphLearn
and the API to describe and parse it.

1 Data format
=============

Graph data can be divided into **vertex data** and **edge data**.
Generally, vertex data contains **vertex ID** and **attribute**,
describing an entity. Edge data contains **source vertex ID** and
**destination vertex ID**, describing the relationship between vertices.
In heterogeneous graph settings, there are multiple types of vertices
and edges. Therefore, we need the type of information of vertices and
edges to distinguish different types. Type information is described
through APIs. Both vertices and edges have attributes. For example, in
“a user bought a certain product on Saturday morning”, the time
information “Saturday morning” is an edge attribute. Also, in many
scenarios, users need the concept of “weight”—vertex weight or edge
weight, as a measure of importance, such as “sample neighbor nodes by
weight”. The sources of “weight” are diverse, varying from task to task.
In supervised learning classification tasks, vertices or edges may also
have labels. We abstract the data formats of these typical settings as
**ATTRIBUTED**, **WEIGHTED, and LABELED**, which are used to represent
vertex or edge containing attributes, weights, and labels. For vertex
data sources and edge data sources, these three can co-exist or
partially exist at the same time.

1.1 Basic format
----------------

The basic vertex data only contains the ID of a vertex. The type of ID
is bigint and each ID uniquely represents one vertex. However, in many
cases vertices with only IDs are far from enough. Therefore, they can
also contain attributes, weights or labels. The basic edge data only
contains the source vertex ID and the destination vertex ID. The ID type
of edge is also bigint and each ID uniquely represents one edge and the
relationship between two vertices. The schema of the basic edge data
source is shown below. The basic edge data format can be used
independently, without attaching attributes, weights, and labels.

The schema of the basic edge data format:

====== ========= ========
source data type comments
====== ========= ========
src_id BIGINT    
dst_id BIGINT    
====== ========= ========

1.2 Attribute format（ATTRIBUTED）
----------------------------------

The attribute format is used to express the attribute information of a
vertex or an edge. In general, vertices have attributes by default
(otherwise only the edge table is sufficient). The attribute has only
one column, which is of string type. Multiple attributes can be divided
by custom separators inside the string. For example, if there are 3
attributes of a vertex, namely ``shanghai, 10, 0.01`` separated by the
separator ‘:’, the attribute data corresponding to the vertex is
``shanghai:10:0.01``. When the data format has attributes regardless of
whether it is vertex data or edge data, it is necessary to display and
specify **ATTRIBUTED** to inform the system in the API description.

The schema of the vertex data attribute format:

========== ========= ========
source     data type comments
========== ========= ========
id         BIGINT    
attributes STRING    
========== ========= ========

The schema of the edge data attribute format:

========== ========= ========
source     data type comments
========== ========= ========
src_id     BIGINT    
dst_id     BIGINT    
attributes STRING    
========== ========= ========

1.3 Weight format（WEIGHTED）
-----------------------------

The weight format is used to express a vertex or an edge with weight.
Similar to attributes, the weight has only one column, which is of type
**float**. When the data format has weights, it is necessary to display
the specified **WEIGHTED** to inform the system, in the API description.

The schema of the vertex data weight format:

========== ========= ========
source     data type comments
========== ========= ========
id         BIGINT    
attributes FLOAT     
========== ========= ========

The schema of the edge data weight format:

====== ========= ========
source data type comments
====== ========= ========
src_id BIGINT    
dst_id BIGINT    
weight FLOAT     
====== ========= ========

1.4 Label format（LABELED）
---------------------------

The label format is used to express the situation where the vertices or
edges are labeled. The label has only one column, which is of type int.
When the data format has labels, it is necessary to display and specify
**LABELD** to inform the system, in the API description.

The schema of the vertex data label format:

====== ========= ========
source data type comments
====== ========= ========
id     BIGINT    
label  INT       
====== ========= ========

The schema of the edge data weight format:

====== ========= ========
source data type comments
====== ========= ========
src_id BIGINT    
dst_id BIGINT    
label  INT       
====== ========= ========

1.5 Putting together
--------------------

ID is required for the vertex and edge data source, while weight, label,
and attribute are optional information. When there are one or more of
**WEIGHTED, ATTRIBUTED, and LABELED** at the same time in the data
source, the combination format of mandatory and optional information
needs to follow a certain order. 1）The order of the format schema for
**vertex data source** is shown as follow.

========== ========= ====================
source     data type comments
========== ========= ====================
id         BIGINT    mandatory
weight     FLOAT     optional: WEIGHTED
label      BIGINT    optional: LABELED
attributes STRING    optional: ATTRIBUTED
========== ========= ====================

2）The order of the format schema for **edge data source** is shown as
follow

========== ========= ====================
source     data type comments
========== ========= ====================
src_id     BIGINT    mandatory
dst_id     BIGINT    mandatory
weight     FLOAT     optional: WEIGHTED
label      BIGINT    optional: LABELED
attributes STRING    optional: ATTRIBUTED
========== ========= ====================

We can choose zero or some optional information but must **guarantee the
orders** as specified in the above tables.

2 Data source type
==================

.. container::

The system abstracts the data access layer to allow easy connection to
multiple types of data sources. Currently, it supports LocalFileSystem.
If it is used on the Alibaba Cloud PAI platform, you can directly read
MaxCompute data tables. The data is represented as a two-dimensional
structure, the row represents a vertex or an edge data, and the column
represents a certain dimension of information of the vertex or edge.

## 2.1 Local FileSystem In the local file, the data types are specified
as follows, where the column name is not required. It supports reading
data from one or more local files to facilitate local debugging process.

======== ======
column   type
======== ======
id       int64
weight   float
label    int32
features string
======== ======

- Vertex file format is defined as follow, where the first line is the
column name indicating the required information or extended information,
separated by **tabs**, and each row element is “column name: data type”.
Each remaining row of data represents the information of a vertex,
corresponding to the information name in the first column, separated by
**tab**.

.. code:: python

   # file://node_table
   id:int64  feature:string
   0 shanghai:0:s2:10:0.1:0.5
   1 beijing:1:s2:11:0.1:0.5
   2 hangzhou:2:s2:12:0.1:0.5
   3 shanghai:3:s2:13:0.1:0.5

2）Edge file format is defined as follow, where the first line is the
column name indicating the required information or extended information,
separated by **tabs**, and each element is “column name: data type”.
Each remaining row of data represents the information of an edge,
corresponding to the information name in the first column, separated by
**tab**.

.. code:: python

   # file://edge_table
   src_id:int64  dst_id:int64  weight:float  feature:string
   0 5 0.215340  red:0:s2:10:0.1:0.5
   0 7 0.933091  grey:0:s2:10:0.1:0.5
   0 1 0.362519  blue:0:s2:10:0.1:0.5
   0 9 0.097545  yellow:0:s2:10:0.1:0.5

By using local files as data sources, you can directly use file paths in
scripts. See the next chapter “`graph object <graph_object_cn.md>`__”
for details.

2.2 Alibaba Cloud MaxCompute data table
---------------------------------------

The data format of MaxCompute data table is described as follow, where
column name is not required.

======== ======
column   type
======== ======
id       BIGINT
weight   FLOAT
label    BIGINT
features STRING
======== ======

To use MaxCompute as a data source, the following two steps are
required:1) Submit a GL Job through the PAI command, and use the
MaxCompute table as the input of the ``tables`` parameter. Multiple
tables are separated by commas.

.. code:: python

   pai -name graphlearn
   -Dscript=''
   -DentryFile=''
   -Dtables="odps://prj/tables/node_table,odps://prj/tables/edge_table"
   ...;

2） In the script, the MaxCompute table parameters are obtained through
FLAG of TensorFlow in order to obtain the data source. The data source
can be one or more.

.. code:: python

   import tensorflow as tf
   import graphlearn as gl

   flags = tf.app.flags
   FLAGS = flags.FLAGS
   flags.DEFINE_string("tables", "", "odps table name")
   node_source, edge_source = FLAGS.tables.split(',')

3 User API
==========

## 3.1 Decoder type ``Decoder`` type is used to described the
aforementioned data format as follows.

.. code:: python

   class Decoder(weighted=False, labeled=False, attr_types=None, attr_delimiter=":")
   """
   weighted:       Describe whether the data source is weighted, the default is False
   labeled:        Describe whether the data source has a label, the default is False
   attr_types:     When the data source has attributes, the parameter is a string list,
                   describing the type of each attribute.
                   Each element in the list only supports "string", "int" and "float" types.
                   The parameter format is like ["string", "int", "float"], representing the data contains 3 attributes,
                   In order, they are string type, int type, float type.
                   The default is None, that is, the data source has no attributes.
   attr_delimiter: When the data has attributes (compressed into a large string), you need to 
                   know how to parse it. This parameter describes the separator between each attribute.
                   For example, "shanghai:0:0.1", the separator is ":". The default is":".
   """

Considering the combination with neural network, string type attributes
are more difficult to handle. The common practice is to first map string
to int through hash, and then encode int into embedding. For this
reason, GL has made a special extension to the attributes of the string
type, that is, it supports converting string to int during the
initialization phase of graph data. At this time, the “string” in the
attr_types parameter needs to be changed to the tuple type
``("string", bucket_size)``, and bucket_size represents the size of the
int space to be converted. When conversion is done, the subsequent
visits will be unified as int-typed attributes. In addition to
simplifying subsequent operations, this conversion will greatly reduce
memory overhead.

## 3.2 Vertex decoder Vertex decoder can take the following formats.

.. code:: python

   import graphlearn as gl

   # schema = (src_id int64, dst_id int64, weight double)
   gl.Decoder(weighted=True)

   # schema = (src_id int64, dst_id int64, label int32)
   gl.Decoder(labeled=True)

   # schema = (src_id int64, dst_id int64, attributes string)
   gl.Decoder(attr_type={your_attr_types}, attr_delimiter={you_delimiter})

   # schema = (src_id int64, dst_id int64, weight float, attributes string)
   ag.Decoder(weightd=True, attr_type={your_attr_types}, attr_delimiter={you_delimiter})

   # schema = (src_id int64, dst_id int64, weight float, label int32)
   gl.Decoder(weighted=True, labeled=True)

   # schema = (src_id int64, dst_id int64, label int32, attributes string)
   gl.Decoder(labeled=True, attr_type={your_attr_types}, attr_delimiter={you_delimiter})

   # schema = (src_id int64, dst_id int64, weight float, label int32 attributes string)
   gl.Decoder(weighted=True, labeled=True, attr_type={your_attr_types}, attr_delimiter={you_delimiter})

## 3.3 Edge decoder Edge decoder can take the following formats.

.. code:: python

   import graphlearn as gl

   # schema = (scr_id int64, dst_id int64)
   gl.Decoder()

   # schema = (src_id int64, dst_id int64, weight float)
   gl.Decoder(weighted=True)

   # schema = (src_id int64, dst_id int64, label int32)
   gl.Decoder(labeled=True)

   # schema = (src_id int64, dst_id int64, attributes string)
   gl.Decoder(attr_type={your_attr_types}, attr_delimiter={you_delimiter})

   # schema = (src_id int64, dst_id int64, weight float, attributes string)
   gl.Decoder(weightd=True, attr_type={your_attr_types}, attr_delimiter={you_delimiter})

   # schema = (src_id int64, dst_id int64, weight float, label int32)
   gl.Decoder(weighted=True, labeled=True)

   # schema = (src_id int64, dst_id int64, weight float, label int32, attributes string)
   gl.Decoder(weighted=True, labeled=True, attr_type={your_attr_types}, attr_delimiter={you_delimiter})

   # schema = (src_id int64, dst_id int64, label int32, attributes string)
   gl.Decoder(labeled=True, attr_type={your_attr_types}, attr_delimiter={you_delimiter})

## 3.4 Example Assume a data source shown in table 1, 2 and 3.

Table 1: item vertex table

===== ==============
id    feature
===== ==============
10001 feature1:1:0.1
10002 feature2:2:0.2
10003 feature3:3:0.3
===== ==============

Table 2 user vertex table

=== ===========
id  feature
=== ===========
123 0.1:0.2:0.3
124 0.4:0.5:0.6
125 0.7:0.8:0.9
=== ===========

Table 3 user-item edge table

====== ====== ======
src_id dst_id weight
====== ====== ======
123    10001  0.1
124    10001  0.2
124    10002  0.3
====== ====== ======

Here, we construct
ʻitem_node_decoder\ ``for item vertex table, ʻuser_node_decoder`` for
user vertex table and ʻedge_decoder\` for edge table with the following
code.

.. code:: python

   import graphlearn as gl

   item_node_decoder = gl.Decoder(attr_types=["string", "int", "float"])
   user_node_decoder = gl.Decoder(attr_types=["float", "float", "float"])
   edge_decoder = gl.Decoder(weighted=True)

After constructing the Decoder for each data source, we add the data
source to the graph and specify the corresponding Decoder. Please refer
to `graph_object <graph_object_en.md>`__ for details.
