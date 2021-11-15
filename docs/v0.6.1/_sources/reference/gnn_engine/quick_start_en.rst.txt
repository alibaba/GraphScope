Quick Start
===========

This tutorial has three parts: - How to quickly try out a graph learning
model using **GL**. - How to import data into **GL**, and use the graph
data, graph sampling and negative sampling APIs. - How to develop your
own GNN models using **GL** and Tensorflow using **GraphSAGE** as an
example.

1 Trying out the built-in models
================================

We have implemented in **GL** a set of popular GNN models, such as
**GCN**, **GraphSAGE**, and some datasets, such as **cora**, **ppi**. To
try out **GL**, we can start with the vertex-classification task in
**core** . The codes of all example models are available in `Example
Models <model_examples.md>`__.

.. code:: shell

   # Data preparation
   cd graph-learn/examples/data/
   python cora.py

   # Training and model assessment
   python train_supervised.py

2 How to use the **GL** APIs
============================

**GL** provides a large collection of basic APIs for developing GNN
models. In this part, we will go through how to use them to (1)
construct graphs, (2) to query graphs, and (3) to generate samples and
(4) negative samples

We have prepared a script
`gen_test_data.py <../examples/basic/distribute/gen_test_data.py>`__ to
generate a local example graph. Below shows a code example running
distributedly. Each process will read the same data file. Please make
sure all the processes have access to the data file.

.. code:: python

   # test.py
   import sys, os
   import getopt

   import graphlearn as gl
   import numpy as np

   def main(argv):
     cur_path = sys.path[0]

     cluster = ""
     job_name = ""
     task_index = 0

     opts, args = getopt.getopt(argv, 'c:j:t:', ['cluster=', 'job_name=','task_index='])
     for opt, arg in opts:
       if opt in ('-c', '--cluster'):
         cluster = arg
       elif opt in ('-j', '--job_name'):
         job_name = arg
       elif opt in ('-t', '--task_index'):
         task_index = int(arg)
       else:
         pass

     # init distributed graph
     g = gl.Graph() \
          .node(os.path.join(cur_path, "data/user"),
               node_type="user", decoder=gl.Decoder(weighted=True)) \
          .node(os.path.join(cur_path, "data/item"),
                node_type="item",
                decoder=gl.Decoder(attr_types=['string', 'int', 'float', 'float', 'string'])) \
          .edge(os.path.join(cur_path, "data/u-i"),
                edge_type=("user", "item", "buy"), decoder=gl.Decoder(weighted=True), directed=False)
     g.init(cluster=cluster, job_name=job_name, task_index=task_index)
     # g.init() # For local.

     if job_name == "server":
       g.wait_for_close()

     if job_name == "client":
       # Lookup a batch of user nodes with given ids.
       nodes = g.V("item", feed=np.array([100, 102])).emit()
       print(nodes.ids)
       print(nodes.int_attrs)

       # Iterate users and random sample items the user has buy.
       q = g.V("user").batch(4) \
                .outV("buy").sample(2).by("random") \
                .values(lambda x: (x[0].weights, x[1].ids))
       while True:
         try:
           print(g.run(q))
         except gl.OutOfRangeError:
           break

       # Random sample seed buy edge from graph,
       # and sample the users who did not buy the items from the seed edges.
       q = g.E("buy").batch(4).shuffle() \
            .inV() \
            .inNeg("buy").sample(3).by("in_degree") \
            .values(lambda x: x[2].ids)
       print(g.run(q))

       g.close()
   if __name__ == "__main__":
     main(sys.argv[1:])

Once done with the deployment of the data and the codes, we start four
processes (two worker processes and two server processes) locally to
simulate a distributed setting.

.. code:: shell

   HERE=$(cd "$(dirname "$0")";pwd)

   rm -rf $HERE/.tmp/tracker
   mkdir -p $HERE/.tmp/tracker

   if [ ! -d "$HERE/data" ]; then
     mkdir -p $HERE/data
     python $HERE/gen_test_data.py # 你需要把gen_test_data.py、test.py放到shell脚本同一目录下
   fi

   python $HERE/test.py \
     --cluster="{\"client_count\": 2, \"server_count\": 2, \"tracker\": \"$HERE/.tmp/tracker\"}" \
     --job_name="server" --task_index=0 &
   sleep 1
   python $HERE/test.py \
     --cluster="{\"client_count\": 2, \"server_count\": 2, \"tracker\": \"$HERE/.tmp/tracker\"}" \
     --job_name="server" --task_index=1 &
   sleep 1
   python $HERE/test.py \
     --cluster="{\"client_count\": 2, \"server_count\": 2, \"tracker\": \"$HERE/.tmp/tracker\"}" \
     --job_name="client" --task_index=0 &
   sleep 1
   python $HERE/test.py \
     --cluster="{\"client_count\": 2, \"server_count\": 2, \"tracker\": \"$HERE/.tmp/tracker\"}" \
     --job_name="client" --task_index=1

3 Developing a GNN model
========================

Next we will go through how to develop a supervised **GraphSAGE** model
using **GL** and **TensorFlow**, and train it on the Cora dataset.
Please refer to `Developing Your Own Model <algo_en.md>`__ for details.

3.1 Data Preparation
--------------------

We use the open-source dataset Cora as an example. The Cora dataset
contains papers from the machine learning field, and the citation
relationship between these papers. Each paper has 1433 attributes. These
papers are classified into seven categories: Case_Based,
Genetic_Algorithms, Neural_Networks, Probabilistic_Methods,
Reinforcement_Learning, Rule_Learning, Theory. We would like to train a
GNN model that automatically classify the papers into these seven
categories. Next we will preprocess the Cora dataset in order to convert
them into the data format suitable for constructing graphs in **GL**.
The codes for data download and preparation are available at
`cora.py <../examples/data/cora.py>`__.

::

   cd ../examples/data
   python cora.py

The vertex and edge data produced are shown below. Specifically, the
edge data shows the citations pairs; the vertex data contains the papers
and their bag of words, attributes and labels. The attributes are
represented using 1433 floats, and the labels have seven types, taking
values from 0 to 6. Besides vertex ids, each edge also has a weight. The
data format is described through ``gl.Decoder``.

.. code:: shell

   src_id:int64   dst_id:int64
   35  1033
   35  103482
   35  103515

::

   id:int64  label:int32   feature:string
   31336      4    0.0:0.0:...
   1061127    1    0.0:0.05882353:...
   1106406    2    0.0:0.0:...

.. code:: python

   import graphlearn as gl

   N_FEATURE = 1433

   # Describe the vertex data format，with lable and attributes
   node_decoder = gl.Decoder(labeled=True, attr_types=["float"] * N_FEATURE)

   # Describe the edge data format, with weights.
   edge_decoder = gl.Decoder(weighted=True)

3.2 Graph Construction
----------------------

Graph construction is the process of loading the vertex and edge data
into memory. After construction, the graph can be queries and sampled.

.. code:: python

   import graphlearn as gl

   # Configure the parameters
   N_CLASS = 7
   N_FEATURE = 1433
   BATCH_SIZE = 140
   HIDDEN_DIM = 128
   N_HOP =  2
   HOPS = [10, 5]
   N_EPOCHES = 2
   DEPTH = 2

   # Define a graph object
   g = gl.Graph()

   # Add the vertex data into the graph through `.node()` and define the node type ("item") and data format.
   # Add the edge data into the graph through `.edge()` and define its type through a triple (source vertex type, destination vertex type, edge type).
   g.node("examples/data/cora/node_table",
          node_type="item",
          decoder=gl.Decoder(labeled=True, attr_types=["float"] * N_FEATURE)) \
     .edge("examples/data/cora/edge_table",
           edge_type=("item", "item", "relation"), decoder=gl.Decoder(weighted=True), directed=False) \


   # Construct the graph. This is a local example. For the distributed example, see [Graph Object#Initialization](graph_object_cn.md)。
   g.init()

3.3 Graph Sampling
------------------

GraphSAGE requires graph sampling to generate input for the neural
network. Here, we use the following sampling steps: (1) sample a batch
of “item” vertices as the seed; (2) sample the 1-hop and 2-hop neighbors
of these “item” vertices, by following the “relation” edges; (3)
retrieve all the vertex attributes along the path, along with the labels
of the seed vertices.

Here we define a data generator, which generates batches of samples
through graph traversal.

.. code:: python

   def sample_gen():
     query = g.V('item').batch(BATCH_SIZE) \
                  .outV("relation").sample(10).by("random") \
                  .outV("relation").sample(5).by("random") \
                  .values(lambda x: (x[0].float_attrs, x[1].float_attrs, x[2].float_attrs, x[0].labels))
     while True:
       try:
         res = g.run(query)
         if res[0].shape[0] < BATCH_SIZE:
           break
         yield tuple([res[0].reshape(-1, N_FEATURE)]) + tuple([res[1].reshape(-1, N_FEATURE)]) \
               + tuple([res[2].reshape(-1, N_FEATURE)]) + tuple([res[3]])
       except gl.OutOfRangeError:
         break

3.4 GNN Model
-------------

In this part, we use the TensorFlow Estimator as an example to
demonstrate developing a GNN model on **GL**.

-  Generate training data through graph sampling as ``input_fn``

.. code:: python

   import tensorflow as tf
   def sample_input_fn():
     ds = tf.data.Dataset.from_generator(
       sample_gen,
       tuple([tf.float64] * 3) + tuple([tf.int32]),
       tuple([tf.TensorShape([BATCH_SIZE, N_FEATURE])]) + \
       tuple([tf.TensorShape([BATCH_SIZE *  HOPS[0], N_FEATURE])] ) + \
       tuple([tf.TensorShape([BATCH_SIZE * HOPS[0] * HOPS[1], N_FEATURE])]) + \
       tuple([tf.TensorShape([BATCH_SIZE])])
     )
     value = ds.repeat(N_EPOCHES).make_one_shot_iterator().get_next()
     layer_features = value[:3]
     features, labels = encode_fn(layer_features, 0, DEPTH), value[3]
     return {"logits": features}, labels

-  Define the Aggregator and Encoder of the GNN model

.. code:: python

   vars = {}
   def aggregate_fn(self_vecs, neigh_vecs, raw_feat_layer_index, layer_index):
     with tf.variable_scope(str(layer_index) + '_layer', reuse=tf.AUTO_REUSE):
       vars['neigh_weights'] = tf.get_variable(shape=[N_CLASS, N_CLASS], name='neigh_weights')
       vars['self_weights'] = tf.get_variable(shape=[N_CLASS, N_CLASS], name='self_weights')
       output_shape = self_vecs.get_shape()
       dropout = 0.5
       neigh_vecs = tf.nn.dropout(neigh_vecs, 1 - dropout)
       self_vecs = tf.nn.dropout(self_vecs, 1 - dropout)
       neigh_vecs = tf.reshape(neigh_vecs,
                               [-1, HOPS[raw_feat_layer_index], N_CLASS])
       neigh_means = tf.reduce_mean(neigh_vecs, axis=-2)

       from_neighs = tf.matmul(neigh_means, vars['neigh_weights'])
       from_self = tf.matmul(self_vecs, vars["self_weights"])

       output = tf.add_n([from_self, from_neighs])
       output = tf.reshape(output, shape=[-1, output_shape[-1]])
       return tf.nn.leaky_relu(output)


   def encode_fn(layer_features, raw_feat_layer_index, depth_to_encode):
     if depth_to_encode > 0:
       h_self_vec = encode_fn(layer_features, raw_feat_layer_index, depth_to_encode - 1)
       h_neighbor_vecs = encode_fn(layer_features, raw_feat_layer_index + 1, depth_to_encode - 1)
       return aggregate_fn(h_self_vec, h_neighbor_vecs, raw_feat_layer_index, depth_to_encode)
     else:
       h_self_vec = tf.cast(layer_features[raw_feat_layer_index], tf.float32)
       h_self_vec = tf.layers.dense(h_self_vec, N_CLASS, activation=tf.nn.leaky_relu)
     return h_self_vec

-  Define features_column

.. code:: python

   features, labels = sample_input_fn()
   feature_columns = []
   for key in features.keys():
     feature_columns.append(tf.feature_column.numeric_column(key=key))

-  Define Loss and Model

.. code:: python

   def loss_fn(logits, labels):
       return tf.reduce_mean(
           tf.nn.sparse_softmax_cross_entropy_with_logits(labels=labels, logits=logits))

   def model_fn(features, labels, mode, params):
       logits = features['logits']
       loss = loss_fn(logits, labels)
       optimizer = tf.train.AdamOptimizer(learning_rate=params["learning_rate"])
       train_op = optimizer.minimize(
           loss=loss, global_step=tf.train.get_global_step())

       spec = tf.estimator.EstimatorSpec(
           mode=tf.estimator.ModeKeys.TRAIN,
           loss=loss,
           train_op=train_op)
       return spec

-  Instantiate Estimator and start training

.. code:: python

   params = {"learning_rate": 1e-4,
             'feature_columns': feature_columns}

   model = tf.estimator.Estimator(model_fn=model_fn,
                                  params=params)
   model.train(input_fn=sample_input_fn)
