# Getting Started

This tutorial will give you a quick tour of GraphScope's features. 
To get started, we’ll start by installing GraphScope. Most of the examples in this guide are based on Python on your local machine, 
but we will also show you how to deploy and run GraphScope on a k8s cluster.

```bash
pip3 install graphscope
```

## One-stop Graph Processing

this a 

````{dropdown} Import GraphScope and load a graph

```python
import graphscope
from graphscope.dataset import load_ogbn_mag

g = load_ogbn_mag()
```
````


````{dropdown} Run interactive queries with Gremlin

```python
# get the endpoint for submitting Gremlin queries on graph g.
interactive = graphscope.gremlin(g)

# count the number of papers two authors (with id 2 and 4307) have co-authored
papers = interactive.execute("g.V().has('author', 'id', 2).out('writes').where(__.in('writes').has('id', 4307)).count()").one()
```
````


````{dropdown} Run analytical algorithms over graph

```python
# extract a subgraph of publication within a time range
sub_graph = interactive.subgraph("g.V().has('year', gte(2014).and(lte(2020))).outE('cites')")

# project the projected graph to simple graph.
simple_g = sub_graph.project(vertices={"paper": []}, edges={"cites": []})

ret1 = graphscope.k_core(simple_g, k=5)
ret2 = graphscope.triangles(simple_g)

# add the results as new columns to the citation graph
sub_graph = sub_graph.add_column(ret1, {"kcore": "r"})
sub_graph = sub_graph.add_column(ret2, {"tc": "r"})
```
````


````{dropdown} Prepare data and engine for learning

```python
# define the features for learning
paper_features = []
for i in range(128):
    paper_features.append("feat_" + str(i))

paper_features.append("kcore")
paper_features.append("tc")

# launch a learning engine.
lg = graphscope.graphlearn(sub_graph, nodes=[("paper", paper_features)],
                  edges=[("paper", "cites", "paper")],
                  gen_labels=[
                      ("train", "paper", 100, (0, 75)),
                      ("val", "paper", 100, (75, 85)),
                      ("test", "paper", 100, (85, 100))
                  ])
```
````


````{dropdown} Define the training process and run it

```python
try:
    # https://www.tensorflow.org/guide/migrate
    import tensorflow.compat.v1 as tf
    tf.disable_v2_behavior()
except ImportError:
    import tensorflow as tf

import graphscope.learning
from graphscope.learning.examples import EgoGraphSAGE
from graphscope.learning.examples import EgoSAGESupervisedDataLoader
from graphscope.learning.examples.tf.trainer import LocalTrainer

# supervised GCN.
def train_gcn(graph, node_type, edge_type, class_num, features_num,
              hops_num=2, nbrs_num=[25, 10], epochs=2,
              hidden_dim=256, in_drop_rate=0.5, learning_rate=0.01,
):
    graphscope.learning.reset_default_tf_graph()

    dimensions = [features_num] + [hidden_dim] * (hops_num - 1) + [class_num]
    model = EgoGraphSAGE(dimensions, act_func=tf.nn.relu, dropout=in_drop_rate)

    # prepare train dataset
    train_data = EgoSAGESupervisedDataLoader(
        graph, graphscope.learning.Mask.TRAIN,
        node_type=node_type, edge_type=edge_type, nbrs_num=nbrs_num, hops_num=hops_num,
    )
    train_embedding = model.forward(train_data.src_ego)
    train_labels = train_data.src_ego.src.labels
    loss = tf.reduce_mean(
        tf.nn.sparse_softmax_cross_entropy_with_logits(
            labels=train_labels, logits=train_embedding,
        )
    )
    optimizer = tf.train.AdamOptimizer(learning_rate=learning_rate)

    # prepare test dataset
    test_data = EgoSAGESupervisedDataLoader(
        graph, graphscope.learning.Mask.TEST,
        node_type=node_type, edge_type=edge_type, nbrs_num=nbrs_num, hops_num=hops_num,
    )
    test_embedding = model.forward(test_data.src_ego)
    test_labels = test_data.src_ego.src.labels
    test_indices = tf.math.argmax(test_embedding, 1, output_type=tf.int32)
    test_acc = tf.div(
        tf.reduce_sum(tf.cast(tf.math.equal(test_indices, test_labels), tf.float32)),
        tf.cast(tf.shape(test_labels)[0], tf.float32),
    )

    # train and test
    trainer = LocalTrainer()
    trainer.train(train_data.iterator, loss, optimizer, epochs=epochs)
    trainer.test(test_data.iterator, test_acc)

train_gcn(lg, node_type="paper", edge_type="cites",
          class_num=349,  # output dimension
          features_num=130,  # input dimension, 128 + kcore + triangle count
)
```
````


## Graph Analytics Quick Start

TBF
