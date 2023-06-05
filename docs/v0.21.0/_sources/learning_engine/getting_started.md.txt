# Getting Started

This guide gives you a quick start to use GraphScope for graph learning tasks on your local machine.

## Installation

We’ll start by installing GraphScope with a single-line command.

```bash
python3 -m pip install graphscope --upgrade
```

If you occur a very low downloading speed, try to use a mirror site for the pip.

```bash
python3 -m pip install graphscope --upgrade \
    -i http://mirrors.aliyun.com/pypi/simple/ --trusted-host=mirrors.aliyun.com
```

By default, GraphScope Learning Engine uses `TensorFlow` as its NN backend,
you also need to install the tensorflow.

```bash
# Installing the latest version of tensorflow may cause dependency
# conflicts with GraphScope, we use v2.8.0 here.
python3 -m pip install tensorflow==2.8.0
```

## Running GraphScope Learning Engine on Local

The `graphscope` package includes everything you need to train GNN models 
on your local machine. Now you may import it in a Python session and start your job.
Use the following example to training train an EgoGraphSAGE model to classify 
the nodes (papers) into 349 categories, each of which represents a venue 
(e.g. pre-print and conference).

```python
try:
    # https://www.tensorflow.org/guide/migrate
    import tensorflow.compat.v1 as tf
    tf.disable_v2_behavior()
except ImportError:
    import tensorflow as tf

import graphscope as gs
from graphscope.dataset import load_ogbn_mag
from graphscope.learning.examples import EgoGraphSAGE
from graphscope.learning.examples import EgoSAGESupervisedDataLoader
from graphscope.learning.examples.tf.trainer import LocalTrainer

gs.set_option(show_log=True)

# Define the training process of EgoGraphSAGE
def train(graph, node_type, edge_type, class_num, features_num,
              hops_num=2, nbrs_num=[25, 10], epochs=2,
              hidden_dim=256, in_drop_rate=0.5, learning_rate=0.01
):
    gs.learning.reset_default_tf_graph()

    dimensions = [features_num] + [hidden_dim] * (hops_num - 1) + [class_num]
    model = EgoGraphSAGE(dimensions, act_func=tf.nn.relu, dropout=in_drop_rate)

    # prepare training dataset
    train_data = EgoSAGESupervisedDataLoader(
        graph, gs.learning.Mask.TRAIN,
        node_type=node_type, edge_type=edge_type,
        nbrs_num=nbrs_num, hops_num=hops_num,
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
        graph, gs.learning.Mask.TEST,
        node_type=node_type, edge_type=edge_type,
        nbrs_num=nbrs_num, hops_num=hops_num,
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

# load the obgn-mag graph as example.
g = load_ogbn_mag()

# define the features for learning.
paper_features = [f"feat_{i}" for i in range(128)]

# launch a learning engine.
lg = gs.graphlearn(
    g,
    nodes=[("paper", paper_features)],
    edges=[("paper", "cites", "paper")],
    gen_labels=[
        ("train", "paper", 100, (0, 75)),
        ("val", "paper", 100, (75, 85)),
        ("test", "paper", 100, (85, 100))
    ]
)

train(lg, node_type="paper", edge_type="cites",
          class_num=349,  # output dimension
          features_num=128,  # input dimension
)
```

## What's the Next

As shown in the above example, it is very easy to use GraphScope to train your 
GNN model on your local machine. Next, you may want to learn more about the following topics:

Next, you may want to learn more about the following topics:

- [Design of the learning engine of GraphScope and its technical details.](learning_engine/design_of_gle)
- [A set of examples with advanced usage, including deploying GLE in a K8s cluster.](learning_engine/guide_and_exmaples)