# Tutorial: Training a Node Classification Model on Your Local Machine

This tutorial presents an end-to-end example that illustrates how GraphScope 
trains the EgoGraphSAGE model for a node classification task. To demonstrate 
this, we utilize the ogbn-mag dataset, which is a heterogeneous academic citation 
network that constitutes a subset of the larger Microsoft Academic Graph. This 
dataset encompasses four types of entities, including papers, authors, 
institutions, and fields of study. Additionally, it comprises four types of directed relations.

Regarding the heterogeneous ogbn-mag data, the GNN task aims to predict the 
class of a paper. To accomplish this, we employ both attribute and structural 
information to classify papers. Specifically, in the graph, each paper node 
features a 128-dimensional word2vec vector that represents its content. This 
vector is obtained by averaging the embeddings of words in the paper's title 
and abstract. It's worth noting that the embeddings of individual words are pre-trained.

## Load Graph
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

# Enable logging
gs.set_option(show_log=True)

# load the obgn-mag graph as example.
g = load_ogbn_mag()

# print the schema of the graph
print(graph)
```

## Define the Training Process for the EgoGraphSAGE Model

```python
def train(graph, node_type, edge_type, class_num, features_num,
              hops_num=2, nbrs_num=[25, 10], epochs=2,
              hidden_dim=256, in_drop_rate=0.5, learning_rate=0.01
):
    gs.learning.reset_default_tf_graph()

    dimensions = [features_num] + [hidden_dim] * (hops_num - 1) + [class_num]
    model = EgoGraphSAGE(dimensions, act_func=tf.nn.relu, dropout=in_drop_rate)

    # prepare the training dataset
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

    # prepare the test dataset
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
```

## Launch the Learning Engine
```python
# define the features for learning, we chose the original 128-dimension feature
i_features = []
for i in range(128):
    i_features.append("feat_" + str(i))

# launch a learning engine, here we split the dataset, 75% as train, 10% as validation and 15% as test.
lg = sess.graphlearn(
    graph,
    nodes=[("paper", i_features)],
    edges=[("paper", "cites", "paper")],
    gen_labels=[
        ("train", "paper", 100, (0, 75)),
        ("val", "paper", 100, (75, 85)),
        ("test", "paper", 100, (85, 100)),
    ],
)
```

## Train the Model
```python
train(lg, node_type="paper", edge_type="cites",
          class_num=349,  # output dimension
          features_num=128,  # input dimension
)
```