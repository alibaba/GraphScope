# Deploy GraphScope on Local

This guide will walk you through the process of installing and using GraphScope on your local machine.

## Prerequisites
- Python 3.7 - 3.11
- Ubuntu 20.04 or later
- CentOS 7 or later
- OpenJDK 8 or later (If you want to use GIE)
- macOS 11.2.1 (Big Sur) or later, with both Intel chip and Apple M1 chip

## Installation
GraphScope is distributed by [Python wheels](https://pypi.org/project/graphscope) , and could be installed by [pip](https://pip.pypa.io/en/stable/) directly.

- Use pip to install latest stable graphscope package

```bash
python3 -m pip install graphscope --upgrade
```

- Use Aliyun mirror to accelerate downloading if you are in CN

```bash
python3 -m pip install graphscope -i http://mirrors.aliyun.com/pypi/simple/ --trusted-host=mirrors.aliyun.com  --upgrade
```



The above command will download all the components required for running GraphScope in standalone mode to your local machine.

Alternatively, you can use a  Docker development environment to get started

```bash
docker run --name dev -it --shm-size=4096m ubuntu:latest

# inside the docker
apt-get update -y && apt-get install python3-pip default-jdk -y
python3 -m pip install graphscope ipython tensorflow
```

## Launch graphscope

### Load graphs
Loading a graph is almost always the first step in using GraphScope.

For a quick start, you can use dataset API to load built-in datasets.

```python
import graphscope
from graphscope.dataset import load_ogbn_mag
graphscope.set_option(show_log=True)
graph = load_ogbn_mag()
```

### Perform various graph tasks
#### Running algorithm using graph analytical engine (GAE)

```python
# Run a lpa algorithms over the graph, and retrieve its result
ret = graphscope.lpa(graph)
df = ret.to_dataframe(selector={'id': 'v.id', 'label': 'r'})
print(df.head())
```

### Perform Gremlin queries using graph interactive engine (GIE)
```python
interactive = graphscope.gremlin(graph)
# check the total number of nodes and edges
node_num = interactive.execute("g.V().count()").one()
edge_num = interactive.execute("g.E().count()").one()
print(f'The graph has {node_num[0]} nodes and {edge_num[0]} edges')
```

### Starting a sampling task using graph learning engine (GLE)


```python
# Define training process
def train_gcn(
    graph,
    node_type,
    edge_type,
    class_num,
    features_num,
    hops_num=2,
    nbrs_num=[25, 10],
    epochs=2,
    train_batch_size=128,
    test_batch_size=128,
    hidden_dim=256,
    in_drop_rate=0.5,
    learning_rate=0.01,
):
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

    graphscope.learning.reset_default_tf_graph()
    dimensions = [features_num] + [hidden_dim] * (hops_num - 1) + [class_num]
    model = EgoGraphSAGE(dimensions, act_func=tf.nn.relu, dropout=in_drop_rate)

    # prepare train dataset
    train_data = EgoSAGESupervisedDataLoader(
        graph,
        graphscope.learning.Mask.TRAIN,
        "random",
        train_batch_size,
        node_type=node_type,
        edge_type=edge_type,
        nbrs_num=nbrs_num,
        hops_num=hops_num,
    )
    train_embedding = model.forward(train_data.src_ego)
    train_labels = train_data.src_ego.src.labels
    loss = tf.reduce_mean(
        tf.nn.sparse_softmax_cross_entropy_with_logits(
            labels=train_labels,
            logits=train_embedding,
        )
    )
    optimizer = tf.train.AdamOptimizer(learning_rate=learning_rate)
    # prepare test dataset
    test_data = EgoSAGESupervisedDataLoader(
        graph,
        graphscope.learning.Mask.TEST,
        "random",
        test_batch_size,
        node_type=node_type,
        edge_type=edge_type,
        nbrs_num=nbrs_num,
        hops_num=hops_num,
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

# GLE on ogbn_mag_small graph
# Define the features for learning
paper_features = [f"feat_{i}" for i in range(128)]
lg = graphscope.graphlearn(
    graph,
    nodes=[("paper", paper_features)],
    edges=[("paper", "cites", "paper")],
    gen_labels=[
        ("train", "paper", 100, (0, 75)),
        ("val", "paper", 100, (75, 85)),
        ("test", "paper", 100, (85, 100)),
    ],
)

# hyperparameters config.
train_gcn(
    lg,
    node_type="paper",
    edge_type="cites",
    class_num=349,  # output dimension
    features_num=128,  # input dimension, number of features
)
```

-----

Great job on completing the tutorial! You've taken the first step towards mastering our product. Now that you know the basics, it's time to start exploring on your own. 
Try experimenting with different engines and see what you can create. 

### Notes
- When you explore around using GAE, it may require on-the-fly algorithm compilation, which requires installation of cmake and g++ (if not available). e.g. to install build toolchains on ubuntu:

    ```bash
    apt update -y &&
    apt install cmake build-essential -y
    ```

- If you wish to experience the latest features, you can install the preview version of the build, which is built every day.
    ```bash
    python3 -m pip install graphscope --pre
    ```