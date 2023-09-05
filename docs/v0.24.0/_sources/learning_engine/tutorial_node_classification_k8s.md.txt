# Tutorial: Training a Node Classification Model on K8s Cluster

GraphScope is designed for processing large graphs, which are usually hard to 
fit in the memory of a single machine. With vineyard as the distributed in-memory 
data manager, GraphScope supports run on a cluster managed by Kubernetes(k8s).

In this tutorial, we revisit the example we present in the first tutorial, 
showing how GraphScope process the node classification task on a Kubernetes cluster.

Please note, since this tutorial is designed to run on a k8s cluster, you need 
to configure your k8s environment before running the example.

## Create a session on kubernetes and load graph
```python
import graphscope
from graphscope.dataset import load_ogbn_mag

# enable logging
graphscope.set_option(show_log=True)  

# init the GraphScope session
sess = graphscope.session(with_dataset=True, k8s_service_type='LoadBalancer', k8s_image_pull_policy='Always')
```
Behind the scenes, the session tries to launch a coordinator, which is the entry for 
the back-end engines. The coordinator manages a cluster of k8s pods (2 pods by default), 
and learning engines ran on them. For each pod in the cluster, there is a vineyard 
instance at service for distributed data in memory.

The log GraphScope coordinator service connected means the session launches 
successfully, and the current Python client has connected to the session.

You can also check a session's status by this.
```python
# check the status of the session
sess
```
Run this cell, you may find a "status" field with value "active". Together with 
the status, it also prints other metainfo of this session, i.e., such as the 
number of workers (pods), the coordinator endpoint for connection, and so on.



```python
# load the obgn_mag dataset in "sess" as a graph
graph = load_ogbn_mag(sess, "/dataset/ogbn_mag_small/")

# print the schema of the graph
print(graph)
```

## Graph neural networks (GNNs)
```python
# define the features for learning, we chose the original 128-dimension feature
i_features = []
for i in range(128):
    i_features.append("feat_" + str(i))

# launch a learning engine. here we split the dataset, 75% as train, 10% as validation and 15% as test.
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

# Then we define the training process using the example EgoGraphSAGE model with tensorflow.
try:
    # https://www.tensorflow.org/guide/migrate
    import tensorflow.compat.v1 as tf
    tf.disable_v2_behavior()
except ImportError:
    import tensorflow as tf

import argparse
import graphscope.learning.graphlearn.python.nn.tf as tfg
from graphscope.learning.examples import EgoGraphSAGE
from graphscope.learning.examples import EgoSAGEUnsupervisedDataLoader
from graphscope.learning.examples.tf.trainer import LocalTrainer

def parse_args():
  argparser = argparse.ArgumentParser("Train EgoSAGE Unsupervised.")
  argparser.add_argument('--batch_size', type=int, default=128)
  argparser.add_argument('--features_num', type=int, default=128)
  argparser.add_argument('--hidden_dim', type=int, default=128)
  argparser.add_argument('--output_dim', type=int, default=128)
  argparser.add_argument('--nbrs_num', type=list, default=[5, 5])
  argparser.add_argument('--neg_num', type=int, default=5)
  argparser.add_argument('--learning_rate', type=float, default=0.0001)
  argparser.add_argument('--epochs', type=int, default=1)
  argparser.add_argument('--agg_type', type=str, default="mean")
  argparser.add_argument('--drop_out', type=float, default=0.0)
  argparser.add_argument('--sampler', type=str, default='random')
  argparser.add_argument('--neg_sampler', type=str, default='in_degree')
  argparser.add_argument('--temperature', type=float, default=0.07)
  return argparser.parse_args()

args = parse_args()

# define model
dims = [args.features_num] + [args.hidden_dim] * (len(args.nbrs_num) - 1) + [args.output_dim]
model = EgoGraphSAGE(dims, agg_type=args.agg_type, dropout=args.drop_out)

# prepare the training dataset
train_data = EgoSAGEUnsupervisedDataLoader(lg, None, sampler=args.sampler, 
                                           neg_sampler=args.neg_sampler, batch_size=args.batch_size,
                                           node_type='paper', edge_type='cites', nbrs_num=args.nbrs_num)
src_emb = model.forward(train_data.src_ego)
dst_emb = model.forward(train_data.dst_ego)
neg_dst_emb = model.forward(train_data.neg_dst_ego)
loss = tfg.unsupervised_softmax_cross_entropy_loss(
    src_emb, dst_emb, neg_dst_emb, temperature=args.temperature)
optimizer = tf.train.AdamOptimizer(learning_rate=args.learning_rate)

# Start training
trainer = LocalTrainer()
trainer.train(train_data.iterator, loss, optimizer, epochs=args.epochs)
```

Finally, a session manages the resources in the cluster, thus it is important to 
release these resources when they are no longer required. To de-allocate the 
resources, use the method close on the session when all the graph tasks are finished.

```python
# close the session
sess.close()
```