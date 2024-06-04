# Persistent storage of graphs on the Kubernetes cluster

If you want to persistently store specific graphs that have been calculated over a long period of time on the Kubernetes cluster and restore them later, this document provides step-by-step instructions on how to do this with the Kubernetes PersistentVolumes.

## Prerequisites
- You have a Kubernetes cluster on hand. If you don't have a Kubernetes cluster, please refer to [Prepare a Kubernetes cluster](./deploy_graphscope_on_self_managed_k8s.md#prepare-a-kubernetes-cluster) for details.

- You have the `graphscope` Python library installed. If you don't have installed it, please refer to [Install GraphScope Client](./deploy_graphscope_on_self_managed_k8s.md#install-graphscope-client) for details.

## Create a pv and pvc

```bash
$ kubectl create namespace graphscope-system
```

Then create the pv as follows, the pv will be mounted to `/var/vineyard/dump` in the Kubernetes node. You can change the path to any other path you want.

```bash
$ cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolume
metadata:
  name: graphscope-pv
  labels:
    app.kubernetes.io/name: test-pv
spec:
  capacity:
    storage: 1Gi
  accessModes:
    - ReadWriteOnce
  hostPath:
    path: /var/vineyard/dump
  storageClassName: manual
EOF
```

Create pvc as follows. Most importantly, the pvc can't be deleted, otherwise the data will be lost.

```bash
$ cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: graphscope-pvc
  namespace: graphscope-system
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: test-pv
  resources:
    requests:
      storage: 1Gi
  accessModes:
    - ReadWriteOnce
  storageClassName: manual
EOF
```

## Store graphs to the pvc

After the above preparations are completed, you can deploy the graphscope cluster as follows:

```python
import graphscope
import os
import vineyard
from graphscope.dataset import load_modern_graph

# export the gs_test_dir to the environment variable
k8s_volumes = {
    "data": {
        "type": "hostPath",
        "field": {"path": os.environ["GS_TEST_DIR"], "type": "Directory"},
        "mounts": {"mountPath": "/testingdata"},
    }
}

# create a graphscope session with the external 
# vineyard deployment.
#  
# Notice, the num_workers should not be greater than the
# number of nodes in the kubernetes cluster.  
sess = graphscope.session(
    num_workers=1,
    k8s_image_registry="docker.io",
    k8s_image_tag="ccc",
    k8s_namespace="graphscope-system",
    k8s_vineyard_deployment="vineyardd-sample",
    k8s_volumes=k8s_volumes,
)

# load modern graph 
graph = load_modern_graph(sess, "/testingdata/modern_graph")

# create the gie instance
interactive = sess.gremlin(graph)

# get the subgraph
sub_graph = interactive.subgraph(
    'g.V().hasLabel("person").outE("knows")'
)

# project the projected graph to simple graph.
simple_g = sub_graph.project(vertices={"person": []}, edges={"knows": []})

pr_result = graphscope.pagerank(simple_g, delta=0.8)
tc_result = graphscope.triangles(simple_g)

# add the PageRank and triangle-counting results as new columns to the property graph
sub_graph.add_column(pr_result, {"Ranking": "r"})
sub_graph.add_column(tc_result, {"TC": "r"})

# print the simple graph and subgraph's vineyard_id
# REMEMBER the several vineyard ids, you need to use them to restore the graphs next time.
print(simple_g.vineyard_id)
# REMEMBER THIS: 997255889378630
print(sub_graph.vineyard_id)
# REMEMBER THIS: 997163552113975

# store the simple graph and subgraph to the pvc
# use the previous path of the pv and the pvc name here
sess.store_graphs_to_pvc(
    graphIDs=[vineyard.ObjectID(simple_g.vineyard_id), vineyard.ObjectID(sub_graph.vineyard_id)],
    path="/var/vineyard/dump",
    pvc_name="graphscope-pvc",
)

# check the simple graph's schema
print(simple_g.schema)
# oid_type: LONG
# vid_type: ULONG
# type: VERTEX
# Label: person
# Properties: 
#
# type: EDGE
# Label: knows
# Properties: 
# Relations: [Relation(source='person', destination='person')]

# check the subgraph's schema
print(sub_graph.schema)

# oid_type: LONG
# vid_type: ULONG
# type: VERTEX
# Label: person
# Properties: Property(0, name, STRING), Property(1, age, INT), Property(2, id, LONG)
#
# type: VERTEX
# Label: software
# Properties: Property(0, name, STRING), Property(1, lang, STRING), Property(2, id, LONG)
#
# type: EDGE
# Label: created
# Properties: Property(0, eid, LONG), Property(1, weight, DOUBLE)
# Relations: [Relation(source='person', destination='software')]
# type: EDGE
# Label: knows
# Properties: Property(0, eid, LONG), Property(1, weight, DOUBLE)
# Relations: [Relation(source='person', destination='person')]

# close the session
sess.close()
```

## Retore graphs from the pvc

Remember the vineyard ids printed above and the pvc name and then you can restore the graphs from the pvc as follows.

```python
import graphscope
import os
import vineyard

# create a graphscope session with the external 
# vineyard deployment.
#  
# Notice, the num_workers should not be greater than the
# number of nodes in the kubernetes cluster.  
sess = graphscope.session(
    num_workers=1,
    k8s_image_registry="docker.io",
    k8s_image_tag="ccc",
    k8s_namespace="graphscope-system",
    k8s_vineyard_deployment="vineyardd-sample",
)

# load graphs from the pvc
sess.restore_graphs_from_pvc(
    path="/var/vineyard/dump",
    pvc_name="graphscope-pvc"
)

# get the simple graph and subgraph
simple_g = sess.g(vineyard.ObjectID(997255889378630))
sub_graph = sess.g(vineyard.ObjectID(997163552113975))

# check the graphs' schema
print(simple_g.schema)
# oid_type: LONG
# vid_type: ULONG
# type: VERTEX
# Label: person
# Properties: 
#
# type: EDGE
# Label: knows
# Properties: 
# Relations: [Relation(source='person', destination='person')]

print(sub_graph.schema)
# oid_type: LONG
# vid_type: ULONG
# type: VERTEX
# Label: person
# Properties: Property(0, name, STRING), Property(1, age, INT), Property(2, id, LONG)
#
# type: VERTEX
# Label: software
# Properties: Property(0, name, STRING), Property(1, lang, STRING), Property(2, id, LONG)
#
# type: EDGE
# Label: created
# Properties: Property(0, eid, LONG), Property(1, weight, DOUBLE)
# Relations: [Relation(source='person', destination='software')]
# type: EDGE
# Label: knows
# Properties: Property(0, eid, LONG), Property(1, weight, DOUBLE)
# Relations: [Relation(source='person', destination='person')]
```

## Clean up

If you don't need the graphs anymore, you can delete the pvc and pv as follows.

```bash
$ kubectl delete pvc graphscope-pvc -n graphscope-system
$ kubectl delete pv graphscope-pv
```
