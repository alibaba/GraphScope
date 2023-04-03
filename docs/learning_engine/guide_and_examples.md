# Guide and Examples

```{toctree} arguments
---
caption: TOC
maxdepth: 1
hidden:
---
tutorial_node_classification_local
tutorial_node_classification_k8s
```


This section contains a guide for the learning engine and a number of examples.

```{tip}
We assume you has read the [getting_started](getting_started.md) section and know how to launch a GraphScope session.
```

We present an end-to-end example, demonstrating how GLE trains a node classification model on a citation network using the local mode of GraphScope.

````{panels}
:header: text-center
:column: col-lg-12 p-2

```{link-button} tutorial_node_classification_local.html
:text: Tutorial
:classes: btn-block stretched-link
```
^^^^^^^^^^^^^^
Training a Node Classification Model on Your Local Machine.
````

GraphScope is designed for processing large graphs, which are usually hard to fit in the memory of a single machine. With vineyard as the distributed in-memory data manager, GraphScope supports run on a cluster managed by Kubernetes(k8s). Next, we revisit the example we present in the first tutorial, showing how GraphScope process the node classification task on a Kubernetes cluster.


````{panels}
:header: text-center
:column: col-lg-12 p-2

```{link-button} tutorial_node_classification_k8s.html
:text: Tutorial
:classes: btn-block stretched-link
```
^^^^^^^^^^^^^^
Training a Node Classification Model on K8s Cluster
````
