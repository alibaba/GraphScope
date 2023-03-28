# Guide and Examples

```{toctree} arguments
---
caption: TOC
maxdepth: 1
hidden:
---
tutorial_run_builtin_algo
tutorial_networkx_operations
tutorial_networkx_algorithms
tutorial_dev_algo_cpp_pie
tutorial_dev_algo_cpp_flash
tutorial_dev_algo_python
tutorial_dev_algo_java
tutorial_run_giraph_apps
tutorial_run_graphx_apps
```


This section contains a guide to the analytical engine and a number of examples.

```{tip}
We assume you has read the [getting_started](getting_started.md) section and know how to launch a GraphScope session.
```

The analytical engine of GraphScope can handles various scenarios, .... In most cases, the built-in algorithms are sufficient for your needs.

````{panels}
:header: text-center
:column: col-lg-12 p-2

```{link-button} tutorial_run_builtin_algo.html
:text: Tutorial
:classes: btn-block stretched-link
```
^^^^^^^^^^^^^^
Using Louvain to detect communities in a social graph.
````

If you are a scientist familiar with NetworkX, you may find GraphScope works well for your known APIs to manipulate graph, to invoke an analysis algorithm, and it works well with other modules in PyData eco-system.

````{panels}
:header: text-center
:column: col-lg-12 p-2

```{link-button} tutorial_networkx_operations.html
:text: Tutorial
:classes: btn-block stretched-link
```
^^^^^^^^^^^^^^
Using NetworkX to manipulate graph and run algorithms.
----

```{link-button} tutorial_networkx_algorithms.html
:text: Tutorial
:classes: btn-block stretched-link
```
^^^^^^^^^^^^^^
Running NetworkX Algorithms on GraphScope
````

If the built-in algorithms are not sufficient for your needs, you can implement your own algorithms in PIE or FLASH model, in Java、C++ or Python, and run them on GraphScope.

````{panels}
:header: text-center
:column: col-lg-12 p-2

```{link-button} tutorial_dev_algo_cpp_pie.html
:text: Tutorial
:classes: btn-block stretched-link
```
^^^^^^^^^^^^^^
Write and run customized PageRank in C++ with PIE model
----

```{link-button} tutorial_dev_algo_cpp_flash.html
:text: Tutorial
:classes: btn-block stretched-link
```
^^^^^^^^^^^^^^
Write and run SSSP in C++ with FLASH model
----

```{link-button} tutorial_dev_algo_python.html
:text: Tutorial
:classes: btn-block stretched-link
```
^^^^^^^^^^^^^^
Write and run algorithms in Python
----

```{link-button} tutorial_dev_algo_java.html
:text: Tutorial
:classes: btn-block stretched-link
```
^^^^^^^^^^^^^^
Write and run algorithms in Java with PIE and Pregel model
````

Better still, if you already have your application running on Giraph or GraphX, the packaged `jar` can directly run on GraphScope. The migration is totally transparent, you even don't need to have the sourcecode!

````{panels}
:header: text-center
:column: col-lg-12 p-2

```{link-button} tutorial_run_giraph_apps.html
:text: Tutorial
:classes: btn-block stretched-link
```
^^^^^^^^^^^^^^
Develop and run (existing) Giraph applications on GraphScope

----

```{link-button} tutorial_run_graphx_apps.html
:text: Tutorial
:classes: btn-block stretched-link
```
^^^^^^^^^^^^^^
Develop and run (existing) GraphX applications on GraphScope
````
