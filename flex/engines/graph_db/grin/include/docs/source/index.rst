.. GRIN documentation master file, created by
   sphinx-quickstart on Thu May 11 17:08:47 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to GRIN's documentation!
================================

**GRIN** is a proposed standard graph retrieval interface in GraphScope.
The goal of GRIN is to provide a common way for the graph computing engines to 
retrieve graph data stored in different storage engines in GraphScope, 
and to simplify the integration of these engines with each other.

GRIN is defined in C, which makes it portable to systems written in different 
programming languages, such as C++, Rust and Java. 
It provides a set of common operations and data structure handles that can 
be used to access graph data, regardless of the underlying storage engine. 

These operations include:

- *Traversal*: navigating the graph structure to explore relationships between vertices
- *Retrieval*: retrieving the data and properties of vertices and edges
- *Filter*: filtering data structures with partitioning or property conditions

GRIN is designed to be read-only, meaning that it does not provide operations for
modifying the graph data. This decision was made to simplify the implementation 
of GRIN and ensure that it can be used safely with any storage engine.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   0.get_started
   1.return_value
   2.api_naming
   3.topology_api
   4.partition_api
   5.property_api
   6.index_api
   7.extension



