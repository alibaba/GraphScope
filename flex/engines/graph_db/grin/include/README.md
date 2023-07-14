# GRIN
GRIN is a proposed standard graph retrieval interface in GraphScope. The goal of GRIN is to provide a common way for the graph computing engines to retrieve graph data stored in different storage engines in GraphScope, and to simplify the integration of these engines with each other.

GRIN is defined in C, which makes it portable to systems written in different programming languages, such as C++, Rust and Java. It provides a set of common operations and data structure handles that can be used to access graph data, regardless of the underlying storage engine. 

These operations include:
* Traversal: navigating the graph structure to explore relationships between vertices
* Retrieval: retrieving the data and properties of vertices and edges
* Filter: filtering data structures with partitioning or property conditions

GRIN is designed to be read-only, meaning that it does not provide operations for modifying the graph data. This decision was made to simplify the implementation of GRIN and ensure that it can be used safely with any storage engine.
