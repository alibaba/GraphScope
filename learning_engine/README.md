# GraphScope Learning Engine: Graph-Learn

[![Translation](https://shields.io/badge/README-%E4%B8%AD%E6%96%87-blue)](README-zh.md)

The learning engine in GraphScope is GraphLearn with some adaptions and wrappers.

[Graph-Learn(GL)](https://github.com/alibaba/graph-learn/) is a distributed framework designed for the development and application of large-scale graph neural networks. It refines and abstracts a set of programming paradigms suitable for the current neural network model. It has been successfully applied to many scenarios such as search recommendation, network security, and knowledge graphs within Alibaba.

To support the diversity and rapid development of GNN in industrial scenarios, GL focuses on portability and scalability, which is more friendly to developers. Developers can use GL to implement a GNNs algorithms, or customize a graph operator, such as graph sampling. The interfaces of GL are provided in the form of Python and NumPy. It is compatible with TensorFlow or PyTorch. Currently, GL has some build-in classic models developed with TensorFlow for the user reference. GL can run in Docker or on a physical machine, and supports both stand-alone and distributed deployment modes.