# Builtin Graph Datasets

GraphScope provides a robust graph loading module to load a graph from a set of files. 

It also ships with a dataset download module, with pre-configured schemas for known datasets as sample data. 

## Basic usage

TBF


## Avaialbe datasets

Currently, the supported graph datasets are listed as below. 

- [ogb-mag](https://ogb.stanford.edu/docs/nodeprop/#ogbn-mag)
- [TBF](#)

The list is growing. Hopefully, it would support all datasets from [ogb](https://ogb.stanford.edu) and [snap](https://snap.stanford.edu/data/index.html). 

## Contributing

You are welcomed to contribute a dataset to GraphScope by submitting a Pull Request. You may following these to add a dataset.

- Find a popular and appropriate graph data. Reformat and organize it as vertex/edge files. If it is a property graph, make one file for each label of vertices/edges. (see [example](#))
- Put all files into a folder and name it as the graph name.  (see [example](#))
- Compress the folder, then upload the compressed file together with the original folder to the dataset folder of the OSS bucket. e.g., assume you have a folder named `foo`, and two files `foo/nodes.csv` and `foo/edge.csv`, the files in the OSS bucket should be organized as,  (see [example](#))
```bash
dataset
|-- foo.tar.gz
|-- foo
    |-- nodes.csv
    |-- edge.csv
```
- Write the loading function load_foo in a new file named python/graphscope/dataset/foo.py.  (see [example](#))
- A corresponding unit test is appreciated!
