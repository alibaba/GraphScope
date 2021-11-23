# Builtin Graph Datasets

GraphScope provides a robust graph loading module to load a graph from a set of files.

It also ships with a dataset download module, with pre-configured schemas for known datasets as sample data.

## Basic usage

TODO: some dataset violate this convention (ogbn_mag_small, ppi, u2i)

The format of the graph loading function is always `load_XXX`, where the `XXX` represent the name of dataset. And they can be imported by `from graphscope.dataset import load_XXX`.
A set of avaiable datasets is listed in the next section.

The signature of the function is `load_XXX(sess=None, prefix=None)`, where
  - `sess` represents the session in which the graph is loaded. `None` means the default session.
  - `prefix` represents where the data files are located in. `None` means let graphscope download it from internet.

For example, let's load the cora dataset in two lines:

```python
from graphscope.dataset import load_cora

g = load_cora()
```


## Available datasets

Currently, the supported graph datasets are listed as below. 

- [cora](https://linqs.soe.ucsc.edu/data)
- [ldbc-snb](http://github.com/ldbc/ldbc_snb_datagen)
- [modern-graph](https://tinkerpop.apache.org/docs/current/tutorials/getting-started)
- [ogbl-collab](https://ogb.stanford.edu/docs/linkprop/#ogbl-collab)
- [ogbl-ddi](https://ogb.stanford.edu/docs/linkprop/#ogbl-ddi)
- [ogbl-arxiv](https://ogb.stanford.edu/docs/nodeprop/#ogbn-arxiv)
- [ogb-mag](https://ogb.stanford.edu/docs/nodeprop/#ogbn-mag)
- [ogbn-proteins](https://ogb.stanford.edu/docs/nodeprop/#ogbn-proteins)
- [p2p-network](http://snap.stanford.edu/data/p2p-Gnutella31.html)
- [ppi](https://humgenomics.biomedcentral.com/articles/10.1186/1479-7364-3-3-291)
- [u2i](https://github.com/alibaba/graph-learn/blob/master/examples/data/u2i.py)

The list is growing. Hopefully, it would support all datasets from [ogb](https://ogb.stanford.edu) and [snap](https://snap.stanford.edu/data/index.html). 

## Contributing

You are welcomed to contribute a dataset to GraphScope by submitting a Pull Request. You may following these to add a dataset.

- Find a popular and appropriate graph data. Reformat and organize it as vertex/edge files. If it is a property graph, make one file for each label of vertices/edges. Vertex files started with the ID, followed by properties; Edge files started with source vertex ID, destination vertex ID, and followed by properties.
```csv
# Vertex file format
id,prop_1,prop_2
0,0.1,a
1,0.2,b
```

```csv
# Edge file format
src_id,dst_id,prop_1,prop_2
0,1,0.3,1000
```

- Put all files into a folder and name it as the graph name.
- Compress the folder, then upload the compressed file together with the original folder to the dataset folder of the OSS bucket. e.g., assume you have a folder named `foo`, and two files `foo/nodes.csv` and `foo/edge.csv`, the files in the OSS bucket should be organized as,
```bash
dataset
|-- foo.tar.gz
|-- foo
    |-- nodes.csv
    |-- edge.csv
```
  See [ogbn_mag](https://github.com/GraphScope/gstest/tree/master/ogbn_mag_small) as a reference for the above steps.
- Write the loading function load_foo in a new file named `foo.py` inside folder `python/graphscope/dataset/`.  (see [dataset](https://github.com/alibaba/GraphScope/tree/main/python/graphscope/dataset))
- A corresponding unit test is appreciated! See [test_download.py](https://github.com/alibaba/GraphScope/blob/main/python/tests/unittest/test_download.py).
