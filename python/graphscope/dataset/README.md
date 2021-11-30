# Builtin Graph Datasets

GraphScope provides a robust graph loading module to load a graph from a set of files.

It also ships with a dataset download module, with pre-configured schemas for known datasets as sample data.

## Basic usage

TODO: some datasets violate this convention (ogbn_mag_small, ppi, u2i, ldbc-snb)

Users can always use a function named like `load_XXX` to load a builtin dataset, where the `XXX` represent the name of graph. These functions can be imported by `from graphscope.dataset import load_XXX` in Python. All available datasets are listed in the next section.

The signature of the function is `load_XXX(sess=None, prefix=None)`, where
  - `sess` represents the session which the graph is loaded. The default value `None` means the default session(local session).
  - `prefix` assigns the existing files location. By default, users do not need to provide this and `graphscope` will download it from the Internet.

For example, we can load the `cora` dataset as a `Graph` in `graphscope` with two lines of code:

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

- Find a popular and appropriate graph data. Reformat and organize it as vertex/edge files. If it is a property graph, make one file for each label of vertices/edges. The columns in vertex files start with the ID, followed by properties. While the columns of edge files start with source vertex ID, destination vertex ID, and followed by properties. e.g., 

```shell
# Vertex file format
id,name,score
0,James,95
1,Helen,85
```

```shell
# Edge file format (represents )
sender,receiver,msg_count,last_sent
0,1,3,2021-11-23
```

- Put all files into a folder and name it as the graph name.
- Compress the folder, then upload the compressed file together with the original folder to the dataset folder of the OSS bucket. e.g., assume you have a folder named `foo`, and two files `foo/nodes.csv` and `foo/edge.csv`, the files in the OSS bucket should be organized as (or see [example](https://github.com/GraphScope/gstest/tree/master/ogbn_mag_small)),
```bash
dataset
|-- foo.tar.gz
|-- foo
    |-- nodes.csv
    |-- edge.csv
```
- Write the loading function load_foo in a new file named `foo.py` inside folder `python/graphscope/dataset/`.  (see [example](https://github.com/alibaba/GraphScope/blob/docs/python/graphscope/dataset/ogbn_mag.py))
- A corresponding unit test is appreciated! See [test_download.py](https://github.com/alibaba/GraphScope/blob/main/python/graphscope/tests/unittest/test_download.py).
