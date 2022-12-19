# Run existing jars developed for Apache Giraph on Graphscope

[Apache Giraph](https://giraph.apache.org/intro.html) is one of the most famous graph computing frameworks, built on top of Apache Hadoop. Through `pregel` interface, user can write ``vertex-cetric`` graph algorithms. 

GraphScope aiming to provide one-stop graph processing framework, including intergrating with popular open-source graph computing framework.
Actually, Giraph algorithms can be easily run on Graphscope without any adaptation.

## Try some example giraph apps

We provide some example giraph algorithms, i.e. SSSP, PageRank in [grape-demo.jar](https://graphscope.oss-cn-beijing.aliyuncs.com/jar/grape-demo-0.19.0-shaded.jar).
You can try to run these Giraph algorithms on GraphScope.

As `Giraph` allows user to load graph with customized loader, we support `Giraph VertexInputFormat` and `Giraph EdgeInputFormat` with `session.load_from` method.
```python
vformat = "giraph:com.alibaba.graphscope.example.giraph.format.P2PVertexInputFormat"
eformat = "giraph:com.alibaba.graphscope.example.giraph.format.P2PEdgeInputFormat"

#clone https://github.com/GraphScope/gstest to GS_TEST_DIR
graph = graphscope_session.load_from(
    vertices="/path/to/vertex-input",
    vformat=vformat,
    edges="/path/to/edge-input",
    eformat=eformat,
)
```

vertices and edges should points to vertex input and edge input. We also provide some example dataset `gstest` at [Graphscope/gstest](https://github.com/7br/gstest.git). 
In this tutorial we will only need `p2p` dataset. You can download it by:

```bash
wget https://raw.githubusercontent.com/GraphScope/gstest/master/p2p-31.e /home/graphscope/p2p-31.e
wget https://raw.githubusercontent.com/GraphScope/gstest/master/p2p-31.v /home/graphscope/p2p-31.v
```

Then you can load graph via graphscope python client, and query the graph with giraph app.

```python
import graphscope
import os
from graphscope.framework.app import load_app

"""Or lauch session in k8s cluster"""
sess = graphscope.session(cluster_type='hosts') 

sess.add_lib("/home/graphscope/grape-demo-0.19.0-shaded.jar")

# Remember to put giraph: before class name.
vformat = "giraph:com.alibaba.graphscope.example.giraph.format.P2PVertexInputFormat"
eformat = "giraph:com.alibaba.graphscope.example.giraph.format.P2PEdgeInputFormat"

# Replace path p2p.v and p2p.3 with your own path.
graph = sess.load_from(
    vertices=os.path.expandvars("/home/graphscope/p2p-31.v"),
    vformat=vformat,
    edges=os.path.expandvars("/home/graphscope/p2p-31.e"),
    eformat=eformat,
)
graph = graph._project_to_simple(v_prop="vdata", e_prop="data")

giraph_sssp = load_app(algo="giraph:com.alibaba.graphscope.example.giraph.SSSP")
ctx = giraph_sssp(graph, sourceId=6)

ctx.to_numpy('r')
```

## Run your own Giraph apps.

After a successful running of example giraph SSSP algorithm, you may want to try your own giraph algorithm on GraphScope(**which runs much faster then Giraph itself**). 

### Develop Giraph algorithm

You can implement your algorithm towards Giraph' original API. For example, you can use Giraph official example apps.

```bash
git clone https://github.com/apache/giraph.git
cd giraph/
mvn package -pl :giraph-examples
```

Then you could find `giraph-examples-1.4.0-SNAPSHOT-for-hadoop-1.2.1-jar-with-dependencies.jar` in directory `giraph-examples/target`.

Although almost all APIs are supported, there are indeed some limitation of Giraph-on-Graphscope.

- Currently graph modification API is not supported.
- Using of Complex Writable will cause performance degradation.

### Submit to GraphScope.

The procedure almost the same as above, except that you need to replace the submitted jar, and choose right `InputFormat` classes.

```python
import graphscope

"""Or lauch session in k8s cluster"""
sess = graphscope.session(cluster_type='hosts') 

# path to local jar file, will be distributed over cluster
graphscope_session.add_lib("path/to/grape-demo.jar")

vformat = "giraph:${vertex-input-format-class-full-name}"
eformat = "giraph:${edge-input-format-class-full-name}"

#clone https://github.com/GraphScope/gstest to GS_TEST_DIR
graph = graphscope_session.load_from(
    vertices=os.path.expandvars("${path-to-vertex-file}"), # path to local vertex file, will be distributed over cluster
    vformat=vformat,
    edges=os.path.expandvars("${path-to-edge-file}"), # path to local edge file,  will be distributed over cluster
    eformat=eformat,
)
graph = graph._project_to_simple(v_prop="vdata", e_prop="data")

giraph_sssp = load_app(algo="giraph:${giraph-computation-class-full-name}")
ctx = giraph_sssp(g, "${a=1,b=2...}")
```