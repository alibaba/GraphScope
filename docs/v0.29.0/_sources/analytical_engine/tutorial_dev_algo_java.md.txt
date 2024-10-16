# Tutorial: Develop your Algorithm in Java with PIE Model
GraphScope provides sufficient support for ``java programmers`` to implement graph algorithms and run it on GraphScope Analytical Engine.
In this tutorial, you will first try to explore GraphScope JavaSDK with some example algorithms, then implement your own algorithm, finally submit to GraphScope.

## Run example algorithms with example jar

An example jar which contains implementation of several graph algorithms(i.e. PageRank, SSSP, BFS) is provided in 
[grape-demo.jar](https://graphscope.oss-cn-beijing.aliyuncs.com/jar/grape-demo-0.19.0-shaded.jar). You can run the graph algorithms provided in this jar by submitting the downloaded jar to GraphScope.

Here we provide an example to run `SSSP` on p2p dataset.

```python
import graphscope
from graphscope.dataset import load_p2p_network
from graphscope.framework.app import load_app

# turn on this line to enable log verbose
# graphscope.set_option(show_log=True) 

"""Or launch session in k8s cluster"""
sess = graphscope.session(cluster_type='hosts') 

graph = load_p2p_network(sess)    

"""Java algorithm need to run on simple graph"""
graph = graph.project(vertices={"host": ['id']}, edges={"connect": ["dist"]})

sess.add_lib("/home/graphscope/grape-demo-0.19.0-shaded.jar") # replace path to grape-demo.jar
sssp=load_app(algo="java_pie:com.alibaba.graphscope.example.sssp.SSSP")
ctx=sssp(graph._project_to_simple(), src=6, threadNum=1)

"""Fetch the result via context"""
ctx.to_numpy("r")
```

For more info about GraphScope Python client, please refer to [GraphScope Python Doc](https://graphscope.io/docs/latest/reference/python_index.html). 

## Implement your own algorithms in Java.

To implement java graph algorithms runnable on GraphScope, all you need is `GRAPE-jdk`, which provide a PIE programming interface.

### Prepare **GRAPE-jdk**

You can include `GRAPE-jdk` as a dependency in your maven project by adding following configuration. 

#### Get from Maven Central Repository

Find the latest version available on [Maven-Central-Repository](https://mvnrepository.com/artifact/com.alibaba.graphscope/grape-jdk)

#### Build from source

```bash
git clone https://github.com/alibaba/GraphScope
cd GraphScope/analytical_engine/java
mvn clean install -DskipTests # the version is specified in pom.xml's revision property
```

```xml
<dependency>
  <groupId>com.alibaba.graphscope</groupId>
  <artifactId>grape-jdk</artifactId>
  <version>${grape-jdk-version}</version>
</dependency>
```

To address the dependency issue in jar packaging, you shall package all your 
dependent jars in a fat-jar. For example, you can use `maven-shade-plugin` to
include runtime jars in your fat-jar.

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <version>3.2.4</version>
</plugin>
```

### Implement your algorithm

Different from the *pregel* interface provided by **Apache Giraph** and **Spark GraphX**, `GRAPE-jdk` provides user with **PIE** programming interface. Unlike *pregel*'s ``vertex-centric`` interface, **PIE** 
models graph computing in a ``subgraph-centric`` manner. 
In `PIE` model, the program requires less supersteps and the size of generated message has been drastically reduced, which lead to great performance improvement.

To implement a `PIE` algorithm, you need to provide two separate functions, `PEval` and `IncEval`. `PEval` function will be execute only once at the first round of computation, and `IncEval` will be called for multiple times until convergence. You are also supposed to provide a class called `Context`. You can put intermediate
results, init configuration in this class. The `init` method will be called before
`PEval`.

Here we provide a simple PIE algorithm which simply traverse the graph.

```java
public class Traverse implements ParallelAppBase<Long, Long, Long, Long, TraverseContext>,
    ParallelEngine {

    @Override
    public void PEval(IFragment<Long, Long, Long, Long> fragment,
        ParallelContextBase<Long, Long, Long, Long> context,
        ParallelMessageManager messageManager) {
        TraverseContext ctx = (TraverseContext) context;
        for (Vertex<Long> vertex : fragment.innerVertices()) {
            AdjList<Long, Long> adjList = fragment.getOutgoingAdjList(vertex);
            for (Nbr<Long, Long> nbr : adjList.iterator()) {
                Vertex<Long> dst = nbr.neighbor();
                //Update largest distance for current vertex
                ctx.vertexArray.setValue(vertex, Math.max(nbr.data(), ctx.vertexArray.get(vertex)));
            }
        }
        messageManager.forceContinue();
    }


    @Override
    public void IncEval(IFragment<Long, Long, Long, Long> fragment,
        ParallelContextBase<Long, Long, Long, Long> context,
        ParallelMessageManager messageManager) {
        TraverseContext ctx = (TraverseContext) context;
        for (Vertex<Long> vertex : fragment.innerVertices()) {
            AdjList<Long, Long> adjList = fragment.getOutgoingAdjList(vertex);
            for (Nbr<Long, Long> nbr : adjList.iterator()) {
                Vertex<Long> dst = nbr.neighbor();
                //Update largest distance for current vertex
                ctx.vertexArray.setValue(vertex, Math.max(nbr.data(), ctx.vertexArray.get(vertex)));
            }
        }
    }
}
```

The corresponding context class parse the input parameter `maxIteration` from a JSONObject.
```java
public class TraverseContext extends
    VertexDataContext<IFragment<Long, Long, Long, Long>, Long> implements ParallelContextBase<Long,Long,Long,Long> {


    public GSVertexArray<Long> vertexArray;
    public int maxIteration;


    @Override
    public void Init(IFragment<Long, Long, Long, Long> frag,
        ParallelMessageManager messageManager, JSONObject jsonObject) {
        createFFIContext(frag, Long.class, false);
        //This vertex Array is created by our framework. Data stored in this array will be available
        //after execution, you can receive them by invoking method provided in Python Context.
        vertexArray = data();
        maxIteration = 10;
        if (jsonObject.containsKey("maxIteration")){
            maxIteration = jsonObject.getInteger("maxIteration");
        }
    }


    @Override
    public void Output(IFragment<Long, Long, Long, Long> frag) {
        //You can also write output logic in this function.
    }
}
```

For more detail usages of `GRAPE-jdk`, you can check [GRAPE-jdk JavaDoc](https://graphscope.io/docs/reference/gae_java/index.html). For more example apps, please refer [GraphScope/gs-algos](https://github.com/GraphScope/gs-algos).

### Submit to GraphScope 

To run your algorithm on GraphScope, your first need to obtain a fat-jar which contains your implementation.

```
mvn package
```

Then a jar with name `{artifact}*-shaded.jar` shall be found under `target/`. 
By submit this jar to graphscope, you can run any algorithms contained with proper
parameters.

```python
import graphscope
from graphscope import JavaApp
from graphscope.dataset import load_p2p_network

"""Or launch session in k8s cluster"""
sess = graphscope.session(cluster_type='hosts')


graph = load_p2p_network(sess)
graph = graph.project(vertices={"host": ['id']}, edges={"connect": ["dist"]})
# you can also use your own graph, refer to graphscope load graph tutorial.
# But remember project to single property graph before running algorithms.

sess.add_lib("{full/path/to/your/packed/jar}")  # *-shaded.jar
app=load_app(algo="java_pie:com.alibaba.graphscope.example.Traverse") # java_pie:{you-full-class-name}
ctx=app(graph, "{param string}") # a=b,c=d
```

After computation, you can obtain the results stored in context with the help of [`Context`](https://graphscope.io/docs/reference/context.html#context).

## GraphScope JavaSDK with GitHub Template

If you don't bother creating new project to try `GRAPE-jdk`, we provide a template project [GraphScope-Java-template](https://github.com/zhanglei1949/GraphScope-Java-template). By click **Use this template**, you can create a new repository with same files and structure of the template repository. You can then try developing your own algorithms in this project.