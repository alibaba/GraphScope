# GRAPE-JDK with GraphScope Analytical Engine

[pagerank-app]: analytical_engine/java/grape-demo/src/main/java/com/alibaba/graphscope/example/property/pagerank/ParallelPropertyPageRankVertexData.java
[sssp-app]: analytical_engine/java/grape-demo/src/main/java/com/alibaba/graphscope/example/property/sssp/ParallelPropertySSSPVertexData.java
[wcc-app]: analytical_engine/java/grape-demo/src/main/java/com/alibaba/graphscope/example/property/wcc/ParallelPropertyWCCVertexData.java
[bfs-app]: analytical_engine/java/grape-demo/src/main/java/com/alibaba/graphscope/example/property/bfs/ParallelPropertyBfsVertexData.java


GRAPE-JDK is a subproject of GraphScope, presenting an efficient Java SDK for the analytical engine.
Powered by [Alibaba-FastFFI](https://github.com/alibaba/fastFFI) and its ability to bridge the huge programming gap between Java and C++, GRAPE-JDK
enables Java programmers to write and run graph algorithms with these benefits.

- **Easy to Program**. GRAPE-JDK mirrors the full-featured GRAPE framework, including ```Fragment```, ```MessageManager```, in a Java style, 
  hence a Java programmer can develop algorithms in GRAPE PIE-model easily.

- **Efficient Execution**. Due to the limited ability to access low-level system resources, existing graph computing systems written in Java are 
  usually suboptimal at efficiency. By leveraging the JNI acceleration provided by [```LLVM4JNI```](https://github.com/alibaba/fastFFI/tree/main/llvm4jni), GRAPE-JDK substantially narrows the gap between apps written in Java and in C++. As [experiments(TODO: link)](#) shows...

<!-- - **Seamless integration with GraphScope**.

  To run a Java app developed with GRAPE-JDK, the user just need to pack Java app into ```jar``` and
  submit in python client, as show in example. The input graph can be either property graph or
  projected graph in GraphScope, and the output can be redirected to client fs, vineyard just like
  normal GraphScope apps. -->
  
## Organization

- **grape-demo** provides example apps and [FFIMirrors](#user-defined-data-structure).
- **grape-jdk** provides the SDK with graph computing interfaces.
- **grape-runtime** contains the essential files for JNI code-gen and the glue code invoked by the analytcial engine (building with `ENABLE_JAVA_SDK`). 

-----
(To be revise)


# How to use

Build from local:
```bash
mvn clean install
```

If you only need interfaces defined in ```grape-jdk```, just type
```bash
mvn clean install -DskipTests -Dmaven.antrun.skip=true
```
which will skip the codegen process.
## From Maven Central repo

TODO
## Simple Examples
Here we provide a simple example illustrating the usage of PIE SDK.
### 0. Include java sdk in your maven project
First, you should include ```grape-jdk``` as maven dependency in your project
```pom.xml```

```xml
    <dependency>
      <groupId>com.alibaba.graphscope</groupId>
      <artifactId>grape-jdk</artifactId>
      <version>0.1</version>
      <classifier>shaded</classifier>
    </dependency>
```

Your may notice that you are required to depend on graphscope-sdk with shaded classifier.
Actually the shaded jar contains all necessary dependencies in one jar.
And in your project, you shall use ```plugin``` to pack your jar with dependencies!
```xml
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
      </plugin>
```
### 1. User-defined data structure

With the codegen abilities provided by [alibaba-FastFFI](https://github.com/alibaba/fastFFI), user 
can define new data structures in java interfaces. User can also define the 
```hash``` and ```equal``` methods.

graphscope-processor will generate the corresponding C++ and java code after ```jar```
is submitted to GraphScope.
```java
@FFIMirror
@FFINameSpace("sample")
@FFITypeAlias("MyData")
public interface MyData extends CXXPointer, FFIJava {
  //Static factory field
  Factory factory = FFITypeFactory.getFactory(MyData.class);

  //To create a instance for MyData
  static MyData create() {
    return factory.create();
  }

  //Getter method
  @FFIGetter
  long id();

  //Setter method
  @FFISetter
  void id(long value);

  //If specified, will be used as hash() in the generated java class.
  //Otherwise will use the default implementation.
  default int javaHashCode() {
    return (int) id();
  }
  
  //If specified, will be used as toString() in the generated java class.
  //Otherwise will use the default implementation.
  default String toJavaString() {
    return String.valueOf(id());
  }

  //If specified, will be used as equals() in the generated java class.
  //Otherwise will use the default implementation.
  default boolean javaEquals(Object obj) {
    if (obj instanceof MyData) {
      MyData other = (MyData) obj;
      return id() == other.id();
    } else {
      return false;
    }
  }

  @FFIFactory
  interface Factory {
    MyData create();
  }
}
```

### 2. User-defined app
Apart from common data structures provided in ```java.lang```, user can also use
```FFIMirror``` in app implementation. For String, user shall use ```FFIByteString```
as substitution.

For example, to implement a app which traverse all vertex in a property graph(ArrowFragment), user
shall code like this
```java
public class PropertyTraverseVertexData
        implements DefaultPropertyAppBase<Long, PropertyTraverseVertexDataContext> {
    @Override
    public void PEval(
            ArrowFragment<Long> fragment,
            PropertyDefaultContextBase<Long> context,
            PropertyMessageManager messageManager) {
        PropertyTraverseVertexDataContext ctx = (PropertyTraverseVertexDataContext) context;
        VertexRange<Long> innerVertices = fragment.innerVertices(0);
        for (Vertex<Long> vertex : innerVertices.locals()) {
            PropertyAdjList<Long> adjList = fragment.getOutgoingAdjList(vertex, 0);
            for (PropertyNbr<Long> cur : adjList.iterator()) {
                ctx.fake_edata = cur.getDouble(0);
                ctx.fake_vid = cur.neighbor().GetValue();
            }
        }
    }

    @Override
    public void IncEval(
            ArrowFragment<Long> fragment,
            PropertyDefaultContextBase<Long> context,
            PropertyMessageManager messageManager) {
        PropertyTraverseVertexDataContext ctx = (PropertyTraverseVertexDataContext) context;
        if (ctx.step >= ctx.maxStep) {
            return;
        }
        VertexRange<Long> innerVertices = fragment.innerVertices(0);
        for (Vertex<Long> vertex : innerVertices.locals()) {
            PropertyAdjList<Long> adjList = fragment.getOutgoingAdjList(vertex, 0);
            for (PropertyNbr<Long> cur : adjList.iterator()) {
                ctx.fake_edata = cur.getDouble(0);
                ctx.fake_vid = cur.neighbor().GetValue();
            }
        }
        messageManager.ForceContinue();
    }
}
```
### 3. Run java app in GraphScope
To run your app on GraphScope, make sure you have installed ```Graphscope``` client, as specified in [GraphScope-README](../README.md).
Then enter python cmd line,
```python3
import graphscope
from graphscope import JavaApp
graphscope.set_option(show_log=True)
"""Or lauch session in k8s cluster"""
sess = graphscope.session(cluster_type='hosts') 

graph = sess.g()
graph = sess.g(directed=False)
graph = graph.add_vertices("gstest/property/p2p-31_property_v_0", label="person")
graph = graph.add_edges("gstest/property/p2p-31_property_e_0", label="knows")

sssp1=JavaApp(
    full_jar_path="~/.m2/repository/com/alibaba/graphscope/grape-demo/0.1/grape-demo-0.1-shaded.jar", 
    java_app_class="com.alibaba.graphscope.example.sssp.SSSPDefault", 
)
ctx2=sssp1(graph,src=6)

graph = graph.project(vertices={"person": ['id']}, edges={"knows": ["dist"]})
"""simple_graph = graph._project_to_simple() will be done by JavaApp"""
sssp3=JavaApp(
    full_jar_path="~/.m2/repository/com/alibaba/graphscope/grape-demo/0.1/grape-demo-0.1-shaded.jar", 
    java_app_class="com.alibaba.graphscope.example.projected.SSSPProjected", 
)
ctx3=sssp3(graph,src=6)

ctx.to_numpy("r:label0.dist_0")
```

# Documentation

Online JavaDoc is availabel at [GraphScope Docs](https://graphscope.io/docs/reference/gae_java/index.html).

You can also generate the documentation with in three different ways.
- use Intellij IDEA plugin: [Intellij IDEA-javadoc](https://www.jetbrains.com/help/idea/working-with-code-documentation.html) 
- Use Eclipse plugin: [Eclipse-javadoc](https://www.tutorialspoint.com/How-to-write-generate-and-use-Javadoc-in-Eclipse). 
- Generate javaDoc from cmd.
```bash
cd ${GRAPHSCOPE_REPO}/analytical_engine/java/grape-jdk
mvn javadoc::javadoc -Djavadoc.output.directory=${OUTPUT_DIR} -Djavadoc.output.destDir=${OUTPUT_DEST_DIR}
```

# Performance

Apart from the user-friendly interface, grape-jdk also provide user with high performance graph analytics. Please refer to [benchmark](performance.md) for the benchmark results.

# TODO
- Support more programming model
  - Pregel
- A test suite for verifying algorithm correctness, without GraphScope analytical engine.
- Documentation
- User-friendly error report





