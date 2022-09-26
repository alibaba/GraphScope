# GRAPE-JDK with GraphScope Analytical Engine

GRAPE-JDK is a subproject of GraphScope, presenting an efficient Java SDK for the analytical engine.
Powered by [Alibaba-FastFFI](https://github.com/alibaba/fastFFI) and its ability to bridge the huge programming gap between Java and C++, GRAPE-JDK
enables Java programmers to write and run graph algorithms with these benefits.

- **Easy to Program**. GRAPE-JDK mirrors the full-featured GRAPE framework, including ```Fragment```, ```MessageManager```, in a Java style, 
  hence a Java programmer can develop algorithms in GRAPE PIE-model easily.

- **Efficient Execution**. Due to the limited ability to access low-level system resources, existing graph computing systems written in Java are 
  usually suboptimal at efficiency. By leveraging the JNI acceleration provided by [```LLVM4JNI```](https://github.com/alibaba/fastFFI/tree/main/llvm4jni),
  GRAPE-JDK substantially narrows the gap between apps written in Java and in C++. As [experiments](#performance) shows, the overall performance gap 
  between native C++ and GRAPE-JDK are lower than 2x, and in some scenarios like PageRank, GRAPE-JDK runs nearly as fast as native C++ implementation. 

- **Support Giraph app**. We also prvide user with a Giraph SDK, giraph-on-grape. The algorithms user implemented on [Giraph](https://github.com/apache/giraph)
 interface can be run on GRAPE-JDK without any modification.

<!-- - **Seamless integration with GraphScope**.

  To run a Java app developed with GRAPE-JDK, the user just need to pack Java app into ```jar``` and
  submit in python client, as show in example. The input graph can be either property graph or
  projected graph in GraphScope, and the output can be redirected to client fs, vineyard just like
  normal GraphScope apps. -->
  
## Organization

- **grape-demo** provides example apps and [FFIMirrors](#user-defined-data-structure).
- **grape-jdk** provides the PIE SDK with graph computing interfaces.
- **grape-runtime** contains the essential files for JNI code-gen and the glue code invoked by the analytcial engine (building with `ENABLE_JAVA_SDK`). 
- **giraph-on-grape** provides the Pregel SDK for Giraph apps.

-----
(To be revise)


## Get grape-jdk

### Building from source

```bash
git clone https://github.com/alibaba/GraphScope.git
cd analytical_engine/java/grape-jdk
mvn clean install
```

This will only install `grape-jdk` for you, if you are only interested in writing 
graph algorithms in java, that's enough for you :D.

To build the whole project, make sure there is one usable c++ compiler in your envirment
and both [`GraphScope-Analytical engine`](https://github.com/alibaba/GraphScope/tree/main/analytical_engine) 
and [`Vineyard`](https://github.com/v6d-io/v6d) is installed.

### From Maven Central repo

TODO

## Getting Started

- [Run a simple sssp](https://graphscope.io/docs/analytics_engine.html#run-a-demo-java-algorithm)
- [Implement your own algorithm](https://graphscope.io/docs/analytics_engine.html#writing-your-own-algorithms-in-java)


# Documentation

Online JavaDoc is available at [GraphScope Docs](https://graphscope.io/docs/reference/gae_java/index.html).

You can also generate the documentation with in three different ways.
- use Intellij IDEA plugin: [Intellij IDEA-javadoc](https://www.jetbrains.com/help/idea/working-with-code-documentation.html) 
- Use Eclipse plugin: [Eclipse-javadoc](https://www.tutorialspoint.com/How-to-write-generate-and-use-Javadoc-in-Eclipse). 
- Generate javaDoc from cmd.
```bash
cd ${GRAPHSCOPE_REPO}/analytical_engine/java/grape-jdk
mvn javadoc::javadoc -Djavadoc.output.directory=${OUTPUT_DIR} -Djavadoc.output.destDir=${OUTPUT_DEST_DIR}
```

# Performance

Apart from the user-friendly interface, grape-jdk also provide user with high performance graph 
analytics experience. Please refer to [benchmark](performance.md) for the benchmark results.

# TODO
- Support more programming model
  - Giraph(Pregel)
- A test suite for verifying algorithm correctness, without GraphScope analytical engine.
- Documentation
- User-friendly error report





