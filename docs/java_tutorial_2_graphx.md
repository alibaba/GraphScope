# Run existing jars developed for Apache GraphX on GraphScope

[Apache Spark](https://spark.apache.org/) is a famous engine for large-scale data analytics. [Spark GraphX](https://spark.apache.org/graphx/) is Spark's graph
computing module, which provides  flexible and efficient graph computation framework.

Graphscope is also developed to be integrated with Spark GraphX. User can easily deploy a graphscope cluster colocated with spark cluster. And by switch `SparkSession` to `GSSparkSession`, user can experience up to 7 times performance 
improvement when running GraphX algorithms.

## Deploy GraphScope along with Spark

We assume you already have a spark cluster deployed. If you don't have a spark cluster deployed, please refer to [spark-cluster-overview](https://spark.apache.org/docs/latest/cluster-overview.html) to deploy a spark cluster.
Spark distributions with **version ==3.1.3** has been tested to be compatible with GraphScope.

Also, GraphScope can be easily distributed with python package. Since GraphScope only 
support python3, you shall upgrade your python enviroment before proceeding on.

Then, on client side, we will use `venv` to create a virtual enviroment pack which contains graphscope package.

```bash
pip3 install virtualenv venv-pack
python -m venv pyspark_venv
source pyspark_venv/bin/activate
pip3 install graphscope
venv-pack -o pyspark_venv_gs.tar.gz
```

Now, `pyspark_venv_gs.tar.gz` contains neccessary enviroments graphscope need. Every time
you submit a job to your spark cluster, remember to upload this pack.

```bash
export PYSPARK_DRIVER_PYTHON=python
export PYSPARK_PYTHON=./environment/bin/python
spark-submit --archives pyspark_venv_gs.tar.gz#environment ...
```

## Run example GraphX apps

Several GraphX algorithms are also contained in [grape-demo.jar](https://graphscope.oss-cn-beijing.aliyuncs.com/jar/grape-demo-0.19.0-shaded.jar). You can have a try to run these GraphX algorithm on GraphScope.

You can download `p2p` dataset and `grape-demo.jar` with following command.
```bash
wget https://raw.githubusercontent.com/GraphScope/gstest/master/p2p-31.e /home/graphscope/p2p-31.e
wget https://graphscope.oss-cn-beijing.aliyuncs.com/jar/grape-demo-0.19.0-shaded.jar /home/graphscope/grape-demo-0.19.0-shaded.jar
```

Different from Giraph-on-Graphscope, for GraphX-GraphScope integration, we need to submit jobs to spark cluster, not with GraphScope python client.


### Submit to Spark

```bash
# Path to Graphscope jars is need for running graphx algo on GraphScope.
# FIXME(yuansi): Here we assume env var GRAPHSCOPE_HOME available in environment.
export GS_JARS=`ls ${GRAPHSCOPE_HOME}/lib/grape-graphx-*.jar`:`ls ${GRAPHSCOPE_HOME}/lib/grape-runtime-*.jar` 

# default port is 7077, for standalone cluster, like spark://${host}:${port}
/bin/spark-submit --verbose --master spark://${master_url} \
--archives pyspark_venv_gs.tar.gz#environment  --jars ${GS_JARS} \
--conf spark.executor.instances=2 \
--conf spark.driver.memory=2g \
--conf spark.executor.memory=10g \
--conf spark.scheduler.minRegisteredResourcesRatio=1.0 \
--conf spark.gs.submit.jar=/home/graphscope/grape-demo-0.19.0-shaded.jar \
--class com.alibaba.graphscope.example.graphx.BFSTest 
/home/graphscope/grape-demo-0.19.0-shaded.jar  /home/graphscope/p2p-31.e 2 1
```

Remember to replace the placeholders like `${master_url}` with acutal cluster url.

## Run customized GraphX apps

To develop your GraphX algorithms which can run on GraphScope, users shall program towards the RDD interfaces provided by Spark GraphX, since all GraphX interfaces are
supported by Graphscope.

### Include dependency

Include `grape-graphx` dependency in your project's `pom.xml`.

```xml
<dependency>
  <groupId>com.alibaba.graphscope</groupId>
  <artifactId>grape-graphx</artifactId>
  <classifier>shaded</classifier>
  <scope>provided</scope>
  <version>0.19.0</version>
</dependency>
```

And you also need to configure `maven-shaded-plugin` with following configuration to make sure the conflicts can be correctly resolved.

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <executions>
        <execution>
        <goals>
            <goal>shade</goal>
        </goals>
        <phase>package</phase>
        <configuration>
            <filters>
                <filter>
                    <artifact>org.apache.spark:*</artifact>
                    <includes>
                        <include>org/apache/spark/**</include>
                    </includes>
                </filter>
            </filters>
            <relocations>
            <relocation>
                <pattern>org.apache.spark.graphx</pattern>
                <shadedPattern>org.apache.spark.gs.graphx</shadedPattern>
            </relocation>
            </relocations>
        </configuration>
        </execution>
    </executions>
</plugin>
```


### Develop customized GraphX algorithm towards GraphScope.


Other than the interface provided by GraphX, GraphScope also provide some other graphscope-only features
via `GSSparkSession`. User shall use `GSSparkSession` insteadof `SparkSession` to make their algorithm runnable on GraphScope.

`GSSparkSession` extends `SparkSession` with following new methods.
```scala
/** GraphgScope related param, setting vineyard memroy size.
*/
def vineyardMemory(memoryStr: String): Builder =
config("spark.gs.vineyard.memory", memoryStr)

/** GraphScope vineyard socket file. Vineyard process should be bound on this address on all workers.
*/
def vineyardSock(filePath: String): Builder = {
config("spark.gs.vineyard.sock", filePath)
}

/** User need to specify the file path to the jar submitted to spark cluster.
*/
def gsSubmitJar(filePath: String): Builder = {
config("spark.gs.submit.jar", filePath)
}

// convert GraphX Graph to GrapeGraph
def toGSGraph[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED]
): GrapeGraphImpl[VD, ED] = {

}

// Load grapeGraph from files.
def loadGraphToGS[VD: ClassTag, ED: ClassTag](
    vFilePath: String,
    eFilePath: String,
    numPartitions: Int
): GrapeGraphImpl[VD, ED] = {
}
```


### Run customized GraphX algorithms on Spark with GraphScope support

Great performace improvement is observed when running graphx algorithms on GraphScope other than GraphX. To enable GraphScope support, just add necessary arguments to spark-submit shell when submit your job, like [Submit example GraphX app to Spark](#submit-to-spark). Just remember to to change jar name, app name and params.