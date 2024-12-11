# Using the Graph Planner JNI Interface for C++ Invocation

Follow the steps below to get started with the Graph Planner JNI interface for c++ invocation.

## Getting Started
### Step 1: Build the Project

Navigate to the project directory and build the package using Maven:
```bash
cd interactive_engine  
mvn clean package -DskipTests -Pgraph-planner-jni 
``` 

### Step 2: Locate and Extract the Package

After the build completes, a tarball named graph-planner-jni.tar.gz will be available in the assembly/target directory. Extract the contents of the tarball:

```bash
cd assembly/target  
tar xvzf graph-planner-jni.tar.gz  
cd graph-planner-jni  
```

### Step 3: Run the Example Binary

To demonstrate the usage of the JNI interface, an example binary test_graph_planner is provided. Use the following command to execute it:

```bash
# bin/test_graph_planner <java class path> <jna lib path> <graph schema path> <graph statistics path> <query> <config path>  
bin/test_graph_planner libs native ./conf/graph.yaml ./conf/modern_statistics.json "MATCH (n) RETURN n, COUNT(n);" ./conf/interactive_config_test.yaml 
``` 

The output includes the physical plan and result schema in YAML format. Below is an example of a result schema:

```yaml
schema:  
  name: default  
  description: default desc  
  mode: READ  
  extension: .so  
  library: libdefault.so  
  params: []  
returns:  
  - name: n  
    type: {primitive_type: DT_UNKNOWN}  
  - name: $f1  
    type: {primitive_type: DT_SIGNED_INT64}  
type: UNKNOWN  
query: MATCH (n) RETURN n, COUNT(n);  
```

The `returns` field defines the result schema. Each entry’s name specifies the column name, and the order of each entry determines the column IDs.

## Explanation

Below is a brief explanation of the interface provided by the example:

### Constructor

```cpp
/**  
 * @brief Constructs a new GraphPlannerWrapper object  
 * @param java_path Java class path  
 * @param jna_path JNA library path  
 * @param graph_schema_yaml Path to the graph schema file in YAML format  
 * @param graph_statistic_json Path to the graph statistics file in JSON format
 */  
GraphPlannerWrapper(const std::string &java_path,  
                    const std::string &jna_path,  
                    const std::string &graph_schema_yaml,  
                    const std::string &graph_statistic_json = "");  
```

## Method

```cpp
/**  
 * @brief Compiles a Cypher query into a physical plan via JNI invocation.  
 * @param compiler_config_path Path to the compiler configuration file.  
 * @param cypher_query_string Cypher query string to compile.  
 * @return Physical plan in bytes and result schema in YAML format.  
 */  
Plan GraphPlannerWrapper::CompilePlan(const std::string &compiler_config_path,  
                                      const std::string &cypher_query_string);  
```
