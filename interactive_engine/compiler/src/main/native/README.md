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

After the build completes, a tarball named `graph-planner-jni.tar.gz` will be available in the `assembly/target` directory. Extract the contents of the tarball:

```bash
cd assembly/target  
tar xvzf graph-planner-jni.tar.gz  
cd graph-planner-jni  
```

### Step 3: Run the Example Binary

To demonstrate the usage of the JNI interface, an example binary `test_graph_planner` is provided. Use the following command to execute it:

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
 * @param graph_schema_yaml Path to the graph schema file in YAML format (optional) 
 * @param graph_statistic_json Path to the graph statistics file in JSON format (optional)
 */  
GraphPlannerWrapper(const std::string &java_path,  
                    const std::string &jna_path,  
                    const std::string &graph_schema_yaml = "",  
                    const std::string &graph_statistic_json = "");  
```

## Method

```cpp
/**
 * @brief Compile a cypher query to a physical plan by JNI invocation.
 * @param compiler_config_path The path of compiler config file.
 * @param cypher_query_string The cypher query string.
 * @param graph_schema_yaml Content of the graph schema in YAML format
 * @param graph_statistic_json Content of the graph statistics in JSON format
 * @return The physical plan in bytes and result schema in yaml.
 */
Plan GraphPlannerWrapper::CompilePlan(const std::string &compiler_config_path,
                                      const std::string &cypher_query_string,
                                      const std::string &graph_schema_yaml,
                                      const std::string &graph_statistic_json)
```

Here is a refined version of your documentation with improvements for clarity, consistency, and readability:

## Restful API

We provide an alternative method to expose the interface as a RESTful API. Follow the steps below to access the interface via REST.

### Step 1: Build the Project

To build the project, run the following command:
```bash
cd interactive_engine
# Use '-Dskip.native=true' to skip compiling C++ native code
mvn clean package -DskipTests -Pgraph-planner-jni -Dskip.native=true
```

### Step 2: Locate and Extract the Package

Once the build completes, a tarball named graph-planner-jni.tar.gz will be available in the assembly/target directory. Extract the contents as follows:

```bash
cd assembly/target
tar xvzf graph-planner-jni.tar.gz
cd graph-planner-jni
```

### Step 3: Start the Graph Planner RESTful Service

To start the service, run the following command:

```bash
java -cp ".:./libs/*" com.alibaba.graphscope.sdk.restful.GraphPlannerService --spring.config.location=./conf/application.yaml
```

### Step 4: Access the RESTful API

To send a request to the RESTful API, use the following `curl` command:

```bash
curl -X POST http://localhost:8080/api/compilePlan \
    -H "Content-Type: application/json" \
    -d "{
        \"configPath\": \"$configPath\",
        \"query\": \"$query\",
        \"schemaYaml\": \"$schemaYaml\",
        \"statsJson\": \"$statsJson\"
    }"
```

Replace `$configPath`, `$query`, `$schemaYaml`, and `$statsJson` with the appropriate values.

The response will be in JSON format, similar to:

```json
{
    "graphPlan": {
        "physicalBytes": "",
        "resultSchemaYaml": ""
    }
}
```

The response contains two fields:
1. physicalBytes: A Base64-encoded string representing the physical plan bytes.
2. resultSchemaYaml: A string representing the YAML schema.

You can decode these values into the required structures.

Alternatively, you can run the following `Java` command to execute the same request:

```bash
java -cp ".:./libs/*" com.alibaba.graphscope.sdk.examples.TestGraphPlanner ./conf/interactive_config_test.yaml "Match (n) Return n;" ./conf/graph.yaml ./conf/modern_statistics.json
```

Here’s an example of how to access the RESTful API and decode the response in Java code:
```
public static void main(String[] args) throws Exception {
    if (args.length < 4) {
        System.out.println("Usage: <configPath> <query> <schemaPath> <statsPath>");
        System.exit(1);
    }
    // set request body in json format
    String jsonPayLoad = createParameters(args[0], args[1], args[2], args[3]).toString();
    HttpClient client = HttpClient.newBuilder().build();
    // create http request, set header and body content
    HttpRequest request =
            HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:8080/api/compilePlan"))
                    .setHeader("Content-Type", "application/json")
                    .POST(
                            HttpRequest.BodyPublishers.ofString(
                                    jsonPayLoad, StandardCharsets.UTF_8))
                    .build();
    // send request and get response
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    String body = response.body();
    // parse response body as json
    JsonNode planNode = (new ObjectMapper()).readTree(body).get("graphPlan");
    // print result
    System.out.println(getPhysicalPlan(planNode));
    System.out.println(getResultSchemaYaml(planNode));
}

private static JsonNode createParameters(
        String configPath, String query, String schemaPath, String statsPath) throws Exception {
    Map<String, String> params =
            ImmutableMap.of(
                    "configPath",
                    configPath,
                    "query",
                    query,
                    "schemaYaml",
                    FileUtils.readFileToString(new File(schemaPath), StandardCharsets.UTF_8),
                    "statsJson",
                    FileUtils.readFileToString(new File(statsPath), StandardCharsets.UTF_8));
    return (new ObjectMapper()).valueToTree(params);
}

// get base64 string from json, convert it to physical bytes , then parse it to PhysicalPlan
private static GraphAlgebraPhysical.PhysicalPlan getPhysicalPlan(JsonNode planNode)
        throws Exception {
    String base64Str = planNode.get("physicalBytes").asText();
    byte[] bytes = java.util.Base64.getDecoder().decode(base64Str);
    return GraphAlgebraPhysical.PhysicalPlan.parseFrom(bytes);
}

// get result schema yaml from json
private static String getResultSchemaYaml(JsonNode planNode) {
    return planNode.get("resultSchemaYaml").asText();
}
```
   