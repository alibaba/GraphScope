# GraphManagementApi

All URIs are relative to *{INTERACTIVE_ENDPOINT}*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**listGraphs**](GraphManagementApi.md#ListGraphs) | **GET** /v1/graph |  |
| [**createGraph**](GraphManagementApi.md#CreateGraph) | **POST** /v1/graph |  |
| [**getGraph**](GraphManagementApi.md#GetGraphMeta) | **GET** /v1/graph/{graph_id} |  |
| [**getSchema**](GraphManagementApi.md#GetGraphSchema) | **GET** /v1/graph/{graph_id}/schema |  |
| [**deleteGraph**](GraphManagementApi.md#deleteGraph) | **DELETE** /v1/graph/{graph_id} |  |
| [**getGraphStatistic**](GraphManagementApi.md#getGraphStatistic) | **GET** /v1/graph/{graph_id}/statistics |  |
| [**createDataloadingJob**](GraphManagementApi.md#createDataloadingJob) | **POST** /v1/graph/{graph_id}/dataloading |  |


<a id="ListGraphs"></a>
# **ListGraphs**
> Result&lt;List&lt;GetGraphResponse&gt;&gt; getAllGraphs()



List all graphs

### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.models.GetGraphResponse;

public class Example {
    public static void main(String[] args) {
        if (System.getenv("INTERACTIVE_ENDPOINT") == null) {
                System.err.print("INTERACTIVE_ENDPOINT is not set");
                return;
        }
        String endpoint = String endpoint = System.getenv("INTERACTIVE_ENDPOINT");
        Driver driver = Driver.connect(endpoint);
        Session session = driver.session();

        Result<List<GetGraphResponse>> getRes = session.getAllGraphs();
        if (!getRes.isOk()) {
            System.out.println("Failed to get graph: " + getRes.getStatusMessage());
            return;
        }
        else {
            System.out.println("Got graphs: " + getRes.getValue());
        }
    }
}
```

### Parameters
None.

### Return type

[**Result&lt;List&lt;GetGraphResponse&gt;&gt;**](GetGraphResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Successful operation |  -  |


<a id="CreateGraph"></a>
# **CreateGraph**
> Result&lt;CreateGraphResponse&gt; createGraph(createGraphRequest)



Create a new graph

### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.models.*;

public class Example {
  private static final String MODERN_GRAPH_SCHEMA_JSON = "{\n" +
          "    \"name\": \"modern_graph\",\n" +
          "    \"description\": \"This is a test graph\",\n" +
          "    \"schema\": {\n" +
          "        \"vertex_types\": [\n" +
          "            {\n" +
          "                \"type_name\": \"person\",\n" +
          "                \"properties\": [\n" +
          "                    {\n" +
          "                        \"property_name\": \"id\",\n" +
          "                        \"property_type\": {\"primitive_type\": \"DT_SIGNED_INT64\"},\n" +
          "                    },\n" +
          "                    {\n" +
          "                        \"property_name\": \"name\",\n" +
          "                        \"property_type\": {\"string\": {\"long_text\": \"\"}},\n" +
          "                    },\n" +
          "                    {\n" +
          "                        \"property_name\": \"age\",\n" +
          "                        \"property_type\": {\"primitive_type\": \"DT_SIGNED_INT32\"},\n" +
          "                    },\n" +
          "                ],\n" +
          "                \"primary_keys\": [\"id\"],\n" +
          "            }\n" +
          "        ],\n" +
          "        \"edge_types\": [\n" +
          "            {\n" +
          "                \"type_name\": \"knows\",\n" +
          "                \"vertex_type_pair_relations\": [\n" +
          "                    {\n" +
          "                        \"source_vertex\": \"person\",\n" +
          "                        \"destination_vertex\": \"person\",\n" +
          "                        \"relation\": \"MANY_TO_MANY\",\n" +
          "                    }\n" +
          "                ],\n" +
          "                \"properties\": [\n" +
          "                    {\n" +
          "                        \"property_name\": \"weight\",\n" +
          "                        \"property_type\": {\"primitive_type\": \"DT_DOUBLE\"},\n" +
          "                    }\n" +
          "                ],\n" +
          "                \"primary_keys\": [],\n" +
          "            }\n" +
          "        ],\n" +
          "    },\n" +
          "}";

  public static void main(String[] args) {
    if (System.getenv("INTERACTIVE_ENDPOINT") == null) {
        System.err.print("INTERACTIVE_ENDPOINT is not set");
        return;
    }
    String endpoint = System.getenv("INTERACTIVE_ENDPOINT");
    Driver driver = Driver.connect(endpoint);
    Session session = driver.session();

    // First create graph
      CreateGraphRequest graph = CreateGraphRequest.fromJson(MODERN_GRAPH_SCHEMA_JSON);
    Result<CreateGraphResponse> rep = session.createGraph(graph);
    if (!rep.isOk()) {
        System.out.println("Failed to create graph: " + rep.getStatusMessage());
        return;
    }
    else {
        System.out.println("Created graph: " + rep.getValue().getGraphId());
    }
    String graphId = rep.getValue().getGraphId();
    System.out.println("GraphId: " + graphId);
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **createGraphRequest** | [**CreateGraphRequest**](CreateGraphRequest.md)|  | |

### Return type

[**Result&lt;CreateGraphResponse&gt;**](CreateGraphResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | successful operation |  -  |
| **400** | BadRequest |  -  |
| **500** | Internal error |  -  |


<a id="BulkLoading"></a>
# **BulkLoading**
> Result&lt;JobResponse&gt; bulkLoading(graphId, schemaMapping)



Create a dataloading job

### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.models.*;

public class Example {
  private static final String MODERN_GRAPH_SCHEMA_JSON = "{\n" +
          "    \"name\": \"modern_graph\",\n" +
          "    \"description\": \"This is a test graph\",\n" +
          "    \"schema\": {\n" +
          "        \"vertex_types\": [\n" +
          "            {\n" +
          "                \"type_name\": \"person\",\n" +
          "                \"properties\": [\n" +
          "                    {\n" +
          "                        \"property_name\": \"id\",\n" +
          "                        \"property_type\": {\"primitive_type\": \"DT_SIGNED_INT64\"},\n" +
          "                    },\n" +
          "                    {\n" +
          "                        \"property_name\": \"name\",\n" +
          "                        \"property_type\": {\"string\": {\"long_text\": \"\"}},\n" +
          "                    },\n" +
          "                    {\n" +
          "                        \"property_name\": \"age\",\n" +
          "                        \"property_type\": {\"primitive_type\": \"DT_SIGNED_INT32\"},\n" +
          "                    },\n" +
          "                ],\n" +
          "                \"primary_keys\": [\"id\"],\n" +
          "            }\n" +
          "        ],\n" +
          "        \"edge_types\": [\n" +
          "            {\n" +
          "                \"type_name\": \"knows\",\n" +
          "                \"vertex_type_pair_relations\": [\n" +
          "                    {\n" +
          "                        \"source_vertex\": \"person\",\n" +
          "                        \"destination_vertex\": \"person\",\n" +
          "                        \"relation\": \"MANY_TO_MANY\",\n" +
          "                    }\n" +
          "                ],\n" +
          "                \"properties\": [\n" +
          "                    {\n" +
          "                        \"property_name\": \"weight\",\n" +
          "                        \"property_type\": {\"primitive_type\": \"DT_DOUBLE\"},\n" +
          "                    }\n" +
          "                ],\n" +
          "                \"primary_keys\": [],\n" +
          "            }\n" +
          "        ],\n" +
          "    },\n" +
          "}";

  private static final String MODERN_GRAPH_BULK_LOADING_JSON = "{\n" +
          "    \"vertex_mappings\": [\n" +
          "        {\n" +
          "            \"type_name\": \"person\",\n" +
          "            \"inputs\": [\"@/tmp/person.csv\"],\n" +
          "            \"column_mappings\": [\n" +
          "                {\"column\": {\"index\": 0, \"name\": \"id\"}, \"property\": \"id\"},\n" +
          "                {\"column\": {\"index\": 1, \"name\": \"name\"}, \"property\": \"name\"},\n" +
          "                {\"column\": {\"index\": 2, \"name\": \"age\"}, \"property\": \"age\"},\n" +
          "            ],\n" +
          "        }\n" +
          "    ],\n" +
          "    \"edge_mappings\": [\n" +
          "        {\n" +
          "            \"type_triplet\": {\n" +
          "                \"edge\": \"knows\",\n" +
          "                \"source_vertex\": \"person\",\n" +
          "                \"destination_vertex\": \"person\",\n" +
          "            },\n" +
          "            \"inputs\": [\n" +
          "                \"@/tmp/person_knows_person.csv\"\n" +
          "            ],\n" +
          "            \"source_vertex_mappings\": [\n" +
          "                {\"column\": {\"index\": 0, \"name\": \"person.id\"}, \"property\": \"id\"}\n" +
          "            ],\n" +
          "            \"destination_vertex_mappings\": [\n" +
          "                {\"column\": {\"index\": 1, \"name\": \"person.id\"}, \"property\": \"id\"}\n" +
          "            ],\n" +
          "            \"column_mappings\": [\n" +
          "                {\"column\": {\"index\": 2, \"name\": \"weight\"}, \"property\": \"weight\"}\n" +
          "            ],\n" +
          "        }\n" +
          "    ],\n" +
          "}";
  public static void main(String[] args) throws IOException {
    // get endpoint from command line
    if (System.getenv("INTERACTIVE_ENDPOINT") == null) {
        System.err.print("INTERACTIVE_ENDPOINT is not set");
        return;
    }
    String endpoint = System.getenv("INTERACTIVE_ENDPOINT");
    Driver driver = Driver.connect(endpoint);
    Session session = driver.session();

    // First create graph
      CreateGraphRequest graph = CreateGraphRequest.fromJson(MODERN_GRAPH_SCHEMA_JSON);
    Result<CreateGraphResponse> rep = session.createGraph(graph);
    if (!rep.isOk()) {
        System.out.println("Failed to create graph: " + rep.getStatusMessage());
        return;
    }
    else {
        System.out.println("Created graph: " + rep.getValue().getGraphId());
    }
    String graphId = rep.getValue().getGraphId();
    SchemaMapping schema = SchemaMapping.fromJson(MODERN_GRAPH_BULK_LOADING_JSON);

    Result<JobResponse> getRes = session.bulkLoading(graphId, schema);
    if (!getRes.isOk()) {
        System.out.println("Failed to bulk loading: " + getRes.getStatusMessage());
        return;
    }
    else {
        System.out.println("Bulk loading job id: " + getRes.getValue().getJobId());
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **graphId** | **String**| The id of graph to do bulk loading. | |
| **schemaMapping** | [**SchemaMapping**](SchemaMapping.md)|  | |

### Return type

[**Result&lt;JobResponse&gt;**](JobResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | successful operation |  -  |

<a id="DeleteGraph"></a>
# **DeleteGraph**
> Result&lt;String&gt; deleteGraph(graphId)



Delete a graph by graph id.

### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;

public class Example {
  public static void main(String[] args) {
    // get endpoint from command line
    if (System.getenv("INTERACTIVE_ENDPOINT") == null) {
        System.err.print("INTERACTIVE_ENDPOINT is not set");
        return;
    }
    String endpoint = System.getenv("INTERACTIVE_ENDPOINT");
    Driver driver = Driver.connect(endpoint);
    Session session = driver.session();

    String graphId = "2"; // Replace with the graph id you want to delete
    Result<String> deleteRes = session.deleteGraph(graphId);
    if (!deleteRes.isOk()) {
        System.out.println("Failed to delete graph: " + deleteRes.getStatusMessage());
        return;
    }
    else {
        System.out.println("Deleted graph: " + deleteRes.getValue());
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **graphId** | **String**| The id of graph to delete | |

### Return type

**Result&lt;String&gt;**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Successful operation |  -  |
| **404** | Not Found |  -  |
| **500** | Internal Error |  -  |

<a id="GetGraphMeta"></a>
# **GetGraphMeta**
> Result&lt;GetGraphResponse&gt; getGraphMeta(graphId)



Get a graph by id

### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;

public class Example {
  public static void main(String[] args) {
    if (System.getenv("INTERACTIVE_ENDPOINT") == null) {
        System.err.print("INTERACTIVE_ENDPOINT is not set");
        return;
    }
    String endpoint = System.getenv("INTERACTIVE_ENDPOINT");
    Driver driver = Driver.connect(endpoint);
    Session session = driver.session();

    Result<GetGraphResponse> getGraphResponseResult = session.getGraphMeta("1");
    if (!getGraphResponseResult.isOk()) {
        System.out.println("Failed to get graph: " + getGraphResponseResult.getStatusMessage());
        return;
    }
    else {
        System.out.println("Got graph: " + getGraphResponseResult.getValue());
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **graphId** | **String**| The id of graph to get | |

### Return type

[**Result&lt;GetGraphResponse&gt;**](GetGraphResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Successful operation |  -  |
| **404** | Not found |  -  |

<a id="GetGraphStatistic"></a>
# **GetGraphStatistic**
> GetGraphStatisticsResponse getGraphStatistic(graphId)



Get the statics info of a graph, including number of vertices for each label, number of edges for each label.

### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.models.GetGraphStatisticsResponse;

public class Example {
  public static void main(String[] args) {
    // get endpoint from command line
    if (System.getenv("INTERACTIVE_ENDPOINT") == null) {
        System.err.print("INTERACTIVE_ENDPOINT is not set");
        return;
    }
    String endpoint = System.getenv("INTERACTIVE_ENDPOINT");
    Driver driver = Driver.connect(endpoint);
    Session session = driver.session();

    Result<GetGraphStatisticsResponse> getRes = session.getGraphStatistics("2");
    if (!getRes.isOk()) {
        System.out.println("Failed to get graph statistics: " + getRes.getStatusMessage());
    } else {
        System.out.println("Got graph statistics: " + getRes.getValue());
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **graphId** | **String**| The id of graph to get statistics | |

### Return type

[**Result&lt;GetGraphStatisticsResponse&gt;**](GetGraphStatisticsResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | successful operation |  -  |
| **500** | Server Internal Error |  -  |
| **404** | Not Found |  -  |
| **503** | Service Unavailable |  -  |

<a id="GetGraphSchema"></a>
# **GetGraphSchema**
> GetGraphSchemaResponse getSchema(graphId)



Get schema by graph id

### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.models.GetGraphSchemaResponse;

public class Example {
  public static void main(String[] args) {
    // get endpoint from command line
    if (System.getenv("INTERACTIVE_ENDPOINT") == null) {
        System.err.print("INTERACTIVE_ENDPOINT is not set");
        return;
    }
    String endpoint = System.getenv("INTERACTIVE_ENDPOINT");
    Driver driver = Driver.connect(endpoint);
    Session session = driver.session();

    Result<GetGraphSchemaResponse> getRes = session.getGraphSchema("2");
    if (!getRes.isOk()) {
        System.out.println("Failed to get graph schema: " + getRes.getStatusMessage());
    } else {
        System.out.println("Got graph schema: " + getRes.getValue());
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **graphId** | **String**| The id of graph to get schema | |

### Return type

[**Result&lt;GetGraphSchemaResponse&gt;**](GetGraphSchemaResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | successful operation |  -  |


