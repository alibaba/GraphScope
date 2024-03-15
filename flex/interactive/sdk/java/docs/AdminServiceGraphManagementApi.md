# AdminServiceGraphManagementApi

All URIs are relative to *https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createDataloadingJob**](AdminServiceGraphManagementApi.md#createDataloadingJob) | **POST** /v1/graph/{graph_name}/dataloading |  |
| [**createGraph**](AdminServiceGraphManagementApi.md#createGraph) | **POST** /v1/graph |  |
| [**deleteGraph**](AdminServiceGraphManagementApi.md#deleteGraph) | **DELETE** /v1/graph/{graph_name} |  |
| [**getSchema**](AdminServiceGraphManagementApi.md#getSchema) | **GET** /v1/graph/{graph_name}/schema |  |
| [**listGraphs**](AdminServiceGraphManagementApi.md#listGraphs) | **GET** /v1/graph |  |


<a id="createDataloadingJob"></a>
# **createDataloadingJob**
> JobResponse createDataloadingJob(graphName, schemaMapping)



Create a dataloading job

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.AdminServiceGraphManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0");

    AdminServiceGraphManagementApi apiInstance = new AdminServiceGraphManagementApi(defaultClient);
    String graphName = "graphName_example"; // String | The name of graph to do bulk loading.
    SchemaMapping schemaMapping = new SchemaMapping(); // SchemaMapping | 
    try {
      JobResponse result = apiInstance.createDataloadingJob(graphName, schemaMapping);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling AdminServiceGraphManagementApi#createDataloadingJob");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **graphName** | **String**| The name of graph to do bulk loading. | |
| **schemaMapping** | [**SchemaMapping**](SchemaMapping.md)|  | |

### Return type

[**JobResponse**](JobResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | successful operation |  -  |

<a id="createGraph"></a>
# **createGraph**
> String createGraph(graph)



Create a new graph

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.AdminServiceGraphManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0");

    AdminServiceGraphManagementApi apiInstance = new AdminServiceGraphManagementApi(defaultClient);
    Graph graph = new Graph(); // Graph | 
    try {
      String result = apiInstance.createGraph(graph);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling AdminServiceGraphManagementApi#createGraph");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **graph** | [**Graph**](Graph.md)|  | |

### Return type

**String**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | successful operation |  -  |

<a id="deleteGraph"></a>
# **deleteGraph**
> String deleteGraph(graphName)



Delete a graph by name

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.AdminServiceGraphManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0");

    AdminServiceGraphManagementApi apiInstance = new AdminServiceGraphManagementApi(defaultClient);
    String graphName = "graphName_example"; // String | The name of graph to delete
    try {
      String result = apiInstance.deleteGraph(graphName);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling AdminServiceGraphManagementApi#deleteGraph");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **graphName** | **String**| The name of graph to delete | |

### Return type

**String**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Successful operation |  -  |

<a id="getSchema"></a>
# **getSchema**
> GraphSchema getSchema(graphName)



Get schema by graph name

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.AdminServiceGraphManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0");

    AdminServiceGraphManagementApi apiInstance = new AdminServiceGraphManagementApi(defaultClient);
    String graphName = "graphName_example"; // String | The name of graph to delete
    try {
      GraphSchema result = apiInstance.getSchema(graphName);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling AdminServiceGraphManagementApi#getSchema");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **graphName** | **String**| The name of graph to delete | |

### Return type

[**GraphSchema**](GraphSchema.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | successful operation |  -  |

<a id="listGraphs"></a>
# **listGraphs**
> List&lt;Graph&gt; listGraphs()



List all graphs

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.AdminServiceGraphManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0");

    AdminServiceGraphManagementApi apiInstance = new AdminServiceGraphManagementApi(defaultClient);
    try {
      List<Graph> result = apiInstance.listGraphs();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling AdminServiceGraphManagementApi#listGraphs");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

[**List&lt;Graph&gt;**](Graph.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Successful operation |  -  |

