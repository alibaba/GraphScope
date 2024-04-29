# AdminServiceGraphManagementApi

All URIs are relative to *https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createDataloadingJob**](AdminServiceGraphManagementApi.md#createDataloadingJob) | **POST** /v1/graph/{graph_id}/dataloading |  |
| [**createGraph**](AdminServiceGraphManagementApi.md#createGraph) | **POST** /v1/graph |  |
| [**deleteGraph**](AdminServiceGraphManagementApi.md#deleteGraph) | **DELETE** /v1/graph/{graph_id} |  |
| [**getGraph**](AdminServiceGraphManagementApi.md#getGraph) | **GET** /v1/graph/{graph_id} |  |
| [**getSchema**](AdminServiceGraphManagementApi.md#getSchema) | **GET** /v1/graph/{graph_id}/schema |  |
| [**listGraphs**](AdminServiceGraphManagementApi.md#listGraphs) | **GET** /v1/graph |  |


<a id="createDataloadingJob"></a>
# **createDataloadingJob**
> JobResponse createDataloadingJob(graphId, schemaMapping)



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
    String graphId = "graphId_example"; // String | The name of graph to do bulk loading.
    SchemaMapping schemaMapping = new SchemaMapping(); // SchemaMapping | 
    try {
      JobResponse result = apiInstance.createDataloadingJob(graphId, schemaMapping);
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
| **graphId** | **String**| The name of graph to do bulk loading. | |
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
> CreateGraphResponse createGraph(createGraphRequest)



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
    CreateGraphRequest createGraphRequest = new CreateGraphRequest(); // CreateGraphRequest | 
    try {
      CreateGraphResponse result = apiInstance.createGraph(createGraphRequest);
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
| **createGraphRequest** | [**CreateGraphRequest**](CreateGraphRequest.md)|  | |

### Return type

[**CreateGraphResponse**](CreateGraphResponse.md)

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

<a id="deleteGraph"></a>
# **deleteGraph**
> String deleteGraph(graphId)



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
    String graphId = "graphId_example"; // String | The name of graph to delete
    try {
      String result = apiInstance.deleteGraph(graphId);
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
| **graphId** | **String**| The name of graph to delete | |

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
| **404** | Not Found |  -  |
| **500** | Internal Error |  -  |

<a id="getGraph"></a>
# **getGraph**
> GetGraphResponse getGraph(graphId)



Get a graph by name

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
    String graphId = "graphId_example"; // String | The name of graph to get
    try {
      GetGraphResponse result = apiInstance.getGraph(graphId);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling AdminServiceGraphManagementApi#getGraph");
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
| **graphId** | **String**| The name of graph to get | |

### Return type

[**GetGraphResponse**](GetGraphResponse.md)

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

<a id="getSchema"></a>
# **getSchema**
> GetGraphSchemaResponse getSchema(graphId)



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
    String graphId = "graphId_example"; // String | The name of graph to delete
    try {
      GetGraphSchemaResponse result = apiInstance.getSchema(graphId);
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
| **graphId** | **String**| The name of graph to delete | |

### Return type

[**GetGraphSchemaResponse**](GetGraphSchemaResponse.md)

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
> List&lt;GetGraphResponse&gt; listGraphs()



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
      List<GetGraphResponse> result = apiInstance.listGraphs();
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

[**List&lt;GetGraphResponse&gt;**](GetGraphResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Successful operation |  -  |

