# GraphServiceVertexManagementApi

All URIs are relative to *https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**addVertex**](GraphServiceVertexManagementApi.md#addVertex) | **POST** /v1/graph/{graph_name}/vertex | Add vertex to the graph |
| [**deleteVertex**](GraphServiceVertexManagementApi.md#deleteVertex) | **DELETE** /v1/graph/{graph_name}/vertex | Remove vertex from the graph |
| [**getVertex**](GraphServiceVertexManagementApi.md#getVertex) | **GET** /v1/graph/{graph_name}/vertex | Get the vertex&#39;s properties with vertex primary key. |
| [**updateVertex**](GraphServiceVertexManagementApi.md#updateVertex) | **PUT** /v1/graph/{graph_name}/vertex | Update vertex&#39;s property |


<a id="addVertex"></a>
# **addVertex**
> String addVertex(graphName, vertexRequest)

Add vertex to the graph

Add the provided vertex to the specified graph. 

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.GraphServiceVertexManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0");

    GraphServiceVertexManagementApi apiInstance = new GraphServiceVertexManagementApi(defaultClient);
    String graphName = "graphName_example"; // String | 
    VertexRequest vertexRequest = new VertexRequest(); // VertexRequest | Add vertex to graph.
    try {
      String result = apiInstance.addVertex(graphName, vertexRequest);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling GraphServiceVertexManagementApi#addVertex");
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
| **graphName** | **String**|  | |
| **vertexRequest** | [**VertexRequest**](VertexRequest.md)| Add vertex to graph. | [optional] |

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
| **200** | Successfully created vertex |  -  |
| **400** | Invalid input vertex |  -  |
| **404** | Graph not found |  -  |
| **409** | Vertex already exists |  -  |
| **500** | Server internal error |  -  |

<a id="deleteVertex"></a>
# **deleteVertex**
> String deleteVertex(graphName, label, primaryKeyValue)

Remove vertex from the graph

Remove the vertex from the specified graph. 

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.GraphServiceVertexManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0");

    GraphServiceVertexManagementApi apiInstance = new GraphServiceVertexManagementApi(defaultClient);
    String graphName = "graphName_example"; // String | 
    String label = "label_example"; // String | The label name of querying vertex.
    Object primaryKeyValue = null; // Object | The value of the querying vertex's primary key
    try {
      String result = apiInstance.deleteVertex(graphName, label, primaryKeyValue);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling GraphServiceVertexManagementApi#deleteVertex");
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
| **graphName** | **String**|  | |
| **label** | **String**| The label name of querying vertex. | |
| **primaryKeyValue** | [**Object**](.md)| The value of the querying vertex&#39;s primary key | |

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
| **200** | Successfully delete vertex |  -  |
| **400** | Invalid input vertex |  -  |
| **404** | Vertex not exists or Graph not exits. |  -  |
| **500** | Server internal error |  -  |

<a id="getVertex"></a>
# **getVertex**
> VertexData getVertex(graphName, label, primaryKeyValue)

Get the vertex&#39;s properties with vertex primary key.

Get the properties for the specified vertex. example: &#x60;&#x60;&#x60;http GET /endpoint?param1&#x3D;value1&amp;param2&#x3D;value2 HTTP/1.1 Host: example.com &#x60;&#x60;&#x60; 

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.GraphServiceVertexManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0");

    GraphServiceVertexManagementApi apiInstance = new GraphServiceVertexManagementApi(defaultClient);
    String graphName = "graphName_example"; // String | The name of the graph
    String label = "label_example"; // String | The label name of querying vertex.
    Object primaryKeyValue = null; // Object | The primary key value of querying vertex.
    try {
      VertexData result = apiInstance.getVertex(graphName, label, primaryKeyValue);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling GraphServiceVertexManagementApi#getVertex");
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
| **graphName** | **String**| The name of the graph | |
| **label** | **String**| The label name of querying vertex. | |
| **primaryKeyValue** | [**Object**](.md)| The primary key value of querying vertex. | |

### Return type

[**VertexData**](VertexData.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Found vertex |  -  |
| **400** | Bad input parameter |  -  |
| **404** | Vertex not found or graph not found |  -  |
| **500** | Server internal error |  -  |

<a id="updateVertex"></a>
# **updateVertex**
> String updateVertex(graphName, vertexRequest)

Update vertex&#39;s property

Remove the vertex from the specified graph. 

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.GraphServiceVertexManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0");

    GraphServiceVertexManagementApi apiInstance = new GraphServiceVertexManagementApi(defaultClient);
    String graphName = "graphName_example"; // String | 
    VertexRequest vertexRequest = new VertexRequest(); // VertexRequest | 
    try {
      String result = apiInstance.updateVertex(graphName, vertexRequest);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling GraphServiceVertexManagementApi#updateVertex");
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
| **graphName** | **String**|  | |
| **vertexRequest** | [**VertexRequest**](VertexRequest.md)|  | [optional] |

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
| **200** | Successfully update vertex |  -  |
| **400** | Invalid input paramters |  -  |
| **404** | Vertex not exists |  -  |
| **500** | Server internal error |  -  |

