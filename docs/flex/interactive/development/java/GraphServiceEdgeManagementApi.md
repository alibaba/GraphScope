# GraphServiceEdgeManagementApi

All URIs are relative to `${INTERACTIVE_ENDPOINT}`

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**addEdge**](GraphServiceEdgeManagementApi.md#addEdge) | **POST** /v1/graph/{graph_id}/edge | Add edge to the graph |
| [**deleteEdge**](GraphServiceEdgeManagementApi.md#deleteEdge) | **DELETE** /v1/graph/{graph_id}/edge | Remove edge from the graph |
| [**getEdge**](GraphServiceEdgeManagementApi.md#getEdge) | **GET** /v1/graph/{graph_id}/edge | Get the edge&#39;s properties with src and dst vertex primary keys. |
| [**updateEdge**](GraphServiceEdgeManagementApi.md#updateEdge) | **PUT** /v1/graph/{graph_id}/edge | Update edge&#39;s property |


<a id="addEdge"></a>
# **addEdge**
> String addEdge(graphId, edgeRequest)

Add edge to the graph

Add the edge to graph. 

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.GraphServiceEdgeManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("{INTERACTIVE_ENDPOINT}");

    GraphServiceEdgeManagementApi apiInstance = new GraphServiceEdgeManagementApi(defaultClient);
    String graphId = "graphId_example"; // String | 
    EdgeRequest edgeRequest = new EdgeRequest(); // EdgeRequest | 
    try {
      String result = apiInstance.addEdge(graphId, edgeRequest);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling GraphServiceEdgeManagementApi#addEdge");
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
| **graphId** | **String**|  | |
| **edgeRequest** | [**EdgeRequest**](EdgeRequest.md)|  | [optional] |

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
| **200** | Successfully insert the edge |  -  |
| **400** | Invalid input edge |  -  |
| **409** | edge already exists |  -  |
| **500** | Server internal error |  -  |

<a id="deleteEdge"></a>
# **deleteEdge**
> String deleteEdge(graphId, srcLabel, srcPrimaryKeyValue, dstLabel, dstPrimaryKeyValue)

Remove edge from the graph

Remove the edge from current graph. 

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.GraphServiceEdgeManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("{INTERACTIVE_ENDPOINT}");

    GraphServiceEdgeManagementApi apiInstance = new GraphServiceEdgeManagementApi(defaultClient);
    String graphId = "graphId_example"; // String | 
    String srcLabel = "person"; // String | The label name of src vertex.
    Object srcPrimaryKeyValue = 1; // Object | The primary key value of src vertex.
    String dstLabel = "software"; // String | The label name of dst vertex.
    Object dstPrimaryKeyValue = 3; // Object | The primary key value of dst vertex.
    try {
      String result = apiInstance.deleteEdge(graphId, srcLabel, srcPrimaryKeyValue, dstLabel, dstPrimaryKeyValue);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling GraphServiceEdgeManagementApi#deleteEdge");
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
| **graphId** | **String**|  | |
| **srcLabel** | **String**| The label name of src vertex. | |
| **srcPrimaryKeyValue** | [**Object**](.md)| The primary key value of src vertex. | |
| **dstLabel** | **String**| The label name of dst vertex. | |
| **dstPrimaryKeyValue** | [**Object**](.md)| The primary key value of dst vertex. | |

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
| **200** | Successfully delete edge |  -  |
| **400** | Invalid input edge |  -  |
| **404** | Edge not exists or Graph not exits |  -  |
| **500** | Server internal error |  -  |

<a id="getEdge"></a>
# **getEdge**
> EdgeData getEdge(graphId, edgeLabel, srcLabel, srcPrimaryKeyValue, dstLabel, dstPrimaryKeyValue)

Get the edge&#39;s properties with src and dst vertex primary keys.

Get the properties for the specified vertex. 

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.GraphServiceEdgeManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("{INTERACTIVE_ENDPOINT}");

    GraphServiceEdgeManagementApi apiInstance = new GraphServiceEdgeManagementApi(defaultClient);
    String graphId = "graphId_example"; // String | 
    String edgeLabel = "created"; // String | The label name of querying edge.
    String srcLabel = "person"; // String | The label name of src vertex.
    Object srcPrimaryKeyValue = 1; // Object | The primary key value of src vertex.
    String dstLabel = "software"; // String | The label name of dst vertex.
    Object dstPrimaryKeyValue = 3; // Object | The value of dst vertex's primary key
    try {
      EdgeData result = apiInstance.getEdge(graphId, edgeLabel, srcLabel, srcPrimaryKeyValue, dstLabel, dstPrimaryKeyValue);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling GraphServiceEdgeManagementApi#getEdge");
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
| **graphId** | **String**|  | |
| **edgeLabel** | **String**| The label name of querying edge. | |
| **srcLabel** | **String**| The label name of src vertex. | |
| **srcPrimaryKeyValue** | [**Object**](.md)| The primary key value of src vertex. | |
| **dstLabel** | **String**| The label name of dst vertex. | |
| **dstPrimaryKeyValue** | [**Object**](.md)| The value of dst vertex&#39;s primary key | |

### Return type

[**EdgeData**](EdgeData.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Found Edge |  -  |
| **400** | Bad input parameter |  -  |
| **404** | Edge not found or Graph not found |  -  |
| **500** | Server internal error |  -  |

<a id="updateEdge"></a>
# **updateEdge**
> String updateEdge(graphId, edgeRequest)

Update edge&#39;s property

Update the edge on the running graph. 

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.GraphServiceEdgeManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("{INTERACTIVE_ENDPOINT}");

    GraphServiceEdgeManagementApi apiInstance = new GraphServiceEdgeManagementApi(defaultClient);
    String graphId = "graphId_example"; // String | 
    EdgeRequest edgeRequest = new EdgeRequest(); // EdgeRequest | 
    try {
      String result = apiInstance.updateEdge(graphId, edgeRequest);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling GraphServiceEdgeManagementApi#updateEdge");
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
| **graphId** | **String**|  | |
| **edgeRequest** | [**EdgeRequest**](EdgeRequest.md)|  | [optional] |

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
| **200** | Successfully update edge |  -  |
| **400** | Invalid input paramters |  -  |
| **404** | Edge not exists |  -  |
| **500** | Server internal error |  -  |

