# QueryServiceApi

All URIs are relative to *https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**procCall**](QueryServiceApi.md#procCall) | **POST** /v1/graph/{graph_id}/query | run queries on graph |


<a id="procCall"></a>
# **procCall**
> CollectiveResults procCall(graphId, queryRequest)

run queries on graph

After the procedure is created, user can use this API to run the procedure. TODO: a full example cypher-&gt;plan-&gt;json. TODO: make sure typeinfo can be passed. 

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.QueryServiceApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0");

    QueryServiceApi apiInstance = new QueryServiceApi(defaultClient);
    String graphId = "graphId_example"; // String | 
    QueryRequest queryRequest = new QueryRequest(); // QueryRequest | 
    try {
      CollectiveResults result = apiInstance.procCall(graphId, queryRequest);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling QueryServiceApi#procCall");
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
| **queryRequest** | [**QueryRequest**](QueryRequest.md)|  | [optional] |

### Return type

[**CollectiveResults**](CollectiveResults.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Successfully runned. Empty if failed? |  -  |
| **500** | Server internal error |  -  |

