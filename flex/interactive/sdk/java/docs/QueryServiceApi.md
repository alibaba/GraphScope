# QueryServiceApi

All URIs are relative to *https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**procCall**](QueryServiceApi.md#procCall) | **POST** /v1/graph/{graph_name}/query | run queries on graph |


<a id="procCall"></a>
# **procCall**
> procCall(graphName, body)

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
    String graphName = "graphName_example"; // String | 
    String body = {"type":"native/proc_call/adhoc","payload":"binary bytes\n"}; // String | 
    try {
      apiInstance.procCall(graphName, body);
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
| **graphName** | **String**|  | |
| **body** | **String**|  | [optional] |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: text/plain

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Successfully runned. Empty if failed? |  -  |
| **500** | Server internal error |  -  |

