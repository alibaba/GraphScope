[Result-doc]: ./reference/com/alibaba/graphscope/interactive/client/common/Result.rst
# EdgeAPI

All URIs are relative to *{INTERACTIVE_ADMIN_ENDPOINT}*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**addEdge**](EdgeApi.md#addEdge) | **POST** /v1/graph/{graph_id}/edge | Add edge to the graph |
| [**deleteEdge**](EdgeApi.md#deleteEdge) | **DELETE** /v1/graph/{graph_id}/edge | Remove edge from the graph |
| [**getEdge**](EdgeApi.md#getEdge) | **GET** /v1/graph/{graph_id}/edge | Get the edge&#39;s properties with src and dst vertex primary keys. |
| [**updateEdge**](EdgeApi.md#updateEdge) | **PUT** /v1/graph/{graph_id}/edge | Update edge&#39;s property |


<a id="addEdge"></a>
# **addEdge**
> [Result][Result-doc]&lt;String&gt; addEdge(graphId, edgeRequest)

Add edge to the graph. 

See [Creating-Graph](GraphManagementApi.md#creategraph) about how to create a graph. Here we use the default graph(with id 1) for example.

### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.models.*;
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;

public class Example {
  public static void main(String[] args) {
    Driver driver = Driver.connect();
    Session session = driver.session();

    String graphId = "1";
    EdgeRequest edgeRequest3 =
            new EdgeRequest()
                    .srcLabel("person")
                    .dstLabel("person")
                    .edgeLabel("knows")
                    .srcPrimaryKeyValue(2)
                    .dstPrimaryKeyValue(4)
                    .addPropertiesItem(new Property().name("weight").value(9.123));
    EdgeRequest edgeRequest4 =
            new EdgeRequest()
                    .srcLabel("person")
                    .dstLabel("person")
                    .edgeLabel("knows")
                    .srcPrimaryKeyValue(2)
                    .dstPrimaryKeyValue(6)
                    .addPropertiesItem(new Property().name("weight").value(3.233));
    List<EdgeRequest> edgeRequests = new ArrayList<>();
    edgeRequests.add(edgeRequest3);
    edgeRequests.add(edgeRequest4);
    Result<String> addEdgeResponse = session.addEdge(graphId, edgeRequests);    
    if (!addEdgeResponse.isOk()) {
        System.out.println("Failed to create edge: " + addEdgeResponse.getStatusMessage());
    }
    else {
        System.out.println("Create edge response: " + addEdgeResponse.getValue());
    }
    return;
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **graphId** | **String**|  | |
| **edgeRequest** | [**List&lt;EdgeRequest&gt;**](EdgeRequest.md)|  | |

### Return type

[Result][Result-doc]&lt;**String**&gt;

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

<a id="getEdge"></a>
# **getEdge**
> [Result][Result-doc]&lt;EdgeData&gt; getEdge(graphId, edgeLabel, srcLabel, srcPrimaryKeyValue, dstLabel, dstPrimaryKeyValue)

Get the edge&#39;s properties with src and dst vertex primary keys.

### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.models.*;
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;

public class Example {
  public static void main(String[] args) {
    Driver driver = Driver.connect();
    Session session = driver.session();

    String graphId = "1";
    Result<EdgeData> getEdgeResponse =
            session.getEdge(graphId, "knows", "person", 2, "person", 4);
    if (getEdgeResponse.isOk()){
        for (Property property : getEdgeResponse.getValue().getProperties()) {
            if (property.getName().equals("weight")) {
                Double weight = Double.parseDouble(property.getValue().toString());
                assert weight.equals(9.123);
            }
        }
    } else {
        System.out.println("Get edge failed: " + getEdgeResponse.getValue());
    }
    return;
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
> [Result][Result-doc]&lt;String&gt; updateEdge(graphId, edgeRequest)

Update edge&#39;s property


### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.models.*;
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;

public class Example {
  public static void main(String[] args) {
    Driver driver = Driver.connect();
    Session session = driver.session();

    String graphId = "1";
    EdgeRequest updateEdgeRequest =
        new EdgeRequest()
                .srcLabel("person")
                .dstLabel("person")
                .edgeLabel("knows")
                .srcPrimaryKeyValue(2)
                .dstPrimaryKeyValue(4)
                .addPropertiesItem(new Property().name("weight").value(3.0));
    Result<String> updateEdgeResponse = session.updateEdge(graphId, updateEdgeRequest);
    if (!updateEdgeResponse.isOk()) {
        System.out.println("Failed to update edge: " + updateEdgeResponse.getStatusMessage());
    }
    else {
        System.out.println("Update edge response: " + updateEdgeResponse.getValue());
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

[Result][Result-doc]&lt;**String**&gt;

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Successfully update edge |  -  |
| **400** | Invalid input parameters |  -  |
| **404** | Edge not exists |  -  |
| **500** | Server internal error |  -  |

