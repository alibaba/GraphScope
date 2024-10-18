[Result-doc]: ./reference/com/alibaba/graphscope/interactive/client/common/Result.rst

# VertexApi

All URIs are relative to  *{INTERACTIVE_ADMIN_ENDPOINT}*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**addVertex**](VertexApi.md#addVertex) | **POST** /v1/graph/{graph_id}/vertex | Add vertex to the graph |
| [**deleteVertex**](VertexApi.md#deleteVertex) | **DELETE** /v1/graph/{graph_id}/vertex | Remove vertex from the graph |
| [**getVertex**](VertexApi.md#getVertex) | **GET** /v1/graph/{graph_id}/vertex | Get the vertex&#39;s properties with vertex primary key. |
| [**updateVertex**](VertexApi.md#updateVertex) | **PUT** /v1/graph/{graph_id}/vertex | Update vertex&#39;s property |


<a id="addVertex"></a>
# **addVertex**
> [Result][Result-doc]&lt;String&gt; addVertex(graphId, vertexEdgeRequest)

Add vertex to the graph. 

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
    VertexRequest vertexRequest =
        new VertexRequest()
                .label("person")
                .primaryKeyValue(8)
                .addPropertiesItem(new Property().name("name").value("mike"))
                .addPropertiesItem(new Property().name("age").value(12));
    VertexEdgeRequest vertexEdgeRequest =
            new VertexEdgeRequest()
                .addVertexRequestItem(vertexRequest);
    Result<String> addVertexResponse = session.addVertex(graphId, vertexEdgeRequest);
    if (!addVertexResponse.isOk()) {
        System.out.println("Failed to create vertex: " + addVertexResponse.getStatusMessage());
    }
    else {
        System.out.println("Create vertex response: " + addVertexResponse.getValue());
    }
    return;
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **graphId** | **String**|  | |
| **vertexEdgeRequest** | [**VertexEdgeRequest**](VertexEdgeRequest.md)|  | |

### Return type

[Result][Result-doc]&lt;String&gt;

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


<a id="getVertex"></a>
# **getVertex**
> [Result][Result-doc]&lt;VertexData&gt; getVertex(graphId, label, primaryKeyValue)

Get the vertex&#39;s properties with vertex primary key.

Get the properties for the specified vertex. example: &#x60;&#x60;&#x60;http GET /endpoint?param1&#x3D;value1&amp;param2&#x3D;value2 HTTP/1.1 Host: example.com &#x60;&#x60;&#x60; 

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
    Result<VertexData> getVertexResponse = session.getVertex(graphId, "person", 8);
    if (getVertexResponse.isOk()){
        for (Property property : getVertexResponse.getValue().getValues()) {
            if (property.getName().equals("name")) {
                assert property.getValue().equals("mike");
            }
            if (property.getName().equals("age")) {
                // object is Integer
                assert property.getValue().equals("12");
            }
        }
    }
    else {
        System.out.println("Failed to create vertex: " + getVertexResponse.getStatusMessage());
    }
    return ;
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **graphId** | **String**| The id of the graph | |
| **label** | **String**| The label name of querying vertex. | |
| **primaryKeyValue** | [**Object**] | The primary key value of querying vertex. | |

### Return type

[Result][Result-doc]&lt;[VertexData](./VertexData.md)&gt;

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
> [Result][Result-doc]&lt;String&gt; updateVertex(graphId, vertexRequest)

Update the vertex's properties.

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
    VertexRequest updateVertexRequest =
        new VertexRequest()
                .label("person")
                .primaryKeyValue(8)
                .addPropertiesItem(new Property().name("name").value("Cindy"))
                .addPropertiesItem(new Property().name("age").value(24));
    Result<String> updateVertexResponse = session.updateVertex(graphId, updateVertexRequest);
    if (updateVertexResponse.isOk()){
        System.out.println("Successfully updated vertex's property");
    }
    else {
         System.out.println("Fail to update vertex's property" + updateVertexResponse.getStatusMessage());
    }
    return ;
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **graphId** | **String**|  | |
| **vertexRequest** | [**VertexRequest**](VertexRequest.md)|  | [optional] |

### Return type

[Result][Result-doc]&lt;String&gt;

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Successfully update vertex |  -  |
| **400** | Invalid input parameters |  -  |
| **404** | Vertex not exists |  -  |
| **500** | Server internal error |  -  |

