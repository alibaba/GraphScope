# QueryServiceApi

All URIs are relative to *{INTERACTIVE_ENDPOINT}*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**CallProcedure**](QueryServiceApi.md#CallProcedure) | **POST** /v1/graph/{graph_id}/query | run queries on graph |
| [**CallProcedureOnCurrentGraph**](QueryServiceApi.md#CallProcedureOnCurrentGraph) | **POST** /v1/graph/current/query | run queries on the running graph |


<a id="CallProcedure"></a>
# **CallProcedure**
> Result&lt;IrResult.CollectiveResults&gt; callProcedure(graphId, request)

Submit procedure call queries to the specified graph.
The output format for the query is define by the [results.proto](https://github.com/alibaba/GraphScope/blob/main/interactive_engine/executor/ir/proto/results.proto).

For the creation of stored procedure please refer to [CypherStoredProcedure](../cypher_procedure.md) and [CppStoredProcedure](../cpp_procedure.md).

### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.ApiClient;
import com.alibaba.graphscope.interactive.ApiException;
import com.alibaba.graphscope.interactive.Configuration;
import com.alibaba.graphscope.interactive.models.*;
import com.alibaba.graphscope.interactive.api.QueryServiceApi;

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

    //Create the graph and register procedure first, and start service on this graph.
    String graphId = "2";
    String procedureId = "testProcedure";
    QueryRequest request = new QueryRequest();
    request.setQueryName(procedureId);
    request.addArgumentsItem(
            new TypedValue()
                    .value(1)
                    .type(
                            new GSDataType(
                                    new PrimitiveType()
                                            .primitiveType(
                                                    PrimitiveType.PrimitiveTypeEnum
                                                            .SIGNED_INT32))));
    Result<IrResult.CollectiveResults> queryRes = session.callProcedure(graphId, request);
    if (!queryRes.isOk()) {
        System.out.println("Failed to call procedure: " + queryRes.getStatusMessage());
        return;
    } else {
        System.out.println("Called procedure: " + queryRes.getValue());
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **graphId** | **String**|  | |
| **body** | **QueryRequest**|  | [optional] |

### Return type

**Result&lt;IrResult.CollectiveResults&gt;**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: text/plain
 - **Accept**: text/plain

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Successfully runned. Empty if failed? |  -  |
| **500** | Server internal error |  -  |

<a id="CallProcedureOnCurrentGraph"></a>
# **CallProcedureOnCurrentGraph**
> Result&lt;IrResult.CollectiveResults&gt; callProcedure(body)

Submit a query to the running graph. 

### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.ApiClient;
import com.alibaba.graphscope.interactive.ApiException;
import com.alibaba.graphscope.interactive.Configuration;
import com.alibaba.graphscope.interactive.models.*;
import com.alibaba.graphscope.interactive.api.QueryServiceApi;

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

    //Create the graph and register procedure first, and start service on this graph.
    String procedureId = "testProcedure";
    QueryRequest request = new QueryRequest();
    request.setQueryName(procedureId);
    request.addArgumentsItem(
            new TypedValue()
                    .value(1)
                    .type(
                            new GSDataType(
                                    new PrimitiveType()
                                            .primitiveType(
                                                    PrimitiveType.PrimitiveTypeEnum
                                                            .SIGNED_INT32))));
    // Note that graph id is not specified, will try to call the procedure on the current running graph, if exits.
    Result<IrResult.CollectiveResults> queryRes = session.callProcedure( request);
    if (!queryRes.isOk()) {
        System.out.println("Failed to call procedure: " + queryRes.getStatusMessage());
        return;
    } else {
        System.out.println("Called procedure: " + queryRes.getValue());
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **body** | **byte[]**|  | [optional] |

### Return type

**Result&lt;IrResult.CollectiveResults&gt;**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: text/plain
 - **Accept**: text/plain

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Successfully runned. Empty if failed? |  -  |
| **500** | Server internal error |  -  |

