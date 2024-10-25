[Result-doc]: ./reference/com/alibaba/graphscope/interactive/client/common/Result.rst

# ProcedureManagementApi

All URIs are relative to *{INTERACTIVE_ADMIN_ENDPOINT}*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**CreateProcedure**](ProcedureManagementApi.md#CreateProcedure) | **POST** /v1/graph/{graph_id}/procedure | Create a procedure on the specified graph |
| [**DeleteProcedure**](ProcedureManagementApi.md#DeleteProcedure) | **DELETE** /v1/graph/{graph_id}/procedure/{procedure_id} | Delete a procedure on the specified graph |
| [**GetProcedure**](ProcedureManagementApi.md#GetProcedure) | **GET** /v1/graph/{graph_id}/procedure/{procedure_id} | Get the metadata of a procedure on the specified graph |
| [**ListProcedures**](ProcedureManagementApi.md#ListProcedures) | **GET** /v1/graph/{graph_id}/procedure | List all procedures bound to a specified graph |
| [**UpdateProcedure**](ProcedureManagementApi.md#UpdateProcedure) | **PUT** /v1/graph/{graph_id}/procedure/{procedure_id} | Update the metadata of the specified graph |


<a id="CreateProcedure"></a>
# **CreateProcedure**
> [Result][Result-doc]&lt;CreateProcedureResponse&gt; createProcedure(graphId, createProcedureRequest)


Create a new procedure on a graph with give id. 
Both `cypher` and `c++` stored procedures could be registered.
Please refer to [CppStoredProcedure](../stored_procedure/cpp_procedure.md) and [CypherStoredProcedure](../../stored_procedures.md).

### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.models.CreateProcedureRequest;
import com.alibaba.graphscope.interactive.models.CreateProcedureResponse;

public class Example {
  public static void main(String[] args) {
    Driver driver = Driver.connect();
    Session session = driver.session();

    String graphId = "1";
    CreateProcedureRequest procedure =
            new CreateProcedureRequest()
                    .name("testProcedure")
                    .description("a simple test procedure")
                    .query("MATCH(p:person) RETURN COUNT(p);")
                    .type(CreateProcedureRequest.TypeEnum.CYPHER);
    Result<CreateProcedureResponse> resp = session.createProcedure(graphId, procedure);
    if (resp.isOk()) {
        System.out.println("create procedure success");
    } else {
        throw new RuntimeException("create procedure failed: " + resp.getStatusMessage());
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **graphId** | **String**|  | |
| **createProcedureRequest** | [**CreateProcedureRequest**](CreateProcedureRequest.md)|  | |

### Return type

[Result][Result-doc]&lt;[CreateProcedureResponse](CreateProcedureResponse.md)&gt;

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | successful operation |  -  |
| **400** | Bad request |  -  |
| **404** | not found |  -  |
| **500** | Internal Error |  -  |

<a id="DeleteProcedure"></a>
# **DeleteProcedure**
> [Result][Result-doc]&lt;String&gt; deleteProcedure(graphId, procedureId)



Delete a procedure on a graph by id

### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;

public class Example {
  public static void main(String[] args) {
    Driver driver = Driver.connect();
    Session session = driver.session();

    String graphId = "1";
    //delete procedure
    Result<String> deleteRes = session.deleteProcedure(graphId, "testProcedure");
    if (!deleteRes.isOk()) {
      System.out.println("Failed to delete procedure: " + deleteRes.getStatusMessage());
    } else {
      System.out.println("Deleted procedure: " + deleteRes.getValue());
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **graphId** | **String**|  | |
| **procedureId** | **String**|  | |

### Return type

[Result][Result-doc]&lt;String&gt;

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

<a id="GetProcedure"></a>
# **GetProcedure**
> GetProcedureResponse getProcedure(graphId, procedureId)



Get a procedure by id

### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.models.GetProcedureResponse;

public class Example {
  public static void main(String[] args) {
    Driver driver = Driver.connect();
    Session session = driver.session();

    String graphId = "1";
    Result<GetProcedureResponse> getRes = session.getProcedure(graphId, "testProcedure");
    if (!getRes.isOk()) {
      System.out.println("Failed to get procedure: " + getRes.getStatusMessage());
    } else {
      System.out.println("Got procedure: " + getRes.getValue());
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **graphId** | **String**|  | |
| **procedureId** | **String**|  | |

### Return type

[Result][Result-doc]&lt;[GetProcedureResponse](GetProcedureResponse.md)&gt;

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | successful operation |  -  |
| **404** | Not found |  -  |

<a id="ListProcedures"></a>
# **ListProcedures**
> [Result][Result-doc]&lt;List&lt;GetProcedureResponse&gt;&gt; listProcedures(graphId)



List all procedures bound to a graph.

### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.models.GetProcedureResponse;

import java.util.List;

public class Example {
  public static void main(String[] args) {
    Driver driver = Driver.connect();
    Session session = driver.session();

    String graphId = "1";
    Result<List<GetProcedureResponse>> listRes = session.listProcedures(graphId);
    if (!listRes.isOk()) {
      System.out.println("Failed to list procedures: " + listRes.getStatusMessage());
    } else {
      System.out.println("Listed procedures: " + listRes.getValue());
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **graphId** | **String**|  | |

### Return type

[Result][Result-doc]]&lt;List&lt;[GetProcedureResponse](GetProcedureResponse.md)&gt;&gt;

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

<a id="UpdateProcedure"></a>
# **UpdateProcedure**
> String updateProcedure(graphId, procedureId, updateProcedureRequest)



Update the metadata of a procedure, i.e. description. The procedure's query or implementation can not be updated.

### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.models.UpdateProcedureRequest;

import java.util.List;

public class Example {
  public static void main(String[] args) {
    Driver driver = Driver.connect();
    Session session = driver.session();

    String graphId = "1";
    String procedureId = "testProcedure";
    UpdateProcedureRequest updateProcedureRequest = new UpdateProcedureRequest();
    updateProcedureRequest.setDescription("a simple test procedure");
    Result<String> updateRes = session.updateProcedure(graphId, procedureId, updateProcedureRequest);
    if (!updateRes.isOk()) {
      System.out.println("Failed to update procedure: " + updateRes.getStatusMessage());
    } else {
      System.out.println("Updated procedure: " + updateRes.getValue());
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **graphId** | **String**|  | |
| **procedureId** | **String**|  | |
| **updateProcedureRequest** | [**UpdateProcedureRequest**](UpdateProcedureRequest.md)|  | [optional] |

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
| **200** | Successful operation |  -  |
| **400** | Bad request |  -  |
| **404** | Not Found |  -  |
| **500** | Internal error |  -  |

