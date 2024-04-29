# AdminServiceProcedureManagementApi

All URIs are relative to `${INTERACTIVE_ENDPOINT}`

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createProcedure**](AdminServiceProcedureManagementApi.md#createProcedure) | **POST** /v1/graph/{graph_id}/procedure |  |
| [**deleteProcedure**](AdminServiceProcedureManagementApi.md#deleteProcedure) | **DELETE** /v1/graph/{graph_id}/procedure/{procedure_id} |  |
| [**getProcedure**](AdminServiceProcedureManagementApi.md#getProcedure) | **GET** /v1/graph/{graph_id}/procedure/{procedure_id} |  |
| [**listProcedures**](AdminServiceProcedureManagementApi.md#listProcedures) | **GET** /v1/graph/{graph_id}/procedure |  |
| [**updateProcedure**](AdminServiceProcedureManagementApi.md#updateProcedure) | **PUT** /v1/graph/{graph_id}/procedure/{procedure_id} |  |


<a id="createProcedure"></a>
# **createProcedure**
> CreateProcedureResponse createProcedure(graphId, createProcedureRequest)



Create a new procedure on a graph

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.AdminServiceProcedureManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("{INTERACTIVE_ENDPOINT}");

    AdminServiceProcedureManagementApi apiInstance = new AdminServiceProcedureManagementApi(defaultClient);
    String graphId = "graphId_example"; // String | 
    CreateProcedureRequest createProcedureRequest = new CreateProcedureRequest(); // CreateProcedureRequest | 
    try {
      CreateProcedureResponse result = apiInstance.createProcedure(graphId, createProcedureRequest);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling AdminServiceProcedureManagementApi#createProcedure");
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
| **createProcedureRequest** | [**CreateProcedureRequest**](CreateProcedureRequest.md)|  | |

### Return type

[**CreateProcedureResponse**](CreateProcedureResponse.md)

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

<a id="deleteProcedure"></a>
# **deleteProcedure**
> String deleteProcedure(graphId, procedureId)



Delete a procedure on a graph by name

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.AdminServiceProcedureManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("{INTERACTIVE_ENDPOINT}");

    AdminServiceProcedureManagementApi apiInstance = new AdminServiceProcedureManagementApi(defaultClient);
    String graphId = "graphId_example"; // String | 
    String procedureId = "procedureId_example"; // String | 
    try {
      String result = apiInstance.deleteProcedure(graphId, procedureId);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling AdminServiceProcedureManagementApi#deleteProcedure");
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
| **procedureId** | **String**|  | |

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

<a id="getProcedure"></a>
# **getProcedure**
> GetProcedureResponse getProcedure(graphId, procedureId)



Get a procedure by name

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.AdminServiceProcedureManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("{INTERACTIVE_ENDPOINT}");

    AdminServiceProcedureManagementApi apiInstance = new AdminServiceProcedureManagementApi(defaultClient);
    String graphId = "graphId_example"; // String | 
    String procedureId = "procedureId_example"; // String | 
    try {
      GetProcedureResponse result = apiInstance.getProcedure(graphId, procedureId);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling AdminServiceProcedureManagementApi#getProcedure");
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
| **procedureId** | **String**|  | |

### Return type

[**GetProcedureResponse**](GetProcedureResponse.md)

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

<a id="listProcedures"></a>
# **listProcedures**
> List&lt;GetProcedureResponse&gt; listProcedures(graphId)



List all procedures

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.AdminServiceProcedureManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("{INTERACTIVE_ENDPOINT}");

    AdminServiceProcedureManagementApi apiInstance = new AdminServiceProcedureManagementApi(defaultClient);
    String graphId = "graphId_example"; // String | 
    try {
      List<GetProcedureResponse> result = apiInstance.listProcedures(graphId);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling AdminServiceProcedureManagementApi#listProcedures");
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

### Return type

[**List&lt;GetProcedureResponse&gt;**](GetProcedureResponse.md)

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

<a id="updateProcedure"></a>
# **updateProcedure**
> String updateProcedure(graphId, procedureId, updateProcedureRequest)



Update procedure on a graph by name

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.AdminServiceProcedureManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("{INTERACTIVE_ENDPOINT}");

    AdminServiceProcedureManagementApi apiInstance = new AdminServiceProcedureManagementApi(defaultClient);
    String graphId = "graphId_example"; // String | 
    String procedureId = "procedureId_example"; // String | 
    UpdateProcedureRequest updateProcedureRequest = new UpdateProcedureRequest(); // UpdateProcedureRequest | 
    try {
      String result = apiInstance.updateProcedure(graphId, procedureId, updateProcedureRequest);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling AdminServiceProcedureManagementApi#updateProcedure");
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
| **procedureId** | **String**|  | |
| **updateProcedureRequest** | [**UpdateProcedureRequest**](UpdateProcedureRequest.md)|  | [optional] |

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
| **200** | Successful operation |  -  |
| **400** | Bad request |  -  |
| **404** | Not Found |  -  |
| **500** | Internal error |  -  |

