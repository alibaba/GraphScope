# AdminServiceProcedureManagementApi

All URIs are relative to *https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createProcedure**](AdminServiceProcedureManagementApi.md#createProcedure) | **POST** /v1/graph/{graph_name}/procedure |  |
| [**deleteProcedure**](AdminServiceProcedureManagementApi.md#deleteProcedure) | **DELETE** /v1/graph/{graph_name}/procedure/{procedure_name} |  |
| [**getProcedure**](AdminServiceProcedureManagementApi.md#getProcedure) | **GET** /v1/graph/{graph_name}/procedure/{procedure_name} |  |
| [**listProcedures**](AdminServiceProcedureManagementApi.md#listProcedures) | **GET** /v1/graph/{graph_name}/procedure |  |
| [**updateProcedure**](AdminServiceProcedureManagementApi.md#updateProcedure) | **PUT** /v1/graph/{graph_name}/procedure/{procedure_name} |  |


<a id="createProcedure"></a>
# **createProcedure**
> String createProcedure(graphName, procedure)



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
    defaultClient.setBasePath("https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0");

    AdminServiceProcedureManagementApi apiInstance = new AdminServiceProcedureManagementApi(defaultClient);
    String graphName = "graphName_example"; // String | 
    Procedure procedure = new Procedure(); // Procedure | 
    try {
      String result = apiInstance.createProcedure(graphName, procedure);
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
| **graphName** | **String**|  | |
| **procedure** | [**Procedure**](Procedure.md)|  | |

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
| **200** | successful operation |  -  |

<a id="deleteProcedure"></a>
# **deleteProcedure**
> String deleteProcedure(graphName, procedureName)



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
    defaultClient.setBasePath("https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0");

    AdminServiceProcedureManagementApi apiInstance = new AdminServiceProcedureManagementApi(defaultClient);
    String graphName = "graphName_example"; // String | 
    String procedureName = "procedureName_example"; // String | 
    try {
      String result = apiInstance.deleteProcedure(graphName, procedureName);
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
| **graphName** | **String**|  | |
| **procedureName** | **String**|  | |

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

<a id="getProcedure"></a>
# **getProcedure**
> Procedure getProcedure(graphName, procedureName)



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
    defaultClient.setBasePath("https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0");

    AdminServiceProcedureManagementApi apiInstance = new AdminServiceProcedureManagementApi(defaultClient);
    String graphName = "graphName_example"; // String | 
    String procedureName = "procedureName_example"; // String | 
    try {
      Procedure result = apiInstance.getProcedure(graphName, procedureName);
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
| **graphName** | **String**|  | |
| **procedureName** | **String**|  | |

### Return type

[**Procedure**](Procedure.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | successful operation |  -  |

<a id="listProcedures"></a>
# **listProcedures**
> List&lt;Procedure&gt; listProcedures(graphName)



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
    defaultClient.setBasePath("https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0");

    AdminServiceProcedureManagementApi apiInstance = new AdminServiceProcedureManagementApi(defaultClient);
    String graphName = "graphName_example"; // String | 
    try {
      List<Procedure> result = apiInstance.listProcedures(graphName);
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
| **graphName** | **String**|  | |

### Return type

[**List&lt;Procedure&gt;**](Procedure.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Successful operation |  -  |

<a id="updateProcedure"></a>
# **updateProcedure**
> String updateProcedure(graphName, procedureName, procedure)



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
    defaultClient.setBasePath("https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0");

    AdminServiceProcedureManagementApi apiInstance = new AdminServiceProcedureManagementApi(defaultClient);
    String graphName = "graphName_example"; // String | 
    String procedureName = "procedureName_example"; // String | 
    Procedure procedure = new Procedure(); // Procedure | 
    try {
      String result = apiInstance.updateProcedure(graphName, procedureName, procedure);
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
| **graphName** | **String**|  | |
| **procedureName** | **String**|  | |
| **procedure** | [**Procedure**](Procedure.md)|  | [optional] |

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

