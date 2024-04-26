# AdminServiceServiceManagementApi

All URIs are relative to *https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**getServiceStatus**](AdminServiceServiceManagementApi.md#getServiceStatus) | **GET** /v1/service/status |  |
| [**restartService**](AdminServiceServiceManagementApi.md#restartService) | **POST** /v1/service/restart |  |
| [**startService**](AdminServiceServiceManagementApi.md#startService) | **POST** /v1/service/start |  |
| [**stopService**](AdminServiceServiceManagementApi.md#stopService) | **POST** /v1/service/stop |  |


<a id="getServiceStatus"></a>
# **getServiceStatus**
> ServiceStatus getServiceStatus()



Get service status

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.AdminServiceServiceManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0");

    AdminServiceServiceManagementApi apiInstance = new AdminServiceServiceManagementApi(defaultClient);
    try {
      ServiceStatus result = apiInstance.getServiceStatus();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling AdminServiceServiceManagementApi#getServiceStatus");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

[**ServiceStatus**](ServiceStatus.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | successful operation |  -  |

<a id="restartService"></a>
# **restartService**
> String restartService()



Start current service

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.AdminServiceServiceManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0");

    AdminServiceServiceManagementApi apiInstance = new AdminServiceServiceManagementApi(defaultClient);
    try {
      String result = apiInstance.restartService();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling AdminServiceServiceManagementApi#restartService");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters
This endpoint does not need any parameter.

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
| **200** | successful operation |  -  |

<a id="startService"></a>
# **startService**
> String startService(startServiceRequest)



Start service on a specified graph

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.AdminServiceServiceManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0");

    AdminServiceServiceManagementApi apiInstance = new AdminServiceServiceManagementApi(defaultClient);
    StartServiceRequest startServiceRequest = new StartServiceRequest(); // StartServiceRequest | Start service on a specified graph
    try {
      String result = apiInstance.startService(startServiceRequest);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling AdminServiceServiceManagementApi#startService");
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
| **startServiceRequest** | [**StartServiceRequest**](StartServiceRequest.md)| Start service on a specified graph | [optional] |

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
| **500** | Internal Error |  -  |

<a id="stopService"></a>
# **stopService**
> String stopService()



Stop current service

### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.AdminServiceServiceManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0");

    AdminServiceServiceManagementApi apiInstance = new AdminServiceServiceManagementApi(defaultClient);
    try {
      String result = apiInstance.stopService();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling AdminServiceServiceManagementApi#stopService");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters
This endpoint does not need any parameter.

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
| **200** | successful operation |  -  |

