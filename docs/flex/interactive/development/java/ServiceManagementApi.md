[Result-doc]: ./reference/com/alibaba/graphscope/interactive/client/common/Result.rst

# ServiceManagementApi

All URIs are relative to *{INTERACTIVE_ADMIN_ENDPOINT}*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**GetServiceStatus**](ServiceManagementApi.md#GetServiceStatus) | **GET** /v1/service/status | Get the status and metrics of service |
| [**RestartService**](ServiceManagementApi.md#RestartService) | **POST** /v1/service/restart | Restart the query service on the current running graph |
| [**StartService**](ServiceManagementApi.md#StartService) | **POST** /v1/service/start | Start the query service on the specified graph |
| [**StopService**](ServiceManagementApi.md#StopService) | **POST** /v1/service/stop | Stop the query service |


<a id="GetServiceStatus"></a>
# **GetServiceStatus**
> [Result][Result-doc]&lt;ServiceStatus&gt; getServiceStatus()



Get service's current status.

### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.models.ServiceStatus;

public class Example {
  public static void main(String[] args) {
    Driver driver = Driver.connect();
    Session session = driver.session();

    //get service status
    Result<ServiceStatus> status = session.getServiceStatus();
    if (!status.isOk()) {
      System.out.println("Failed to get service status: " + status.getStatusMessage());
    } else {
      System.out.println("Service status: " + status.getValue());
    }
  }
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

[Result][Result-doc]&lt;[ServiceStatus](./ServiceStatus.md)&gt;

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | successful operation |  -  |

<a id="RestartService"></a>
# **RestartService**
> [Result][Result-doc]&lt;String&gt; restartService()



Restart query service on current running graph.

### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.models.ServiceStatus;

public class Example {
  public static void main(String[] args) {
    Driver driver = Driver.connect();
    Session session = driver.session();

    Result<String> restart = session.restartService();
    if (!restart.isOk()) {
      System.out.println("Failed to restart service: " + restart.getStatusMessage());
      return;
    } else {
      System.out.println("Service restarted: " + restart.getValue());
    }
  }
}
```

### Parameters
This endpoint does not need any parameter.

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
| **200** | successful operation |  -  |

<a id="StartService"></a>
# **StartService**
> [Result][Result-doc]&lt;String&gt; startService(startServiceRequest)



Start service on a specified graph

### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.models.StartServiceRequest;

public class Example {
  public static void main(String[] args) {
    Driver driver = Driver.connect();
    Session session = driver.session();

    Result<String> start = session.startService(new StartServiceRequest().graphId("1"));
    if (!start.isOk()) {
      System.out.println("Failed to start service: " + start.getStatusMessage());
    } else {
      System.out.println("Service started: " + start.getValue());
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **startServiceRequest** | [**StartServiceRequest**](StartServiceRequest.md)| Start service on a specified graph | [optional] |

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
| **200** | successful operation |  -  |
| **500** | Internal Error |  -  |

<a id="StopService"></a>
# **StopService**
> [Result][Result-doc]&lt;String&gt; stopService()



Stop query service.

### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.models.StartServiceRequest;

public class Example {
  public static void main(String[] args) {
    Driver driver = Driver.connect();
    Session session = driver.session();

    Result<String> stop = session.stopService();
    if (!stop.isOk()) {
      System.out.println("Failed to stop service: " + stop.getStatusMessage());
      return;
    } else {
      System.out.println("Service stopped: " + stop.getValue());
    }
  }
}
```

### Parameters
This endpoint does not need any parameter.

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
| **200** | successful operation |  -  |

