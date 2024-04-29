# AdminServiceJobManagementApi

All URIs are relative to `${INTERACTIVE_ENDPOINT}`

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**deleteJobById**](AdminServiceJobManagementApi.md#deleteJobById) | **DELETE** /v1/job/{job_id} |  |
| [**getJobById**](AdminServiceJobManagementApi.md#getJobById) | **GET** /v1/job/{job_id} |  |
| [**listJobs**](AdminServiceJobManagementApi.md#listJobs) | **GET** /v1/job |  |


<a id="deleteJobById"></a>
# **deleteJobById**
> String deleteJobById(jobId)



### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.AdminServiceJobManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("{INTERACTIVE_ENDPOINT}");

    AdminServiceJobManagementApi apiInstance = new AdminServiceJobManagementApi(defaultClient);
    String jobId = "jobId_example"; // String | 
    try {
      String result = apiInstance.deleteJobById(jobId);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling AdminServiceJobManagementApi#deleteJobById");
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
| **jobId** | **String**|  | |

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

<a id="getJobById"></a>
# **getJobById**
> JobStatus getJobById(jobId)



### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.AdminServiceJobManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("{INTERACTIVE_ENDPOINT}");

    AdminServiceJobManagementApi apiInstance = new AdminServiceJobManagementApi(defaultClient);
    String jobId = "jobId_example"; // String | The id of the job, returned from POST /v1/graph/{graph_id}/dataloading
    try {
      JobStatus result = apiInstance.getJobById(jobId);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling AdminServiceJobManagementApi#getJobById");
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
| **jobId** | **String**| The id of the job, returned from POST /v1/graph/{graph_id}/dataloading | |

### Return type

[**JobStatus**](JobStatus.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | successful operation |  -  |

<a id="listJobs"></a>
# **listJobs**
> List&lt;JobStatus&gt; listJobs()



### Example
```java
// Import classes:
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.models.*;
import com.alibaba.graphscope.interactive.AdminServiceJobManagementApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("{INTERACTIVE_ENDPOINT}");

    AdminServiceJobManagementApi apiInstance = new AdminServiceJobManagementApi(defaultClient);
    try {
      List<JobStatus> result = apiInstance.listJobs();
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling AdminServiceJobManagementApi#listJobs");
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

[**List&lt;JobStatus&gt;**](JobStatus.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | successful operation |  -  |

