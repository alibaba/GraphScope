# JobManagementApi

All URIs are relative to *{INTERACTIVE_ENDPOINT}*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**CancellJob**](JobManagementApi.md#CancellJob) | **DELETE** /v1/job/{job_id} |  |
| [**GetJobById**](JobManagementApi.md#GetJobById) | **GET** /v1/job/{job_id} |  |
| [**ListJobs**](JobManagementApi.md#ListJobs) | **GET** /v1/job |  |


<a id="CancellJob"></a>
# **CancellJob**
> Result&lt;String&gt; cancellJob(jobId)

See [bulkLoading](GraphManagementApi.md#bulkloading) about how to submit a bulk loading job.


### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;

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

    String jobId = "2";  // See GraphManagementAPI#bulkLoading about how to submit a bulk loading job
    Result<String> getRes = session.cancelJob(jobId);
    if (!getRes.isOk()) {
        System.out.println("Failed to cancel job: " + getRes.getStatusMessage());
    } else {
        System.out.println("Canceled job: " + getRes.getValue());
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **jobId** | **String**|  | |

### Return type

**Result&lt;String&gt;**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Successful operation |  -  |

<a id="GetJobById"></a>
# **GetJobById**
> Result&lt;JobStatus&gt; getJobById(jobId)



### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;

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

    String jobId = "2";  // See GraphManagementAPI#bulkLoading about how to submit a bulk loading job
    Result<JobStatus> getJobRes = session.getJobStatus(jobId);
    if (!getJobRes.isOk()) {
        System.out.println("Failed to get job status: " + getJobRes.getStatusMessage());
    } else {
        System.out.println("Got job status: " + getJobRes.getValue());
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **jobId** | **String**| The id of the job, returned from POST /v1/graph/{graph_id}/dataloading | |

### Return type

[**Result&lt;JobStatus&gt;**](JobStatus.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | successful operation |  -  |

<a id="ListJobs"></a>
# **ListJobs**
> List&lt;JobStatus&gt; listJobs()



### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;

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

    Result<List<JobStatus>> getJobsRes = session.listJobs();
    if (!getJobsRes.isOk()) {
        System.out.println("Failed to get jobs: " + getJobsRes.getStatusMessage());
    } else {
        System.out.println("Got jobs: " + getJobsRes.getValue());
    }
  }
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

[**Result&lt;List&lt;JobStatus&gt;&gt;**](JobStatus.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | successful operation |  -  |

