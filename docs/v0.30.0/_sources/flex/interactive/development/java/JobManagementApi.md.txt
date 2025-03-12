[Result-doc]: ./reference/com/alibaba/graphscope/interactive/client/common/Result.rst

# JobManagementApi

All URIs are relative to *{INTERACTIVE_ADMIN_ENDPOINT}*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**CancelJob**](JobManagementApi.md#CancelJob) | **DELETE** /v1/job/{job_id} | Cancel the job with specified jobId |
| [**GetJobById**](JobManagementApi.md#GetJobById) | **GET** /v1/job/{job_id} | Get the metadata of the job with specified jobId |
| [**ListJobs**](JobManagementApi.md#ListJobs) | **GET** /v1/job | List all jobs(including history jobs) |


<a id="CancelJob"></a>
# **CancelJob**
> [Result][Result-doc]&lt;String&gt; cancelJob(jobId)

See [bulkLoading](GraphManagementApi.md#bulkloading) about how to submit a bulk loading job.


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

<a id="GetJobById"></a>
# **GetJobById**
> [Result][Result-doc]&lt;JobStatus&gt; getJobById(jobId)



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

[Result][Result-doc]&lt;[JobStatus](JobStatus.md)&gt;

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
    Driver driver = Driver.connect();
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

[Result][Result-doc]&lt;List&lt;[JobStatus](JobStatus.md)&gt;&gt;

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | successful operation |  -  |

