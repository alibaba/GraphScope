# gs_interactive.AdminServiceJobManagementApi

All URIs are relative to *{INTERACTIVE_ADMIN_ENDPOINT}*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**CancelJob**](JobManagementApi.md#CancelJob) | **DELETE** /v1/job/{job_id} | Cancell the job with specified jobId |
| [**GetJobById**](JobManagementApi.md#GetJobById) | **GET** /v1/job/{job_id} | Get the metadata of the job with specified jobId |
| [**ListJobs**](JobManagementApi.md#ListJobs) | **GET** /v1/job | List all jobs(including history jobs) |

# **CancelJob**
> [Result](./result.rst)[str] cancel_job(job_id)



### Example


```python
from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

driver = Driver()
sess = driver.session()

# loading_resp = sess.bulk_loading(graph_id, bulk_load_request)
# job_id = resp.get_value().job_id
resp = sess.cancel_job(job_id)
assert resp.is_ok()
print("cancel job resp", resp)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **job_id** | **str**|  | 

### Return type

[Result](./result.rst)[**str**]

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful operation |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

# **GetJobById**
> [Result](./result.rst)[JobStatus] get_job(job_id)



### Example


```python
resp = sess.get_job(job_id)
assert resp.is_ok()
status = resp.get_value().status
print("job status: ", status)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **job_id** | **str**| The id of the job, returned from POST /v1/graph/{graph_id}/dataloading | 

### Return type

[Result](./result.rst)[[**JobStatus**](JobStatus.md)]

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

# **ListJobs**
> [Result](./result.rst)[List[JobStatus]] list_jobs()



### Example


```python
resp = sess.list_jobs(job_id)
assert resp.is_ok()
print("list job resp: ", resp)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[Result](./result.rst)[[**List[JobStatus]**](JobStatus.md)]

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

