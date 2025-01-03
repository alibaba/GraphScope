# graphscope.flex.rest.JobApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**delete_job_by_id**](JobApi.md#delete_job_by_id) | **DELETE** /api/v1/job/{job_id} | 
[**get_dataloading_job_config**](JobApi.md#get_dataloading_job_config) | **POST** /api/v1/graph/{graph_id}/dataloading/config | 
[**get_job_by_id**](JobApi.md#get_job_by_id) | **GET** /api/v1/job/{job_id} | 
[**list_jobs**](JobApi.md#list_jobs) | **GET** /api/v1/job | 
[**submit_dataloading_job**](JobApi.md#submit_dataloading_job) | **POST** /api/v1/graph/{graph_id}/dataloading | 


# **delete_job_by_id**
> str delete_job_by_id(job_id, delete_scheduler=delete_scheduler)



Delete job by ID

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = graphscope.flex.rest.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with graphscope.flex.rest.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = graphscope.flex.rest.JobApi(api_client)
    job_id = 'job_id_example' # str | 
    delete_scheduler = True # bool |  (optional)

    try:
        api_response = api_instance.delete_job_by_id(job_id, delete_scheduler=delete_scheduler)
        print("The response of JobApi->delete_job_by_id:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling JobApi->delete_job_by_id: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **job_id** | **str**|  | 
 **delete_scheduler** | **bool**|  | [optional] 

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfuly cancelled the job |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_dataloading_job_config**
> DataloadingMRJobConfig get_dataloading_job_config(graph_id, dataloading_job_config)



Post to get the data loading configuration for MapReduce Task

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.dataloading_job_config import DataloadingJobConfig
from graphscope.flex.rest.models.dataloading_mr_job_config import DataloadingMRJobConfig
from graphscope.flex.rest.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = graphscope.flex.rest.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with graphscope.flex.rest.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = graphscope.flex.rest.JobApi(api_client)
    graph_id = 'graph_id_example' # str | 
    dataloading_job_config = graphscope.flex.rest.DataloadingJobConfig() # DataloadingJobConfig | 

    try:
        api_response = api_instance.get_dataloading_job_config(graph_id, dataloading_job_config)
        print("The response of JobApi->get_dataloading_job_config:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling JobApi->get_dataloading_job_config: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **dataloading_job_config** | [**DataloadingJobConfig**](DataloadingJobConfig.md)|  | 

### Return type

[**DataloadingMRJobConfig**](DataloadingMRJobConfig.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully returned the job configuration for MapReduce Task |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_job_by_id**
> JobStatus get_job_by_id(job_id)



Get job status by ID

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.job_status import JobStatus
from graphscope.flex.rest.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = graphscope.flex.rest.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with graphscope.flex.rest.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = graphscope.flex.rest.JobApi(api_client)
    job_id = 'job_id_example' # str | 

    try:
        api_response = api_instance.get_job_by_id(job_id)
        print("The response of JobApi->get_job_by_id:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling JobApi->get_job_by_id: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **job_id** | **str**|  | 

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
**200** | Successfully returned the job status |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_jobs**
> List[JobStatus] list_jobs()



List all jobs

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.job_status import JobStatus
from graphscope.flex.rest.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = graphscope.flex.rest.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with graphscope.flex.rest.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = graphscope.flex.rest.JobApi(api_client)

    try:
        api_response = api_instance.list_jobs()
        print("The response of JobApi->list_jobs:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling JobApi->list_jobs: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**List[JobStatus]**](JobStatus.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful returned all the jobs |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **submit_dataloading_job**
> CreateDataloadingJobResponse submit_dataloading_job(graph_id, dataloading_job_config)



Submit a dataloading job

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.create_dataloading_job_response import CreateDataloadingJobResponse
from graphscope.flex.rest.models.dataloading_job_config import DataloadingJobConfig
from graphscope.flex.rest.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = graphscope.flex.rest.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with graphscope.flex.rest.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = graphscope.flex.rest.JobApi(api_client)
    graph_id = 'graph_id_example' # str | 
    dataloading_job_config = graphscope.flex.rest.DataloadingJobConfig() # DataloadingJobConfig | 

    try:
        api_response = api_instance.submit_dataloading_job(graph_id, dataloading_job_config)
        print("The response of JobApi->submit_dataloading_job:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling JobApi->submit_dataloading_job: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **dataloading_job_config** | [**DataloadingJobConfig**](DataloadingJobConfig.md)|  | 

### Return type

[**CreateDataloadingJobResponse**](CreateDataloadingJobResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully submitted the job |  -  |
**400** | Bad request |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

