# graphscope.flex.rest.DeploymentApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_deployment_info**](DeploymentApi.md#get_deployment_info) | **GET** /api/v1/deployment | 
[**get_deployment_pod_log**](DeploymentApi.md#get_deployment_pod_log) | **GET** /api/v1/deployment/log | 
[**get_deployment_resource_usage**](DeploymentApi.md#get_deployment_resource_usage) | **GET** /api/v1/deployment/resource/usage | 
[**get_deployment_status**](DeploymentApi.md#get_deployment_status) | **GET** /api/v1/deployment/status | 
[**get_storage_usage**](DeploymentApi.md#get_storage_usage) | **GET** /api/v1/deployment/storage/usage | 


# **get_deployment_info**
> RunningDeploymentInfo get_deployment_info()



Deployment information

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.running_deployment_info import RunningDeploymentInfo
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
    api_instance = graphscope.flex.rest.DeploymentApi(api_client)

    try:
        api_response = api_instance.get_deployment_info()
        print("The response of DeploymentApi->get_deployment_info:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DeploymentApi->get_deployment_info: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**RunningDeploymentInfo**](RunningDeploymentInfo.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully returned the deployment information |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_deployment_pod_log**
> GetPodLogResponse get_deployment_pod_log(pod_name, component, from_cache)



[Deprecated] Get kubernetes pod's log

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.get_pod_log_response import GetPodLogResponse
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
    api_instance = graphscope.flex.rest.DeploymentApi(api_client)
    pod_name = 'pod_name_example' # str | 
    component = 'component_example' # str | 
    from_cache = True # bool | 

    try:
        api_response = api_instance.get_deployment_pod_log(pod_name, component, from_cache)
        print("The response of DeploymentApi->get_deployment_pod_log:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DeploymentApi->get_deployment_pod_log: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **pod_name** | **str**|  | 
 **component** | **str**|  | 
 **from_cache** | **bool**|  | 

### Return type

[**GetPodLogResponse**](GetPodLogResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully returned the pod&#39;s log |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_deployment_resource_usage**
> GetResourceUsageResponse get_deployment_resource_usage()



[Deprecated] Get resource usage(cpu/memory) of cluster

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.get_resource_usage_response import GetResourceUsageResponse
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
    api_instance = graphscope.flex.rest.DeploymentApi(api_client)

    try:
        api_response = api_instance.get_deployment_resource_usage()
        print("The response of DeploymentApi->get_deployment_resource_usage:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DeploymentApi->get_deployment_resource_usage: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**GetResourceUsageResponse**](GetResourceUsageResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully returned the resource usage |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_deployment_status**
> RunningDeploymentStatus get_deployment_status()



Get deployment status of cluster

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.running_deployment_status import RunningDeploymentStatus
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
    api_instance = graphscope.flex.rest.DeploymentApi(api_client)

    try:
        api_response = api_instance.get_deployment_status()
        print("The response of DeploymentApi->get_deployment_status:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DeploymentApi->get_deployment_status: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**RunningDeploymentStatus**](RunningDeploymentStatus.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully returned the deployment status |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_storage_usage**
> GetStorageUsageResponse get_storage_usage()



[Deprecated] Get storage usage of Groot

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.get_storage_usage_response import GetStorageUsageResponse
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
    api_instance = graphscope.flex.rest.DeploymentApi(api_client)

    try:
        api_response = api_instance.get_storage_usage()
        print("The response of DeploymentApi->get_storage_usage:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DeploymentApi->get_storage_usage: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**GetStorageUsageResponse**](GetStorageUsageResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully returned the resource usage |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

