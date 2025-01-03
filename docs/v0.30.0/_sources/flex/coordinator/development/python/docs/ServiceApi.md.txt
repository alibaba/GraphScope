# graphscope.flex.rest.ServiceApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_service_status_by_id**](ServiceApi.md#get_service_status_by_id) | **GET** /api/v1/graph/{graph_id}/service | 
[**list_service_status**](ServiceApi.md#list_service_status) | **GET** /api/v1/service | 
[**restart_service**](ServiceApi.md#restart_service) | **POST** /api/v1/service/restart | 
[**start_service**](ServiceApi.md#start_service) | **POST** /api/v1/service/start | 
[**stop_service**](ServiceApi.md#stop_service) | **POST** /api/v1/service/stop | 


# **get_service_status_by_id**
> ServiceStatus get_service_status_by_id(graph_id)



Get service status by graph ID

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.service_status import ServiceStatus
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
    api_instance = graphscope.flex.rest.ServiceApi(api_client)
    graph_id = 'graph_id_example' # str | 

    try:
        api_response = api_instance.get_service_status_by_id(graph_id)
        print("The response of ServiceApi->get_service_status_by_id:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ServiceApi->get_service_status_by_id: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 

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
**200** | Successfully returned the service status |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_service_status**
> List[ServiceStatus] list_service_status()



List all service status

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.service_status import ServiceStatus
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
    api_instance = graphscope.flex.rest.ServiceApi(api_client)

    try:
        api_response = api_instance.list_service_status()
        print("The response of ServiceApi->list_service_status:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ServiceApi->list_service_status: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**List[ServiceStatus]**](ServiceStatus.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully returned all service status |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **restart_service**
> str restart_service()



Restart current service

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
    api_instance = graphscope.flex.rest.ServiceApi(api_client)

    try:
        api_response = api_instance.restart_service()
        print("The response of ServiceApi->restart_service:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ServiceApi->restart_service: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

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
**200** | Successfully restarted the service |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **start_service**
> str start_service(start_service_request=start_service_request)



Start service

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.start_service_request import StartServiceRequest
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
    api_instance = graphscope.flex.rest.ServiceApi(api_client)
    start_service_request = graphscope.flex.rest.StartServiceRequest() # StartServiceRequest |  (optional)

    try:
        api_response = api_instance.start_service(start_service_request=start_service_request)
        print("The response of ServiceApi->start_service:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ServiceApi->start_service: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **start_service_request** | [**StartServiceRequest**](StartServiceRequest.md)|  | [optional] 

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully started the service |  -  |
**400** | Bad request |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **stop_service**
> str stop_service()



Stop current service

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
    api_instance = graphscope.flex.rest.ServiceApi(api_client)

    try:
        api_response = api_instance.stop_service()
        print("The response of ServiceApi->stop_service:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ServiceApi->stop_service: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

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
**200** | Successfully stopped the service |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

