# gs_interactive.AdminServiceServiceManagementApi

All URIs are relative to *{INTERACTIVE_ENDPOINT}*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**GetServiceStatus**](ServiceManagementApi.md#GetServiceStatus) | **GET** /v1/service/status | Get the status and metrics of service |
| [**RestartService**](ServiceManagementApi.md#RestartService) | **POST** /v1/service/restart | Restart the query service on the current running graph |
| [**StartService**](ServiceManagementApi.md#StartService) | **POST** /v1/service/start | Start the query service on the specified graph |
| [**StopService**](ServiceManagementApi.md#StopService) | **POST** /v1/service/stop | Stop the query service |


# **get_service_status**
> ServiceStatus get_service_status()



Get service status

### Example


```python
import gs_interactive
from gs_interactive.models.service_status import ServiceStatus
from gs_interactive.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to {INTERACTIVE_ENDPOINT}
# See configuration.py for a list of all supported configuration parameters.
configuration = gs_interactive.Configuration(
    host = "{INTERACTIVE_ENDPOINT}"
)


# Enter a context with an instance of the API client
with gs_interactive.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = gs_interactive.AdminServiceServiceManagementApi(api_client)

    try:
        api_response = api_instance.get_service_status()
        print("The response of AdminServiceServiceManagementApi->get_service_status:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceServiceManagementApi->get_service_status: %s\n" % e)
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
**200** | successful operation |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **restart_service**
> str restart_service()



Start current service

### Example


```python
import gs_interactive
from gs_interactive.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to {INTERACTIVE_ENDPOINT}
# See configuration.py for a list of all supported configuration parameters.
configuration = gs_interactive.Configuration(
    host = "{INTERACTIVE_ENDPOINT}"
)


# Enter a context with an instance of the API client
with gs_interactive.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = gs_interactive.AdminServiceServiceManagementApi(api_client)

    try:
        api_response = api_instance.restart_service()
        print("The response of AdminServiceServiceManagementApi->restart_service:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceServiceManagementApi->restart_service: %s\n" % e)
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
**200** | successful operation |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **start_service**
> str start_service(start_service_request=start_service_request)



Start service on a specified graph

### Example


```python
import gs_interactive
from gs_interactive.models.start_service_request import StartServiceRequest
from gs_interactive.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to {INTERACTIVE_ENDPOINT}
# See configuration.py for a list of all supported configuration parameters.
configuration = gs_interactive.Configuration(
    host = "{INTERACTIVE_ENDPOINT}"
)


# Enter a context with an instance of the API client
with gs_interactive.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = gs_interactive.AdminServiceServiceManagementApi(api_client)
    start_service_request = gs_interactive.StartServiceRequest() # StartServiceRequest | Start service on a specified graph (optional)

    try:
        api_response = api_instance.start_service(start_service_request=start_service_request)
        print("The response of AdminServiceServiceManagementApi->start_service:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceServiceManagementApi->start_service: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **start_service_request** | [**StartServiceRequest**](StartServiceRequest.md)| Start service on a specified graph | [optional] 

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
**200** | successful operation |  -  |
**500** | Internal Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **stop_service**
> str stop_service()



Stop current service

### Example


```python
import gs_interactive
from gs_interactive.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to {INTERACTIVE_ENDPOINT}
# See configuration.py for a list of all supported configuration parameters.
configuration = gs_interactive.Configuration(
    host = "{INTERACTIVE_ENDPOINT}"
)


# Enter a context with an instance of the API client
with gs_interactive.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = gs_interactive.AdminServiceServiceManagementApi(api_client)

    try:
        api_response = api_instance.stop_service()
        print("The response of AdminServiceServiceManagementApi->stop_service:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceServiceManagementApi->stop_service: %s\n" % e)
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
**200** | successful operation |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

