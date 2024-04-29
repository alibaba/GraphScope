# interactive_sdk.AdminServiceServiceManagementApi

All URIs are relative to `${INTERACTIVE_ENDPOINT}`

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_service_status**](AdminServiceServiceManagementApi.md#get_service_status) | **GET** /v1/service/status | 
[**restart_service**](AdminServiceServiceManagementApi.md#restart_service) | **POST** /v1/service/restart | 
[**start_service**](AdminServiceServiceManagementApi.md#start_service) | **POST** /v1/service/start | 
[**stop_service**](AdminServiceServiceManagementApi.md#stop_service) | **POST** /v1/service/stop | 


# **get_service_status**
> ServiceStatus get_service_status()



Get service status

### Example


```python
import time
import os
import interactive_sdk
from interactive_sdk.models.service_status import ServiceStatus
from interactive_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to {INTERACTIVE_ENDPOINT}
# See configuration.py for a list of all supported configuration parameters.
configuration = interactive_sdk.Configuration(
    host = "{INTERACTIVE_ENDPOINT}"
)


# Enter a context with an instance of the API client
with interactive_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = interactive_sdk.AdminServiceServiceManagementApi(api_client)

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
import time
import os
import interactive_sdk
from interactive_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to {INTERACTIVE_ENDPOINT}
# See configuration.py for a list of all supported configuration parameters.
configuration = interactive_sdk.Configuration(
    host = "{INTERACTIVE_ENDPOINT}"
)


# Enter a context with an instance of the API client
with interactive_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = interactive_sdk.AdminServiceServiceManagementApi(api_client)

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
import time
import os
import interactive_sdk
from interactive_sdk.models.start_service_request import StartServiceRequest
from interactive_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to {INTERACTIVE_ENDPOINT}
# See configuration.py for a list of all supported configuration parameters.
configuration = interactive_sdk.Configuration(
    host = "{INTERACTIVE_ENDPOINT}"
)


# Enter a context with an instance of the API client
with interactive_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = interactive_sdk.AdminServiceServiceManagementApi(api_client)
    start_service_request = interactive_sdk.StartServiceRequest() # StartServiceRequest | Start service on a specified graph (optional)

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
import time
import os
import interactive_sdk
from interactive_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to {INTERACTIVE_ENDPOINT}
# See configuration.py for a list of all supported configuration parameters.
configuration = interactive_sdk.Configuration(
    host = "{INTERACTIVE_ENDPOINT}"
)


# Enter a context with an instance of the API client
with interactive_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = interactive_sdk.AdminServiceServiceManagementApi(api_client)

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

