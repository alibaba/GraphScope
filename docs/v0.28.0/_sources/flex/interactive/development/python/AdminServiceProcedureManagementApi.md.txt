# gs_interactive.AdminServiceProcedureManagementApi

All URIs are relative to *{INTERACTIVE_ENDPOINT}*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_procedure**](AdminServiceProcedureManagementApi.md#create_procedure) | **POST** /v1/graph/{graph_id}/procedure | 
[**delete_procedure**](AdminServiceProcedureManagementApi.md#delete_procedure) | **DELETE** /v1/graph/{graph_id}/procedure/{procedure_id} | 
[**get_procedure**](AdminServiceProcedureManagementApi.md#get_procedure) | **GET** /v1/graph/{graph_id}/procedure/{procedure_id} | 
[**list_procedures**](AdminServiceProcedureManagementApi.md#list_procedures) | **GET** /v1/graph/{graph_id}/procedure | 
[**update_procedure**](AdminServiceProcedureManagementApi.md#update_procedure) | **PUT** /v1/graph/{graph_id}/procedure/{procedure_id} | 


# **create_procedure**
> CreateProcedureResponse create_procedure(graph_id, create_procedure_request)



Create a new procedure on a graph

### Example


```python
import gs_interactive
from gs_interactive.models.create_procedure_request import CreateProcedureRequest
from gs_interactive.models.create_procedure_response import CreateProcedureResponse
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
    api_instance = gs_interactive.AdminServiceProcedureManagementApi(api_client)
    graph_id = 'graph_id_example' # str | 
    create_procedure_request = gs_interactive.CreateProcedureRequest() # CreateProcedureRequest | 

    try:
        api_response = api_instance.create_procedure(graph_id, create_procedure_request)
        print("The response of AdminServiceProcedureManagementApi->create_procedure:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceProcedureManagementApi->create_procedure: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **create_procedure_request** | [**CreateProcedureRequest**](CreateProcedureRequest.md)|  | 

### Return type

[**CreateProcedureResponse**](CreateProcedureResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**400** | Bad request |  -  |
**404** | not found |  -  |
**500** | Internal Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_procedure**
> str delete_procedure(graph_id, procedure_id)



Delete a procedure on a graph by name

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
    api_instance = gs_interactive.AdminServiceProcedureManagementApi(api_client)
    graph_id = 'graph_id_example' # str | 
    procedure_id = 'procedure_id_example' # str | 

    try:
        api_response = api_instance.delete_procedure(graph_id, procedure_id)
        print("The response of AdminServiceProcedureManagementApi->delete_procedure:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceProcedureManagementApi->delete_procedure: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **procedure_id** | **str**|  | 

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
**200** | Successful operation |  -  |
**404** | Not Found |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_procedure**
> GetProcedureResponse get_procedure(graph_id, procedure_id)



Get a procedure by name

### Example


```python
import gs_interactive
from gs_interactive.models.get_procedure_response import GetProcedureResponse
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
    api_instance = gs_interactive.AdminServiceProcedureManagementApi(api_client)
    graph_id = 'graph_id_example' # str | 
    procedure_id = 'procedure_id_example' # str | 

    try:
        api_response = api_instance.get_procedure(graph_id, procedure_id)
        print("The response of AdminServiceProcedureManagementApi->get_procedure:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceProcedureManagementApi->get_procedure: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **procedure_id** | **str**|  | 

### Return type

[**GetProcedureResponse**](GetProcedureResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**404** | Not found |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_procedures**
> List[GetProcedureResponse] list_procedures(graph_id)



List all procedures

### Example


```python
import gs_interactive
from gs_interactive.models.get_procedure_response import GetProcedureResponse
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
    api_instance = gs_interactive.AdminServiceProcedureManagementApi(api_client)
    graph_id = 'graph_id_example' # str | 

    try:
        api_response = api_instance.list_procedures(graph_id)
        print("The response of AdminServiceProcedureManagementApi->list_procedures:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceProcedureManagementApi->list_procedures: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 

### Return type

[**List[GetProcedureResponse]**](GetProcedureResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful operation |  -  |
**404** | Not found |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_procedure**
> str update_procedure(graph_id, procedure_id, update_procedure_request=update_procedure_request)



Update procedure on a graph by name

### Example


```python
import gs_interactive
from gs_interactive.models.update_procedure_request import UpdateProcedureRequest
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
    api_instance = gs_interactive.AdminServiceProcedureManagementApi(api_client)
    graph_id = 'graph_id_example' # str | 
    procedure_id = 'procedure_id_example' # str | 
    update_procedure_request = gs_interactive.UpdateProcedureRequest() # UpdateProcedureRequest |  (optional)

    try:
        api_response = api_instance.update_procedure(graph_id, procedure_id, update_procedure_request=update_procedure_request)
        print("The response of AdminServiceProcedureManagementApi->update_procedure:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceProcedureManagementApi->update_procedure: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **procedure_id** | **str**|  | 
 **update_procedure_request** | [**UpdateProcedureRequest**](UpdateProcedureRequest.md)|  | [optional] 

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
**200** | Successful operation |  -  |
**400** | Bad request |  -  |
**404** | Not Found |  -  |
**500** | Internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

