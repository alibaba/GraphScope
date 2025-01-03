# graphscope.flex.rest.StoredProcedureApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_stored_procedure**](StoredProcedureApi.md#create_stored_procedure) | **POST** /api/v1/graph/{graph_id}/storedproc | 
[**delete_stored_procedure_by_id**](StoredProcedureApi.md#delete_stored_procedure_by_id) | **DELETE** /api/v1/graph/{graph_id}/storedproc/{stored_procedure_id} | 
[**get_stored_procedure_by_id**](StoredProcedureApi.md#get_stored_procedure_by_id) | **GET** /api/v1/graph/{graph_id}/storedproc/{stored_procedure_id} | 
[**list_stored_procedures**](StoredProcedureApi.md#list_stored_procedures) | **GET** /api/v1/graph/{graph_id}/storedproc | 
[**update_stored_procedure_by_id**](StoredProcedureApi.md#update_stored_procedure_by_id) | **PUT** /api/v1/graph/{graph_id}/storedproc/{stored_procedure_id} | 


# **create_stored_procedure**
> CreateStoredProcResponse create_stored_procedure(graph_id, create_stored_proc_request)



Create a new stored procedure on a certain graph

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.create_stored_proc_request import CreateStoredProcRequest
from graphscope.flex.rest.models.create_stored_proc_response import CreateStoredProcResponse
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
    api_instance = graphscope.flex.rest.StoredProcedureApi(api_client)
    graph_id = 'graph_id_example' # str | 
    create_stored_proc_request = graphscope.flex.rest.CreateStoredProcRequest() # CreateStoredProcRequest | 

    try:
        api_response = api_instance.create_stored_procedure(graph_id, create_stored_proc_request)
        print("The response of StoredProcedureApi->create_stored_procedure:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling StoredProcedureApi->create_stored_procedure: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **create_stored_proc_request** | [**CreateStoredProcRequest**](CreateStoredProcRequest.md)|  | 

### Return type

[**CreateStoredProcResponse**](CreateStoredProcResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully created a stored procedure |  -  |
**400** | Bad request |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_stored_procedure_by_id**
> str delete_stored_procedure_by_id(graph_id, stored_procedure_id)



Delete a stored procedure by ID

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
    api_instance = graphscope.flex.rest.StoredProcedureApi(api_client)
    graph_id = 'graph_id_example' # str | 
    stored_procedure_id = 'stored_procedure_id_example' # str | 

    try:
        api_response = api_instance.delete_stored_procedure_by_id(graph_id, stored_procedure_id)
        print("The response of StoredProcedureApi->delete_stored_procedure_by_id:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling StoredProcedureApi->delete_stored_procedure_by_id: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **stored_procedure_id** | **str**|  | 

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
**200** | Successfully deleted the stored procedure |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_stored_procedure_by_id**
> GetStoredProcResponse get_stored_procedure_by_id(graph_id, stored_procedure_id)



Get a stored procedure by ID

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.get_stored_proc_response import GetStoredProcResponse
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
    api_instance = graphscope.flex.rest.StoredProcedureApi(api_client)
    graph_id = 'graph_id_example' # str | 
    stored_procedure_id = 'stored_procedure_id_example' # str | 

    try:
        api_response = api_instance.get_stored_procedure_by_id(graph_id, stored_procedure_id)
        print("The response of StoredProcedureApi->get_stored_procedure_by_id:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling StoredProcedureApi->get_stored_procedure_by_id: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **stored_procedure_id** | **str**|  | 

### Return type

[**GetStoredProcResponse**](GetStoredProcResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully returned the stored procedure |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_stored_procedures**
> List[GetStoredProcResponse] list_stored_procedures(graph_id)



List all stored procedures on a certain graph

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.get_stored_proc_response import GetStoredProcResponse
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
    api_instance = graphscope.flex.rest.StoredProcedureApi(api_client)
    graph_id = 'graph_id_example' # str | 

    try:
        api_response = api_instance.list_stored_procedures(graph_id)
        print("The response of StoredProcedureApi->list_stored_procedures:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling StoredProcedureApi->list_stored_procedures: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 

### Return type

[**List[GetStoredProcResponse]**](GetStoredProcResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful operation |  -  |
**400** | Bad request |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_stored_procedure_by_id**
> str update_stored_procedure_by_id(graph_id, stored_procedure_id, update_stored_proc_request=update_stored_proc_request)



Update a stored procedure by ID

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.update_stored_proc_request import UpdateStoredProcRequest
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
    api_instance = graphscope.flex.rest.StoredProcedureApi(api_client)
    graph_id = 'graph_id_example' # str | 
    stored_procedure_id = 'stored_procedure_id_example' # str | 
    update_stored_proc_request = graphscope.flex.rest.UpdateStoredProcRequest() # UpdateStoredProcRequest |  (optional)

    try:
        api_response = api_instance.update_stored_procedure_by_id(graph_id, stored_procedure_id, update_stored_proc_request=update_stored_proc_request)
        print("The response of StoredProcedureApi->update_stored_procedure_by_id:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling StoredProcedureApi->update_stored_procedure_by_id: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **stored_procedure_id** | **str**|  | 
 **update_stored_proc_request** | [**UpdateStoredProcRequest**](UpdateStoredProcRequest.md)|  | [optional] 

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
**200** | Successfully updated the stored procedure |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

