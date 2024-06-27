# GraphManagementApi

All URIs are relative to *{INTERACTIVE_ENDPOINT}*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**ListGraphs**](GraphManagementApi.md#ListGraphs) | **GET** /v1/graph | List all graphs' metadata |
| [**CreateGraph**](GraphManagementApi.md#CreateGraph) | **POST** /v1/graph | Create a new graph  |
| [**GetGraphMeta**](GraphManagementApi.md#GetGraphMeta) | **GET** /v1/graph/{graph_id} | Get the metadata for a graph identified by the specified graphId |
| [**GetGraphSchema**](GraphManagementApi.md#GetGraphSchema) | **GET** /v1/graph/{graph_id}/schema | Get the schema for a graph identified by the specified graphId |
| [**DeleteGraph**](GraphManagementApi.md#DeleteGraph) | **DELETE** /v1/graph/{graph_id} | Remove the graph identified by the specified graphId |
| [**GetGraphStatistic**](GraphManagementApi.md#GetGraphStatistic) | **GET** /v1/graph/{graph_id}/statistics |Get the statistics for a graph identified by the specified graphId  |
| [**BulkLoading**](GraphManagementApi.md#BulkLoading) | **POST** /v1/graph/{graph_id}/dataloading | Create a bulk loading job for the graph identified by the specified graphId |


# **create_dataloading_job**
> ResultJobResponse create_dataloading_job(graph_id, schema_mapping)



Create a dataloading job

### Example


```python
import gs_interactive
from gs_interactive.models.job_response import JobResponse
from gs_interactive.models.schema_mapping import SchemaMapping
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
    api_instance = gs_interactive.AdminServiceGraphManagementApi(api_client)
    graph_id = 'graph_id_example' # str | The name of graph to do bulk loading.
    schema_mapping = gs_interactive.SchemaMapping() # SchemaMapping | 

    try:
        api_response = api_instance.create_dataloading_job(graph_id, schema_mapping)
        print("The response of AdminServiceGraphManagementApi->create_dataloading_job:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->create_dataloading_job: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**| The name of graph to do bulk loading. | 
 **schema_mapping** | [**SchemaMapping**](SchemaMapping.md)|  | 

### Return type

[**JobResponse**](JobResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_graph**
> CreateGraphResponse create_graph(create_graph_request)



Create a new graph

### Example


```python
import gs_interactive
from gs_interactive.models.create_graph_request import CreateGraphRequest
from gs_interactive.models.create_graph_response import CreateGraphResponse
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
    api_instance = gs_interactive.AdminServiceGraphManagementApi(api_client)
    create_graph_request = gs_interactive.CreateGraphRequest() # CreateGraphRequest | 

    try:
        api_response = api_instance.create_graph(create_graph_request)
        print("The response of AdminServiceGraphManagementApi->create_graph:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->create_graph: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **create_graph_request** | [**CreateGraphRequest**](CreateGraphRequest.md)|  | 

### Return type

[**CreateGraphResponse**](CreateGraphResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**400** | BadRequest |  -  |
**500** | Internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_graph**
> str delete_graph(graph_id)



Delete a graph by name

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
    api_instance = gs_interactive.AdminServiceGraphManagementApi(api_client)
    graph_id = 'graph_id_example' # str | The name of graph to delete

    try:
        api_response = api_instance.delete_graph(graph_id)
        print("The response of AdminServiceGraphManagementApi->delete_graph:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->delete_graph: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**| The name of graph to delete | 

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
**500** | Internal Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_graph**
> GetGraphResponse get_graph(graph_id)



Get a graph by name

### Example


```python
import gs_interactive
from gs_interactive.models.get_graph_response import GetGraphResponse
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
    api_instance = gs_interactive.AdminServiceGraphManagementApi(api_client)
    graph_id = 'graph_id_example' # str | The name of graph to get

    try:
        api_response = api_instance.get_graph(graph_id)
        print("The response of AdminServiceGraphManagementApi->get_graph:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->get_graph: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**| The name of graph to get | 

### Return type

[**GetGraphResponse**](GetGraphResponse.md)

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

# **get_graph_statistic**
> GetGraphStatisticsResponse get_graph_statistic(graph_id)



Get the statics info of a graph, including number of vertices for each label, number of edges for each label.

### Example


```python
import gs_interactive
from gs_interactive.models.get_graph_statistics_response import GetGraphStatisticsResponse
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
    api_instance = gs_interactive.AdminServiceGraphManagementApi(api_client)
    graph_id = 'graph_id_example' # str | The id of graph to get statistics

    try:
        api_response = api_instance.get_graph_statistic(graph_id)
        print("The response of AdminServiceGraphManagementApi->get_graph_statistic:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->get_graph_statistic: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**| The id of graph to get statistics | 

### Return type

[**GetGraphStatisticsResponse**](GetGraphStatisticsResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**500** | Server Internal Error |  -  |
**404** | Not Found |  -  |
**503** | Service Unavailable |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_schema**
> GetGraphSchemaResponse get_schema(graph_id)



Get schema by graph name

### Example


```python
import gs_interactive
from gs_interactive.models.get_graph_schema_response import GetGraphSchemaResponse
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
    api_instance = gs_interactive.AdminServiceGraphManagementApi(api_client)
    graph_id = 'graph_id_example' # str | The name of graph to delete

    try:
        api_response = api_instance.get_schema(graph_id)
        print("The response of AdminServiceGraphManagementApi->get_schema:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->get_schema: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**| The name of graph to delete | 

### Return type

[**GetGraphSchemaResponse**](GetGraphSchemaResponse.md)

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

# **list_graphs**
> List[GetGraphResponse] list_graphs()



List all graphs

### Example


```python
import gs_interactive
from gs_interactive.models.get_graph_response import GetGraphResponse
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
    api_instance = gs_interactive.AdminServiceGraphManagementApi(api_client)

    try:
        api_response = api_instance.list_graphs()
        print("The response of AdminServiceGraphManagementApi->list_graphs:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->list_graphs: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**List[GetGraphResponse]**](GetGraphResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful operation |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

