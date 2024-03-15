# interactive_sdk.AdminServiceGraphManagementApi

All URIs are relative to *https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_dataloading_job**](AdminServiceGraphManagementApi.md#create_dataloading_job) | **POST** /v1/graph/{graph_name}/dataloading | 
[**create_graph**](AdminServiceGraphManagementApi.md#create_graph) | **POST** /v1/graph | 
[**delete_graph**](AdminServiceGraphManagementApi.md#delete_graph) | **DELETE** /v1/graph/{graph_name} | 
[**get_schema**](AdminServiceGraphManagementApi.md#get_schema) | **GET** /v1/graph/{graph_name}/schema | 
[**list_graphs**](AdminServiceGraphManagementApi.md#list_graphs) | **GET** /v1/graph | 


# **create_dataloading_job**
> JobResponse create_dataloading_job(graph_name, schema_mapping)



Create a dataloading job

### Example


```python
import time
import os
import interactive_sdk
from interactive_sdk.models.job_response import JobResponse
from interactive_sdk.models.schema_mapping import SchemaMapping
from interactive_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0
# See configuration.py for a list of all supported configuration parameters.
configuration = interactive_sdk.Configuration(
    host = "https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0"
)


# Enter a context with an instance of the API client
with interactive_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = interactive_sdk.AdminServiceGraphManagementApi(api_client)
    graph_name = 'graph_name_example' # str | The name of graph to do bulk loading.
    schema_mapping = interactive_sdk.SchemaMapping() # SchemaMapping | 

    try:
        api_response = api_instance.create_dataloading_job(graph_name, schema_mapping)
        print("The response of AdminServiceGraphManagementApi->create_dataloading_job:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->create_dataloading_job: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_name** | **str**| The name of graph to do bulk loading. | 
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
> str create_graph(graph)



Create a new graph

### Example


```python
import time
import os
import interactive_sdk
from interactive_sdk.models.graph import Graph
from interactive_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0
# See configuration.py for a list of all supported configuration parameters.
configuration = interactive_sdk.Configuration(
    host = "https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0"
)


# Enter a context with an instance of the API client
with interactive_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = interactive_sdk.AdminServiceGraphManagementApi(api_client)
    graph = interactive_sdk.Graph() # Graph | 

    try:
        api_response = api_instance.create_graph(graph)
        print("The response of AdminServiceGraphManagementApi->create_graph:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->create_graph: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph** | [**Graph**](Graph.md)|  | 

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

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_graph**
> str delete_graph(graph_name)



Delete a graph by name

### Example


```python
import time
import os
import interactive_sdk
from interactive_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0
# See configuration.py for a list of all supported configuration parameters.
configuration = interactive_sdk.Configuration(
    host = "https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0"
)


# Enter a context with an instance of the API client
with interactive_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = interactive_sdk.AdminServiceGraphManagementApi(api_client)
    graph_name = 'graph_name_example' # str | The name of graph to delete

    try:
        api_response = api_instance.delete_graph(graph_name)
        print("The response of AdminServiceGraphManagementApi->delete_graph:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->delete_graph: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_name** | **str**| The name of graph to delete | 

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

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_schema**
> GraphSchema get_schema(graph_name)



Get schema by graph name

### Example


```python
import time
import os
import interactive_sdk
from interactive_sdk.models.graph_schema import GraphSchema
from interactive_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0
# See configuration.py for a list of all supported configuration parameters.
configuration = interactive_sdk.Configuration(
    host = "https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0"
)


# Enter a context with an instance of the API client
with interactive_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = interactive_sdk.AdminServiceGraphManagementApi(api_client)
    graph_name = 'graph_name_example' # str | The name of graph to delete

    try:
        api_response = api_instance.get_schema(graph_name)
        print("The response of AdminServiceGraphManagementApi->get_schema:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceGraphManagementApi->get_schema: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_name** | **str**| The name of graph to delete | 

### Return type

[**GraphSchema**](GraphSchema.md)

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
> List[Graph] list_graphs()



List all graphs

### Example


```python
import time
import os
import interactive_sdk
from interactive_sdk.models.graph import Graph
from interactive_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0
# See configuration.py for a list of all supported configuration parameters.
configuration = interactive_sdk.Configuration(
    host = "https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0"
)


# Enter a context with an instance of the API client
with interactive_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = interactive_sdk.AdminServiceGraphManagementApi(api_client)

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

[**List[Graph]**](Graph.md)

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

