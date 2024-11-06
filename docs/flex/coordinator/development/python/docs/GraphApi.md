# graphscope.flex.rest.GraphApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_edge_type**](GraphApi.md#create_edge_type) | **POST** /api/v1/graph/{graph_id}/schema/edge | 
[**create_graph**](GraphApi.md#create_graph) | **POST** /api/v1/graph | 
[**create_vertex_type**](GraphApi.md#create_vertex_type) | **POST** /api/v1/graph/{graph_id}/schema/vertex | 
[**delete_edge_type_by_name**](GraphApi.md#delete_edge_type_by_name) | **DELETE** /api/v1/graph/{graph_id}/schema/edge/{type_name} | 
[**delete_graph_by_id**](GraphApi.md#delete_graph_by_id) | **DELETE** /api/v1/graph/{graph_id} | 
[**delete_vertex_type_by_name**](GraphApi.md#delete_vertex_type_by_name) | **DELETE** /api/v1/graph/{graph_id}/schema/vertex/{type_name} | 
[**get_graph_by_id**](GraphApi.md#get_graph_by_id) | **GET** /api/v1/graph/{graph_id} | 
[**get_schema_by_id**](GraphApi.md#get_schema_by_id) | **GET** /api/v1/graph/{graph_id}/schema | 
[**import_schema_by_id**](GraphApi.md#import_schema_by_id) | **POST** /api/v1/graph/{graph_id}/schema | 
[**list_graphs**](GraphApi.md#list_graphs) | **GET** /api/v1/graph | 


# **create_edge_type**
> str create_edge_type(graph_id, create_edge_type=create_edge_type)



Create a edge type

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.create_edge_type import CreateEdgeType
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
    api_instance = graphscope.flex.rest.GraphApi(api_client)
    graph_id = 'graph_id_example' # str | 
    create_edge_type = graphscope.flex.rest.CreateEdgeType() # CreateEdgeType |  (optional)

    try:
        api_response = api_instance.create_edge_type(graph_id, create_edge_type=create_edge_type)
        print("The response of GraphApi->create_edge_type:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphApi->create_edge_type: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **create_edge_type** | [**CreateEdgeType**](CreateEdgeType.md)|  | [optional] 

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
**200** | Successful created the edge type |  -  |
**400** | Bad request |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_graph**
> CreateGraphResponse create_graph(create_graph_request)



Create a new graph

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.create_graph_request import CreateGraphRequest
from graphscope.flex.rest.models.create_graph_response import CreateGraphResponse
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
    api_instance = graphscope.flex.rest.GraphApi(api_client)
    create_graph_request = graphscope.flex.rest.CreateGraphRequest() # CreateGraphRequest | 

    try:
        api_response = api_instance.create_graph(create_graph_request)
        print("The response of GraphApi->create_graph:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphApi->create_graph: %s\n" % e)
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
**200** | The graph was created |  -  |
**400** | Bad request |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_vertex_type**
> str create_vertex_type(graph_id, create_vertex_type)



Create a vertex type

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.create_vertex_type import CreateVertexType
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
    api_instance = graphscope.flex.rest.GraphApi(api_client)
    graph_id = 'graph_id_example' # str | 
    create_vertex_type = graphscope.flex.rest.CreateVertexType() # CreateVertexType | 

    try:
        api_response = api_instance.create_vertex_type(graph_id, create_vertex_type)
        print("The response of GraphApi->create_vertex_type:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphApi->create_vertex_type: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **create_vertex_type** | [**CreateVertexType**](CreateVertexType.md)|  | 

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
**200** | Successful created a vertex type |  -  |
**400** | Bad request |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_edge_type_by_name**
> str delete_edge_type_by_name(graph_id, type_name, source_vertex_type, destination_vertex_type)



Delete edge type by name

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
    api_instance = graphscope.flex.rest.GraphApi(api_client)
    graph_id = 'graph_id_example' # str | 
    type_name = 'type_name_example' # str | 
    source_vertex_type = 'source_vertex_type_example' # str | 
    destination_vertex_type = 'destination_vertex_type_example' # str | 

    try:
        api_response = api_instance.delete_edge_type_by_name(graph_id, type_name, source_vertex_type, destination_vertex_type)
        print("The response of GraphApi->delete_edge_type_by_name:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphApi->delete_edge_type_by_name: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **type_name** | **str**|  | 
 **source_vertex_type** | **str**|  | 
 **destination_vertex_type** | **str**|  | 

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
**200** | Successful deleted the edge type |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_graph_by_id**
> str delete_graph_by_id(graph_id)



Delete graph by ID

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
    api_instance = graphscope.flex.rest.GraphApi(api_client)
    graph_id = 'graph_id_example' # str | 

    try:
        api_response = api_instance.delete_graph_by_id(graph_id)
        print("The response of GraphApi->delete_graph_by_id:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphApi->delete_graph_by_id: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 

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
**200** | Successfully deleted the graph |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_vertex_type_by_name**
> str delete_vertex_type_by_name(graph_id, type_name)



Delete vertex type by name

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
    api_instance = graphscope.flex.rest.GraphApi(api_client)
    graph_id = 'graph_id_example' # str | 
    type_name = 'type_name_example' # str | 

    try:
        api_response = api_instance.delete_vertex_type_by_name(graph_id, type_name)
        print("The response of GraphApi->delete_vertex_type_by_name:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphApi->delete_vertex_type_by_name: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **type_name** | **str**|  | 

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
**200** | Successful deleted the vertex type |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_graph_by_id**
> GetGraphResponse get_graph_by_id(graph_id)



Get graph by ID

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.get_graph_response import GetGraphResponse
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
    api_instance = graphscope.flex.rest.GraphApi(api_client)
    graph_id = 'graph_id_example' # str | 

    try:
        api_response = api_instance.get_graph_by_id(graph_id)
        print("The response of GraphApi->get_graph_by_id:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphApi->get_graph_by_id: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 

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
**200** | Successfully returned the graph |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_schema_by_id**
> GetGraphSchemaResponse get_schema_by_id(graph_id)



Get graph schema by ID

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.get_graph_schema_response import GetGraphSchemaResponse
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
    api_instance = graphscope.flex.rest.GraphApi(api_client)
    graph_id = 'graph_id_example' # str | 

    try:
        api_response = api_instance.get_schema_by_id(graph_id)
        print("The response of GraphApi->get_schema_by_id:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphApi->get_schema_by_id: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 

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
**200** | Successfully returned the graph schema |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **import_schema_by_id**
> str import_schema_by_id(graph_id, create_graph_schema_request)



Import graph schema

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.create_graph_schema_request import CreateGraphSchemaRequest
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
    api_instance = graphscope.flex.rest.GraphApi(api_client)
    graph_id = 'graph_id_example' # str | 
    create_graph_schema_request = graphscope.flex.rest.CreateGraphSchemaRequest() # CreateGraphSchemaRequest | 

    try:
        api_response = api_instance.import_schema_by_id(graph_id, create_graph_schema_request)
        print("The response of GraphApi->import_schema_by_id:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphApi->import_schema_by_id: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **create_graph_schema_request** | [**CreateGraphSchemaRequest**](CreateGraphSchemaRequest.md)|  | 

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
**200** | Successful imported the graph schema |  -  |
**400** | Bad request |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_graphs**
> List[GetGraphResponse] list_graphs()



List all graphs

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.get_graph_response import GetGraphResponse
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
    api_instance = graphscope.flex.rest.GraphApi(api_client)

    try:
        api_response = api_instance.list_graphs()
        print("The response of GraphApi->list_graphs:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphApi->list_graphs: %s\n" % e)
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
**200** | Successfully returned all graphs |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

