# gs_interactive.GraphServiceVertexManagementApi

All URIs are relative to *{INTERACTIVE_ENDPOINT}*

Method | HTTP request | Description
------------- | ------------- | -------------
[**add_vertex**](GraphServiceVertexManagementApi.md#add_vertex) | **POST** /v1/graph/{graph_id}/vertex | Add vertex to the graph
[**delete_vertex**](GraphServiceVertexManagementApi.md#delete_vertex) | **DELETE** /v1/graph/{graph_id}/vertex | Remove vertex from the graph
[**get_vertex**](GraphServiceVertexManagementApi.md#get_vertex) | **GET** /v1/graph/{graph_id}/vertex | Get the vertex&#39;s properties with vertex primary key.
[**update_vertex**](GraphServiceVertexManagementApi.md#update_vertex) | **PUT** /v1/graph/{graph_id}/vertex | Update vertex&#39;s property


# **add_vertex**
> str add_vertex(graph_id, vertex_request=vertex_request)

Add vertex to the graph

Add the provided vertex to the specified graph. 

### Example


```python
import gs_interactive
from gs_interactive.models.vertex_request import VertexRequest
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
    api_instance = gs_interactive.GraphServiceVertexManagementApi(api_client)
    graph_id = 'graph_id_example' # str | 
    vertex_request = {"label":"person","primimary_key_value":1,"properties":{"age":2,"name":"Bob"}} # VertexRequest | Add vertex to graph. (optional)

    try:
        # Add vertex to the graph
        api_response = api_instance.add_vertex(graph_id, vertex_request=vertex_request)
        print("The response of GraphServiceVertexManagementApi->add_vertex:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphServiceVertexManagementApi->add_vertex: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **vertex_request** | [**VertexRequest**](VertexRequest.md)| Add vertex to graph. | [optional] 

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
**200** | Successfully created vertex |  -  |
**400** | Invalid input vertex |  -  |
**404** | Graph not found |  -  |
**409** | Vertex already exists |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_vertex**
> str delete_vertex(graph_id, label, primary_key_value)

Remove vertex from the graph

Remove the vertex from the specified graph. 

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
    api_instance = gs_interactive.GraphServiceVertexManagementApi(api_client)
    graph_id = 'graph_id_example' # str | 
    label = 'label_example' # str | The label name of querying vertex.
    primary_key_value = None # object | The value of the querying vertex's primary key

    try:
        # Remove vertex from the graph
        api_response = api_instance.delete_vertex(graph_id, label, primary_key_value)
        print("The response of GraphServiceVertexManagementApi->delete_vertex:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphServiceVertexManagementApi->delete_vertex: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **label** | **str**| The label name of querying vertex. | 
 **primary_key_value** | [**object**](.md)| The value of the querying vertex&#39;s primary key | 

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
**200** | Successfully delete vertex |  -  |
**400** | Invalid input vertex |  -  |
**404** | Vertex not exists or Graph not exits. |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_vertex**
> VertexData get_vertex(graph_id, label, primary_key_value)

Get the vertex's properties with vertex primary key.

Get the properties for the specified vertex. example: ```http GET /endpoint?param1=value1&param2=value2 HTTP/1.1 Host: example.com ``` 

### Example


```python
import gs_interactive
from gs_interactive.models.vertex_data import VertexData
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
    api_instance = gs_interactive.GraphServiceVertexManagementApi(api_client)
    graph_id = 'graph_id_example' # str | The name of the graph
    label = 'label_example' # str | The label name of querying vertex.
    primary_key_value = None # object | The primary key value of querying vertex.

    try:
        # Get the vertex's properties with vertex primary key.
        api_response = api_instance.get_vertex(graph_id, label, primary_key_value)
        print("The response of GraphServiceVertexManagementApi->get_vertex:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphServiceVertexManagementApi->get_vertex: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**| The name of the graph | 
 **label** | **str**| The label name of querying vertex. | 
 **primary_key_value** | [**object**](.md)| The primary key value of querying vertex. | 

### Return type

[**VertexData**](VertexData.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Found vertex |  -  |
**400** | Bad input parameter |  -  |
**404** | Vertex not found or graph not found |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_vertex**
> str update_vertex(graph_id, vertex_request=vertex_request)

Update vertex's property

Remove the vertex from the specified graph. 

### Example


```python
import gs_interactive
from gs_interactive.models.vertex_request import VertexRequest
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
    api_instance = gs_interactive.GraphServiceVertexManagementApi(api_client)
    graph_id = 'graph_id_example' # str | 
    vertex_request = {"label":"person","primary_key_value":2,"properties":{"age":24,"name":"Cindy"}} # VertexRequest |  (optional)

    try:
        # Update vertex's property
        api_response = api_instance.update_vertex(graph_id, vertex_request=vertex_request)
        print("The response of GraphServiceVertexManagementApi->update_vertex:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphServiceVertexManagementApi->update_vertex: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **vertex_request** | [**VertexRequest**](VertexRequest.md)|  | [optional] 

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
**200** | Successfully update vertex |  -  |
**400** | Invalid input paramters |  -  |
**404** | Vertex not exists |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

