# gs_interactive.GraphServiceEdgeManagementApi

All URIs are relative to *{INTERACTIVE_ENDPOINT}*

Method | HTTP request | Description
------------- | ------------- | -------------
[**add_edge**](GraphServiceEdgeManagementApi.md#add_edge) | **POST** /v1/graph/{graph_id}/edge | Add edge to the graph
[**delete_edge**](GraphServiceEdgeManagementApi.md#delete_edge) | **DELETE** /v1/graph/{graph_id}/edge | Remove edge from the graph
[**get_edge**](GraphServiceEdgeManagementApi.md#get_edge) | **GET** /v1/graph/{graph_id}/edge | Get the edge&#39;s properties with src and dst vertex primary keys.
[**update_edge**](GraphServiceEdgeManagementApi.md#update_edge) | **PUT** /v1/graph/{graph_id}/edge | Update edge&#39;s property


# **add_edge**
> str add_edge(graph_id, edge_request=edge_request)

Add edge to the graph

Add the edge to graph. 

### Example


```python
import gs_interactive
from gs_interactive.models.edge_request import EdgeRequest
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
    api_instance = gs_interactive.GraphServiceEdgeManagementApi(api_client)
    graph_id = 'graph_id_example' # str | 
    edge_request = {"src_label":"person","dst_label":"software","edge_label":"created","src_pk_name":"id","src_pk_value":1,"dst_pk_name":"id","dst_pk_value":3,"properties":[{"name":"weight","value":0.2}]} # EdgeRequest |  (optional)

    try:
        # Add edge to the graph
        api_response = api_instance.add_edge(graph_id, edge_request=edge_request)
        print("The response of GraphServiceEdgeManagementApi->add_edge:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphServiceEdgeManagementApi->add_edge: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **edge_request** | [**EdgeRequest**](EdgeRequest.md)|  | [optional] 

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
**200** | Successfully insert the edge |  -  |
**400** | Invalid input edge |  -  |
**409** | edge already exists |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_edge**
> str delete_edge(graph_id, src_label, src_primary_key_value, dst_label, dst_primary_key_value)

Remove edge from the graph

Remove the edge from current graph. 

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
    api_instance = gs_interactive.GraphServiceEdgeManagementApi(api_client)
    graph_id = 'graph_id_example' # str | 
    src_label = 'person' # str | The label name of src vertex.
    src_primary_key_value = 1 # object | The primary key value of src vertex.
    dst_label = 'software' # str | The label name of dst vertex.
    dst_primary_key_value = 3 # object | The primary key value of dst vertex.

    try:
        # Remove edge from the graph
        api_response = api_instance.delete_edge(graph_id, src_label, src_primary_key_value, dst_label, dst_primary_key_value)
        print("The response of GraphServiceEdgeManagementApi->delete_edge:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphServiceEdgeManagementApi->delete_edge: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **src_label** | **str**| The label name of src vertex. | 
 **src_primary_key_value** | [**object**](.md)| The primary key value of src vertex. | 
 **dst_label** | **str**| The label name of dst vertex. | 
 **dst_primary_key_value** | [**object**](.md)| The primary key value of dst vertex. | 

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
**200** | Successfully delete edge |  -  |
**400** | Invalid input edge |  -  |
**404** | Edge not exists or Graph not exits |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_edge**
> EdgeData get_edge(graph_id, edge_label, src_label, src_primary_key_value, dst_label, dst_primary_key_value)

Get the edge's properties with src and dst vertex primary keys.

Get the properties for the specified vertex. 

### Example


```python
import gs_interactive
from gs_interactive.models.edge_data import EdgeData
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
    api_instance = gs_interactive.GraphServiceEdgeManagementApi(api_client)
    graph_id = 'graph_id_example' # str | 
    edge_label = 'created' # str | The label name of querying edge.
    src_label = 'person' # str | The label name of src vertex.
    src_primary_key_value = 1 # object | The primary key value of src vertex.
    dst_label = 'software' # str | The label name of dst vertex.
    dst_primary_key_value = 3 # object | The value of dst vertex's primary key

    try:
        # Get the edge's properties with src and dst vertex primary keys.
        api_response = api_instance.get_edge(graph_id, edge_label, src_label, src_primary_key_value, dst_label, dst_primary_key_value)
        print("The response of GraphServiceEdgeManagementApi->get_edge:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphServiceEdgeManagementApi->get_edge: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **edge_label** | **str**| The label name of querying edge. | 
 **src_label** | **str**| The label name of src vertex. | 
 **src_primary_key_value** | [**object**](.md)| The primary key value of src vertex. | 
 **dst_label** | **str**| The label name of dst vertex. | 
 **dst_primary_key_value** | [**object**](.md)| The value of dst vertex&#39;s primary key | 

### Return type

[**EdgeData**](EdgeData.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Found Edge |  -  |
**400** | Bad input parameter |  -  |
**404** | Edge not found or Graph not found |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_edge**
> str update_edge(graph_id, edge_request=edge_request)

Update edge's property

Update the edge on the running graph. 

### Example


```python
import gs_interactive
from gs_interactive.models.edge_request import EdgeRequest
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
    api_instance = gs_interactive.GraphServiceEdgeManagementApi(api_client)
    graph_id = 'graph_id_example' # str | 
    edge_request = {"src_label":"person","dst_label":"software","edge_label":"created","src_pk_name":"id","src_pk_value":1,"dst_pk_name":"id","dst_pk_value":3,"properties":[{"name":"weight","value":0.3}]} # EdgeRequest |  (optional)

    try:
        # Update edge's property
        api_response = api_instance.update_edge(graph_id, edge_request=edge_request)
        print("The response of GraphServiceEdgeManagementApi->update_edge:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphServiceEdgeManagementApi->update_edge: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **edge_request** | [**EdgeRequest**](EdgeRequest.md)|  | [optional] 

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
**200** | Successfully update edge |  -  |
**400** | Invalid input paramters |  -  |
**404** | Edge not exists |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

