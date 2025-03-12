# VertexApi

All URIs are relative to *{INTERACTIVE_ADMIN_ENDPOINT}*

Method | HTTP request | Description
------------- | ------------- | -------------
[**add_vertex**](VertexApi.md#add_vertex) | **POST** /v1/graph/{graph_id}/vertex | Add vertex to the graph
[**get_vertex**](VertexApi.md#get_vertex) | **GET** /v1/graph/{graph_id}/vertex | Get the vertex&#39;s properties with vertex primary key.
[**update_vertex**](VertexApi.md#update_vertex) | **PUT** /v1/graph/{graph_id}/vertex | Update vertex&#39;s property


# **add_vertex**
> [Result](./result.rst)[str] add_vertex(graph_id, vertex_edge_request)

Add the provided vertex to the specified graph. 

### Example


```python
from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

driver = Driver()
sess = driver.session()

graph_id = "1"

vertex_request = [
    VertexRequest(
        label="person",
        primary_key_value=8,
        properties=[
            ModelProperty(name="name",value="mike"),
            ModelProperty(name="age", value=12),
        ],
    ),
]
resp = sess.add_vertex(
    graph_id,
    VertexEdgeRequest(vertex_request=vertex_request),
)
print(resp)
assert resp.is_ok()
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **vertex_edge_request** | [**VertexEdgeRequest**](VertexEdgeRequest.md)|  | 

### Return type

[Result](./result.rst)[**str**]

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

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)


# **get_vertex**
> [Result](./result.rst)[VertexData] get_vertex(graph_id, label, primary_key_value)

Get the vertex's properties with vertex primary key.

Get the properties for the specified vertex. example: ```http GET /endpoint?param1=value1&param2=value2 HTTP/1.1 Host: example.com ``` 

### Example


```python
from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

driver = Driver()
sess = driver.session()

graph_id = "1"

# get vertex
resp = sess.get_vertex(graph_id, "person", 8)
assert resp.is_ok()
for k, v in resp.get_value().values:
    if k == "name":
        assert v == "mike"
    if k == "age":
        assert v == 12
print(resp)
assert resp.is_ok()
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**| The id of the graph | 
 **label** | **str**| The label name of querying vertex. | 
 **primary_key_value** | [**object**](.md)| The primary key value of querying vertex. | 

### Return type

[Result](./result.rst)[[**VertexData**](VertexData.md)]

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

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

# **update_vertex**
> [Result](./result.rst)[str] update_vertex(graph_id, vertex_request=vertex_request)

Update vertex's property

Remove the vertex from the specified graph. 

### Example


```python
from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

driver = Driver()
sess = driver.session()

graph_id = "1"

vertex_request = VertexRequest(
    label="person",
    primary_key_value=1,
    properties=[
        ModelProperty(name="name", value="Cindy"),
        ModelProperty(name="age", value=24),
    ],
)
# update vertex
resp = sess.update_vertex(graph_id, vertex_request)
print(resp)
assert resp.is_ok()
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **vertex_request** | [**VertexRequest**](VertexRequest.md)|  | [optional] 

### Return type

[Result](./result.rst)[**str**]

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully update vertex |  -  |
**400** | Invalid input parameters |  -  |
**404** | Vertex not exists |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

