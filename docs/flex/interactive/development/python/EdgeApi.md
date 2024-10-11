# EdgeApi

All URIs are relative to *{INTERACTIVE_ADMIN_ENDPOINT}*

Method | HTTP request | Description
------------- | ------------- | -------------
[**add_edge**](EdgeApi.md#add_edge) | **POST** /v1/graph/{graph_id}/edge | Add edge to the graph
[**get_edge**](EdgeApi.md#get_edge) | **GET** /v1/graph/{graph_id}/edge | Get the edge&#39;s properties with src and dst vertex primary keys.
[**update_edge**](EdgeApi.md#update_edge) | **PUT** /v1/graph/{graph_id}/edge | Update edge&#39;s property


# **add_edge**
> [Result](./result.rst)[str] add_edge(graph_id, edge_request)

Add a edge to the graph.

### Example


```python
from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

driver = Driver()
sess = driver.session()

graph_id = "1"

edge_request = [
    EdgeRequest(
        src_label="person",
        dst_label="person",
        edge_label="knows",
        src_primary_key_value=2,
        dst_primary_key_value=4,
        properties=[ModelProperty(name="weight", value=9.123)],
    ),
    EdgeRequest(
        src_label="person",
        dst_label="person",
        edge_label="knows",
        src_primary_key_value=2,
        dst_primary_key_value=6,
        properties=[ModelProperty(name="weight", value=3.233)],
    ),
]
resp = sess.add_edge(graph_id, edge_request)
print(resp)
assert resp.is_ok()
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **edge_request** | [**List[EdgeRequest]**](EdgeRequest.md)|  | 

### Return type

[Result](./result.rst)[str]

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

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)


# **get_edge**
> [Result](./result.rst)[EdgeData] get_edge(graph_id, edge_label, src_label, src_primary_key_value, dst_label, dst_primary_key_value)

Get the edge's properties with src and dst vertex primary keys.

Get the properties for the specified vertex. 

### Example


```python
from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

driver = Driver()
sess = driver.session()

graph_id = "1"

resp = sess.get_edge(graph_id, "knows", "person", 2, "person", 4)
assert resp.is_ok()
for k, v in resp.get_value().properties:
    if k == "weight":
        assert v == 9.123
print(resp)
assert resp.is_ok()
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

[Result](./result.rst)[[EdgeData](EdgeData.md)]

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

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

# **update_edge**
> [Result](./result.rst)[str] update_edge(graph_id, edge_request=edge_request)

Update edge's property

Update the edge on the running graph. 

### Example


```python
from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

driver = Driver()
sess = driver.session()

graph_id = "1"

resp = sess.update_edge(
    graph_id,
    EdgeRequest(
        src_label="person",
        dst_label="person",
        edge_label="knows",
        src_primary_key_value=2,
        dst_primary_key_value=4,
        properties=[ModelProperty(name="weight", value=3)],
    ),
)
print(resp)
assert resp.is_ok()
```


### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **edge_request** | [**EdgeRequest**](EdgeRequest.md)|  | [optional] 

### Return type

[Result](./result.rst)[str]

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully update edge |  -  |
**400** | Invalid input parameters |  -  |
**404** | Edge not exists |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

