# gs_interactive.AdminServiceProcedureManagementApi

All URIs are relative to *{INTERACTIVE_ADMIN_ENDPOINT}*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**CreateProcedure**](ProcedureManagementApi.md#CreateProcedure) | **POST** /v1/graph/{graph_id}/procedure | Create a procedure on the specified graph |
| [**DeleteProcedure**](ProcedureManagementApi.md#DeleteProcedure) | **DELETE** /v1/graph/{graph_id}/procedure/{procedure_id} | Delete a procedure on the specified graph |
| [**GetProcedure**](ProcedureManagementApi.md#GetProcedure) | **GET** /v1/graph/{graph_id}/procedure/{procedure_id} | Get the metadata of a procedure on the specified graph |
| [**ListProcedures**](ProcedureManagementApi.md#ListProcedures) | **GET** /v1/graph/{graph_id}/procedure | List all procedures bound to a specified graph |
| [**UpdateProcedure**](ProcedureManagementApi.md#UpdateProcedure) | **PUT** /v1/graph/{graph_id}/procedure/{procedure_id} | Update the metadata of the specified graph |



# **CreateProcedure**
> [Result](./result.rst)[CreateProcedureResponse] create_procedure(graph_id, create_procedure_request)



Create a new procedure on a graph

### Example


```python
from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

test_graph_def = {
    "name": "test_graph",
    "description": "This is a test graph",
    "schema": {
        "vertex_types": [
            {
                "type_name": "person",
                "properties": [
                    {
                        "property_name": "id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_name": "name",
                        "property_type": {"string": {"long_text": ""}},
                    },
                    {
                        "property_name": "age",
                        "property_type": {"primitive_type": "DT_SIGNED_INT32"},
                    },
                ],
                "primary_keys": ["id"],
            }
        ],
        "edge_types": [
            {
                "type_name": "knows",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "person",
                        "destination_vertex": "person",
                        "relation": "MANY_TO_MANY",
                    }
                ],
                "properties": [
                    {
                        "property_name": "weight",
                        "property_type": {"primitive_type": "DT_DOUBLE"},
                    }
                ],
                "primary_keys": [],
            }
        ],
    },
}
driver = Driver()
sess = driver.session()
create_graph_request = CreateGraphRequest.from_dict(test_graph_def)
resp = sess.create_graph(create_graph_request)
assert resp.is_ok()
graph_id = resp.get_value().graph_id
print("Graph id: ", graph_id)

# Create procedure
create_proc_request = CreateProcedureRequest(
    name="test_procedure",
    description="test procedure",
    query="MATCH (n) RETURN COUNT(n);",
    type="cypher",
)
resp = sess.create_procedure(graph_id, create_proc_request)
assert resp.is_ok()
proc_id = resp.get_value().procedure_id
print("procedure id", proc_id)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **create_procedure_request** | [**CreateProcedureRequest**](CreateProcedureRequest.md)|  | 

### Return type

[Result](./result.rst)[[**CreateProcedureResponse**](CreateProcedureResponse.md)]

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

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

# **DeleteProcedure**
> [Result](./result.rst)[str] delete_procedure(graph_id, procedure_id)



Delete a procedure on a graph by name

### Example


```python
resp = sess.delete_procedure(graph_id, proc_id)
assert resp.is_ok()
print("delete procedure result", resp)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **procedure_id** | **str**|  | 

### Return type

[Result](./result.rst)[**str**]

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

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

# **GetProcedure**
> [Result](./result.rst)[GetProcedureResponse] get_procedure(graph_id, procedure_id)



Get a procedure by graph id and procedure id.

### Example


```python
resp = sess.get_procedure(graph_id, proc_id)
assert resp.is_ok()
print("get procedure ", resp)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **procedure_id** | **str**|  | 

### Return type

[Result](./result.rst)[[**GetProcedureResponse**](GetProcedureResponse.md)]

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

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

# **ListProcedures**
> [Result](./result.rst)[List[GetProcedureResponse]] list_procedures(graph_id)



List all procedures bound to a graph

### Example


```python
resp = sess.list_procedures(graph_id)
assert resp.is_ok()
print("list all procedures", resp)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 

### Return type

[Result](./result.rst)[[**List[GetProcedureResponse]**](GetProcedureResponse.md)]

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

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

# **UpdateProcedure**
> [Result](./result.rst)[str] update_procedure(graph_id, procedure_id, update_procedure_request=update_procedure_request)



Update the metadata of a procedure, i.e. description. The procedure's query or implementation can not be updated.

### Example


```python
update_proc_req = UpdateProcedureRequest(description="A new description")
resp = sess.update_procedure(graph_id, proc_id, update_proc_req)
assert resp.is_ok()
print("update proc success", resp)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **procedure_id** | **str**|  | 
 **update_procedure_request** | [**UpdateProcedureRequest**](UpdateProcedureRequest.md)|  | [optional] 

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
**200** | Successful operation |  -  |
**400** | Bad request |  -  |
**404** | Not Found |  -  |
**500** | Internal error |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

