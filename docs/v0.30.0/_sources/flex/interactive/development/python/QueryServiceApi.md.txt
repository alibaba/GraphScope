# gs_interactive.QueryServiceApi

All URIs are relative to *{INTERACTIVE_ADMIN_ENDPOINT}*

Although Interactive supports multiple graphs in storage level, the query service currently could only runs on a single graph. 
This means that at any given time, only one graph can provide query services. 
If you attempt to submit a query to a graph that is not currently running, we will throw an error directly.

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**CallProcedure**](QueryServiceApi.md#CallProcedure) | **POST** /v1/graph/{graph_id}/query | submit query to the graph identified by the specified graph id |
| [**CallProcedureOnCurrentGraph**](QueryServiceApi.md#CallProcedureOnCurrentGraph) | **POST** /v1/graph/current/query | submit query to the current running graph |


# **CallProcedure**
> [Result](./result.rst)[CollectiveResults] call_procedure(graph_id, params)

Submit procedure call queries to the specified graph.
The output format for the query is define by the [results.proto](https://github.com/alibaba/GraphScope/blob/main/interactive_engine/executor/ir/proto/results.proto).

For the creation of stored procedure please refer to [CypherStoredProcedure](../../stored_procedures.md) and [CppStoredProcedure](../stored_procedure/cpp_procedure.md).

### Example


```python
# create graph..
# create procedure

req = QueryRequest(
    query_name=proc_id,
    arguments=[
        TypedValue(
            type=GSDataType(
                PrimitiveType(primitive_type="DT_SIGNED_INT32")
            ),
            value = 1
        )
    ]
)
resp = sess.call_procedure(graph_id = graph_id, params = req)
assert resp.is_ok()
print("call procedure result: ", resp.get_value())
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **params** | **QueryRequest**|  | [optional] 

### Return type

[Result](./result.rst)[**CollectiveResults**]

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: text/plain
 - **Accept**: text/plain

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully runned. Empty if failed? |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

# **CallProcedureOnCurrentGraph**
> [Result](./result.rst)[CollectiveResults] call_procedure_current(params)

run queries on the running graph

Submit a query to the running graph. 

### Example


```python
# create graph..
# create procedure

req = QueryRequest(
    query_name=proc_id,
    arguments=[
        TypedValue(
            type=GSDataType(
                PrimitiveType(primitive_type="DT_SIGNED_INT32")
            ),
            value = 1
        )
    ]
)
resp = sess.call_procedure(params = req)
assert resp.is_ok()
print("call procedure result: ", resp.get_value())
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | **QueryRequest**|  | [optional] 

### Return type

[Result](./result.rst)[**CollectiveResults**]

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: text/plain
 - **Accept**: text/plain

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully runned. Empty if failed? |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

