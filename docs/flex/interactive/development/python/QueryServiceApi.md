# gs_interactive.QueryServiceApi

All URIs are relative to *{INTERACTIVE_ENDPOINT}*

Although Interactive supports multiple graphs in storage level, the query service currently could only runs on a single graph. 
This means that at any given time, only one graph can provide query services. 
If you attempt to submit a query to a graph that is not currently running, we will throw an error directly.

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**CallProcedure**](QueryServiceApi.md#CallProcedure) | **POST** /v1/graph/{graph_id}/query | submit query to the graph identified by the specified graph id |
| [**CallProcedureOnCurrentGraph**](QueryServiceApi.md#CallProcedureOnCurrentGraph) | **POST** /v1/graph/current/query | submit query to the current running graph |


# **proc_call**
> bytearray proc_call(graph_id, x_interactive_request_format, body=body)

Submit procedure call queries to the specified graph.
The output format for the query is define by the [results.proto](https://github.com/alibaba/GraphScope/blob/main/interactive_engine/executor/ir/proto/results.proto).

For the creation of stored procedure please refer to [CypherStoredProcedure](../cypher_procedure.md) and [CppStoredProcedure](../cpp_procedure.md).

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
    api_instance = gs_interactive.QueryServiceApi(api_client)
    graph_id = 'graph_id_example' # str | 
    x_interactive_request_format = 'x_interactive_request_format_example' # str | 
    body = None # bytearray |  (optional)

    try:
        # run queries on graph
        api_response = api_instance.proc_call(graph_id, x_interactive_request_format, body=body)
        print("The response of QueryServiceApi->proc_call:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling QueryServiceApi->proc_call: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **x_interactive_request_format** | **str**|  | 
 **body** | **bytearray**|  | [optional] 

### Return type

**bytearray**

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

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **proc_call_current**
> bytearray proc_call_current(x_interactive_request_format, body=body)

run queries on the running graph

Submit a query to the running graph. 

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
    api_instance = gs_interactive.QueryServiceApi(api_client)
    x_interactive_request_format = 'x_interactive_request_format_example' # str | 
    body = None # bytearray |  (optional)

    try:
        # run queries on the running graph
        api_response = api_instance.proc_call_current(x_interactive_request_format, body=body)
        print("The response of QueryServiceApi->proc_call_current:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling QueryServiceApi->proc_call_current: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **x_interactive_request_format** | **str**|  | 
 **body** | **bytearray**|  | [optional] 

### Return type

**bytearray**

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

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

