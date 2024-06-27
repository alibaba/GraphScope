# gs_interactive.QueryServiceApi

All URIs are relative to *{INTERACTIVE_ENDPOINT}*

Method | HTTP request | Description
------------- | ------------- | -------------
[**proc_call**](QueryServiceApi.md#proc_call) | **POST** /v1/graph/{graph_id}/query | run queries on graph
[**proc_call_current**](QueryServiceApi.md#proc_call_current) | **POST** /v1/graph/current/query | run queries on the running graph


# **proc_call**
> bytearray proc_call(graph_id, x_interactive_request_format, body=body)

run queries on graph

After the procedure is created, user can use this API to run the procedure. TODO: a full example cypher->plan->json. TODO: make sure typeinfo can be passed. 

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

