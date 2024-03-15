# interactive_sdk.QueryServiceApi

All URIs are relative to *https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0*

Method | HTTP request | Description
------------- | ------------- | -------------
[**proc_call**](QueryServiceApi.md#proc_call) | **POST** /v1/graph/{graph_name}/query | run queries on graph


# **proc_call**
> proc_call(graph_name, body=body)

run queries on graph

After the procedure is created, user can use this API to run the procedure. TODO: a full example cypher->plan->json. TODO: make sure typeinfo can be passed. 

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
    api_instance = interactive_sdk.QueryServiceApi(api_client)
    graph_name = 'graph_name_example' # str | 
    body = {"type":"native/proc_call/adhoc","payload":"binary bytes\n"} # str |  (optional)

    try:
        # run queries on graph
        api_instance.proc_call(graph_name, body=body)
    except Exception as e:
        print("Exception when calling QueryServiceApi->proc_call: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_name** | **str**|  | 
 **body** | **str**|  | [optional] 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: text/plain

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully runned. Empty if failed? |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

