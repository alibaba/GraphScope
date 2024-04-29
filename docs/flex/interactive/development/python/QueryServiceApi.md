# interactive_sdk.QueryServiceApi

All URIs are relative to `${INTERACTIVE_ENDPOINT}`

Method | HTTP request | Description
------------- | ------------- | -------------
[**proc_call**](QueryServiceApi.md#proc_call) | **POST** /v1/graph/{graph_id}/query | run queries on graph


# **proc_call**
> CollectiveResults proc_call(graph_id, query_request=query_request)

run queries on graph

After the procedure is created, user can use this API to run the procedure. TODO: a full example cypher->plan->json. TODO: make sure typeinfo can be passed. 

### Example


```python
import time
import os
import interactive_sdk
from interactive_sdk.models.collective_results import CollectiveResults
from interactive_sdk.models.query_request import QueryRequest
from interactive_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to {INTERACTIVE_ENDPOINT}
# See configuration.py for a list of all supported configuration parameters.
configuration = interactive_sdk.Configuration(
    host = "{INTERACTIVE_ENDPOINT}"
)


# Enter a context with an instance of the API client
with interactive_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = interactive_sdk.QueryServiceApi(api_client)
    graph_id = 'graph_id_example' # str | 
    query_request = interactive_sdk.QueryRequest() # QueryRequest |  (optional)

    try:
        # run queries on graph
        api_response = api_instance.proc_call(graph_id, query_request=query_request)
        print("The response of QueryServiceApi->proc_call:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling QueryServiceApi->proc_call: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **query_request** | [**QueryRequest**](QueryRequest.md)|  | [optional] 

### Return type

[**CollectiveResults**](CollectiveResults.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully runned. Empty if failed? |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

