# gs_interactive.AdminServiceServiceManagementApi

All URIs are relative to *{INTERACTIVE_ADMIN_ENDPOINT}*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**GetServiceStatus**](ServiceManagementApi.md#GetServiceStatus) | **GET** /v1/service/status | Get the status and metrics of service |
| [**RestartService**](ServiceManagementApi.md#RestartService) | **POST** /v1/service/restart | Restart the query service on the current running graph |
| [**StartService**](ServiceManagementApi.md#StartService) | **POST** /v1/service/start | Start the query service on the specified graph |
| [**StopService**](ServiceManagementApi.md#StopService) | **POST** /v1/service/stop | Stop the query service |


# **GetServiceStatus**
> [Result](./result.rst)[ServiceStatus] get_service_status()



Get service status

### Example


```python
resp = sess.get_service_status()
assert resp.is_ok()
print("Current service status", resp)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[Result](./result.rst)[[**ServiceStatus**](ServiceStatus.md)]

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

# **RestartService**
> [Result](./result.rst)[str] restart_service()



Start current service

### Example


```python
resp = sess.restart_service()
assert resp.is_ok()
print("restart service result", resp)
```



### Parameters

This endpoint does not need any parameter.

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
**200** | successful operation |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

# **StartService**
> [Result](./result.rst)[str] start_service(start_service_request=start_service_request)



Start service on a specified graph

### Example


```python
resp = sess.start_service(
    start_service_request=StartServiceRequest(graph_id=graph_id)
)
assert resp.is_ok()
print("restart service result: ", resp.get_value())
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **start_service_request** | [**StartServiceRequest**](StartServiceRequest.md)| Start service on a specified graph | [optional] 

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
**200** | successful operation |  -  |
**500** | Internal Error |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

# **StopService**
> [Result](./result.rst)[str] stop_service(graph_id : str = None)

Stop the current query service while the admin service continues to operate. You can specify a `graph_id` to indicate which graph's service to stop. Since only one graph can run at a time, the `graph_id` will verify if the query service is active on that graph. If no `graph_id` is provided, the query service will be stopped without this verification.


### Example


```python
stop_res = sess.stop_service(graph_id = '1')
assert stop_res.is_ok()
print("stop service result", stop_res)
```



### Parameters

This endpoint does not need any parameter.

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
**200** | successful operation |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

