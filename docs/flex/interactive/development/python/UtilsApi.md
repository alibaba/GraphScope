# gs_interactive.UtilsApi

All URIs are relative to *{INTERACTIVE_ENDPOINT}*

Method | HTTP request | Description
------------- | ------------- | -------------
[**upload_file**](UtilsApi.md#upload_file) | **POST** /v1/file/upload | 


# **upload_file**
> UploadFileResponse upload_file(filestorage=filestorage)



### Example


```python
import gs_interactive
from gs_interactive.models.upload_file_response import UploadFileResponse
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
    api_instance = gs_interactive.UtilsApi(api_client)
    filestorage = None # bytearray |  (optional)

    try:
        api_response = api_instance.upload_file(filestorage=filestorage)
        print("The response of UtilsApi->upload_file:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling UtilsApi->upload_file: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **filestorage** | **bytearray**|  | [optional] 

### Return type

[**UploadFileResponse**](UploadFileResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: multipart/form-data
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**500** | Server Internal Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

