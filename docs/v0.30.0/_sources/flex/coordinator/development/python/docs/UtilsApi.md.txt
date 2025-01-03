# graphscope.flex.rest.UtilsApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**upload_file**](UtilsApi.md#upload_file) | **POST** /api/v1/file/uploading | 


# **upload_file**
> UploadFileResponse upload_file(filestorage=filestorage)



### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.upload_file_response import UploadFileResponse
from graphscope.flex.rest.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = graphscope.flex.rest.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with graphscope.flex.rest.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = graphscope.flex.rest.UtilsApi(api_client)
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
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

