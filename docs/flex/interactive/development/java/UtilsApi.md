# UtilsApi

All URIs are relative to *{INTERACTIVE_ENDPOINT}*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**uploadFile**](UtilsApi.md#uploadFile) | **POST** /v1/file/upload |  |


<a id="uploadFile"></a>
# **uploadFile**
> UploadFileResponse uploadFile(filestorage)



### Example
```java
// Import classes:
import com.alibaba.graphscope.interactive.ApiClient;
import com.alibaba.graphscope.interactive.ApiException;
import com.alibaba.graphscope.interactive.Configuration;
import com.alibaba.graphscope.interactive.models.*;
import com.alibaba.graphscope.interactive.api.UtilsApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("{INTERACTIVE_ENDPOINT}");

    UtilsApi apiInstance = new UtilsApi(defaultClient);
    File filestorage = new File("/path/to/file"); // File | 
    try {
      UploadFileResponse result = apiInstance.uploadFile(filestorage);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling UtilsApi#uploadFile");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **filestorage** | **File**|  | [optional] |

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
| **200** | successful operation |  -  |
| **500** | Server Internal Error |  -  |

