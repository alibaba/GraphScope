# graphscope.flex.rest.AlertApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_alert_receiver**](AlertApi.md#create_alert_receiver) | **POST** /api/v1/alert/receiver | 
[**delete_alert_message_in_batch**](AlertApi.md#delete_alert_message_in_batch) | **DELETE** /api/v1/alert/message-collection | 
[**delete_alert_receiver_by_id**](AlertApi.md#delete_alert_receiver_by_id) | **DELETE** /api/v1/alert/receiver/{receiver_id} | 
[**delete_alert_rule_by_id**](AlertApi.md#delete_alert_rule_by_id) | **DELETE** /api/v1/alert/rule/{rule_id} | 
[**list_alert_messages**](AlertApi.md#list_alert_messages) | **GET** /api/v1/alert/message | 
[**list_alert_receivers**](AlertApi.md#list_alert_receivers) | **GET** /api/v1/alert/receiver | 
[**list_alert_rules**](AlertApi.md#list_alert_rules) | **GET** /api/v1/alert/rule | 
[**update_alert_message_in_batch**](AlertApi.md#update_alert_message_in_batch) | **PUT** /api/v1/alert/message-collection/status | 
[**update_alert_receiver_by_id**](AlertApi.md#update_alert_receiver_by_id) | **PUT** /api/v1/alert/receiver/{receiver_id} | 
[**update_alert_rule_by_id**](AlertApi.md#update_alert_rule_by_id) | **PUT** /api/v1/alert/rule/{rule_id} | 


# **create_alert_receiver**
> str create_alert_receiver(create_alert_receiver_request)



Create a new alert receiver

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.create_alert_receiver_request import CreateAlertReceiverRequest
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
    api_instance = graphscope.flex.rest.AlertApi(api_client)
    create_alert_receiver_request = graphscope.flex.rest.CreateAlertReceiverRequest() # CreateAlertReceiverRequest | 

    try:
        api_response = api_instance.create_alert_receiver(create_alert_receiver_request)
        print("The response of AlertApi->create_alert_receiver:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AlertApi->create_alert_receiver: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **create_alert_receiver_request** | [**CreateAlertReceiverRequest**](CreateAlertReceiverRequest.md)|  | 

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully created the alert receiver |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_alert_message_in_batch**
> str delete_alert_message_in_batch(message_ids)



Delete alert message in batch

### Example


```python
import graphscope.flex.rest
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
    api_instance = graphscope.flex.rest.AlertApi(api_client)
    message_ids = 'message_ids_example' # str | A list of message id separated by comma, e.g. id1,id2,id3

    try:
        api_response = api_instance.delete_alert_message_in_batch(message_ids)
        print("The response of AlertApi->delete_alert_message_in_batch:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AlertApi->delete_alert_message_in_batch: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **message_ids** | **str**| A list of message id separated by comma, e.g. id1,id2,id3 | 

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully deleted the alert message |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_alert_receiver_by_id**
> str delete_alert_receiver_by_id(receiver_id)



Delete the alert receiver by ID

### Example


```python
import graphscope.flex.rest
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
    api_instance = graphscope.flex.rest.AlertApi(api_client)
    receiver_id = 'receiver_id_example' # str | 

    try:
        api_response = api_instance.delete_alert_receiver_by_id(receiver_id)
        print("The response of AlertApi->delete_alert_receiver_by_id:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AlertApi->delete_alert_receiver_by_id: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **receiver_id** | **str**|  | 

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully deleted the alert receiver |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_alert_rule_by_id**
> str delete_alert_rule_by_id(rule_id)



### Example


```python
import graphscope.flex.rest
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
    api_instance = graphscope.flex.rest.AlertApi(api_client)
    rule_id = 'rule_id_example' # str | 

    try:
        api_response = api_instance.delete_alert_rule_by_id(rule_id)
        print("The response of AlertApi->delete_alert_rule_by_id:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AlertApi->delete_alert_rule_by_id: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **rule_id** | **str**|  | 

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully deleted the alert rule |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_alert_messages**
> List[GetAlertMessageResponse] list_alert_messages(alert_type=alert_type, status=status, severity=severity, start_time=start_time, end_time=end_time, limit=limit)



List all alert messages

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.get_alert_message_response import GetAlertMessageResponse
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
    api_instance = graphscope.flex.rest.AlertApi(api_client)
    alert_type = 'alert_type_example' # str |  (optional)
    status = 'status_example' # str |  (optional)
    severity = 'severity_example' # str |  (optional)
    start_time = 'start_time_example' # str | format with \"2023-02-21-11-56-30\" (optional)
    end_time = 'end_time_example' # str | format with \"2023-02-21-11-56-30\" (optional)
    limit = 56 # int |  (optional)

    try:
        api_response = api_instance.list_alert_messages(alert_type=alert_type, status=status, severity=severity, start_time=start_time, end_time=end_time, limit=limit)
        print("The response of AlertApi->list_alert_messages:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AlertApi->list_alert_messages: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **alert_type** | **str**|  | [optional] 
 **status** | **str**|  | [optional] 
 **severity** | **str**|  | [optional] 
 **start_time** | **str**| format with \&quot;2023-02-21-11-56-30\&quot; | [optional] 
 **end_time** | **str**| format with \&quot;2023-02-21-11-56-30\&quot; | [optional] 
 **limit** | **int**|  | [optional] 

### Return type

[**List[GetAlertMessageResponse]**](GetAlertMessageResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully returned the alert messages |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_alert_receivers**
> List[GetAlertReceiverResponse] list_alert_receivers()



List all alert receivers

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.get_alert_receiver_response import GetAlertReceiverResponse
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
    api_instance = graphscope.flex.rest.AlertApi(api_client)

    try:
        api_response = api_instance.list_alert_receivers()
        print("The response of AlertApi->list_alert_receivers:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AlertApi->list_alert_receivers: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**List[GetAlertReceiverResponse]**](GetAlertReceiverResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully returned the alert receivers |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_alert_rules**
> List[GetAlertRuleResponse] list_alert_rules()



List all alert rules

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.get_alert_rule_response import GetAlertRuleResponse
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
    api_instance = graphscope.flex.rest.AlertApi(api_client)

    try:
        api_response = api_instance.list_alert_rules()
        print("The response of AlertApi->list_alert_rules:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AlertApi->list_alert_rules: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**List[GetAlertRuleResponse]**](GetAlertRuleResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully returned the alert rules |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_alert_message_in_batch**
> str update_alert_message_in_batch(update_alert_message_status_request=update_alert_message_status_request)



Update the message status in batch

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.update_alert_message_status_request import UpdateAlertMessageStatusRequest
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
    api_instance = graphscope.flex.rest.AlertApi(api_client)
    update_alert_message_status_request = graphscope.flex.rest.UpdateAlertMessageStatusRequest() # UpdateAlertMessageStatusRequest |  (optional)

    try:
        api_response = api_instance.update_alert_message_in_batch(update_alert_message_status_request=update_alert_message_status_request)
        print("The response of AlertApi->update_alert_message_in_batch:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AlertApi->update_alert_message_in_batch: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **update_alert_message_status_request** | [**UpdateAlertMessageStatusRequest**](UpdateAlertMessageStatusRequest.md)|  | [optional] 

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful operation |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_alert_receiver_by_id**
> str update_alert_receiver_by_id(receiver_id, create_alert_receiver_request=create_alert_receiver_request)



Update alert receiver by ID

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.create_alert_receiver_request import CreateAlertReceiverRequest
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
    api_instance = graphscope.flex.rest.AlertApi(api_client)
    receiver_id = 'receiver_id_example' # str | 
    create_alert_receiver_request = graphscope.flex.rest.CreateAlertReceiverRequest() # CreateAlertReceiverRequest |  (optional)

    try:
        api_response = api_instance.update_alert_receiver_by_id(receiver_id, create_alert_receiver_request=create_alert_receiver_request)
        print("The response of AlertApi->update_alert_receiver_by_id:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AlertApi->update_alert_receiver_by_id: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **receiver_id** | **str**|  | 
 **create_alert_receiver_request** | [**CreateAlertReceiverRequest**](CreateAlertReceiverRequest.md)|  | [optional] 

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully updated the alert receiver |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_alert_rule_by_id**
> str update_alert_rule_by_id(rule_id, create_alert_rule_request=create_alert_rule_request)



### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.create_alert_rule_request import CreateAlertRuleRequest
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
    api_instance = graphscope.flex.rest.AlertApi(api_client)
    rule_id = 'rule_id_example' # str | 
    create_alert_rule_request = graphscope.flex.rest.CreateAlertRuleRequest() # CreateAlertRuleRequest |  (optional)

    try:
        api_response = api_instance.update_alert_rule_by_id(rule_id, create_alert_rule_request=create_alert_rule_request)
        print("The response of AlertApi->update_alert_rule_by_id:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AlertApi->update_alert_rule_by_id: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **rule_id** | **str**|  | 
 **create_alert_rule_request** | [**CreateAlertRuleRequest**](CreateAlertRuleRequest.md)|  | [optional] 

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully updated the alert rule |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

