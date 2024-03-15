# interactive_sdk.AdminServiceProcedureManagementApi

All URIs are relative to *https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_procedure**](AdminServiceProcedureManagementApi.md#create_procedure) | **POST** /v1/graph/{graph_name}/procedure | 
[**delete_procedure**](AdminServiceProcedureManagementApi.md#delete_procedure) | **DELETE** /v1/graph/{graph_name}/procedure/{procedure_name} | 
[**get_procedure**](AdminServiceProcedureManagementApi.md#get_procedure) | **GET** /v1/graph/{graph_name}/procedure/{procedure_name} | 
[**list_procedures**](AdminServiceProcedureManagementApi.md#list_procedures) | **GET** /v1/graph/{graph_name}/procedure | 
[**update_procedure**](AdminServiceProcedureManagementApi.md#update_procedure) | **PUT** /v1/graph/{graph_name}/procedure/{procedure_name} | 


# **create_procedure**
> str create_procedure(graph_name, procedure)



Create a new procedure on a graph

### Example


```python
import time
import os
import interactive_sdk
from interactive_sdk.models.procedure import Procedure
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
    api_instance = interactive_sdk.AdminServiceProcedureManagementApi(api_client)
    graph_name = 'graph_name_example' # str | 
    procedure = interactive_sdk.Procedure() # Procedure | 

    try:
        api_response = api_instance.create_procedure(graph_name, procedure)
        print("The response of AdminServiceProcedureManagementApi->create_procedure:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceProcedureManagementApi->create_procedure: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_name** | **str**|  | 
 **procedure** | [**Procedure**](Procedure.md)|  | 

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
**200** | successful operation |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_procedure**
> str delete_procedure(graph_name, procedure_name)



Delete a procedure on a graph by name

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
    api_instance = interactive_sdk.AdminServiceProcedureManagementApi(api_client)
    graph_name = 'graph_name_example' # str | 
    procedure_name = 'procedure_name_example' # str | 

    try:
        api_response = api_instance.delete_procedure(graph_name, procedure_name)
        print("The response of AdminServiceProcedureManagementApi->delete_procedure:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceProcedureManagementApi->delete_procedure: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_name** | **str**|  | 
 **procedure_name** | **str**|  | 

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
**200** | Successful operation |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_procedure**
> Procedure get_procedure(graph_name, procedure_name)



Get a procedure by name

### Example


```python
import time
import os
import interactive_sdk
from interactive_sdk.models.procedure import Procedure
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
    api_instance = interactive_sdk.AdminServiceProcedureManagementApi(api_client)
    graph_name = 'graph_name_example' # str | 
    procedure_name = 'procedure_name_example' # str | 

    try:
        api_response = api_instance.get_procedure(graph_name, procedure_name)
        print("The response of AdminServiceProcedureManagementApi->get_procedure:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceProcedureManagementApi->get_procedure: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_name** | **str**|  | 
 **procedure_name** | **str**|  | 

### Return type

[**Procedure**](Procedure.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_procedures**
> List[Procedure] list_procedures(graph_name)



List all procedures

### Example


```python
import time
import os
import interactive_sdk
from interactive_sdk.models.procedure import Procedure
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
    api_instance = interactive_sdk.AdminServiceProcedureManagementApi(api_client)
    graph_name = 'graph_name_example' # str | 

    try:
        api_response = api_instance.list_procedures(graph_name)
        print("The response of AdminServiceProcedureManagementApi->list_procedures:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceProcedureManagementApi->list_procedures: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_name** | **str**|  | 

### Return type

[**List[Procedure]**](Procedure.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful operation |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_procedure**
> str update_procedure(graph_name, procedure_name, procedure=procedure)



Update procedure on a graph by name

### Example


```python
import time
import os
import interactive_sdk
from interactive_sdk.models.procedure import Procedure
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
    api_instance = interactive_sdk.AdminServiceProcedureManagementApi(api_client)
    graph_name = 'graph_name_example' # str | 
    procedure_name = 'procedure_name_example' # str | 
    procedure = interactive_sdk.Procedure() # Procedure |  (optional)

    try:
        api_response = api_instance.update_procedure(graph_name, procedure_name, procedure=procedure)
        print("The response of AdminServiceProcedureManagementApi->update_procedure:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminServiceProcedureManagementApi->update_procedure: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_name** | **str**|  | 
 **procedure_name** | **str**|  | 
 **procedure** | [**Procedure**](Procedure.md)|  | [optional] 

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

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

