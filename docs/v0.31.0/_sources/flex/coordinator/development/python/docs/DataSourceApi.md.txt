# graphscope.flex.rest.DataSourceApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**bind_datasource_in_batch**](DataSourceApi.md#bind_datasource_in_batch) | **POST** /api/v1/graph/{graph_id}/datasource | 
[**get_datasource_by_id**](DataSourceApi.md#get_datasource_by_id) | **GET** /api/v1/graph/{graph_id}/datasource | 
[**unbind_edge_datasource**](DataSourceApi.md#unbind_edge_datasource) | **DELETE** /api/v1/graph/{graph_id}/datasource/edge/{type_name} | 
[**unbind_vertex_datasource**](DataSourceApi.md#unbind_vertex_datasource) | **DELETE** /api/v1/graph/{graph_id}/datasource/vertex/{type_name} | 


# **bind_datasource_in_batch**
> str bind_datasource_in_batch(graph_id, schema_mapping)



Bind data sources in batches

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.schema_mapping import SchemaMapping
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
    api_instance = graphscope.flex.rest.DataSourceApi(api_client)
    graph_id = 'graph_id_example' # str | 
    schema_mapping = graphscope.flex.rest.SchemaMapping() # SchemaMapping | 

    try:
        api_response = api_instance.bind_datasource_in_batch(graph_id, schema_mapping)
        print("The response of DataSourceApi->bind_datasource_in_batch:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DataSourceApi->bind_datasource_in_batch: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **schema_mapping** | [**SchemaMapping**](SchemaMapping.md)|  | 

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
**200** | Successful bind the data sources |  -  |
**400** | Bad request |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_datasource_by_id**
> SchemaMapping get_datasource_by_id(graph_id)



Get data source by ID

### Example


```python
import graphscope.flex.rest
from graphscope.flex.rest.models.schema_mapping import SchemaMapping
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
    api_instance = graphscope.flex.rest.DataSourceApi(api_client)
    graph_id = 'graph_id_example' # str | 

    try:
        api_response = api_instance.get_datasource_by_id(graph_id)
        print("The response of DataSourceApi->get_datasource_by_id:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DataSourceApi->get_datasource_by_id: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 

### Return type

[**SchemaMapping**](SchemaMapping.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful returned all data sources |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **unbind_edge_datasource**
> str unbind_edge_datasource(graph_id, type_name, source_vertex_type, destination_vertex_type)



Unbind datas ource on an edge type

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
    api_instance = graphscope.flex.rest.DataSourceApi(api_client)
    graph_id = 'graph_id_example' # str | 
    type_name = 'type_name_example' # str | 
    source_vertex_type = 'source_vertex_type_example' # str | 
    destination_vertex_type = 'destination_vertex_type_example' # str | 

    try:
        api_response = api_instance.unbind_edge_datasource(graph_id, type_name, source_vertex_type, destination_vertex_type)
        print("The response of DataSourceApi->unbind_edge_datasource:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DataSourceApi->unbind_edge_datasource: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **type_name** | **str**|  | 
 **source_vertex_type** | **str**|  | 
 **destination_vertex_type** | **str**|  | 

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
**200** | Successfully unbind the data source |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **unbind_vertex_datasource**
> str unbind_vertex_datasource(graph_id, type_name)



Unbind data source on a vertex type

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
    api_instance = graphscope.flex.rest.DataSourceApi(api_client)
    graph_id = 'graph_id_example' # str | 
    type_name = 'type_name_example' # str | 

    try:
        api_response = api_instance.unbind_vertex_datasource(graph_id, type_name)
        print("The response of DataSourceApi->unbind_vertex_datasource:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DataSourceApi->unbind_vertex_datasource: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **type_name** | **str**|  | 

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
**200** | Successfully unbind the data source |  -  |
**500** | Server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

