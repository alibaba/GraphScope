# GraphManagementApI

All URIs are relative to *{INTERACTIVE_ADMIN_ENDPOINT}*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**ListGraphs**](GraphManagementApi.md#ListGraphs) | **GET** /v1/graph | List all graphs' metadata |
| [**CreateGraph**](GraphManagementApi.md#CreateGraph) | **POST** /v1/graph | Create a new graph  |
| [**GetGraphMeta**](GraphManagementApi.md#GetGraphMeta) | **GET** /v1/graph/{graph_id} | Get the metadata for a graph identified by the specified graphId |
| [**GetGraphSchema**](GraphManagementApi.md#GetGraphSchema) | **GET** /v1/graph/{graph_id}/schema | Get the schema for a graph identified by the specified graphId |
| [**DeleteGraph**](GraphManagementApi.md#DeleteGraph) | **DELETE** /v1/graph/{graph_id} | Remove the graph identified by the specified graphId |
| [**GetGraphStatistics**](GraphManagementApi.md#GetGraphStatistics) | **GET** /v1/graph/{graph_id}/statistics |Get the statistics for a graph identified by the specified graphId  |
| [**BulkLoading**](GraphManagementApi.md#BulkLoading) | **POST** /v1/graph/{graph_id}/dataloading | Create a bulk loading job for the graph identified by the specified graphId |
| [**createVertexType**](GraphManagementApi.md#createVertexType) | **POST** /v1/graph/{graph_id}/schema/vertex | Add a vertex type for a graph identified by the specified graphId |
| [**updateVertexType**](GraphManagementApi.md#updateVertexType) | **PUT** /v1/graph/{graph_id}/schema/vertex | Update a vertex type for a graph identified by the specified graphId |
| [**deleteVertexType**](GraphManagementApi.md#deleteVertexType)| **Delete** /v1/graph/{graph_id}/schema/vertex | Delete a vertex type from a graph identified by the specified graphId |
| [**createEdgeType**](GraphManagementApi.md#createEdgeType) | **POST** /v1/graph/{graph_id}/schema/edge | Add an edge type for a graph identified by the specified graphId |
| [**updateEdgeType**](GraphManagementApi.md#updateEdgeType) | **PUT** /v1/graph/{graph_id}/schema/edge | Update an edge type for a graph identified by the specified graphId |
| [**deleteEdgeType**](GraphManagementApi.md#deleteEdgeType) | **Delete** /v1/graph/{graph_id}/schema/edge | Delete an edge type from a graph identified by the specified graphId |
| [**getSnapshotStatus**](./GraphManagementApi.md#getSnapshotStatus) | **GET** /v1/graph/{graph_id}/snapshot/{snapshot_id}/status | Get the status of the snapshot with specified snapshot_id on the specified graph



# **CreateGraph**
> [Result](./result.rst)[CreateGraphResponse] create_graph(create_graph_request)



Create a new graph

### Example


```python
from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

test_graph_def = {
    "name": "test_graph",
    "description": "This is a test graph",
    "schema": {
        "vertex_types": [
            {
                "type_name": "person",
                "properties": [
                    {
                        "property_name": "id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_name": "name",
                        "property_type": {"string": {"long_text": ""}},
                    },
                    {
                        "property_name": "age",
                        "property_type": {"primitive_type": "DT_SIGNED_INT32"},
                    },
                ],
                "primary_keys": ["id"],
            }
        ],
        "edge_types": [
            {
                "type_name": "knows",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "person",
                        "destination_vertex": "person",
                        "relation": "MANY_TO_MANY",
                    }
                ],
                "properties": [
                    {
                        "property_name": "weight",
                        "property_type": {"primitive_type": "DT_DOUBLE"},
                    }
                ],
                "primary_keys": [],
            }
        ],
    },
}
driver = Driver()
sess = driver.session()
create_graph_request = CreateGraphRequest.from_dict(test_graph_def)
resp = sess.create_graph(create_graph_request)
assert resp.is_ok()
graph_id = resp.get_value().graph_id
print("Graph id: ", graph_id)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **create_graph_request** | [**CreateGraphRequest**](CreateGraphRequest.md)|  | 

### Return type

[Reesult](./result.rst)[[**CreateGraphResponse**](CreateGraphResponse.md)]


### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**400** | BadRequest |  -  |
**500** | Internal error |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)



# **DeleteGraph**
> [Result](./result.rst)[str] delete_graph(graph_id)



Delete a graph by id.

### Example


```python
resp = sess.delete_graph(graph_id)
assert resp.is_ok()
print("graph deleted: ", resp)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**| The name of graph to delete | 

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
**200** | Successful operation |  -  |
**404** | Not Found |  -  |
**500** | Internal Error |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

# **GetGraphMeta**
> GetGraphResponse get_graph(graph_id)



Get the metadata of a graph.

### Example


```python
resp = sess.get_graph_meta(graph_id)
assert resp.is_ok()
print("Got metadata for graph {} is {}", graph_id, resp)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**| The id of graph | 

### Return type

[Result](./result.rst)[[**GetGraphResponse**](GetGraphResponse.md)]

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful operation |  -  |
**404** | Not found |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

# **GetGraphStatistics**
> GetGraphStatisticsResponse get_graph_statistic(graph_id)



Get the statics info of a graph, including number of vertices for each label, number of edges for each label.

### Example


```python
resp = sess.get_graph_statistics(graph_id)
assert resp.is_ok()
print("Got statistics for graph {} is {}", graph_id, resp)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**| The id of graph to get statistics | 

### Return type

[Result](./result.rst)[[**GetGraphStatisticsResponse**](GetGraphStatisticsResponse.md)]

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**500** | Server Internal Error |  -  |
**404** | Not Found |  -  |
**503** | Service Unavailable |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

# **GetGraphSchema**
> [Result](./result.rst)[GetGraphSchemaResponse] get_graph_schema(graph_id)



Get the schema of the graph by graph_id.

### Example


```python
resp = sess.get_graph_schema(graph_id)
assert resp.is_ok()
print("Got schema for graph {} is {}", graph_id, resp)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**| The name of graph to delete | 

### Return type

[Result](./result.rst)[[**GetGraphSchemaResponse**](GetGraphSchemaResponse.md)]

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

# **ListGraphs**
> [Result](./result.rst)[List[GetGraphResponse]] list_graphs()



List all graphs

### Example


```python
resp = sess.list_graphs()
assert resp.is_ok()
print("List all graphs", resp)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[Result](./result.rst)[[**List[GetGraphResponse]**](GetGraphResponse.md)]

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful operation |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)


# **BulkLoading**
> [Result](./result.rst)[JobResponse] bulk_loading(graph_id, schema_mapping)



Create a dataloading job

### Example


```python
test_graph_datasource = {
    "vertex_mappings": [
        {
            "type_name": "person",
            "inputs": ["@/path/to/person.csv"],
            "column_mappings": [
                {"column": {"index": 0, "name": "id"}, "property": "id"},
                {"column": {"index": 1, "name": "name"}, "property": "name"},
                {"column": {"index": 2, "name": "age"}, "property": "age"},
            ],
        }
    ],
    "edge_mappings": [
        {
            "type_triplet": {
                "edge": "knows",
                "source_vertex": "person",
                "destination_vertex": "person",
            },
            "inputs": [
                "@/path/to/person_knows_person.csv"
            ],
            "source_vertex_mappings": [
                {"column": {"index": 0, "name": "person.id"}, "property": "id"}
            ],
            "destination_vertex_mappings": [
                {"column": {"index": 1, "name": "person.id"}, "property": "id"}
            ],
            "column_mappings": [
                {"column": {"index": 2, "name": "weight"}, "property": "weight"}
            ],
        }
    ],
}
bulk_load_request = SchemaMapping.from_dict(test_graph_datasource)
resp = sess.bulk_loading(graph_id, bulk_load_request)
assert resp.is_ok()
job_id = resp.get_value().job_id
print("job id ", job_id)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**| The name of graph to do bulk loading. | 
 **schema_mapping** | [**SchemaMapping**](SchemaMapping.md)|  | 

### Return type

[Result](./result.rst)[[**JobResponse**](JobResponse.md)]

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

# **createVertexType**
> [Result](./result.rst)<String> create_vertex_type(graph_id, vertex_type_request)


Create a new vertex type in the specified graph by graph_id

### Example


```python
from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

driver = Driver()
sess = driver.session()

graph_id = "1"

vertex_type_request = CreateVertexType(
    type_name="new_person",
    properties=[
        CreatePropertyMeta(
            property_name="id",
            property_type=GSDataType.from_dict({"primitive_type": "DT_SIGNED_INT64"}),
        ),
        CreatePropertyMeta(
            property_name="name",
            property_type=GSDataType.from_dict({"string": {"long_text": ""}}),
        ),
    ],
    primary_keys=["id"],
)
# create a new vertex type
resp = sess.create_vertex_type(graph_id, vertex_type_request)
print(resp)
assert resp.is_ok()
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **vertex_type_request** | [**CreateVertexType**](CreateVertexType.md) | the vertex type to create |

### Return type

[Result](./result.rst)[[**str**]]

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**400** | Invalid input parameters |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

# **updateVertexType**
> [Result](./result.rst)<String> update_vertex_type(graph_id, vertex_type_request)


Update a vertex type in the specified graph by graph_id

### Example


```python
from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

driver = Driver()
sess = driver.session()

graph_id = "1"

vertex_type_request = CreateVertexType(
    type_name="new_person",
    properties=[
        CreatePropertyMeta(
            property_name="age",
            property_type=GSDataType.from_dict({"primitive_type": "DT_SIGNED_INT32"}),
        ),
    ],
    primary_keys=["id"],
)
# update vertex type by adding an age property
resp = sess.update_vertex_type(graph_id, vertex_type_request)
print(resp)
assert resp.is_ok()
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **vertex_type_request** | [**CreateVertexType**](CreateVertexType.md) | the vertex type to update | only support to update the vertex type by adding extra properties in Insight

### Return type

[Result](./result.rst)[[**str**]]

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**400** | Invalid input parameters |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

# **deleteVertexType**
> [Result](./result.rst)<String> delete_vertex_type(graph_id, vertex_type_name)


Delete a vertex type by name in the specified graph by graph_id

### Example


```python
from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

driver = Driver()
sess = driver.session()

graph_id = "1"

vertex_type_name = "new_person"
# delete vertex type by name
resp = sess.delete_vertex_type(graph_id, vertex_type_name)
print(resp)
assert resp.is_ok()
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **vertex_type_name** | **str** | the vertex type to delete | 

### Return type

[Result](./result.rst)[[**str**]]

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**400** | Invalid input parameters |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

# **createEdgeType**
> [Result](./result.rst)<String> create_edge_type(graph_id, edge_type_request)


Create a new edge type in the specified graph by graph_id

### Example


```python
from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

driver = Driver()
sess = driver.session()

graph_id = "1"

edge_type_request = CreateEdgeType(
    type_name="new_knows",
    vertex_type_pair_relations=[
        BaseEdgeTypeVertexTypePairRelationsInner(
            source_vertex="new_person",
            destination_vertex="new_person",
            relation="MANY_TO_MANY",
        )
    ],
    properties=[
        CreatePropertyMeta(
            property_name="weight",
            property_type=GSDataType.from_dict({"primitive_type": "DT_DOUBLE"}),
        )
    ],
)
# create a new edge type
resp = sess.create_edge_type(graph_id, edge_type_request)
print(resp)
assert resp.is_ok()
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **edge_type_request** | [**CreateEdgeType**](CreateEdgeType.md) | the edge type to create | support the inclusion of primary keys (optional) in Insight

### Return type

[Result](./result.rst)[[**str**]]

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**400** | Invalid input parameters |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

# **updateEdgeType**
> [Result](./result.rst)<String> update_edge_type(graph_id, edge_type_request)


Create a new edge type in the specified graph by graph_id

### Example


```python
from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

driver = Driver()
sess = driver.session()

graph_id = "1"

edge_type_request = CreateEdgeType(
    type_name="new_knows",
    properties=[
        CreatePropertyMeta(
            property_name="new_weight",
            property_type=GSDataType.from_dict({"primitive_type": "DT_DOUBLE"}),
        )
    ],
)
# update the edge type by adding a new property new_weight
resp = sess.create_edge_type(graph_id, edge_type_request)
print(resp)
assert resp.is_ok()
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **edge_type_request** | [**CreateEdgeType**](CreateEdgeType.md) | the edge type to update | only support to update the edge type by adding extra properties in Insight

### Return type

[Result](./result.rst)[[**str**]]

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**400** | Invalid input parameters |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

# **deleteEdgeType**
> [Result](./result.rst)<String> delete_edge_type(graph_id, delete_edge_type_request)


Delete an edge type from the specified graph by graph_id

### Example


```python
from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

driver = Driver()
sess = driver.session()

graph_id = "1"

# delete the edge type by specifying edge_type_name, source_vertex_type_name, and destination_vertex_type_name
edge_type_name = "new_knows"
src_type_name = "new_person"
dst_type_name = "new_person"
resp = sess.delete_edge_type(graph_id, edge_type_name, src_type_name, dst_type_name)
print(resp)
assert resp.is_ok()
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **edge_type_name** | **str** | | 
 **source_vertex_type_name** | **str** | | 
 **destination_vertex_type_name** | **str** | | 


### Return type

[Result](./result.rst)[[**str**]]

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**400** | Invalid input parameters |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)

# **getSnapshotStatus**
> [Result](./result.rst)[SnapshotStatus]  get_snapshot_status(graph_id, snapshot_id)


 Get the status of a snapshot with the specified snapshot id

### Example


```python
from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

driver = Driver()
sess = driver.session()

graph_id = "1"
# A snapshot_id for test. The ddl operations, e.g., create_vertex_type, will return the snapshot id.
snapshot_id = "123"

# get the status of specified snapshot_id, to see if it is available
resp = sess.get_snapshot_status(graph_id, snapshot_id)
print(resp)
assert resp.is_ok()
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_id** | **str**|  | 
 **snapshot_id** | **str** | | 


### Return type

[Result](./result.rst)[[**SnapshotStatus**](SnapshotStatus.md)]

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**400** | Invalid input parameters |  -  |
**500** | Server internal error |  -  |

[[Back to top]](#) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to python_sdk]](python_sdk.md)