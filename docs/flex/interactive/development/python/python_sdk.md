# Python SDK Reference

The Interactive Python SDK Reference is a comprehensive guide designed to assist developers in integrating the Interactive service into their Python applications. This SDK allows users to seamlessly connect to Interactive and harness its powerful features for graph management, stored procedure management, and query execution.

## Requirements.

Python 3.7+

## Installation & Usage
### pip install

```bash
pip3 install interactive-sdk
```

Then import the package:
```python
import interactive_sdk
```

### Setuptools

Install via [Setuptools](http://pypi.python.org/pypi/setuptools).

```sh
python setup.py install --user
```
(or `sudo python setup.py install` to install the package for all users)

Then import the package:
```python
import interactive_sdk
```

### Tests

Execute `pytest` to run the tests.

## Getting Started

First, install and start the interactive service via [Interactive Getting Started](https://graphscope.io/docs/flex/interactive/getting_started), and you will get the endpoint for the Interactive service.

```bash
Interactive Service is listening at ${INTERACTIVE_ENDPOINT}.
```

Then, connect to the interactive endpoint, and try to run a simple query with following code.

```python

from interactive_sdk.client.driver import Driver

# replace endpoint with the actual interactive endpoint, this is mock server just for testing.
interactive_endpoint='https://virtserver.swaggerhub.com/GRAPHSCOPE/interactive/1.0.0/'
driver = Driver(endpoint=interactive_endpoint)

# Interactive will initially start on a builtin modern graph. You can run a simple cypher query
with driver.getNeo4jSession() as session:
    resp = session.run('MATCH(n) RETURN COUNT(n);')
    for record in resp:
        print('record: ', record)
        # record:  <Record $f0=6>
```

For a more detailed example, please refer to [Python SDK Example](https://github.com/alibaba/GraphScope/flex/interactive/sdk/examples/python/basic_example.py).


## Documentation for Service APIs

All URIs are relative to `${INTERACTIVE_ENDPOINT}`

Class | Method | HTTP request | Description
------------ | ------------- | ------------- | -------------
*AdminServiceGraphManagementApi* | [**create_dataloading_job**](./AdminServiceGraphManagementApi.md#create_dataloading_job) | **POST** /v1/graph/{graph_id}/dataloading | 
*AdminServiceGraphManagementApi* | [**create_graph**](./AdminServiceGraphManagementApi.md#create_graph) | **POST** /v1/graph | 
*AdminServiceGraphManagementApi* | [**delete_graph**](./AdminServiceGraphManagementApi.md#delete_graph) | **DELETE** /v1/graph/{graph_id} | 
*AdminServiceGraphManagementApi* | [**get_graph**](./AdminServiceGraphManagementApi.md#get_graph) | **GET** /v1/graph/{graph_id} | 
*AdminServiceGraphManagementApi* | [**get_schema**](./AdminServiceGraphManagementApi.md#get_schema) | **GET** /v1/graph/{graph_id}/schema | 
*AdminServiceGraphManagementApi* | [**list_graphs**](./AdminServiceGraphManagementApi.md#list_graphs) | **GET** /v1/graph | 
*AdminServiceJobManagementApi* | [**delete_job_by_id**](./AdminServiceJobManagementApi.md#delete_job_by_id) | **DELETE** /v1/job/{job_id} | 
*AdminServiceJobManagementApi* | [**get_job_by_id**](./AdminServiceJobManagementApi.md#get_job_by_id) | **GET** /v1/job/{job_id} | 
*AdminServiceJobManagementApi* | [**list_jobs**](./AdminServiceJobManagementApi.md#list_jobs) | **GET** /v1/job | 
*AdminServiceProcedureManagementApi* | [**create_procedure**](./AdminServiceProcedureManagementApi.md#create_procedure) | **POST** /v1/graph/{graph_id}/procedure | 
*AdminServiceProcedureManagementApi* | [**delete_procedure**](./AdminServiceProcedureManagementApi.md#delete_procedure) | **DELETE** /v1/graph/{graph_id}/procedure/{procedure_id} | 
*AdminServiceProcedureManagementApi* | [**get_procedure**](./AdminServiceProcedureManagementApi.md#get_procedure) | **GET** /v1/graph/{graph_id}/procedure/{procedure_id} | 
*AdminServiceProcedureManagementApi* | [**list_procedures**](./AdminServiceProcedureManagementApi.md#list_procedures) | **GET** /v1/graph/{graph_id}/procedure | 
*AdminServiceProcedureManagementApi* | [**update_procedure**](./AdminServiceProcedureManagementApi.md#update_procedure) | **PUT** /v1/graph/{graph_id}/procedure/{procedure_id} | 
*AdminServiceServiceManagementApi* | [**get_service_status**](./AdminServiceServiceManagementApi.md#get_service_status) | **GET** /v1/service/status | 
*AdminServiceServiceManagementApi* | [**restart_service**](./AdminServiceServiceManagementApi.md#restart_service) | **POST** /v1/service/restart | 
*AdminServiceServiceManagementApi* | [**start_service**](./AdminServiceServiceManagementApi.md#start_service) | **POST** /v1/service/start | 
*AdminServiceServiceManagementApi* | [**stop_service**](./AdminServiceServiceManagementApi.md#stop_service) | **POST** /v1/service/stop | 
*GraphServiceEdgeManagementApi* | [**add_edge**](./GraphServiceEdgeManagementApi.md#add_edge) | **POST** /v1/graph/{graph_id}/edge | Add edge to the graph
*GraphServiceEdgeManagementApi* | [**delete_edge**](./GraphServiceEdgeManagementApi.md#delete_edge) | **DELETE** /v1/graph/{graph_id}/edge | Remove edge from the graph
*GraphServiceEdgeManagementApi* | [**get_edge**](./GraphServiceEdgeManagementApi.md#get_edge) | **GET** /v1/graph/{graph_id}/edge | Get the edge&#39;s properties with src and dst vertex primary keys.
*GraphServiceEdgeManagementApi* | [**update_edge**](./GraphServiceEdgeManagementApi.md#update_edge) | **PUT** /v1/graph/{graph_id}/edge | Update edge&#39;s property
*GraphServiceVertexManagementApi* | [**add_vertex**](./GraphServiceVertexManagementApi.md#add_vertex) | **POST** /v1/graph/{graph_id}/vertex | Add vertex to the graph
*GraphServiceVertexManagementApi* | [**delete_vertex**](./GraphServiceVertexManagementApi.md#delete_vertex) | **DELETE** /v1/graph/{graph_id}/vertex | Remove vertex from the graph
*GraphServiceVertexManagementApi* | [**get_vertex**](./GraphServiceVertexManagementApi.md#get_vertex) | **GET** /v1/graph/{graph_id}/vertex | Get the vertex&#39;s properties with vertex primary key.
*GraphServiceVertexManagementApi* | [**update_vertex**](./GraphServiceVertexManagementApi.md#update_vertex) | **PUT** /v1/graph/{graph_id}/vertex | Update vertex&#39;s property
*QueryServiceApi* | [**proc_call**](./QueryServiceApi.md#proc_call) | **POST** /v1/graph/{graph_id}/query | run queries on graph


## Documentation for Data Structures

 - [BaseEdgeType](./BaseEdgeType.md)
 - [BaseEdgeTypeVertexTypePairRelationsInner](./BaseEdgeTypeVertexTypePairRelationsInner.md)
 - [BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams](./BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams.md)
 - [BasePropertyMeta](./BasePropertyMeta.md)
 - [BaseVertexType](./BaseVertexType.md)
 - [BaseVertexTypeXCsrParams](./BaseVertexTypeXCsrParams.md)
 - [Collection](./Collection.md)
 - [CollectiveResults](./CollectiveResults.md)
 - [Column](./Column.md)
 - [ColumnMapping](./ColumnMapping.md)
 - [CreateEdgeType](./CreateEdgeType.md)
 - [CreateGraphRequest](./CreateGraphRequest.md)
 - [CreateGraphResponse](./CreateGraphResponse.md)
 - [CreateGraphSchemaRequest](./CreateGraphSchemaRequest.md)
 - [CreateProcedureRequest](./CreateProcedureRequest.md)
 - [CreateProcedureResponse](./CreateProcedureResponse.md)
 - [CreatePropertyMeta](./CreatePropertyMeta.md)
 - [CreateVertexType](./CreateVertexType.md)
 - [EdgeData](./EdgeData.md)
 - [EdgeMapping](./EdgeMapping.md)
 - [EdgeMappingDestinationVertexMappingsInner](./EdgeMappingDestinationVertexMappingsInner.md)
 - [EdgeMappingSourceVertexMappingsInner](./EdgeMappingSourceVertexMappingsInner.md)
 - [EdgeMappingSourceVertexMappingsInnerColumn](./EdgeMappingSourceVertexMappingsInnerColumn.md)
 - [EdgeMappingTypeTriplet](./EdgeMappingTypeTriplet.md)
 - [EdgeRequest](./EdgeRequest.md)
 - [Element](./Element.md)
 - [FixedChar](./FixedChar.md)
 - [FixedCharChar](./FixedCharChar.md)
 - [GSDataType](./GSDataType.md)
 - [GetEdgeType](./GetEdgeType.md)
 - [GetGraphResponse](./GetGraphResponse.md)
 - [GetGraphSchemaResponse](./GetGraphSchemaResponse.md)
 - [GetProcedureResponse](./GetProcedureResponse.md)
 - [GetPropertyMeta](./GetPropertyMeta.md)
 - [GetVertexType](./GetVertexType.md)
 - [JobResponse](./JobResponse.md)
 - [JobStatus](./JobStatus.md)
 - [KeyValue](./KeyValue.md)
 - [LongText](./LongText.md)
 - [ModelProperty](./ModelProperty.md)
 - [Parameter](./Parameter.md)
 - [PrimitiveType](./PrimitiveType.md)
 - [PropertyArray](./PropertyArray.md)
 - [QueryRequest](./QueryRequest.md)
 - [Record](./Record.md)
 - [SchemaMapping](./SchemaMapping.md)
 - [SchemaMappingLoadingConfig](./SchemaMappingLoadingConfig.md)
 - [SchemaMappingLoadingConfigFormat](./SchemaMappingLoadingConfigFormat.md)
 - [ServiceStatus](./ServiceStatus.md)
 - [StartServiceRequest](./StartServiceRequest.md)
 - [StoredProcedureMeta](./StoredProcedureMeta.md)
 - [StringType](./StringType.md)
 - [StringTypeString](./StringTypeString.md)
 - [TemporalType](./TemporalType.md)
 - [TimeStampType](./TimeStampType.md)
 - [TypedValue](./TypedValue.md)
 - [UpdateProcedureRequest](./UpdateProcedureRequest.md)
 - [VarChar](./VarChar.md)
 - [VarCharVarChar](./VarCharVarChar.md)
 - [VertexData](./VertexData.md)
 - [VertexMapping](./VertexMapping.md)
 - [VertexRequest](./VertexRequest.md)


<a id="documentation-for-authorization"></a>
## Documentation For Authorization

Authentication is not supported yet, and we will be introducing authorization-related implementation in the near future.



