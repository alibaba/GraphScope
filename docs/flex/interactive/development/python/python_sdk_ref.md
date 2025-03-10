# Python SDK Reference

The Interactive Python SDK allows users to seamlessly connect to Interactive or Insight(Groot) and harness their powerful features for graph management, stored procedure management, and query execution.

## Documentation for Service APIs

The Service APIs in interactive SDK are divided into five categories.
- GraphManagementApi
- ProcedureManagementApi
- JobManagementApi
- ServiceManagementApi
- QueryServiceApi
- VertexApi
- EdgeApi

All URIs are relative to `${INTERACTIVE_ADMIN_ENDPOINT}`

Class | Method | HTTP request | Description | Interactive Support | Insight Support
------------ | ------------- | ------------- | ------------- | ------------- | -------------
*GraphManagementApi* | [**BulkLoading**](./GraphManagementApi.md#Bulkloading) | **POST** /v1/graph/{graph_id}/dataloading | | [x] | [ ]
*GraphManagementApi* | [**CreateGraph**](./GraphManagementApi.md#CreateGraph) | **POST** /v1/graph | | [x] | [x]
*GraphManagementApi* | [**DeleteGraph**](./GraphManagementApi.md#DeleteGraph) | **DELETE** /v1/graph/{graph_id} | | [x] | [x]
*GraphManagementApi* | [**GetGraphMeta**](./GraphManagementApi.md#GetGraphMeta) | **GET** /v1/graph/{graph_id} | | [x] | [ ]
*GraphManagementApi* | [**GetGraphSchema**](./GraphManagementApi.md#GetGraphSchema) | **GET** /v1/graph/{graph_id}/schema | | [x] | [x]
*GraphManagementApi*| [**createVertexType**](./GraphManagementApi.md#createVertexType) | **POST** /v1/graph/{graph_id}/schema/vertex | |  [ ] | [x]
*GraphManagementApi*| [**updateVertexType**](./GraphManagementApi.md#updateVertexType) | **PUT** /v1/graph/{graph_id}/schema/vertex | |  [ ] | [x]
*GraphManagementApi*| [**deleteVertexType**](./GraphManagementApi.md#deleteVertexType)| **Delete** /v1/graph/{graph_id}/schema/vertex | |  [ ] | [x]
*GraphManagementApi*| [**createEdgeType**](./GraphManagementApi.md#createEdgeType) | **POST** /v1/graph/{graph_id}/schema/edge | |  [ ] | [x]
*GraphManagementApi*| [**updateEdgeType**](./GraphManagementApi.md#updateEdgeType) | **PUT** /v1/graph/{graph_id}/schema/edge | |  [ ] | [x]
*GraphManagementApi*| [**deleteEdgeType**](./GraphManagementApi.md#deleteEdgeType) | **Delete** /v1/graph/{graph_id}/schema/edge | |  [ ] | [x]
*GraphManagementApi* | [**ListGraphs**](./GraphManagementApi.md#ListGraphs) | **GET** /v1/graph | | [x] | [ ]
*GraphManagementApi* | [**GetGraphStatistics**](./GraphManagementApi.md#GetGraphStatistics) | **GET** /v1/graph/{graph_id}/statistics | | [x] | [ ]
*GraphManagementApi*| [**getSnapshotStatus**](./GraphManagementApi.md#getSnapshotStatus) | **GET** /v1/graph/{graph_id}/snapshot/{snapshot_id}/status | |  [ ] | [x]
*JobManagementApi* | [**CancelJob**](./JobManagementApi.md#CancelJob) | **DELETE** /v1/job/{job_id} | | [x] | [ ]
*JobManagementApi* | [**GetJobById**](./JobManagementApi.md#GetJobById) | **GET** /v1/job/{job_id} | | [x] | [ ]
*JobManagementApi* | [**ListJobs**](./JobManagementApi.md#ListJobs) | **GET** /v1/job | | [x] | [ ]
*ProcedureManagementApi* | [**CreateProcedure**](./ProcedureManagementApi.md#CreateProcedure) | **POST** /v1/graph/{graph_id}/procedure | | [x] | [ ]
*ProcedureManagementApi* | [**DeleteProcedure**](./ProcedureManagementApi.md#DeleteProcedure) | **DELETE** /v1/graph/{graph_id}/procedure/{procedure_id} | | [x] | [ ]
*ProcedureManagementApi* | [**GetProcedure**](./ProcedureManagementApi.md#GetProcedure) | **GET** /v1/graph/{graph_id}/procedure/{procedure_id} | | [x] | [ ]
*ProcedureManagementApi* | [**ListProcedures**](./ProcedureManagementApi.md#ListProcedures) | **GET** /v1/graph/{graph_id}/procedure | | [x] | [ ]
*ProcedureManagementApi* | [**UpdateProcedure**](./ProcedureManagementApi.md#UpdateProcedure) | **PUT** /v1/graph/{graph_id}/procedure/{procedure_id} | | [x] | [ ]
*ServiceManagementApi* | [**GetServiceStatus**](./ServiceManagementApi.md#GetServiceStatus) | **GET** /v1/service/status | | [x] | [x]
*ServiceManagementApi* | [**RestartService**](./ServiceManagementApi.md#RestartService) | **POST** /v1/service/restart | | [x] | [ ]
*ServiceManagementApi* | [**StartService**](./ServiceManagementApi.md#StartService) | **POST** /v1/service/start | | [x] | [ ]
*ServiceManagementApi* | [**StopService**](./ServiceManagementApi.md#StopService) | **POST** /v1/service/stop | | [x] | [ ]
*QueryServiceApi* | [**CallProcedure**](./QueryServiceApi.md#CallProcedure) | **POST** /v1/graph/{graph_id}/query | | [x] | [ ]
*QueryServiceApi* | [**CallProcedureOnCurrentGraph**](./QueryServiceApi.md#CallProcedureOnCurrentGraph) | **POST** /v1/graph/current/query | | [x] | [ ]
*VertexApi* | [**addVertex**](./VertexApi.md#addVertex) | **POST** /v1/graph/{graph_id}/vertex | Add vertex to the graph | [x] | [x]
*VertexApi* | [**getVertex**](./VertexApi.md#getVertex) | **GET** /v1/graph/{graph_id}/vertex | Get the vertex&#39;s properties with vertex primary key. | [x] | [ ]
*VertexApi* | [**updateVertex**](./VertexApi.md#updateVertex) | **PUT** /v1/graph/{graph_id}/vertex | Update vertex&#39;s property | [x] | [x]
*VertexApi* | [**deleteVertex**](./VertexApi.md#deleteVertex) | **DELETE** /v1/graph/{graph_id}/vertex | Delete vertex from the graph | [ ] | [x]
*EdgeApi* | [**addEdge**](./EdgeApi.md#addEdge) | **POST** /v1/graph/{graph_id}/edge | Add edge to the graph | [x] | [x]
*EdgeApi* | [**getEdge**](./EdgeApi.md#getEdge) | **GET** /v1/graph/{graph_id}/edge | Get the edge&#39;s properties with src and dst vertex primary keys. | [x] | [ ]
*EdgeApi* | [**updateEdge**](./EdgeApi.md#updateEdge) | **PUT** /v1/graph/{graph_id}/edge | Update edge&#39;s property | [x] | [x]
*EdgeApi* | [**deleteEdge**](./EdgeApi.md#deleteEdge) | **DELETE** /v1/graph/{graph_id}/edge | Delete the edge from the graph | [ ] | [x]

## Documentation for Utilities APIs

- [Driver](./driver.rst)
- [Session](./session.rst)
- [Result](./result.rst)
- [Status](./status.rst)
- [Encoder&Decoder](./encoder.rst)

## Documentation for Data Structures

 - [APIResponseWithCode](./APIResponseWithCode.md)
 - [BaseEdgeType](./BaseEdgeType.md)
 - [BaseEdgeTypeVertexTypePairRelationsInner](./BaseEdgeTypeVertexTypePairRelationsInner.md)
 - [BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams](./BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams.md)
 - [BasePropertyMeta](./BasePropertyMeta.md)
 - [BaseVertexType](./BaseVertexType.md)
 - [BaseVertexTypeXCsrParams](./BaseVertexTypeXCsrParams.md)
 - [ColumnMapping](./ColumnMapping.md)
 - [CreateEdgeType](./CreateEdgeType.md)
 - [CreateGraphRequest](./CreateGraphRequest.md)
 - [CreateGraphResponse](./CreateGraphResponse.md)
 - [CreateGraphSchemaRequest](./CreateGraphSchemaRequest.md)
 - [CreateProcedureRequest](./CreateProcedureRequest.md)
 - [CreateProcedureResponse](./CreateProcedureResponse.md)
 - [CreatePropertyMeta](./CreatePropertyMeta.md)
 - [CreateVertexType](./CreateVertexType.md)
 - [DateType](./DateType.md)
 - [EdgeData](./EdgeData.md)
 - [EdgeMapping](./EdgeMapping.md)
 - [EdgeMappingDestinationVertexMappingsInner](./EdgeMappingDestinationVertexMappingsInner.md)
 - [EdgeMappingSourceVertexMappingsInner](./EdgeMappingSourceVertexMappingsInner.md)
 - [EdgeMappingSourceVertexMappingsInnerColumn](./EdgeMappingSourceVertexMappingsInnerColumn.md)
 - [EdgeMappingTypeTriplet](./EdgeMappingTypeTriplet.md)
 - [EdgeRequest](./EdgeRequest.md)
 - [EdgeStatistics](./EdgeStatistics.md)
 - [FixedChar](./FixedChar.md)
 - [FixedCharChar](./FixedCharChar.md)
 - [GSDataType](./GSDataType.md)
 - [GetEdgeType](./GetEdgeType.md)
 - [GetGraphResponse](./GetGraphResponse.md)
 - [GetGraphSchemaResponse](./GetGraphSchemaResponse.md)
 - [GetGraphStatisticsResponse](./GetGraphStatisticsResponse.md)
 - [GetProcedureResponse](./GetProcedureResponse.md)
 - [GetPropertyMeta](./GetPropertyMeta.md)
 - [GetVertexType](./GetVertexType.md)
 - [JobResponse](./JobResponse.md)
 - [JobStatus](./JobStatus.md)
 - [LongText](./LongText.md)
 - [Parameter](./Parameter.md)
 - [PrimitiveType](./PrimitiveType.md)
 - [Property](./Property.md)
 - [QueryRequest](./QueryRequest.md)
 - [SchemaMapping](./SchemaMapping.md)
 - [SchemaMappingLoadingConfig](./SchemaMappingLoadingConfig.md)
 - [SchemaMappingLoadingConfigDataSource](./SchemaMappingLoadingConfigDataSource.md)
 - [SchemaMappingLoadingConfigFormat](./SchemaMappingLoadingConfigFormat.md)
 - [SchemaMappingLoadingConfigXCsrParams](./SchemaMappingLoadingConfigXCsrParams.md)
 - [ServiceStatus](./ServiceStatus.md)
 - [StartServiceRequest](./StartServiceRequest.md)
 - [StoredProcedureMeta](./StoredProcedureMeta.md)
 - [StringType](./StringType.md)
 - [StringTypeString](./StringTypeString.md)
 - [TemporalType](./TemporalType.md)
 - [TemporalTypeTemporal](./TemporalTypeTemporal.md)
 - [TimeStampType](./TimeStampType.md)
 - [TypedValue](./TypedValue.md)
 - [UpdateProcedureRequest](./UpdateProcedureRequest.md)
 - [UploadFileResponse](./UploadFileResponse.md)
 - [VarChar](./VarChar.md)
 - [VarCharVarChar](./VarCharVarChar.md)
 - [VertexData](./VertexData.md)
 - [VertexEdgeRequest](./VertexEdgeRequest.md)
 - [VertexMapping](./VertexMapping.md)
 - [VertexRequest](./VertexRequest.md)
 - [VertexStatistics](./VertexStatistics.md)
 - [VertexTypePairStatistics](./VertexTypePairStatistics.md)


<a id="documentation-for-authorization"></a>
## Documentation For Authorization

Authentication is not supported yet, and we will be introducing authorization-related implementation in the near future.



