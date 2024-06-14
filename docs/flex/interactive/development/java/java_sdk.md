# Java SDK Reference

The Interactive Java SDK Reference is a comprehensive guide for developers looking to integrate the Interactive service into their Java applications. This SDK allows users to seamlessly connect to Interactive and leverage its powerful features for graph management, stored procedure management, and query execution.


## Requirements

Building the API client library requires:
1. Java 1.8+
2. Maven (3.8.3+)/Gradle (7.2+)

## Installation

To install the API client library to your local Maven repository, simply execute:

```shell
git clone https://github.com/alibaba/GraphScope.git
cd GraphScope/flex/interactive/sdk/java
mvn clean install
```

To deploy it to a remote Maven repository instead, configure the settings of the repository and execute:

```shell
mvn clean deploy
```

Refer to the [OSSRH Guide](http://central.sonatype.org/pages/ossrh-guide.html) for more information.

### Maven users

Add this dependency to your project's POM:

```xml
<dependency>
  <groupId>com.alibaba.graphscope</groupId>
  <artifactId>interactive-sdk</artifactId>
  <version>0.3</version>
  <scope>compile</scope>
</dependency>
```

### Others

At first generate the JAR by executing:

```shell
mvn clean package
```

Then manually install the following JARs:

* `target/interactive-sdk-0.3.jar`
* `target/lib/*.jar`

## Getting Started

First, install and start the interactive service via [Interactive Getting Started](https://graphscope.io/docs/flex/interactive/getting_started), and you will get the endpoint for the Interactive service.

```bash
Interactive Service is listening at ${INTERACTIVE_ENDPOINT}.
```

Then, connect to the interactive endpoint, and try to run a simple query with following code.

```java
package com.alibaba.graphscope;

import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;

public class GettingStarted {
    public static void main(String[] args) {
        //get endpoint from command line
        if (args.length != 1) {
            System.out.println("Usage: <endpoint>");
            return;
        }
        String endpoint = args[0];
        Driver driver = Driver.connect(endpoint);
        Session session = driver.session();

        // start a query
        // run cypher query
        try (org.neo4j.driver.Session neo4jSession = driver.getNeo4jSession()) {
            org.neo4j.driver.Result result = neo4jSession.run("MATCH(a) return COUNT(a);");
            System.out.println("result: " + result.toString());
        }
        return;
    }
}
```

For more a more detailed example, please refer to [Java SDK Example](https://github.com/alibaba/GraphScope/flex/interactive/sdk/examples/java/interactive-sdk-example/)

## Documentation for Service APIs

All URIs are relative to `${INTERACTIVE_ENDPOINT}`

Class | Method | HTTP request | Description
------------ | ------------- | ------------- | -------------
*AdminServiceGraphManagementApi* | [**createDataloadingJob**](./AdminServiceGraphManagementApi.md#createDataloadingJob) | **POST** /v1/graph/{graph_id}/dataloading | 
*AdminServiceGraphManagementApi* | [**createGraph**](./AdminServiceGraphManagementApi.md#createGraph) | **POST** /v1/graph | 
*AdminServiceGraphManagementApi* | [**deleteGraph**](./AdminServiceGraphManagementApi.md#deleteGraph) | **DELETE** /v1/graph/{graph_id} | 
*AdminServiceGraphManagementApi* | [**getGraph**](./AdminServiceGraphManagementApi.md#getGraph) | **GET** /v1/graph/{graph_id} | 
*AdminServiceGraphManagementApi* | [**getSchema**](./AdminServiceGraphManagementApi.md#getSchema) | **GET** /v1/graph/{graph_id}/schema | 
*AdminServiceGraphManagementApi* | [**listGraphs**](./AdminServiceGraphManagementApi.md#listGraphs) | **GET** /v1/graph | 
*AdminServiceJobManagementApi* | [**deleteJobById**](./AdminServiceJobManagementApi.md#deleteJobById) | **DELETE** /v1/job/{job_id} | 
*AdminServiceJobManagementApi* | [**getJobById**](./AdminServiceJobManagementApi.md#getJobById) | **GET** /v1/job/{job_id} | 
*AdminServiceJobManagementApi* | [**listJobs**](./AdminServiceJobManagementApi.md#listJobs) | **GET** /v1/job | 
*AdminServiceProcedureManagementApi* | [**createProcedure**](./AdminServiceProcedureManagementApi.md#createProcedure) | **POST** /v1/graph/{graph_id}/procedure | 
*AdminServiceProcedureManagementApi* | [**deleteProcedure**](./AdminServiceProcedureManagementApi.md#deleteProcedure) | **DELETE** /v1/graph/{graph_id}/procedure/{procedure_id} | 
*AdminServiceProcedureManagementApi* | [**getProcedure**](./AdminServiceProcedureManagementApi.md#getProcedure) | **GET** /v1/graph/{graph_id}/procedure/{procedure_id} | 
*AdminServiceProcedureManagementApi* | [**listProcedures**](./AdminServiceProcedureManagementApi.md#listProcedures) | **GET** /v1/graph/{graph_id}/procedure | 
*AdminServiceProcedureManagementApi* | [**updateProcedure**](./AdminServiceProcedureManagementApi.md#updateProcedure) | **PUT** /v1/graph/{graph_id}/procedure/{procedure_id} | 
*AdminServiceServiceManagementApi* | [**getServiceStatus**](./AdminServiceServiceManagementApi.md#getServiceStatus) | **GET** /v1/service/status | 
*AdminServiceServiceManagementApi* | [**restartService**](./AdminServiceServiceManagementApi.md#restartService) | **POST** /v1/service/restart | 
*AdminServiceServiceManagementApi* | [**startService**](./AdminServiceServiceManagementApi.md#startService) | **POST** /v1/service/start | 
*AdminServiceServiceManagementApi* | [**stopService**](./AdminServiceServiceManagementApi.md#stopService) | **POST** /v1/service/stop | 
*GraphServiceEdgeManagementApi* | [**addEdge**](./GraphServiceEdgeManagementApi.md#addEdge) | **POST** /v1/graph/{graph_id}/edge | Add edge to the graph
*GraphServiceEdgeManagementApi* | [**deleteEdge**](./GraphServiceEdgeManagementApi.md#deleteEdge) | **DELETE** /v1/graph/{graph_id}/edge | Remove edge from the graph
*GraphServiceEdgeManagementApi* | [**getEdge**](./GraphServiceEdgeManagementApi.md#getEdge) | **GET** /v1/graph/{graph_id}/edge | Get the edge&#39;s properties with src and dst vertex primary keys.
*GraphServiceEdgeManagementApi* | [**updateEdge**](./GraphServiceEdgeManagementApi.md#updateEdge) | **PUT** /v1/graph/{graph_id}/edge | Update edge&#39;s property
*GraphServiceVertexManagementApi* | [**addVertex**](./GraphServiceVertexManagementApi.md#addVertex) | **POST** /v1/graph/{graph_id}/vertex | Add vertex to the graph
*GraphServiceVertexManagementApi* | [**deleteVertex**](./GraphServiceVertexManagementApi.md#deleteVertex) | **DELETE** /v1/graph/{graph_id}/vertex | Remove vertex from the graph
*GraphServiceVertexManagementApi* | [**getVertex**](./GraphServiceVertexManagementApi.md#getVertex) | **GET** /v1/graph/{graph_id}/vertex | Get the vertex&#39;s properties with vertex primary key.
*GraphServiceVertexManagementApi* | [**updateVertex**](./GraphServiceVertexManagementApi.md#updateVertex) | **PUT** /v1/graph/{graph_id}/vertex | Update vertex&#39;s property
*QueryServiceApi* | [**procCall**](./QueryServiceApi.md#procCall) | **POST** /v1/graph/{graph_id}/query | run queries on graph


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
 - [Parameter](./Parameter.md)
 - [PrimitiveType](./PrimitiveType.md)
 - [Property](./Property.md)
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
## Documentation for Authorization

Authentication is not supported yet, and we will be introducing authorization-related implementation in the near future.


